from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.ingest.http import HttpError
from src.ingest.repo import GeminiContentSummaryRow, GeminiSummaryInputRow

PROMPT_TEMPLATE = """Book Title: {book_title}
Book Author: {book_author}
ISBN13: {isbn13}

You are generating a content summary for subject and theme extraction.

Your goal is to describe what the book is substantively about so that downstream processing can identify the central subjects, themes, issues, settings, and historical or social context present in the work.

Return a single JSON object that matches the provided response schema.

Prioritize:
central subjects and topics
major themes and recurring ideas
social, political, psychological, historical, or cultural issues
setting and context when they shape the content
main character or focal figure only insofar as it helps explain the content

Do not prioritize:
marketing language
reader appeal
sales language
vague praise
generic mood unless it reflects a core theme
spoiler-heavy details

Field requirements:
- isbn13: copy the ISBN13 exactly as provided above
- summary: exactly one paragraph of 120 to 180 words
- content_tag_seed: an array with exactly 8 short tags capturing the most useful subject/theme/context signals"""

TAG_PREFIX = "Content Tags Seed:"
RESPONSE_SCHEMA = {
    "type": "object",
    "required": ["isbn13", "summary", "content_tag_seed"],
    "properties": {
        "isbn13": {
            "type": "string",
            "description": "The ISBN13 copied exactly from the prompt input.",
        },
        "summary": {
            "type": "string",
            "description": "A single-paragraph content summary between 120 and 180 words.",
        },
        "content_tag_seed": {
            "type": "array",
            "description": "Exactly 8 concise subject/theme/context tags.",
            "minItems": 8,
            "maxItems": 8,
            "items": {
                "type": "string",
            },
        },
    },
}
TERMINAL_BATCH_STATES = {
    "BATCH_STATE_SUCCEEDED",
    "BATCH_STATE_FAILED",
    "BATCH_STATE_CANCELLED",
    "BATCH_STATE_EXPIRED",
}


@dataclass(frozen=True)
class GeminiConfig:
    api_url: str
    api_key: str
    model: str = "gemini-2.5-flash-lite"
    temperature: float = 0.0


class GeminiClient:
    def __init__(self, cfg: GeminiConfig) -> None:
        self.cfg = cfg

    def build_batch_request_line(self, row: GeminiSummaryInputRow) -> dict[str, Any]:
        prompt = PROMPT_TEMPLATE.format(
            book_title=(row.title or "").strip() or "Unknown",
            book_author=(row.author or "").strip() or "Unknown",
            isbn13=row.isbn13,
        )
        return {
            "key": row.isbn13,
            "request": {
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            {"text": prompt},
                        ],
                    }
                ],
                "generationConfig": {
                    "temperature": self.cfg.temperature,
                    "candidateCount": 1,
                    "responseMimeType": "application/json",
                    "responseJsonSchema": RESPONSE_SCHEMA,
                },
            },
        }

    def upload_jsonl_file(self, path: Path, display_name: str) -> dict[str, Any]:
        path = path.resolve()
        total_bytes = path.stat().st_size
        start_response = self._request(
            "post",
            f"{self.cfg.api_url.replace('/v1beta', '')}/upload/v1beta/files",
            headers={
                "x-goog-api-key": self.cfg.api_key,
                "X-Goog-Upload-Protocol": "resumable",
                "X-Goog-Upload-Command": "start",
                "X-Goog-Upload-Header-Content-Length": str(total_bytes),
                "X-Goog-Upload-Header-Content-Type": "application/jsonl",
                "Content-Type": "application/json",
            },
            json_body={"file": {"display_name": display_name}},
            expected_json=False,
        )
        upload_url = start_response.headers.get("x-goog-upload-url")
        if not upload_url:
            raise HttpError("Missing x-goog-upload-url header from Gemini file upload start response.")

        with path.open("rb") as fh:
            upload_response = self._request(
                "post",
                upload_url,
                headers={
                    "x-goog-api-key": self.cfg.api_key,
                    "Content-Length": str(total_bytes),
                    "X-Goog-Upload-Offset": "0",
                    "X-Goog-Upload-Command": "upload, finalize",
                },
                data=fh.read(),
            )
        return upload_response

    def create_batch_job(self, input_file_name: str, display_name: str) -> dict[str, Any]:
        return self._request(
            "post",
            f"{self.cfg.api_url}/models/{self.cfg.model}:batchGenerateContent",
            headers={
                "x-goog-api-key": self.cfg.api_key,
                "Content-Type": "application/json",
            },
            json_body={
                "batch": {
                    "display_name": display_name,
                    "input_config": {
                        "file_name": input_file_name,
                    },
                }
            },
        )

    def get_batch_job(self, batch_name: str) -> dict[str, Any]:
        return self._request(
            "get",
            f"{self.cfg.api_url}/{batch_name}",
            headers={"x-goog-api-key": self.cfg.api_key},
        )

    def download_file_bytes(self, file_name: str) -> bytes:
        file_payload = self._request(
            "get",
            f"{self.cfg.api_url}/{file_name}",
            headers={"x-goog-api-key": self.cfg.api_key},
        )
        download_uri = file_payload.get("downloadUri") or file_payload.get("download_uri")
        if not isinstance(download_uri, str) or not download_uri.strip():
            raise HttpError(f"Missing downloadUri for Gemini file: {file_name}")

        response = self._request(
            "get",
            download_uri,
            headers={"x-goog-api-key": self.cfg.api_key},
            expected_json=False,
        )
        return response.content

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=0.8, min=1, max=20),
        retry=retry_if_exception_type((requests.RequestException, HttpError)),
    )
    def _request(
        self,
        method: str,
        url: str,
        headers: Optional[dict[str, str]] = None,
        json_body: Optional[dict[str, Any]] = None,
        data: Optional[bytes] = None,
        expected_json: bool = True,
    ) -> Any:
        response = requests.request(
            method=method.upper(),
            url=url,
            headers=headers,
            json=json_body,
            data=data,
            timeout=120,
        )
        if response.status_code == 429:
            raise HttpError("Rate limited (HTTP 429)")
        if response.status_code >= 500:
            raise HttpError(f"Server error (HTTP {response.status_code})")
        if response.status_code >= 400:
            raise HttpError(f"Client error (HTTP {response.status_code}): {response.text[:500]}")
        if not expected_json:
            return response
        return response.json()

def extract_generated_text(payload: dict[str, Any]) -> Optional[str]:
    candidates = payload.get("candidates")
    if not isinstance(candidates, list):
        return None

    collected_parts: list[str] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        content = candidate.get("content")
        if not isinstance(content, dict):
            continue
        parts = content.get("parts")
        if not isinstance(parts, list):
            continue
        for part in parts:
            if isinstance(part, dict) and isinstance(part.get("text"), str):
                collected_parts.append(part["text"].strip())

    combined = "\n".join([part for part in collected_parts if part])
    return combined or None


def parse_summary_response(raw_text: str) -> tuple[Optional[str], Sequence[str], Optional[str]]:
    structured_summary, structured_tags, structured_error = parse_structured_summary_response(raw_text)
    if structured_summary is not None or structured_tags or structured_error != "structured_json_invalid":
        return structured_summary, structured_tags, structured_error

    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
    if not lines:
        return None, [], "empty_response"

    tag_line = lines[-1]
    if not tag_line.startswith(TAG_PREFIX):
        return clean_whitespace(" ".join(lines)), [], "tag_line_missing"

    summary = clean_whitespace(" ".join(lines[:-1]))
    tags = [clean_whitespace(part) for part in tag_line[len(TAG_PREFIX):].split(";")]
    tags = [tag for tag in tags if tag]

    errors: list[str] = []
    word_count = len(summary.split()) if summary else 0
    if summary and not 120 <= word_count <= 180:
        errors.append(f"word_count_out_of_range:{word_count}")
    if len(tags) != 8:
        errors.append(f"tag_count_invalid:{len(tags)}")

    return summary or None, tags, ",".join(errors) if errors else None


def parse_structured_summary_response(
    raw_text: str,
) -> tuple[Optional[str], Sequence[str], Optional[str]]:
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        return None, [], "structured_json_invalid"

    if not isinstance(parsed, dict):
        return None, [], "structured_json_not_object"

    summary_value = parsed.get("summary")
    summary = clean_whitespace(summary_value) if isinstance(summary_value, str) else None

    tags_value = parsed.get("content_tag_seed")
    tags: list[str] = []
    if isinstance(tags_value, list):
        for item in tags_value:
            if isinstance(item, str):
                cleaned = clean_whitespace(item)
                if cleaned:
                    tags.append(cleaned)

    isbn13_value = parsed.get("isbn13")
    isbn13 = clean_whitespace(isbn13_value) if isinstance(isbn13_value, str) else None

    errors: list[str] = []
    if not isbn13:
        errors.append("isbn13_missing")
    if not summary:
        errors.append("summary_missing")
    if summary:
        word_count = len(summary.split())
        if not 120 <= word_count <= 180:
            errors.append(f"word_count_out_of_range:{word_count}")
    if len(tags) != 8:
        errors.append(f"tag_count_invalid:{len(tags)}")

    return summary, tags, ",".join(errors) if errors else None


def parse_batch_result_line(raw_line: str) -> GeminiContentSummaryRow:
    parsed = json.loads(raw_line)
    isbn13 = _extract_result_key(parsed)
    response_payload = parsed.get("response")
    error_payload = parsed.get("error")

    if isinstance(error_payload, dict):
        error_message = error_payload.get("message") or json.dumps(error_payload, ensure_ascii=False)
        return GeminiContentSummaryRow(
            isbn13=isbn13,
            summary=None,
            content_tags_seed=[],
            raw_response=json.dumps(parsed, ensure_ascii=False),
            last_error=f"batch_error:{clean_whitespace(str(error_message))[:300]}",
        )

    if not isinstance(response_payload, dict):
        return GeminiContentSummaryRow(
            isbn13=isbn13,
            summary=None,
            content_tags_seed=[],
            raw_response=json.dumps(parsed, ensure_ascii=False),
            last_error="batch_response_missing",
        )

    raw_text = extract_generated_text(response_payload)
    if not raw_text:
        return GeminiContentSummaryRow(
            isbn13=isbn13,
            summary=None,
            content_tags_seed=[],
            raw_response=json.dumps(parsed, ensure_ascii=False),
            last_error="batch_text_missing",
        )

    summary, tags, format_error = parse_summary_response(raw_text)
    structured_isbn13_error = validate_structured_isbn13(raw_text, expected_isbn13=isbn13)
    if structured_isbn13_error:
        format_error = ",".join([x for x in [format_error, structured_isbn13_error] if x])
    return GeminiContentSummaryRow(
        isbn13=isbn13,
        summary=summary,
        content_tags_seed=tags,
        raw_response=raw_text,
        last_error=format_error,
    )


def clean_whitespace(value: str) -> str:
    return " ".join(value.split()).strip()


def _extract_result_key(parsed: dict[str, Any]) -> str:
    key = parsed.get("key")
    if isinstance(key, str) and key.strip():
        return key.strip()
    metadata = parsed.get("metadata")
    if isinstance(metadata, dict):
        metadata_key = metadata.get("key")
        if isinstance(metadata_key, str) and metadata_key.strip():
            return metadata_key.strip()
    return ""


def validate_structured_isbn13(raw_text: str, expected_isbn13: str) -> Optional[str]:
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        return None

    if not isinstance(parsed, dict):
        return "structured_json_not_object"

    isbn13_value = parsed.get("isbn13")
    if not isinstance(isbn13_value, str) or not isbn13_value.strip():
        return "isbn13_missing"
    if isbn13_value.strip() != expected_isbn13:
        return "isbn13_mismatch"
    return None
