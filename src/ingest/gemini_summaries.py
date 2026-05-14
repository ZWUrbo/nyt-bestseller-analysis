from __future__ import annotations

import json
from typing import Any, Optional, Sequence

from src.ingest.gemini import extract_generated_text
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


def build_batch_request_line(row: GeminiSummaryInputRow, temperature: float = 0.0) -> dict[str, Any]:
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
                "temperature": temperature,
                "candidateCount": 1,
                "responseMimeType": "application/json",
                "responseJsonSchema": RESPONSE_SCHEMA,
            },
        },
    }


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
