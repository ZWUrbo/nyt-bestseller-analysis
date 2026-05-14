from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.ingest.http import HttpError

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
