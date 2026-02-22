from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from src.ingest.http import HttpClient, RateLimiter
from src.ingest.repo import OpenLibraryEnrichmentRow

OPENLIBRARY_BASE_URL = "https://openlibrary.org"


@dataclass(frozen=True)
class OpenLibraryConfig:
    rps: float = 5.0


class OpenLibraryClient:
    def __init__(self, http: HttpClient, cfg: OpenLibraryConfig) -> None:
        self.http = http
        self.cfg = cfg
        self.limiter = RateLimiter(cfg.rps)

    def fetch_isbn13_work(self, isbn13: str) -> OpenLibraryEnrichmentRow:
        normalized_isbn13 = isbn13.strip()
        if not normalized_isbn13:
            return OpenLibraryEnrichmentRow(
                isbn13=isbn13,
                work_key=None,
                subjects=[],
                subject_places=[],
                description=None,
                last_error="isbn13_missing",
            )

        self.limiter.wait()
        edition = self.http.get_json_or_none(f"{OPENLIBRARY_BASE_URL}/isbn/{normalized_isbn13}.json")
        if not edition:
            return OpenLibraryEnrichmentRow(
                isbn13=normalized_isbn13,
                work_key=None,
                subjects=[],
                subject_places=[],
                description=None,
                last_error="edition_not_found",
            )

        work_key = _extract_work_key(edition)
        if not work_key:
            return OpenLibraryEnrichmentRow(
                isbn13=normalized_isbn13,
                work_key=None,
                subjects=[],
                subject_places=[],
                description=None,
                last_error="work_key_missing",
            )

        self.limiter.wait()
        work = self.http.get_json_or_none(f"{OPENLIBRARY_BASE_URL}{work_key}.json")
        if not work:
            return OpenLibraryEnrichmentRow(
                isbn13=normalized_isbn13,
                work_key=work_key,
                subjects=[],
                subject_places=[],
                description=None,
                last_error="work_not_found",
            )

        return OpenLibraryEnrichmentRow(
            isbn13=normalized_isbn13,
            work_key=work_key,
            subjects=_extract_string_list(work.get("subjects")),
            subject_places=_extract_string_list(work.get("subject_places")),
            description=_extract_description(work.get("description")),
            last_error=None,
        )


def _safe_str(value: Any) -> Optional[str]:
    if isinstance(value, str):
        v = value.strip()
        return v or None
    return None


def _extract_work_key(edition: Dict[str, Any]) -> Optional[str]:
    works = edition.get("works") or []
    for item in works:
        if isinstance(item, dict):
            key = _safe_str(item.get("key"))
            if key and key.startswith("/works/"):
                return key
    return None


def _extract_description(value: Any) -> Optional[str]:
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, dict):
        # Open Library commonly returns {"type": "/type/text", "value": "..."}
        inner = value.get("value")
        if isinstance(inner, str):
            return inner.strip() or None
    return None


def _extract_string_list(value: Any) -> Sequence[str]:
    if not isinstance(value, list):
        return []
    out: List[str] = []
    for item in value:
        if isinstance(item, str):
            clean = item.strip()
            if clean:
                out.append(clean)
    return _unique_preserve_order(out)


def _unique_preserve_order(values: Sequence[str]) -> Sequence[str]:
    seen = set()
    out: List[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out
