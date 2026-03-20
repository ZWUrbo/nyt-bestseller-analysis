from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

from src.ingest.http import HttpClient, RateLimiter
from src.ingest.repo import HardcoverEnrichmentRow

EDITION_LOOKUP_QUERY = """
query EditionByIsbn($isbn13: String!) {
  editions(where: {isbn_13: {_eq: $isbn13}}, limit: 1) {
    book_id
  }
}
"""

BOOK_LOOKUP_QUERY = """
query BookById($bookId: Int!) {
  books(where: {id: {_eq: $bookId}}, limit: 1) {
    title
    description
    rating
    ratings_count
    users_read_count
    cached_tags
  }
}
"""


@dataclass(frozen=True)
class HardcoverConfig:
    api_url: str
    api_token: str
    rps: float = 1.0


class HardcoverClient:
    def __init__(self, http: HttpClient, cfg: HardcoverConfig) -> None:
        self.http = http
        self.cfg = cfg
        self.limiter = RateLimiter(cfg.rps)

    def fetch_isbn13_book(self, isbn13: str) -> HardcoverEnrichmentRow:
        normalized_isbn13 = isbn13.strip()
        if not normalized_isbn13:
            return HardcoverEnrichmentRow(
                isbn13=isbn13,
                book_id=None,
                title=None,
                description=None,
                rating=None,
                ratings_count=None,
                users_read_count=None,
                cached_tags=None,
                last_error="isbn13_missing",
            )

        edition_payload = self._query(
            EDITION_LOOKUP_QUERY,
            {"isbn13": normalized_isbn13},
        )
        editions = edition_payload.get("data", {}).get("editions") or []
        if not editions:
            return HardcoverEnrichmentRow(
                isbn13=normalized_isbn13,
                book_id=None,
                title=None,
                description=None,
                rating=None,
                ratings_count=None,
                users_read_count=None,
                cached_tags=None,
                last_error="edition_not_found",
            )

        book_id = _extract_int(editions[0].get("book_id"))
        if book_id is None:
            return HardcoverEnrichmentRow(
                isbn13=normalized_isbn13,
                book_id=None,
                title=None,
                description=None,
                rating=None,
                ratings_count=None,
                users_read_count=None,
                cached_tags=None,
                last_error="book_id_missing",
            )

        book_payload = self._query(BOOK_LOOKUP_QUERY, {"bookId": book_id})
        books = book_payload.get("data", {}).get("books") or []
        if not books:
            return HardcoverEnrichmentRow(
                isbn13=normalized_isbn13,
                book_id=book_id,
                title=None,
                description=None,
                rating=None,
                ratings_count=None,
                users_read_count=None,
                cached_tags=None,
                last_error="book_not_found",
            )

        book = books[0]
        return HardcoverEnrichmentRow(
            isbn13=normalized_isbn13,
            book_id=book_id,
            title=_safe_str(book.get("title")),
            description=_safe_str(book.get("description")),
            rating=_extract_float(book.get("rating")),
            ratings_count=_extract_int(book.get("ratings_count")),
            users_read_count=_extract_int(book.get("users_read_count")),
            cached_tags=_serialize_json(book.get("cached_tags")),
            last_error=None,
        )

    def _query(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        self.limiter.wait()
        return self.http.post_json(
            self.cfg.api_url,
            json_body={"query": query, "variables": variables},
            headers={
                "Content-Type": "application/json",
                "authorization": self.cfg.api_token,
            },
        )


def _safe_str(value: Any) -> Optional[str]:
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    return None


def _extract_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def _extract_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _serialize_json(value: Any) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)
