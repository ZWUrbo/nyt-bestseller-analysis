from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional

from src.ingest.http import HttpClient, HttpError, RateLimiter
from src.ingest.repo import HardcoverAuthorRow, HardcoverEnrichmentRow

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
    contributions {
      author_id
    }
  }
}
"""

AUTHOR_QUERY_TEMPLATE = """
query AuthorById($authorId: Int!) {{
  authors(where: {{id: {{_eq: $authorId}}}}, limit: 1) {{
    {fields}
  }}
}}
"""

AUTHOR_FIELD_ORDER = [
    "name",
    "born_date",
    "born_year",
    "death_year",
    "location",
    "gender_id",
    "is_lgbtq",
    "is_bipoc",
]
UNSUPPORTED_FIELD_RE = re.compile(r"Cannot query field ['\"]([^'\"]+)['\"]")


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
        self._supported_author_fields: list[str] | None = None

    def fetch_isbn13_book(self, isbn13: str) -> HardcoverEnrichmentRow:
        normalized_isbn13 = isbn13.strip()
        if not normalized_isbn13:
            return HardcoverEnrichmentRow(
                isbn13=isbn13,
                book_id=None,
                author_id=None,
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
                author_id=None,
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
                author_id=None,
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
                author_id=None,
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
            author_id=_extract_author_id(book.get("contributions")),
            title=_safe_str(book.get("title")),
            description=_safe_str(book.get("description")),
            rating=_extract_float(book.get("rating")),
            ratings_count=_extract_int(book.get("ratings_count")),
            users_read_count=_extract_int(book.get("users_read_count")),
            cached_tags=_serialize_json(book.get("cached_tags")),
            last_error=None,
        )

    def fetch_author(self, author_id: int) -> HardcoverAuthorRow:
        if author_id <= 0:
            return HardcoverAuthorRow(
                author_id=author_id,
                name=None,
                born_date=None,
                born_year=None,
                death_year=None,
                location=None,
                gender_id=None,
                is_lgbtq=None,
                is_bipoc=None,
                last_error="author_id_invalid",
            )

        author_payload = self._query_author(author_id)
        authors = author_payload.get("data", {}).get("authors") or []
        if not authors:
            return HardcoverAuthorRow(
                author_id=author_id,
                name=None,
                born_date=None,
                born_year=None,
                death_year=None,
                location=None,
                gender_id=None,
                is_lgbtq=None,
                is_bipoc=None,
                last_error="author_not_found",
            )

        author = authors[0]
        return HardcoverAuthorRow(
            author_id=author_id,
            name=_safe_str(author.get("name")),
            born_date=_safe_str(author.get("born_date")),
            born_year=_extract_int(author.get("born_year")),
            death_year=_extract_int(author.get("death_year")),
            location=_safe_str(author.get("location")),
            gender_id=_extract_int(author.get("gender_id")),
            is_lgbtq=_extract_bool(author.get("is_lgbtq")),
            is_bipoc=_extract_bool(author.get("is_bipoc")),
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

    def _query_author(self, author_id: int) -> Dict[str, Any]:
        if self._supported_author_fields is None:
            self._supported_author_fields = list(AUTHOR_FIELD_ORDER)

        while self._supported_author_fields:
            query = AUTHOR_QUERY_TEMPLATE.format(
                fields="\n    ".join(self._supported_author_fields)
            )
            try:
                return self._query(query, {"authorId": author_id})
            except HttpError as exc:
                unsupported_field = _extract_unsupported_field(str(exc))
                if unsupported_field and unsupported_field in self._supported_author_fields:
                    self._supported_author_fields.remove(unsupported_field)
                    continue
                raise

        return {"data": {"authors": []}}


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


def _extract_author_id(value: Any) -> Optional[int]:
    if not isinstance(value, list):
        return None
    for item in value:
        if not isinstance(item, dict):
            continue
        author_id = _extract_int(item.get("author_id"))
        if author_id is not None:
            return author_id
    return None


def _extract_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    return None


def _extract_unsupported_field(error_message: str) -> Optional[str]:
    match = UNSUPPORTED_FIELD_RE.search(error_message)
    if match:
        return match.group(1)
    return None
