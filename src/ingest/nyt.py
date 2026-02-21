from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, Iterable, List, Optional, Sequence
from datetime import date, timedelta

from src.ingest.http import HttpClient, RateLimiter
from src.ingest.repo import NytEntry

# NYT Books API: overview returns top 5 books for all lists; accepts published_date query param.
# Spec: /svc/books/v3/lists/overview.json?published_date=YYYY-MM-DD :contentReference[oaicite:2]{index=2}
NYT_LISTS_ENDPOINT = "https://api.nytimes.com/svc/books/v3/lists/overview.json"

@dataclass(frozen=True)
class NytConfig:
    api_key: str
    rps: float = 2.0


class NytClient:
    def __init__(self, http: HttpClient, cfg: NytConfig) -> None:
        if not cfg.api_key:
            raise ValueError("NYT_API_KEY is missing. Set it in your environment or .env.")
        self.http = http
        self.cfg = cfg
        self.limiter = RateLimiter(cfg.rps)
    
    def fetch_lists_for_date(self, published_date: str) -> List[NytEntry]:
        """
        Fetch NYT Best Sellers overview for a specific published_date (YYYY-MM-DD).

        NOTE: The overview endpoint returns the TOP 5 books for each list(not the full list)
        """
        self.limiter.wait()
        data = self.http.get_json(
            NYT_LISTS_ENDPOINT,
            params={"api-key": self.cfg.api_key, "published_date": published_date},
        )

        results = Dict[str, Any] = data.get("results") or {}
        # Some responses include a top-level published_date in results, fall bavck to requested date.
        resolved_published_date = results.get("published_date") or published_date

        lists = results.get("lists") or []
        entries: List[NytEntry] = []

        for lst in lists:
            list_name = (lst.get("list_name") or "").strip()
            # "books" should be a list of dicts for top books in that list
            books = list.get("books") or []
            for book in books:
                #NYT typically uses these keys in overview books.
                title = (book.get("title") or "").strip()
                if not title:
                    continue

                entries.append(
                    NytEntry(
                        list_name = list_name,
                        published_date = resolved_published_date,
                        rank = book.get("rank"),
                        weeks_on_list = book.get("weeks_on_list"),
                        title = title,
                        author = book.get("author"),
                        publisher = book.get("publisher"),
                        isbn13 = book.get("primary_isbn13"),
                        isbn10 = book.get("primary_isbn10"),
                        description = book.get("description")
                    )
                )
            # Filter out any weird empty list names/titles
            return [e for e in entries if e.title.strip() and e.list_name.strip()]
    
@staticmethod
def iter_weekly_dates(start: date, end: date) -> Iterable[date]:
    """
    NYTlists are weekly. We'll step by 7 days.
    """
    d = start
    while d <= end:
        yield d
        d += timedelta(days=7)

