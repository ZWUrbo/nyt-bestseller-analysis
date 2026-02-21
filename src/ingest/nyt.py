from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, Iterable, List, Optional, Sequence
from datetime import date, timedelta

from src.ingest.http import HttpClient, RateLimiter
from src.ingest.repo import NytEntry

NYT_LISTS_ENDPOINT = "https://api.nytimes.com/svc/books/v3/lists/names.json"

@dataclass(frozen=True)
class NytConfig:
    api_key: str
    rps: float = 2.0


class NytClient:
    def __init__(self, http: HttpClient, cfg: NytConfig) -> None:
        if not cfg.api_key:
            raise ValueError("NYT_API_KEY is missing. SEt it in your environment or .env.")
        self.http = http
        self.cfg = cfg
        self.limiter = RateLimiter(cfg.rps)
    
    def fetch_lists_for_date(self, published_date: str) -> List[NytEntry]:
        """
        Fetch ALL lists for a specific published_date (YYYY-MM-DD).
        This endpoint returns list entries across multiple categories.
        """
        self.limiter.wait()
        data = self.http.get_json(
            NYT_LISTS_ENDPOINT,
            params={"api-key": self.cfg.api_key, "published_date": published_date},
        )
        results = data.get("results", [])
        entries: List[NytEntry] = []
        for item in results:
            # NYT returns an array book_details with 1 element (usually)
            bd = None
            bds = item.get("book_details") or []
            if bds:
                bd = bds[0]
            entries.append(
                NytEntry(
                    list_name = item.get("list_name",""),
                    published_date = item.get("published_date",published_date),
                    rank = item.get("rank"),
                    weeks_on_list = item.get("weeks_on_list"),
                    title = (bd or {}).get("titlle") or item.get("title") or "",
                    author = (bd or {}).get("author") or item.get("author"),
                    publisher = (bd or {}).get("publisher"),
                    isbn13 = (bd or {}).get("primary_isbn13") or item.get("primary_isbn13"),
                    isbn10 = (bd or {}).get("primary_isbn10") or item.get("primary_isbn10"),
                    description = (bd or {}).get("description")
                )
            )
        # Filter out empty titles
        return [e for e in entries if e.title.strip()]
    
@staticmethod
def iter_weekly_dates(start: date, end: date) -> Iterable[date]:
    """
    NYTlists are weekly. We'll step by 7 days.
    """
    d = start
    while d <= end:
        yield d
        d += timedelta(days=7)

