from __future__ import annotations

import argparse
from datetime import date

from src.config import settings
from src.utils.logging import get_logger
from src.utils.io import connect_sqlite
from src.ingest.http import HttpClient
from src.ingest.nyt import NytClient, NytConfig
from src.ingest.repo import Repo


logger = get_logger("fetch_nyt")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--start", type=str, default=None, help="Start date YYYY-MM-DD (default: Jan 1 of START_YEAR)")
    p.add_argument("--end", type=str, default=None, help="End date YYYY-MM-DD (default: Dec 31 of END_YEAR)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    settings.ensure_dirs()

    start = date.fromisoformat(args.start) if args.start else date(settings.start_year, 1, 1)
    end = date.fromisoformat(args.end) if args.end else date(settings.end_year, 12, 31)

    http = HttpClient(settings.http_cache_path, settings.http_cache_expire_seconds)
    nyt = NytClient(http, NytConfig(api_key=settings.nyt_api_key, rps=settings.nyt_rps))

    conn = connect_sqlite(settings.db_path)
    repo = Repo(conn)
    repo.init_schema()

    logger.info("NYT ingestion uses the overview endpoint (top 15 books per list per week).")

    total = 0
    failures = 0
    for d in NytClient.iter_weekly_dates(start, end):
        ds = d.isoformat()
        try:
            entries = nyt.fetch_lists_for_date(ds)
            n = repo.upsert_nyt_entries(entries)
            total += n
            logger.info(f"{ds}: upserted {n} NYT entries")
        except Exception:
            failures += 1
            logger.exception(f"{ds}: failed")

    logger.info(f"Done. Total NYT upserts: {total} | failures: {failures}")
    conn.close()

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
