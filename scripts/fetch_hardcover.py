from __future__ import annotations

import argparse
from typing import List

from src.config import settings
from src.ingest.hardcover import HardcoverClient, HardcoverConfig
from src.ingest.http import HttpClient
from src.ingest.repo import HardcoverEnrichmentRow, Repo
from src.utils.io import connect_sqlite
from src.utils.logging import get_logger

logger = get_logger("fetch_hardcover")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=1000, help="Max number of ISBN13 values to process per run.")
    p.add_argument(
        "--refresh-all",
        action="store_true",
        help="Reprocess all distinct NYT ISBN13 values, not only missing Hardcover rows.",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Commit batch size for hardcover_enrichment upserts.",
    )
    return p.parse_args()


def flush_batches(
    repo: Repo,
    enrichment_rows: List[HardcoverEnrichmentRow],
) -> int:
    count = repo.upsert_hardcover_enrichment(enrichment_rows) if enrichment_rows else 0
    enrichment_rows.clear()
    return count


def main() -> None:
    args = parse_args()
    settings.ensure_dirs()

    if not settings.hardcover_api_token:
        raise SystemExit("HARDCOVER_API_TOKEN is required for Hardcover enrichment.")

    http = HttpClient(
        cache_path=settings.http_cache_path,
        expire_seconds=settings.http_cache_expire_seconds,
        contact_email=settings.contact_email,
    )
    hardcover = HardcoverClient(
        http,
        HardcoverConfig(
            api_url=settings.hardcover_api_url,
            api_token=settings.hardcover_api_token,
            rps=settings.hardcover_rps,
        ),
    )

    conn = connect_sqlite(settings.db_path)
    repo = Repo(conn)
    repo.init_schema()

    isbn13_values = repo.list_nyt_isbn13(
        limit=args.limit,
        missing_only=not args.refresh_all,
        enrichment_table="hardcover_enrichment",
    )

    if not isbn13_values:
        logger.info("No ISBN13 rows to process.")
        conn.close()
        return

    logger.info(
        "Hardcover enrichment: processing %s isbn13 values (refresh_all=%s).",
        len(isbn13_values),
        args.refresh_all,
    )

    total_upserts = 0
    failures = 0
    enrichment_batch: List[HardcoverEnrichmentRow] = []

    for idx, isbn13 in enumerate(isbn13_values, start=1):
        try:
            row = hardcover.fetch_isbn13_book(isbn13)
            if row.last_error:
                failures += 1
            enrichment_batch.append(row)
        except Exception as exc:
            failures += 1
            enrichment_batch.append(
                HardcoverEnrichmentRow(
                    isbn13=isbn13,
                    book_id=None,
                    title=None,
                    description=None,
                    rating=None,
                    ratings_count=None,
                    users_read_count=None,
                    cached_tags=None,
                    last_error=f"unexpected_error:{str(exc)[:200]}",
                )
            )

        if len(enrichment_batch) >= args.batch_size:
            n = flush_batches(repo, enrichment_batch)
            total_upserts += n
            logger.info(
                "Progress %s/%s | enrichment_upserts=%s | failures=%s",
                idx,
                len(isbn13_values),
                total_upserts,
                failures,
            )

    n = flush_batches(repo, enrichment_batch)
    total_upserts += n

    logger.info(
        "Done. isbn13_processed=%s enrichment_upserts=%s failures=%s",
        len(isbn13_values),
        total_upserts,
        failures,
    )
    conn.close()


if __name__ == "__main__":
    main()
