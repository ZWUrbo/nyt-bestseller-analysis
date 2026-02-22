from __future__ import annotations

import argparse
from typing import List

from src.config import settings
from src.ingest.http import HttpClient
from src.ingest.openlibrary import OpenLibraryClient, OpenLibraryConfig
from src.ingest.repo import OpenLibraryEnrichmentRow, Repo
from src.utils.io import connect_sqlite
from src.utils.logging import get_logger

logger = get_logger("fetch_openlibrary")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=1000, help="Max number of ISBN13 values to process per run.")
    p.add_argument(
        "--refresh-all",
        action="store_true",
        help="Reprocess all distinct NYT ISBN13 values, not only missing work mappings.",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Commit batch size for openlibrary_enrichment upserts.",
    )
    return p.parse_args()


def flush_batches(
    repo: Repo,
    enrichment_rows: List[OpenLibraryEnrichmentRow],
) -> int:
    count = repo.upsert_openlibrary_enrichment(enrichment_rows) if enrichment_rows else 0
    enrichment_rows.clear()
    return count


def main() -> None:
    args = parse_args()
    settings.ensure_dirs()

    http = HttpClient(
        cache_path=settings.http_cache_path,
        expire_seconds=settings.http_cache_expire_seconds,
        contact_email=settings.contact_email,
    )
    ol = OpenLibraryClient(http, OpenLibraryConfig(rps=settings.openlibrary_rps))

    conn = connect_sqlite(settings.db_path)
    repo = Repo(conn)
    repo.init_schema()

    isbn13_values = repo.list_nyt_isbn13(
        limit=args.limit,
        missing_only=not args.refresh_all,
    )

    if not isbn13_values:
        logger.info("No ISBN13 rows to process.")
        conn.close()
        return

    logger.info(
        "Open Library enrichment: processing %s isbn13 values (refresh_all=%s).",
        len(isbn13_values),
        args.refresh_all,
    )

    total_upserts = 0
    failures = 0
    enrichment_batch: List[OpenLibraryEnrichmentRow] = []

    for idx, isbn13 in enumerate(isbn13_values, start=1):
        try:
            row = ol.fetch_isbn13_work(isbn13)
            if row.last_error:
                failures += 1
            enrichment_batch.append(row)
        except Exception as exc:
            failures += 1
            enrichment_batch.append(
                OpenLibraryEnrichmentRow(
                    isbn13=isbn13,
                    work_key=None,
                    subjects=[],
                    subject_places=[],
                    description=None,
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
