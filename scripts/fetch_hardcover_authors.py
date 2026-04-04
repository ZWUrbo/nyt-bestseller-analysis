from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import settings
from src.ingest.hardcover import HardcoverClient, HardcoverConfig
from src.ingest.http import HttpClient
from src.ingest.repo import HardcoverAuthorRow, Repo
from src.utils.io import connect_sqlite
from src.utils.logging import get_logger

logger = get_logger("fetch_hardcover_authors")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=1000, help="Max number of Hardcover author ids to process per run.")
    p.add_argument(
        "--refresh-all",
        action="store_true",
        help="Reprocess all distinct Hardcover author ids, not only missing author rows.",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Commit batch size for hardcover_authors upserts.",
    )
    return p.parse_args()


def flush_batches(
    repo: Repo,
    author_rows: List[HardcoverAuthorRow],
) -> int:
    count = repo.upsert_hardcover_authors(author_rows) if author_rows else 0
    author_rows.clear()
    return count


def main() -> None:
    args = parse_args()
    settings.ensure_dirs()

    if not settings.hardcover_api_token:
        raise SystemExit("HARDCOVER_API_TOKEN is required for Hardcover author enrichment.")

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

    author_ids = repo.list_hardcover_author_ids(
        limit=args.limit,
        missing_only=not args.refresh_all,
    )

    if not author_ids:
        logger.info("No Hardcover author ids to process.")
        conn.close()
        return

    logger.info(
        "Hardcover author enrichment: processing %s author ids (refresh_all=%s).",
        len(author_ids),
        args.refresh_all,
    )

    total_upserts = 0
    failures = 0
    author_batch: List[HardcoverAuthorRow] = []

    for idx, author_id in enumerate(author_ids, start=1):
        try:
            row = hardcover.fetch_author(author_id)
            if row.last_error:
                failures += 1
            author_batch.append(row)
        except Exception as exc:
            failures += 1
            author_batch.append(
                HardcoverAuthorRow(
                    author_id=author_id,
                    name=None,
                    born_date=None,
                    born_year=None,
                    death_year=None,
                    location=None,
                    gender_id=None,
                    is_lgbtq=None,
                    is_bipoc=None,
                    last_error=f"unexpected_error:{str(exc)[:200]}",
                )
            )

        if len(author_batch) >= args.batch_size:
            n = flush_batches(repo, author_batch)
            total_upserts += n
            logger.info(
                "Progress %s/%s | author_upserts=%s | failures=%s",
                idx,
                len(author_ids),
                total_upserts,
                failures,
            )

    n = flush_batches(repo, author_batch)
    total_upserts += n

    logger.info(
        "Done. author_ids_processed=%s author_upserts=%s failures=%s",
        len(author_ids),
        total_upserts,
        failures,
    )
    conn.close()


if __name__ == "__main__":
    main()
