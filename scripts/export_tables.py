from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.config import settings
from src.utils.io import connect_sqlite

TABLE_NAMES = ("nyt_entries", "openlibrary_enrichment", "hardcover_enrichment")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export analysis-ready CSV snapshots for core SQLite tables."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=settings.db_path,
        help="Path to the SQLite database.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=settings.data_dir / "processed" / "exports",
        help="Destination directory for exported CSV files.",
    )
    return parser.parse_args()


def export_table(db_path: Path, output_dir: Path, table_name: str) -> tuple[Path, int, int]:
    conn = connect_sqlite(db_path)
    try:
        frame = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    finally:
        conn.close()

    output_path = output_dir / f"{table_name}.csv"
    frame.to_csv(output_path, index=False)
    return output_path, len(frame), len(frame.columns)


def main() -> None:
    args = parse_args()
    settings.ensure_dirs()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    for table_name in TABLE_NAMES:
        output_path, row_count, column_count = export_table(
            db_path=args.db_path,
            output_dir=args.output_dir,
            table_name=table_name,
        )
        print(
            f"Exported {table_name} to {output_path} "
            f"(rows={row_count}, columns={column_count})."
        )


if __name__ == "__main__":
    main()
