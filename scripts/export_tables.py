from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import settings
from src.utils.io import connect_sqlite

TABLE_NAMES = ("nyt_entries", "openlibrary_enrichment", "hardcover_enrichment", "hardcover_authors")
HARDCOVER_CATEGORY_COLUMNS = {
    "Content Warning": "content_warning",
    "Genre": "genre",
    "Mood": "mood",
    "Tag": "tag",
}
WHITESPACE_RE = re.compile(r"\s+")


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


def normalize_text(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    return WHITESPACE_RE.sub(" ", value.replace("\r", " ").replace("\n", " ")).strip()


def parse_cached_tags(raw_value: Any) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(raw_value, str) or not raw_value.strip():
        return {}
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {
        key: value
        for key, value in parsed.items()
        if isinstance(key, str) and isinstance(value, list)
    }


def extract_tag_names(category_items: list[dict[str, Any]]) -> list[str]:
    tag_names: set[str] = set()
    for item in category_items:
        if not isinstance(item, dict):
            continue
        raw_tag = item.get("tag")
        if not isinstance(raw_tag, str) or not raw_tag.strip():
            raw_tag = item.get("tagSlug")
        cleaned = normalize_text(raw_tag)
        if cleaned:
            tag_names.add(cleaned)
    return sorted(tag_names, key=str.casefold)


def make_hardcover_export_frame(frame: pd.DataFrame) -> pd.DataFrame:
    export_frame = frame.copy()

    for column in export_frame.select_dtypes(include=["object"]).columns:
        export_frame[column] = export_frame[column].map(normalize_text)

    parsed_tags = export_frame["cached_tags"].map(parse_cached_tags) if "cached_tags" in export_frame.columns else None
    if parsed_tags is not None:
        for category_name, column_stub in HARDCOVER_CATEGORY_COLUMNS.items():
            names_by_row = parsed_tags.map(lambda parsed: extract_tag_names(parsed.get(category_name, [])))
            export_frame[f"{column_stub}_tags"] = names_by_row.map(" | ".join)
            export_frame[f"{column_stub}_tag_count"] = names_by_row.map(len).astype("int64")
        export_frame = export_frame.rename(columns={"cached_tags": "cached_tags_json"})

    return export_frame


def make_hardcover_author_export_frame(frame: pd.DataFrame) -> pd.DataFrame:
    export_frame = frame.copy()

    for column in export_frame.select_dtypes(include=["object"]).columns:
        export_frame[column] = export_frame[column].map(normalize_text)

    for column in ("is_lgbtq", "is_bipoc"):
        if column in export_frame.columns:
            export_frame[column] = export_frame[column].map(
                lambda value: None if pd.isna(value) else bool(int(value))
            )

    return export_frame


def export_table(db_path: Path, output_dir: Path, table_name: str) -> tuple[Path, int, int]:
    conn = connect_sqlite(db_path)
    try:
        frame = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    finally:
        conn.close()

    if table_name == "hardcover_enrichment":
        frame = make_hardcover_export_frame(frame)
    elif table_name == "hardcover_authors":
        frame = make_hardcover_author_export_frame(frame)

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
