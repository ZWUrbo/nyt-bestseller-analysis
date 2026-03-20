from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import settings
from src.utils.io import connect_sqlite

CATEGORY_OUTPUTS = {
    "Content Warning": "content_warning",
    "Genre": "genre",
    "Mood": "mood",
    "Tag": "tag",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export binary document-term matrices keyed by isbn13 from Hardcover cached_tags."
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
        default=settings.data_dir / "processed" / "features" / "hardcover_tag_dtms",
        help="Destination directory for exported CSV files.",
    )
    parser.add_argument(
        "--min-doc-freq",
        type=int,
        default=1,
        help="Keep only terms that appear in at least this many isbn13 rows.",
    )
    return parser.parse_args()


def load_source_frame(db_path: Path) -> pd.DataFrame:
    query = """
    SELECT DISTINCT
        n.isbn13,
        COALESCE(h.cached_tags, '') AS cached_tags
    FROM nyt_entries n
    LEFT JOIN hardcover_enrichment h
        ON h.isbn13 = n.isbn13
    WHERE n.isbn13 IS NOT NULL
      AND TRIM(n.isbn13) <> ''
    ORDER BY n.isbn13
    """
    conn = connect_sqlite(db_path)
    try:
        return pd.read_sql_query(query, conn)
    finally:
        conn.close()


def parse_cached_tags(raw_value: str) -> dict[str, list[dict[str, Any]]]:
    if not raw_value or not raw_value.strip():
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


def iter_term_rows(source_frame: pd.DataFrame, category_name: str) -> Iterable[dict[str, str | int]]:
    for row in source_frame.itertuples(index=False):
        parsed_tags = parse_cached_tags(row.cached_tags)
        category_items = parsed_tags.get(category_name) or []
        seen_terms: set[str] = set()

        for item in category_items:
            if not isinstance(item, dict):
                continue
            tag_slug = item.get("tagSlug")
            if isinstance(tag_slug, str):
                cleaned = tag_slug.strip()
                if cleaned:
                    seen_terms.add(cleaned)

        for term in seen_terms:
            yield {"isbn13": row.isbn13, "term": term, "value": 1}


def build_matrix(source_frame: pd.DataFrame, category_name: str, min_doc_freq: int) -> pd.DataFrame:
    term_frame = pd.DataFrame.from_records(iter_term_rows(source_frame, category_name))

    if term_frame.empty:
        return pd.DataFrame({"isbn13": source_frame["isbn13"]}).drop_duplicates()

    if min_doc_freq > 1:
        doc_freq = term_frame.groupby("term")["isbn13"].nunique()
        allowed_terms = doc_freq[doc_freq >= min_doc_freq].index
        term_frame = term_frame[term_frame["term"].isin(allowed_terms)]

    if term_frame.empty:
        return pd.DataFrame({"isbn13": source_frame["isbn13"]}).drop_duplicates()

    matrix = (
        term_frame.pivot_table(
            index="isbn13",
            columns="term",
            values="value",
            aggfunc="max",
            fill_value=0,
        )
        .astype("int8")
        .reset_index()
    )

    all_isbn = pd.DataFrame({"isbn13": source_frame["isbn13"]}).drop_duplicates()
    matrix = all_isbn.merge(matrix, on="isbn13", how="left").fillna(0)

    for column in matrix.columns:
        if column != "isbn13":
            matrix[column] = matrix[column].astype("int8")

    return matrix.sort_values("isbn13").reset_index(drop=True)


def main() -> None:
    args = parse_args()
    settings.ensure_dirs()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    source_frame = load_source_frame(args.db_path)
    min_doc_freq = max(args.min_doc_freq, 1)

    for category_name, output_stub in CATEGORY_OUTPUTS.items():
        matrix = build_matrix(
            source_frame=source_frame,
            category_name=category_name,
            min_doc_freq=min_doc_freq,
        )
        output_path = args.output_dir / f"isbn13_{output_stub}_dtm.csv"
        matrix.to_csv(output_path, index=False)
        print(
            f"Exported {category_name} DTM to {output_path} "
            f"(rows={len(matrix)}, columns={len(matrix.columns) - 1})."
        )


if __name__ == "__main__":
    main()
