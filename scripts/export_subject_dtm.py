from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.config import settings
from src.utils.io import connect_sqlite

WHITESPACE_RE = re.compile(r"\s+")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export a binary document-term matrix keyed by isbn13 using subjects and subject_places."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=settings.db_path,
        help="Path to the SQLite database.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=settings.data_dir / "processed" / "isbn13_subject_dtm.csv",
        help="Destination CSV path.",
    )
    parser.add_argument(
        "--min-doc-freq",
        type=int,
        default=1,
        help="Keep only terms that appear in at least this many isbn13 rows.",
    )
    parser.add_argument(
        "--keep-source-tags",
        action="store_true",
        help="Keep ingestion artifacts such as nyt:* and New York Times bestseller.",
    )
    return parser.parse_args()


def normalize_term(term: str) -> str:
    return WHITESPACE_RE.sub(" ", term.strip().lower())


def should_keep_term(term: str, keep_source_tags: bool) -> bool:
    if not term:
        return False
    if keep_source_tags:
        return True
    if term.startswith("nyt:"):
        return False
    if term == "new york times bestseller":
        return False
    return True


def split_terms(raw_value: str | None) -> list[str]:
    if not raw_value:
        return []
    return [part for part in raw_value.split("|") if part and part.strip()]


def iter_term_rows(frame: pd.DataFrame, keep_source_tags: bool) -> Iterable[dict[str, str]]:
    for row in frame.itertuples(index=False):
        seen_terms: set[str] = set()

        for raw_term in split_terms(row.subjects):
            term = normalize_term(raw_term)
            if should_keep_term(term, keep_source_tags):
                seen_terms.add(f"subject:{term}")

        for raw_term in split_terms(row.subject_places):
            term = normalize_term(raw_term)
            if should_keep_term(term, keep_source_tags):
                seen_terms.add(f"subject_place:{term}")

        for term in seen_terms:
            yield {"isbn13": row.isbn13, "term": term, "value": 1}


def load_source_frame(db_path: Path) -> pd.DataFrame:
    query = """
    SELECT DISTINCT
        n.isbn13,
        COALESCE(e.subjects, '') AS subjects,
        COALESCE(e.subject_places, '') AS subject_places
    FROM nyt_entries n
    LEFT JOIN openlibrary_enrichment e
        ON e.isbn13 = n.isbn13
    WHERE n.isbn13 IS NOT NULL
      AND TRIM(n.isbn13) <> ''
    ORDER BY n.isbn13
    """
    conn = connect_sqlite(db_path)
    try:
        return pd.read_sql_query(query, conn)
    finally:
        conn.close()


def build_matrix(source_frame: pd.DataFrame, min_doc_freq: int, keep_source_tags: bool) -> pd.DataFrame:
    term_frame = pd.DataFrame.from_records(iter_term_rows(source_frame, keep_source_tags))

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

    source_frame = load_source_frame(args.db_path)
    matrix = build_matrix(
        source_frame=source_frame,
        min_doc_freq=max(args.min_doc_freq, 1),
        keep_source_tags=args.keep_source_tags,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    matrix.to_csv(args.output, index=False)

    print(
        f"Exported binary document-term matrix to {args.output} "
        f"(rows={len(matrix)}, columns={len(matrix.columns) - 1})."
    )


if __name__ == "__main__":
    main()
