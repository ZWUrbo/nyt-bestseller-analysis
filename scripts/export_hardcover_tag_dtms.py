from __future__ import annotations

import argparse
import json
import re
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
WHITESPACE_RE = re.compile(r"\s+")
UUID_SUFFIX_RE = re.compile(r"[-_](?:[0-9a-f]{8,}|[0-9]{6,})(?:-[0-9a-f]{4,})*$")
LEADING_GENRE_PREFIX_RE = re.compile(r"^genre\s+")
NON_ALNUM_RE = re.compile(r"[^a-z0-9'&\- ]+")
ONLY_CODE_RE = re.compile(r"^(?:\d{4}|\d{10,}|[0-9a-f]{8,})$")
GENERIC_TERMS_BY_CATEGORY = {
    "Genre": {
        "ability",
        "adult",
        "american",
        "audiobook",
        "audiobooks on cd",
        "books and reading",
        "fiction",
        "general",
        "nonfiction",
        "women",
    },
    "Mood": {
        "contemp",
    },
    "Tag": {
        "amazing",
        "audio",
        "audio atlanta",
        "audio austin",
        "audible",
        "audiobook",
        "audiobookshelf",
        "bogklub",
        "bookclub books",
        "calibre migration",
        "didn t finish",
        "ebook",
        "from audible",
        "from nas",
        "general",
        "history",
        "kindle unlimited",
        "libby",
        "library",
    },
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


def normalize_hardcover_term(raw_term: str, category_name: str) -> str | None:
    term = raw_term.strip().lower()
    if not term:
        return None

    term = UUID_SUFFIX_RE.sub("", term)
    term = term.replace("_", " ").replace("/", " ").replace("-", " ")
    term = LEADING_GENRE_PREFIX_RE.sub("", term)
    term = NON_ALNUM_RE.sub(" ", term)
    term = WHITESPACE_RE.sub(" ", term).strip(" '")
    term = LEADING_GENRE_PREFIX_RE.sub("", term)

    if not term:
        return None
    if ONLY_CODE_RE.fullmatch(term):
        return None
    if any(char.isdigit() for char in term):
        return None

    words = term.split()
    if len(words) > 4:
        return None
    if words and words[-1] in {"and", "for", "in", "of", "the", "to"}:
        return None
    if term in GENERIC_TERMS_BY_CATEGORY.get(category_name, set()):
        return None

    return term


def iter_term_rows(source_frame: pd.DataFrame, category_name: str) -> Iterable[dict[str, str | int]]:
    for row in source_frame.itertuples(index=False):
        parsed_tags = parse_cached_tags(row.cached_tags)
        category_items = parsed_tags.get(category_name) or []
        seen_terms: set[str] = set()

        for item in category_items:
            if not isinstance(item, dict):
                continue
            raw_term = item.get("tag")
            if not isinstance(raw_term, str) or not raw_term.strip():
                raw_term = item.get("tagSlug")
            if isinstance(raw_term, str):
                cleaned = normalize_hardcover_term(raw_term, category_name)
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


def build_tableau_frame(source_frame: pd.DataFrame, category_name: str, min_doc_freq: int) -> pd.DataFrame:
    term_frame = pd.DataFrame.from_records(iter_term_rows(source_frame, category_name))

    if term_frame.empty:
        return pd.DataFrame(columns=["isbn13", "term", "value"])

    if min_doc_freq > 1:
        doc_freq = term_frame.groupby("term")["isbn13"].nunique()
        allowed_terms = doc_freq[doc_freq >= min_doc_freq].index
        term_frame = term_frame[term_frame["term"].isin(allowed_terms)]

    if term_frame.empty:
        return pd.DataFrame(columns=["isbn13", "term", "value"])

    return (
        term_frame.drop_duplicates(subset=["isbn13", "term"])
        .rename(columns={"term": output_term_column_name(category_name)})
        .sort_values(["isbn13", output_term_column_name(category_name)])
        .reset_index(drop=True)
    )


def output_term_column_name(category_name: str) -> str:
    return category_name.strip().lower().replace(" ", "_")


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

        tableau_frame = build_tableau_frame(
            source_frame=source_frame,
            category_name=category_name,
            min_doc_freq=min_doc_freq,
        )
        tableau_output_path = args.output_dir / f"isbn13_{output_stub}_tableau.csv"
        tableau_frame.to_csv(tableau_output_path, index=False)
        print(
            f"Exported {category_name} Tableau source to {tableau_output_path} "
            f"(rows={len(tableau_frame)}, columns={len(tableau_frame.columns)})."
        )


if __name__ == "__main__":
    main()
