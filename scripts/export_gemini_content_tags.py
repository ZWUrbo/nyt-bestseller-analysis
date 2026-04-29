from __future__ import annotations

import argparse
import csv
import re
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_DB_PATH = DATA_DIR / "interim" / "books.db"

WHITESPACE_RE = re.compile(r"\s+")
NULL_TEXT = "None"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export Gemini seed content tags to a Tableau-friendly "
            "isbn13/content tag CSV."
        )
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="Path to the SQLite database.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DATA_DIR / "processed" / "features" / "isbn13_content_tags_seed_tableau.csv",
        help="Destination CSV path for the Tableau-friendly content tag export.",
    )
    return parser.parse_args()


def normalize_text(value: object) -> object:
    if value is None:
        return NULL_TEXT
    if not isinstance(value, str):
        return value
    return WHITESPACE_RE.sub(" ", value.replace("\r", " ").replace("\n", " ")).strip()


def normalize_tag_seed(value: object) -> str | None:
    cleaned = normalize_text(value)
    if not isinstance(cleaned, str) or not cleaned:
        return None
    return cleaned.lower().title()


def connect_sqlite(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def load_gemini_rows(db_path: Path) -> list[dict[str, object]]:
    query = """
    SELECT
        isbn13,
        content_tags_seed
    FROM gemini_content_summaries
    ORDER BY isbn13
    """
    conn = connect_sqlite(db_path)
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def split_content_tags(raw_value: object) -> list[str]:
    if not isinstance(raw_value, str) or not raw_value.strip():
        return []

    seen_tags: set[str] = set()
    ordered_tags: list[str] = []
    for part in raw_value.split(";"):
        cleaned = normalize_tag_seed(part)
        if not cleaned:
            continue
        if cleaned in seen_tags:
            continue
        seen_tags.add(cleaned)
        ordered_tags.append(cleaned)
    return ordered_tags


def build_tableau_rows(rows: list[dict[str, object]]) -> list[dict[str, str | int]]:
    seen_pairs: set[tuple[str, str]] = set()
    tableau_rows: list[dict[str, str | int]] = []
    for row in rows:
        isbn13 = normalize_text(row.get("isbn13"))
        if not isinstance(isbn13, str) or not isbn13:
            continue
        for tag in split_content_tags(row.get("content_tags_seed")):
            pair = (isbn13, tag)
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            tableau_rows.append(
                {
                    "isbn13": isbn13,
                    "content_tag_seed": tag,
                    "value": 1,
                }
            )

    return sorted(tableau_rows, key=lambda row: (str(row["isbn13"]), str(row["content_tag_seed"])))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as outfile:
        writer = csv.DictWriter(
            outfile,
            fieldnames=fieldnames,
            quoting=csv.QUOTE_ALL,
            lineterminator="\n",
            restval=NULL_TEXT,
        )
        writer.writeheader()
        writer.writerows(
            {
                fieldname: normalize_text(row.get(fieldname))
                for fieldname in fieldnames
            }
            for row in rows
        )


def output_fieldnames() -> list[str]:
    return ["isbn13", "content_tag_seed", "value"]


def main() -> None:
    args = parse_args()
    (DATA_DIR / "raw").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "interim").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "processed").mkdir(parents=True, exist_ok=True)

    source_rows = load_gemini_rows(args.db_path)
    output_rows = build_tableau_rows(source_rows)

    write_csv(args.output, output_fieldnames(), output_rows)

    print(
        f"Exported Gemini content tags Tableau source to {args.output} "
        f"(rows={len(output_rows)}, columns={len(output_fieldnames())})."
    )


if __name__ == "__main__":
    main()
