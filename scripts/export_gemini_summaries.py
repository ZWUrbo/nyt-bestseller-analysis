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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export gemini_content_summaries to a flat CSV and a Tableau-friendly "
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
        default=DATA_DIR / "processed" / "exports" / "gemini_content_summaries.csv",
        help="Destination CSV path for the Gemini table export.",
    )
    parser.add_argument(
        "--tableau-output",
        type=Path,
        default=DATA_DIR / "processed" / "features" / "isbn13_content_tags_seed_tableau.csv",
        help="Destination CSV path for the Tableau-friendly content tag export.",
    )
    return parser.parse_args()


def normalize_text(value: object) -> object:
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
        summary,
        content_tags_seed,
        raw_response,
        last_error,
        last_checked_at
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


def make_export_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    export_rows: list[dict[str, object]] = []
    for row in rows:
        normalized_row = {
            column_name: normalize_text(value)
            for column_name, value in row.items()
        }
        normalized_row["content_tags_seed"] = "; ".join(
            split_content_tags(row.get("content_tags_seed"))
        )
        export_rows.append(normalized_row)
    return export_rows


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
    with path.open("w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def export_fieldnames(rows: list[dict[str, object]]) -> list[str]:
    if rows:
        return list(rows[0].keys())
    return [
        "isbn13",
        "summary",
        "content_tags_seed",
        "raw_response",
        "last_error",
        "last_checked_at",
    ]


def tableau_fieldnames() -> list[str]:
    return ["isbn13", "content_tag_seed", "value"]


def main() -> None:
    args = parse_args()
    (DATA_DIR / "raw").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "interim").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "processed").mkdir(parents=True, exist_ok=True)

    source_rows = load_gemini_rows(args.db_path)
    export_rows = make_export_rows(source_rows)
    tableau_rows = build_tableau_rows(source_rows)

    write_csv(args.output, export_fieldnames(export_rows), export_rows)
    write_csv(args.tableau_output, tableau_fieldnames(), tableau_rows)

    print(
        f"Exported Gemini summaries to {args.output} "
        f"(rows={len(export_rows)}, columns={len(export_fieldnames(export_rows))})."
    )
    print(
        f"Exported Gemini content tags Tableau source to {args.tableau_output} "
        f"(rows={len(tableau_rows)}, columns={len(tableau_fieldnames())})."
    )


if __name__ == "__main__":
    main()
