from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import settings
from src.utils.io import connect_sqlite
from src.utils.logging import get_logger

logger = get_logger("create_gemini_author_details_batch")

PROMPT_TEMPLATE = """You are extracting structured data about an author. Use only verifiable, widely known facts. Do not infer or guess.

Input:
Author: {author}
Book: {book}

Rules:
- Only return a value if there is clear, high-confidence evidence.
- If information is unknown, disputed, private, or not publicly confirmed, return null.
- Do NOT infer sensitive attributes (BIPOC, LGBTQ) without explicit public confirmation.
- Age must be based on reliable sources (current age or age at death if deceased).
- Nationality must be a recognized country name.
- Gender must be:
  1 = Male
  2 = Female
  3 = Non-Binary
- Boolean fields must be true, false, or null (not strings).

Output:
Return ONLY a valid JSON object with this exact schema:

{
  "age": number | null,
  "nationality": string | null,
  "gender": 1 | 2 | 3 | null,
  "bipoc": boolean | null,
  "lgbtq": boolean | null
}"""

RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["age", "nationality", "gender", "bipoc", "lgbtq"],
    "additionalProperties": False,
    "properties": {
        "age": {
            "type": ["number", "null"],
            "description": "Current age, or age at death if deceased.",
        },
        "nationality": {
            "type": ["string", "null"],
            "description": "A recognized country name.",
        },
        "gender": {
            "type": ["integer", "null"],
            "enum": [1, 2, 3, None],
            "description": "1 = Male, 2 = Female, 3 = Non-Binary.",
        },
        "bipoc": {
            "type": ["boolean", "null"],
        },
        "lgbtq": {
            "type": ["boolean", "null"],
        },
    },
}


@dataclass(frozen=True)
class GeminiAuthorDetailsInputRow:
    author: str
    book: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of distinct NYT author values to include.",
    )
    p.add_argument(
        "--jsonl-path",
        type=Path,
        default=None,
        help="Optional explicit path for the generated JSONL batch input file.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    settings.ensure_dirs()

    conn = connect_sqlite(settings.db_path)
    try:
        rows = list_author_detail_inputs(conn, limit=args.limit)
    finally:
        conn.close()

    if not rows:
        logger.info("No NYT authors to process.")
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    gemini_dir = settings.data_dir / "interim" / "gemini"
    jsonl_path = (
        args.jsonl_path
        or (gemini_dir / f"gemini_author_details_batch_requests_{timestamp}.jsonl")
    ).resolve()

    write_jsonl_input(jsonl_path, rows)
    logger.info("Wrote Gemini author details batch input JSONL: %s (requests=%s)", jsonl_path, len(rows))


def list_author_detail_inputs(conn: Any, limit: int | None = None) -> list[GeminiAuthorDetailsInputRow]:
    sql = """
    WITH ranked AS (
        SELECT
            TRIM(author) AS author_name,
            NULLIF(TRIM(title), '') AS title,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(author)
                ORDER BY
                    COALESCE(weeks_on_list, 0) DESC,
                    published_date DESC,
                    COALESCE(rank, 9999) ASC,
                    title ASC
            ) AS row_num
        FROM nyt_entries
        WHERE author IS NOT NULL
          AND TRIM(author) <> ''
          AND title IS NOT NULL
          AND TRIM(title) <> ''
    )
    SELECT author_name, title
    FROM ranked
    WHERE row_num = 1
    ORDER BY author_name
    """
    params: list[object] = []
    if limit is not None:
        sql += "\nLIMIT ?"
        params.append(limit)

    rows: list[GeminiAuthorDetailsInputRow] = []
    for author, title in conn.execute(sql, tuple(params)).fetchall():
        rows.append(GeminiAuthorDetailsInputRow(author=author, book=title))
    return rows


def write_jsonl_input(jsonl_path: Path, rows: list[GeminiAuthorDetailsInputRow]) -> None:
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with jsonl_path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(build_batch_request_line(row), ensure_ascii=False) + "\n")


def build_batch_request_line(row: GeminiAuthorDetailsInputRow) -> dict[str, Any]:
    prompt = PROMPT_TEMPLATE.replace("{author}", row.author).replace("{book}", row.book)
    return {
        "key": row.author,
        "request": {
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {"text": prompt},
                    ],
                }
            ],
            "generationConfig": {
                "temperature": 0.0,
                "candidateCount": 1,
                "responseMimeType": "application/json",
                "responseJsonSchema": RESPONSE_SCHEMA,
            },
        },
    }


if __name__ == "__main__":
    main()
