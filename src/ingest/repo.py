from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Sequence
import sqlite3
from datetime import datetime, timezone

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS nyt_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    list_name TEXT NOT NULL,
    published_date TEXT NOT NULL, -- YYYY-MM-DD(NYT list date format)
    rank INTEGER,
    weeks_on_list INTEGER,
    title TEXT NOT NULL,
    author TEXT,
    publisher TEXT,
    isbn13 TEXT,
    description TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    nyt_author_id INTEGER,
    FOREIGN KEY (nyt_author_id) REFERENCES nyt_authors(id),
    UNIQUE(list_name, published_date, title, author)
);

CREATE TABLE IF NOT EXISTS nyt_authors (
    id INTEGER PRIMARY KEY,
    author_name TEXT NOT NULL UNIQUE,
    representative_book TEXT,
    hardcover_author_id INTEGER,
    last_checked_at TEXT NOT NULL,
    FOREIGN KEY (hardcover_author_id) REFERENCES hardcover_authors(author_id)
);

CREATE TABLE IF NOT EXISTS openlibrary_enrichment (
    isbn13 TEXT PRIMARY KEY,
    work_key TEXT,
    subjects TEXT, -- pipe-separated
    subject_places TEXT, -- pipe-separated
    description TEXT, -- normalized text
    last_error TEXT,
    last_checked_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hardcover_enrichment (
    isbn13 TEXT PRIMARY KEY,
    book_id INTEGER,
    author_id INTEGER,
    title TEXT,
    description TEXT,
    rating REAL,
    ratings_count INTEGER,
    users_read_count INTEGER,
    cached_tags TEXT, -- serialized JSON
    last_error TEXT,
    last_checked_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hardcover_authors (
    author_id INTEGER PRIMARY KEY,
    name TEXT,
    born_date TEXT,
    born_year INTEGER,
    death_year INTEGER,
    location TEXT,
    gender_id INTEGER,
    is_lgbtq INTEGER,
    is_bipoc INTEGER,
    last_error TEXT,
    last_checked_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gemini_content_summaries (
    isbn13 TEXT PRIMARY KEY,
    summary TEXT,
    content_tags_seed TEXT,
    raw_response TEXT,
    last_error TEXT,
    last_checked_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gemini_author_details (
    id INTEGER PRIMARY KEY,
    nyt_author_id INTEGER NOT NULL UNIQUE,
    birth_year INTEGER,
    nationality TEXT,
    ethnicity TEXT,
    gender INTEGER,
    is_bipoc INTEGER,
    is_lgbtq INTEGER,
    raw_response TEXT,
    last_error TEXT,
    last_checked_at TEXT NOT NULL,
    FOREIGN KEY (nyt_author_id) REFERENCES nyt_authors(id)
);

CREATE INDEX IF NOT EXISTS idx_nyt_isbn13 ON nyt_entries(isbn13);
CREATE INDEX IF NOT EXISTS idx_openlibrary_enrimchment_isbn13 ON openlibrary_enrichment(isbn13);
CREATE INDEX IF NOT EXISTS idx_hardcover_enrichment_isbn13 ON hardcover_enrichment(isbn13);
CREATE INDEX IF NOT EXISTS idx_hardcover_enrichment_author_id ON hardcover_enrichment(author_id);
CREATE INDEX IF NOT EXISTS idx_nyt_authors_hardcover_author_id ON nyt_authors(hardcover_author_id);
"""

@dataclass (frozen=True)
class NytEntry:
    list_name: str
    published_date: str
    rank: Optional[int]
    weeks_on_list: Optional[int]
    title: str
    author: Optional[str]
    publisher: Optional[str]
    isbn13: Optional[str]
    description: Optional[str]

@dataclass(frozen=True)
class OpenLibraryEnrichmentRow:
    isbn13: str
    work_key: Optional[str]
    subjects: Sequence[str]
    subject_places: Sequence[str]
    description: Optional[str]
    last_error: Optional[str]

@dataclass(frozen=True)
class HardcoverEnrichmentRow:
    isbn13: str
    book_id: Optional[int]
    author_id: Optional[int]
    title: Optional[str]
    description: Optional[str]
    rating: Optional[float]
    ratings_count: Optional[int]
    users_read_count: Optional[int]
    cached_tags: Optional[str]
    last_error: Optional[str]


@dataclass(frozen=True)
class HardcoverAuthorRow:
    author_id: int
    name: Optional[str]
    born_date: Optional[str]
    born_year: Optional[int]
    death_year: Optional[int]
    location: Optional[str]
    gender_id: Optional[int]
    is_lgbtq: Optional[bool]
    is_bipoc: Optional[bool]
    last_error: Optional[str]


@dataclass(frozen=True)
class GeminiSummaryInputRow:
    isbn13: str
    title: Optional[str]
    author: Optional[str]


@dataclass(frozen=True)
class GeminiContentSummaryRow:
    isbn13: str
    summary: Optional[str]
    content_tags_seed: Sequence[str]
    raw_response: Optional[str]
    last_error: Optional[str]


@dataclass(frozen=True)
class GeminiAuthorDetailsInputRow:
    nyt_author_id: int
    author: str
    book: str


@dataclass(frozen=True)
class GeminiAuthorDetailsRow:
    nyt_author_id: int
    birth_year: Optional[int]
    nationality: Optional[str]
    ethnicity: Optional[str]
    gender: Optional[int]
    is_bipoc: Optional[bool]
    is_lgbtq: Optional[bool]
    raw_response: Optional[str]
    last_error: Optional[str]

class Repo:
    def __init__(self, conn: sqlite3.Connection) -> None:
         self.conn = conn

    def init_schema(self) -> None:
        self.conn.executescript(SCHEMA_SQL)
        self._ensure_column("hardcover_enrichment", "author_id", "INTEGER")
        self._ensure_column("nyt_entries", "nyt_author_id", "INTEGER")
        self.refresh_nyt_authors()
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_nyt_entries_author_id ON nyt_entries(nyt_author_id)")
        self.conn.commit()
    
    def upsert_nyt_entries(self, entries: Iterable[NytEntry]) -> int:
        sql = """
        INSERT INTO nyt_entries
        (list_name, published_date, rank, weeks_on_list, title, author, publisher, isbn13, description)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(list_name, published_date, title, author) DO UPDATE SET
            rank=excluded.rank,
            weeks_on_list=excluded.weeks_on_list,
            publisher=excluded.publisher,
            isbn13=COALESCE(excluded.isbn13, nyt_entries.isbn13),
            description=COALESCE(excluded.description, nyt_entries.description)       
        """
        cur = self.conn.cursor()
        count = 0
        for e in entries:
            cur.execute(
                sql,
                (
                    e.list_name,
                    e.published_date,
                    e.rank,
                    e.weeks_on_list,
                    e.title,
                    e.author,
                    e.publisher,
                    e.isbn13,
                    e.description
                ),
            )
            count += 1
        self.conn.commit()
        return count
    
    def list_nyt_isbn13(
        self,
        limit: int = 500,
        missing_only: bool = True,
        enrichment_table: str = "openlibrary_enrichment",
    ) -> list[str]:
        if enrichment_table not in {
            "openlibrary_enrichment",
            "hardcover_enrichment",
            "gemini_content_summaries",
        }:
            raise ValueError(f"Unsupported enrichment table: {enrichment_table}")

        sql = """
        SELECT DISTINCT n.isbn13
        FROM nyt_entries n
        LEFT JOIN {enrichment_table} e ON e.isbn13 = n.isbn13
        WHERE n.isbn13 IS NOT NULL
          AND TRIM(n.isbn13) <> ''
          AND (? = 0 OR e.isbn13 IS NULL)
        ORDER BY n.isbn13
        LIMIT ?
        """
        sql = sql.format(enrichment_table=enrichment_table)
        cur = self.conn.execute(sql, (1 if missing_only else 0, limit))
        return [row[0] for row in cur.fetchall() if row[0]]

    def list_gemini_summary_inputs(
        self,
        limit: int = 1000,
        missing_only: bool = True,
    ) -> list[GeminiSummaryInputRow]:
        return self._select_gemini_summary_inputs(
            limit=limit,
            missing_only=missing_only,
        )

    def _select_gemini_summary_inputs(
        self,
        limit: Optional[int] = None,
        missing_only: bool = False,
        extra_where: str = "",
        params: tuple[object, ...] = (),
    ) -> list[GeminiSummaryInputRow]:
        sql = """
        SELECT
            n.isbn13,
            NULLIF(MAX(n.title), '') AS title,
            NULLIF(MAX(n.author), '') AS author
        FROM nyt_entries n
        LEFT JOIN gemini_content_summaries g
            ON g.isbn13 = n.isbn13
        WHERE n.isbn13 IS NOT NULL
          AND TRIM(n.isbn13) <> ''
          AND (? = 0 OR g.isbn13 IS NULL)
          {extra_where}
        GROUP BY n.isbn13
        ORDER BY n.isbn13
        """
        sql = sql.format(extra_where=extra_where)
        query_params: list[object] = [1 if missing_only else 0, *params]
        if limit is not None:
            sql += "\nLIMIT ?"
            query_params.append(limit)

        cur = self.conn.execute(sql, tuple(query_params))
        rows: list[GeminiSummaryInputRow] = []
        for row in cur.fetchall():
            rows.append(
                GeminiSummaryInputRow(
                    isbn13=row[0],
                    title=row[1],
                    author=row[2],
                )
            )
        return rows

    def list_gemini_author_details_inputs(
        self,
        limit: Optional[int] = 1000,
        missing_only: bool = True,
    ) -> list[GeminiAuthorDetailsInputRow]:
        sql = """
        SELECT
            a.id,
            a.author_name,
            a.representative_book
        FROM nyt_authors a
        LEFT JOIN gemini_author_details g
            ON g.nyt_author_id = a.id
        WHERE (? = 0 OR g.nyt_author_id IS NULL OR g.last_error IS NOT NULL)
        ORDER BY a.author_name
        """
        query_params: list[object] = [1 if missing_only else 0]
        if limit is not None:
            sql += "\nLIMIT ?"
            query_params.append(limit)

        rows: list[GeminiAuthorDetailsInputRow] = []
        for row in self.conn.execute(sql, tuple(query_params)).fetchall():
            rows.append(
                GeminiAuthorDetailsInputRow(
                    nyt_author_id=row[0],
                    author=row[1],
                    book=row[2],
                )
            )
        return rows

    def list_hardcover_author_ids(
        self,
        limit: int = 500,
        missing_only: bool = True,
    ) -> list[int]:
        sql = """
        SELECT DISTINCT h.author_id
        FROM hardcover_enrichment h
        LEFT JOIN hardcover_authors a ON a.author_id = h.author_id
        WHERE h.author_id IS NOT NULL
          AND (? = 0 OR a.author_id IS NULL)
        ORDER BY h.author_id
        LIMIT ?
        """
        cur = self.conn.execute(sql, (1 if missing_only else 0, limit))
        return [row[0] for row in cur.fetchall() if row[0] is not None]

    def upsert_openlibrary_enrichment(self, rows: Iterable[OpenLibraryEnrichmentRow]) -> int:
        sql = """
        INSERT INTO openlibrary_enrichment
        (isbn13, work_key, subjects, subject_places, description, last_error, last_checked_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(isbn13) DO UPDATE SET
            work_key=COALESCE(excluded.work_key, openlibrary_enrichment.work_key),
            subjects=COALESCE(excluded.subjects, openlibrary_enrichment.subjects),
            subject_places=COALESCE(excluded.subject_places, openlibrary_enrichment.subject_places),
            description=COALESCE(excluded.description, openlibrary_enrichment.description),
            last_error=excluded.last_error,
            last_checked_at=excluded.last_checked_at
        """
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        cur = self.conn.cursor()
        count = 0
        for r in rows:
            cur.execute(
                sql,
                (
                    r.isbn13,
                    r.work_key,
                    "|".join([x for x in r.subjects if x]),
                    "|".join([x for x in r.subject_places if x]),
                    r.description,
                    r.last_error,
                    now,
                ),
            )
            count += 1
        self.conn.commit()
        return count

    def upsert_hardcover_enrichment(self, rows: Iterable[HardcoverEnrichmentRow]) -> int:
        sql = """
        INSERT INTO hardcover_enrichment
        (isbn13, book_id, author_id, title, description, rating, ratings_count, users_read_count, cached_tags, last_error, last_checked_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(isbn13) DO UPDATE SET
            book_id=COALESCE(excluded.book_id, hardcover_enrichment.book_id),
            author_id=COALESCE(excluded.author_id, hardcover_enrichment.author_id),
            title=COALESCE(excluded.title, hardcover_enrichment.title),
            description=COALESCE(excluded.description, hardcover_enrichment.description),
            rating=COALESCE(excluded.rating, hardcover_enrichment.rating),
            ratings_count=COALESCE(excluded.ratings_count, hardcover_enrichment.ratings_count),
            users_read_count=COALESCE(excluded.users_read_count, hardcover_enrichment.users_read_count),
            cached_tags=COALESCE(excluded.cached_tags, hardcover_enrichment.cached_tags),
            last_error=excluded.last_error,
            last_checked_at=excluded.last_checked_at
        """
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        cur = self.conn.cursor()
        count = 0
        for r in rows:
            cur.execute(
                sql,
                (
                    r.isbn13,
                    r.book_id,
                    r.author_id,
                    r.title,
                    r.description,
                    r.rating,
                    r.ratings_count,
                    r.users_read_count,
                    r.cached_tags,
                    r.last_error,
                    now,
                ),
            )
            count += 1
        self.conn.commit()
        return count

    def upsert_hardcover_authors(self, rows: Iterable[HardcoverAuthorRow]) -> int:
        sql = """
        INSERT INTO hardcover_authors
        (author_id, name, born_date, born_year, death_year, location, gender_id, is_lgbtq, is_bipoc, last_error, last_checked_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(author_id) DO UPDATE SET
            name=COALESCE(excluded.name, hardcover_authors.name),
            born_date=COALESCE(excluded.born_date, hardcover_authors.born_date),
            born_year=COALESCE(excluded.born_year, hardcover_authors.born_year),
            death_year=COALESCE(excluded.death_year, hardcover_authors.death_year),
            location=COALESCE(excluded.location, hardcover_authors.location),
            gender_id=COALESCE(excluded.gender_id, hardcover_authors.gender_id),
            is_lgbtq=COALESCE(excluded.is_lgbtq, hardcover_authors.is_lgbtq),
            is_bipoc=COALESCE(excluded.is_bipoc, hardcover_authors.is_bipoc),
            last_error=excluded.last_error,
            last_checked_at=excluded.last_checked_at
        """
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        cur = self.conn.cursor()
        count = 0
        for r in rows:
            cur.execute(
                sql,
                (
                    r.author_id,
                    r.name,
                    r.born_date,
                    r.born_year,
                    r.death_year,
                    r.location,
                    r.gender_id,
                    _bool_to_int(r.is_lgbtq),
                    _bool_to_int(r.is_bipoc),
                    r.last_error,
                    now,
                ),
            )
            count += 1
        self.conn.commit()
        return count

    def upsert_gemini_content_summaries(
        self,
        rows: Iterable[GeminiContentSummaryRow],
    ) -> int:
        sql = """
        INSERT INTO gemini_content_summaries
        (isbn13, summary, content_tags_seed, raw_response, last_error, last_checked_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(isbn13) DO UPDATE SET
            summary=COALESCE(excluded.summary, gemini_content_summaries.summary),
            content_tags_seed=COALESCE(excluded.content_tags_seed, gemini_content_summaries.content_tags_seed),
            raw_response=COALESCE(excluded.raw_response, gemini_content_summaries.raw_response),
            last_error=excluded.last_error,
            last_checked_at=excluded.last_checked_at
        """
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        cur = self.conn.cursor()
        count = 0
        for r in rows:
            cur.execute(
                sql,
                (
                    r.isbn13,
                    r.summary,
                    "; ".join([x for x in r.content_tags_seed if x]),
                    r.raw_response,
                    r.last_error,
                    now,
                ),
            )
            count += 1
        self.conn.commit()
        return count

    def upsert_gemini_author_details(
        self,
        rows: Iterable[GeminiAuthorDetailsRow],
    ) -> int:
        sql = """
        INSERT INTO gemini_author_details
        (nyt_author_id, birth_year, nationality, ethnicity, gender, is_bipoc, is_lgbtq, raw_response, last_error, last_checked_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(nyt_author_id) DO UPDATE SET
            birth_year=COALESCE(excluded.birth_year, gemini_author_details.birth_year),
            nationality=COALESCE(excluded.nationality, gemini_author_details.nationality),
            ethnicity=COALESCE(excluded.ethnicity, gemini_author_details.ethnicity),
            gender=COALESCE(excluded.gender, gemini_author_details.gender),
            is_bipoc=COALESCE(excluded.is_bipoc, gemini_author_details.is_bipoc),
            is_lgbtq=COALESCE(excluded.is_lgbtq, gemini_author_details.is_lgbtq),
            raw_response=COALESCE(excluded.raw_response, gemini_author_details.raw_response),
            last_error=excluded.last_error,
            last_checked_at=excluded.last_checked_at
        """
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        cur = self.conn.cursor()
        count = 0
        for r in rows:
            cur.execute(
                sql,
                (
                    r.nyt_author_id,
                    r.birth_year,
                    r.nationality,
                    r.ethnicity,
                    r.gender,
                    _bool_to_int(r.is_bipoc),
                    _bool_to_int(r.is_lgbtq),
                    r.raw_response,
                    r.last_error,
                    now,
                ),
            )
            count += 1
        self.conn.commit()
        return count

    def refresh_nyt_authors(self) -> None:
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self.conn.execute(
            """
            INSERT INTO nyt_authors
            (author_name, representative_book, hardcover_author_id, last_checked_at)
            WITH ranked_books AS (
                SELECT
                    TRIM(n.author) AS author_name,
                    NULLIF(TRIM(n.title), '') AS representative_book,
                    ROW_NUMBER() OVER (
                        PARTITION BY TRIM(n.author)
                        ORDER BY
                            COALESCE(n.weeks_on_list, 0) DESC,
                            n.published_date DESC,
                            COALESCE(n.rank, 9999) ASC,
                            n.title ASC
                    ) AS row_num
                FROM nyt_entries n
                WHERE n.author IS NOT NULL
                  AND TRIM(n.author) <> ''
                  AND n.title IS NOT NULL
                  AND TRIM(n.title) <> ''
            ),
            hardcover_author_ids AS (
                SELECT
                    TRIM(n.author) AS author_name,
                    CASE
                        WHEN COUNT(DISTINCT h.author_id) = 1 THEN MIN(h.author_id)
                        ELSE NULL
                    END AS hardcover_author_id
                FROM nyt_entries n
                LEFT JOIN hardcover_enrichment h
                    ON h.isbn13 = n.isbn13
                   AND h.author_id IS NOT NULL
                WHERE n.author IS NOT NULL
                  AND TRIM(n.author) <> ''
                GROUP BY TRIM(n.author)
            )
            SELECT
                b.author_name,
                b.representative_book,
                h.hardcover_author_id,
                ?
            FROM ranked_books b
            LEFT JOIN hardcover_author_ids h
                ON h.author_name = b.author_name
            WHERE b.row_num = 1
            ON CONFLICT(author_name) DO UPDATE SET
                representative_book=COALESCE(excluded.representative_book, nyt_authors.representative_book),
                hardcover_author_id=excluded.hardcover_author_id,
                last_checked_at=excluded.last_checked_at
            """,
            (now,),
        )
        self.conn.execute(
            """
            UPDATE nyt_entries
            SET nyt_author_id = (
                SELECT a.id
                FROM nyt_authors a
                WHERE a.author_name = TRIM(nyt_entries.author)
            )
            WHERE author IS NOT NULL
              AND TRIM(author) <> ''
            """
        )

    def _ensure_column(self, table_name: str, column_name: str, column_type: str) -> None:
        existing_columns = self._table_columns(table_name)
        if column_name not in existing_columns:
            self.conn.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
            )

    def _table_columns(self, table_name: str) -> set[str]:
        return {
            row[1]
            for row in self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }


def _bool_to_int(value: Optional[bool]) -> Optional[int]:
    if value is None:
        return None
    return int(value)
