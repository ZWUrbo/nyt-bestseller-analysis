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
    isbn10 TEXT,
    description TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(list_name, published_date, title, author)
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

CREATE INDEX IF NOT EXISTS idx_nyt_isbn13 ON nyt_entries(isbn13);
CREATE INDEX IF NOT EXISTS idx_openlibrary_enrichment_work_key ON openlibrary_enrichment(work_key);
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
    isbn10: Optional[str]
    description: Optional[str]

@dataclass(frozen=True)
class OpenLibraryEnrichmentRow:
    isbn13: str
    work_key: Optional[str]
    subjects: Sequence[str]
    subject_places: Sequence[str]
    description: Optional[str]
    last_error: Optional[str]

class Repo:
    def __init__(self, conn: sqlite3.Connection) -> None:
         self.conn = conn

    def init_schema(self) -> None:
        self.conn.executescript(SCHEMA_SQL)
        self.conn.commit()
    
    def upsert_nyt_entries(self, entries: Iterable[NytEntry]) -> int:
        sql = """
        INSERT INTO nyt_entries
        (list_name, published_date, rank, weeks_on_list, title, author, publisher, isbn13, isbn10, description)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(list_name, published_date, title, author) DO UPDATE SET
            rank=excluded.rank,
            weeks_on_list=excluded.weeks_on_list,
            publisher=excluded.publisher,
            isbn13=COALESCE(excluded.isbn13, nyt_entries.isbn13),
            isbn10=COALESCE(excluded.isbn10, nyt_entries.isbn10),
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
                    e.isbn10,
                    e.description
                ),
            )
            count += 1
        self.conn.commit()
        return count
    
    def list_nyt_isbn13(self, limit: int = 500, missing_only: bool = True) -> list[str]:
        sql = """
        SELECT DISTINCT n.isbn13
        FROM nyt_entries n
        LEFT JOIN openlibrary_enrichment e ON e.isbn13 = n.isbn13
        WHERE n.isbn13 IS NOT NULL
          AND TRIM(n.isbn13) <> ''
          AND (? = 0 OR e.isbn13 IS NULL)
        ORDER BY n.isbn13
        LIMIT ?
        """
        cur = self.conn.execute(sql, (1 if missing_only else 0, limit))
        return [row[0] for row in cur.fetchall() if row[0]]

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
