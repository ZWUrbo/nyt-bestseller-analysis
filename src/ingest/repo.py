from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Sequence, Dict, Any
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

CREATE TABLE IF NOT EXISTS books (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT UNIQUE, -- Open Library work key like /works/OLxxxxW
    title TEXT,
    author_names TEXT, -- pipe-separated
    first_publish_year INTEGER,
    language_codes TEXT, -- pipe-separated
    isbn13 TEXT, -- pipe-separated
    isbn10 TEXT, -- pipe-separated
    subjects TEXT, -- pipe-separated
    description TEXT, -- normalized text
    last_enriched_at TEXT
);

CREATE TABLE IF NOT EXISTS awards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    award_name TEXT NOT NULL,
    year INTEGER NOT NULL,
    category TEXT,
    outcome TEXT NOT NULL, -- nominee or winner
    title TEXT NOT NULL,
    author TEXT,
    source TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(award_name, year, category, title, author)
);

CREATE INDEX IF NOT EXISTS idx_nyt_isbn13 ON nyt_entries(isbn13);
CREATE INDEX IF NOT EXISTS idx_books_isbn13 ON books(isbn13);
CREATE INDEX IF NOT EXISTS idx_awards_title ON awards(title);
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
class OpenLibraryBook:
    key: str
    title: Optional[str]
    author_names: Sequence[str]
    first_publish_year: Optional[int]
    language_codes: Sequence[str]
    isbn13: Sequence[str]
    isbn10: Sequence[str]
    subjects: Sequence[str]
    description: Optional[str]

@dataclass(frozen=True)
class AwardRow:
    award_name: str
    year: int
    category: Optional[str]
    outcome: str # nominee/winner
    title: str
    author: Optional[str]
    source: Optional[str]

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
            isbn13=COALESCE(exluded.isbn13, nyt_entries.isbn13),
            isbn10=COALESCE(exluded.isbn10, nyt_entries.isbn10
            description=COALESCE(exluded.description, nyt_entries.description)       
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
    
    def upsert_books(self, books: Iterable[OpenLibraryBook]) -> int:
        sql = """
        INSERT INTO books
        (key, title, author_names, first_publish_year, language_codes, isbn13, isbn10, subjects, description, last_enriched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            title=COALESCE(excluded.title, books.title),
            author_names=COALESCE(excluded.author_names, books.author_names),
            first_publish_year=COALESCE(excluded.first_publish_year, books.first_publish_year),
            language_codes=COALESCE(excluded.language_codes, books.language_codes),
            isbn13=COALESCE(excluded.isbn13, books.isbn13),
            isbn10=COALESCE(excluded.isbn10, books.isbn10),
            subjects=COALESCE(excluded.subjects, books.subjects),
            description=COALESCE(excluded.description, books.description),
            last_enriched_at=excluded.last_enriched_at
        """
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        cur = self.conn.cursor()
        count = 0
        for b in books:
            cur.execute(
                sql,
                (
                    b.key,
                    b.title,
                    "|".join([x for x in b.author_names if x]),
                    b.first_publish_year,
                    "|".join([x for x in b.language_codes if x]),
                    "|".join([x for x in b.isbn13 if x]),
                    "|".join([x for x in b.isbn10 if x]),
                    "|".join([x for x in b.subjects if x]),
                    b.description,
                    now
                ),
            )
            count += 1
        self.conn.commit()
        return count
    
    def upsert_awards(self, rows: Iterable[AwardRow]) -> int:
        sql = """
        INSERT INTO awards (award_name, year, category, outcome, title, author, source)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(award_name, year, category, title, author) DO UPDATE SET
            source=COALESCE(excluded.source, awards.source);
        """
        cur = self.conn.cursor()
        count = 0   
        for r in rows:
            cur.execute(
                sql,
                (
                    r.award_name,
                    r.year,
                    r.category,
                    r.outcome,
                    r.title,
                    r.author,
                    r.source
                ),
            )
            count += 1
        self.conn.commit()
        return count