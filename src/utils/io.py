from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional
import sqlite3

def read_json(path: Path) -> DICT[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))

def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def connect_sqlite(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
                          (table,)
    )
    return cur.fetchone() is not None

def get_one(conn: sqlite3.Connection, query: str, params: tuple = ()) -> Optional[tuple]:
    cur = conn.execute(query, params)
    return cur.fetchone()