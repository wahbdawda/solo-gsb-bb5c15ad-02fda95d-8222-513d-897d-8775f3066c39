"""SQLite connection management and schema initialisation.

A single shared connection serialised by a re-entrant lock is used. SQLite is
single-writer anyway and the lock makes the "read current state -> stage packet
-> commit run" transaction logic deterministic under concurrent requests.
"""
from __future__ import annotations

import os
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager

from app.config import settings

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS devices (
    id              TEXT PRIMARY KEY,
    start_seq       INTEGER NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    last_seq        INTEGER,
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS packets (
    id                  INTEGER PRIMARY KEY,
    device_id           TEXT NOT NULL REFERENCES devices(id),
    seq                 INTEGER NOT NULL,
    idempotency_key     TEXT NOT NULL UNIQUE,
    payload             TEXT NOT NULL,
    received_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    committed_at        TEXT
);

-- Exactly one row per (device, seq): staging space and committed history share
-- the same table, committed rows are simply those with committed_at NOT NULL.
CREATE UNIQUE INDEX IF NOT EXISTS ux_packets_device_seq
    ON packets(device_id, seq);

CREATE INDEX IF NOT EXISTS ix_packets_device_committed
    ON packets(device_id, committed_at);

CREATE TABLE IF NOT EXISTS duplicates (
    id                  INTEGER PRIMARY KEY,
    device_id           TEXT NOT NULL,
    seq                 INTEGER NOT NULL,
    idempotency_key     TEXT NOT NULL,
    payload             TEXT NOT NULL,
    reason              TEXT NOT NULL,
    original_packet_id  INTEGER,
    received_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS ix_duplicates_device
    ON duplicates(device_id, received_at);
"""

_lock = threading.RLock()
_conn: sqlite3.Connection | None = None


def _connect(db_path: str) -> sqlite3.Connection:
    directory = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def init_db(db_path: str | None = None) -> None:
    """Open the connection (creating the file if needed) and create the schema."""
    global _conn
    with _lock:
        if _conn is None:
            _conn = _connect(db_path or settings.db_path)
        _conn.executescript(SCHEMA)


def get_conn() -> sqlite3.Connection:
    if _conn is None:  # pragma: no cover - init_db runs on application startup
        init_db()
    assert _conn is not None
    return _conn


@contextmanager
def transaction() -> Iterator[sqlite3.Connection]:
    """Serializable read/write transaction guarded by the global lock."""
    conn = get_conn()
    with _lock:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


@contextmanager
def read_tx() -> Iterator[sqlite3.Connection]:
    conn = get_conn()
    with _lock:
        conn.execute("BEGIN")
        try:
            yield conn
        finally:
            conn.execute("ROLLBACK")


def reset_for_tests(db_path: str) -> None:
    """Close the current connection and reopen against a fresh database."""
    global _conn
    with _lock:
        if _conn is not None:
            _conn.close()
            _conn = None
        if db_path != ":memory:":
            for suffix in ("", "-wal", "-shm"):
                path = db_path + suffix
                if os.path.exists(path):
                    os.remove(path)
        init_db(db_path)
