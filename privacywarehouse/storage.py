import hashlib
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":")).encode()).hexdigest()


def connect(root, schema):
    directory = Path(root)
    directory.mkdir(parents=True, exist_ok=True)
    database = sqlite3.connect(directory / "catalog.sqlite3", timeout=10)
    database.row_factory = sqlite3.Row
    database.execute("PRAGMA foreign_keys=ON")
    database.executescript(schema)
    return database


@contextmanager
def atomic(database):
    database.execute("BEGIN IMMEDIATE")
    try:
        yield database
        database.commit()
    except BaseException:
        database.rollback()
        raise


def records(database, sql, parameters=()):
    return [dict(row) for row in database.execute(sql, parameters)]


def require_text(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name}需要非空字符串")
    return value
