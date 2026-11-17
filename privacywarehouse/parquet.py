import json
import os
import tempfile
import uuid
from datetime import date, datetime
from pathlib import Path

import duckdb


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def write_rows(root, rows):
    rows = list(rows)
    if not rows:
        return None
    directory = Path(root)
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / (uuid.uuid4().hex + ".parquet")
    temporary = destination.with_suffix(".pending")
    database = duckdb.connect(config={"threads": 1})
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", encoding="utf-8") as source:
            json.dump(rows, source, ensure_ascii=False)
            source.flush()
            database.execute("CREATE TABLE payload AS SELECT * FROM read_json_auto("
                             + literal(source.name) + ", format='array')")
            database.execute("COPY payload TO " + literal(temporary) + " (FORMAT PARQUET)")
        os.replace(temporary, destination)
    finally:
        database.close()
        if temporary.exists():
            temporary.unlink()
    return str(destination.resolve())


def query(paths, sql="SELECT * FROM payload", parameters=()):
    paths = list(paths)
    if not paths:
        return []
    if any(not Path(path).is_file() for path in paths):
        raise FileNotFoundError("数据文件不存在")
    database = duckdb.connect(config={"threads": 1})
    try:
        database.read_parquet(paths).create_view("payload")
        result = database.execute(sql, parameters)
        names = [column[0] for column in result.description]
        return [normalize(dict(zip(names, row))) for row in result.fetchall()]
    finally:
        database.close()


def normalize(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {name: normalize(item) for name, item in value.items()}
    if isinstance(value, list):
        return [normalize(item) for item in value]
    return value
