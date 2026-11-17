import json
from pathlib import Path

from .parquet import query, write_rows
from .storage import atomic, connect, records

DDL = """
CREATE TABLE IF NOT EXISTS aliases(source TEXT,alias TEXT,person TEXT,PRIMARY KEY(source,alias));
CREATE TABLE IF NOT EXISTS landing(batch TEXT PRIMARY KEY,source TEXT,checksum TEXT,path TEXT);
CREATE TABLE IF NOT EXISTS activity(event_id TEXT PRIMARY KEY,person TEXT,source TEXT,alias TEXT,organization TEXT,day TEXT,batch TEXT REFERENCES landing(batch));
CREATE TABLE IF NOT EXISTS versions(id INTEGER PRIMARY KEY,path TEXT,summary TEXT);
CREATE TABLE IF NOT EXISTS erasures(id INTEGER PRIMARY KEY,person TEXT UNIQUE,state TEXT,started_at TEXT,completed_at TEXT);
CREATE TABLE IF NOT EXISTS suppressed_aliases(source TEXT,alias TEXT,erasure_id INTEGER REFERENCES erasures(id),PRIMARY KEY(source,alias));
CREATE TABLE IF NOT EXISTS erasure_steps(erasure_id INTEGER,scope TEXT,state TEXT,detail TEXT,updated_at TEXT,PRIMARY KEY(erasure_id,scope));
CREATE TABLE IF NOT EXISTS version_erasures(version_id INTEGER,erasure_id INTEGER,before_summary TEXT,after_summary TEXT,delta TEXT,notice TEXT,PRIMARY KEY(version_id,erasure_id));
CREATE TABLE IF NOT EXISTS managed_exports(id INTEGER PRIMARY KEY,version_id INTEGER,path TEXT UNIQUE,deleted INTEGER DEFAULT 0);
"""


class Warehouse:
    def __init__(self, root):
        self.root = Path(root)
        self.db = connect(root, DDL)

    def close(self):
        self.db.close()

    def publish(self):
        with atomic(self.db):
            suppressed = records(self.db, 'SELECT source,alias FROM suppressed_aliases')
            sql = 'SELECT * FROM activity'
            parameters = ()
            if suppressed:
                marks = ','.join('(?,?)' for _ in suppressed)
                flat = [item for pair in suppressed for item in pair]
                sql += (' WHERE (source,alias) NOT IN (VALUES ' + marks + ')')
                parameters = tuple(flat)
            sql += ' ORDER BY event_id'
            rows = records(self.db, sql, parameters)
            path = write_rows(self.root / 'facts', rows)
            summary = query([path],
                            'SELECT organization,COUNT(*) visits,COUNT(DISTINCT person) people FROM payload GROUP BY organization ORDER BY organization') if path else []
            cursor = self.db.execute('INSERT INTO versions(path,summary) VALUES(?,?)',
                                     (path, json.dumps(summary, ensure_ascii=False)))
        return cursor.lastrowid

    def read(self, version):
        row = self.db.execute('SELECT * FROM versions WHERE id=?', (version,)).fetchone()
        if not row:
            raise KeyError(version)
        suppressed = records(self.db, 'SELECT source,alias FROM suppressed_aliases')
        rows = []
        if row['path']:
            if suppressed:
                marks = ','.join('(?,?)' for _ in suppressed)
                flat = [item for pair in suppressed for item in pair]
                rows = query([row['path']],
                             'SELECT * FROM payload WHERE (source,alias) NOT IN (VALUES '
                             + marks + ') ORDER BY event_id', tuple(flat))
            else:
                rows = query([row['path']], 'SELECT * FROM payload ORDER BY event_id')
        notices = [dict(json.loads(item['notice']))
                   for item in self.db.execute(
                       'SELECT notice FROM version_erasures WHERE version_id=? AND notice IS NOT NULL ORDER BY erasure_id',
                       (version,)).fetchall()]
        return {'rows': rows, 'organizations': json.loads(row['summary']),
                'erasure_notices': notices}

    def export_person(self, version, person, destination, managed=False):
        rows = [row for row in self.read(version)['rows'] if row['person'] == person]
        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(rows, ensure_ascii=False), encoding='utf-8')
        if managed:
            with atomic(self.db):
                self.db.execute('INSERT OR REPLACE INTO managed_exports(version_id,path,deleted) VALUES(?,?,0)',
                                (version, str(target.resolve())))
        return len(rows)
