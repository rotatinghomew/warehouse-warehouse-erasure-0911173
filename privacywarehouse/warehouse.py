import json
from pathlib import Path

from .parquet import query, write_rows
from .storage import atomic, connect, records

DDL="""
CREATE TABLE IF NOT EXISTS aliases(source TEXT,alias TEXT,person TEXT,PRIMARY KEY(source,alias));
CREATE TABLE IF NOT EXISTS landing(batch TEXT PRIMARY KEY,source TEXT,checksum TEXT,path TEXT);
CREATE TABLE IF NOT EXISTS activity(event_id TEXT PRIMARY KEY,person TEXT,source TEXT,alias TEXT,organization TEXT,day TEXT,batch TEXT REFERENCES landing(batch));
CREATE TABLE IF NOT EXISTS versions(id INTEGER PRIMARY KEY,path TEXT,summary TEXT);
"""


class Warehouse:
    def __init__(self,root):
        self.root=Path(root)
        self.db=connect(root,DDL)

    def close(self):
        self.db.close()

    def publish(self):
        with atomic(self.db):
            rows=records(self.db,'SELECT * FROM activity ORDER BY event_id')
            path=write_rows(self.root/'facts',rows)
            summary=query([path],'SELECT organization,COUNT(*) visits,COUNT(DISTINCT person) people FROM payload GROUP BY organization ORDER BY organization') if path else []
            cursor=self.db.execute('INSERT INTO versions(path,summary) VALUES(?,?)',(path,json.dumps(summary,ensure_ascii=False)))
        return cursor.lastrowid

    def read(self,version):
        row=self.db.execute('SELECT * FROM versions WHERE id=?',(version,)).fetchone()
        if not row:
            raise KeyError(version)
        return {'rows':query([row['path']],'SELECT * FROM payload ORDER BY event_id') if row['path'] else [],'organizations':json.loads(row['summary'])}

    def export_person(self,version,person,destination):
        rows=[row for row in self.read(version)['rows'] if row['person']==person]
        Path(destination).write_text(json.dumps(rows,ensure_ascii=False),encoding='utf-8')
        return len(rows)
