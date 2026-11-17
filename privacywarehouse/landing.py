import json
from pathlib import Path

from .subjects import resolve
from .storage import atomic, fingerprint, require_text


def receive(warehouse,batch,source,rows):
    from datetime import date
    checksum=fingerprint([source,rows])
    with atomic(warehouse.db):
        old=warehouse.db.execute('SELECT checksum FROM landing WHERE batch=?',(batch,)).fetchone()
        if old:
            if old[0]!=checksum:
                raise ValueError("原始报送批次冲突")
            return False
        normalized=[]
        for row in rows:
            require_text(row['event_id'],'事件标识')
            require_text(row['organization'],'组织')
            date.fromisoformat(row['day'])
            person=resolve(warehouse.db,source,row['alias'])
            normalized.append(dict(row,person=person,source=source))
        directory=warehouse.root/'landing'
        directory.mkdir(exist_ok=True)
        path=directory/(fingerprint([batch,source])+'.json')
        temporary=path.with_suffix('.pending')
        temporary.write_text(json.dumps({'batch':batch,'source':source,'rows':rows},ensure_ascii=False),encoding='utf-8')
        temporary.replace(path)
        warehouse.db.execute('INSERT INTO landing VALUES(?,?,?,?)',(batch,source,checksum,str(path.resolve())))
        for row in normalized:
            warehouse.db.execute('INSERT INTO activity VALUES(?,?,?,?,?,?,?)',(row['event_id'],row['person'],row['source'],row['alias'],row['organization'],row['day'],batch))
    return True
