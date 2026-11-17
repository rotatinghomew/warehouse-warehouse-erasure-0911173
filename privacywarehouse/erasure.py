"""贯穿数据链的授权撤回擦除。

设计要点：
- suppressed_aliases 是持久化抑制登记：只要 (source,alias) 在登记内，注册、接收、
  发布和历史版本读取都无法重新出现该标识。
- 擦除按 catalog/landing/facts/versions/exports 五个范围分步执行，每步状态落库，
  中断后可凭库中状态续跑；只有所有范围完成并通过实际存储校验才报告完成。
- 共享文件只改写受抑制的行，其他人记录原样保留；组织汇总保留删前/删后/差额。
- 证据只保留计数、文件摘要等非个人明细，不另存可重新识别个人的审计副本。
"""
import json
from datetime import datetime, timezone
from pathlib import Path

from .parquet import query, replace_rows
from .storage import atomic, file_fingerprint, fingerprint, records

SCOPES = ('catalog', 'landing', 'facts', 'versions', 'exports')


class SuppressedIdentityError(ValueError):
    """命中已撤回授权的来源别名，该数据不得入仓或重新发布。"""


class ErasureIncomplete(RuntimeError):
    """擦除存在未完成范围；报告随异常一起返回，便于续跑或人工介入。"""

    def __init__(self, report):
        super().__init__('擦除尚未完成: ' + ','.join(report['incomplete_scopes']))
        self.report = report


def _now():
    return datetime.now(timezone.utc).isoformat()


def _alias_pairs(database, person=None, erasure_id=None):
    if erasure_id is not None:
        rows = database.execute(
            'SELECT source,alias FROM suppressed_aliases WHERE erasure_id=?', (erasure_id,)).fetchall()
    else:
        rows = database.execute('SELECT source,alias FROM suppressed_aliases').fetchall()
    pairs = [(row['source'], row['alias']) for row in rows]
    if person is not None:
        rows = database.execute('SELECT source,alias FROM aliases WHERE person=?', (person,)).fetchall()
        pairs.extend((row['source'], row['alias']) for row in rows)
    return sorted(set(pairs))


def _marks(pairs):
    return ','.join('(?,?)' for _ in pairs)


def _flat(pairs):
    return [item for pair in pairs for item in pair]


def is_suppressed(database, source, alias):
    return database.execute(
        'SELECT 1 FROM suppressed_aliases WHERE source=? AND alias=?', (source, alias)).fetchone() is not None


def assert_not_suppressed(database, source, alias):
    if is_suppressed(database, source, alias):
        raise SuppressedIdentityError((source, alias))


# ---- 各范围处理：返回可落库的非个人证据（计数与文件摘要） ----

def _handle_catalog(warehouse, person, erasure_id):
    database = warehouse.db
    pairs = _alias_pairs(database, person=person, erasure_id=erasure_id)
    if not pairs:
        return {'known_aliases': 0, 'activity_rows_removed': 0}
    removed = database.execute(
        'DELETE FROM activity WHERE (source,alias) IN (VALUES '
        + _marks(pairs) + ')', _flat(pairs)).rowcount
    moved = 0
    for source, alias in pairs:
        cursor = database.execute(
            'INSERT OR IGNORE INTO suppressed_aliases(source,alias,erasure_id) VALUES(?,?,?)',
            (source, alias, erasure_id))
        moved += cursor.rowcount
        database.execute('DELETE FROM aliases WHERE source=? AND alias=?', (source, alias))
    return {'known_aliases': len(pairs), 'aliases_newly_suppressed': moved,
            'activity_rows_removed': removed}


def _handle_landing(warehouse, person, erasure_id):
    database = warehouse.db
    pairs = _alias_pairs(database, erasure_id=erasure_id)
    evidence = {'files_rewritten': 0, 'files_deleted': 0, 'rows_removed': 0}
    if not pairs:
        return evidence
    tracked = database.execute('SELECT * FROM landing').fetchall()
    for entry in tracked:
        path = Path(entry['path']) if entry['path'] else None
        if not path:
            # 前序擦除已删除整文件并清空路径，无需重复处理
            continue
        if not path.is_file():
            raise FileNotFoundError(f"原始报送文件缺失: {entry['batch']}")
        document = json.loads(path.read_text(encoding='utf-8'))
        rows = document.get('rows', [])
        kept = [row for row in rows if (document.get('source'), row.get('alias')) not in set(pairs)]
        removed = len(rows) - len(kept)
        if not removed:
            continue
        evidence['rows_removed'] += removed
        if not kept:
            path.unlink()
            database.execute(
                "UPDATE landing SET path=NULL,checksum='' WHERE batch=?", (entry['batch'],))
            evidence['files_deleted'] += 1
        else:
            temporary = path.with_suffix('.pending')
            document['rows'] = kept
            temporary.write_text(json.dumps(document, ensure_ascii=False), encoding='utf-8')
            temporary.replace(path)
            checksum = fingerprint([document['source'], kept])
            database.execute('UPDATE landing SET checksum=? WHERE batch=?',
                             (checksum, entry['batch']))
            evidence['files_rewritten'] += 1
            evidence.setdefault('rewritten_fingerprints', []).append(
                {'batch': entry['batch'], 'sha256': file_fingerprint(path)})
    return evidence


def _handle_facts(warehouse, person, erasure_id):
    database = warehouse.db
    pairs = _alias_pairs(database, erasure_id=erasure_id)
    evidence = {'files_rewritten': 0, 'files_deleted': 0, 'rows_removed': 0}
    if not pairs:
        return evidence
    versions = database.execute('SELECT id,path FROM versions WHERE path IS NOT NULL').fetchall()
    for entry in versions:
        path = Path(entry['path'])
        if not path.is_file():
            raise FileNotFoundError(f"版本明细文件缺失: v{entry['id']}")
        before = query([str(path)], 'SELECT * FROM payload')
        kept = [row for row in before if (row['source'], row['alias']) not in set(pairs)]
        removed = len(before) - len(kept)
        if not removed:
            continue
        evidence['rows_removed'] += removed
        if not kept:
            path.unlink()
            database.execute('UPDATE versions SET path=NULL WHERE id=?', (entry['id'],))
            evidence['files_deleted'] += 1
        else:
            replace_rows(path, kept)
            evidence['files_rewritten'] += 1
            evidence.setdefault('rewritten_fingerprints', []).append(
                {'version': entry['id'], 'sha256': file_fingerprint(path)})
    facts = warehouse.root / 'facts'
    if facts.is_dir():
        known = {str(Path(row['path']).resolve()) for row in
                 database.execute("SELECT path FROM versions WHERE path IS NOT NULL").fetchall()}
        for stray in facts.glob('*.parquet'):
            if str(stray.resolve()) in known:
                continue
            rows = query([str(stray)])
            if any((row['source'], row['alias']) in set(pairs) for row in rows):
                kept = [row for row in rows if (row['source'], row['alias']) not in set(pairs)]
                evidence['rows_removed'] += len(rows) - len(kept)
                if kept:
                    replace_rows(stray, kept)
                    evidence['files_rewritten'] += 1
                else:
                    stray.unlink()
                    evidence['files_deleted'] += 1
    return evidence


def _summary(rows):
    totals = {}
    people = {}
    for row in rows:
        org = row['organization']
        totals[org] = totals.get(org, 0) + 1
        people.setdefault(org, set()).add(row['person'])
    return [{'organization': org, 'visits': totals[org], 'people': len(people.get(org, ()))}
            for org in sorted(totals)]


def _handle_versions(warehouse, person, erasure_id):
    database = warehouse.db
    pairs = _alias_pairs(database, erasure_id=erasure_id)
    evidence = {'versions_updated': 0}
    if not pairs:
        return evidence
    versions = database.execute('SELECT * FROM versions').fetchall()
    for entry in versions:
        already = database.execute(
            'SELECT 1 FROM version_erasures WHERE version_id=? AND erasure_id=?',
            (entry['id'], erasure_id)).fetchone()
        if already:
            continue
        before_summary = json.loads(entry['summary']) if entry['summary'] else []
        rows = query([str(entry['path'])]) if entry['path'] else []
        after_summary = _summary(rows)
        before_map = {item['organization']: item for item in before_summary}
        after_map = {item['organization']: item for item in after_summary}
        delta = []
        for org in sorted(set(before_map) | set(after_map)):
            old = before_map.get(org, {'visits': 0, 'people': 0})
            new = after_map.get(org, {'visits': 0, 'people': 0})
            if old != new:
                delta.append({'organization': org,
                              'visits_before': old['visits'], 'visits_after': new['visits'],
                              'visits_removed': old['visits'] - new['visits'],
                              'people_before': old['people'], 'people_after': new['people'],
                              'people_removed': old['people'] - new['people']})
        notice = {'erasure_id': erasure_id, 'version_id': entry['id'],
                  'suppressed_alias_count': len(pairs),
                  'message': '该历史版本中属于撤回授权参与者的明细已被擦除，对应部分不可继续读取；'
                             '共享文件内其他人的记录及组织级删前/删后差额仍可解释。'}
        database.execute(
            'INSERT INTO version_erasures VALUES(?,?,?,?,?,?)',
            (entry['id'], erasure_id, json.dumps(before_summary, ensure_ascii=False),
             json.dumps(after_summary, ensure_ascii=False), json.dumps(delta, ensure_ascii=False),
             json.dumps(notice, ensure_ascii=False)))
        database.execute('UPDATE versions SET summary=? WHERE id=?',
                         (json.dumps(after_summary, ensure_ascii=False), entry['id']))
        evidence['versions_updated'] += 1
        evidence.setdefault('deltas', []).extend(delta)
    return evidence


def _handle_exports(warehouse, person, erasure_id):
    database = warehouse.db
    evidence = {'managed_exports_deleted': 0, 'already_absent': 0}
    for entry in database.execute('SELECT * FROM managed_exports WHERE deleted=0').fetchall():
        path = Path(entry['path'])
        if path.is_file():
            path.unlink()
            evidence['managed_exports_deleted'] += 1
        else:
            evidence['already_absent'] += 1
        database.execute('UPDATE managed_exports SET deleted=1 WHERE id=?', (entry['id'],))
    return evidence


_HANDLERS = {'catalog': _handle_catalog, 'landing': _handle_landing,
             'facts': _handle_facts, 'versions': _handle_versions,
             'exports': _handle_exports}


def _record_step(database, erasure_id, scope, state, detail):
    database.execute(
        'INSERT INTO erasure_steps VALUES(?,?,?,?,?) ON CONFLICT(erasure_id,scope) DO UPDATE SET '
        'state=excluded.state,detail=excluded.detail,updated_at=excluded.updated_at',
        (erasure_id, scope, state, json.dumps(detail, ensure_ascii=False), _now()))


def _pipeline(warehouse, erasure_id, person):
    database = warehouse.db
    failures = 0
    for scope in SCOPES:
        state = database.execute(
            'SELECT state FROM erasure_steps WHERE erasure_id=? AND scope=?',
            (erasure_id, scope)).fetchone()
        if state and state['state'] == 'complete' and scope not in ('landing', 'facts'):
            # landing/facts 面对的是真实文件，检查点可能还原旧文件，必须每次重新对账
            continue
        try:
            detail = _HANDLERS[scope](warehouse, person, erasure_id)
            _record_step(database, erasure_id, scope, 'complete', detail)
        except BaseException as exc:
            failures += 1
            _record_step(database, erasure_id, scope, 'failed',
                         {'error': type(exc).__name__, 'message': str(exc)})
    return failures


def _run_pipeline(warehouse, erasure_id, person):
    failures = _pipeline(warehouse, erasure_id, person)
    if not failures:
        warehouse.db.execute("UPDATE erasures SET state='complete',completed_at=? WHERE id=?",
                             (_now(), erasure_id))


def _report(database, erasure_id, warehouse_root=None):
    erasure = database.execute('SELECT * FROM erasures WHERE id=?', (erasure_id,)).fetchone()
    steps = records(database, 'SELECT scope,state,detail FROM erasure_steps WHERE erasure_id=? ORDER BY scope',
                    (erasure_id,))
    incomplete = [step['scope'] for step in steps
                  if step['state'] != 'complete']
    missing = [scope for scope in SCOPES
               if not any(step['scope'] == scope for step in steps)]
    return {
        'erasure_id': erasure_id,
        'state': erasure['state'],
        'complete': erasure['state'] == 'complete' and not incomplete and not missing,
        'incomplete_scopes': sorted(set(incomplete + missing)),
        'scopes': {step['scope']: {'state': step['state'],
                                   'detail': json.loads(step['detail']) if step['detail'] else None}
                   for step in steps},
        'verification': verify_erasure(database, warehouse_root=warehouse_root),
    }


def _resume_locked(warehouse, erasure_id):
    database = warehouse.db
    entry = database.execute('SELECT * FROM erasures WHERE id=?', (erasure_id,)).fetchone()
    if not entry:
        raise KeyError(erasure_id)
    if entry['state'] != 'complete':
        database.execute("UPDATE erasures SET state='running' WHERE id=?", (erasure_id,))
    try:
        _run_pipeline(warehouse, erasure_id, entry['person'])
    except BaseException:
        pass
    return _report(database, erasure_id, warehouse_root=warehouse.root)


def erase_person(warehouse, person):
    database = warehouse.db
    with atomic(database):
        existing = database.execute('SELECT id,state FROM erasures WHERE person=?', (person,)).fetchone()
        if existing:
            erasure_id = existing['id']
            if existing['state'] != 'complete':
                database.execute("UPDATE erasures SET state='running' WHERE id=?", (erasure_id,))
        else:
            cursor = database.execute(
                "INSERT INTO erasures(person,state,started_at) VALUES(?,'running',?)",
                (person, _now()))
            erasure_id = cursor.lastrowid
        report = _resume_locked(warehouse, erasure_id)
    if not report['complete']:
        raise ErasureIncomplete(report)
    return report


def resume_erasure(warehouse, person=None, erasure_id=None):
    database = warehouse.db
    with atomic(database):
        if erasure_id is None:
            row = database.execute('SELECT id FROM erasures WHERE person=?', (person,)).fetchone()
            if not row:
                raise KeyError(person)
            erasure_id = row['id']
        report = _resume_locked(warehouse, erasure_id)
    if not report['complete']:
        raise ErasureIncomplete(report)
    return report


def suppress_alias(warehouse, source, alias, person):
    """登记新确认的来源别名，并把擦除范围扩展到全部已知存储与派生数据。"""
    database = warehouse.db
    with atomic(database):
        blocked = database.execute(
            'SELECT erasure_id FROM suppressed_aliases WHERE source=? AND alias=?',
            (source, alias)).fetchone()
        if blocked:
            report = _resume_locked(warehouse, blocked['erasure_id'])
            if not report['complete']:
                raise ErasureIncomplete(report)
            return report
        row = database.execute('SELECT person FROM aliases WHERE source=? AND alias=?',
                               (source, alias)).fetchone()
        if row and row['person'] != person:
            raise ValueError("别名已经属于另一身份，不能静默改挂到撤回对象")
        erasure = database.execute('SELECT id,state FROM erasures WHERE person=?', (person,)).fetchone()
        if not erasure:
            cursor = database.execute(
                "INSERT INTO erasures(person,state,started_at) VALUES(?,'running',?)",
                (person, _now()))
            erasure_id = cursor.lastrowid
        else:
            erasure_id = erasure['id']
        database.execute(
            'INSERT INTO suppressed_aliases(source,alias,erasure_id) VALUES(?,?,?)',
            (source, alias, erasure_id))
        database.execute('DELETE FROM aliases WHERE source=? AND alias=?', (source, alias))
        for scope in SCOPES:
            database.execute(
                'UPDATE erasure_steps SET state=? WHERE erasure_id=? AND scope=?',
                ('pending', erasure_id, scope))
            if not database.execute(
                    'SELECT 1 FROM erasure_steps WHERE erasure_id=? AND scope=?',
                    (erasure_id, scope)).fetchone():
                _record_step(database, erasure_id, scope, 'pending', {})
        report = _resume_locked(warehouse, erasure_id)
    if not report['complete']:
        raise ErasureIncomplete(report)
    return report


# ---- 基于实际存储的校验：返回所有仍可定位到受抑制标识的残留 ----

def verify_erasure(database, warehouse_root=None):
    pairs = _alias_pairs(database)
    pair_set = set(pairs)
    residuals = []
    if pairs:
        for row in database.execute(
                "SELECT event_id,source,alias FROM activity WHERE (source,alias) IN (VALUES "
                + _marks(pairs) + ")", _flat(pairs)).fetchall():
            residuals.append({'scope': 'catalog', 'location': 'activity:' + row['event_id']})
        for row in database.execute(
                "SELECT source,alias FROM aliases WHERE (source,alias) IN (VALUES "
                + _marks(pairs) + ")", _flat(pairs)).fetchall():
            residuals.append({'scope': 'catalog',
                              'location': f"aliases:{row['source']}/{row['alias']}"})
    for entry in database.execute('SELECT * FROM landing').fetchall():
        if not entry['path']:
            continue
        path = Path(entry['path'])
        if not path.is_file():
            residuals.append({'scope': 'landing', 'location': entry['batch'], 'issue': '文件缺失，状态未知'})
            continue
        document = json.loads(path.read_text(encoding='utf-8'))
        hits = [row['event_id'] for row in document.get('rows', [])
                if (document.get('source'), row.get('alias')) in pair_set]
        if hits:
            residuals.append({'scope': 'landing', 'location': str(path), 'matching_events': len(hits)})
    if warehouse_root is not None:
        facts = Path(warehouse_root) / 'facts'
        if facts.is_dir():
            for path in sorted(facts.glob('*.parquet')):
                rows = query([str(path)])
                hits = [row['event_id'] for row in rows if (row['source'], row['alias']) in pair_set]
                if hits:
                    residuals.append({'scope': 'facts', 'location': str(path),
                                      'matching_events': len(hits)})
    for entry in database.execute("SELECT * FROM versions WHERE path IS NOT NULL").fetchall():
        path = Path(entry['path'])
        if not path.is_file():
            residuals.append({'scope': 'facts', 'location': f"v{entry['id']}", 'issue': '文件缺失，状态未知'})
            continue
        rows = query([str(path)])
        hits = [row['event_id'] for row in rows if (row['source'], row['alias']) in pair_set]
        if hits:
            residuals.append({'scope': 'facts', 'location': str(path),
                              'version': entry['id'], 'matching_events': len(hits)})
    for entry in database.execute('SELECT * FROM managed_exports WHERE deleted=0').fetchall():
        path = Path(entry['path'])
        if path.is_file():
            residuals.append({'scope': 'exports', 'location': str(path)})
    return {'suppressed_alias_count': len(pairs), 'residual_count': len(residuals),
            'residuals': residuals}


def reconcile(warehouse):
    """从较早检查点恢复后调用：强制重跑全部范围，并对实际存储做全量对账修复。"""
    database = warehouse.db
    with atomic(database):
        for entry in database.execute('SELECT id,person,state FROM erasures').fetchall():
            database.execute("UPDATE erasures SET state='running' WHERE id=?", (entry['id'],))
            # 检查点可能把已擦除的文件或行还原，因此不能信任历史完成标记
            for scope in SCOPES:
                _record_step(database, entry['id'], scope, 'pending', {})
            _run_pipeline(warehouse, entry['id'], entry['person'])
        reports = []
        incomplete = []
        for entry in database.execute('SELECT id FROM erasures ORDER BY id').fetchall():
            report = _report(database, entry['id'], warehouse_root=warehouse.root)
            reports.append(report)
            incomplete.extend(report['incomplete_scopes'])
        verification = verify_erasure(database, warehouse_root=warehouse.root)
    if incomplete or verification['residual_count']:
        raise ErasureIncomplete({'reports': reports, 'verification': verification,
                                 'incomplete_scopes': sorted(set(incomplete))})
    return {'reports': reports, 'verification': verification}
