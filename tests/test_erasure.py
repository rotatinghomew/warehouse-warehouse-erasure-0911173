import json
import tempfile
import unittest
from pathlib import Path

from privacywarehouse.warehouse import Warehouse
from privacywarehouse.subjects import register
from privacywarehouse.landing import receive
from privacywarehouse import erasure
from privacywarehouse.erasure import (ErasureIncomplete, SuppressedIdentityError,
                                      erase_person, reconcile, resume_erasure,
                                      suppress_alias, verify_erasure)
from privacywarehouse.parquet import query, write_rows


def event(event, alias, organization='O1', day='2026-11-01'):
    return {'event_id': event, 'alias': alias, 'organization': organization, 'day': day}


class ErasureCase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.warehouse = Warehouse(self.directory)

    def tearDown(self):
        self.warehouse.close()
        self.temporary.cleanup()

    def build(self):
        db = self.warehouse.db
        register(db, 'A', 'a', 'P1')
        register(db, 'B', 'b', 'P1')
        register(db, 'A', 'c', 'P2')
        receive(self.warehouse, 'B1', 'A', [event('E1', 'a'), event('E3', 'c')])
        receive(self.warehouse, 'B2', 'B', [event('E2', 'b', organization='O2')])
        version = self.warehouse.publish()
        managed = self.directory / 'exports' / 'p1.json'
        self.warehouse.export_person(version, 'P1', managed, managed=True)
        outside = self.directory / 'outside.json'
        self.warehouse.export_person(version, 'P1', outside, managed=False)
        return version, managed, outside

    def test_full_chain_erasure_keeps_others(self):
        version, managed, outside = self.build()
        report = erase_person(self.warehouse, 'P1')
        self.assertTrue(report['complete'])
        self.assertEqual(report['incomplete_scopes'], [])
        db = self.warehouse.db
        self.assertEqual(db.execute('SELECT COUNT(*) FROM activity').fetchone()[0], 1)
        self.assertEqual(db.execute("SELECT event_id FROM activity").fetchone()[0], 'E3')
        self.assertEqual(db.execute('SELECT COUNT(*) FROM aliases').fetchone()[0], 1)
        self.assertEqual(db.execute('SELECT COUNT(*) FROM suppressed_aliases').fetchone()[0], 2)
        # 原始落地区：共享报送被改写保留他人，单人报送被删除
        landing_doc = json.loads(Path(db.execute(
            "SELECT path FROM landing WHERE batch='B1'").fetchone()[0]).read_text())
        self.assertEqual([row['event_id'] for row in landing_doc['rows']], ['E3'])
        self.assertIsNone(db.execute("SELECT path FROM landing WHERE batch='B2'").fetchone()[0])
        # 固定版本Parquet：他人保留，P1明细消失
        path = db.execute('SELECT path FROM versions WHERE id=?', (version,)).fetchone()[0]
        ids = sorted(row['event_id'] for row in query([path]))
        self.assertEqual(ids, ['E3'])
        read = self.warehouse.read(version)
        self.assertEqual([row['event_id'] for row in read['rows']], ['E3'])
        self.assertEqual(len(read['erasure_notices']), 1)
        self.assertIn('不可继续读取', read['erasure_notices'][0]['message'])
        # 受管导出已删除；未纳入管理边界的导出不会被冒认处理
        self.assertFalse(managed.exists())
        self.assertTrue(outside.exists())
        # 校验基于实际存储，无残留
        verification = verify_erasure(db, warehouse_root=self.directory)
        self.assertEqual(verification['residual_count'], 0)
        self.assertEqual(verification['suppressed_alias_count'], 2)

    def test_organization_delta_explains_before_and_after(self):
        version, _, _ = self.build()
        erase_person(self.warehouse, 'P1')
        row = self.warehouse.db.execute(
            'SELECT * FROM version_erasures WHERE version_id=?', (version,)).fetchone()
        delta = json.loads(row['delta'])
        o2 = next(item for item in delta if item['organization'] == 'O2')
        self.assertEqual((o2['visits_before'], o2['visits_after'], o2['visits_removed']), (1, 0, 1))
        self.assertEqual((o2['people_before'], o2['people_after'], o2['people_removed']), (1, 0, 1))
        o1 = next(item for item in delta if item['organization'] == 'O1')
        self.assertEqual((o1['visits_before'], o1['visits_after']), (2, 1))
        self.assertEqual((o1['people_before'], o1['people_after']), (2, 1))
        after = json.loads(row['after_summary'])
        self.assertEqual(self.warehouse.read(version)['organizations'], after)

    def test_old_files_and_reregistration_are_blocked(self):
        self.build()
        erase_person(self.warehouse, 'P1')
        with self.assertRaises(SuppressedIdentityError):
            receive(self.warehouse, 'B3', 'A', [event('E9', 'a')])
        with self.assertRaises(SuppressedIdentityError):
            register(self.warehouse.db, 'A', 'a', 'P9')
        # 被拦截批次没有任何落库或落地痕迹
        self.assertEqual(self.warehouse.db.execute(
            "SELECT COUNT(*) FROM landing WHERE batch='B3'").fetchone()[0], 0)
        self.assertEqual(list((self.directory / 'landing').glob('*B3*')), [])
        # 含受抑制标识的混批整体拒绝，其他人的行也不会重复入仓
        before = self.warehouse.db.execute('SELECT COUNT(*) FROM activity').fetchone()[0]
        with self.assertRaises(SuppressedIdentityError):
            receive(self.warehouse, 'B4', 'A', [event('E10', 'a'), event('E11', 'c')])
        self.assertEqual(self.warehouse.db.execute('SELECT COUNT(*) FROM activity').fetchone()[0], before)

    def test_historical_version_cannot_bypass_suppression(self):
        version, _, _ = self.build()
        erase_person(self.warehouse, 'P1')
        read = self.warehouse.read(version)
        self.assertTrue(all(row['person'] != 'P1' for row in read['rows']))
        self.assertTrue(all((row['source'], row['alias']) not in {('A', 'a'), ('B', 'b')}
                            for row in read['rows']))
        reopened = Warehouse(self.directory)
        try:
            self.assertTrue(all(row['person'] != 'P1'
                                for row in reopened.read(version)['rows']))
        finally:
            reopened.close()

    def test_interrupted_erasure_reports_scope_and_resumes(self):
        self.build()

        def fail_once(warehouse, person, erasure_id):
            raise OSError('模拟共享文件改写失败')

        erasure._HANDLERS['landing'] = fail_once
        try:
            with self.assertRaises(ErasureIncomplete) as context:
                erase_person(self.warehouse, 'P1')
            report = context.exception.report
        finally:
            from privacywarehouse.erasure import _handle_landing
            erasure._HANDLERS['landing'] = _handle_landing
        self.assertIn('landing', report['incomplete_scopes'])
        self.assertEqual(report['scopes']['catalog']['state'], 'complete')
        self.assertEqual(report['scopes']['landing']['state'], 'failed')
        self.assertEqual(report['scopes']['landing']['detail']['error'], 'OSError')
        # catalog 范围已真正生效
        self.assertEqual(self.warehouse.db.execute(
            'SELECT COUNT(*) FROM suppressed_aliases').fetchone()[0], 2)
        # 重跑同一人是续跑而非新请求；完成后才报告完成
        report = resume_erasure(self.warehouse, person='P1')
        self.assertTrue(report['complete'])
        self.assertEqual(verify_erasure(self.warehouse.db, self.directory)['residual_count'], 0)

    def test_new_alias_extends_completed_erasure(self):
        version, _, _ = self.build()
        erase_person(self.warehouse, 'P1')
        # 新来源先确认了一个别名，随后撤回范围扩展
        register(self.warehouse.db, 'C', 'd', 'P1')
        receive(self.warehouse, 'B5', 'C', [event('E5', 'd')])
        report = suppress_alias(self.warehouse, 'C', 'd', 'P1')
        self.assertTrue(report['complete'])
        self.assertEqual(self.warehouse.db.execute(
            'SELECT COUNT(*) FROM suppressed_aliases').fetchone()[0], 3)
        with self.assertRaises(SuppressedIdentityError):
            receive(self.warehouse, 'B6', 'C', [event('E6', 'd')])
        with self.assertRaises(SuppressedIdentityError):
            register(self.warehouse.db, 'C', 'd', 'P1')
        self.assertEqual(verify_erasure(self.warehouse.db, self.directory)['residual_count'], 0)
        # 新版本发布也无法带回受抑制行
        new_version = self.warehouse.publish()
        self.assertTrue(all(row['event_id'] != 'E5'
                            for row in self.warehouse.read(new_version)['rows']))

    def test_checkpoint_restore_is_reconciled(self):
        version, _, _ = self.build()
        facts = self.directory / 'facts'
        erase_person(self.warehouse, 'P1')
        # 模拟从较早检查点恢复：旧Parquet和旧原始文件被还原，活动表也被带回
        restored_rows = [
            {'event_id': 'E1', 'person': 'P1', 'source': 'A', 'alias': 'a',
             'organization': 'O1', 'day': '2026-11-01', 'batch': 'B1'},
            {'event_id': 'E3', 'person': 'P2', 'source': 'A', 'alias': 'c',
             'organization': 'O1', 'day': '2026-11-01', 'batch': 'B1'}]
        restored = Path(write_rows(facts, restored_rows))
        from privacywarehouse.storage import atomic
        with atomic(self.warehouse.db):
            self.warehouse.db.execute(
                "INSERT OR REPLACE INTO activity VALUES('E1','P1','A','a','O1','2026-11-01','B1')")
        verification = verify_erasure(self.warehouse.db, self.directory)
        self.assertGreaterEqual(verification['residual_count'], 2)
        result = reconcile(self.warehouse)
        self.assertEqual(result['verification']['residual_count'], 0)
        # 恢复文件是含他人的共享文件：P1被再次清除、P2保留，文件不整批丢弃
        self.assertTrue(restored.exists())
        rows = query([str(restored)])
        self.assertEqual([row['event_id'] for row in rows], ['E3'])
        flat = [row for path in facts.glob('*.parquet') for row in query([str(path)])]
        self.assertTrue(all(row['person'] != 'P1' for row in flat))
        self.assertEqual(self.warehouse.db.execute(
            "SELECT event_id FROM activity").fetchone()[0], 'E3')
        # 抑制登记本身仍在，读取固定版本依旧受限
        self.assertTrue(all(row['person'] != 'P1'
                            for row in self.warehouse.read(version)['rows']))
        # 只含撤回者的孤儿恢复文件应被整体删除
        solo = Path(write_rows(facts, [restored_rows[0]]))
        reconcile(self.warehouse)
        self.assertFalse(solo.exists())
        self.assertEqual(verify_erasure(self.warehouse.db, self.directory)['residual_count'], 0)

    def test_idempotent_erasure_and_no_double_counting(self):
        version, _, _ = self.build()
        first = erase_person(self.warehouse, 'P1')
        second = erase_person(self.warehouse, 'P1')
        self.assertTrue(second['complete'])
        self.assertEqual(first['erasure_id'], second['erasure_id'])
        self.assertEqual(self.warehouse.db.execute('SELECT COUNT(*) FROM erasures').fetchone()[0], 1)
        # 他人活动与版本统计未被重复处理
        self.assertEqual(self.warehouse.db.execute('SELECT COUNT(*) FROM activity').fetchone()[0], 1)
        summary = self.warehouse.read(version)['organizations']
        self.assertEqual(summary, [{'organization': 'O1', 'visits': 1, 'people': 1}])

    def test_audit_evidence_contains_no_person_detail(self):
        self.build()
        report = erase_person(self.warehouse, 'P1')
        blob = json.dumps(report, ensure_ascii=False)
        for identifier in ('E1', 'E2'):
            self.assertNotIn(identifier, blob)
        steps = self.warehouse.db.execute('SELECT detail FROM erasure_steps').fetchall()
        for step in steps:
            self.assertNotIn('E1', step['detail'])

    def test_fully_erased_version_drops_file_but_keeps_explanation(self):
        db = self.warehouse.db
        register(db, 'A', 'a', 'P1')
        receive(self.warehouse, 'B1', 'A', [event('E1', 'a')])
        version = self.warehouse.publish()
        path = db.execute('SELECT path FROM versions WHERE id=?', (version,)).fetchone()[0]
        erase_person(self.warehouse, 'P1')
        self.assertFalse(Path(path).exists())
        self.assertIsNone(db.execute('SELECT path FROM versions WHERE id=?', (version,)).fetchone()[0])
        read = self.warehouse.read(version)
        self.assertEqual(read['rows'], [])
        self.assertEqual(len(read['erasure_notices']), 1)
        record = db.execute('SELECT * FROM version_erasures WHERE version_id=?', (version,)).fetchone()
        self.assertEqual(json.loads(record['delta'])[0]['visits_removed'], 1)

    def test_unmanaged_export_is_reported_as_outside_boundary(self):
        version, managed, outside = self.build()
        report = erase_person(self.warehouse, 'P1')
        self.assertTrue(report['complete'])
        # 只有显式纳入管理的导出才在擦除边界内，仓外文件不会被路径猜测式处理
        tracked = [row['path'] for row in
                   self.warehouse.db.execute('SELECT path FROM managed_exports').fetchall()]
        self.assertEqual(tracked, [str(managed.resolve())])
        self.assertTrue(outside.exists())
        self.assertEqual(verify_erasure(self.warehouse.db, self.directory)['residual_count'], 0)
