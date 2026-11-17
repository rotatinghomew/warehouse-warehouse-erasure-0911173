from .erasure import SuppressedIdentityError, erase_person, reconcile
from .warehouse import Warehouse
from .subjects import register
from .landing import receive
from .parquet import write_rows
from pathlib import Path


def run(root):
    warehouse = Warehouse(root)
    try:
        register(warehouse.db, '学校端', '别名A', '匿名人1')
        register(warehouse.db, '场馆端', '别名B', '匿名人1')
        register(warehouse.db, '学校端', '别名C', '匿名人2')
        receive(warehouse, '报送A', '学校端',
                [{'event_id': 'E1', 'alias': '别名A', 'organization': 'O1', 'day': '2026-11-01'},
                 {'event_id': 'E3', 'alias': '别名C', 'organization': 'O1', 'day': '2026-11-01'}])
        receive(warehouse, '报送B', '场馆端',
                [{'event_id': 'E2', 'alias': '别名B', 'organization': 'O1', 'day': '2026-11-01'}])
        version = warehouse.publish()
        export = Path(root) / 'exports' / 'person1.json'
        warehouse.export_person(version, '匿名人1', export, managed=True)
        before = warehouse.read(version)
        report = erase_person(warehouse, '匿名人1')
        after = warehouse.read(version)
        blocked = None
        try:
            receive(warehouse, '报送C', '学校端',
                    [{'event_id': 'E4', 'alias': '别名A', 'organization': 'O1', 'day': '2026-11-02'}])
        except SuppressedIdentityError as exc:
            blocked = str(exc)
        restored = warehouse.root / 'facts' / 'restored-checkpoint.parquet'
        restored.parent.mkdir(exist_ok=True)
        write_rows(restored.parent,
                   [{'event_id': 'E1', 'person': '匿名人1', 'source': '学校端', 'alias': '别名A',
                     'organization': 'O1', 'day': '2026-11-01', 'batch': '报送A'}])
        restored_path = str(restored.resolve())
        reconciliation = reconcile(warehouse)
        return {'before_erasure': before, 'erasure_report': report, 'after_erasure': after,
                'reingestion_blocked': blocked,
                'restored_checkpoint_reconciled': reconciliation,
                'surviving_detail_files': [str(p) for p in (warehouse.root / 'facts').glob('*.parquet')],
                'restored_checkpoint_removed': not Path(restored_path).exists()}
    finally:
        warehouse.close()
