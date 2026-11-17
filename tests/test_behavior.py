import tempfile
import unittest


class WorkspaceTest(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory()
        self.directory=self.temporary.name
        self.resources=[]

    def open(self, factory):
        resource=factory(self.directory)
        self.resources.append(resource)
        return resource

    def tearDown(self):
        for resource in reversed(self.resources):
            resource.close()
        self.temporary.cleanup()

from privacywarehouse.warehouse import Warehouse
from privacywarehouse.subjects import register, resolve
from privacywarehouse.landing import receive


def activity(event='E1',alias='a'):
    return {'event_id':event,'alias':alias,'organization':'O1','day':'2026-11-01'}


class Behavior(WorkspaceTest):
    def test_alias_domain(self):
        warehouse=self.open(Warehouse)
        self.assertTrue(register(warehouse.db,'A','a','P1'))
        self.assertFalse(register(warehouse.db,'A','a','P1'))
        register(warehouse.db,'B','a','P2')
        self.assertEqual(resolve(warehouse.db,'B','a'),'P2')
        with self.assertRaises(ValueError):
            register(warehouse.db,'A','a','P2')

    def test_unknown_identity_is_not_silently_dropped(self):
        warehouse=self.open(Warehouse)
        with self.assertRaises(KeyError):
            receive(warehouse,'B1','A',[activity()])
        self.assertEqual(warehouse.db.execute('SELECT COUNT(*) FROM landing').fetchone()[0],0)

    def test_receive_and_raw_provenance(self):
        from pathlib import Path
        warehouse=self.open(Warehouse)
        register(warehouse.db,'A','a','P1')
        self.assertTrue(receive(warehouse,'B1','A',[activity()]))
        self.assertFalse(receive(warehouse,'B1','A',[activity()]))
        with self.assertRaises(ValueError):
            receive(warehouse,'B1','A',[activity('E2')])
        path=warehouse.db.execute('SELECT path FROM landing').fetchone()[0]
        self.assertIn('E1',Path(path).read_text())

    def test_aliases_and_shared_file(self):
        import json
        from pathlib import Path
        warehouse=self.open(Warehouse)
        register(warehouse.db,'A','a','P1')
        register(warehouse.db,'A','b','P1')
        register(warehouse.db,'A','c','P2')
        receive(warehouse,'B1','A',[activity(),activity('E2','b'),activity('E3','c')])
        version=warehouse.publish()
        self.assertEqual(warehouse.read(version)['organizations'][0]['people'],2)
        output=Path(self.directory)/'person.json'
        self.assertEqual(warehouse.export_person(version,'P1',output),2)
        self.assertEqual(len(json.loads(output.read_text())),2)
        reopened=self.open(Warehouse)
        self.assertEqual(len(reopened.read(version)['rows']),3)
