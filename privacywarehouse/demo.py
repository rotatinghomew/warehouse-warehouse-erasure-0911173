from .warehouse import Warehouse
from .subjects import register
from .landing import receive


def run(root):
    warehouse=Warehouse(root)
    try:
        register(warehouse.db,'学校端','别名A','匿名人1')
        register(warehouse.db,'场馆端','别名B','匿名人1')
        receive(warehouse,'报送A','学校端',[{'event_id':'E1','alias':'别名A','organization':'O1','day':'2026-11-01'}])
        receive(warehouse,'报送B','场馆端',[{'event_id':'E2','alias':'别名B','organization':'O1','day':'2026-11-01'}])
        return warehouse.read(warehouse.publish())
    finally:
        warehouse.close()
