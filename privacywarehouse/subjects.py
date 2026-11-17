from .erasure import assert_not_suppressed
from .storage import atomic, require_text


def register(database, source, alias, person):
    for value in (source, alias, person):
        require_text(value, '身份标识')
    with atomic(database):
        assert_not_suppressed(database, source, alias)
        old = database.execute('SELECT person FROM aliases WHERE source=? AND alias=?',
                               (source, alias)).fetchone()
        if old:
            if old[0] != person:
                raise ValueError("别名已经属于另一身份")
            return False
        database.execute('INSERT INTO aliases VALUES(?,?,?)', (source, alias, person))
    return True


def resolve(database, source, alias):
    row = database.execute('SELECT person FROM aliases WHERE source=? AND alias=?',
                           (source, alias)).fetchone()
    if not row:
        raise KeyError((source, alias))
    return row[0]
