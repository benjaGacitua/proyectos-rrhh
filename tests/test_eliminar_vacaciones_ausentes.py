"""Check del path de borrado de vacaciones fantasma (hard delete).

Fake cursor en memoria: no toca DB. Verifica guard 30%, ventana y borrado real.
Correr: python -m tests.test_eliminar_vacaciones_ausentes
"""
from app.etl.load import eliminar_vacaciones_ausentes


class FakeCursor:
    def __init__(self, ids_ventana):
        self._ids = ids_ventana
        self.deleted = None
        self._last = None

    def execute(self, sql, params=None):
        if sql.strip().startswith("SELECT"):
            self._last = [(i,) for i in self._ids]
        elif sql.strip().startswith("DELETE"):
            self.deleted = list(params[0])

    def fetchall(self):
        return self._last

    def close(self):
        pass


class FakeConn:
    def __init__(self, ids_ventana):
        self.cursor_obj = FakeCursor(ids_ventana)

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        pass

    def rollback(self):
        pass


def _run(ids_db, ids_api, **kw):
    conn = FakeConn(ids_db)
    api = [{"id": i} for i in ids_api]
    borrados = eliminar_vacaciones_ausentes(conn, api, **kw)
    return borrados, conn.cursor_obj.deleted


# 1) Fantasma normal (bajo umbral): DB {1..5}, API {1..4} -> 1/5=20% -> borra 5
borrados, deleted = _run([1, 2, 3, 4, 5], [1, 2, 3, 4])
assert borrados == [5] and deleted == [5], (borrados, deleted)

# 2) Guard 30%: DB {1,2,3}, API {1} -> 2/3 > 30% -> NO borra
borrados, deleted = _run([1, 2, 3], [1])
assert borrados == [] and deleted is None, (borrados, deleted)

# 3) API vacía -> guard, no borra
borrados, deleted = _run([1, 2, 3], [])
assert borrados == [] and deleted is None, (borrados, deleted)

# 4) Sin fantasmas: DB {1,2}, API {1,2,3} -> nada que borrar
borrados, deleted = _run([1, 2], [1, 2, 3])
assert borrados == [] and deleted is None, (borrados, deleted)

# 5) Sweep umbral_pct=1.0: DB {1,2,3,4}, API {1} -> borra 2,3,4 (guard off)
borrados, deleted = _run([1, 2, 3, 4], [1], umbral_pct=1.0)
assert sorted(borrados) == [2, 3, 4], (borrados, deleted)

print("OK — eliminar_vacaciones_ausentes: 5 casos pasados")
