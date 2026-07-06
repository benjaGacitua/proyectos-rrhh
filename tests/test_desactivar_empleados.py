"""Test mínimo de reconciliación de empleados borrados (status='eliminado').

Ejecutar: python -m tests.test_desactivar_empleados   (desde la raíz del repo)
"""
import os
import sys

# Sin webhook → la notificación Telegram sale temprano, no toca la red.
os.environ.pop("N8N_WEBHOOK_URL", None)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.etl.load import desactivar_empleados_ausentes, _normalizar_rut


class FakeCursor:
    def __init__(self, filas):
        self._filas = filas
        self.marcados = None

    def execute(self, sql, params=None):
        if sql.strip().upper().startswith("SELECT"):
            self._result = list(self._filas)
        elif sql.strip().upper().startswith("UPDATE"):
            self.marcados = list(params[0])

    def fetchall(self):
        return self._result

    def close(self):
        pass


class FakeConn:
    def __init__(self, filas):
        self.cursor_obj = FakeCursor(filas)
        self.committed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def rollback(self):
        pass


def test_ausente_por_id_y_rut_se_marca():
    # DB: 5 no-eliminados. API trae 4. Solo id=5 ausente (1/5=20% < umbral) → eliminado.
    filas = [(i, f"{i}.111.111-1") for i in range(1, 6)]
    conn = FakeConn(filas)
    api = [{"id": i, "rut": f"{i}111111-1"} for i in range(1, 5)]
    marcados = desactivar_empleados_ausentes(conn, api)
    assert marcados == [(5, "5.111.111-1")], marcados
    assert conn.cursor_obj.marcados == [5], conn.cursor_obj.marcados


def test_presente_por_rut_con_id_distinto_no_se_toca():
    # id cambió (recontratación) pero mismo rut, en formato distinto → sigue presente.
    filas = [(100, "12.345.678-9")]
    conn = FakeConn(filas)
    api = [{"id": 999, "rut": "12345678-9"}]  # id distinto, rut igual sin puntos
    marcados = desactivar_empleados_ausentes(conn, api)
    assert marcados == [], marcados
    assert conn.cursor_obj.marcados is None


def test_guard_umbral_no_desactiva():
    # 10 filas, API solo trae 1 → 9 candidatos (90%) supera umbral 30% → no toca nada.
    filas = [(i, f"{i}-0") for i in range(1, 11)]
    conn = FakeConn(filas)
    api = [{"id": 1, "rut": "1-0"}]
    marcados = desactivar_empleados_ausentes(conn, api)
    assert marcados == [], marcados
    assert conn.cursor_obj.marcados is None


def test_guard_api_vacia():
    conn = FakeConn([(1, "1-1")])
    assert desactivar_empleados_ausentes(conn, []) == []
    assert conn.cursor_obj.marcados is None


def test_normalizar_rut():
    assert _normalizar_rut("12.345.678-9") == "12345678-9"
    assert _normalizar_rut(" 12345678-9 ") == "12345678-9"
    assert _normalizar_rut("-") is None
    assert _normalizar_rut("") is None
    assert _normalizar_rut(None) is None


if __name__ == "__main__":
    test_ausente_por_id_y_rut_se_marca()
    test_presente_por_rut_con_id_distinto_no_se_toca()
    test_guard_umbral_no_desactiva()
    test_guard_api_vacia()
    test_normalizar_rut()
    print("OK: todos los asserts pasaron.")
