"""Barrido completo one-shot de registros fantasma (incidencias + vacaciones).

Trae TODO el endpoint (sin ventana de fechas) y borra las filas que existen en
la DB pero ya no en el origen BUK. Pensado para correr UNA vez y limpiar el
histórico; el DAG diario luego mantiene la limpieza con su ventana normal.

Uso:
    python -m app.scripts.sweep_ghosts            # dry-run: solo reporta cuántos borraría
    python -m app.scripts.sweep_ghosts --apply    # ejecuta el borrado

En dry-run no toca la DB. El guard del 30% se desactiva en --apply (umbral_pct=1.0)
porque un barrido histórico sí espera borrar muchas filas.
"""
import sys

from app.config import settings
from app.utils.db_client import get_db_connection
from app.etl import extract, load


def _fetch_incidencias():
    datos = []
    for nombre, url in settings.API_ENDPOINTS.items():
        datos.extend(extract.obtener_datos_paginados(url, aplicar_filtro_fechas=False, nombre_endpoint=nombre))
    return datos


def _fetch_vacaciones():
    # dias_atras grande => end_after ~10 años => universo completo
    return extract.obtener_datos_vacaciones(settings.API_BASE_URL, dias_atras=3650)


def _contar_fantasmas(conn, tabla, ids_api):
    with conn.cursor() as cur:
        cur.execute(f"SELECT id FROM {tabla}")
        ids_db = {row[0] for row in cur.fetchall()}
    return len(ids_db - ids_api), len(ids_db)


def main(apply: bool):
    conn = get_db_connection()
    try:
        inc = _fetch_incidencias()
        vac = _fetch_vacaciones()
        ids_inc = {int(i["id"]) for i in inc if i.get("id") is not None}
        ids_vac = {int(v["id"]) for v in vac if v.get("id") is not None}

        g_inc, t_inc = _contar_fantasmas(conn, "rh.consolidado_incidencias", ids_inc)
        g_vac, t_vac = _contar_fantasmas(conn, "rh.vacations", ids_vac)

        print(f"Incidencias: API={len(ids_inc)} DB={t_inc} fantasmas={g_inc}")
        print(f"Vacaciones : API={len(ids_vac)} DB={t_vac} fantasmas={g_vac}")

        if not apply:
            print("\nDRY-RUN — nada borrado. Corre con --apply para ejecutar.")
            return

        # cutoff/fecha None = universo completo; umbral 1.0 = sin guard (sweep esperado grande)
        borrados_inc = load.eliminar_incidencias_ausentes(conn, inc, fecha_inicio=None, fecha_fin=None, umbral_pct=1.0)
        borrados_vac = load.eliminar_vacaciones_ausentes(conn, vac, cutoff_end_date=None, umbral_pct=1.0)
        print(f"\nBorradas: incidencias={len(borrados_inc)} vacaciones={len(borrados_vac)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
