"""
ETL diario de procesos de selección (reclutamiento).
API BUK Chile → rh.selection_processes (PostgreSQL vía túnel SSH).

Estrategia de carga: upsert por id_proceso (ON CONFLICT DO UPDATE).
"""

import time

import psycopg2.extras
import requests

from app.utils.logger import setup_logger

logger = setup_logger(__name__)

# =============================================================================
# DDL / SQL
# =============================================================================

DDL = """
CREATE TABLE IF NOT EXISTS rh.selection_processes (
    id_proceso          INTEGER      PRIMARY KEY,
    name_role           VARCHAR(255),
    area_id             INTEGER,
    company_id          INTEGER,
    workday             VARCHAR(255),
    hours_by_week       NUMERIC(5,2),
    article_22          BOOLEAN,
    status              VARCHAR(255),
    start_date          DATE,
    end_date            DATE,
    created_at          TIMESTAMPTZ,
    supervisor_id       INTEGER,
    recruiter_id        INTEGER,
    fecha_actualizacion TIMESTAMP    DEFAULT NOW()
);
"""

COLS = [
    "id_proceso",
    "name_role",
    "area_id",
    "company_id",
    "workday",
    "hours_by_week",
    "article_22",
    "status",
    "start_date",
    "end_date",
    "created_at",
    "supervisor_id",
    "recruiter_id",
]

BULK_UPSERT = """
INSERT INTO rh.selection_processes (
    id_proceso, name_role, area_id, company_id, workday, hours_by_week,
    article_22, status, start_date, end_date, created_at,
    supervisor_id, recruiter_id
) VALUES %s
ON CONFLICT (id_proceso) DO UPDATE SET
    name_role           = EXCLUDED.name_role,
    area_id             = EXCLUDED.area_id,
    company_id          = EXCLUDED.company_id,
    workday             = EXCLUDED.workday,
    hours_by_week       = EXCLUDED.hours_by_week,
    article_22          = EXCLUDED.article_22,
    status              = EXCLUDED.status,
    start_date          = EXCLUDED.start_date,
    end_date            = EXCLUDED.end_date,
    created_at          = EXCLUDED.created_at,
    supervisor_id       = EXCLUDED.supervisor_id,
    recruiter_id        = EXCLUDED.recruiter_id,
    fecha_actualizacion = NOW();
"""


# =============================================================================
# EXTRACCIÓN
# =============================================================================

def obtener_todos_los_procesos(token, url):
    headers = {"auth_token": token}
    procesos = []
    url_actual = url
    pagina = 1

    logger.info("Obteniendo procesos de selección desde BUK Chile...")

    while url_actual:
        logger.info(f"Página {pagina}...")
        try:
            resp = requests.get(url_actual, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            for proceso in data.get("data", []):
                procesos.append({
                    "id_proceso": proceso.get("id"),
                    "name_role": proceso.get("name"),
                    "area_id": (proceso.get("area") or {}).get("id"),
                    "company_id": (proceso.get("company") or {}).get("id"),
                    "workday": proceso.get("workday"),
                    "hours_by_week": proceso.get("hours_by_week"),
                    "article_22": proceso.get("article_22"),
                    "status": proceso.get("status"),
                    "start_date": proceso.get("start_date"),
                    "end_date": proceso.get("end_date"),
                    "created_at": proceso.get("created_at"),
                    "supervisor_id": (proceso.get("supervisor") or {}).get("id"),
                    "recruiter_id": (proceso.get("recruiter") or {}).get("id"),
                })

            logger.info(f"Página {pagina}: {len(data.get('data', []))} procesos")
            url_actual = data.get("pagination", {}).get("next")
            pagina += 1
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            logger.error(f"Error en petición API: {e}")
            break

    logger.info(f"Total procesos de selección obtenidos: {len(procesos)}")
    return procesos


# =============================================================================
# CARGA
# =============================================================================

def _crear_tabla(conn):
    with conn.cursor() as cursor:
        cursor.execute(DDL)
    conn.commit()
    logger.info("Tabla rh.selection_processes creada/verificada correctamente")


def _sincronizar_procesos(conn, procesos):
    template = "(" + ", ".join(["%s"] * len(COLS)) + ")"
    tuples = [tuple(p[c] for c in COLS) for p in procesos]

    with conn.cursor() as cursor:
        psycopg2.extras.execute_values(
            cursor, BULK_UPSERT, tuples, template=template, page_size=500
        )
    conn.commit()
    logger.info(
        f"Sincronización completada — {len(procesos)} procesos insertados/actualizados"
    )


# =============================================================================
# ORQUESTADOR
# =============================================================================

def ejecutar_flujo_seleccion(conn, token, url):
    """
    ETL completo de procesos de selección:
      1. Garantiza la tabla rh.selection_processes.
      2. Extrae procesos desde la API BUK (paginado).
      3. Upsert por id_proceso.

    Devuelve True si el flujo terminó sin errores fatales.
    """
    procesos = obtener_todos_los_procesos(token, url)

    if not procesos:
        logger.warning("La API no devolvió procesos de selección.")
        return False

    try:
        _crear_tabla(conn)
        _sincronizar_procesos(conn, procesos)
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error fatal sincronizando procesos de selección: {e}")
        raise
