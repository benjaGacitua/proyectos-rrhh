"""
Carga procesos de selección de BUK en PostgreSQL vía túnel SSH.
"""

import logging
import os
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

# ─── CONFIG ───────────────────────────────────────────────────────────────────
load_dotenv()

TOKEN = os.getenv("TOKEN")
URL_SELECCION = os.getenv("URL_SELECCION")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

Path(__file__).resolve().parent.parent.joinpath("logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            Path(__file__).resolve().parent.parent / "logs" / "log_seleccion.log",
            encoding="utf-8",
        ),
    ],
)

logger = logging.getLogger(__name__)

# ─── DDL ──────────────────────────────────────────────────────────────────────

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

COLS_SELECTION_PROCESSES = [
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
    id_proceso,
    name_role,
    area_id,
    company_id,
    workday,
    hours_by_week,
    article_22,
    status,
    start_date,
    end_date,
    created_at,
    supervisor_id,
    recruiter_id
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

# ─── API ──────────────────────────────────────────────────────────────────────

def obtener_todos_los_procesos():
    headers = {"auth_token": TOKEN}
    procesos = []
    url_actual = URL_SELECCION
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
                    "area_id": proceso.get("area", {}).get("id"),
                    "company_id": proceso.get("company", {}).get("id"),
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

# ─── DB ───────────────────────────────────────────────────────────────────────

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        options="-c search_path=rh",
    )


def crear_tabla(conn):
    cursor = conn.cursor()

    try:
        cursor.execute(DDL)
        conn.commit()
        logger.info("Tabla rh.selection_processes creada/verificada correctamente")
    finally:
        cursor.close()


def sincronizar_procesos(conn, procesos):
    cursor = conn.cursor()

    try:
        template = "(" + ", ".join(["%s"] * len(COLS_SELECTION_PROCESSES)) + ")"

        tuples = [
            tuple(proceso[col] for col in COLS_SELECTION_PROCESSES)
            for proceso in procesos
        ]

        psycopg2.extras.execute_values(
            cursor,
            BULK_UPSERT,
            tuples,
            template=template,
            page_size=500,
        )

        conn.commit()
        logger.info(
            f"Sincronización completada — {len(procesos)} procesos insertados/actualizados"
        )

    except Exception as e:
        conn.rollback()
        logger.error(f"Error fatal sincronizando procesos: {e}")
        raise

    finally:
        cursor.close()

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    if not TOKEN:
        logger.error("TOKEN no está en el .env")
        sys.exit(1)

    if not URL_SELECCION:
        logger.error("URL_SELECCION no está en el .env")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("CARGA PROCESOS DE SELECCIÓN BUK — vía túnel SSH")
    logger.info(f"Conexión: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    logger.info("=" * 60)

    procesos = obtener_todos_los_procesos()

    if not procesos:
        logger.warning("No se obtuvieron procesos de selección desde la API")
        sys.exit(0)

    conn = get_connection()

    try:
        crear_tabla(conn)
        sincronizar_procesos(conn, procesos)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
