"""
Script de carga histórica única para rh.historical_settlements.

Ejecuta ejecutar_carga_historica() que itera TODOS los períodos desde
start_year/start_month hasta el mes actual, eliminando e insertando
los datos de cada período (DELETE + INSERT).

Uso (desde actualizacion-tablas-2/):
    python scripts/carga_historica_settlements.py

O dentro del contenedor Airflow/ETL:
    docker exec -it <container_name> python scripts/carga_historica_settlements.py
"""

import sys
import os

# Asegura que app.* sea importable al correr como script directo
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings
from app.utils.logger import setup_logger
from app.utils.db_client import get_db_connection
from app.etl.settlements_chile import ejecutar_carga_historica

logger = setup_logger(__name__)

START_YEAR  = 2019
START_MONTH = 1


def main():
    if not settings.URL_SETTLEMENTS_CHILE:
        logger.error("URL_SETTLEMENTS_CHILE no está configurada en .env — abortando.")
        sys.exit(1)

    logger.info("=== INICIO CARGA HISTÓRICA SETTLEMENTS ===")
    conn = get_db_connection()
    try:
        resultado = ejecutar_carga_historica(
            conn,
            settings.TOKEN,
            settings.URL_SETTLEMENTS_CHILE,
            start_year=START_YEAR,
            start_month=START_MONTH,
        )
    finally:
        conn.close()

    logger.info(
        f"=== FIN CARGA HISTÓRICA ==="
        f" OK: {resultado['periodos_ok']},"
        f" Sin datos: {resultado['periodos_vacios']},"
        f" Errores: {resultado['periodos_error']}"
    )

    if resultado["periodos_error"] > 0:
        logger.error("Hubo errores en algunos períodos — revisar logs.")
        sys.exit(1)


if __name__ == "__main__":
    main()
