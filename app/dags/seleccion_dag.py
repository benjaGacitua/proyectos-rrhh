"""
DAG: etl_seleccion_cramer
Actualiza procesos de selección (reclutamiento): BUK API → PostgreSQL (vía túnel SSH).

Corre de lunes a viernes a las 07:30. Independiente del DAG etl_rh_cramer
(que corre a las 02:00) porque tiene una cadencia distinta.

Todas las importaciones del dominio (app.*) se realizan dentro del callable
para evitar ejecuciones en tiempo de parseo del DAG.

Notificaciones Telegram:
    - on_failure_callback → alerta inmediata vía n8n (requiere N8N_WEBHOOK_URL).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def _callback_fallo(context):
    """Envía alerta inmediata a Telegram cuando la tarea falla."""
    from app.utils.telegram_notifier import notificar_tarea

    ti = context["task_instance"]
    exc = context.get("exception")
    log_excerpt = f"{type(exc).__name__}: {exc}" if exc else "Sin detalle de excepción"

    notificar_tarea(
        state="failed",
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=context.get("run_id", "desconocido"),
        try_number=ti.try_number,
        ts=context.get("ts", ""),
        log_excerpt=log_excerpt,
    )


default_args = {
    "owner": "rh_cramer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "on_failure_callback": _callback_fallo,
}


def _etl_seleccion():
    from app.config import settings
    from app.utils.db_client import get_db_connection
    from app.etl.seleccion import ejecutar_flujo_seleccion

    if not settings.URL_SELECCION:
        raise ValueError("URL_SELECCION no está configurada en .env")

    conn = get_db_connection()
    try:
        exito = ejecutar_flujo_seleccion(conn, settings.TOKEN, settings.URL_SELECCION)
        if not exito:
            raise RuntimeError("ETL selección terminó sin datos — revisar logs.")
    finally:
        conn.close()


with DAG(
    dag_id="etl_seleccion_cramer",
    default_args=default_args,
    description="ETL diario de procesos de selección: BUK API → PostgreSQL vía túnel SSH",
    schedule="30 7 * * 1-5",   # Lunes a viernes a las 07:30
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["rh", "etl", "seleccion"],
) as dag:

    PythonOperator(
        task_id="etl_seleccion",
        python_callable=_etl_seleccion,
    )
