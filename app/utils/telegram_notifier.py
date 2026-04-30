"""
Envía notificaciones al webhook n8n que las reenvía a Telegram.

Requiere la variable de entorno N8N_WEBHOOK_URL apuntando al endpoint
configurado en el workflow 'Airflow ETL Logs a Telegram'.
"""
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import logging
import os
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

_TIMEOUT = 10  # segundos


def _webhook_url() -> str | None:
    try:
        from app.config import settings
        url = getattr(settings, "N8N_WEBHOOK_URL", None)
    except Exception:
        url = None
    return url or os.getenv("N8N_WEBHOOK_URL")


def notificar_tarea(
    *,
    state: str,
    dag_id: str,
    task_id: str,
    run_id: str,
    try_number=None,
    ts: str | None = None,
    log_excerpt: str = "",
    log_url: str = "",
) -> None:
    """
    Envía un evento de tarea al webhook n8n.

    Parámetros mapeados al payload que espera el nodo 'Preparar Mensaje':
        state        → payload.state
        dag_id       → payload.dag_id
        task_id      → payload.task_id
        run_id       → payload.run_id
        try_number   → payload.try_number
        ts           → payload.ts
        log_excerpt  → payload.log_excerpt
        log_url      → payload.log_url
    """
    url = _webhook_url()
    if not url:
        logger.warning("N8N_WEBHOOK_URL no configurado — notificación Telegram omitida.")
        return

    payload = {
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "state": state,
        "try_number": try_number,
        "ts": ts or datetime.now().isoformat(),
        "log_excerpt": log_excerpt,
        "log_url": log_url,
    }

    try:
        resp = requests.post(url, json=payload, timeout=_TIMEOUT, verify=False)
        resp.raise_for_status()
        logger.debug(f"Notificación enviada a n8n — task={task_id} state={state}")
    except Exception as exc:
        # Nunca debe bloquear la ejecución del DAG
        logger.warning(f"No se pudo enviar notificación a n8n: {exc}")
