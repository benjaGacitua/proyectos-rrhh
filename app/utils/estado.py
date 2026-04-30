import json
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

STAGING_DIR = os.getenv("STAGING_DIR", "/app/staging")
ARCHIVO_ESTADO = os.path.join(STAGING_DIR, "estado_sincronizacion.json")


def _cargar_estado() -> dict:
    if not os.path.exists(ARCHIVO_ESTADO):
        return {}
    try:
        with open(ARCHIVO_ESTADO, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error leyendo estado_sincronizacion.json: {e}")
        return {}


def _guardar_estado(estado: dict):
    os.makedirs(STAGING_DIR, exist_ok=True)
    try:
        with open(ARCHIVO_ESTADO, "w", encoding="utf-8", newline="\n") as f:
            json.dump(estado, f, indent=4, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
    except Exception as e:
        logger.error(f"Error guardando estado_sincronizacion.json: {e}")


def tarea_ya_completada(nombre_tarea: str) -> bool:
    """Returns True if the task completed successfully today."""
    estado = _cargar_estado()
    info = estado.get(nombre_tarea)
    if info and info.get("status") == "OK" and info.get("fecha") == datetime.now().strftime("%Y-%m-%d"):
        return True
    return False


def tarea_ya_completada_este_mes(nombre_tarea: str) -> bool:
    """Returns True if the task completed successfully at any point this calendar month."""
    estado = _cargar_estado()
    info = estado.get(nombre_tarea)
    if info and info.get("status") == "OK":
        try:
            fecha_tarea = datetime.strptime(info["fecha"], "%Y-%m-%d")
            hoy = datetime.now()
            return fecha_tarea.year == hoy.year and fecha_tarea.month == hoy.month
        except (ValueError, KeyError):
            return False
    return False


def marcar_tarea(nombre_tarea: str, exito: bool = True, error_msg: str = None):
    """Updates the status of a task in estado_sincronizacion.json."""
    estado = _cargar_estado()
    estado[nombre_tarea] = {
        "fecha": datetime.now().strftime("%Y-%m-%d"),
        "status": "OK" if exito else "ERROR",
        "ultimo_error": error_msg if not exito else None,
        "updated_at": datetime.now().strftime("%H:%M:%S"),
    }
    _guardar_estado(estado)
    if exito:
        logger.info(f"Tarea '{nombre_tarea}' marcada como OK.")
    else:
        logger.warning(f"Tarea '{nombre_tarea}' marcada como ERROR: {error_msg}")
