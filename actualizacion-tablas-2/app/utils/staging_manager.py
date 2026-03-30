import json
import logging
import os

logger = logging.getLogger(__name__)

CARPETA_TEMP = os.getenv("STAGING_DIR", "/app/staging")


def _garantizar_directorio():
    os.makedirs(CARPETA_TEMP, exist_ok=True)


def guardar_temporal(nombre_entidad, datos):
    _garantizar_directorio()
    ruta = os.path.join(CARPETA_TEMP, f"staging_{nombre_entidad}.json")
    try:
        with open(ruta, "w", encoding="utf-8") as f:
            json.dump(datos, f, ensure_ascii=False, indent=4, default=str)
        logger.info(f"[{nombre_entidad.upper()}] Staging guardado ({len(datos)} registros).")
        return True
    except Exception as e:
        logger.error(f"[{nombre_entidad.upper()}] Error guardando staging: {e}")
        return False


def cargar_temporal(nombre_entidad):
    ruta = os.path.join(CARPETA_TEMP, f"staging_{nombre_entidad}.json")
    if not os.path.exists(ruta):
        return None
    try:
        with open(ruta, "r", encoding="utf-8") as f:
            datos = json.load(f)
        logger.info(f"[{nombre_entidad.upper()}] Staging cargado desde caché ({len(datos)} registros).")
        return datos
    except Exception as e:
        logger.error(f"[{nombre_entidad.upper()}] Archivo de staging corrupto: {e}")
        return None


def eliminar_temporal(nombre_entidad):
    ruta = os.path.join(CARPETA_TEMP, f"staging_{nombre_entidad}.json")
    if not os.path.exists(ruta):
        return
    try:
        os.remove(ruta)
        logger.info(f"[{nombre_entidad.upper()}] Staging eliminado tras carga exitosa.")
    except Exception as e:
        logger.error(f"[{nombre_entidad.upper()}] No se pudo eliminar staging: {e}")
