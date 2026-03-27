import json
import os
from datetime import datetime

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR))
ARCHIVO_ESTADO = os.path.join(ROOT_DIR, "estado_sincronizacion.json")
LOGS_DIR = os.path.join(ROOT_DIR, "logs")

def garantizar_directorios():
    """Crea la carpeta de logs si no existe en la raíz."""
    if not os.path.exists(LOGS_DIR):
        try:
            os.makedirs(LOGS_DIR, exist_ok=True)
            print(f"Carpeta logs creada en: {LOGS_DIR}")
        except Exception as e:
            print(f"Error creando carpeta logs: {e}")

garantizar_directorios()

def cargar_estado():
    """Lee el archivo JSON. Si no existe, devuelve un diccionario vacío."""
    if not os.path.exists(ARCHIVO_ESTADO):
        return {}
    try:
        with open(ARCHIVO_ESTADO, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def guardar_estado(estado):
    """Guarda el diccionario de estado en el archivo JSON."""
    os.makedirs(os.path.dirname(ARCHIVO_ESTADO), exist_ok=True)
    # Escritura "in-place" (mismo archivo) para que editores/FS watchers refresquen bien.
    with open(ARCHIVO_ESTADO, "w", encoding="utf-8", newline="\n") as f:
        json.dump(estado, f, indent=4, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())

def tarea_ya_completada(nombre_tarea):
    """
    Verifica si una tarea se completó exitosamente HOY.
    Devuelve True si ya está lista, False si hay que ejecutarla.
    """
    estado = cargar_estado()
    info_tarea = estado.get(nombre_tarea)
    
    fecha_hoy = datetime.now().strftime('%Y-%m-%d')

    if info_tarea:
        # Verificamos si está OK y si la fecha coincide con hoy
        if info_tarea.get('status') == 'OK' and info_tarea.get('fecha') == fecha_hoy:
            return True
    
    return False

def marcar_tarea(nombre_tarea, exito=True, error_msg=None):
    """Actualiza el estado de una tarea."""
    estado = cargar_estado()
    fecha_hoy = datetime.now().strftime('%Y-%m-%d')
    
    estado[nombre_tarea] = {
        "fecha": fecha_hoy,
        "status": "OK" if exito else "ERROR",
        "ultimo_error": error_msg if not exito else None,
        "updated_at": datetime.now().strftime('%H:%M:%S')
    }
    
    guardar_estado(estado)