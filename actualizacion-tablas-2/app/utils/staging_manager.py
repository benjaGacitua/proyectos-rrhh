import json
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CARPETA_TEMP = os.path.join(BASE_DIR, "temp")

def obtener_ruta_archivo(nombre_entidad):
    """
    Genera la ruta completa: temp/staging_vacaciones.json
    Y se asegura de que la carpeta exista.
    """
    if not os.path.exists(CARPETA_TEMP):
        os.makedirs(CARPETA_TEMP)
        print(f"Carpeta '{CARPETA_TEMP}' creada.")

    nombre_archivo = f"staging_{nombre_entidad}.json"
    
    return os.path.join(CARPETA_TEMP, nombre_archivo)

def guardar_temporal(nombre_entidad, datos):
    ruta_completa = obtener_ruta_archivo(nombre_entidad)
    
    try:
        with open(ruta_completa, 'w', encoding='utf-8') as f:
            json.dump(datos, f, ensure_ascii=False, indent=4, default=str)
        print(f"[{nombre_entidad.upper()}] Datos guardados en '{ruta_completa}'.")
        return True
    except Exception as e:
        print(f"Error guardando staging para {nombre_entidad}: {e}")
        return False

def cargar_temporal(nombre_entidad):
    ruta_completa = os.path.join(CARPETA_TEMP, f"staging_{nombre_entidad}.json")
    if os.path.exists(ruta_completa):
        try:
            with open(ruta_completa, 'r', encoding='utf-8') as f:
                datos = json.load(f)
            print(f"[{nombre_entidad.upper()}] Recuperado desde caché local ({len(datos)} registros).")
            return datos
        except Exception as e:
            print(f"Archivo corrupto para {nombre_entidad}: {e}")
    return None

def eliminar_temporal(nombre_entidad):
    ruta_completa = os.path.join(CARPETA_TEMP, f"staging_{nombre_entidad}.json")
    if os.path.exists(ruta_completa):
        try:
            os.remove(ruta_completa)
            print(f"[{nombre_entidad.upper()}] Archivo temporal eliminado.")
        except Exception as e:
            print(f"No se pudo eliminar archivo de {nombre_entidad}: {e}")