import requests
import time
from pathlib import Path
from app.utils.logger import setup_logger
from app.config import settings

logger = setup_logger(__name__)

#! /// Llamada API tablas de incidencias ///
def construir_url_con_fechas(base_url: str, fecha_inicio: str = None, fecha_fin: str = None):
    if fecha_inicio and fecha_fin:
        separador = "&" if "?" in base_url else "?"
        return f"{base_url}{separador}from={fecha_inicio}&to={fecha_fin}"
    return base_url

def obtener_datos_paginados(endpoint_url: str, aplicar_filtro_fechas: bool = False, fecha_inicio: str = None, fecha_fin: str = None):
    headers = {"auth_token": settings.TOKEN}
    todos_los_datos = []
    
    url_actual = endpoint_url
    if aplicar_filtro_fechas and fecha_inicio and fecha_fin:
        url_actual = construir_url_con_fechas(endpoint_url, fecha_inicio, fecha_fin)
        logger.info(f"Comenzando extracción con filtro: {url_actual}")
    else:
        logger.info(f"Comenzando extracción completa: {endpoint_url}")
    
    pagina_actual = 1

    while url_actual:
        logger.info(f"Obteniendo página {pagina_actual}...")
        try:
            respuesta = requests.get(url_actual, headers=headers, timeout=10)
            respuesta.raise_for_status()

            respuesta_api = respuesta.json()
            datos_pagina = respuesta_api.get('data', [])
            pagination_info = respuesta_api.get('pagination', {})

            todos_los_datos.extend(datos_pagina)
            
            # Info de progreso
            total_pages = pagination_info.get('total_pages', 1)
            
            url_actual = pagination_info.get('next')
            pagina_actual += 1
            time.sleep(0.5) # Respetar rate limits

        except requests.exceptions.RequestException as e:
            logger.error(f"Error en página {pagina_actual}: {e}")
            break

    logger.info(f"Extracción finalizada. Total registros: {len(todos_los_datos)}")
    return todos_los_datos
#! \\\ ======================================================================================= \\\

#! /// Llamada API employees ///
def to_int_or_none(value):
    """Convierte un valor a int, o devuelve None si no es válido."""
    if value is None:
        return None
    try:
        # Intentar convertir a float primero (para manejar '100.0') y luego a int
        return int(float(value))
    except (ValueError, TypeError):
        # Si falla (ej. es '', 'N/A', '-'), devuelve None
        return None

def to_str_or_none(value, max_length=None):
    """Convierte un valor a str, o devuelve None si está vacío, nulo o es '-'.
    Si max_length está definido, trunca el string a ese tamaño."""
    if value is None:
        return None
    try:
        s_val = str(value).strip()
        if not s_val or s_val == '-':
            return None
        # Truncar si se especifica max_length
        if max_length and len(s_val) > max_length:
            logger.warning(f"Valor truncado de {len(s_val)} a {max_length} caracteres: {s_val[:20]}...")
            return s_val[:max_length]
        return s_val
    except (ValueError, TypeError):
        return None

def obtener_todos_los_empleados_filtrados(url_employees: str = settings.API_BASE_URL):
    """Obtiene y filtra los datos de empleados desde la API externa."""
    headers = {"auth_token": settings.TOKEN}
    empleados_filtrados = []
    url_actual = url_employees + "employees"
    pagina_actual = 1

    logger.info(f"DEBUG - TOKEN cargado: {settings.TOKEN[0] if settings.TOKEN else 'VACÍO'}...")
    logger.info(f"DEBUG - URL cargada: '{url_actual}'")

    if not url_actual:
        logger.error("ERROR: URL_EMPLOYEES está vacía!")
        return []

    logger.info("Comenzando la obtención de todos los empleados con paginación...")
    
    #*================================================================================#
    #*Implementar extracción segmentada para reducir el tiempo de ejecución del script
    #*================================================================================#

    while url_actual:
        logger.info(f"Obteniendo página {pagina_actual}...")
        try:
            respuesta = requests.get(url_actual, headers=headers)
            respuesta.raise_for_status()
            respuesta_api = respuesta.json()
            empleados_pagina = respuesta_api['data']
            pagination_info = respuesta_api['pagination']

            for empleado_completo in empleados_pagina:
                raw_first_name = empleado_completo.get("first_name")
                first_name_filtered = raw_first_name.split()[0] if raw_first_name else None

                empleado_filtrado = {
                    "person_id": empleado_completo.get("person_id"),
                    "id": empleado_completo.get("id"),
                    "full_name": empleado_completo.get("full_name"),
                    "first_name": first_name_filtered,
                    "last_name": empleado_completo.get("surname"),
                    "rut": empleado_completo.get("rut"),
                    "picture_url": empleado_completo.get("picture_url"),
                    "email": empleado_completo.get("email"),
                    "personal_email": empleado_completo.get("personal_email"),
                    "address": empleado_completo.get("address"),
                    "street": empleado_completo.get("street"),
                    "street_number": empleado_completo.get("street_number"),
                    "city": empleado_completo.get("city"),
                    "province": empleado_completo.get("province"),
                    "district": empleado_completo.get("district"),
                    "region": empleado_completo.get("region"),
                    "phone": empleado_completo.get("phone"),
                    "gender": empleado_completo.get("gender"),
                    "birthday": empleado_completo.get("birthday"),
                    "university": empleado_completo.get("university"),
                    "degree": empleado_completo.get("degree"),
                    "bank": empleado_completo.get("bank"),
                    "account_type": empleado_completo.get("account_type"),
                    "account_number": to_str_or_none(empleado_completo.get("account_number")),
                    "nationality": empleado_completo.get("nationality"),
                    "civil_status": empleado_completo.get("civil_status"),
                    "health_company": empleado_completo.get("health_company"),
                    "pension_regime": empleado_completo.get("pension_regime"),
                    "pension_fund": empleado_completo.get("pension_fund"),
                    "active_until": empleado_completo.get("active_until"),
                    "afc": empleado_completo.get("afc"),
                    "retired": empleado_completo.get("retired"),
                    "retirement_regime": empleado_completo.get("retirement_regime"),
                    "active_since": empleado_completo.get("active_since"),
                    "status": empleado_completo.get("status"),
                    "start_date": empleado_completo.get("current_job", {}).get("start_date"),
                    "end_date": empleado_completo.get("current_job", {}).get("end_date"),
                    "termination_reason": empleado_completo.get("termination_reason"),
                    "payment_method": empleado_completo.get("payment_method"),
                    "id_boss": empleado_completo.get("current_job", {}).get("boss", {}).get("id"),
                    "rut_boss": empleado_completo.get("current_job", {}).get("boss", {}).get("rut"),
                    "base_wage": to_int_or_none(empleado_completo.get("current_job", {}).get("base_wage",)),
                    "contract_type": empleado_completo.get("current_job", {}).get("contract_type"),
                    "contract_finishing_date_1": empleado_completo.get("current_job", {}).get("contract_finishing_date_1"),
                    "contract_finishing_date_2": empleado_completo.get("current_job", {}).get("contract_finishing_date_2"),
                    'name_role': empleado_completo.get("current_job", {}).get("role", {}).get("name"),
                    'area_id': empleado_completo.get("current_job", {}).get("area_id"),
                    "cost_center": empleado_completo.get("current_job", {}).get("cost_center"),
                    "ctrlit_recinto": empleado_completo.get("current_job", {}).get("custom_attributes", {}).get("ctrlit_recinto"),
                }
                empleados_filtrados.append(empleado_filtrado)

            logger.info(f"Página {pagina_actual}: {len(empleados_pagina)} empleados procesados.")
            logger.info(f"Total acumulado: {len(empleados_filtrados)} empleados filtrados.")

            url_actual = pagination_info.get('next')
            pagina_actual += 1
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            logger.error(f"Error en la petición API: {e}")
            break
        except Exception as api_ex:
            logger.exception(f"Error procesando datos de la API: {api_ex}")
            break # Detener si hay un error inesperado al procesar la respuesta

    logger.info(f"¡Paginación completada! Total de empleados filtrados: {len(empleados_filtrados)}")
    return empleados_filtrados
#! \\\ ======================================================================================= \\\

#! /// Extracción Actualización Tabla Incidencias ///
#? La función que ejecuta se traspasó a upload.py
def queries_de_incidencias():

    BASE_DIR = Path(__file__).resolve().parent.parent
    PATH_CREATE = BASE_DIR / "sql" / "Crear tabla incidencias.sql"
    PATH_MERGE = BASE_DIR  / "sql" / "Actualización de tabla incidencias consolidada SQL Server.sql"

    try:
        print(f"Leyendo script desde: {PATH_CREATE}")
        
        if not PATH_CREATE.exists():
            raise FileNotFoundError(f"No se encontró el archivo: {PATH_CREATE}")

        sql_create = PATH_CREATE.read_text(encoding='utf-8') 

        sql_merge = PATH_MERGE.read_text(encoding='utf-8')

        return sql_create, sql_merge

    except Exception as e:
        print(f"Error CRÍTICO durante la ejecución SQL: {e}")
        raise e 
#! \\\ ======================================================================================= \\\

#! /// Extracción Actualización de Áreas ///
def obtener_datos_tabla_areas(url_areas: str = settings.API_BASE_URL):
    """
    Obtiene todas las áreas desde la API con paginación y las devuelve filtradas.
    """
    headers = {"auth_token": settings.TOKEN}
    areas_filtradas = []
    url_actual = url_areas + "organization/areas/?status=both"
    pagina_actual = 1
    
    print(f"Comenzando obtención de áreas: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        while url_actual:
            print(f"Procesando página {pagina_actual}...")
            
            respuesta = requests.get(url_actual, headers=headers, timeout=30)
            respuesta.raise_for_status()
            
            respuesta_api = respuesta.json()
            areas_pagina = respuesta_api['data']
            pagination_info = respuesta_api['pagination']
            
            # Filtrar cada área de esta página
            for area_completa in areas_pagina:
                area_filtrada = {
                    "id": area_completa.get("id"),
                    "name": area_completa.get("name"),
                    "address": area_completa.get("address"),
                    "first_level_id": area_completa.get("first_level_id"),
                    "first_level_name": area_completa.get("first_level_name"),
                    "second_level_id": area_completa.get("second_level_id"),
                    "second_level_name": area_completa.get("second_level_name"),
                    "cost_center": area_completa.get("cost_center"),
                    "status": area_completa.get("status"),
                    "city": area_completa.get("city"),
                }
                areas_filtradas.append(area_filtrada)
            
            print(f"Página {pagina_actual}: {len(areas_pagina)} áreas procesadas")
            
            # Obtener la URL de la siguiente página
            url_actual = pagination_info.get('next')
            pagina_actual += 1
            
            # Pausa para no sobrecargar la API
            time.sleep(0.5)
            
    except Exception as e:
        print(f"Error crítico en extracción API: {e}")
        return [] # Retornamos lista vacía en caso de error para no romper el flujo
        
    print(f"[{time.strftime('%H:%M:%S')}] Extracción finalizada. Total áreas: {len(areas_filtradas)}")
    return areas_filtradas
#! \\\ ======================================================================================= \\\

#! /// Extracción Vacaciones ///
def obtener_datos_vacaciones(url_vacaciones: str = settings.API_BASE_URL):

    headers = {"auth_token": settings.TOKEN}
    url_actual = url_vacaciones + "vacations"
    vacaciones_obtenidas = []
    pagina_actual = 1
    
    print(f"Comenzando obtención de vacaciones: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        while url_actual:
            print(f"Procesando página {pagina_actual}...")
            
            respuesta = requests.get(url_actual, headers=headers, timeout=30)
            respuesta.raise_for_status()
            
            respuesta_api = respuesta.json()
            vacaciones_pagina = respuesta_api['data']
            pagination_info = respuesta_api['pagination']

            for vacaciones in vacaciones_pagina:
                vacaciones_filtradas = {
                    "id": vacaciones.get("id"),
                    "employee_id": vacaciones.get("employee_id"),
                    "approved_by_id": vacaciones.get("approved_by_id"),
                    "working_days": vacaciones.get("working_days"),
                    "calendar_days": vacaciones.get("calendar_days"),
                    "workday_stage": vacaciones.get("workday_stage"),
                    "start_date": vacaciones.get("start_date"),
                    "end_date": vacaciones.get("end_date"),
                    "requested_at": vacaciones.get("requested_at"),
                    "approved_at": vacaciones.get("approved_at"),
                    "type": vacaciones.get("type"),
                    "status": vacaciones.get("status"),
                    "vacation_type_id": vacaciones.get("vacation_type_id")
                    }
                vacaciones_obtenidas.append(vacaciones_filtradas)            
            
            print(f"Página {pagina_actual}: {len(vacaciones_pagina)} vacaciones procesadas")
            
            # Obtener la URL de la siguiente página
            url_actual = pagination_info.get('next')
            pagina_actual += 1
            
            # Pausa para no sobrecargar la API
            time.sleep(0.5)
        
    except Exception as e:
        print(f"Error crítico en extracción API: {e}")
        return [] # Retornamos lista vacía en caso de error para no romper el flujo
        
    print(f"[{time.strftime('%H:%M:%S')}] Extracción finalizada. Total áreas: {len(vacaciones_filtradas)}")
    return vacaciones_obtenidas



