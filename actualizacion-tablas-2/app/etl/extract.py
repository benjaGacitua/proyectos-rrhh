import requests
import time
from pathlib import Path
from datetime import datetime
from app.utils.logger import setup_logger
from app.config import settings
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


logger = setup_logger(__name__)

# /// Funciones auxiliares ///
def construir_url_con_fechas(base_url: str, fecha_inicio: str = None, fecha_fin: str = None):
    if fecha_inicio and fecha_fin:
        separador = "&" if "?" in base_url else "?"
        return f"{base_url}{separador}from={fecha_inicio}&to={fecha_fin}"
    return base_url

# /// Funciones auxiliares ///
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

# /// Funciones auxiliares ///
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

# /// Funciones auxiliares ///
def _to_date_str(value):
    if value is None:
        return None
    try:
        s_val = str(value).strip()
        if not s_val:
            return None
        return s_val[:10]
    except Exception:
        return None

# /// Funciones auxiliares ///
def _to_datetime_str(value):
    if value is None:
        return None
    try:
        s_val = str(value).strip()
        if not s_val:
            return None
        return s_val.replace("T", " ").replace("Z", "")
    except Exception:
        return None

# /// Funciones auxiliares ///
def _calcular_dias_count(start_date, end_date):
    if not start_date or not end_date:
        return None
    try:
        d_inicio = datetime.strptime(start_date, "%Y-%m-%d").date()
        d_fin = datetime.strptime(end_date, "%Y-%m-%d").date()
        return (d_fin - d_inicio).days + 1
    except Exception:
        return None

# /// Funciones auxiliares ///
def normalizar_incidencia(registro: dict, nombre_endpoint: str = ""):
    endpoint = (nombre_endpoint or "").lower()

    id_evento = to_int_or_none(registro.get("id"))
    employee_id = to_int_or_none(registro.get("employee_id"))
    start_date = _to_date_str(registro.get("start_date"))
    end_date = _to_date_str(registro.get("end_date"))
    created_at = _to_datetime_str(
        registro.get("created_at")
        or registro.get("requested_at")
        or registro.get("approved_at")
    )
    status = to_str_or_none(registro.get("status"), 50)
    day_percent = to_str_or_none(registro.get("day_percent"), 5)

    if endpoint == "permissions":
        tipo_permiso = to_str_or_none(registro.get("permission_type_code"), 100)
    elif endpoint == "licences":
        tipo_permiso = to_str_or_none(registro.get("licence_type"), 100)
    elif endpoint == "absences":
        tipo_permiso = to_str_or_none(registro.get("absence_type_code"), 100)
    else:
        tipo_permiso = to_str_or_none(
            registro.get("type_permission")
            or registro.get("permission_type_code")
            or registro.get("licence_type")
            or registro.get("absence_type_code"),
            100,
        )

    days_count = to_int_or_none(registro.get("days_count"))
    if days_count is None:
        days_count = _calcular_dias_count(start_date, end_date)

    if not id_evento or not employee_id:
        return None

    return {
        "id": id_evento,
        "employee_id": employee_id,
        "days_count": days_count,
        "day_percent": day_percent,
        "type_permission": tipo_permiso,
        "start_date": start_date,
        "end_date": end_date,
        "status": status,
        "created_at": created_at,
    }

def obtener_datos_paginados(endpoint_url: str, aplicar_filtro_fechas: bool = False, fecha_inicio: str = None, fecha_fin: str = None, nombre_endpoint: str = ""):
    """
    Obteniene los datos de los endpoints de incidencias filtrados por fechas:
    - licences
    - absences
    - permissions
    """
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

            for registro in datos_pagina:
                incidencia = normalizar_incidencia(registro, nombre_endpoint=nombre_endpoint)
                if incidencia:
                    todos_los_datos.append(incidencia)
            
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

def obtener_todos_los_empleados_filtrados(url_employees: str = settings.API_BASE_URL):
    """Obtiene y filtra los datos de empleados desde la API."""
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
                    "id": empleado_completo.get("id"),
                    "person_id": empleado_completo.get("person_id"),
                    "full_name": empleado_completo.get("full_name"),
                    "first_name": first_name_filtered,
                    "last_name": empleado_completo.get("surname"),
                    "rut": empleado_completo.get("rut"),
                    "active_since": empleado_completo.get("active_since"),
                    "status": empleado_completo.get("status"),
                    "start_date": empleado_completo.get("current_job", {}).get("start_date"),
                    "end_date": empleado_completo.get("current_job", {}).get("end_date"),
                    'name_role': empleado_completo.get("current_job", {}).get("role", {}).get("name"),
                    'area_id': empleado_completo.get("current_job", {}).get("area_id"),
                    "email": empleado_completo.get("email"),
                    "personal_email": empleado_completo.get("personal_email"),
                    "rut_boss": empleado_completo.get("current_job", {}).get("boss", {}).get("rut"),
                    "address": empleado_completo.get("address"),
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
                    "payment_method": empleado_completo.get("payment_method"),
                    "base_wage": to_int_or_none(empleado_completo.get("current_job", {}).get("base_wage",)),
                    "nationality": empleado_completo.get("nationality"),
                    "civil_status": empleado_completo.get("civil_status"),
                    "health_company": empleado_completo.get("health_company"),
                    "pension_regime": empleado_completo.get("pension_regime"),
                    "pension_fund": empleado_completo.get("pension_fund"),
                    "active_until": empleado_completo.get("active_until"),
                    "afc": empleado_completo.get("afc"),
                    "retired": empleado_completo.get("retired"),
                    "retirement_regime": empleado_completo.get("retirement_regime"),
                    "termination_reason": empleado_completo.get("termination_reason"),
                    "contract_type": empleado_completo.get("current_job", {}).get("contract_type"),
                    "contract_finishing_date_1": empleado_completo.get("current_job", {}).get("contract_finishing_date_1"),
                    "contract_finishing_date_2": empleado_completo.get("current_job", {}).get("contract_finishing_date_2"),
                    "cost_center": empleado_completo.get("current_job", {}).get("cost_center"),
                    "ctrlit_recinto": empleado_completo.get("current_job", {}).get("custom_attributes", {}).get("ctrlit_recinto"),
                    "picture_url": empleado_completo.get("picture_url"),
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

    retry_strategy = Retry(
        total=3,  # Número de reintentos
        backoff_factor=4,  # Tiempo de espera en segundos.
        status_forcelist=[429, 500, 502, 503, 504], # Códigos a reintentar
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    headers = {"auth_token": settings.TOKEN}
    url_actual = url_vacaciones + "vacations"
    vacaciones_obtenidas = []
    pagina_actual = 1
    
    print(f"Comenzando obtención de vacaciones: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        while url_actual:
            try:
                print(f"Procesando página {pagina_actual}...")
                
                respuesta = http.get(url_actual, headers=headers, timeout=60)
                respuesta.raise_for_status()
                
                respuesta_api = respuesta.json()
                vacaciones_pagina = respuesta_api['data']
                pagination_info = respuesta_api['pagination']

                for vacaciones in vacaciones_pagina:
                    vacaciones_filtradas = {
                        "id": vacaciones.get("id"),
                        "employee_id": vacaciones.get("employee_id"),
                        "working_days": vacaciones.get("working_days"),
                        "workday_stage": vacaciones.get("workday_stage"),
                        "type": vacaciones.get("type"),
                        "status": vacaciones.get("status"),
                        "start_date": vacaciones.get("start_date"),
                        "end_date": vacaciones.get("end_date")
                        }
                    vacaciones_obtenidas.append(vacaciones_filtradas)            
                
                print(f"Página {pagina_actual}: {len(vacaciones_pagina)} vacaciones procesadas")
                
                # Obtener la URL de la siguiente página
                url_actual = pagination_info.get('next')
                pagina_actual += 1
                
                # Pausa para no sobrecargar la API
                time.sleep(0.5)

            except requests.exceptions.HTTPError as errh:
                print(f"\nError HTTP en pág {pagina_actual}: {errh}")
                # Si fallan los 3 reintentos, rompemos el bucle pero NO la función
                break 
            except requests.exceptions.ConnectionError as errc:
                print(f"\nError de Conexión en pág {pagina_actual}: {errc}")
                break
            except requests.exceptions.Timeout as errt:
                print(f"\nTimeout en pág {pagina_actual}: {errt}")
                break
            except Exception as e:
                print(f"\nError inesperado en pág {pagina_actual}: {e}")
                break
        
    except Exception as e_critico:
        print(f"\nError crítico global: {e_critico}")
        
    print(f"\n[{time.strftime('%H:%M:%S')}] Extracción finalizada (o interrumpida).")
    print(f"Total registros rescatados: {len(vacaciones_obtenidas)}")
    
    return vacaciones_obtenidas


