import sys
import os

# Ajustar path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings
from app.utils.logger import setup_logger
from app.utils.db_client import get_db_connection
from app.etl import extract, load, create, update
from app.utils.estado import tarea_ya_completada, marcar_tarea


logger = setup_logger(__name__)

def main():
    logger.info("--- INICIANDO PROCESO ETL ---")
    
    # 1. Conexión a Base de Datos (Con encriptación habilitada)
    conexion = get_db_connection()
    if not conexion:
        logger.error("No se pudo establecer conexión. Abortando.")
        return

    empleados_filtrados_api = []
    datos_areas = None
    datos_tablas_genericas = {}
    datos_vacaciones = []
    
    # =========================================================================
    #! 2. EXTRACCIÓN (Extract)
    # =========================================================================
    #? A. Extracción Employees
    nombre_tarea_empleados = 'extraccion_employees'
    if tarea_ya_completada(nombre_tarea_empleados):
        logger.info(f"Saltando '{nombre_tarea_empleados}': Ya se completó hoy.")
    else:
        try:
            logger.info("Extrayendo datos de empleados...")
            empleados_filtrados_api = extract.obtener_todos_los_empleados_filtrados(settings.API_BASE_URL)

            if not empleados_filtrados_api:
                logger.warning("No se obtuvieron empleados de la API.")
            else:
                marcar_tarea(nombre_tarea_empleados, exito=True)

        except Exception as e:
            error_msg = f"Fallo crítico extracción empleados: {e}"
            logger.error(error_msg)
            marcar_tarea(nombre_tarea_empleados, exito=False, error_msg=str(e))

    #? B. Extracción Endpoints Incidencias
    for nombre_endpoint, url_endpoint in settings.API_ENDPOINTS.items():
    
        # 1. Filtro de exclusión original
        if nombre_endpoint in ['employees', 'areas']: 
            continue

        # 2. Verificar estado individualmente
        if tarea_ya_completada(nombre_endpoint):
            logger.info(f"Saltando '{nombre_endpoint}': Ya se completó hoy.")
            continue    
        try:
            logger.info(f"Procesando endpoint genérico: {nombre_endpoint}")
            datos = extract.obtener_datos_paginados(
                url_endpoint,
                aplicar_filtro_fechas=settings.FILTRAR_POR_FECHAS,
                fecha_inicio=settings.FECHA_INICIO,
                fecha_fin=settings.FECHA_FIN
            )
            datos_tablas_genericas[nombre_endpoint] = datos

            # Éxito
            marcar_tarea(nombre_endpoint, exito=True)
            logger.info(f"Endpoint '{nombre_endpoint}' extraído correctamente.")

        except Exception as e:
            logger.error(f"Fallo crítico extracción genérica en '{nombre_endpoint}': {e}")
            marcar_tarea(nombre_endpoint, exito=False, error_msg=str(e))
            continue

    #? C. Extracción Áreas
    nombre_tarea_areas = 'extraccion_areas'
    if tarea_ya_completada(nombre_tarea_areas):
        logger.info(f"Saltando '{nombre_tarea_areas}': Ya se completó hoy.")
    else:
        try:
            logger.info("Extrayendo datos de áreas...")
            datos_areas = extract.obtener_datos_tabla_areas(settings.API_BASE_URL) 

            marcar_tarea(nombre_tarea_areas, exito=True)
            logger.info(f"Extracción de áreas completada.")

        except Exception as e:
            logger.error(f"Fallo crítico extracción áreas: {e}")
            marcar_tarea(nombre_tarea_areas, exito=False, error_msg=str(e))
    
    #? D. Extracción Vacaciones
    nombre_tarea_vacaciones = 'extraccion_vacaciones'
    if tarea_ya_completada(nombre_tarea_vacaciones):
        logger.info(f"Saltando '{nombre_tarea_vacaciones}': Ya se completó hoy.")
    else:
        try:
            logger.info("Extrayendo datos de vacaciones...")
            datos_vacaciones = extract.obtener_datos_vacaciones(settings.API_BASE_URL) 

            marcar_tarea(nombre_tarea_vacaciones, exito=True)
            logger.info(f"Extracción de vacaciones completada.")
        
        except Exception as e:
            logger.error(f"Fallo crítico extracción vacaciones: {e}")
            marcar_tarea(nombre_tarea_vacaciones, exito=False, error_msg=str(e))


    #! =========================================================================
    #! 3. CARGA (Load)
    #! =========================================================================
    #? A. Carga Tabla Employees 
    # Solo intentamos cargar si NO está lista Y si tenemos datos extraídos
    nombre_tarea_carga_emp = 'carga_employees'

    if tarea_ya_completada(nombre_tarea_carga_emp):
        logger.info(f"Saltando Carga Empleados: Ya se completó hoy.")
    elif not empleados_filtrados_api:
        logger.warning(f"No se puede ejecutar '{nombre_tarea_carga_emp}' porque no hay datos en memoria.")
    else:
        try:
            logger.info("Iniciando Job de Empleados (Merge + Encriptación)...")
            load.job_sincronizar_empleados(empleados_filtrados_api, conexion) 

            marcar_tarea(nombre_tarea_carga_emp, exito=True)
            logger.info("Carga de empleados finalizada correctamente.")

        except Exception as e:
            logger.error(f"Fallo en carga de empleados: {e}")
            marcar_tarea(nombre_tarea_carga_emp, exito=False, error_msg=str(e))
    
    #? B. Carga de Tablas Indicencias
    try:
        for nombre_tabla, datos in datos_tablas_genericas.items():
            tarea_incidencia = f"carga_tabla_{nombre_tabla}"

            if tarea_ya_completada(tarea_incidencia):
                logger.info(f"Saltando Carga '{nombre_tabla}': Ya se completó hoy.")
                continue

            if datos:
                logger.info(f"Cargando tabla de incidencia: {nombre_tabla}")
                load.crear_e_insertar_tablas_incidencias(conexion, nombre_tabla, datos)
                marcar_tarea(tarea_incidencia, exito=True)
            else:
                logger.warning(f"Datos vacíos para {nombre_tabla}, no se carga nada.")
    except Exception as e:
        logger.error(f"Fallo en carga de incidencias: {e}")

    #? C. Carga de Tabla Áreas
    nombre_tarea_carga_areas = 'carga_areas'
    if tarea_ya_completada(nombre_tarea_carga_areas):
        logger.info(f"Saltando Carga Áreas: Ya se completó hoy.")

    elif datos_areas:
        try:
            logger.info("Procesando carga de Áreas (Trigger + Tablas)...")
            create.verificar_tablas_areas(conexion)
            load.cargar_datos_areas(conexion, datos_areas)
            
            marcar_tarea(nombre_tarea_carga_areas, exito=True)
            logger.info("Carga de áreas finalizada correctamente.")

        except Exception as e:
            logger.error(f"Fallo en carga de áreas: {e}")
            marcar_tarea(nombre_tarea_carga_areas, exito=False, error_msg=str(e))

    #? D. Carga de Tabla Vacaciones
    nombre_tarea_carga_vacaciones = 'carga_vacaciones'
    if tarea_ya_completada(nombre_tarea_carga_vacaciones):
        logger.info(f"Saltando Carga Vacaciones: Ya se completó hoy.")

    elif datos_vacaciones:
        try:
            logger.info("Iniciando Carga de Vacaciones...")
            load.cargar_datos_vacaciones(conexion, datos_vacaciones)

            marcar_tarea(nombre_tarea_carga_vacaciones, exito=True)
            logger.info("Carga de vacaciones finalizada correctamente.")

        except Exception as e:
            logger.error(f"Fallo en carga de vacaciones: {e}")
            marcar_tarea(nombre_tarea_carga_vacaciones, exito=False, error_msg=str(e))


    # =========================================================================
    #! 4. EJECUCIÓN DE QUERIES
    # =========================================================================
    #? D. Carga Específica: Incidencias (SQL Files)
    nombre_tarea_queries = 'ejecucion_queries_incidencias'

    if tarea_ya_completada(nombre_tarea_queries):
        logger.info(f"Saltando Queries Incidencias: Ya se ejecutaron hoy.")
    else:
        try:
            logger.info("Actualizando Tabla Incidencias...")
            query_crear, query_actualizar = extract.queries_de_incidencias()
            update.actualizar_tabla_incidencias(conexion, query_crear, query_actualizar)

            marcar_tarea(nombre_tarea_queries, exito=True)
            logger.info("Queries de incidencias ejecutadas correctamente.")
            
        except Exception as e:
            logger.error(f"Fallo en incidencias: {e}")
            marcar_tarea(nombre_tarea_queries, exito=False, error_msg=str(e))

    # =========================================================================
    #! 4. CIERRE
    # =========================================================================
    try:
        conexion.close()
        logger.info("Conexión cerrada correctamente.")
    except Exception:
        pass # Si ya estaba cerrada o falló

    logger.info("--- PROCESO ETL COMPLETADO ---")

if __name__ == "__main__":
    main()