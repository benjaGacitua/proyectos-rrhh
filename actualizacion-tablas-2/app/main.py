import sys
import os

# Ajustar path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings
from app.utils.logger import setup_logger
from app.utils.db_client import get_db_connection
from app.etl import extract, load, create, update
from app.utils.estado import tarea_ya_completada, marcar_tarea
from app.utils.etl_core import ejecutar_flujo_etl

logger = setup_logger(__name__)

def main():
    logger.info("--- INICIANDO PROCESO ETL ---")
    
    # 1. Conexión a Base de Datos (Con encriptación habilitada)
    conexion = get_db_connection()
    if not conexion:
        logger.error("No se pudo establecer conexión. Abortando.")
        return

    try:
        # =========================================================================
        # BLOQUE 1: EMPLEADOS (Extract + Load)
        # =========================================================================
        tarea_emp = 'etl_completo_employees' # Unificamos nombre de tarea
        
        if tarea_ya_completada(tarea_emp):
            logger.info(f"Saltando '{tarea_emp}': Ya se completó hoy.")
        else:
            exito_emp = ejecutar_flujo_etl(
                nombre_entidad="employees",
                # Usamos lambda para pasar argumentos fijos (URL)
                funcion_extraccion=lambda: extract.obtener_todos_los_empleados_filtrados(settings.API_BASE_URL),
                # La función de carga original ya recibe (data, conexion), pero etl_core pasa (conexion, data)
                # Asegúrate que el orden coincida o usa lambda para invertir:
                funcion_carga=lambda conn, data: load.job_sincronizar_empleados(data, conn),
                conexion_db=conexion
            )
            
            if exito_emp:
                marcar_tarea(tarea_emp, exito=True)
            else:
                marcar_tarea(tarea_emp, exito=False, error_msg="Fallo en flujo ETL employees")

        # =========================================================================
        # BLOQUE 2: ÁREAS
        # =========================================================================
        tarea_areas = 'etl_completo_areas'

        if tarea_ya_completada(tarea_areas):
            logger.info(f"Saltando '{tarea_areas}': Ya se completó hoy.")
        else:
            # Definimos una pequeña función wrapper porque Areas requiere un paso extra (create)
            def carga_areas_wrapper(conn, data):
                create.verificar_tablas_areas(conn)
                load.cargar_datos_areas(conn, data)

            exito_areas = ejecutar_flujo_etl(
                nombre_entidad="areas",
                funcion_extraccion=lambda: extract.obtener_datos_tabla_areas(settings.API_BASE_URL),
                funcion_carga=carga_areas_wrapper,
                conexion_db=conexion
            )
            
            if exito_areas:
                marcar_tarea(tarea_areas, exito=True)

        # =========================================================================
        # BLOQUE 3: VACACIONES
        # =========================================================================
        tarea_vac = 'etl_completo_vacaciones'

        if tarea_ya_completada(tarea_vac):
            logger.info(f"Saltando '{tarea_vac}': Ya se completó hoy.")
        else:
            exito_vac = ejecutar_flujo_etl(
                nombre_entidad="vacaciones",
                funcion_extraccion=lambda: extract.obtener_datos_vacaciones(settings.API_BASE_URL),
                funcion_carga=load.cargar_datos_vacaciones, # Asumiendo firma: (conexion, data)
                conexion_db=lambda: get_db_connection()
            )
            
            if exito_vac:
                marcar_tarea(tarea_vac, exito=True)
        
        # =========================================================================
        # BLOQUE 4: ENDPOINTS GENÉRICOS / INCIDENCIAS
        # =========================================================================
        for nombre_endpoint, url_endpoint in settings.API_ENDPOINTS.items():
            
            # Filtros
            if nombre_endpoint in ['employees', 'areas']: continue
            
            tarea_gen = f"etl_completo_{nombre_endpoint}"
            
            if tarea_ya_completada(tarea_gen):
                logger.info(f"Saltando '{tarea_gen}': Ya se completó hoy.")
                continue

            # Configuramos la extracción con los parámetros de fechas
            def extract_generico_wrapper():
                return extract.obtener_datos_paginados(
                    url_endpoint,
                    aplicar_filtro_fechas=settings.FILTRAR_POR_FECHAS,
                    fecha_inicio=settings.FECHA_INICIO,
                    fecha_fin=settings.FECHA_FIN
                )
            
            # Configuramos la carga pasando el nombre de tabla dinámica
            def load_generico_wrapper(conn, data):
                # Asumimos que esta función devuelve algo o lanza error
                load.crear_e_insertar_tablas_incidencias(conn, nombre_endpoint, data)

            exito_gen = ejecutar_flujo_etl(
                nombre_entidad=nombre_endpoint,
                funcion_extraccion=extract_generico_wrapper,
                funcion_carga=load_generico_wrapper,
                conexion_db=conexion
            )

            if exito_gen:
                marcar_tarea(tarea_gen, exito=True)
        
        # =========================================================================
        # BLOQUE 5: EJECUCIÓN DE QUERIES FINALES
        # =========================================================================
        nombre_tarea_queries = 'ejecucion_queries_incidencias'
        
        if not tarea_ya_completada(nombre_tarea_queries):
            try:
                logger.info("Actualizando Tabla Incidencias (Post-Proceso SQL)...")
                query_crear, query_actualizar = extract.queries_de_incidencias()
                update.actualizar_tabla_incidencias(conexion, query_crear, query_actualizar)
                marcar_tarea(nombre_tarea_queries, exito=True)
                logger.info("Queries finales ejecutadas correctamente.")
            except Exception as e:
                logger.error(f"Fallo en queries finales: {e}")
                marcar_tarea(nombre_tarea_queries, exito=False, error_msg=str(e))

    finally:
        # Cierre seguro de conexión
        try:
            conexion.close()
            logger.info("Conexión cerrada.")
        except Exception:
            pass
            
    logger.info("--- PROCESO ETL FINALIZADO ---")

if __name__ == "__main__":
    main()