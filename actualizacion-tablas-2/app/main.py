import sys
import os

# Ajustar path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings
from app.utils.logger import setup_logger
from app.utils.db_client import get_db_connection
from app.etl import extract, load, create, contract_alerts
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
        # DDL único: schema rh y todas las tablas (creacion_tablas.sql)
        if not create.garantizar_tablas_rh(conexion):
            logger.error("No se pudo verificar/crear el esquema RH. Abortando.")
            return

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
            exito_areas = ejecutar_flujo_etl(
                nombre_entidad="areas",
                funcion_extraccion=lambda: extract.obtener_datos_tabla_areas(settings.API_BASE_URL),
                funcion_carga=load.cargar_datos_areas,
                conexion_db=conexion
            )
            
            if exito_areas:
                marcar_tarea(tarea_areas, exito=True)
            load.reintentar_errores_entidad(conexion, "areas")

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
                conexion_db=conexion
            )
            
            if exito_vac:
                marcar_tarea(tarea_vac, exito=True)
            load.reintentar_errores_entidad(conexion, "vacations")
        
        # =========================================================================
        # BLOQUE 4: INCIDENCIAS CONSOLIDADAS (licences + absences + permissions)
        # =========================================================================
        tarea_incidencias = "etl_completo_consolidado_incidencias"

        if tarea_ya_completada(tarea_incidencias):
            logger.info(f"Saltando '{tarea_incidencias}': Ya se completó hoy.")
        else:
            incidencias_consolidadas = []
            exito_extraccion = True

            for nombre_endpoint, url_endpoint in settings.API_ENDPOINTS.items():
                try:
                    logger.info(f"Iniciando extracción de incidencias desde endpoint '{nombre_endpoint}'...")
                    incidencias_endpoint = extract.obtener_datos_paginados(
                        url_endpoint,
                        aplicar_filtro_fechas=settings.FILTRAR_POR_FECHAS,
                        fecha_inicio=settings.FECHA_INICIO,
                        fecha_fin=settings.FECHA_FIN,
                        nombre_endpoint=nombre_endpoint,
                    )
                    incidencias_consolidadas.extend(incidencias_endpoint)
                    logger.info(
                        f"Endpoint '{nombre_endpoint}' procesado. Registros válidos: {len(incidencias_endpoint)}."
                    )
                except Exception as e:
                    exito_extraccion = False
                    logger.exception(f"Error extrayendo endpoint '{nombre_endpoint}': {e}")

            if exito_extraccion:
                try:
                    load.crear_e_insertar_tablas_incidencias(
                        conexion, "consolidado_incidencias", incidencias_consolidadas
                    )
                    marcar_tarea(tarea_incidencias, exito=True)
                    load.reintentar_errores_entidad(conexion, "consolidado_incidencias")
                except Exception as e:
                    logger.error(f"Fallo en carga consolidada de incidencias: {e}")
                    marcar_tarea(tarea_incidencias, exito=False, error_msg=str(e))
            else:
                marcar_tarea(
                    tarea_incidencias,
                    exito=False,
                    error_msg="Falló la extracción de uno o más endpoints de incidencias.",
                )

        # =========================================================================
        # BLOQUE 5: ALERTAS DE CONTRATO
        # =========================================================================
        tarea_alertas = "etl_completo_contract_alerts"

        if tarea_ya_completada(tarea_alertas):
            logger.info(f"Saltando '{tarea_alertas}': Ya se completó hoy.")
        else:
            try:
                resultado = contract_alerts.generar_alertas_contrato(conexion)
                logger.info(
                    "Generación de alertas de contrato completada. "
                    f"Procesados: {resultado.get('procesados', 0)}, "
                    f"Alertas: {resultado.get('alertas', 0)}, "
                    f"Errores: {resultado.get('errores', 0)}."
                )
                marcar_tarea(tarea_alertas, exito=True)
            except Exception as e:
                logger.error(f"Fallo en generación de alertas de contrato: {e}")
                marcar_tarea(tarea_alertas, exito=False, error_msg=str(e))

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