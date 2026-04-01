"""
DAG: etl_rh_cramer
Orquesta el ETL diario RH: BUK API → PostgreSQL (vía túnel SSH).

Dependencias entre tareas:
    garantizar_tablas → etl_areas → etl_employees
                                        ├── etl_vacaciones
                                        ├── etl_incidencias
                                        └── etl_contract_alerts

Todas las importaciones del dominio (app.*) se realizan dentro de cada
callable para evitar ejecuciones en tiempo de parseo del DAG.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "rh_cramer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# =============================================================================
# CALLABLES — una función por tarea, cada una abre/cierra su propia conexión
# =============================================================================

def _garantizar_tablas():
    from app.utils.db_client import get_db_connection
    from app.etl import create

    conn = get_db_connection()
    try:
        if not create.garantizar_tablas_rh(conn):
            raise RuntimeError("No se pudo crear/verificar el schema rh.")
    finally:
        conn.close()


def _etl_areas():
    from app.config import settings
    from app.utils.db_client import get_db_connection
    from app.etl import extract, load
    from app.utils.etl_core import ejecutar_flujo_etl

    conn = get_db_connection()
    try:
        exito = ejecutar_flujo_etl(
            nombre_entidad="areas",
            funcion_extraccion=lambda: extract.obtener_datos_tabla_areas(settings.API_BASE_URL),
            funcion_carga=load.cargar_datos_areas,
            conexion_db=conn,
        )
        load.reintentar_errores_entidad(conn, "areas")
        if not exito:
            raise RuntimeError("ETL areas terminó con errores — revisar staging.")
    finally:
        conn.close()


def _etl_employees():
    from app.config import settings
    from app.utils.db_client import get_db_connection
    from app.etl import extract, load
    from app.utils.etl_core import ejecutar_flujo_etl

    conn = get_db_connection()
    try:
        exito = ejecutar_flujo_etl(
            nombre_entidad="employees",
            funcion_extraccion=lambda: extract.obtener_todos_los_empleados_filtrados(
                settings.API_BASE_URL
            ),
            # job_sincronizar_empleados recibe (data, conn), etl_core pasa (conn, data)
            funcion_carga=lambda conn, data: load.job_sincronizar_empleados(data, conn),
            conexion_db=conn,
        )
        if not exito:
            raise RuntimeError("ETL employees terminó con errores — revisar staging.")
    finally:
        conn.close()


def _etl_vacaciones():
    from app.config import settings
    from app.utils.db_client import get_db_connection
    from app.etl import extract, load
    from app.utils.etl_core import ejecutar_flujo_etl

    conn = get_db_connection()
    try:
        exito = ejecutar_flujo_etl(
            nombre_entidad="vacaciones",
            funcion_extraccion=lambda: extract.obtener_datos_vacaciones(settings.API_BASE_URL),
            funcion_carga=load.cargar_datos_vacaciones,
            conexion_db=conn,
        )
        load.reintentar_errores_entidad(conn, "vacations")
        if not exito:
            raise RuntimeError("ETL vacaciones terminó con errores — revisar staging.")
    finally:
        conn.close()


def _etl_incidencias():
    from app.config import settings
    from app.utils.db_client import get_db_connection
    from app.etl import extract, load

    conn = get_db_connection()
    try:
        incidencias = []
        for nombre_endpoint, url_endpoint in settings.API_ENDPOINTS.items():
            datos = extract.obtener_datos_paginados(
                url_endpoint,
                aplicar_filtro_fechas=settings.FILTRAR_POR_FECHAS,
                fecha_inicio=settings.FECHA_INICIO,
                fecha_fin=settings.FECHA_FIN,
                nombre_endpoint=nombre_endpoint,
            )
            incidencias.extend(datos)

        load.crear_e_insertar_tablas_incidencias(conn, "consolidado_incidencias", incidencias)
        load.reintentar_errores_entidad(conn, "consolidado_incidencias")
    finally:
        conn.close()


def _etl_contract_alerts():
    from app.utils.db_client import get_db_connection
    from app.etl import contract_alerts

    conn = get_db_connection()
    try:
        resultado = contract_alerts.generar_alertas_contrato(conn)
        if resultado.get("errores", 0) > 0:
            raise RuntimeError(
                f"Generación de alertas terminó con {resultado['errores']} errores."
            )
    finally:
        conn.close()


# =============================================================================
# DEFINICIÓN DEL DAG
# =============================================================================

with DAG(
    dag_id="etl_rh_cramer",
    default_args=default_args,
    description="ETL diario RH: BUK API → PostgreSQL vía túnel SSH",
    schedule="0 2 * * 1-5",   # Lunes a viernes a las 02:00
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["rh", "etl"],
) as dag:

    t_ddl = PythonOperator(
        task_id="garantizar_tablas",
        python_callable=_garantizar_tablas,
    )

    t_areas = PythonOperator(
        task_id="etl_areas",
        python_callable=_etl_areas,
    )

    t_employees = PythonOperator(
        task_id="etl_employees",
        python_callable=_etl_employees,
    )

    t_vacaciones = PythonOperator(
        task_id="etl_vacaciones",
        python_callable=_etl_vacaciones,
    )

    t_incidencias = PythonOperator(
        task_id="etl_incidencias",
        python_callable=_etl_incidencias,
    )

    t_alerts = PythonOperator(
        task_id="etl_contract_alerts",
        python_callable=_etl_contract_alerts,
    )

    # Áreas primero (employees valida area_id contra rh.areas)
    # Employees antes de incidencias y alertas (ambas validan employee_id)
    t_ddl >> t_areas >> t_employees >> [t_vacaciones, t_incidencias, t_alerts]
