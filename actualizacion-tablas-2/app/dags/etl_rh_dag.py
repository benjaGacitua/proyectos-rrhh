"""
DAG: etl_rh_cramer
Orquesta el ETL diario RH: BUK API → PostgreSQL (vía túnel SSH).

Dependencias entre tareas:
    garantizar_tablas → etl_areas → etl_employees
                                        ├── etl_vacaciones
                                        ├── etl_incidencias
                                        └── etl_contract_alerts
                                              └── notificar_resumen  (ALL_DONE)

Todas las importaciones del dominio (app.*) se realizan dentro de cada
callable para evitar ejecuciones en tiempo de parseo del DAG.

Notificaciones Telegram:
    - on_failure_callback en cada tarea → envía alerta inmediata vía n8n.
    - Tarea final 'notificar_resumen' → envía resumen global del DAG run.
    Requiere N8N_WEBHOOK_URL en .env apuntando al endpoint 'airflow-etl-log'.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


# =============================================================================
# CALLBACKS DE NOTIFICACIÓN
# =============================================================================

def _callback_fallo(context):
    """Envía alerta inmediata a Telegram cuando una tarea falla."""
    from app.utils.telegram_notifier import notificar_tarea

    ti = context["task_instance"]
    exc = context.get("exception")
    log_excerpt = f"{type(exc).__name__}: {exc}" if exc else "Sin detalle de excepción"

    notificar_tarea(
        state="failed",
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=context.get("run_id", "desconocido"),
        try_number=ti.try_number,
        ts=context.get("ts", ""),
        log_excerpt=log_excerpt,
    )


default_args = {
    "owner": "rh_cramer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "on_failure_callback": _callback_fallo,
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


def _notificar_resumen(**context):
    """Tarea final: envía resumen global del DAG run a Telegram."""
    from airflow.utils.state import State
    from app.utils.telegram_notifier import notificar_tarea

    ti = context["task_instance"]
    dag_run = ti.get_dagrun()

    TAREAS_ETL = [
        "garantizar_tablas",
        "etl_areas",
        "etl_employees",
        "etl_vacaciones",
        "etl_incidencias",
        "etl_contract_alerts",
    ]

    task_instances = {t.task_id: t for t in dag_run.get_task_instances()}

    lineas = []
    estado_global = "success"
    for task_id in TAREAS_ETL:
        t = task_instances.get(task_id)
        estado = t.state if t else "no ejecutada"
        lineas.append(f"  {task_id}: {estado}")
        if estado in (State.FAILED, State.UPSTREAM_FAILED):
            estado_global = "failed"

    log_excerpt = "Resumen de tareas:\n" + "\n".join(lineas)

    notificar_tarea(
        state=estado_global,
        dag_id=ti.dag_id,
        task_id="[resumen_dag]",
        run_id=context.get("run_id", "desconocido"),
        ts=context.get("ts", ""),
        log_excerpt=log_excerpt,
    )


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

    t_resumen = PythonOperator(
        task_id="notificar_resumen",
        python_callable=_notificar_resumen,
        trigger_rule=TriggerRule.ALL_DONE,  # Se ejecuta siempre, fallen o no las tareas anteriores
        on_failure_callback=None,           # El resumen no necesita callback de fallo propio
    )

    # Áreas primero (employees valida area_id contra rh.areas)
    # Employees antes de incidencias y alertas (ambas validan employee_id)
    t_ddl >> t_areas >> t_employees >> [t_vacaciones, t_incidencias, t_alerts] >> t_resumen
