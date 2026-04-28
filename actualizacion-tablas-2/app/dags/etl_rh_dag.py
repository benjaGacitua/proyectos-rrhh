"""
DAG: etl_rh_cramer
Orquesta el ETL diario RH: BUK API → PostgreSQL (vía túnel SSH).

Dependencias entre tareas:
    garantizar_tablas ─┬─ check_inicio_mes → etl_settlements_chile ────────────────────────────────┐
                       └─ etl_areas → etl_employees ─┬─ etl_vacaciones                             ├─→ notificar_resumen (ALL_DONE)
                                                      ├─ etl_contract_alerts                        │
                                                      ├─ etl_sync_lavanderia ───────────────────────┤
                                                      └─ etl_incidencias ─┬─ refresh_ausentismo ────┤
                                                                           ├─ refresh_kpi_mensual ───┤
                                                                           └─ refresh_kpi_semanal ───┘

    check_inicio_mes: ShortCircuitOperator — solo días 1-5 del mes.
    En días 6+, etl_settlements_chile queda en 'skipped' (no cuenta como error).

    Bloque KPI (refresh_ausentismo, refresh_kpi_mensual, refresh_kpi_semanal):
    Corre en paralelo después de etl_incidencias, ya que depende de datos frescos
    de consolidado_incidencias y employees.

Todas las importaciones del dominio (app.*) se realizan dentro de cada
callable para evitar ejecuciones en tiempo de parseo del DAG.

Notificaciones Telegram:
    - on_failure_callback en cada tarea → envía alerta inmediata vía n8n.
    - Tarea final 'notificar_resumen' → envía resumen global del DAG run.
    Requiere N8N_WEBHOOK_URL en .env apuntando al endpoint 'airflow-etl-log'.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
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

def _es_inicio_de_mes(**context):
    """
    ShortCircuitOperator: devuelve True solo los primeros 5 días del mes.
    Usar día <= 5 (en lugar de == 1) permite reintentos si el DAG falla el día 1.
    Cuando devuelve False, las tareas downstream se marcan como 'skipped' sin error.
    """
    return context["logical_date"].day <= 16


def _etl_settlements_chile():
    from app.config import settings
    from app.utils.db_client import get_db_connection
    from app.etl.settlements_chile import ejecutar_flujo_settlements

    if not settings.URL_SETTLEMENTS_CHILE:
        raise ValueError("URL_SETTLEMENTS_CHILE no está configurada en .env")

    conn = get_db_connection()
    try:
        hoy = datetime.now()
        # Cargar siempre el mes anterior (las liquidaciones del mes en curso
        # no están cerradas hasta el cierre de nómina del mes siguiente)
        if hoy.month == 1:
            year, month = hoy.year - 1, 12
        else:
            year, month = hoy.year, hoy.month - 1

        exito = ejecutar_flujo_settlements(
            conn,
            settings.TOKEN,
            settings.URL_SETTLEMENTS_CHILE,
            year,
            month,
        )
        if not exito:
            raise RuntimeError("ETL settlements terminó con errores — revisar logs.")
    finally:
        conn.close()


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


def _etl_sync_lavanderia():
    from app.utils.db_client import get_db_connection, get_lavanderia_connection
    from app.etl.sync_lavanderia import sincronizar_personal_lavanderia

    conn_rh = get_db_connection()
    conn_lav = get_lavanderia_connection()
    try:
        resultado = sincronizar_personal_lavanderia(conn_rh, conn_lav)
        if resultado["errores"] > 0:
            raise RuntimeError(
                f"Sync lavandería terminó con {resultado['errores']} errores — revisar logs."
            )
    finally:
        conn_rh.close()
        conn_lav.close()


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
        "check_inicio_mes",
        "etl_settlements_chile",
        "etl_areas",
        "etl_employees",
        "etl_vacaciones",
        "etl_incidencias",
        "etl_contract_alerts",
        "etl_sync_lavanderia",
        "refresh_ausentismo",
        "refresh_kpi_mensual",
        "refresh_kpi_semanal",
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
    tags=["rh", "etl", "kpi"],
) as dag:

    t_ddl = PythonOperator(
        task_id="garantizar_tablas",
        python_callable=_garantizar_tablas,
    )

    # --- Bloque mensual (solo días 1-5 de cada mes) ---
    t_check_mes = ShortCircuitOperator(
        task_id="check_inicio_mes",
        python_callable=_es_inicio_de_mes,
        on_failure_callback=None,
        # ignore_downstream_trigger_rules=False: las tareas bloqueadas quedan
        # en 'skipped' y no impiden que t_areas arranque normalmente.
        ignore_downstream_trigger_rules=False,
    )

    t_settlements = PythonOperator(
        task_id="etl_settlements_chile",
        python_callable=_etl_settlements_chile,
    )

    # --- Bloque diario ---
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

    t_sync_lavanderia = PythonOperator(
        task_id="etl_sync_lavanderia",
        python_callable=_etl_sync_lavanderia,
    )

    # --- Bloque KPI (depende de incidencias y employees frescos) ---
    # Usa PostgresOperator porque son simples CALLs a procedures en PostgreSQL.
    # Requiere la conexión 'postgres_rh' configurada en Airflow Admin → Connections.
    t_refresh_ausentismo = PostgresOperator(
        task_id="refresh_ausentismo",
        postgres_conn_id="postgres_rh",
        sql="CALL rh.refresh_reporte_ausentismo();",
    )

    t_refresh_kpi_mensual = PostgresOperator(
        task_id="refresh_kpi_mensual",
        postgres_conn_id="postgres_rh",
        sql="CALL rh.refresh_kpi_inasistencias();",
    )

    t_refresh_kpi_semanal = PostgresOperator(
        task_id="refresh_kpi_semanal",
        postgres_conn_id="postgres_rh",
        sql="CALL rh.refresh_kpi_inasistencias_semanal();",
    )

    t_resumen = PythonOperator(
        task_id="notificar_resumen",
        python_callable=_notificar_resumen,
        trigger_rule=TriggerRule.ALL_DONE,  # Se ejecuta siempre, fallen o no las tareas anteriores
        on_failure_callback=None,           # El resumen no necesita callback de fallo propio
    )

    # Flujo mensual: corre en paralelo al bloque diario, ambos arrancan desde t_ddl
    t_ddl >> t_check_mes >> t_settlements >> t_resumen

    # Flujo diario (áreas primero porque employees valida area_id)
    t_ddl >> t_areas >> t_employees >> [t_vacaciones, t_incidencias, t_alerts, t_sync_lavanderia]

    # Bloque KPI: arranca cuando incidencias Y employees están listos
    # (t_employees ya terminó porque t_incidencias depende de él)
    t_incidencias >> [t_refresh_ausentismo, t_refresh_kpi_mensual, t_refresh_kpi_semanal]

    # Todo converge en el resumen
    [t_vacaciones, t_alerts, t_sync_lavanderia, t_refresh_ausentismo, t_refresh_kpi_mensual, t_refresh_kpi_semanal] >> t_resumen