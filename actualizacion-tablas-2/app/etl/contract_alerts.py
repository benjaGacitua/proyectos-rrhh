from datetime import datetime

from app.utils.logger import setup_logger

logger = setup_logger(__name__)


def _to_date_obj(value):
    if value is None:
        return None
    if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
        return value
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _calcular_alerta_contrato(empleado: dict):
    """
    Calcula fecha y metadatos de alerta según reglas de negocio de contratos.
    """
    tipo_contrato = (empleado.get("contract_type") or "").strip().lower()
    status = (empleado.get("status") or "").strip().lower()
    metodo_pago = (empleado.get("payment_method") or "").strip().lower()
    active_since = _to_date_obj(empleado.get("active_since"))
    plazo_1 = _to_date_obj(empleado.get("contract_finishing_date_1"))
    plazo_2 = _to_date_obj(empleado.get("contract_finishing_date_2"))

    if status != "activo" or metodo_pago != "transferencia bancaria" or tipo_contrato == "indefinido":
        return None

    if not active_since or tipo_contrato != "fijo" or not plazo_1:
        return None

    if plazo_2:
        motivo = "Renovacion a segundo plazo"
        tipo_alerta = "SEGUNDO_PLAZO"
        fecha_alerta = plazo_1
    else:
        motivo = "Paso a contrato indefinido"
        tipo_alerta = "INDEFINIDO"
        fecha_alerta = plazo_1

    hoy = datetime.now().date()
    return {
        "alert_date": fecha_alerta,
        "alert_type": tipo_alerta,
        "alert_reason": motivo,
        "days_since_start": (hoy - active_since).days,
        "expiration": (hoy - fecha_alerta).days,
    }


def generar_alertas_contrato(conexion):
    """
    Genera/actualiza alertas de contrato en `rh.contract_alerts` usando `rh.employees`.
    """
    cursor = conexion.cursor()
    try:
        logger.info("Iniciando generacion de alertas de contrato...")
        cursor.execute(
            """
            SELECT
                id, rut, full_name, first_name, email, name_role, start_date, contract_type,
                rut_boss, active_since, contract_finishing_date_1, contract_finishing_date_2,
                status, payment_method
            FROM rh.employees
            """
        )
        rows = cursor.fetchall()
        if not rows:
            logger.warning("No hay empleados en rh.employees para generar alertas.")
            return {"procesados": 0, "alertas": 0, "errores": 0}

        empleados = []
        for row in rows:
            empleados.append(
                {
                    "id": row[0],
                    "rut": row[1],
                    "full_name": row[2],
                    "first_name": row[3],
                    "email": row[4],
                    "name_role": row[5],
                    "start_date": row[6],
                    "contract_type": row[7],
                    "rut_boss": row[8],
                    "active_since": row[9],
                    "contract_finishing_date_1": row[10],
                    "contract_finishing_date_2": row[11],
                    "status": row[12],
                    "payment_method": row[13],
                }
            )

        empleados_por_rut = {e.get("rut"): e for e in empleados if e.get("rut")}

        sql_upsert = """
            INSERT INTO rh.contract_alerts (
                employee_id, rut, employee_name, employee_role, employee_start_date, email,
                employee_contract_type, boss_name, boss_email, boss_of_boss_name, boss_of_boss_email,
                alert_date, alert_type, alert_reason, expiration, days_since_start,
                first_alert_sent, second_alert_sent, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                FALSE, FALSE, NOW(), NOW()
            )
            ON CONFLICT (employee_id) DO UPDATE SET
                rut = EXCLUDED.rut,
                employee_name = EXCLUDED.employee_name,
                employee_role = EXCLUDED.employee_role,
                employee_start_date = EXCLUDED.employee_start_date,
                email = EXCLUDED.email,
                employee_contract_type = EXCLUDED.employee_contract_type,
                boss_name = EXCLUDED.boss_name,
                boss_email = EXCLUDED.boss_email,
                boss_of_boss_name = EXCLUDED.boss_of_boss_name,
                boss_of_boss_email = EXCLUDED.boss_of_boss_email,
                alert_date = EXCLUDED.alert_date,
                alert_type = EXCLUDED.alert_type,
                alert_reason = EXCLUDED.alert_reason,
                expiration = EXCLUDED.expiration,
                days_since_start = EXCLUDED.days_since_start,
                first_alert_sent = COALESCE(rh.contract_alerts.first_alert_sent, FALSE),
                second_alert_sent = COALESCE(rh.contract_alerts.second_alert_sent, FALSE),
                updated_at = NOW()
        """

        procesados = 0
        alertas = 0
        errores = 0

        for empleado in empleados:
            procesados += 1
            alerta = _calcular_alerta_contrato(empleado)
            if not alerta:
                continue

            try:
                cursor.execute("SAVEPOINT sp_contract_alert")
                jefe = empleados_por_rut.get(empleado.get("rut_boss"))
                jefe_del_jefe = empleados_por_rut.get(jefe.get("rut_boss")) if jefe and jefe.get("rut_boss") else None

                cursor.execute(
                    sql_upsert,
                    (
                        empleado.get("id"),
                        empleado.get("rut"),
                        empleado.get("full_name"),
                        empleado.get("name_role"),
                        _to_date_obj(empleado.get("start_date")),
                        empleado.get("email"),
                        empleado.get("contract_type"),
                        jefe.get("first_name") if jefe else None,
                        jefe.get("email") if jefe else None,
                        jefe_del_jefe.get("first_name") if jefe_del_jefe else None,
                        jefe_del_jefe.get("email") if jefe_del_jefe else None,
                        alerta["alert_date"],
                        alerta["alert_type"],
                        alerta["alert_reason"],
                        alerta["expiration"],
                        alerta["days_since_start"],
                    ),
                )
                alertas += 1
                cursor.execute("RELEASE SAVEPOINT sp_contract_alert")
            except Exception as e_row:
                cursor.execute("ROLLBACK TO SAVEPOINT sp_contract_alert")
                cursor.execute("RELEASE SAVEPOINT sp_contract_alert")
                errores += 1
                logger.exception(f"Error generando alerta contrato employee_id={empleado.get('id')}: {e_row}")

        conexion.commit()
        logger.info(
            f"Alertas de contrato finalizadas. Procesados: {procesados}, Alertas upsert: {alertas}, Errores: {errores}."
        )
        return {"procesados": procesados, "alertas": alertas, "errores": errores}
    except Exception as e:
        conexion.rollback()
        logger.exception(f"Error en generacion de alertas de contrato: {e}")
        raise
    finally:
        cursor.close()
