"""
ETL mensual de liquidaciones de sueldos (Chile).
API BUK → rh.historical_settlements + rh.historical_settlement_items (PostgreSQL).

Estrategia de carga: DELETE + INSERT por período (anio/mes).
No usa MERGE/upsert porque el proceso corre una vez al mes y se espera
que los datos de un período dado sean estables al momento de la ejecución.
"""

import json
import time
import requests
from datetime import date, datetime

from app.utils.logger import setup_logger

logger = setup_logger(__name__)

PAGE_SIZE = 100


# =============================================================================
# HELPERS DE TRANSFORMACIÓN
# =============================================================================

def _safe_float(value):
    if value is None:
        return None
    if isinstance(value, str) and value.strip() in ("", "N/A", "-", "null", "NULL"):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        logger.warning(f"No se pudo convertir a float: {value!r}")
        return None


def _flatten_liquidation(liq):
    """Transforma un registro raw de la API en una fila para rh.historical_settlements."""
    year = liq.get("year")
    month = liq.get("month")

    if year and month:
        try:
            year_int = int(year)
            month_int = int(month)
            pay_period = date(year_int, month_int, 1)
            periodo = f"{month_int:02d}-{year_int}"
        except Exception as e:
            logger.warning(f"Error convirtiendo fecha: year={year}, month={month}: {e}")
            pay_period = periodo = year_int = month_int = None
    else:
        pay_period = periodo = year_int = month_int = None

    return {
        "liquidacion_id": liq.get("liquidacion_id"),
        "employee_id":    liq.get("employee_id"),
        "rut":            liq.get("rut"),
        "periodo":        periodo,
        "anio":           year_int,
        "mes":            month_int,
        "pay_period":     pay_period,
        "dias_trabajados":              _safe_float(liq.get("worked_days")),
        "dias_no_trabajados":           _safe_float(liq.get("noworked_days")),
        "ingreso_bruto":                _safe_float(liq.get("income_gross")),
        "ingreso_neto":                 _safe_float(liq.get("income_net")),
        "ingreso_afp":                  _safe_float(liq.get("income_afp")),
        "ingreso_ips":                  _safe_float(liq.get("income_ips")),
        "total_ingresos_imponibles":    _safe_float(liq.get("total_income_taxable")),
        "total_ingresos_no_imponibles": _safe_float(liq.get("total_income_notaxable")),
        "total_descuentos_legales":     _safe_float(liq.get("total_legal_discounts")),
        "total_otros_descuentos":       _safe_float(liq.get("total_other_discounts")),
        "liquido_a_pagar":              _safe_float(liq.get("liquid_reach")),
        "base_imponible":               _safe_float(liq.get("taxable_base")),
        "cerrada":    liq.get("closed"),
        "raw_payload": json.dumps(liq),
    }


def _explode_items(liq_list):
    """Genera filas de items a partir de la lista de liquidaciones raw."""
    items = []
    for liq in liq_list:
        lid = liq.get("liquidacion_id")
        eid = liq.get("employee_id")
        rut = liq.get("rut")
        for line in liq.get("lines_settlement", []) or []:
            items.append({
                "liquidacion_id": lid,
                "employee_id":    eid,
                "rut":            rut,
                "item_type":      line.get("type"),
                "income_type":    line.get("income_type"),
                "subtype":        line.get("subtype"),
                "name":           line.get("name"),
                "amount":         _safe_float(line.get("amount")),
                "taxable":        _safe_float(line.get("taxable")),
                "imponible":      _safe_float(line.get("imponible")),
                "anticipo":       _safe_float(line.get("anticipo")),
                "credit_type":    line.get("credit_type"),
                "institution":    line.get("institution"),
                "description":    line.get("description"),
                "code":           line.get("code"),
                "item_code":      line.get("item_code"),
            })
    return items


# =============================================================================
# EXTRACCIÓN
# =============================================================================

def _request_with_retry(session, url, headers, max_retries=5, backoff_base=1.5, timeout=30):
    """GET con reintentos exponenciales para errores de red / 5xx."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, headers=headers, timeout=timeout)
            if 500 <= resp.status_code < 600:
                raise requests.exceptions.HTTPError(f"Server error {resp.status_code}")
            resp.raise_for_status()
            return resp.json()
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.HTTPError,
        ) as e:
            wait = backoff_base ** attempt
            logger.warning(
                f"Request error (attempt {attempt}/{max_retries}) para {url}: {e} — "
                f"reintentando en {wait:.1f}s"
            )
            if attempt == max_retries:
                raise
            time.sleep(wait)


def get_all_settlements(token, base_url, date_param):
    """
    Pagina la API y devuelve todas las liquidaciones para date_param (formato dd-mm-YYYY).
    """
    session = requests.Session()
    headers = {"auth_token": token}
    page = 1
    all_data = []

    while True:
        url = f"{base_url}?date={date_param}&page_size={PAGE_SIZE}&page={page}"
        data = _request_with_retry(session, url, headers)
        page_data = data.get("data", [])
        if not page_data:
            break
        all_data.extend(page_data)
        if len(page_data) < PAGE_SIZE:
            break
        page += 1
        time.sleep(0.2)

    return all_data


# =============================================================================
# CARGA
# =============================================================================

def _delete_period(cursor, year, month):
    """Elimina registros existentes del período en ambas tablas (items primero por FK)."""
    cursor.execute(
        """
        DELETE FROM rh.historical_settlement_items
        WHERE liquidacion_id IN (
            SELECT liquidacion_id
            FROM rh.historical_settlements
            WHERE anio = %s AND mes = %s AND liquidacion_id IS NOT NULL
        )
        """,
        (year, month),
    )
    items_deleted = cursor.rowcount

    cursor.execute(
        "DELETE FROM rh.historical_settlements WHERE anio = %s AND mes = %s",
        (year, month),
    )
    settlements_deleted = cursor.rowcount

    logger.info(
        f"   Eliminados {settlements_deleted} liquidaciones y "
        f"{items_deleted} items del período {month:02d}-{year}"
    )
    return settlements_deleted, items_deleted


def _insert_settlements(cursor, rows):
    """Inserta filas en rh.historical_settlements."""
    sql = """
        INSERT INTO rh.historical_settlements (
            liquidacion_id, employee_id, rut, periodo, anio, mes, pay_period,
            dias_trabajados, dias_no_trabajados,
            ingreso_bruto, ingreso_neto, ingreso_afp, ingreso_ips,
            total_ingresos_imponibles, total_ingresos_no_imponibles,
            total_descuentos_legales, total_otros_descuentos,
            liquido_a_pagar, base_imponible,
            cerrada, raw_payload, created_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, NOW()
        )
    """
    inserted = errors = 0
    for row in rows:
        try:
            cursor.execute("SAVEPOINT sp_settlement")
            cursor.execute(sql, (
                row["liquidacion_id"], row["employee_id"], row["rut"],
                row["periodo"], row["anio"], row["mes"], row["pay_period"],
                row["dias_trabajados"], row["dias_no_trabajados"],
                row["ingreso_bruto"], row["ingreso_neto"],
                row["ingreso_afp"], row["ingreso_ips"],
                row["total_ingresos_imponibles"], row["total_ingresos_no_imponibles"],
                row["total_descuentos_legales"], row["total_otros_descuentos"],
                row["liquido_a_pagar"], row["base_imponible"],
                row["cerrada"], row["raw_payload"],
            ))
            cursor.execute("RELEASE SAVEPOINT sp_settlement")
            inserted += 1
        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp_settlement")
            logger.error(f"   Error insertando liquidación {row.get('liquidacion_id')}: {e}")
            errors += 1

    logger.info(f"   Insertadas {inserted} liquidaciones ({errors} errores)")
    return inserted, errors


def _insert_items(cursor, items):
    """Inserta filas en rh.historical_settlement_items."""
    sql = """
        INSERT INTO rh.historical_settlement_items (
            liquidacion_id, employee_id, rut,
            item_type, income_type, subtype, name,
            amount, taxable, imponible, anticipo,
            credit_type, institution, description, code, item_code,
            create_at
        ) VALUES (
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            NOW()
        )
    """
    inserted = errors = 0
    for item in items:
        try:
            cursor.execute("SAVEPOINT sp_item")
            cursor.execute(sql, (
                item["liquidacion_id"], item["employee_id"], item["rut"],
                item["item_type"], item["income_type"], item["subtype"], item["name"],
                item["amount"], item["taxable"], item["imponible"], item["anticipo"],
                item["credit_type"], item["institution"], item["description"],
                item["code"], item["item_code"],
            ))
            cursor.execute("RELEASE SAVEPOINT sp_item")
            inserted += 1
        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp_item")
            logger.error(
                f"   Error insertando item de liquidación {item.get('liquidacion_id')}: {e}"
            )
            errors += 1

    logger.info(f"   Insertados {inserted} items ({errors} errores)")
    return inserted, errors


# =============================================================================
# ORQUESTADORES
# =============================================================================

def ejecutar_carga_historica(conn, token, base_url, start_year=2019, start_month=1):
    """
    Carga histórica completa: itera todos los períodos desde start_year/start_month
    hasta el mes actual inclusive y llama a ejecutar_flujo_settlements por cada uno.

    Pensado para ejecución única al inicializar la BD.
    Devuelve dict con totales: periodos_ok, periodos_error, periodos_vacios.
    """
    current_year, current_month = start_year, start_month
    today = datetime.now()
    periods = []
    while (current_year, current_month) <= (today.year, today.month):
        periods.append((current_year, current_month))
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1

    total = len(periods)
    ok = errors = vacios = 0

    logger.info(f"Iniciando carga histórica: {total} períodos desde {start_month:02d}-{start_year}")

    for i, (year, month) in enumerate(periods, start=1):
        label = f"{month:02d}-{year}"
        logger.info(f"[{i}/{total}] Procesando {label}...")
        try:
            exito = ejecutar_flujo_settlements(conn, token, base_url, year, month)
            if exito:
                ok += 1
            else:
                vacios += 1  # API sin datos para ese período (mes anterior a contrataciones, etc.)
        except Exception as e:
            logger.error(f"[{i}/{total}] Error inesperado en período {label}: {e}")
            errors += 1
        time.sleep(0.5)  # pausa breve entre períodos para no saturar la API

    logger.info(
        f"Carga histórica finalizada — OK: {ok}, Sin datos: {vacios}, Errores: {errors}"
    )
    return {"periodos_ok": ok, "periodos_vacios": vacios, "periodos_error": errors}


def ejecutar_flujo_settlements(conn, token, base_url, year, month):
    """
    ETL completo para un período (year/month):
      1. Extrae liquidaciones desde la API.
      2. Elimina registros previos del período.
      3. Inserta liquidaciones y sus items.

    Devuelve True si el flujo terminó sin errores fatales.
    """
    date_param = f"01-{month:02d}-{year}"
    label = f"{month:02d}-{year}"
    logger.info(f"Iniciando ETL settlements para período {label}...")

    # Extracción
    try:
        raw_liqs = get_all_settlements(token, base_url, date_param)
    except Exception as e:
        logger.error(f"Error extrayendo liquidaciones del período {label}: {e}")
        return False

    logger.info(f"Liquidaciones obtenidas de API: {len(raw_liqs)}")

    if not raw_liqs:
        logger.warning(f"API no devolvió liquidaciones para {label}. Abortando carga.")
        return False

    # Transformación
    settlements = [_flatten_liquidation(l) for l in raw_liqs]
    items = _explode_items(raw_liqs)
    logger.info(f"Items de liquidación a insertar: {len(items)}")

    # Carga
    cursor = conn.cursor()
    try:
        _delete_period(cursor, year, month)
        _insert_settlements(cursor, settlements)
        _insert_items(cursor, items)
        conn.commit()
        logger.info(f"ETL settlements {label} completado correctamente.")
        return True
    except Exception as e:
        logger.error(f"Error fatal durante la carga de settlements {label}: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
