import requests
import time
import os
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import pandas as pd
import pyodbc
import logging
import sys
import json
import traceback
from pathlib import Path
from dotenv import load_dotenv

# Cargar .env
load_dotenv(override=True)

SCRIPT_PATH = Path(__file__).resolve()
SCRIPT_DIR = SCRIPT_PATH.parent 
PROJECT_ROOT = SCRIPT_DIR.parent
LOG_FOLDER = PROJECT_ROOT / "logs"
LOG_FOLDER.mkdir(parents=True, exist_ok=True)
log_filename = LOG_FOLDER / f"log_settlements_update.log"

# ========== CONFIGURACIÓN LOGGING ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        #logging.StreamHandler()  
    ]
)

logger = logging.getLogger(__name__)

# ========== CONFIGURACIÓN BD SQL SERVER ==========
SQL_SERVER = os.getenv('SQL_SERVER')
SQL_DATABASE = os.getenv('SQL_DATABASE')

# Nombre de las tablas
TABLE_PAYROLLS = "historical_settlements"
TABLE_ITEMS = "historical_settlement_items"

# Nombre de la clave de cifrado 
CEK_NAME = "CEK_Sueldos_Nueva"  
PAGE_SIZE = 100

def get_db_connection():
    """Establece conexión con SQL Server usando Windows Authentication y Always Encrypted."""
    conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={os.getenv('SQL_SERVER')};"
            f"DATABASE={os.getenv('SQL_DATABASE')};"
            f"UID={os.getenv('SQL_USER')};"
            f"PWD={os.getenv('SQL_PASSWORD')};"
            "ColumnEncryption=Enabled;"      
            "TrustServerCertificate=yes;"
            "ConnectRetryCount=3;"
            "ConnectRetryInterval=10;"
            "KeepAlive=30"
        )
    return pyodbc.connect(conn_str)

# ------------------ Utils API ------------------
def request_with_retry(session, url, headers, max_retries=5, backoff_base=1.5, timeout=30):
    """
    Realiza GET con reintentos exponenciales para errores de red / 5xx.
    Devuelve response.json() o lanza excepción si falla después de retries.
    """
    attempt = 0
    while attempt < max_retries:
        try:
            resp = session.get(url, headers=headers, timeout=timeout)
            if 500 <= resp.status_code < 600:
                raise requests.exceptions.HTTPError(f"Server error {resp.status_code}")
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.HTTPError) as e:
            attempt += 1
            wait = backoff_base ** attempt
            logger.warning(f"  Request error (attempt {attempt}/{max_retries}) for URL: {url}\n     {e}\n     Waiting {wait:.1f}s before retry")
            time.sleep(wait)
        except Exception as e:
            attempt += 1
            wait = backoff_base ** attempt
            logger.warning(f"  Unexpected error (attempt {attempt}/{max_retries}): {e}\n     Waiting {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError(f"Failed to GET {url} after {max_retries} attempts")

def get_all_liquidaciones_data(base_url, token, date_param):
    """
    Pagina y obtiene todas las liquidaciones para date_param (formato dd-mm-YYYY).
    Usa request_with_retry para tolerar errores temporales.
    Devuelve lista de liquidaciones (list of dicts).
    """
    session = requests.Session()
    headers = {"auth_token": token}
    page = 1
    all_data = []

    while True:
        url = f"{base_url}?date={date_param}&page_size={PAGE_SIZE}&page={page}"
        try:
            data = request_with_retry(session, url, headers)
        except requests.exceptions.HTTPError as e:
            if hasattr(e, 'response') and e.response.status_code == 401:
                logger.error(" Error de autenticación! Token inválido o expirado.")
                raise
            else:
                logger.error(f"   Error HTTP inesperado en la página {page} para {date_param}: {e}")
                raise
        except Exception as e:
            logger.error(f"   Error obteniendo página {page} para {date_param}: {e}")
            raise

        page_data = data.get("data", [])
        if not page_data:
            break

        all_data.extend(page_data)

        if len(page_data) < PAGE_SIZE:
            break

        page += 1
        time.sleep(0.2)

    return all_data

# ------------------ Transformaciones ------------------
def safe_float(value):
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == '':
        return None
    if value in ['N/A', '-', 'null', 'NULL']:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        logger.warning(f"No se pudo convertir a float: {value} (tipo: {type(value)})")
        return None

def flatten_liquidation(liq):
    """
    Genera un dict plano (fila) a partir de una liquidación.
    """
    year = liq.get("year")
    month = liq.get("month")
    
    if year and month:
        try:
            year_int = int(year)
            month_int = int(month)
            pay_period = date(year_int, month_int, 1)
            period_label = f"{month_int:02d}-{year_int}"
        except Exception as e:
            logger.warning(f"ERROR convirtiendo fecha: year={year}, month={month}, error={e}")
            pay_period = None
            period_label = None
            year_int = None
            month_int = None
    else:
        pay_period = None
        period_label = None
        year_int = None
        month_int = None

    liquidacion_id = liq.get("liquidacion_id")
    if not liquidacion_id:
        logger.warning(f"WARNING: liquidacion_id es None o vacío para registro: {liq.get('employee_id')}")

    flattened = {
        "Liquidacion_ID": liquidacion_id,
        "ID_Persona": liq.get("person_id"),
        "ID_Empleado": liq.get("employee_id"),
        "RUT": liq.get("rut"),
        "Periodo": period_label,
        "Ano": year_int,
        "Mes": month_int,
        "Pay_Period": pay_period,
        "Dias_Trabajados": safe_float(liq.get("worked_days")),           
        "Dias_No_Trabajados": safe_float(liq.get("noworked_days")),      
        "Ingreso_Bruto": safe_float(liq.get("income_gross")),            
        "Ingreso_Neto": safe_float(liq.get("income_net")),               
        "Ingreso_AFP": safe_float(liq.get("income_afp")),                
        "Ingreso_IPS": safe_float(liq.get("income_ips")),                
        "Total_Ingresos_Imponibles": safe_float(liq.get("total_income_taxable")),        
        "Total_Ingresos_No_Imponibles": safe_float(liq.get("total_income_notaxable")),   
        "Total_Descuentos_Legales": safe_float(liq.get("total_legal_discounts")),        
        "Total_Otros_Descuentos": safe_float(liq.get("total_other_discounts")),          
        "Liquido_a_Pagar": safe_float(liq.get("liquid_reach")),          
        "Base_Imponible": safe_float(liq.get("taxable_base")),           
        "Cerrada": liq.get("closed"),
        "raw_payload": json.dumps(liq) if liq else None
    }
    return flattened

def explode_items_for_month(liq_list):
    """
    Recibe lista de liquidaciones (raw) y genera listado de items (filas).
    """
    items = []
    for liq in liq_list:
        lid = liq.get("liquidacion_id")
        pid = liq.get("person_id")
        eid = liq.get("employee_id")
        rut = liq.get("rut") 
        lines = liq.get("lines_settlement", []) or []
        for line in lines:
            items.append({
                "Liquidacion_ID": lid,
                "ID_Persona": pid,
                "ID_Empleado": eid,
                "RUT": rut,
                "type": line.get("type"),
                "income_type": line.get("income_type"),
                "subtype": line.get("subtype"),
                "name": line.get("name"),
                "amount": safe_float(line.get("amount")),
                "taxable": line.get("taxable"),
                "imponible": line.get("imponible"),
                "anticipo": line.get("anticipo"),
                "credit_type": line.get("credit_type"),
                "institution": line.get("institution"),
                "description": line.get("description"),
                "code": line.get("code"),
                "item_code": line.get("item_code")
            })
    return items

# ------------------ Guardado en BD SQL Server ------------------
def ensure_tables_exist(cursor, conexion):
    # Verificar si la tabla payrolls existe
    cursor.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'dbo' 
        AND TABLE_NAME = ?
    """, TABLE_PAYROLLS)
    
    if cursor.fetchone()[0] == 0:
        logger.info(f"Creando tabla {TABLE_PAYROLLS}...")
        
        # TODO: Agregar ENCRYPTED WITH cuando se definan las columnas sensibles
        create_payrolls_sql = f"""
                CREATE TABLE dbo.{TABLE_PAYROLLS} (
                    id BIGINT IDENTITY(1,1) PRIMARY KEY,
                    Liquidacion_ID BIGINT NULL,
                    ID_Persona BIGINT NULL,
                    ID_Empleado BIGINT NULL,
                    RUT VARCHAR(50) NULL,
                    Periodo VARCHAR(16) NULL,
                    Ano INT NULL,
                    Mes INT NULL,
                    Pay_Period DATE NULL,
                    Dias_Trabajados FLOAT NULL,
                    Dias_No_Trabajados FLOAT NULL,
                    Ingreso_Bruto FLOAT
                        ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{CEK_NAME}],
                                        ENCRYPTION_TYPE = RANDOMIZED,
                                        ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NULL,
                    Ingreso_Neto FLOAT
                        ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{CEK_NAME}],
                                        ENCRYPTION_TYPE = RANDOMIZED,
                                        ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NULL,
                    Ingreso_AFP FLOAT
                        ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{CEK_NAME}],
                                        ENCRYPTION_TYPE = RANDOMIZED,
                                        ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NULL,
                    Ingreso_IPS FLOAT
                        ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{CEK_NAME}],
                                        ENCRYPTION_TYPE = RANDOMIZED,
                                        ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NULL,
                    Total_Ingresos_Imponibles FLOAT
                        ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{CEK_NAME}],
                                        ENCRYPTION_TYPE = RANDOMIZED,
                                        ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NULL,
                    Total_Ingresos_No_Imponibles FLOAT
                        ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{CEK_NAME}],
                                        ENCRYPTION_TYPE = RANDOMIZED,
                                        ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NULL,
                    Total_Descuentos_Legales FLOAT
                        ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{CEK_NAME}],
                                        ENCRYPTION_TYPE = RANDOMIZED,
                                        ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NULL,
                    Total_Otros_Descuentos FLOAT
                        ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{CEK_NAME}],
                                        ENCRYPTION_TYPE = RANDOMIZED,
                                        ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NULL,
                    Liquido_a_Pagar FLOAT
                        ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{CEK_NAME}],
                                        ENCRYPTION_TYPE = RANDOMIZED,
                                        ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NULL,
                    Base_Imponible FLOAT
                        ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{CEK_NAME}],
                                        ENCRYPTION_TYPE = RANDOMIZED,
                                        ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NULL,
                    Cerrada BIT NULL,
                    raw_payload NVARCHAR(MAX) NULL,
                    created_at DATETIME2(7) DEFAULT GETDATE()
                );

                -- Índice único compuesto para evitar duplicados
                CREATE UNIQUE INDEX ux_{TABLE_PAYROLLS}_liquidacion_period 
                ON dbo.{TABLE_PAYROLLS} (Liquidacion_ID, Ano, Mes) 
                WHERE Liquidacion_ID IS NOT NULL AND Ano IS NOT NULL AND Mes IS NOT NULL;

                -- Índices adicionales para performance
                CREATE INDEX idx_{TABLE_PAYROLLS}_empleado ON dbo.{TABLE_PAYROLLS} (ID_Empleado);
                CREATE INDEX idx_{TABLE_PAYROLLS}_periodo ON dbo.{TABLE_PAYROLLS} (Pay_Period);
                CREATE INDEX idx_{TABLE_PAYROLLS}_ano_mes ON dbo.{TABLE_PAYROLLS} (Ano, Mes);
                CREATE INDEX idx_{TABLE_PAYROLLS}_rut ON dbo.{TABLE_PAYROLLS} (RUT);
                """
        
        cursor.execute(create_payrolls_sql)
        conexion.commit()
        logger.info(f"✅ Tabla {TABLE_PAYROLLS} creada con índices.")
    else:
        logger.info(f"Tabla {TABLE_PAYROLLS} ya existe.")
    
    # Verificar si la tabla items existe
    cursor.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'dbo' 
        AND TABLE_NAME = ?
    """, TABLE_ITEMS)
    
    if cursor.fetchone()[0] == 0:
        logger.info(f"Creando tabla {TABLE_ITEMS}...")
        
        create_items_sql = f""" 
                CREATE TABLE dbo.{TABLE_ITEMS} (
                    id BIGINT IDENTITY(1,1) PRIMARY KEY,
                    Liquidacion_ID BIGINT NULL,
                    ID_Persona BIGINT NULL,
                    ID_Empleado BIGINT NULL,
                    RUT VARCHAR(50) NULL,
                    item_type VARCHAR(50) NULL,
                    income_type VARCHAR(100) NULL,
                    subtype VARCHAR(100) NULL,
                    name VARCHAR(255) NULL,
                    amount FLOAT
                        ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{CEK_NAME}],
                                        ENCRYPTION_TYPE = RANDOMIZED,
                                        ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NULL,
                    taxable BIT NULL,
                    imponible BIT NULL,
                    anticipo BIT NULL,
                    credit_type VARCHAR(100) NULL,
                    institution VARCHAR(255) NULL,
                    description NVARCHAR(MAX) NULL,
                    code VARCHAR(100) NULL,
                    item_code VARCHAR(100) NULL,
                    created_at DATETIME2(7) DEFAULT GETDATE()
                );

                CREATE INDEX idx_{TABLE_ITEMS}_liquidacion ON dbo.{TABLE_ITEMS} (Liquidacion_ID);
                CREATE INDEX idx_{TABLE_ITEMS}_empleado ON dbo.{TABLE_ITEMS} (ID_Empleado);
                CREATE INDEX idx_{TABLE_ITEMS}_rut ON dbo.{TABLE_ITEMS} (RUT);
                """
        
        cursor.execute(create_items_sql)
        conexion.commit()
        logger.info(f"✅ Tabla {TABLE_ITEMS} creada.")
    else:
        logger.info(f"Tabla {TABLE_ITEMS} ya existe.")

def save_payrolls_batch(cursor, conexion, df_payrolls):
    """
    Inserta/actualiza liquidaciones usando MERGE (equivalente a UPSERT).
    """
    if df_payrolls is None or df_payrolls.empty:
        logger.warning("   No hay filas de liquidaciones para insertar.")
        return 0

    df = df_payrolls.copy()
    df = df.dropna(subset=['Liquidacion_ID', 'Ano', 'Mes'])
    
    if df.empty:
        logger.warning("   No quedan filas válidas después del filtrado.")
        return 0

    procesados = 0
    errores = 0

    for _, row in df.iterrows():
        try:
            sql = f"""
            MERGE INTO dbo.{TABLE_PAYROLLS} AS T
            USING (
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ) AS S (
                Liquidacion_ID, ID_Persona, ID_Empleado, RUT, Periodo, Ano, Mes, Pay_Period,
                Dias_Trabajados, Dias_No_Trabajados, Ingreso_Bruto, Ingreso_Neto, Ingreso_AFP,
                Ingreso_IPS, Total_Ingresos_Imponibles, Total_Ingresos_No_Imponibles,
                Total_Descuentos_Legales, Total_Otros_Descuentos, Liquido_a_Pagar,
                Base_Imponible, Cerrada, raw_payload
            )
            ON (T.Liquidacion_ID = S.Liquidacion_ID AND T.Ano = S.Ano AND T.Mes = S.Mes)
            
            WHEN MATCHED THEN
                UPDATE SET
                    T.ID_Persona = S.ID_Persona,
                    T.ID_Empleado = S.ID_Empleado,
                    T.RUT = S.RUT,
                    T.Periodo = S.Periodo,
                    T.Pay_Period = S.Pay_Period,
                    T.Dias_Trabajados = S.Dias_Trabajados,
                    T.Dias_No_Trabajados = S.Dias_No_Trabajados,
                    T.Ingreso_Bruto = S.Ingreso_Bruto,
                    T.Ingreso_Neto = S.Ingreso_Neto,
                    T.Ingreso_AFP = S.Ingreso_AFP,
                    T.Ingreso_IPS = S.Ingreso_IPS,
                    T.Total_Ingresos_Imponibles = S.Total_Ingresos_Imponibles,
                    T.Total_Ingresos_No_Imponibles = S.Total_Ingresos_No_Imponibles,
                    T.Total_Descuentos_Legales = S.Total_Descuentos_Legales,
                    T.Total_Otros_Descuentos = S.Total_Otros_Descuentos,
                    T.Liquido_a_Pagar = S.Liquido_a_Pagar,
                    T.Base_Imponible = S.Base_Imponible,
                    T.Cerrada = S.Cerrada,
                    T.raw_payload = S.raw_payload
            
            WHEN NOT MATCHED THEN
                INSERT (
                    Liquidacion_ID, ID_Persona, ID_Empleado, RUT, Periodo, Ano, Mes, Pay_Period,
                    Dias_Trabajados, Dias_No_Trabajados, Ingreso_Bruto, Ingreso_Neto, Ingreso_AFP,
                    Ingreso_IPS, Total_Ingresos_Imponibles, Total_Ingresos_No_Imponibles,
                    Total_Descuentos_Legales, Total_Otros_Descuentos, Liquido_a_Pagar,
                    Base_Imponible, Cerrada, raw_payload
                )
                VALUES (
                    S.Liquidacion_ID, S.ID_Persona, S.ID_Empleado, S.RUT, S.Periodo, S.Ano, S.Mes, S.Pay_Period,
                    S.Dias_Trabajados, S.Dias_No_Trabajados, S.Ingreso_Bruto, S.Ingreso_Neto, S.Ingreso_AFP,
                    S.Ingreso_IPS, S.Total_Ingresos_Imponibles, S.Total_Ingresos_No_Imponibles,
                    S.Total_Descuentos_Legales, S.Total_Otros_Descuentos, S.Liquido_a_Pagar,
                    S.Base_Imponible, S.Cerrada, S.raw_payload
                );
            """
            
            values = (
                row.get("Liquidacion_ID"), row.get("ID_Persona"), row.get("ID_Empleado"),
                row.get("RUT"), row.get("Periodo"), row.get("Ano"), row.get("Mes"),
                row.get("Pay_Period"), row.get("Dias_Trabajados"), row.get("Dias_No_Trabajados"),
                row.get("Ingreso_Bruto"), row.get("Ingreso_Neto"), row.get("Ingreso_AFP"),
                row.get("Ingreso_IPS"), row.get("Total_Ingresos_Imponibles"),
                row.get("Total_Ingresos_No_Imponibles"), row.get("Total_Descuentos_Legales"),
                row.get("Total_Otros_Descuentos"), row.get("Liquido_a_Pagar"),
                row.get("Base_Imponible"), row.get("Cerrada"), row.get("raw_payload")
            )
            
            cursor.execute(sql, values)
            procesados += 1
            
            if procesados % 100 == 0:
                logger.info(f"   Procesados {procesados} registros...")
                
        except Exception as e:
            logger.error(f"   Error procesando liquidación {row.get('Liquidacion_ID')}: {e}")
            errores += 1

    conexion.commit()
    logger.info(f"   ✅ Insertados/actualizados: {procesados} registros en {TABLE_PAYROLLS}")
    if errores > 0:
        logger.warning(f"   ⚠️  Errores: {errores}")
    
    return procesados

def delete_items_for_period(cursor, conexion, year, month):
    """
    Elimina items existentes para un período antes de reinsertarlos.
    """
    sql = f"""
        DELETE FROM dbo.{TABLE_ITEMS} 
        WHERE Liquidacion_ID IN (
            SELECT Liquidacion_ID FROM dbo.{TABLE_PAYROLLS} 
            WHERE Ano = ? AND Mes = ?
        )
    """
    cursor.execute(sql, (year, month))
    deleted = cursor.rowcount
    conexion.commit()
    logger.info(f"   🗑️ Eliminados {deleted} items existentes del período {month:02d}-{year}")
    return deleted

def save_items_batch(cursor, conexion, items_df):
    """
    Inserta items en la tabla de settlement items.
    """
    if items_df is None or items_df.empty:
        logger.warning("   No hay items para insertar.")
        return 0

    procesados = 0
    errores = 0

    for _, row in items_df.iterrows():
        try:
            sql = f"""
                INSERT INTO dbo.{TABLE_ITEMS} (
                    Liquidacion_ID, ID_Persona, ID_Empleado, RUT, item_type, income_type, subtype,
                    name, amount, taxable, imponible, anticipo, credit_type, institution,
                    description, code, item_code
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """

            values = (
                row.get("Liquidacion_ID"), row.get("ID_Persona"), row.get("ID_Empleado"),
                row.get("RUT"), 
                row.get("type"), row.get("income_type"), row.get("subtype"),
                row.get("name"), row.get("amount"), row.get("taxable"),
                row.get("imponible"), row.get("anticipo"), row.get("credit_type"),
                row.get("institution"), row.get("description"), row.get("code"),
                row.get("item_code")
            )
            
            cursor.execute(sql, values)
            procesados += 1
            
        except Exception as e:
            logger.error(f"   Error insertando item: {e}")
            errores += 1

    conexion.commit()
    logger.info(f"   ✅ Insertados: {procesados} items en {TABLE_ITEMS}")
    if errores > 0:
        logger.warning(f"   ⚠️  Errores: {errores}")
    
    return procesados

# ------------------ Periodos mensuales ------------------
def generate_monthly_periods(start_year=2019, start_month=1):
    periods = []
    current = datetime(start_year, start_month, 1)
    today = datetime.now()
    while current.year < today.year or (current.year == today.year and current.month <= today.month):
        periods.append({
            "year": current.year,
            "month": current.month,
            "date_param": f"01-{current.strftime('%m')}-{current.year}",
            "label": f"{current.strftime('%m')}-{current.year}"
        })
        current += relativedelta(months=1)
    return periods

# ------------------ Verificación ------------------
def verify_data_insertion(cursor, year, month):
    """
    Verifica que los datos se insertaron correctamente.
    """
    cursor.execute(f"""
        SELECT COUNT(*) FROM dbo.{TABLE_PAYROLLS} 
        WHERE Ano = ? AND Mes = ?
    """, year, month)
    count_total = cursor.fetchone()[0]
    
    cursor.execute(f"""
        SELECT COUNT(DISTINCT Liquidacion_ID) FROM dbo.{TABLE_PAYROLLS} 
        WHERE Ano = ? AND Mes = ?
    """, year, month)
    count_distinct_ids = cursor.fetchone()[0]
    
    logger.info(f"VERIFICACIÓN - Año: {year}, Mes: {month}")
    logger.info(f"  Registros totales: {count_total}")
    logger.info(f"  IDs distintos: {count_distinct_ids}")
    
    return count_total > 0 and count_total == count_distinct_ids

# ------------------ MAIN PROCESS ------------------
def main():
    logger.info("========== INICIANDO PROCESO ETL DE LIQUIDACIONES (SQL SERVER) ==========")
    
    token = os.getenv("TOKEN")
    base_url = os.getenv("URL_SETTLEMENTS_CHILE")
    
    if not token or not base_url:
        logger.error("ERROR: TOKEN o URL_SETTLEMENTS_CHILE no configurados en .env")
        return

    conexion = None
    cursor = None

    try:
        conexion = get_db_connection()
        cursor = conexion.cursor()
        logger.info(f"✅ Conectado a SQL Server: {SQL_SERVER}/{SQL_DATABASE}")
        
        ensure_tables_exist(cursor, conexion)
        
        # Procesar solo el mes actual
        today = datetime.now()
        
        #Procesar meses anteriores
        #today = datetime.now() - relativedelta(months=1)
        
        
        periods = [{
            "year": today.year,
            "month": today.month,
            "date_param": f"01-{today.strftime('%m')}-{today.year}",
            "label": f"{today.strftime('%m')}-{today.year}"
        }]
        
        # Para procesar todos los períodos:
        #periods = generate_monthly_periods(2019, 1)
        
        logger.info(f"Períodos a procesar: {len(periods)}")

        total_processed = 0
        total_items = 0
        failed_periods = []

        for i, p in enumerate(periods, start=1):
            logger.info("\n" + "="*60)
            logger.info(f"Procesando período {i}/{len(periods)}: {p['label']}")
            
            try:
                liqs = get_all_liquidaciones_data(base_url, token, p['date_param'])
                logger.info(f"   Liquidaciones obtenidas: {len(liqs)}")

                flat_rows = [flatten_liquidation(l) for l in liqs]
                df_liqs = pd.DataFrame(flat_rows)
                
                inserted = save_payrolls_batch(cursor, conexion, df_liqs)
                total_processed += inserted

                verify_data_insertion(cursor, p['year'], p['month'])

                delete_items_for_period(cursor, conexion, p['year'], p['month'])
                
                items = explode_items_for_month(liqs)
                df_items = pd.DataFrame(items)
                inserted_items = save_items_batch(cursor, conexion, df_items)
                total_items += inserted_items

                time.sleep(0.3)

            except Exception as e:
                logger.error(f"   Falló procesamiento para período {p['label']}: {e}")
                logger.error(f"   Traceback: {traceback.format_exc()}")
                failed_periods.append({"period": p['label'], "error": str(e)})
                conexion.rollback()
                continue

        logger.info("\n" + "="*60)
        logger.info("PROCESO FINALIZADO")
        logger.info(f"Total liquidaciones procesadas: {total_processed}")
        logger.info(f"Total items procesados: {total_items}")
        
        if failed_periods:
            logger.warning("Períodos con error:")
            for f in failed_periods:
                logger.warning(f" - {f['period']}: {f['error']}")

    except Exception as e:
        logger.critical(f"Error general en la sincronización: {e}", exc_info=True)
        if conexion:
            conexion.rollback()

    finally:
        if cursor:
            cursor.close()
        if conexion:
            conexion.close()
            logger.info("Conexión a SQL Server cerrada.")

if __name__ == "__main__":
    logger.info("Ejecutando sincronización programada...")
    main()
    sys.exit(0)