#Librerías
import time
import datetime
from datetime import timedelta
import sys
from dotenv import load_dotenv
import os
import logging  
import pyodbc
load_dotenv()

# "logs" se creará en el directorio donde se ejecuta el script.
LOG_FOLDER = "logs" 
# Define el nombre base de tu script para el archivo log
SCRIPT_NAME = "Registro_inserciones"

# Nombre del archivo de log diario (Ej: nuevo_script_2025-09-24.log)
LOG_FILENAME = f"{SCRIPT_NAME}_{datetime.datetime.today().date()}.log"
LOG_PATH = os.path.join(LOG_FOLDER, LOG_FILENAME)

# Asegura que la carpeta de logs exista
os.makedirs(LOG_FOLDER, exist_ok=True)

# Configura el logger principal
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,  # Nivel de registro (INFO, DEBUG, ERROR, etc.)
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configura un handler para que los mensajes también se muestren en la consola/salida (stdout)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(console_handler)

# --- FIN DE CONFIGURACIÓN DEL LOGGER ---

# Configuración base de datos

DB_SERVER = os.getenv("SQL_SERVER") 
DB_NAME = os.getenv("SQL_DATABASE")

# Función para calcular la fecha de alerta
def calcular_fecha_alerta(empleado):
    """
    Calcula la fecha de alerta según el tipo de contrato y status.
    """
    tipo_contrato = (empleado.get("contract_type") or "").lower()
    status = (empleado.get("status") or "").lower()
    metodo_pago = (empleado.get("payment_method") or "").lower()
    fecha_activacion = empleado.get("active_since")
    termino_primer_plazo = empleado.get("contract_finishing_date_1")
    termino_segundo_plazo = empleado.get("contract_finishing_date_2")
    
    # Solo considerar empleados activos con transferencia bancaria o tipo de contrato fijo
    if status != "activo" or metodo_pago != "transferencia bancaria" or tipo_contrato == "indefinido":
        return None
    
    try:
        if fecha_activacion:
            fecha_activacion = datetime.datetime.strptime(fecha_activacion, "%Y-%m-%d")
            primer_plazo = datetime.datetime.strptime(termino_primer_plazo, "%Y-%m-%d") if termino_primer_plazo else None
            segundo_plazo = datetime.datetime.strptime(termino_segundo_plazo, "%Y-%m-%d") if termino_segundo_plazo else None
        else:
            return None
        
        fecha_alerta, motivo, tipo_alerta = None, None, None
        
        # Primera alerta → paso a segundo plazo
        if tipo_contrato == "fijo" and primer_plazo is not None and segundo_plazo is not None:
            fecha_alerta = primer_plazo - timedelta(days=0) # Alerta el mismo día del término del primer plazo
            motivo = "Renovación a segundo plazo"
            tipo_alerta = "SEGUNDO_PLAZO"
            dias_vencimiento = (datetime.datetime.now() - fecha_alerta).days
        
        # Segunda alerta → paso a indefinido  
        elif tipo_contrato == "fijo" and primer_plazo is not None and segundo_plazo is None:
            fecha_alerta = primer_plazo - timedelta(days=0) # Alerta el mismo día del término del segundo plazo
            motivo = "Paso a contrato indefinido"
            tipo_alerta = "INDEFINIDO"
            dias_vencimiento = (datetime.datetime.now() - fecha_alerta).days
        
        if fecha_alerta:
            return {
                "fecha_alerta": fecha_alerta.strftime("%Y-%m-%d"),
                "motivo": motivo,
                "tipo_alerta": tipo_alerta,
                "dias_desde_inicio": (datetime.datetime.now() - fecha_activacion).days,
                "dias_vencimiento": dias_vencimiento
            }
        else:
            return None
            
    except ValueError as e:
        print(f"Error procesando fechas para {empleado.get('full_name')}: {e}")
        return None


def obtener_info_jefe(id_boss, rut_boss, empleados_lista):
    if not id_boss and not rut_boss:
        return None
    
    for empleado in empleados_lista:
        if (id_boss and empleado.get("id") == id_boss) or (rut_boss and empleado.get("rut") == rut_boss):
            return empleado 
    
    return None

#Función para generar alertas
def generar_alertas(cursor, conexion):
    """
    Procesa los empleados de la base de datos y genera alertas de contrato.
    """
    # Crear la tabla de alertas si no existe (CON LAS NUEVAS COLUMNAS)
    sql_create_table = """
    IF OBJECT_ID('dbo.contract_alerts', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.contract_alerts (
            employee_id INT NOT NULL,
            employee_name VARCHAR(255),
            employee_rut VARCHAR(50) PRIMARY KEY,
            employee_role VARCHAR(255),
            employee_start_date DATE,
            email VARCHAR(255),
            employee_contract_type VARCHAR(50),
            boss_name VARCHAR(255),
            boss_email VARCHAR(255),
            boss_of_boss_name VARCHAR(255),
            boss_of_boss_email VARCHAR(255),
            alert_date DATE NOT NULL,
            alert_type VARCHAR(50),
            alert_reason VARCHAR(MAX),
            expiration INT,
            days_since_start INT,
            first_alert_sent BIT DEFAULT 0,
            second_alert_sent BIT DEFAULT 0,
            created_at DATETIME2 DEFAULT SYSDATETIME(),
            updated_at DATETIME2 NULL
        );

        CREATE INDEX idx_employee_id ON dbo.contract_alerts(employee_id);
        CREATE INDEX idx_alert_date ON dbo.contract_alerts(alert_date);

        PRINT 'Tabla "contract_alerts" e índices creados exitosamente.';
    END
    ELSE
    BEGIN
        PRINT 'La tabla "contract_alerts" ya existe. No se realizaron cambios.';
    END
    """
    # 2. Query para CREAR EL TRIGGER (SIN 'GO')
    #    (Se ejecuta en un lote separado)
    sql_create_trigger = """
    IF OBJECT_ID('trg_contract_alerts_update', 'TR') IS NULL
    BEGIN
        -- Usamos EXEC para ejecutar DDL (como CREATE TRIGGER) dentro de un bloque IF
        EXEC('
        CREATE TRIGGER trg_contract_alerts_update
        ON dbo.contract_alerts
        AFTER UPDATE
        AS
        BEGIN
            SET NOCOUNT ON;
            UPDATE t
            SET updated_at = SYSDATETIME()
            FROM dbo.contract_alerts AS t
            INNER JOIN inserted AS i ON t.employee_rut = i.employee_rut;
        END
        ')
        PRINT 'Trigger "trg_contract_alerts_update" creado exitosamente.';
    END
    ELSE
    BEGIN
        PRINT 'El trigger "trg_contract_alerts_update" ya existe.';
    END
    """
    # 3. Ejecutar ambas queries por separado
    print("Verificando/creando tabla 'contract_alerts'...")
    cursor.execute(sql_create_table)
    print("Verificando/creando trigger 'trg_contract_alerts_update'...")
    cursor.execute(sql_create_trigger)
    # Es buena idea hacer commit de los cambios DDL (creación de objetos)
    conexion.commit() 
    print("Tabla y Trigger verificados exitosamente")
    
    # Consulta para obtener todos los empleados necesarios (sin cambios aquí)
    sql_empleados = """
    SELECT 
        id, person_id, full_name, first_name, rut, email, name_role,
        start_date, contract_type, id_boss, rut_boss,
        active_since, contract_finishing_date_1, contract_finishing_date_2,
        status, payment_method
    FROM dbo.employees 
    WHERE status = 'activo'
    """

    cursor.execute(sql_empleados)
    empleados_db = cursor.fetchall()
    
    empleados_lista = []
    columnas = [
        'id', 'person_id', 'full_name', 'first_name', 'rut', 'email', 'name_role',
        'start_date', 'contract_type', 'id_boss', 'rut_boss',
        'active_since', 'contract_finishing_date_1', 'contract_finishing_date_2',
        'status', 'payment_method'
    ]
    
    for empleado_tupla in empleados_db:
        empleado_dict = {}
        for i, columna in enumerate(columnas):
            valor = empleado_tupla[i]
            if isinstance(valor, datetime.date):
                valor = valor.strftime("%Y-%m-%d")
            empleado_dict[columna] = valor
        empleados_lista.append(empleado_dict)
    
    print(f"Cargados {len(empleados_lista)} empleados activos desde la BD")
    
    alertas_insertadas = 0
    empleados_procesados = 0
    errores = 0

    for empleado in empleados_lista:
        empleados_procesados += 1
        alerta = calcular_fecha_alerta(empleado)
        
        if alerta:
            try:
                jefe_directo = obtener_info_jefe(
                    empleado.get("id_boss"), 
                    empleado.get("rut_boss"),
                    empleados_lista
                )
                boss_name = jefe_directo.get("first_name") if jefe_directo else None
                boss_email = jefe_directo.get("email") if jefe_directo else None
                jefe_del_jefe = None
                if jefe_directo:
                    jefe_del_jefe = obtener_info_jefe(
                        jefe_directo.get("id_boss"),
                        jefe_directo.get("rut_boss"),
                        empleados_lista
                    )
                boss_of_boss_name = jefe_del_jefe.get("first_name") if jefe_del_jefe else None
                boss_of_boss_email = jefe_del_jefe.get("email") if jefe_del_jefe else None

                sql_insert = """
                MERGE INTO dbo.contract_alerts AS Target
                USING (
                    VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                ) AS Source (
                    employee_id, employee_name, 
                    employee_rut, employee_role, 
                    email,
                    employee_start_date, employee_contract_type,
                    boss_name, boss_email,
                    boss_of_boss_name, boss_of_boss_email,
                    alert_date, alert_type, 
                    alert_reason, days_since_start,
                    expiration
                )
                -- 3. Define la condición de "duplicado" (la Primary Key)
                ON (Target.employee_rut = Source.employee_rut)

                -- 4. Acción para ON DUPLICATE KEY UPDATE
                WHEN MATCHED THEN
                    UPDATE SET
                        Target.employee_name = Source.employee_name,
                        Target.employee_role = Source.employee_role,
                        Target.email = Source.email,
                        Target.employee_start_date = Source.employee_start_date,
                        Target.employee_contract_type = Source.employee_contract_type,
                        Target.boss_name = Source.boss_name,
                        Target.boss_email = Source.boss_email,
                        Target.boss_of_boss_name = Source.boss_of_boss_name,
                        Target.boss_of_boss_email = Source.boss_of_boss_email,
                        Target.alert_date = Source.alert_date,
                        Target.alert_type = Source.alert_type,
                        Target.alert_reason = Source.alert_reason,
                        Target.days_since_start = Source.days_since_start,
                        Target.expiration = Source.expiration,
                        Target.first_alert_sent = COALESCE(Target.first_alert_sent, 0), -- FALSE es 0
                        Target.second_alert_sent = COALESCE(Target.second_alert_sent, 0) -- FALSE es 0
                        -- No es necesario 'updated_at' aquí, el TRIGGER lo hará automáticamente

                -- 5. Acción para el INSERT normal
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (
                        employee_id, employee_name, 
                        employee_rut, employee_role, 
                        email,
                        employee_start_date, employee_contract_type,
                        boss_name, boss_email,
                        boss_of_boss_name, boss_of_boss_email,
                        alert_date, alert_type, 
                        alert_reason, days_since_start,
                        expiration
                        -- Las columnas con DEFAULT (created_at, first_alert, etc.) se llenarán solas
                    )
                    VALUES (
                        Source.employee_id, Source.employee_name, 
                        Source.employee_rut, Source.employee_role, 
                        Source.email,
                        Source.employee_start_date, Source.employee_contract_type,
                        Source.boss_name, Source.boss_email,
                        Source.boss_of_boss_name, Source.boss_of_boss_email,
                        Source.alert_date, Source.alert_type, 
                        Source.alert_reason, Source.days_since_start,
                        Source.expiration
                    );
                """
                
                # Ejecutar inserción (con los nuevos valores)
                cursor.execute(sql_insert, (
                    empleado["id"],
                    empleado["full_name"],
                    empleado["rut"],
                    empleado["name_role"],
                    empleado["email"],
                    empleado["start_date"],
                    empleado["contract_type"],
                    boss_name,
                    boss_email,
                    boss_of_boss_name,
                    boss_of_boss_email,
                    alerta["fecha_alerta"],
                    alerta["tipo_alerta"],
                    alerta["motivo"],
                    alerta["dias_desde_inicio"],
                    alerta["dias_vencimiento"]
                ))
                
                alertas_insertadas += 1
                
                if empleados_procesados % 50 == 0:
                    print(f"Procesados: {empleados_procesados}/{len(empleados_lista)} empleados...")
                    
            except Exception as e:
                errores += 1
                print(f"Error insertando alerta para {empleado.get('first_name', 'N/A')}: {e}")

    conexion.commit()
    print(f"""
        === PROCESO COMPLETADO ===
        Empleados procesados: {empleados_procesados}
        Alertas generadas: {alertas_insertadas}
        Errores: {errores}
        Cambios guardados en la base de datos
        """)
# Función principal para ejecutar alertas
def job_generar_alertas():
    """
    Función principal que se ejecutará para generar alertas de contratos.
    """
    print(f"\nINICIANDO GENERACIÓN DE ALERTAS: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Conexión a SQL Server usando pyodbc #
    try:
        logging.info("Conectando a SQL Server...")
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={DB_SERVER};"
            f"DATABASE={DB_NAME};"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
        conexion = pyodbc.connect(conn_str)
        cursor = conexion.cursor()
        logging.info(f"Conectado a SQL Server y usando la base: {DB_NAME}")

        # Generar nuevas alertas
        generar_alertas(cursor, conexion)
        print(f"GENERACIÓN DE ALERTAS COMPLETADA: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        

    except pyodbc.Error as e:
        # Un 'except' más específico para errores de BD
        print(f"Error de pyodbc en la generación de alertas: {e}")
        logging.error(f"Error de pyodbc: {e}")
    except Exception as e:
        print(f"Error general en la generación de alertas: {e}")
        logging.error(f"Error general: {e}")
    finally:
        if cursor:
            cursor.close()
        if conexion:
            conexion.close()
            print("Conexión cerrada (desde finally).")

if __name__ == "__main__":
    print("SISTEMA DE ALERTAS DE CONTRATOS - MODO PROGRAMADOR DE TAREAS")
    print("="*70)
    
    # Ejecutar SOLO UNA VEZ (para el Programador de Tareas)
    print("Ejecutando generación de alertas programada...")
    job_generar_alertas()
    
    print("Tarea completada. El script finalizará automáticamente.")
    print("La próxima ejecución será programada por Windows.")
    
    sys.exit(0)