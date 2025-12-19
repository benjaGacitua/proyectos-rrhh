from app.utils.logger import setup_logger

logger = setup_logger(__name__)

def verificar_tablas_areas(conexion):
    """
    Crea la tabla y el trigger si no existen.
    """
    cursor = conexion.cursor()
    logger.info("Verificando esquema de base de datos (DDL)...")
    
    sql_create_table = """
    IF OBJECT_ID('dbo.areas', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.areas (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            address VARCHAR(MAX),
            first_level_id INT,
            first_level_name VARCHAR(255),
            second_level_id INT,
            second_level_name VARCHAR(255),
            cost_center VARCHAR(100),
            status VARCHAR(50),
            city VARCHAR(100),
            fecha_actualizacion DATETIME2 DEFAULT SYSDATETIME()
        );
    END
    """
    
    sql_create_trigger = """
    IF OBJECT_ID('trg_areas_update', 'TR') IS NULL
    BEGIN
        EXEC('
        CREATE TRIGGER trg_areas_update ON dbo.areas
        AFTER UPDATE AS
        BEGIN
            SET NOCOUNT ON;
            UPDATE t SET fecha_actualizacion = SYSDATETIME()
            FROM dbo.areas AS t INNER JOIN inserted AS i ON t.id = i.id;
        END
        ')
    END
    """

    try:
        cursor.execute(sql_create_table)
        cursor.execute(sql_create_trigger)
        conexion.commit()
        logger.info("Tabla y Trigger verificados correctamente.")
        return True
    except Exception as e:
        logger.error(f"Error inicializando tablas: {e}")
        conexion.rollback()
        return False
    finally:
        cursor.close()

#! Función relacionada a load.job_sincronizar_empleados.
#! Se encarga de corroborar la existencia de la tabla employees
#! y crearla de ser necesario considerando encriptación
def garantizar_tabla_empleado(conexion):

    logger.info("Conectando a SQL Server...")

    # Usamos la conexión recibida
    cursor = conexion.cursor()

    logger.info(f"Conectado a SQL Server y usando la base: IARRHH")

    # --- LÓGICA PARA CREAR TABLA SI NO EXISTE ---
    logger.info("Verificando existencia de la tabla 'dbo.employees'...")
    if not cursor.tables(table='employees', schema='dbo', tableType='TABLE').fetchone():
        logger.info("La tabla 'dbo.employees' no existe. Creándola con cifrado...")

        # Define el nombre exacto de tu Clave de Cifrado de Columna (CEK) creada en SSMS
        cek_name = "CEK_Sueldos_Nueva" 

        sql_create_table = f"""
        CREATE TABLE dbo.employees (
            person_id INT NULL,
            full_name VARCHAR(255) NULL,
            first_name VARCHAR(100) NULL,
            last_name VARCHAR(100) NULL,
            rut VARCHAR(50) NOT NULL PRIMARY KEY,
            picture_url VARCHAR(MAX) NULL,
            id INT NULL,
            email VARCHAR(255) NULL,
            personal_email VARCHAR(255) NULL,
            address VARCHAR(MAX) NULL,
            street VARCHAR(255) NULL,
            street_number VARCHAR(50) NULL,
            city VARCHAR(100) NULL,
            province VARCHAR(100) NULL,
            district VARCHAR(100) NULL,
            region VARCHAR(100) NULL,
            phone VARCHAR(50) NULL,
            gender VARCHAR(50) NULL,
            birthday DATE NULL,
            university VARCHAR(255) NULL,
            degree VARCHAR(255) NULL,
            bank VARCHAR(100) NULL,
            account_type NVARCHAR(50) NULL,
            account_number NVARCHAR(50) COLLATE Modern_Spanish_BIN2 
                ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{cek_name}],
                                ENCRYPTION_TYPE = RANDOMIZED,
                                ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NULL,
            nationality VARCHAR(100) NULL,
            civil_status VARCHAR(50) NULL,
            health_company VARCHAR(255) NULL,
            pension_regime VARCHAR(255) NULL,
            pension_fund VARCHAR(255) NULL,
            active_until DATE NULL,
            afc VARCHAR(50) NULL,
            retired BIT NULL,
            retirement_regime VARCHAR(255) NULL,
            active_since DATE NULL,
            status VARCHAR(50) NULL,
            start_date DATE NULL,
            end_date DATE NULL,
            termination_reason VARCHAR(255) NULL,
            payment_method VARCHAR(50) NULL,
            id_boss INT NULL,
            rut_boss VARCHAR(50) NULL,
            base_wage INT
                ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [{cek_name}],
                                ENCRYPTION_TYPE = RANDOMIZED,
                                ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') NULL,
            contract_type VARCHAR(50) NULL,
            contract_finishing_date_1 DATE NULL,
            contract_finishing_date_2 DATE NULL,
            name_role VARCHAR(255) NULL,
            area_id INT NULL,
            cost_center VARCHAR(255) NULL,
            ctrlit_recinto VARCHAR(255) NULL,
            updated_at DATETIME2(7) NULL
        );
        """
        cursor.execute(sql_create_table)
        conexion.commit()
        logger.info("Tabla 'dbo.employees' creada exitosamente con columnas cifradas.")
    else:
        logger.info("La tabla 'dbo.employees' ya existe.")