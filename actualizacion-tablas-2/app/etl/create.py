from app.utils.logger import setup_logger

logger = setup_logger(__name__)


def garantizar_tablas_rh(conexion) -> bool:
    """
    Crea/verifica el schema `rh` y todas las tablas definidas en `creacion_tablas.sql`.

    Nota: en PostgreSQL se reemplazan constructs de SQL Server (dbo, OBJECT_ID, triggers, cifrado en columnas).
    """
    cursor = conexion.cursor()
    try:
        logger.info("Verificando/creando DDL de tablas RH (PostgreSQL)...")

        cursor.execute("CREATE SCHEMA IF NOT EXISTS rh;")

        # --- rh.employees ---
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS rh.employees (
                id INT PRIMARY KEY NOT NULL,
                person_id INT NOT NULL,
                full_name VARCHAR(255) NOT NULL,
                first_name VARCHAR(100) NULL,
                last_name VARCHAR(100) NULL,
                rut VARCHAR(50),
                active_since DATE,
                status VARCHAR(50),
                start_date DATE,
                end_date DATE,
                name_role VARCHAR(255),
                area_id INT,
                email VARCHAR(255),
                personal_email VARCHAR(255),
                rut_boss VARCHAR(50),
                address TEXT,
                district VARCHAR(100),
                region VARCHAR(100),
                phone VARCHAR(50),
                gender VARCHAR(50),
                birthday DATE,
                university VARCHAR(255),
                degree VARCHAR(255),
                bank VARCHAR(100),
                account_type VARCHAR(50),
                account_number VARCHAR(50),
                payment_method VARCHAR(50),
                base_wage INT,
                nationality VARCHAR(100),
                civil_status VARCHAR(50),
                health_company VARCHAR(255),
                pension_regime VARCHAR(255),
                pension_fund VARCHAR(255),
                active_until DATE,
                afc VARCHAR(50),
                retired BOOLEAN,
                retirement_regime VARCHAR(255),
                termination_reason VARCHAR(255),
                contract_type VARCHAR(50),
                contract_finishing_date_1 DATE,
                contract_finishing_date_2 DATE,
                cost_center VARCHAR(255),
                ctrlit_recinto VARCHAR(255),
                picture_url TEXT,
                updated_at TIMESTAMP(7),
                created_at TIMESTAMP(7)
            );
            """
        )

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_employees_rut ON rh.employees(rut);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_employees_id ON rh.employees(id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_employees_area_id ON rh.employees(area_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_employees_name_role ON rh.employees(name_role);")

        # --- rh.consolidado_incidencias ---
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS rh.consolidado_incidencias (
                id INT PRIMARY KEY,
                employee_id INT REFERENCES rh.employees(id),
                days_count INT,
                day_percent VARCHAR(5),
                type_permission VARCHAR(100),
                start_date DATE,
                end_date DATE,
                status VARCHAR(50),
                created_at TIMESTAMP(7)
            );
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_consolidado_employee_id ON rh.consolidado_incidencias(employee_id);"
        )

        # --- rh.vacations ---
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS rh.vacations (
                id INT PRIMARY KEY,
                employee_id INT,
                working_days INT,
                workday_stage VARCHAR(50),
                type VARCHAR(50),
                status VARCHAR(50),
                start_date DATE,
                end_date DATE,
                created_at TIMESTAMP(7),
                updated_at TIMESTAMP(7)
            );
            """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_vacations_employee_id ON rh.vacations(employee_id);")

        # --- rh.contract_alerts ---
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS rh.contract_alerts (
                employee_id INT PRIMARY KEY REFERENCES rh.employees(id),
                rut VARCHAR(50),
                employee_name VARCHAR(255),
                employee_role VARCHAR(255),
                employee_start_date DATE,
                email VARCHAR(255),
                employee_contract_type VARCHAR(50),
                boss_name VARCHAR(255),
                boss_email VARCHAR(255),
                boss_of_boss_name VARCHAR(255),
                boss_of_boss_email VARCHAR(255),
                alert_date DATE,
                alert_type VARCHAR(50),
                alert_reason TEXT,
                expiration INT,
                days_since_start INT,
                first_alert_sent BOOLEAN DEFAULT FALSE,
                second_alert_sent BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP(7),
                updated_at TIMESTAMP(7)
            );
            """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_contract_alerts_rut ON rh.contract_alerts(rut);")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_contract_alerts_employee_id ON rh.contract_alerts(employee_id);"
        )

        # --- rh.areas ---
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS rh.areas (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                address TEXT,
                first_level_id INT,
                first_level_name VARCHAR(255),
                second_level_id INT,
                second_level_name VARCHAR(255),
                cost_center VARCHAR(100),
                status VARCHAR(50),
                city VARCHAR(100),
                created_at TIMESTAMP(7),
                updated_at TIMESTAMP(7)
            );
            """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_areas_id ON rh.areas(id);")

        # --- rh.historical_settlements ---
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS rh.historical_settlements (
                id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY NOT NULL,
                liquidacion_id INT,
                employee_id INT NOT NULL REFERENCES rh.employees(id),
                rut VARCHAR(20),
                periodo VARCHAR(16),
                anio INT,
                mes INT,
                pay_period DATE,
                dias_trabajados FLOAT,
                dias_no_trabajados FLOAT,
                ingreso_bruto FLOAT,
                ingreso_neto FLOAT,
                ingreso_afp FLOAT,
                ingreso_ips FLOAT,
                total_ingresos_imponibles FLOAT,
                total_ingresos_no_imponibles FLOAT,
                total_descuentos_legales FLOAT,
                total_otros_descuentos FLOAT,
                liquido_a_pagar FLOAT,
                base_imponible FLOAT,
                cerrada BOOLEAN,
                raw_payload TEXT,
                created_at TIMESTAMP(7)
            );
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_historical_settlements_liquidacion_id
            ON rh.historical_settlements(liquidacion_id);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_historical_settlements_employee_id
            ON rh.historical_settlements(employee_id);
            """
        )

        # --- rh.historical_settlement_items ---
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS rh.historical_settlement_items (
                id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY NOT NULL,
                liquidacion_id INT,
                employee_id INT REFERENCES rh.employees(id),
                rut VARCHAR(20),
                item_type VARCHAR(100),
                income_type VARCHAR(100),
                subtype VARCHAR(100),
                name VARCHAR(255),
                amount FLOAT,
                taxable FLOAT,
                imponible FLOAT,
                anticipo FLOAT,
                credit_type VARCHAR(100),
                institution VARCHAR(100),
                description TEXT,
                code VARCHAR(100),
                item_code VARCHAR(100),
                create_at TIMESTAMP(7)
            );
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_historical_settlement_items_liquidacion_id
            ON rh.historical_settlement_items(liquidacion_id);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_historical_settlement_items_employee_id
            ON rh.historical_settlement_items(employee_id);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_historical_settlement_items_rut
            ON rh.historical_settlement_items(rut);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_historical_settlement_items_item_type
            ON rh.historical_settlement_items(item_type);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_historical_settlement_items_income_type
            ON rh.historical_settlement_items(income_type);
            """
        )

        # --- rh.job_history ---
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS rh.job_history (
                job_id INT PRIMARY KEY,
                person_id INT NOT NULL REFERENCES rh.employees(id),
                rut VARCHAR(20),
                start_date DATE,
                end_date DATE,
                base_wage FLOAT,
                name_role VARCHAR(255),
                boss_id INT REFERENCES rh.employees(id),
                boss_rut VARCHAR(20),
                created_at TIMESTAMP(7),
                updated_at TIMESTAMP(7)
            );
            """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_history_job_id ON rh.job_history(job_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_history_person_id ON rh.job_history(person_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_history_rut ON rh.job_history(rut);")

        # --- rh.reporte_ausentismo_empleados ---
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS rh.reporte_ausentismo_empleados (
                rut VARCHAR(20) NOT NULL PRIMARY KEY,
                full_name VARCHAR(200),
                active_since DATE,
                dias_corridos_contratado INT,
                dias_habiles_transcurridos INT,
                dias_totales_ausentismo INT DEFAULT 0,
                dias_totales_habiles_ausentismo INT DEFAULT 0,
                created_at TIMESTAMP(7),
                updated_at TIMESTAMP(7)
            );
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_reporte_ausentismo_full_name
            ON rh.reporte_ausentismo_empleados(full_name);
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_reporte_ausentismo_rut
            ON rh.reporte_ausentismo_empleados(rut);
            """
        )

        conexion.commit()
        logger.info("DDL de tablas RH verificado/creado correctamente.")
        return True
    except Exception as e:
        logger.error(f"Error inicializando tablas RH: {e}")
        conexion.rollback()
        return False
    finally:
        cursor.close()
