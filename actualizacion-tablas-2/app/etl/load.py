from app.utils.logger import setup_logger
from app.etl.create import garantizar_tabla_empleado
import time

logger = setup_logger(__name__)

#! /// Load de tabla incidencias --> Relacionada con función extract.obtener_datos_paginados /// 
def crear_e_insertar_tablas_incidencias(conexion, nombre_tabla: str, datos: list):
    cursor = conexion.cursor()
    
    if not datos:
        logger.warning(f"No hay datos para la tabla '{nombre_tabla}'. Omitiendo.")
        return

    # Definición dinámica de columnas
    columnas = list(datos[0].keys())
    columnas_con_tipos = []
    for col in columnas:
        if col == 'id':
            columnas_con_tipos.append(f"[{col}] INT PRIMARY KEY")
        else:
            columnas_con_tipos.append(f"[{col}] NVARCHAR(MAX)")

    column_definitions = ", ".join(columnas_con_tipos)
    
    # Crear tabla si no existe
    if not cursor.tables(table=nombre_tabla, tableType='TABLE').fetchone():
        try:
            logging_msg = f"Creando tabla '{nombre_tabla}'..."
            logger.info(logging_msg)
            cursor.execute(f"CREATE TABLE {nombre_tabla} ({column_definitions})")
            conexion.commit()
        except Exception as e:
            logger.error(f"Error creando tabla: {e}")
            return

    # Preparar Query MERGE
    logger.info(f"Iniciando MERGE en '{nombre_tabla}'...")
    
    col_list_insert = ", ".join([f"[{col}]" for col in columnas])
    col_list_source_values = ", ".join([f"S.[{col}]" for col in columnas])
    update_clause = ", ".join([f"T.[{col}] = S.[{col}]" for col in columnas if col != 'id'])
    placeholders = ", ".join(['?'] * len(columnas))
    col_list_source_names = ", ".join([f"[{col}]" for col in columnas])
    
    sql_merge = f"""
    MERGE INTO {nombre_tabla} AS T
    USING (VALUES ({placeholders})) AS S ({col_list_source_names})
    ON (T.id = S.id)
    WHEN MATCHED THEN UPDATE SET {update_clause}
    WHEN NOT MATCHED THEN INSERT ({col_list_insert}) VALUES ({col_list_source_values});
    """

    contador_afectado = 0
    # Iteración de inserción (Candidato a optimización futura)
    for item in datos:
        try:
            values = []
            for col in columnas:
                val = item.get(col)
                if col == 'id':
                    values.append(int(val) if val is not None else None)
                else:
                    values.append(str(val) if val is not None else None)
            
            cursor.execute(sql_merge, tuple(values))
            contador_afectado += cursor.rowcount
        except Exception as error:
            logger.error(f"Error en MERGE id {item.get('id', 'N/A')}: {error}")

    conexion.commit()
    logger.info(f"Finalizado '{nombre_tabla}': {contador_afectado} registros procesados.")
    cursor.close()

#! /// Load de tabla employees --> Relacionada con función extract.obtener_todos_los_empleados_filtrados ///
def job_sincronizar_empleados(empleados_filtrados_api: list, conexion):
    logger.info(f"\nINICIANDO SINCRONIZACIÓN: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        #! Linea comentada para que la función no extraiga 
        #! los empleados y sólo los inserte
        #empleados_filtrados_api = obtener_todos_los_empleados_filtrados()
        if not empleados_filtrados_api:
            logger.warning("No se obtuvieron empleados de la API.")
            return

        empleados_por_persona = {}
        empleados_filtrados_validos = [
            e for e in empleados_filtrados_api 
            if e.get("account_number") not in [None, "", "-"]
        ]

        logger.info(f"Filtrados {len(empleados_filtrados_api) - len(empleados_filtrados_validos)} empleados sin número de cuenta.")
        logger.info(f"Se procesarán {len(empleados_filtrados_validos)} empleados con cuenta válida.")

        # Agrupación combinada (RUT + cuenta)
        empleados_por_persona = {}
        for e in empleados_filtrados_validos:
            rut = e.get('rut')
            account = e.get('account_number')

            # Prioriza RUT si existe, si no usa cuenta
            clave = rut if rut else account
            if not clave:
                logger.warning(f"Empleado sin RUT ni cuenta, omitido: {e.get('full_name')}")
                continue

            if clave not in empleados_por_persona:
                empleados_por_persona[clave] = []
            empleados_por_persona[clave].append(e)

        empleados_filtrados_priorizados = []

        for clave, lista_empleados in empleados_por_persona.items():
            activos = [emp for emp in lista_empleados if emp.get('status') == 'activo']
            if activos:
                empleados_filtrados_priorizados.extend(activos)
            else:
                if lista_empleados:
                    emp_a_marcar = lista_empleados[0]
                    emp_a_marcar['status'] = 'inactivo'
                    empleados_filtrados_priorizados.append(emp_a_marcar)

        empleados_filtrados = empleados_filtrados_priorizados

        #? Se añadió función que se encarga de crear la tabla employees
        #? en caso de que no exista.
        garantizar_tabla_empleado(conexion)
        cursor = conexion.cursor()
        logger.info("Conectado a SQL Server y usando la base: IARRHH")

        logger.info("Insertando/Actualizando empleados en la tabla SQL...")
        procesados = 0
        errores = 0

        # =================================================================
        # CAPTURAR TIMESTAMP ANTES DE LA CARGA
        # =================================================================
        cursor.execute("SELECT GETDATE()")
        timestamp_inicio_sync = cursor.fetchone()[0]
        logger.info(f"Timestamp de inicio de sincronización: {timestamp_inicio_sync}")

        # 1. Chequeo
        sql_check = "SELECT rut FROM dbo.employees WHERE rut = ?"

        # 2. Insert (Incluye todos los campos + GETDATE())
        sql_insert = """
            INSERT INTO dbo.employees (
                person_id, id, full_name, first_name, last_name, rut, picture_url, email, personal_email, active_since,
                status, payment_method, id_boss, rut_boss, contract_type, start_date,
                end_date, contract_finishing_date_1, contract_finishing_date_2, area_id, cost_center,
                address, street, street_number, city, province, district, region, phone,
                gender, birthday, university, degree, bank, account_type,
                account_number, nationality, civil_status, health_company,
                pension_regime, pension_fund, active_until, termination_reason, afc, retired,
                retirement_regime, base_wage, name_role, ctrlit_recinto, updated_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE()
            )
        """

        # 3. Update (Excluye rut del SET, lo usa en WHERE. Usa GETDATE() para updated_at)
        sql_update = """
            UPDATE dbo.employees SET
                person_id=?, id=?, full_name=?, first_name=?, last_name=?,
                picture_url=?, email=?, personal_email=?, active_since=?,
                status=?, payment_method=?, id_boss=?, rut_boss=?, contract_type=?,
                start_date=?, end_date=?, contract_finishing_date_1=?, contract_finishing_date_2=?,
                area_id=?, cost_center=?, address=?, street=?, street_number=?,
                city=?, province=?, district=?, region=?, phone=?,
                gender=?, birthday=?, university=?, degree=?, bank=?, account_type=?,
                account_number=?, nationality=?, civil_status=?, health_company=?,
                pension_regime=?, pension_fund=?, active_until=?, termination_reason=?, afc=?, retired=?,
                retirement_regime=?, base_wage=?, name_role=?, ctrlit_recinto=?, 
                updated_at=GETDATE()
            WHERE rut = ?
        """

        # Desactivamos fast_executemany por seguridad con encriptación
        cursor.fast_executemany = False

        logger.info("=== MUESTRA DE DATOS ANTES DE INSERTAR ===")
        for i, emp in enumerate(empleados_filtrados[:5]):  # Solo los primeros 5
            logger.info(f"Empleado {i+1}:")
            logger.info(f"  RUT: {emp.get('rut')}")
            logger.info(f"  account_number: '{emp.get('account_number')}' (len: {len(str(emp.get('account_number'))) if emp.get('account_number') else 0})")
            logger.info(f"  base_wage: {emp.get('base_wage')} (type: {type(emp.get('base_wage'))})")
        logger.info("=" * 50)
        
        for e in empleados_filtrados:
            try:
                rut_empleado = e.get("rut")
                
                # Paso 1: Verificar existencia
                cursor.execute(sql_check, (rut_empleado,))
                existe = cursor.fetchone()

                if existe:
                    # --- UPDATE ---
                    # El orden debe coincidir con sql_update. RUT va al final.
                    values_update = (
                        e.get("person_id"), e.get("id"), e.get("full_name"), e.get("first_name"), e.get("last_name"),
                        e.get("picture_url"), e.get("email"), e.get("personal_email"), e.get("active_since"),
                        e.get("status"), e.get("payment_method"), e.get("id_boss"), e.get("rut_boss"), e.get("contract_type"),
                        e.get("start_date"), e.get("end_date"), e.get("contract_finishing_date_1"), e.get("contract_finishing_date_2"),
                        e.get("area_id"), e.get("cost_center"), e.get("address"), e.get("street"), e.get("street_number"),
                        e.get("city"), e.get("province"), e.get("district"), e.get("region"), e.get("phone"),
                        e.get("gender"), e.get("birthday"), e.get("university"), e.get("degree"), e.get("bank"), e.get("account_type"),
                        e.get("account_number"), # Columna Encriptada (Auto-detectado)
                        e.get("nationality"), e.get("civil_status"), e.get("health_company"),
                        e.get("pension_regime"), e.get("pension_fund"), e.get("active_until"), e.get("termination_reason"), e.get("afc"), e.get("retired"),
                        e.get("retirement_regime"), 
                        e.get("base_wage"),      # Columna Encriptada (Auto-detectado)
                        e.get("name_role"), e.get("ctrlit_recinto"),
                        rut_empleado             # WHERE rut = ?
                    )
                    cursor.execute(sql_update, values_update)

                else:
                    # --- INSERT ---
                    values_insert = (
                        e.get("person_id"), e.get("id"), e.get("full_name"), e.get("first_name"), e.get("last_name"),
                        e.get("rut"), e.get("picture_url"), e.get("email"), e.get("personal_email"), e.get("active_since"),
                        e.get("status"), e.get("payment_method"), e.get("id_boss"), e.get("rut_boss"), e.get("contract_type"),
                        e.get("start_date"), e.get("end_date"), e.get("contract_finishing_date_1"), e.get("contract_finishing_date_2"),
                        e.get("area_id"), e.get("cost_center"), e.get("address"), e.get("street"), e.get("street_number"),
                        e.get("city"), e.get("province"), e.get("district"), e.get("region"), e.get("phone"),
                        e.get("gender"), e.get("birthday"), e.get("university"), e.get("degree"), e.get("bank"),
                        e.get("account_type"), e.get("account_number"), e.get("nationality"), e.get("civil_status"),
                        e.get("health_company"), e.get("pension_regime"), e.get("pension_fund"), e.get("active_until"),
                        e.get("termination_reason"), e.get("afc"), e.get("retired"), e.get("retirement_regime"),
                        e.get("base_wage"), e.get("name_role"), e.get("ctrlit_recinto")
                    )
                    cursor.execute(sql_insert, values_insert)

                procesados += 1
                if procesados % 100 == 0:
                    logger.info(f"Procesados {procesados} empleados...")
                    conexion.commit() # Commit parcial

            except Exception as error:
                logger.exception(f"Error procesando empleado {e.get('rut', 'N/A')}: {error}")
                errores += 1

        conexion.commit() # Guarda todos los cambios finales

        logger.info(f"Procesamiento completado. Total: {procesados}, Errores: {errores}")

        # =================================================================
        # DESACTIVAR EMPLEADOS NO ENCONTRADOS
        # =================================================================
        logger.info("Iniciando desactivación de empleados no encontrados en la API...")
        
        try:
            sql_desactivar = """
            UPDATE dbo.employees
            SET status = 'inactivo', updated_at = GETDATE()
            WHERE updated_at < ? AND (status != 'inactivo' OR status IS NULL)
            """
            cursor.execute(sql_desactivar, (timestamp_inicio_sync,))
            filas_afectadas = cursor.rowcount
            conexion.commit()
            logger.info(f"Desactivación completa. {filas_afectadas} empleados marcados como 'inactivo'.")
            
        except Exception as e_desactivar:
            logger.exception(f"Error grave durante la desactivación: {e_desactivar}")
            conexion.rollback()

    except Exception as e:
        logger.exception(f"Error fatal en el job de sincronización: {e}")
        if conexion:
            conexion.rollback()
    finally:
        if cursor:
            cursor.close()
        # NOTA: No cerramos la 'conexion' aquí porque se cierra en el main
        logger.info("Cursor cerrado.")

    logger.info(f"SINCRONIZACIÓN FINALIZADA: {time.strftime('%Y-%m-%d %H:%M:%S')}")


#! /// Load de tabla areas --> Relacionada con función extract.obtener_datos_tabla_areas ///
def cargar_datos_areas(conexion, areas_datos):
    """
    Realiza la carga (Upsert: Insertar o Actualizar) de los datos en SQL Server.
    """
    if not areas_datos:
        print("No hay datos para cargar.")
        return

    cursor = conexion.cursor()
    print(f"Iniciando carga de {len(areas_datos)} registros en BD...")
    
    # Contadores
    stats = {"insert": 0, "update": 0, "error": 0}

    # Queries preparadas
    sql_check = "SELECT COUNT(*) FROM dbo.areas WHERE id = ?"
    
    sql_update = """
        UPDATE dbo.areas SET 
            name=?, address=?, first_level_id=?, first_level_name=?,
            second_level_id=?, second_level_name=?, cost_center=?, 
            status=?, city=?
        WHERE id=?
    """
    
    sql_insert = """
        INSERT INTO dbo.areas (
            id, name, address, first_level_id, first_level_name,
            second_level_id, second_level_name, cost_center, status, city
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    try:
        for i, area in enumerate(areas_datos):
            try:
                area_id = area["id"]
                
                # 1. Verificar existencia
                cursor.execute(sql_check, (area_id,))
                existe = cursor.fetchone()[0] > 0
                
                if existe:
                    # UPDATE
                    cursor.execute(sql_update, (
                        area["name"], area["address"], area["first_level_id"], 
                        area["first_level_name"], area["second_level_id"], 
                        area["second_level_name"], area["cost_center"], 
                        area["status"], area["city"], 
                        area_id 
                    ))
                    stats["update"] += 1
                else:
                    # INSERT
                    cursor.execute(sql_insert, (
                        area_id, area["name"], area["address"], 
                        area["first_level_id"], area["first_level_name"], 
                        area["second_level_id"], area["second_level_name"], 
                        area["cost_center"], area["status"], area["city"]
                    ))
                    stats["insert"] += 1
                
                # Commit parcial cada 50 registros para no saturar el log de transacciones
                if (i + 1) % 50 == 0:
                    conexion.commit()
                    print(f"   Progreso: {i + 1}/{len(areas_datos)}...")

            except Exception as e_row:
                print(f"   Error en ID {area.get('id')}: {e_row}")
                stats["error"] += 1

        conexion.commit() # Commit final
        print(f"\nCarga completada: {stats['insert']} Inserts, {stats['update']} Updates, {stats['error']} Errores.")

    except Exception as e:
        print(f"Error general en la carga: {e}")
        conexion.rollback()
    finally:
        cursor.close()

#! /// Load de tabla vacaciones --> Relacionada con función extract.obtener_datos_vacaciones ///
def cargar_datos_vacaciones(conexion, vacaciones_datos, batch_size=50):
    
    if not vacaciones_datos:
        print("No hay datos para cargar.")
        return 

    cursor = conexion.cursor()
    print(f"Iniciando carga de {len(vacaciones_datos)} registros en BD...")
    
    # Contadores
    stats = {"insert": 0, "update": 0, "error": 0}

    sql_update = """
        UPDATE dbo.vacations SET
            status = ?,
            approved_by_id = ?,
            approved_at = ?,
            workday_stage = ?,
            updated_at_local = GETDATE()
        WHERE external_id = ?
    """
    
    sql_insert = """
        INSERT INTO dbo.vacations (
            external_id, employee_id, approved_by_id, working_days, calendar_days,
            workday_stage, start_date, end_date, requested_at, approved_at,
            type, status, vacation_type_id, created_at_local
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
    """

    def limpiar_fecha(fecha_str, es_solo_fecha=False):
        if not fecha_str or str(fecha_str).strip() == '':
            return None
        
        s = str(fecha_str).strip()
        
        if s == '' or s == '0000-00-00':
            return None

        s = s.replace('T', ' ').replace('Z', '')

        if es_solo_fecha and len(s) >= 10:
            return s[:10]
        
        if not es_solo_fecha and '.' in s:
            s = s.split('.')[0]

        return s

    for index, vac in enumerate(vacaciones_datos):
        try:
            approved_by = vac.get('approved_by_id')
            if approved_by == 0: approved_by = None

            start_date_clean = limpiar_fecha(vac.get('start_date'), es_solo_fecha=True)
            end_date_clean = limpiar_fecha(vac.get('end_date'), es_solo_fecha=True)

            requested_at_clean = limpiar_fecha(vac.get('requested_at'), es_solo_fecha=False)
            approved_at_clean = limpiar_fecha(vac.get('approved_at'), es_solo_fecha=False)

            # Parámetros para el INSERT
            params_insert = (
                vac['id'], vac['employee_id'], approved_by, vac['working_days'], vac['calendar_days'],
                vac['workday_stage'], start_date_clean, end_date_clean, requested_at_clean, approved_at_clean,
                vac['type'], vac['status'], vac['vacation_type_id']
            )
            
            # --- INTENTO DE INSERCIÓN ---
            try:
                cursor.execute(sql_insert, params_insert)
                stats["insert"] += 1
            
            except Exception as e:
                # Si entra aquí, es porque el ID ya existe (violación del índice UNIQUE)
                if '23000' in str(e): #? El código 23000 en SQL es el error de violación de integridad 
                    cursor.execute(sql_update, (
                        vac['status'], approved_by, approved_at_clean, vac['workday_stage'], 
                        vac['id'] # WHERE external_id = id
                    ))
                    stats["update"] += 1
                else:
                    raise e
            
            if (index + 1) % batch_size == 0:
                conexion.commit()
                time.sleep(0.1) 
                print(f"   ... Lote procesado: {index + 1} registros guardados.")

        except Exception as e:
            # Detección específica de caída de red
            msg_error = str(e)
            if '08S01' in msg_error or 'Communication link failure' in msg_error:
                print(f"¡ALERTA CRÍTICA! Se perdió la conexión en el registro {vac.get('id')}.")
                print("Intenta reducir el tamaño del lote o revisar la VPN/Internet.")
                # Aquí podrías implementar una lógica de reconexión si tuvieras la connection string
                break # Rompemos el ciclo porque la conexión ya no sirve
            
            # Error de datos normal
            print(f"\nERROR SQL - ID REGISTRO: {vac.get('id')}")
            print(f"Mensaje Técnico: {e}")
            stats["error"] += 1

    try:
        conexion.commit() # Un solo commit al final es mucho más rápido
        print(f"\nCarga completada: {stats['insert']} Inserts, {stats['update']} Updates, {stats['error']} Errores.")
    except Exception as e:
        print(f"Error al hacer commit final: {e}")
        conexion.rollback()
    finally:
        cursor.close()