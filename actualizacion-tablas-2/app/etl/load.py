from app.utils.logger import setup_logger
import time
import json
from datetime import datetime
from pathlib import Path

logger = setup_logger(__name__)


def _registrar_error_fila(entidad: str, clave: str, payload: dict, error: Exception):
    base_dir = Path(__file__).resolve().parent.parent / "utils" / "temp" / "errores"
    base_dir.mkdir(parents=True, exist_ok=True)
    archivo = base_dir / f"errores_{entidad}_{datetime.now().strftime('%Y%m%d')}.jsonl"
    registro = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "entidad": entidad,
        "clave": clave,
        "error": str(error),
        "payload": payload,
    }
    with archivo.open("a", encoding="utf-8") as f:
        f.write(json.dumps(registro, ensure_ascii=True) + "\n")

def _obtener_archivo_errores_mas_reciente(entidad: str):
    base_dir = Path(__file__).resolve().parent.parent / "utils" / "temp" / "errores"
    if not base_dir.exists():
        return None
    archivos = sorted(base_dir.glob(f"errores_{entidad}_*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    return archivos[0] if archivos else None

def reintentar_errores_entidad(conexion, entidad: str, max_registros: int = None):
    """
    Reintenta filas fallidas persistidas en JSONL para una entidad.
    Entidades soportadas para retry automatico: areas, vacations, consolidado_incidencias.
    """
    entidad = (entidad or "").strip().lower()
    archivo = _obtener_archivo_errores_mas_reciente(entidad)
    if not archivo:
        logger.info(f"No hay archivo de errores para '{entidad}'.")
        return {"reintentados": 0, "recuperados": 0, "fallidos": 0}

    lineas = archivo.read_text(encoding="utf-8").splitlines()
    if not lineas:
        return {"reintentados": 0, "recuperados": 0, "fallidos": 0}

    if max_registros is not None:
        lineas = lineas[: max(0, int(max_registros))]

    if entidad == "areas":
        funcion_reintento = cargar_datos_areas
    elif entidad == "vacations":
        funcion_reintento = cargar_datos_vacaciones
    elif entidad == "consolidado_incidencias":
        funcion_reintento = lambda conn, data: crear_e_insertar_tablas_incidencias(conn, "consolidado_incidencias", data)
    else:
        logger.warning(
            f"Retry automatico no soportado para '{entidad}'. Soportadas: areas, vacations, consolidado_incidencias."
        )
        return {"reintentados": 0, "recuperados": 0, "fallidos": 0}

    recuperados = 0
    fallidos = []
    reintentados = 0

    for linea in lineas:
        try:
            registro = json.loads(linea)
            payload = registro.get("payload", {})
            if not isinstance(payload, dict):
                raise ValueError("Payload invalido en archivo de errores.")
            funcion_reintento(conexion, [payload])
            recuperados += 1
            reintentados += 1
        except Exception as e:
            reintentados += 1
            fallidos.append({
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "entidad": entidad,
                "clave": registro.get("clave", "sin_clave") if "registro" in locals() else "sin_clave",
                "error": str(e),
                "payload": payload if "payload" in locals() else {},
            })

    if fallidos:
        with archivo.open("w", encoding="utf-8") as f:
            for item in fallidos:
                f.write(json.dumps(item, ensure_ascii=True) + "\n")
    else:
        archivo.unlink(missing_ok=True)

    logger.info(
        f"Retry '{entidad}' finalizado. Reintentados: {reintentados}, Recuperados: {recuperados}, Fallidos: {len(fallidos)}."
    )
    return {"reintentados": reintentados, "recuperados": recuperados, "fallidos": len(fallidos)}

#! /// Load de tabla incidencias --> Relacionada con función extract.obtener_datos_paginados /// 
def crear_e_insertar_tablas_incidencias(conexion, nombre_tabla: str, datos: list):
    cursor = conexion.cursor()

    def _to_date(value):
        if value is None:
            return None
        try:
            return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
        except Exception:
            return None

    def _to_datetime(value):
        if value is None:
            return None
        try:
            v = str(value).replace("T", " ").replace("Z", "").strip()
            if "." in v:
                v = v.split(".")[0]
            return datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    def _calc_days_count(start_date, end_date):
        if not start_date or not end_date:
            return None
        try:
            return (end_date - start_date).days + 1
        except Exception:
            return None

    if not datos:
        logger.warning(f"No hay datos para la tabla '{nombre_tabla}'. Omitiendo.")
        cursor.close()
        return

    try:
        logger.info(
            f"Iniciando carga de incidencias en 'rh.consolidado_incidencias'. Registros recibidos: {len(datos)}"
        )

        employee_ids = sorted(
            {
                int(d.get("employee_id"))
                for d in datos
                if d.get("employee_id") is not None
            }
        )

        if not employee_ids:
            logger.warning("No hay employee_id válidos en incidencias. Omitiendo carga.")
            return

        cursor.execute(
            """
            SELECT id
            FROM rh.employees
            WHERE id = ANY(%s)
            """,
            (employee_ids,),
        )
        empleados = {row[0] for row in cursor.fetchall()}

        sql_insert = """
            INSERT INTO rh.consolidado_incidencias (
                id,
                employee_id,
                days_count,
                day_percent,
                type_permission,
                start_date,
                end_date,
                status,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        sql_update = """
            UPDATE rh.consolidado_incidencias
            SET
                employee_id = %s,
                days_count = %s,
                day_percent = %s,
                type_permission = %s,
                status = %s
            WHERE id = %s
        """
        sql_check = """
            SELECT 1
            FROM rh.consolidado_incidencias
            WHERE id = %s
            LIMIT 1
        """

        insertados = 0
        actualizados = 0
        duplicados = 0
        errores = 0
        omitidos_sin_empleado = 0

        for item in datos:
            try:
                cursor.execute("SAVEPOINT sp_incidencia")
                id_evento = item.get("id")
                employee_id = item.get("employee_id")
                if id_evento is None or employee_id not in empleados:
                    omitidos_sin_empleado += 1
                    cursor.execute("RELEASE SAVEPOINT sp_incidencia")
                    continue

                start_date = _to_date(item.get("start_date"))
                end_date = _to_date(item.get("end_date"))
                created_at = _to_datetime(item.get("created_at"))
                type_permission = item.get("type_permission")
                days_count = item.get("days_count")
                if days_count is None:
                    days_count = _calc_days_count(start_date, end_date)

                cursor.execute(
                    sql_check,
                    (id_evento,),
                )
                existe = cursor.fetchone()

                if existe:
                    cursor.execute(
                        sql_update,
                        (
                            employee_id,
                            days_count,
                            item.get("day_percent"),
                            type_permission,
                            item.get("status"),
                            id_evento,
                        ),
                    )
                    if cursor.rowcount > 0:
                        actualizados += 1
                    else:
                        duplicados += 1
                else:
                    cursor.execute(
                        sql_insert,
                        (
                            id_evento,
                            employee_id,
                            days_count,
                            item.get("day_percent"),
                            type_permission,
                            start_date,
                            end_date,
                            item.get("status"),
                            created_at,
                        ),
                    )
                    insertados += 1

                cursor.execute("RELEASE SAVEPOINT sp_incidencia")
            except Exception as e_row:
                cursor.execute("ROLLBACK TO SAVEPOINT sp_incidencia")
                cursor.execute("RELEASE SAVEPOINT sp_incidencia")
                errores += 1
                _registrar_error_fila(
                    entidad="consolidado_incidencias",
                    clave=str(item.get("id", item.get("employee_id", "sin_id"))),
                    payload=item,
                    error=e_row,
                )
                logger.exception(f"Error procesando incidencia employee_id={item.get('employee_id')}: {e_row}")

        conexion.commit()
        logger.info(
            "Carga de incidencias finalizada. "
            f"Inserts: {insertados}, Updates: {actualizados}, Duplicados omitidos: {duplicados}, Errores: {errores}. "
            f"Omitidos por employee_id no existente en employees: {omitidos_sin_empleado}."
        )
    except Exception as e:
        conexion.rollback()
        logger.exception(f"Error cargando incidencias en consolidado: {e}")
        raise
    finally:
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

        # DDL: garantizar_tablas_rh() en main.py
        cursor = conexion.cursor()
        logger.info("Conectado a PostgreSQL (schema rh).")

        logger.info("Insertando/Actualizando empleados en la tabla rh.employees...")
        procesados = 0
        errores = 0

        sql_upsert = """
            INSERT INTO rh.employees (
                id, person_id, full_name, first_name, last_name, rut, active_since, status,
                start_date, end_date, name_role, area_id, email, personal_email, rut_boss,
                address, district, region, phone, gender, birthday, university, degree, bank,
                account_type, account_number, payment_method, base_wage, nationality, civil_status,
                health_company, pension_regime, pension_fund, active_until, afc, retired,
                retirement_regime, termination_reason, contract_type, contract_finishing_date_1,
                contract_finishing_date_2, cost_center, ctrlit_recinto, picture_url, updated_at, created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, NOW(), NOW()
            )
            ON CONFLICT (id) DO UPDATE SET
                person_id = EXCLUDED.person_id,
                full_name = EXCLUDED.full_name,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                rut = EXCLUDED.rut,
                active_since = EXCLUDED.active_since,
                status = EXCLUDED.status,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                name_role = EXCLUDED.name_role,
                area_id = EXCLUDED.area_id,
                email = EXCLUDED.email,
                personal_email = EXCLUDED.personal_email,
                rut_boss = EXCLUDED.rut_boss,
                address = EXCLUDED.address,
                district = EXCLUDED.district,
                region = EXCLUDED.region,
                phone = EXCLUDED.phone,
                gender = EXCLUDED.gender,
                birthday = EXCLUDED.birthday,
                university = EXCLUDED.university,
                degree = EXCLUDED.degree,
                bank = EXCLUDED.bank,
                account_type = EXCLUDED.account_type,
                account_number = EXCLUDED.account_number,
                payment_method = EXCLUDED.payment_method,
                base_wage = EXCLUDED.base_wage,
                nationality = EXCLUDED.nationality,
                civil_status = EXCLUDED.civil_status,
                health_company = EXCLUDED.health_company,
                pension_regime = EXCLUDED.pension_regime,
                pension_fund = EXCLUDED.pension_fund,
                active_until = EXCLUDED.active_until,
                afc = EXCLUDED.afc,
                retired = EXCLUDED.retired,
                retirement_regime = EXCLUDED.retirement_regime,
                termination_reason = EXCLUDED.termination_reason,
                contract_type = EXCLUDED.contract_type,
                contract_finishing_date_1 = EXCLUDED.contract_finishing_date_1,
                contract_finishing_date_2 = EXCLUDED.contract_finishing_date_2,
                cost_center = EXCLUDED.cost_center,
                ctrlit_recinto = EXCLUDED.ctrlit_recinto,
                picture_url = EXCLUDED.picture_url,
                updated_at = NOW()
        """

        logger.info("=== MUESTRA DE DATOS ANTES DE INSERTAR ===")
        for i, emp in enumerate(empleados_filtrados[:5]):  # Solo los primeros 5
            logger.info(f"Empleado {i+1}:")
            logger.info(f"  RUT: {emp.get('rut')}")
            logger.info(f"  account_number: '{emp.get('account_number')}' (len: {len(str(emp.get('account_number'))) if emp.get('account_number') else 0})")
            logger.info(f"  base_wage: {emp.get('base_wage')} (type: {type(emp.get('base_wage'))})")
        logger.info("=" * 50)
        cursor.execute("SELECT id FROM rh.areas")
        area_ids_validos = {row[0] for row in cursor.fetchall()}
        empleados_con_area_invalida = 0

        for e in empleados_filtrados:
            try:
                cursor.execute("SAVEPOINT sp_empleado")
                area_id = e.get("area_id")
                if area_id not in area_ids_validos:
                    area_id = None
                    empleados_con_area_invalida += 1

                values_upsert = (
                    e.get("id"), e.get("person_id"), e.get("full_name"), e.get("first_name"), e.get("last_name"), e.get("rut"),
                    e.get("active_since"), e.get("status"), e.get("start_date"), e.get("end_date"), e.get("name_role"),
                    area_id, e.get("email"), e.get("personal_email"), e.get("rut_boss"), e.get("address"),
                    e.get("district"), e.get("region"), e.get("phone"), e.get("gender"), e.get("birthday"),
                    e.get("university"), e.get("degree"), e.get("bank"), e.get("account_type"), e.get("account_number"),
                    e.get("payment_method"), e.get("base_wage"), e.get("nationality"), e.get("civil_status"),
                    e.get("health_company"), e.get("pension_regime"), e.get("pension_fund"), e.get("active_until"),
                    e.get("afc"), e.get("retired"), e.get("retirement_regime"), e.get("termination_reason"),
                    e.get("contract_type"), e.get("contract_finishing_date_1"), e.get("contract_finishing_date_2"),
                    e.get("cost_center"), e.get("ctrlit_recinto"), e.get("picture_url")
                )
                cursor.execute(sql_upsert, values_upsert)
                cursor.execute("RELEASE SAVEPOINT sp_empleado")

                procesados += 1
                if procesados % 100 == 0:
                    logger.info(f"Procesados {procesados} empleados...")
                    conexion.commit() # Commit parcial

            except Exception as error:
                # Evita dejar la transacción en estado abortado por un solo registro
                cursor.execute("ROLLBACK TO SAVEPOINT sp_empleado")
                cursor.execute("RELEASE SAVEPOINT sp_empleado")
                _registrar_error_fila(
                    entidad="employees",
                    clave=str(e.get("id", e.get("rut", "sin_id"))),
                    payload=e,
                    error=error,
                )
                logger.exception(f"Error procesando empleado {e.get('rut', 'N/A')}: {error}")
                errores += 1

        conexion.commit() # Guarda todos los cambios finales

        logger.info(f"Procesamiento completado. Total: {procesados}, Errores: {errores}")
        logger.info(f"Empleados con area_id no existente en rh.areas (guardados con NULL): {empleados_con_area_invalida}")

        logger.info(
            "Desactivación automática de empleados deshabilitada (opción B). "
            "El estado se mantiene según datos de API y upsert individual."
        )

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

    # Query preparada (upsert PostgreSQL)
    sql_upsert = """
        INSERT INTO rh.areas (
            id, name, address, first_level_id, first_level_name,
            second_level_id, second_level_name, cost_center, status, city, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            address = EXCLUDED.address,
            first_level_id = EXCLUDED.first_level_id,
            first_level_name = EXCLUDED.first_level_name,
            second_level_id = EXCLUDED.second_level_id,
            second_level_name = EXCLUDED.second_level_name,
            cost_center = EXCLUDED.cost_center,
            status = EXCLUDED.status,
            city = EXCLUDED.city,
            updated_at = NOW()
    """

    try:
        for i, area in enumerate(areas_datos):
            try:
                cursor.execute("SAVEPOINT sp_area")
                area_id = area["id"]
                
                cursor.execute(sql_upsert, (
                    area_id, area["name"], area["address"],
                    area["first_level_id"], area["first_level_name"],
                    area["second_level_id"], area["second_level_name"],
                    area["cost_center"], area["status"], area["city"]
                ))
                stats["insert"] += 1
                cursor.execute("RELEASE SAVEPOINT sp_area")
                
                # Commit parcial cada 50 registros para no saturar el log de transacciones
                if (i + 1) % 50 == 0:
                    conexion.commit()
                    print(f"   Progreso: {i + 1}/{len(areas_datos)}...")

            except Exception as e_row:
                cursor.execute("ROLLBACK TO SAVEPOINT sp_area")
                cursor.execute("RELEASE SAVEPOINT sp_area")
                _registrar_error_fila(
                    entidad="areas",
                    clave=str(area.get("id", "sin_id")),
                    payload=area,
                    error=e_row,
                )
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

    sql_upsert = """
        INSERT INTO rh.vacations (
            id, employee_id, working_days, workday_stage, type, status,
            start_date, end_date, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (id) DO UPDATE SET
            employee_id = EXCLUDED.employee_id,
            working_days = EXCLUDED.working_days,
            workday_stage = EXCLUDED.workday_stage,
            type = EXCLUDED.type,
            status = EXCLUDED.status,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            updated_at = NOW()
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
            cursor.execute("SAVEPOINT sp_vacacion")
            employee_id = vac.get("employee_id")
            start_date_clean = limpiar_fecha(vac.get('start_date'), es_solo_fecha=True)
            end_date_clean = limpiar_fecha(vac.get('end_date'), es_solo_fecha=True)

            cursor.execute(
                sql_upsert,
                (
                    vac['id'],
                    employee_id,
                    vac['working_days'],
                    vac['workday_stage'],
                    vac['type'],
                    vac['status'],
                    start_date_clean,
                    end_date_clean,
                ),
            )
            stats["insert"] += 1
            cursor.execute("RELEASE SAVEPOINT sp_vacacion")
            
            if (index + 1) % batch_size == 0:
                conexion.commit()
                time.sleep(0.1) 
                print(f"   ... Lote procesado: {index + 1} registros guardados.")

        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp_vacacion")
            cursor.execute("RELEASE SAVEPOINT sp_vacacion")
            _registrar_error_fila(
                entidad="vacations",
                clave=str(vac.get("employee_id", vac.get("id", "sin_id"))),
                payload=vac,
                error=e,
            )
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