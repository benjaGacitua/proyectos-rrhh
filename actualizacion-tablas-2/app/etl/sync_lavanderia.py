from app.utils.logger import setup_logger

logger = setup_logger(__name__)

_QUERY_AREAS_RH = """
    SELECT DISTINCT
        id               AS rh_area_id,
        name             AS nombre_area,
        second_level_id  AS rh_subarea_id,
        second_level_name AS nombre_subarea,
        first_level_id   AS rh_empresa_id,
        first_level_name AS nombre_empresa
    FROM rh.areas
    WHERE name IS NOT NULL
"""

_QUERY_EMPLOYEES = """
    SELECT
        e.rut,
        e.full_name             AS nombre_completo,
        e.name_role             AS cargo,
        e.area_id               AS rh_area_id,
        a.second_level_id       AS rh_subarea_id,
        a.first_level_id        AS rh_empresa_id,
        (e.status = 'activo')   AS activo,
        e.picture_url           AS url_picture
    FROM rh.employees e
    JOIN rh.areas a ON e.area_id = a.id
    WHERE e.rut IS NOT NULL
      AND e.full_name IS NOT NULL
      AND e.name_role IS NOT NULL
      AND e.area_id IS NOT NULL
      AND a.second_level_id IS NOT NULL
      AND a.first_level_id IS NOT NULL
"""

_SQL_UPSERT_PERSONAL = """
    INSERT INTO public.personal (
        rut, nombre_completo, cargo, area_id, subarea_id, empresa_id, activo, url_picture
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (rut) DO UPDATE SET
        nombre_completo = EXCLUDED.nombre_completo,
        cargo           = EXCLUDED.cargo,
        area_id         = EXCLUDED.area_id,
        subarea_id      = EXCLUDED.subarea_id,
        empresa_id      = EXCLUDED.empresa_id,
        activo          = EXCLUDED.activo,
        url_picture     = EXCLUDED.url_picture
"""


def _sync_tablas_referencia(cur_rh, cur_lav, conn_lav):
    """
    Sincroniza areas, subareas y empresa en lavandería desde rh.areas usando
    los nombres como clave (UNIQUE constraint). Retorna tres dicts de mapeo:
        rh_area_id      → lav area_id
        rh_subarea_id   → lav subarea_id
        rh_empresa_id   → lav empresa_id
    """
    cur_rh.execute(_QUERY_AREAS_RH)
    filas = cur_rh.fetchall()

    rh_to_lav_area = {}
    rh_to_lav_subarea = {}
    rh_to_lav_empresa = {}

    for rh_area_id, nombre_area, rh_subarea_id, nombre_subarea, rh_empresa_id, nombre_empresa in filas:
        if nombre_area and rh_area_id is not None:
            cur_lav.execute(
                """
                INSERT INTO public.areas (nombre_area) VALUES (%s)
                ON CONFLICT (nombre_area) DO UPDATE SET nombre_area = EXCLUDED.nombre_area
                RETURNING area_id
                """,
                (nombre_area,),
            )
            rh_to_lav_area[rh_area_id] = cur_lav.fetchone()[0]

        if nombre_subarea and rh_subarea_id is not None:
            cur_lav.execute(
                """
                INSERT INTO public.subareas (nombre_subarea) VALUES (%s)
                ON CONFLICT (nombre_subarea) DO UPDATE SET nombre_subarea = EXCLUDED.nombre_subarea
                RETURNING subarea_id
                """,
                (nombre_subarea,),
            )
            rh_to_lav_subarea[rh_subarea_id] = cur_lav.fetchone()[0]

        if nombre_empresa and rh_empresa_id is not None:
            cur_lav.execute(
                """
                INSERT INTO public.empresa (nombre_empresa) VALUES (%s)
                ON CONFLICT (nombre_empresa) DO UPDATE SET nombre_empresa = EXCLUDED.nombre_empresa
                RETURNING empresa_id
                """,
                (nombre_empresa,),
            )
            rh_to_lav_empresa[rh_empresa_id] = cur_lav.fetchone()[0]

    conn_lav.commit()
    logger.info(
        f"Tablas referencia sincronizadas — "
        f"areas: {len(rh_to_lav_area)}, subareas: {len(rh_to_lav_subarea)}, "
        f"empresas: {len(rh_to_lav_empresa)}"
    )
    return rh_to_lav_area, rh_to_lav_subarea, rh_to_lav_empresa


def sincronizar_personal_lavanderia(conn_rh, conn_lav) -> dict:
    cur_rh = conn_rh.cursor()
    cur_lav = conn_lav.cursor()
    procesados = errores = omitidos = 0

    try:
        rh_to_lav_area, rh_to_lav_subarea, rh_to_lav_empresa = _sync_tablas_referencia(
            cur_rh, cur_lav, conn_lav
        )

        cur_rh.execute(_QUERY_EMPLOYEES)
        rows = cur_rh.fetchall()
        logger.info(f"Empleados candidatos a sincronizar: {len(rows)}")

        for rut, nombre_completo, cargo, rh_area_id, rh_subarea_id, rh_empresa_id, activo, url_picture in rows:
            lav_area_id    = rh_to_lav_area.get(rh_area_id)
            lav_subarea_id = rh_to_lav_subarea.get(rh_subarea_id)
            lav_empresa_id = rh_to_lav_empresa.get(rh_empresa_id)

            if not lav_area_id or not lav_subarea_id or not lav_empresa_id:
                logger.warning(
                    f"Omitiendo rut={rut}: sin mapeo para "
                    f"area={rh_area_id}, subarea={rh_subarea_id}, empresa={rh_empresa_id}"
                )
                omitidos += 1
                continue

            try:
                cur_lav.execute("SAVEPOINT sp_personal")
                cur_lav.execute(
                    _SQL_UPSERT_PERSONAL,
                    (rut, nombre_completo, cargo, lav_area_id, lav_subarea_id, lav_empresa_id, activo, url_picture),
                )
                cur_lav.execute("RELEASE SAVEPOINT sp_personal")
                procesados += 1

                if procesados % 100 == 0:
                    conn_lav.commit()
                    logger.info(f"Procesados {procesados} registros...")

            except Exception as e:
                cur_lav.execute("ROLLBACK TO SAVEPOINT sp_personal")
                cur_lav.execute("RELEASE SAVEPOINT sp_personal")
                logger.warning(f"Error sincronizando rut={rut}: {e}")
                errores += 1

        conn_lav.commit()
        logger.info(
            f"Sync lavandería finalizado — Procesados: {procesados}, "
            f"Omitidos: {omitidos}, Errores: {errores}"
        )
        return {"procesados": procesados, "omitidos": omitidos, "errores": errores}

    except Exception as e:
        conn_lav.rollback()
        logger.error(f"Error fatal sincronizando personal lavandería: {e}")
        raise
    finally:
        cur_rh.close()
        cur_lav.close()
