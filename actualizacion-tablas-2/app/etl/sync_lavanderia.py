from app.utils.logger import setup_logger

logger = setup_logger(__name__)

# Empleados activos con área completa. Se excluyen filas sin rut, nombre, cargo
# o sin los tres niveles de área (second_level_id → subarea, first_level_id → empresa).
_QUERY_SOURCE = """
    SELECT
        e.rut,
        e.full_name                 AS nombre_completo,
        e.name_role                 AS cargo,
        e.area_id,
        a.second_level_id           AS subarea_id,
        a.first_level_id            AS empresa_id,
        (e.status = 'activo')       AS activo,
        e.picture_url               AS url_picture
    FROM rh.employees e
    JOIN rh.areas a ON e.area_id = a.id
    WHERE e.rut IS NOT NULL
      AND e.full_name IS NOT NULL
      AND e.name_role IS NOT NULL
      AND e.area_id IS NOT NULL
      AND a.second_level_id IS NOT NULL
      AND a.first_level_id IS NOT NULL
"""

# No se tocan talla_id ni huella_digital — son gestionados por la app de lavandería.
_SQL_UPSERT = """
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


def sincronizar_personal_lavanderia(conn_rh, conn_lav) -> dict:
    cur_rh = conn_rh.cursor()
    cur_lav = conn_lav.cursor()
    procesados = errores = omitidos = 0

    try:
        cur_rh.execute(_QUERY_SOURCE)
        rows = cur_rh.fetchall()
        logger.info(f"Empleados a sincronizar con lavandería: {len(rows)}")

        for row in rows:
            rut = row[0]
            try:
                cur_lav.execute("SAVEPOINT sp_personal")
                cur_lav.execute(_SQL_UPSERT, row)
                cur_lav.execute("RELEASE SAVEPOINT sp_personal")
                procesados += 1

                if procesados % 100 == 0:
                    conn_lav.commit()
                    logger.info(f"Procesados {procesados} registros...")

            except Exception as e:
                cur_lav.execute("ROLLBACK TO SAVEPOINT sp_personal")
                cur_lav.execute("RELEASE SAVEPOINT sp_personal")
                # FK violation más probable: area_id/subarea_id/empresa_id no existe en lavandería
                logger.warning(f"Error sincronizando rut={rut}: {e}")
                errores += 1

        conn_lav.commit()
        logger.info(
            f"Sync lavandería finalizado — Procesados: {procesados}, Errores: {errores}"
        )
        return {"procesados": procesados, "errores": errores}

    except Exception as e:
        conn_lav.rollback()
        logger.error(f"Error fatal sincronizando personal lavandería: {e}")
        raise
    finally:
        cur_rh.close()
        cur_lav.close()
