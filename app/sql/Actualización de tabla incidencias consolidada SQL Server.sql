WITH IncidenciasSource AS (
    SELECT
        id_empleado, rut_empleado, nombre_completo, fecha_inicio,
        fecha_fin, creado_en, tipo_permiso,
        estado, -- <--- 1. Aquí seleccionamos la columna para que esté disponible arriba
        ROW_NUMBER() OVER(
            PARTITION BY id_empleado, fecha_inicio, tipo_permiso 
            ORDER BY creado_en DESC 
        ) AS rn
    FROM (
        -- Bloque 1: Permisos
        SELECT
            p.employee_id AS id_empleado, e.rut AS rut_empleado, e.full_name AS nombre_completo,
            p.start_date AS fecha_inicio, p.end_date AS fecha_fin, 
            p.status AS estado, -- Se define el alias 'estado'
            p.created_at AS creado_en,
            p.permission_type_code AS tipo_permiso
        FROM dbo.permissions AS p
        INNER JOIN dbo.employees AS e ON p.employee_id = e.id

        UNION ALL

        -- Bloque 2: Licencias
        SELECT
            l.employee_id, e.rut, e.full_name,
            l.start_date, l.end_date, 
            l.status, -- Hereda el nombre 'estado' del primer bloque
            l.created_at,
            l.licence_type
        FROM dbo.licences AS l
        INNER JOIN dbo.employees AS e ON l.employee_id = e.id

        UNION ALL

        -- Bloque 3: Ausencias
        SELECT
            a.employee_id, e.rut, e.full_name,
            a.start_date, a.end_date, 
            a.status,
            a.created_at,
            a.absence_type_code
        FROM dbo.absences AS a
        INNER JOIN dbo.employees AS e ON a.employee_id = e.id
    ) AS SubQuery
)
MERGE INTO dbo.consolidado_incidencias AS Target
USING (
    SELECT * FROM IncidenciasSource WHERE rn = 1
) AS Source
ON (
    Target.id_empleado = Source.id_empleado AND 
    Target.fecha_inicio = Source.fecha_inicio AND
    Target.tipo_permiso = Source.tipo_permiso
)
-- 2. Actualizamos el status si el registro ya existe
WHEN MATCHED THEN
    UPDATE SET
        Target.fecha_fin = Source.fecha_fin,
        Target.rut_empleado = Source.rut_empleado,
        Target.nombre_completo = Source.nombre_completo,
        Target.status = Source.estado -- <--- Actualización del estado

-- 3. Insertamos el status si es un registro nuevo
WHEN NOT MATCHED BY TARGET THEN
    INSERT (id_empleado, rut_empleado, nombre_completo, fecha_inicio, fecha_fin, creado_en, tipo_permiso, status)
    VALUES (Source.id_empleado, Source.rut_empleado, Source.nombre_completo, Source.fecha_inicio, Source.fecha_fin, Source.creado_en, Source.tipo_permiso, Source.estado);