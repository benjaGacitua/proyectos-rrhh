IF OBJECT_ID('dbo.consolidado_incidencias', 'U') IS NULL
BEGIN

    CREATE TABLE dbo.consolidado_incidencias (
        id_empleado INT NOT NULL,
        rut_empleado VARCHAR(50),
        nombre_completo VARCHAR(255),
        fecha_inicio DATE NOT NULL,
        fecha_fin DATE,

        dias_duracion AS (DATEDIFF(day, fecha_inicio, fecha_fin) + 1),
        
        creado_en DATETIME2,
        tipo_permiso VARCHAR(100) NOT NULL, 
        
        PRIMARY KEY (id_empleado, fecha_inicio, tipo_permiso)
    );
    PRINT 'Tabla "consolidado_incidencias" creada exitosamente (con dias_duracion).';
END
ELSE
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.columns 
                WHERE Name = N'dias_duracion' 
                AND Object_ID = Object_ID(N'dbo.consolidado_incidencias'))
    BEGIN
        ALTER TABLE dbo.consolidado_incidencias
        ADD dias_duracion AS (DATEDIFF(day, fecha_inicio, fecha_fin) + 1);
        
        PRINT 'Columna "dias_duracion" agregada a la tabla existente.';
    END
    ELSE
    BEGIN
        PRINT 'La tabla "consolidado_incidencias" ya existe y ya tiene la columna "dias_duracion".';
    END
END