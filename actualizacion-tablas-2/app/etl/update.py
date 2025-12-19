
#! Este proceso está relacionado con el merge que se realiza en SQL
#! NO en la extracción de datos desde la API y su inserción
#! a la base de datos. Trabaja con datos ya cargados.
def actualizar_tabla_incidencias(conexion, query_crear, query_actualizar):
    
    try:        
        cursor = conexion.cursor()

        if query_crear:
            cursor.execute(query_crear)
            print("Tabla creada/verificada.")

        if query_actualizar:
            cursor.execute(query_actualizar)
            print("Merge ejecutado.")
            
        conexion.commit()
        cursor.close()
        
    except Exception as e:
        print(f"Error en el proceso de carga: {e}")
