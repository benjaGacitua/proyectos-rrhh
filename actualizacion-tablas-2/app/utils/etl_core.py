import sys
import math
import time
import app.utils.staging_manager as staging

def ejecutar_flujo_etl(nombre_entidad, funcion_extraccion, funcion_carga, conexion_db):
    """
    Función maestra que orquesta el ETL para cualquier entidad.
    
    Args:
        nombre_entidad (str): Ej. "vacaciones", "empleados" (para logs y archivos).
        funcion_extraccion (callable): Función que retorna una lista de datos (API).
        funcion_carga (callable): Función que recibe (conexion, datos) e inserta en SQL.
        conexion_db (obj): Objeto de conexión a base de datos.
    """
    
    print(f"\n{'='*10} PROCESANDO: {nombre_entidad.upper()} {'='*10}")
    
    datos_procesar = []
    origen_datos = "API"

    # 1. VERIFICAR SI HAY TRABAJO PENDIENTE (STAGING)
    datos_cache = staging.cargar_temporal(nombre_entidad)
    
    if datos_cache:
        print(f"Se encontraron datos previos pendientes para '{nombre_entidad}'.")
        # Aquí puedes poner un input() si quieres preguntar, 
        # o asumimos 'True' para automatización.
        usar_cache = input("¿Usar datos locales? (s/n): ").lower() == 's'
        
        if usar_cache:
            datos_procesar = datos_cache
            origen_datos = "CACHE"
    
    # 2. EXTRACCIÓN (Si no usamos caché)
    if not datos_procesar:
        print(f"Iniciando extracción desde API para {nombre_entidad}...")
        try:
            # Ejecutamos la función que nos pasaron como parámetro
            datos_procesar = funcion_extraccion() 
            
            if datos_procesar:
                # Guardamos inmediatamente por seguridad
                staging.guardar_temporal(nombre_entidad, datos_procesar)
            else:
                print(f"La API no devolvió datos para {nombre_entidad}. Saltando proceso.")
                return False
        except Exception as e:
            print(f"Error crítico extrayendo {nombre_entidad}: {e}")
            return False

    # 3. CARGA (LOAD)
    total_registros = len(datos_procesar)
    print(f"Preparando carga de {total_registros} registros ({origen_datos}) a SQL...")
    
    # Definimos un tamaño de bloque seguro para no saturar la red
    TAMANO_BLOQUE = 5000 
    bloques_totales = math.ceil(total_registros / TAMANO_BLOQUE)
    errores_fatales = False

    try:
        for i in range(0, total_registros, TAMANO_BLOQUE):
            # Recortamos el pedazo de la lista
            lote = datos_procesar[i : i + TAMANO_BLOQUE]
            numero_bloque = (i // TAMANO_BLOQUE) + 1
            
            print(f"   -> Procesando Bloque {numero_bloque}/{bloques_totales} ({len(lote)} registros)...")
            
            conexion_temporal = None
            try:
                conexion_temporal = conexion_db

                funcion_carga(conexion_temporal, lote)

                time.sleep(1)

            except Exception as e:
                print(f"Error CRÍTICO en el bloque {numero_bloque}: {e}")
                errores_fatales = True
                # Si falla un bloque masivo, detenemos para no corromper datos futuros
                if conexion_temporal:
                    try:
                        conexion_temporal.close()
                    except:
                        pass

        # 4. LIMPIEZA (Solo si la carga no falló)
        if not errores_fatales:
            print(f"{nombre_entidad} procesado exitosamente.")
            staging.eliminar_temporal(nombre_entidad)
            return True
        else:
            print(f"El proceso de {nombre_entidad} terminó con errores. No se borra el temporal.")
            return False
            
    except Exception as e:
        print(f"Error general durante la orquestación de carga: {e}")
        return False