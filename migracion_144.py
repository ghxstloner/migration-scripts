import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
import re
from datetime import datetime, timedelta

# Cargar variables de entorno desde el archivo .env
load_dotenv()

def crear_conexion_db():
    """Crea y retorna una conexión a la base de datos MySQL."""
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_DATABASE')
        )
        if connection.is_connected():
            print("Conexión a MySQL exitosa.")
            return connection
    except Error as e:
        print(f"Error al conectar a MySQL: {e}")
        return None

def limpiar_ficha(num_empleado):
    """
    Limpia el número de empleado, extrayendo solo los dígitos
    y convirtiéndolo a entero.
    """
    if pd.isna(num_empleado) or not isinstance(num_empleado, str):
        return None

    try:
        solo_digitos = re.sub(r'\D', '', num_empleado)
        if solo_digitos:
            return int(solo_digitos)
        else:
            return None
    except (ValueError, TypeError):
        return None

def obtener_personal_id_por_ficha(cursor, ficha):
    """Busca un empleado por su número de ficha y retorna su ID y nombre."""
    if ficha is None:
        return None
    try:
        query = "SELECT personal_id, CONCAT(nombres, ' ', apellidos) as nombre_completo FROM nompersonal WHERE ficha = %s"
        cursor.execute(query, (ficha,))
        resultado = cursor.fetchone()
        return resultado if resultado else None
    except Error as e:
        print(f"Error buscando ficha {ficha}: {e}")
        return None

def obtener_tipo_justificacion(cursor):
    """Obtiene el ID del tipo de justificación para 144 horas."""
    try:
        query = "SELECT idtipo FROM tipo_justificacion WHERE descripcion LIKE '%144%' OR tiempo_maximo = 144 LIMIT 1"
        cursor.execute(query)
        resultado = cursor.fetchone()
        return resultado[0] if resultado else 2  # Valor por defecto
    except Error as e:
        print(f"Error obteniendo tipo de justificación: {e}")
        return 2

def verificar_horas_ya_acreditadas(cursor, ficha, tipo_justificacion):
    """Verifica si ya se acreditaron las 144 horas este año."""
    try:
        anio_actual = datetime.now().year
        query = """SELECT id FROM dias_incapacidad 
                   WHERE ficha = %s 
                   AND tipo_justificacion = %s
                   AND YEAR(fecha) = %s
                   AND observacion LIKE '%ACREDITACIÓN ANUAL LEY 15%'"""
        cursor.execute(query, (ficha, tipo_justificacion, anio_actual))
        resultado = cursor.fetchone()
        return resultado is not None
    except Error as e:
        print(f"Error verificando horas ya acreditadas: {e}")
        return False

def acreditar_144_horas(cursor, ficha, tipo_justificacion):
    """Acredita las 144 horas en la tabla dias_incapacidad."""
    try:
        fecha_acreditacion = datetime.now().strftime('%Y-%m-%d')
        anio_acreditacion = datetime.now().year
        tiempo_val = 144.0
        observacion_val = f"ACREDITACIÓN ANUAL LEY 15 - Año {anio_acreditacion}"
        fecha_vence_val = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d %H:%M:%S')
        
        # 144 horas equivalen a 18 días en una jornada de 8 horas (144 / 8 = 18)
        dias_val = 18
        horas_val = 0
        minutos_val = 0
        
        dias_restante_val = dias_val
        horas_restante_val = horas_val
        minutos_restante_val = minutos_val
        created_by_val = 'admin'
        
        insert_query = """INSERT INTO dias_incapacidad (
            ficha, tipo_justificacion, fecha, tiempo, observacion, fecha_vence, 
            dias, horas, minutos, dias_restante, horas_restante, minutos_restante, 
            created_by, created_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
        )"""
        
        cursor.execute(insert_query, (
            ficha, tipo_justificacion, fecha_acreditacion, tiempo_val, observacion_val, 
            fecha_vence_val, dias_val, horas_val, minutos_val, dias_restante_val, 
            horas_restante_val, minutos_restante_val, created_by_val
        ))
        
        return cursor.rowcount > 0
    except Error as e:
        print(f"Error acreditando 144 horas: {e}")
        return False

def actualizar_discapacidad_y_horas(cursor, personal_id, ficha):
    """Actualiza los campos de discapacidad y acredita las 144 horas."""
    # Primero actualizar discapacidad
    query = "UPDATE nompersonal SET tiene_discapacidad = 1, discapacidad_senadis = 1 WHERE personal_id = %s"
    
    try:
        cursor.execute(query, (personal_id,))
        discapacidad_actualizada = cursor.rowcount > 0
        
        if not discapacidad_actualizada:
            return False, "Error actualizando discapacidad"
        
        # Obtener tipo de justificación
        tipo_justificacion = obtener_tipo_justificacion(cursor)
        
        # Verificar si ya tiene horas acreditadas este año
        ya_tiene_horas = verificar_horas_ya_acreditadas(cursor, ficha, tipo_justificacion)
        
        if ya_tiene_horas:
            return True, "Discapacidad actualizada - Ya tenía 144 horas este año"
        
        # Acreditar las 144 horas
        horas_acreditadas = acreditar_144_horas(cursor, ficha, tipo_justificacion)
        
        if horas_acreditadas:
            return True, "Discapacidad actualizada + 144 horas acreditadas"
        else:
            return True, "Discapacidad actualizada - Error acreditando horas"
            
    except Error as e:
        print(f"Error al actualizar personal_id {personal_id}: {e}")
        return False, f"Error: {e}"

def migrar_discapacidad_desde_excel(ruta_excel, connection):
    """Función principal para leer el Excel y actualizar los datos de discapacidad."""
    print(f"\n=== Iniciando Migración desde: {ruta_excel} ===")
    
    try:
        df = pd.read_excel(ruta_excel, dtype=str)
    except FileNotFoundError:
        print(f"❌ ERROR: Archivo no encontrado en la ruta: {ruta_excel}")
        return
    except Exception as e:
        print(f"❌ ERROR: No se pudo leer el archivo Excel. Causa: {e}")
        return

    print(f"Filas totales en el archivo: {len(df)}")

    # Limpiar los nombres de las columnas
    df.columns = df.columns.str.strip()
    print(f"Columnas detectadas: {df.columns.tolist()}")
    
    # Solo necesitamos la columna del número de empleado
    if 'N° de Empleado' not in df.columns:
        print(f"❌ ERROR: La columna 'N° de Empleado' no se encontró en el archivo Excel.")
        return

    # Filtrar filas que tengan número de empleado
    df_valido = df.dropna(subset=['N° de Empleado'])
    print(f"Filas con datos suficientes para procesar: {len(df_valido)}")
    
    if df_valido.empty:
        print("No hay datos válidos para procesar. Finalizando.")
        return
    
    cursor = connection.cursor()
    actualizados = 0
    errores = 0
    no_encontrados = 0
    ya_tenian_horas = 0

    for index, row in df_valido.iterrows():
        try:
            ficha = limpiar_ficha(row.get('N° de Empleado'))

            if ficha is None:
                print(f"⚠️ Fila {index+2}: N° de Empleado '{row.get('N° de Empleado')}' inválido. SALTANDO.")
                errores += 1
                continue

            empleado_info = obtener_personal_id_por_ficha(cursor, ficha)
            if not empleado_info:
                print(f"❓ Fila {index+2}: Empleado con ficha {ficha} no encontrado en la BD. SALTANDO.")
                no_encontrados += 1
                continue
            
            personal_id, nombre_completo = empleado_info
            
            actualizado, mensaje = actualizar_discapacidad_y_horas(cursor, personal_id, ficha)
            
            if actualizado:
                print(f"✅ Ficha {ficha} ({nombre_completo}): {mensaje}")
                actualizados += 1
                if "Ya tenía 144 horas" in mensaje:
                    ya_tenian_horas += 1
            else:
                errores += 1
                print(f"❌ Fila {index+2}: Error al actualizar ficha {ficha}. {mensaje}")

        except Exception as e:
            print(f"❌ Fila {index+2}: Error general inesperado: {e}")
            errores += 1
            continue

    # Confirmar todos los cambios en la base de datos
    connection.commit()
    cursor.close()
    print("\n" + "="*60)
    print("=== Resumen de la Migración ===")
    print(f"Registros actualizados exitosamente: {actualizados}")
    print(f"Empleados que ya tenían horas este año: {ya_tenian_horas}")
    print(f"Empleados no encontrados en la BD: {no_encontrados}")
    print(f"Registros con errores o saltados: {errores}")
    print("="*60)


# --- Bloque de Ejecución Principal ---
if __name__ == "__main__":
    db_connection = crear_conexion_db()

    if db_connection:
        ruta_archivo_excel = 'formatos/144_horas.xlsx' 
        
        if not os.path.exists(ruta_archivo_excel):
            print(f"El archivo no se encuentra en la ruta especificada: {ruta_archivo_excel}")
        else:
            print("🚀 Iniciando actualización de discapacidad y acreditación de 144 horas...")
            
            migrar_discapacidad_desde_excel(ruta_archivo_excel, db_connection)
            
        db_connection.close()
        print("\n🏁 Proceso de migración completado.")