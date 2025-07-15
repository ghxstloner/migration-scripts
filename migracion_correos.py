import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
import re

load_dotenv()

def crear_conexion_db():
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_DATABASE')
        )
        if connection.is_connected():
            print("Conectado a MySQL")
            return connection
    except Error as e:
        print(f"Error MySQL: {e}")
        return None

def limpiar_y_convertir_clave(clave_str):
    """Convierte E03940 a 3940 para que coincida con ficha"""
    if pd.isna(clave_str) or not isinstance(clave_str, str):
        return None
    numeros = re.findall(r'\d+', clave_str)
    if numeros:
        return int("".join(numeros))
    return None

def limpiar_correo(correo):
    """Limpia y valida formato de correo electrónico"""
    if pd.isna(correo):
        return None
    
    correo_str = str(correo).strip()
    # Remover comillas y espacios extras
    correo_limpio = correo_str.replace("'", "").replace('"', "").strip()
    
    # Validación básica de formato de correo
    if correo_limpio and "@" in correo_limpio and "." in correo_limpio:
        return correo_limpio
    
    return None

def obtener_personal_id_por_ficha(cursor, ficha):
    """Busca empleado por ficha"""
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

def actualizar_correo_institucional(cursor, personal_id, correo):
    """Actualiza el correo institucional del empleado"""
    try:
        query = "UPDATE nompersonal SET correo_institucional = %s WHERE personal_id = %s"
        cursor.execute(query, (correo, personal_id))
        return cursor.rowcount > 0
    except Error as e:
        print(f"Error actualizando correo para personal_id {personal_id}: {e}")
        return False

def migrar_correos_desde_excel(ruta_excel, connection):
    print(f"\n=== Procesando: Correos Institucionales ===")
    
    try:
        df = pd.read_excel(ruta_excel)
    except Exception as e:
        print(f"ERROR leyendo archivo Excel: {e}")
        return

    print(f"Filas totales: {len(df)}")
    
    # Filtrar filas que tengan al menos clave y correo
    df_valido = df[df['Clave'].notna() & df['Correo electrónico'].notna()]
    print(f"Filas con clave y correo válidos: {len(df_valido)}")
    
    if df_valido.empty:
        print("No hay datos válidos para procesar")
        return
    
    cursor = connection.cursor()
    actualizados = 0
    errores = 0
    no_encontrados = 0

    for index, row in df_valido.iterrows():
        try:
            # Limpiar y convertir clave a ficha
            ficha = limpiar_y_convertir_clave(row['Clave'])
            if ficha is None:
                print(f"⚠ Fila {index+1}: Clave inválida '{row['Clave']}' - SALTANDO")
                errores += 1
                continue
            
            # Limpiar correo
            correo = limpiar_correo(row['Correo electrónico'])
            if correo is None:
                print(f"⚠ Fila {index+1}: Correo inválido '{row['Correo electrónico']}' - SALTANDO")
                errores += 1
                continue
            
            # Buscar empleado por ficha
            empleado_info = obtener_personal_id_por_ficha(cursor, ficha)
            if empleado_info is None:
                print(f"⚠ Fila {index+1}: Empleado no encontrado para ficha {ficha} - SALTANDO")
                no_encontrados += 1
                continue
            
            personal_id, nombre_completo = empleado_info
            
            # Actualizar correo institucional
            if actualizar_correo_institucional(cursor, personal_id, correo):
                print(f"✓ Ficha {ficha} - {nombre_completo} - {correo}")
                actualizados += 1
            else:
                print(f"✗ Fila {index+1}: Error actualizando correo para {nombre_completo}")
                errores += 1

        except Error as e:
            print(f"✗ Fila {index+1}: Error de base de datos - {e}")
            errores += 1
            connection.rollback()
            continue
        except Exception as e:
            print(f"✗ Fila {index+1}: Error general - {e}")
            errores += 1
            continue

    connection.commit()
    cursor.close()
    print(f"=== Resultado Correos: {actualizados} actualizados, {no_encontrados} no encontrados, {errores} errores ===")

def mostrar_estadisticas_correos(connection):
    """Muestra las estadísticas de los correos después de la migración"""
    try:
        cursor = connection.cursor()
        
        print(f"\n=== Estadísticas Post-Migración ===")
        
        # Estadísticas generales
        query_stats = """
            SELECT 
                COUNT(*) as total_empleados,
                COUNT(correo_institucional) as con_correo_institucional,
                COUNT(email) as con_correo_personal,
                COUNT(CASE WHEN correo_institucional IS NOT NULL AND correo_institucional != '' THEN 1 END) as con_correo_institucional_valido
            FROM nompersonal
            WHERE estado != 'De Baja'
        """
        
        cursor.execute(query_stats)
        stats = cursor.fetchone()
        
        print(f"Total empleados activos: {stats[0]}")
        print(f"Con correo institucional: {stats[1]}")
        print(f"Con correo personal: {stats[2]}")
        print(f"Con correo institucional válido: {stats[3]}")
        
        # Empleados sin correo institucional
        query_sin_correo = """
            SELECT ficha, CONCAT(nombres, ' ', apellidos) as nombre_completo
            FROM nompersonal 
            WHERE estado != 'De Baja' 
            AND (correo_institucional IS NULL OR correo_institucional = '')
            ORDER BY ficha
            LIMIT 10
        """
        
        cursor.execute(query_sin_correo)
        sin_correo = cursor.fetchall()
        
        if sin_correo:
            print(f"\n=== Empleados sin correo institucional (muestra de 10) ===")
            for ficha, nombre in sin_correo:
                print(f"Ficha {ficha}: {nombre}")
        
        cursor.close()
        
    except Error as e:
        print(f"Error obteniendo estadísticas: {e}")

# Ejecución principal
if __name__ == "__main__":
    db_connection = crear_conexion_db()

    if db_connection:
        ruta_archivo_excel = 'formatos/Correos_V2.xlsx'
        
        if not os.path.exists(ruta_archivo_excel):
            print(f"Archivo no encontrado: {ruta_archivo_excel}")
        else:
            print("🚀 Iniciando actualización de correos institucionales...")
            print("=" * 60)
            
            migrar_correos_desde_excel(ruta_archivo_excel, db_connection)
            
            # Mostrar estadísticas finales
            mostrar_estadisticas_correos(db_connection)
            
        db_connection.close()
        print("\n🏁 Actualización completada")
        print("=" * 60) 