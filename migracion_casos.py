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

def limpiar_y_convertir_ficha(ficha_str):
    """Convierte E03940 a 3940"""
    if pd.isna(ficha_str) or not isinstance(ficha_str, str):
        return None
    numeros = re.findall(r'\d+', ficha_str)
    if numeros:
        return int("".join(numeros))
    return None

def obtener_personal_id_por_ficha(cursor, ficha_str):
    ficha_limpia = limpiar_y_convertir_ficha(ficha_str)
    if ficha_limpia is None:
        return None
    
    try:
        query = "SELECT personal_id FROM nompersonal WHERE ficha = %s"
        cursor.execute(query, (ficha_limpia,))
        resultado = cursor.fetchone()
        return resultado[0] if resultado else None
    except Error as e:
        print(f"Error buscando ficha {ficha_limpia}: {e}")
        return None

def obtener_o_crear_abogado_id(cursor, nombre_abogado):
    if pd.isna(nombre_abogado) or not str(nombre_abogado).strip():
        return None
    
    nombre_abogado = str(nombre_abogado).strip()
    
    # Mapeo específico para abogados conocidos
    mapeo_abogados = {
        'M. Allen': 'Marcos Allen',
        'M.Allen': 'Marcos Allen'
    }
    
    if nombre_abogado in mapeo_abogados:
        nombre_abogado = mapeo_abogados[nombre_abogado]
    
    try:
        query_select = "SELECT id FROM abogados WHERE nombre = %s"
        cursor.execute(query_select, (nombre_abogado,))
        resultado = cursor.fetchone()

        if resultado:
            return resultado[0]
        else:
            print(f"Creando abogado: '{nombre_abogado}'")
            query_insert = "INSERT INTO abogados (nombre) VALUES (%s)"
            cursor.execute(query_insert, (nombre_abogado,))
            return cursor.lastrowid
    except Error as e:
        print(f"Error con abogado '{nombre_abogado}': {e}")
        return None

def limpiar_valor(valor):
    """Limpia valores NaN y los convierte a None para SQL"""
    if pd.isna(valor):
        return None
    return str(valor).strip() if str(valor).strip() else None

def migrar_casos_desde_hoja_excel(ruta_excel, nombre_hoja, connection):
    print(f"\n=== Procesando: {nombre_hoja} ===")
    
    try:
        df = pd.read_excel(ruta_excel, sheet_name=nombre_hoja)
    except Exception as e:
        print(f"ERROR leyendo {nombre_hoja}: {e}")
        return

    print(f"Filas totales: {len(df)}")
    
    # Filtrar solo filas que tengan "No." (ficha del empleado)
    df = df.dropna(subset=['No.'])
    df = df[df['No.'].notna()]
    
    print(f"Filas con No. válido: {len(df)}")
    
    if df.empty:
        print("No hay datos válidos para procesar")
        return
    
    cursor = connection.cursor()
    insertados = 0
    errores = 0

    for index, row in df.iterrows():
        # Validar que tenga Ref (memo_ref)
        memo_ref = limpiar_valor(row.get('Ref'))
        if not memo_ref:
            continue
            
        # Obtener empleado por ficha del campo "No."
        ficha_excel = row.get('No.')
        empleado_id_principal = obtener_personal_id_por_ficha(cursor, ficha_excel)
        
        if empleado_id_principal is None:
            print(f"⚠ Fila {index+1}: Empleado no encontrado para ficha '{ficha_excel}'")
            errores += 1
            continue

        # Mapeo específico por hoja
        if nombre_hoja == 'Internos':
            # En Internos: "Para" es tanto para_caso como abogado responsable
            para_caso_valor = limpiar_valor(row.get('Para'))
            abogado_responsable = limpiar_valor(row.get('Para'))  # Mismo valor para ambos
            de_quien = limpiar_valor(row.get('De'))
            fecha_recibido_col = 'F. Recibido'
            fecha_cierre_col = 'F. Cierre'
            acciones_col = 'Acciones'
            
        elif nombre_hoja == 'Externos':
            # En Externos: no hay columna "Para", usar otra lógica
            para_caso_valor = None  # O algún otro campo si existe
            abogado_responsable = limpiar_valor(row.get('Responsable'))
            de_quien = None  # No hay columna "De" en Externos
            fecha_recibido_col = 'Fecha'
            fecha_cierre_col = 'Fecha de Cierre'
            acciones_col = 'Acción'
        else:
            continue

        # Procesar abogado
        abogado_id = obtener_o_crear_abogado_id(cursor, abogado_responsable)
        
        # Procesar fechas
        fecha_recibido = None
        fecha_recibido_raw = row.get(fecha_recibido_col)
        if pd.notna(fecha_recibido_raw):
            try:
                fecha_recibido = pd.to_datetime(fecha_recibido_raw).strftime('%Y-%m-%d')
            except:
                pass
                
        fecha_cierre = None
        fecha_cierre_raw = row.get(fecha_cierre_col)
        if pd.notna(fecha_cierre_raw):
            try:
                fecha_cierre = pd.to_datetime(fecha_cierre_raw).strftime('%Y-%m-%d')
            except:
                pass
        
        # Otros campos
        asunto = limpiar_valor(row.get('Asunto'))
        acciones_tomadas = limpiar_valor(row.get(acciones_col))
        estado = 'Cerrado' if str(row.get('Estado', '')).strip().lower() == 'cerrado' else 'En Proceso'

        query = """
            INSERT IGNORE INTO casos_legales (
                memo_ref, de_quien, para_caso, asunto, fecha_recibido, 
                fecha_cierre, acciones_tomadas, abogado_responsable_id, estado,
                empleado_id, para_empleado_id, responsable_empleado_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, NULL)
        """
        values = (
            memo_ref,
            de_quien,
            para_caso_valor,  # Ahora usa el valor del campo "Para"
            asunto,
            fecha_recibido,
            fecha_cierre,
            acciones_tomadas,
            abogado_id,
            estado,
            empleado_id_principal
        )
        
        try:
            cursor.execute(query, values)
            if cursor.rowcount > 0:
                print(f"✓ {memo_ref}")
                insertados += 1
            else:
                print(f"⚠ {memo_ref} (ya existía)")
        except Error as e:
            print(f"✗ {memo_ref}: {e}")
            errores += 1
            connection.rollback()
            continue

    connection.commit()
    cursor.close()
    print(f"=== Resultado {nombre_hoja}: {insertados} insertados, {errores} errores ===")

# Ejecución
if __name__ == "__main__":
    db_connection = crear_conexion_db()

    if db_connection:
        ruta_archivo_excel = 'formatos/CasosAbogados.xlsx'
        
        if not os.path.exists(ruta_archivo_excel):
            print(f"Archivo no encontrado: {ruta_archivo_excel}")
        else:
            hojas_a_procesar = ['Internos', 'Externos']
            
            for hoja in hojas_a_procesar:
                migrar_casos_desde_hoja_excel(ruta_archivo_excel, hoja, db_connection)
            
        db_connection.close()
        print("\n🏁 Migración completada")