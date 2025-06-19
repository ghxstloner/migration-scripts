import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
import re
from datetime import date

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

def limpiar_cedula(cedula):
    """Limpia formato de cédula"""
    if pd.isna(cedula):
        return None
    
    cedula_str = str(cedula).strip()
    # Remover comillas, espacios y guiones extras
    cedula_limpia = cedula_str.replace("'", "").replace('"', "").strip()
    
    # Si tiene espacios o guiones al final, limpiarlos
    if cedula_limpia.endswith(' -') or cedula_limpia.endswith('-'):
        cedula_limpia = cedula_limpia.rstrip(' -')
    
    return cedula_limpia if cedula_limpia else None

def obtener_personal_id_por_ficha(cursor, ficha_str):
    """Busca empleado por ficha"""
    ficha_limpia = limpiar_y_convertir_ficha(ficha_str)
    if ficha_limpia is None:
        return None
    
    try:
        query = "SELECT personal_id FROM nompersonal WHERE ficha = %s AND estado != 'De Baja'"
        cursor.execute(query, (ficha_limpia,))
        resultado = cursor.fetchone()
        return resultado[0] if resultado else None
    except Error as e:
        print(f"Error buscando ficha {ficha_limpia}: {e}")
        return None

def obtener_personal_id_por_cedula(cursor, cedula):
    """Busca empleado por cédula"""
    cedula_limpia = limpiar_cedula(cedula)
    if not cedula_limpia:
        return None
    
    try:
        query = "SELECT personal_id FROM nompersonal WHERE cedula = %s AND estado != 'De Baja'"
        cursor.execute(query, (cedula_limpia,))
        resultado = cursor.fetchone()
        return resultado[0] if resultado else None
    except Error as e:
        print(f"Error buscando cédula {cedula_limpia}: {e}")
        return None

def mapear_tipo_sancion_a_subtipo_id(cursor, tipo_sancion):
    """Mapea el tipo del Excel al ID del subtipo en la BD"""
    if pd.isna(tipo_sancion):
        return None
    
    tipo_limpio = str(tipo_sancion).strip().lower()
    
    # Mapeo de tipos del Excel a nombres en BD
    mapeo_tipos = {
        'amonestación': 'Amonestación Escrita',
        'amonestacion': 'Amonestación Escrita', 
        'amonestación escrita': 'Amonestación Escrita',
        'amonestacion escrita': 'Amonestación Escrita',
        'suspensión': 'Suspensión',
        'suspension': 'Suspensión',
        'verbal': 'Advertencia verbal',
        'advertencia verbal': 'Advertencia verbal',
        'amonestación verbal': 'Advertencia verbal',
        'amonestacion verbal': 'Advertencia verbal',
        'despido': 'Despido',
        # Agregar casos específicos del Excel
        'amonestación panamá solidario': 'Amonestación Escrita',
        'amonestacion panama solidario': 'Amonestación Escrita'
    }
    
    nombre_subtipo = mapeo_tipos.get(tipo_limpio)
    if not nombre_subtipo:
        print(f"⚠ Tipo de sanción no mapeado: '{tipo_sancion}' -> usando 'Amonestación Escrita' por defecto")
        nombre_subtipo = 'Amonestación Escrita'
    
    try:
        query = """SELECT id_expediente_subtipo 
                   FROM expediente_subtipo 
                   WHERE nombre_subtipo = %s AND id_expediente_tipo = 5"""
        cursor.execute(query, (nombre_subtipo,))
        resultado = cursor.fetchone()
        
        if resultado:
            return resultado[0]
        else:
            print(f"⚠ Subtipo no encontrado en BD: '{nombre_subtipo}' - usando ID 3 por defecto")
            return 3  # Amonestación Escrita por defecto
            
    except Error as e:
        print(f"Error buscando subtipo '{nombre_subtipo}': {e}")
        return 3  # Default

def generar_numero_expediente(cursor, subtipo_id):
    """Genera número de expediente incrementando correlativo del subtipo"""
    try:
        # Actualizar correlativo
        query_update = "UPDATE expediente_subtipo SET correlativo = correlativo + 1 WHERE id_expediente_subtipo = %s"
        cursor.execute(query_update, (subtipo_id,))
        
        # Obtener nuevo correlativo
        query_select = "SELECT correlativo FROM expediente_subtipo WHERE id_expediente_subtipo = %s"
        cursor.execute(query_select, (subtipo_id,))
        resultado = cursor.fetchone()
        
        return resultado[0] if resultado else 1
        
    except Error as e:
        print(f"Error generando número de expediente para subtipo {subtipo_id}: {e}")
        return 1

def limpiar_valor(valor):
    """Limpia valores NaN y los convierte a None para SQL"""
    if pd.isna(valor):
        return None
    valor_str = str(valor).strip()
    return valor_str if valor_str and valor_str.lower() != 'nan' else None

def obtener_valor_seguro(valor, default=""):
    """Obtiene un valor seguro que nunca será None para campos NOT NULL"""
    if pd.isna(valor):
        return default
    valor_str = str(valor).strip()
    return valor_str if valor_str and valor_str.lower() != 'nan' else default

def procesar_fecha(fecha_raw):
    """Procesa fechas del Excel de manera segura"""
    if pd.isna(fecha_raw):
        return None
    try:
        return pd.to_datetime(fecha_raw).strftime('%Y-%m-%d')
    except:
        return None

def obtener_cedula_empleado(cursor, personal_id):
    """Obtiene la cédula del empleado por su personal_id"""
    try:
        query = "SELECT cedula FROM nompersonal WHERE personal_id = %s"
        cursor.execute(query, (personal_id,))
        resultado = cursor.fetchone()
        return resultado[0] if resultado else None
    except Error as e:
        print(f"Error obteniendo cédula para personal_id {personal_id}: {e}")
        return None

def migrar_sanciones_desde_excel(ruta_excel, connection):
    print(f"\n=== Procesando: Sanciones ===")
    
    try:
        df = pd.read_excel(ruta_excel)
    except Exception as e:
        print(f"ERROR leyendo archivo Excel: {e}")
        return

    print(f"Filas totales: {len(df)}")
    
    # Filtrar filas que tengan al menos ficha o cédula
    df_valido = df[df['No.'].notna() | df['Cédula '].notna()]
    print(f"Filas con empleado identificable: {len(df_valido)}")
    
    if df_valido.empty:
        print("No hay datos válidos para procesar")
        return
    
    cursor = connection.cursor()
    insertados = 0
    errores = 0

    for index, row in df_valido.iterrows():
        try:
            # Buscar empleado primero por ficha, luego por cédula
            empleado_id = None
            identificador = ""
            
            # Intentar por ficha
            if pd.notna(row.get('No.')):
                empleado_id = obtener_personal_id_por_ficha(cursor, row['No.'])
                identificador = f"ficha {row['No.']}"
            
            # Si no se encontró por ficha, intentar por cédula
            if empleado_id is None and pd.notna(row.get('Cédula ')):
                empleado_id = obtener_personal_id_por_cedula(cursor, row['Cédula '])
                identificador = f"cédula {row['Cédula ']}"
            
            # Validar que se encontró el empleado
            if empleado_id is None:
                print(f"⚠ Fila {index+1}: Empleado no encontrado para {identificador} - SALTANDO")
                errores += 1
                continue
            
            # Obtener cédula del empleado
            cedula_empleado = obtener_cedula_empleado(cursor, empleado_id)
            if not cedula_empleado:
                print(f"⚠ Fila {index+1}: No se pudo obtener cédula del empleado ID {empleado_id}")
                errores += 1
                continue
            
            # Procesar campos obligatorios
            memo = limpiar_valor(row.get('Memo'))
            if not memo:
                memo = f"S/N-{index+1}"  # Generar memo por defecto
            
            fecha = procesar_fecha(row.get('Fecha'))
            if not fecha:
                fecha = date.today().strftime('%Y-%m-%d')  # Usar fecha actual si no hay
            
            tipo_sancion = limpiar_valor(row.get('Tipo'))
            if not tipo_sancion:
                print(f"⚠ Fila {index+1}: Sin tipo de sanción - SALTANDO")
                errores += 1
                continue
            
            # CAMPOS NOT NULL - usar valores seguros
            falta_cometida = obtener_valor_seguro(row.get('Falta Cometida'), "Falta no especificada")
            descripcion = obtener_valor_seguro(row.get('Observaciones'), "Sin observaciones adicionales")
            
            # Mapear tipo de sanción a subtipo
            subtipo_id = mapear_tipo_sancion_a_subtipo_id(cursor, tipo_sancion)
            
            # Generar número de expediente
            accion_nro = generar_numero_expediente(cursor, subtipo_id)
            
            # Verificar si ya existe este memo
            query_check = "SELECT COUNT(*) FROM expediente WHERE memo = %s AND tipo = 5"
            cursor.execute(query_check, (memo,))
            existe = cursor.fetchone()[0] > 0
            
            if existe:
                memo = f"{memo}-{accion_nro}"  # Modificar memo si existe
            
            # Fechas de suspensión (si es suspensión)
            fecha_inicio_suspension = None
            fecha_fin_suspension = None
            
            if tipo_sancion and 'suspens' in tipo_sancion.lower():
                # Buscar si hay columnas de fechas de suspensión
                fecha_inicio_suspension = procesar_fecha(row.get('Fecha Inicio Suspensión'))
                fecha_fin_suspension = procesar_fecha(row.get('Fecha Fin Suspensión'))
            
            # Insertar sanción
            query_insert = """
                INSERT INTO expediente (
                    cedula, personal_id, fecha, fecha_inicio_suspension, fecha_fin_suspension,
                    tipo, subtipo, accion_nro, memo, falta_cometida, descripcion,
                    estatus, fecha_creacion, usuario_creacion
                ) VALUES (
                    %s, %s, %s, %s, %s, 5, %s, %s, %s, %s, %s, 1, NOW(), 'migracion_excel'
                )
            """
            
            valores = (
                cedula_empleado,          # cedula
                empleado_id,              # personal_id
                fecha,                    # fecha
                fecha_inicio_suspension,  # fecha_inicio_suspension
                fecha_fin_suspension,     # fecha_fin_suspension
                subtipo_id,               # subtipo
                accion_nro,               # accion_nro
                memo,                     # memo
                falta_cometida,           # falta_cometida (NOT NULL)
                descripcion               # descripcion (NOT NULL)
            )
            
            cursor.execute(query_insert, valores)
            
            # Obtener nombre del empleado para el log
            query_nombre = "SELECT CONCAT(nombres, ' ', apellidos) FROM nompersonal WHERE personal_id = %s"
            cursor.execute(query_nombre, (empleado_id,))
            nombre_empleado = cursor.fetchone()
            nombre_empleado = nombre_empleado[0] if nombre_empleado else "N/A"
            
            print(f"✓ {memo} - {nombre_empleado} - {tipo_sancion}")
            insertados += 1

        except Error as e:
            print(f"✗ Fila {index+1} ({memo if 'memo' in locals() else 'S/N'}): {e}")
            errores += 1
            connection.rollback()
            continue
        except Exception as e:
            print(f"✗ Fila {index+1}: Error general - {e}")
            errores += 1
            continue

    connection.commit()
    cursor.close()
    print(f"=== Resultado Sanciones: {insertados} insertados, {errores} errores ===")

def mostrar_estadisticas_subtipos(connection):
    """Muestra las estadísticas de los subtipos después de la migración"""
    try:
        cursor = connection.cursor()
        
        print(f"\n=== Estadísticas Post-Migración ===")
        
        query = """
            SELECT 
                es.nombre_subtipo,
                COUNT(e.cod_expediente_det) as cantidad,
                es.correlativo
            FROM expediente_subtipo es
            LEFT JOIN expediente e ON es.id_expediente_subtipo = e.subtipo AND e.tipo = 5
            WHERE es.id_expediente_tipo = 5
            GROUP BY es.id_expediente_subtipo, es.nombre_subtipo, es.correlativo
            ORDER BY cantidad DESC
        """
        
        cursor.execute(query)
        resultados = cursor.fetchall()
        
        print(f"{'Subtipo':<25} {'Cantidad':<10} {'Correlativo':<12}")
        print("=" * 50)
        
        total_sanciones = 0
        for subtipo, cantidad, correlativo in resultados:
            print(f"{subtipo:<25} {cantidad:<10} {correlativo:<12}")
            total_sanciones += cantidad
        
        print("=" * 50)
        print(f"{'TOTAL':<25} {total_sanciones:<10}")
        
        cursor.close()
        
    except Error as e:
        print(f"Error obteniendo estadísticas: {e}")

# Ejecución principal
if __name__ == "__main__":
    db_connection = crear_conexion_db()

    if db_connection:
        ruta_archivo_excel = 'formatos/CasosSanciones.xlsx'
        
        if not os.path.exists(ruta_archivo_excel):
            print(f"Archivo no encontrado: {ruta_archivo_excel}")
        else:
            print("🚀 Iniciando migración de sanciones disciplinarias...")
            print("=" * 60)
            
            migrar_sanciones_desde_excel(ruta_archivo_excel, db_connection)
            
            # Mostrar estadísticas finales
            mostrar_estadisticas_subtipos(db_connection)
            
        db_connection.close()
        print("\n🏁 Migración completada")
        print("=" * 60)