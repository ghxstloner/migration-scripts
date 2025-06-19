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
        'M.Allen': 'Marcos Allen',
        'R. Rivera': 'Reynaldo Rivera',
        'AFRA': 'AFRA',
        'AFV Asoc': 'AFV Asociados',
        'IGRA': 'IGRA'
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
            query_insert = "INSERT INTO abogados (nombre, activo) VALUES (%s, 1)"
            cursor.execute(query_insert, (nombre_abogado,))
            return cursor.lastrowid
    except Error as e:
        print(f"Error con abogado '{nombre_abogado}': {e}")
        return None

def buscar_empleado_por_nombre_aproximado(cursor, nombre):
    """Busca empleado por nombre aproximado para campos Para/De"""
    if pd.isna(nombre) or not str(nombre).strip():
        return None
    
    nombre = str(nombre).strip()
    
    try:
        # Buscar por coincidencia aproximada
        query = """
            SELECT personal_id 
            FROM nompersonal 
            WHERE estado != 'De Baja' 
            AND (
                CONCAT(nombres, ' ', apellidos) LIKE %s OR
                nombres LIKE %s OR
                apellidos LIKE %s
            )
            LIMIT 1
        """
        search_term = f"%{nombre}%"
        cursor.execute(query, (search_term, search_term, search_term))
        resultado = cursor.fetchone()
        return resultado[0] if resultado else None
    except Error as e:
        print(f"Error buscando empleado '{nombre}': {e}")
        return None

def limpiar_valor(valor):
    """Limpia valores NaN y los convierte a None para SQL"""
    if pd.isna(valor):
        return None
    valor_str = str(valor).strip()
    return valor_str if valor_str and valor_str.lower() != 'nan' else None

def procesar_fecha(fecha_raw):
    """Procesa fechas del Excel de manera segura"""
    if pd.isna(fecha_raw):
        return None
    try:
        return pd.to_datetime(fecha_raw).strftime('%Y-%m-%d')
    except:
        return None

def determinar_nivel_importancia(asunto, para_campo=None, de_campo=None):
    """Determina nivel de importancia basado en palabras clave"""
    texto_completo = f"{asunto or ''} {para_campo or ''} {de_campo or ''}".lower()
    
    palabras_alto = ['urgente', 'crítico', 'demanda', 'legal', 'tribunal', 'juicio', 'sanción', 'juzgado', 'fiscalía']
    palabras_bajo = ['consulta', 'información', 'orientación', 'pregunta']
    
    if any(palabra in texto_completo for palabra in palabras_alto):
        return 'ALTO'
    elif any(palabra in texto_completo for palabra in palabras_bajo):
        return 'BAJO'
    return 'MEDIO'

def determinar_posible_riesgo(asunto, estado=None):
    """Determina posible riesgo basado en contenido"""
    texto = (asunto or '').lower()
    
    palabras_critico = ['demanda', 'juicio', 'tribunal', 'multa', 'fiscalía']
    palabras_alto = ['legal', 'sanción', 'investigación', 'denuncia', 'juzgado']
    palabras_bajo = ['consulta', 'información', 'orientación']
    
    if any(palabra in texto for palabra in palabras_critico):
        return 'CRITICO'
    elif any(palabra in texto for palabra in palabras_alto):
        return 'ALTO'
    elif any(palabra in texto for palabra in palabras_bajo):
        return 'BAJO'
    return 'MEDIO'

def migrar_casos_desde_hoja_excel(ruta_excel, nombre_hoja, connection):
    print(f"\n=== Procesando: {nombre_hoja} ===")
    
    try:
        df = pd.read_excel(ruta_excel, sheet_name=nombre_hoja)
    except Exception as e:
        print(f"ERROR leyendo {nombre_hoja}: {e}")
        return

    print(f"Filas totales: {len(df)}")
    
    # Filtrar solo filas que tengan "Ref" (memo_ref)
    df = df.dropna(subset=['Ref'])
    df = df[df['Ref'].notna()]
    print(f"Filas con Ref válido: {len(df)}")
    
    if df.empty:
        print("No hay datos válidos para procesar")
        return
    
    cursor = connection.cursor()
    insertados = 0
    errores = 0

    for index, row in df.iterrows():
        try:
            # Validar que tenga Ref (memo_ref)
            memo_ref = limpiar_valor(row.get('Ref'))
            if not memo_ref:
                print(f"⚠ Fila {index+1}: Sin memo/referencia")
                errores += 1
                continue
            
            # Asunto puede ser vacío ahora
            asunto = limpiar_valor(row.get('Asunto'))
            # Si no hay asunto, usar un valor por defecto
            if not asunto:
                asunto = f"Sin asunto especificado - {memo_ref}"

            # Obtener empleado - lógica corregida (MANTENER VALIDACIÓN OBLIGATORIA)
            empleado_id_principal = None
            
            # Primero intentar por campo "No." si existe
            ficha_excel = row.get('No.') if 'No.' in df.columns and pd.notna(row.get('No.')) else None
            
            if ficha_excel:
                empleado_id_principal = obtener_personal_id_por_ficha(cursor, ficha_excel)
            
            # Si no se encontró por ficha, intentar por cédula (especialmente para Internos)
            if empleado_id_principal is None:
                cedula = limpiar_valor(row.get('Cédula'))
                if cedula:
                    try:
                        query_cedula = "SELECT personal_id FROM nompersonal WHERE cedula = %s LIMIT 1"
                        cursor.execute(query_cedula, (cedula,))
                        resultado_cedula = cursor.fetchone()
                        empleado_id_principal = resultado_cedula[0] if resultado_cedula else None
                    except Error as e:
                        print(f"Error buscando por cédula {cedula}: {e}")
            
            # SI NO ENCUENTRA EMPLEADO, NO INSERTAR (mantener esta validación)
            if empleado_id_principal is None:
                print(f"⚠ Fila {index+1}: Empleado no encontrado para ficha '{ficha_excel}' o cédula - SALTANDO")
                errores += 1
                continue

            # Mapeo específico por hoja
            if nombre_hoja == 'Externos':
                # Hoja EXTERNOS - NO tiene campos Para/De
                para_valor = None
                de_valor = None
                fecha_recibido_col = 'Fecha'
                fecha_cierre_col = 'Fecha de Cierre'
                acciones_col = 'Acción'
                responsable_col = 'Responsable'
                
                # Campos Para/De son None
                para_empleado_id = None
                de_empleado_id = None
                para_texto_libre = None
                de_texto_libre = None
                
            elif nombre_hoja == 'Internos':
                # Hoja INTERNOS - SÍ tiene campos Para/De
                para_valor = limpiar_valor(row.get('Para'))
                de_valor = limpiar_valor(row.get('De'))
                fecha_recibido_col = 'F. Recibido'
                fecha_cierre_col = 'F. Cierre'
                acciones_col = 'Acciones'
                responsable_col = 'Responsable'
                
                # Intentar buscar empleados por nombre, si no, usar texto libre
                para_empleado_id = buscar_empleado_por_nombre_aproximado(cursor, para_valor) if para_valor else None
                de_empleado_id = buscar_empleado_por_nombre_aproximado(cursor, de_valor) if de_valor else None
                
                para_texto_libre = para_valor if para_valor and not para_empleado_id else None
                de_texto_libre = de_valor if de_valor and not de_empleado_id else None
            else:
                print(f"⚠ Hoja desconocida: {nombre_hoja}")
                continue

            # Procesar abogado responsable
            responsable_valor = limpiar_valor(row.get(responsable_col))
            abogado_responsable_id = obtener_o_crear_abogado_id(cursor, responsable_valor)

            # Procesar fechas
            fecha_recibido = procesar_fecha(row.get(fecha_recibido_col))
            fecha_cierre = procesar_fecha(row.get(fecha_cierre_col))
            
            # Fecha de creación (usar fecha_recibido o hoy)
            fecha_creacion = fecha_recibido if fecha_recibido else date.today().strftime('%Y-%m-%d')
            
            # Otros campos
            acciones_tomadas = limpiar_valor(row.get(acciones_col))
            estado_raw = limpiar_valor(row.get('Estado'))
            estado = 'Cerrado' if estado_raw and estado_raw.lower() in ['cerrado', 'finalizado'] else 'En Proceso'
            
            # Campos nuevos con valores inteligentes
            nivel_importancia = determinar_nivel_importancia(asunto, para_valor, de_valor)
            posible_riesgo = determinar_posible_riesgo(asunto, estado)
            tipo_reporte = 'EXTERNO' if nombre_hoja == 'Externos' else 'INTERNO'

            # Query de inserción SIN IGNORE para permitir duplicados
            query = """
                INSERT INTO casos_legales (
                    empleado_id, memo_ref, asunto, estado, acciones_tomadas,
                    fecha_recibido, fecha_cierre, fecha_creacion, observaciones,
                    de_quien, de_empleado_id, de_texto_libre,
                    para_caso, para_empleado_id, para_texto_libre,
                    abogado_responsable_id, nivel_importancia, posible_riesgo, tipo_reporte,
                    created_by, activo
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1
                )
            """
            
            values = (
                empleado_id_principal,  # empleado_id
                memo_ref,              # memo_ref
                asunto,                # asunto (ahora puede ser el valor por defecto)
                estado,                # estado
                acciones_tomadas,      # acciones_tomadas
                fecha_recibido,        # fecha_recibido
                fecha_cierre,          # fecha_cierre
                fecha_creacion,        # fecha_creacion
                None,                  # observaciones (no viene del Excel)
                de_valor,              # de_quien (campo legacy)
                de_empleado_id,        # de_empleado_id (nuevo)
                de_texto_libre,        # de_texto_libre (nuevo)
                para_valor,            # para_caso (campo legacy)
                para_empleado_id,      # para_empleado_id (nuevo)
                para_texto_libre,      # para_texto_libre (nuevo)
                abogado_responsable_id, # abogado_responsable_id
                nivel_importancia,     # nivel_importancia
                posible_riesgo,        # posible_riesgo
                tipo_reporte,          # tipo_reporte
                'migracion_excel'      # created_by
            )
            
            cursor.execute(query, values)
            print(f"✓ {memo_ref} ({nombre_hoja}) - {asunto[:30]}...")
            insertados += 1

        except Error as e:
            print(f"✗ Fila {index+1} ({memo_ref}): {e}")
            errores += 1
            connection.rollback()
            continue
        except Exception as e:
            print(f"✗ Fila {index+1}: Error general - {e}")
            errores += 1
            continue

    connection.commit()
    cursor.close()
    print(f"=== Resultado {nombre_hoja}: {insertados} insertados, {errores} errores ===")

# Ejecución principal
if __name__ == "__main__":
    db_connection = crear_conexion_db()

    if db_connection:
        ruta_archivo_excel = 'formatos/CasosAbogados.xlsx'
        
        if not os.path.exists(ruta_archivo_excel):
            print(f"Archivo no encontrado: {ruta_archivo_excel}")
        else:
            print("🚀 Iniciando migración de casos legales...")
            print("=" * 60)
            
            hojas_a_procesar = ['Externos', 'Internos']
            
            for hoja in hojas_a_procesar:
                migrar_casos_desde_hoja_excel(ruta_archivo_excel, hoja, db_connection)
            
        db_connection.close()
        print("\n🏁 Migración completada")
        print("=" * 60)