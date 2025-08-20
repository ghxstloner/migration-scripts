import pandas as pd
import mysql.connector
from mysql.connector import Error
from tqdm import tqdm
import re
from datetime import datetime
import unicodedata

# ==============================================================================
# CONFIGURACIÓN - ¡IMPORTANTE! DEBES RELLENAR ESTA SECCIÓN
# ==============================================================================

# --- Conexión a la Base de Datos ---
DB_CONFIG = {
    'host': 'localhost',        # O la IP de tu servidor de base de datos
    'user': 'root',
    'password': 'root',
    'database': 'aitsa_rrhh'
}

# --- Ruta al Archivo de Origen ---
EXCEL_FILE_PATH = 'formatos/Control de Capacitaciones excel.xlsx'

# ==============================================================================
# FIN DE LA CONFIGURACIÓN
# ==============================================================================


def connect_db():
    """Establece la conexión con la base de datos."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            print("Conexión a la base de datos establecida correctamente.")
            return conn
    except Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def clean_text(text):
    """Limpia y estandariza una cadena de texto."""
    if not isinstance(text, str):
        return str(text) if text is not None else ""
    # Elimina saltos de línea, múltiples espacios y espacios al inicio/final
    return re.sub(r'\s+', ' ', text).strip()

def normalize_name(name):
    """Normaliza un nombre removiendo acentos y convirtiendo a minúsculas."""
    if not name:
        return ""
    # Remover acentos
    name = unicodedata.normalize('NFD', name)
    name = ''.join(char for char in name if unicodedata.category(char) != 'Mn')
    return name.lower().strip()

def get_or_create(cursor, table, data_dict, lookup_column):
    """
    Busca un registro en una tabla. Si no existe, lo crea.
    Devuelve el ID del registro.
    """
    lookup_value = data_dict[lookup_column]
    
    query = f"SELECT * FROM {table} WHERE {lookup_column} = %s"
    cursor.execute(query, (lookup_value,))
    result = cursor.fetchone()
    cursor.fetchall()  # Consumir cualquier resultado pendiente
    
    if result:
        # Devolver solo el ID (el primer elemento del tuple)
        return result[0]
    else:
        columns = ', '.join(data_dict.keys())
        placeholders = ', '.join(['%s'] * len(data_dict))
        insert_query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        cursor.execute(insert_query, list(data_dict.values()))
        return cursor.lastrowid

def find_employee_id(cursor, first_name, last_name):
    """Busca el personal_id en la tabla nompersonal con búsqueda inteligente."""
    
    # Normalizar nombres
    first_name_norm = normalize_name(first_name)
    last_name_norm = normalize_name(last_name)
    
    # Estrategia 1: Búsqueda exacta
    query = """
        SELECT personal_id FROM nompersonal 
        WHERE LOWER(nombres) LIKE %s AND LOWER(apellidos) LIKE %s
    """
    cursor.execute(query, (f"%{first_name_norm}%", f"%{last_name_norm}%"))
    result = cursor.fetchone()
    cursor.fetchall()  # Consumir resultados pendientes
    
    if result:
        return result[0]
    
    # Estrategia 2: Solo el primer nombre si es compuesto
    first_name_parts = first_name_norm.split()
    if len(first_name_parts) > 1:
        first_name_simple = first_name_parts[0]
        cursor.execute(query, (f"%{first_name_simple}%", f"%{last_name_norm}%"))
        result = cursor.fetchone()
        cursor.fetchall()
        
        if result:
            return result[0]
    
    # Estrategia 3: Búsqueda por apellido y primera letra del nombre
    if first_name_norm:
        cursor.execute(query, (f"{first_name_norm[0]}%", f"%{last_name_norm}%"))
        result = cursor.fetchone()
        cursor.fetchall()
        
        if result:
            return result[0]
    
    return None

def parse_dates(date_string):
    """Intenta extraer fecha de inicio y fin de los formatos de texto."""
    if not isinstance(date_string, str):
        return None, None
        
    date_string = date_string.lower().replace('.', '')
    
    # Mapeo de meses en español
    meses = {"enero": "01", "febrero": "02", "marzo": "03", "abril": "04", "mayo": "05", "junio": "06", 
             "julio": "07", "agosto": "08", "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"}

    # Caso: "18 de marzo al 1 de julio de 2022"
    match = re.search(r'(\d{1,2} de \w+) al (\d{1,2} de \w+ de \d{4})', date_string)
    if match:
        start_part, end_part = match.groups()
        # Extraer el año del final
        year = end_part.split(' de ')[-1]
        start_str = start_part + " de " + year
        end_str = end_part
        
        for k, v in meses.items():
            start_str = start_str.replace(k, v)
            end_str = end_str.replace(k, v)
        
        try:
            start_date = datetime.strptime(start_str.replace(' de ', '-'), '%d-%m-%Y').date()
            end_date = datetime.strptime(end_str.replace(' de ', '-'), '%d-%m-%Y').date()
            return start_date, end_date
        except ValueError:
            pass

    # Caso: "16 y 17 de junio de 2022"
    match = re.search(r'(\d{1,2}) y (\d{1,2} de \w+ de \d{4})', date_string)
    if match:
        day1, rest = match.groups()
        try:
            date_str_clean = rest
            for k, v in meses.items():
                date_str_clean = date_str_clean.replace(k, v)
            
            end_date = datetime.strptime(date_str_clean.replace(' de ', '-'), '%d-%m-%Y').date()
            start_date = end_date.replace(day=int(day1))
            return start_date, end_date
        except ValueError:
            pass

    # Caso: "16 de junio de 2022" (fecha única)
    match = re.search(r'(\d{1,2} de \w+ de \d{4})', date_string)
    if match:
        date_str = match.group(1)
        try:
            for k, v in meses.items():
                date_str = date_str.replace(k, v)
            
            parsed_date = datetime.strptime(date_str.replace(' de ', '-'), '%d-%m-%Y').date()
            return parsed_date, parsed_date
        except ValueError:
            pass

    # Si falla todo, intentamos un parse simple para fechas con comas
    try:
        date_str_clean = date_string.split(',')[0].strip()
        for k, v in meses.items():
            date_str_clean = date_str_clean.replace(k, v)
        
        parsed_date = datetime.strptime(date_str_clean.replace(' de ', '-'), '%d-%m-%Y').date()
        return parsed_date, parsed_date
    except (ValueError, IndexError):
        return None, None


def main():
    """Función principal que ejecuta el proceso de migración."""
    print("Iniciando proceso de migración...")
    
    # --- 1. EXTRACCIÓN ---
    try:
        df = pd.read_excel(EXCEL_FILE_PATH, sheet_name='Control de Capacitaciones 2022')
        print(f"Archivo Excel '{EXCEL_FILE_PATH}' cargado. {len(df)} filas encontradas.")
    except FileNotFoundError:
        print(f"ERROR: No se encontró el archivo en la ruta: {EXCEL_FILE_PATH}")
        return
    except Exception as e:
        print(f"ERROR: No se pudo leer el archivo Excel: {e}")
        return

    # --- 2. TRANSFORMACIÓN ---
    print("Transformando datos...")
    
    # Definir las columnas a propagar (rellenar hacia abajo) - CORREGIDO el warning
    cols_to_fill = ['NOMBRE DE LA CAPACITACIÓN', 'OBJETIVO', 'PROVEEDOR', 'FECHA', 'LUGAR / MODALIDAD', 'COSTO POR COLABORADOR']
    df[cols_to_fill] = df[cols_to_fill].ffill()  # Usar ffill() en lugar de fillna(method='ffill')
    
    # Eliminar filas sin nombre o apellido de empleado
    df.dropna(subset=['NOMBRE', 'APELLIDO'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    print(f"Datos transformados. {len(df)} registros de inscripción válidos para procesar.")

    # --- 3. CARGA ---
    conn = connect_db()
    if not conn:
        return
        
    # Usar buffered=True para evitar "Unread result found"
    cursor = conn.cursor(buffered=True)
    
    cache = {
        'proveedores': {},
        'cursos': {},
        'ofertas': {}
    }
    
    skipped_employees = set()

    try:
        print("Iniciando carga de datos en la base de datos...")
        for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Procesando inscripciones"):
            
            nombre_curso = clean_text(row.get('NOMBRE DE LA CAPACITACIÓN'))
            objetivo = clean_text(row.get('OBJETIVO'))
            proveedor_nombre = clean_text(row.get('PROVEEDOR'))
            nombre_empleado = clean_text(row.get('NOMBRE'))
            apellido_empleado = clean_text(row.get('APELLIDO'))
            costo = pd.to_numeric(row.get('COSTO POR COLABORADOR'), errors='coerce')
            modalidad = str(row.get('LUGAR / MODALIDAD')).strip().upper()
            
            if not nombre_curso or not proveedor_nombre:
                continue

            # Procesar proveedor
            if proveedor_nombre not in cache['proveedores']:
                proveedor_data = {'nombre_proveedor': proveedor_nombre}
                proveedor_id = get_or_create(cursor, 'capacitaciones_proveedores', proveedor_data, 'nombre_proveedor')
                cache['proveedores'][proveedor_nombre] = proveedor_id
            proveedor_id = cache['proveedores'][proveedor_nombre]
            
            # Procesar curso
            if nombre_curso not in cache['cursos']:
                curso_data = {
                    'nombre_curso': nombre_curso,
                    'objetivo_curso': objetivo,
                    'tipo': 'Externa',
                    'ambito': 'Nacional'
                }
                curso_id = get_or_create(cursor, 'capacitaciones_cursos', curso_data, 'nombre_curso')
                cache['cursos'][nombre_curso] = curso_id
            curso_id = cache['cursos'][nombre_curso]

            # Procesar fechas
            fecha_str = row.get('FECHA')
            fecha_inicio, fecha_fin = parse_dates(fecha_str)
            
            if not fecha_inicio:
                print(f"\nADVERTENCIA: No se pudo parsear la fecha '{fecha_str}' para el curso '{nombre_curso}'. Saltando esta oferta.")
                continue

            # Procesar oferta
            oferta_key = (curso_id, proveedor_id, fecha_inicio.isoformat(), fecha_fin.isoformat())
            if oferta_key not in cache['ofertas']:
                # Definir modalidades válidas
                modalidades_validas = ['PRESENCIAL', 'VIRTUAL', 'E-LEARNING', 'HIBRIDO']
                oferta_data = {
                    'curso_id': curso_id,
                    'proveedor_id': proveedor_id,
                    'fecha_inicio': fecha_inicio,
                    'fecha_fin': fecha_fin,
                    'modalidad': modalidad if modalidad in modalidades_validas else 'PRESENCIAL',
                    'costo_por_participante': costo if pd.notna(costo) else 0.00
                }
                columns = ', '.join(oferta_data.keys())
                placeholders = ', '.join(['%s'] * len(oferta_data))
                insert_query = f"INSERT INTO capacitaciones_ofertas_cursos ({columns}) VALUES ({placeholders})"
                cursor.execute(insert_query, list(oferta_data.values()))
                oferta_id = cursor.lastrowid
                cache['ofertas'][oferta_key] = oferta_id
            oferta_id = cache['ofertas'][oferta_key]

            # Buscar empleado
            personal_id = find_employee_id(cursor, nombre_empleado, apellido_empleado)
            if not personal_id:
                employee_full_name = f"{nombre_empleado} {apellido_empleado}"
                if employee_full_name not in skipped_employees:
                    print(f"\nADVERTENCIA: No se encontró el empleado '{employee_full_name}' en la tabla 'nompersonal'. Se omitirán sus inscripciones.")
                    skipped_employees.add(employee_full_name)
                continue

            # Verificar si ya existe la inscripción
            check_query = "SELECT inscripcion_id FROM capacitaciones_inscripciones WHERE personal_id = %s AND oferta_id = %s"
            cursor.execute(check_query, (personal_id, oferta_id))
            existing = cursor.fetchone()
            cursor.fetchall()  # Consumir resultados pendientes
            
            if existing:
                continue

            # Crear inscripción
            inscripcion_data = {
                'personal_id': personal_id,
                'oferta_id': oferta_id,
                'estado_asistencia': 'Asistió',
                'costo_final_participante': costo if pd.notna(costo) else 0.00,
                'fecha_inscripcion': fecha_inicio
            }
            columns = ', '.join(inscripcion_data.keys())
            placeholders = ', '.join(['%s'] * len(inscripcion_data))
            insert_query = f"INSERT INTO capacitaciones_inscripciones ({columns}) VALUES ({placeholders})"
            cursor.execute(insert_query, list(inscripcion_data.values()))

        conn.commit()
        print("\n¡Migración completada con éxito!")
        if skipped_employees:
            print(f"\nEmpleados no encontrados y omitidos ({len(skipped_employees)}):")
            for emp in sorted(list(skipped_employees)):
                print(f"- {emp}")

    except Error as e:
        print(f"\nERROR: Ocurrió un error durante la carga de datos. Se revertirán los cambios. Detalle: {e}")
        conn.rollback()
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("Conexión a la base de datos cerrada.")


if __name__ == '__main__':
    main()