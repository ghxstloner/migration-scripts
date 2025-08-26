import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
import re
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta

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

def limpiar_tablas_vacaciones(cursor):
    """Limpia la tabla de vacaciones usando TRUNCATE para reiniciar el auto_increment."""
    try:
        print("Limpiando tabla periodos_vacaciones...")
        # Usamos TRUNCATE para mayor eficiencia y para reiniciar el AUTO_INCREMENT
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cursor.execute("TRUNCATE TABLE periodos_vacaciones")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        print("Tabla limpiada correctamente.")
        return True
    except Error as e:
        print(f"Error limpiando tablas: {e}")
        return False

def limpiar_ficha(num_empleado):
    """Limpia el número de empleado, extrayendo solo los dígitos."""
    if pd.isna(num_empleado):
        return None
    try:
        # Extrae todos los dígitos de la cadena
        solo_digitos = re.sub(r'\D', '', str(num_empleado))
        return int(solo_digitos) if solo_digitos else None
    except (ValueError, TypeError):
        return None

def limpiar_cedula(cedula_raw):
    """Limpia y formatea el número de cédula."""
    if pd.isna(cedula_raw):
        return None
    # Elimina todo lo que no sea número o letra (para casos como E-8-12345)
    return re.sub(r'[^a-zA-Z0-9]', '', str(cedula_raw)).strip()

def obtener_empleado_por_ficha_cedula(cursor, ficha, cedula):
    """Busca un empleado por su número de ficha o cédula."""
    try:
        query = "SELECT personal_id, cedula, apenom, fecing, ficha FROM nompersonal WHERE ficha = %s OR cedula = %s"
        cursor.execute(query, (ficha, cedula))
        return cursor.fetchone()
    except Error as e:
        print(f"Error buscando empleado (ficha {ficha}, cédula {cedula}): {e}")
        return None

def normalizar_fecha(fecha_input):
    """Convierte de forma segura varios formatos de fecha a un objeto datetime."""
    if pd.isna(fecha_input):
        return None
    if isinstance(fecha_input, datetime):
        return fecha_input
    if isinstance(fecha_input, date):
        return datetime.combine(fecha_input, datetime.min.time())
    return None # Si no es un tipo de fecha reconocido, no se procesa

def generar_periodos_historicos(cursor, empleado_info, dias_pendientes, dias_caducados):
    """
    Genera los períodos históricos de forma precisa, distribuyendo tanto los días
    caducados como el saldo en sus respectivos períodos hacia atrás, respetando
    la regla de adquisición de derecho a los 11 meses.
    """
    try:
        personal_id, cedula, nombre_completo, fecing, ficha = empleado_info
        
        fecha_ingreso = normalizar_fecha(fecing)
        if not fecha_ingreso:
            return False, f"Empleado {ficha} no tiene fecha de ingreso válida."

        dias_saldo = int(float(dias_pendientes)) if pd.notna(dias_pendientes) else 0
        dias_caducados_int = int(float(dias_caducados)) if pd.notna(dias_caducados) else 0
        
        # --- MANEJO DE SALDO NEGATIVO ---
        if dias_saldo < 0:
            fecha_actual = datetime.now()
            anio_actual_aniversario = fecha_ingreso.year + (fecha_actual.year - fecha_ingreso.year)
            fecha_aniversario_actual = fecha_ingreso.replace(year=anio_actual_aniversario)
            
            if fecha_actual < fecha_aniversario_actual:
                fecha_inicio_periodo = fecha_aniversario_actual - relativedelta(years=1)
                fecha_fin_periodo = fecha_aniversario_actual - timedelta(days=1)
            else:
                fecha_inicio_periodo = fecha_aniversario_actual
                fecha_fin_periodo = fecha_aniversario_actual + relativedelta(years=1) - timedelta(days=1)
            
            descripcion = f"Ajuste por migración de saldo negativo: {dias_saldo} días"
            cursor.execute(
                """INSERT INTO periodos_vacaciones (cedula, tipo, fini_periodo, ffin_periodo, asignados, dias, saldo, caducados, estatus, observacion, saldo_anterior)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (cedula, 4, fecha_inicio_periodo.date(), fecha_fin_periodo.date(), 0, abs(dias_saldo), dias_saldo, 0, 1, descripcion, 0)
            )
            return True, f"Ajuste por saldo negativo ({dias_saldo} días) creado."

        if dias_saldo <= 0 and dias_caducados_int <= 0:
            return False, "No hay días positivos para migrar."

        # --- LÓGICA UNIFICADA Y CORREGIDA PARA CREAR PERÍODOS HISTÓRICOS ---
        
        dias_caducados_restantes = dias_caducados_int
        dias_saldo_restantes = dias_saldo
        fecha_actual = datetime.now()
        
        # --- INICIO DE LA MODIFICACIÓN CLAVE ---
        # Determina el punto de partida del bucle basado en la regla de los 11 meses.
        # Esto asegura que se incluya el período actual si el derecho ya fue adquirido.
        aniversario_en_anio_actual = fecha_ingreso.replace(year=fecha_actual.year)
        fecha_derecho_adquirido = aniversario_en_anio_actual - relativedelta(months=1)

        if fecha_actual >= fecha_derecho_adquirido:
            # Si ya se cumplieron 11 meses del período actual, el punto de partida es el aniversario de este año.
            punto_partida_loop = aniversario_en_anio_actual
        else:
            # Si no, el último período con derecho adquirido fue el del año pasado.
            punto_partida_loop = aniversario_en_anio_actual - relativedelta(years=1)
        
        anio_periodo_actual = punto_partida_loop.year
        # --- FIN DE LA MODIFICACIÓN CLAVE ---
        
        periodos_creados = 0

        while (dias_saldo_restantes > 0 or dias_caducados_restantes > 0) and anio_periodo_actual >= fecha_ingreso.year:
            
            # La fecha de fin del período de trabajo es el día del aniversario (fecha de adquisición)
            fecha_adquisicion = fecha_ingreso.replace(year=anio_periodo_actual)
            fecha_inicio_periodo_trabajo = fecha_adquisicion - relativedelta(years=1)
            
            if fecha_inicio_periodo_trabajo < fecha_ingreso:
                fecha_inicio_periodo_trabajo = fecha_ingreso
            if fecha_adquisicion <= fecha_inicio_periodo_trabajo:
                break
            
            fecha_vencimiento = fecha_adquisicion + relativedelta(years=3)
            es_periodo_caducado = fecha_actual > fecha_vencimiento

            saldo_este_periodo = 0
            caducados_este_periodo = 0

            if es_periodo_caducado and dias_caducados_restantes > 0:
                caducados_este_periodo = min(dias_caducados_restantes, 30)
                dias_caducados_restantes -= caducados_este_periodo
            elif not es_periodo_caducado and dias_saldo_restantes > 0:
                saldo_este_periodo = min(dias_saldo_restantes, 30)
                dias_saldo_restantes -= saldo_este_periodo
            
            if saldo_este_periodo > 0 or caducados_este_periodo > 0:
                total_asignados = saldo_este_periodo + caducados_este_periodo
                descripcion = f"Migración histórica - Saldo: {saldo_este_periodo}, Caducados: {caducados_este_periodo}"

                # El período en la BD va desde el inicio del trabajo hasta el día ANTES del aniversario
                fini_db = fecha_inicio_periodo_trabajo.date()
                ffin_db = (fecha_adquisicion - timedelta(days=1)).date()

                cursor.execute(
                    """INSERT INTO periodos_vacaciones (cedula, tipo, fini_periodo, ffin_periodo, asignados, dias, saldo, caducados, estatus, observacion, saldo_anterior)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (cedula, 1, fini_db, ffin_db, total_asignados, 0, saldo_este_periodo, caducados_este_periodo, 1, descripcion, 0)
                )
                periodos_creados += 1

            anio_periodo_actual -= 1

        if periodos_creados > 0:
            return True, f"Migrados {dias_saldo} días de saldo y {dias_caducados_int} caducados en {periodos_creados} período(s) históricos."
        else:
            return False, "No se generaron períodos (revisar datos de entrada)."

    except Error as e:
        return False, f"Error de base de datos: {e}"
    except Exception as e:
        return False, f"Error inesperado en la lógica de generación: {e}"

def migrar_vacaciones_desde_excel(ruta_excel, connection):
    """Función principal para leer el Excel y migrar las vacaciones."""
    print(f"\nIniciando Migración de Vacaciones desde: {ruta_excel}")
    
    try:
        df = pd.read_excel(ruta_excel, dtype=str)
    except Exception as e:
        print(f"ERROR: No se pudo leer el archivo Excel. Causa: {e}")
        return

    df.columns = df.columns.str.strip()
    
    # Mapeo de columnas robusto a posibles variaciones
    MAPEO_COLUMNAS = {
        'ficha': 'NO. DE EMPLEADO',
        'cedula': 'No. DE CEDULA',
        'dias_pendientes': 'DIAS PENDIENTES A LA FECHA',
        'dias_caducados': 'DIAS CADUCADOS'
    }
    
    # Normalizar nombres de columnas en el DataFrame para que coincidan
    df.rename(columns=lambda c: re.sub(r'\s+', ' ', c).strip(), inplace=True)
    
    print(f"Columnas detectadas y normalizadas: {df.columns.tolist()}")

    cursor = connection.cursor()
    if not limpiar_tablas_vacaciones(cursor):
        return
    
    migrados = 0
    errores = 0
    no_encontrados = 0
    sin_dias_para_migrar = 0

    print(f"\nIniciando procesamiento de {len(df)} registros")

    for index, row in df.iterrows():
        ficha_raw = row.get(MAPEO_COLUMNAS['ficha'])
        cedula_raw = row.get(MAPEO_COLUMNAS['cedula'])
        
        ficha = limpiar_ficha(ficha_raw)
        cedula = limpiar_cedula(cedula_raw)
        
        if ficha is None and cedula is None:
            continue

        try:
            empleado_info = obtener_empleado_por_ficha_cedula(cursor, ficha, cedula)
            if not empleado_info:
                print(f"Fila {index+2}: Empleado no encontrado (Ficha: {ficha}, Cédula: {cedula}). SALTANDO.")
                no_encontrados += 1
                continue
            
            dias_pendientes = row.get(MAPEO_COLUMNAS['dias_pendientes'])
            dias_caducados = row.get(MAPEO_COLUMNAS['dias_caducados'])
            
            migrado, mensaje = generar_periodos_historicos(cursor, empleado_info, dias_pendientes, dias_caducados)
            
            if migrado:
                print(f"ÉXITO Ficha {ficha}: {mensaje}")
                migrados += 1
            else:
                if "No hay días" in mensaje:
                    sin_dias_para_migrar += 1
                else:
                    print(f"ERROR Fila {index+2} (Ficha {ficha}): {mensaje}")
                    errores += 1

        except Exception as e:
            print(f"ERROR CRÍTICO Fila {index+2} (Ficha {ficha}): {e}")
            errores += 1
            continue

    if migrados > 0:
        connection.commit()
        print(f"\nTransacción confirmada. {migrados} empleado(s) con datos migrados.")
    else:
        connection.rollback()
        print("\nNo se realizaron cambios en la base de datos.")
    
    cursor.close()
    
    print("\n" + "="*70)
    print("RESUMEN DE LA MIGRACIÓN")
    print(f"Registros procesados:         {len(df)}")
    print(f"Empleados migrados:           {migrados}")
    print(f"Sin días para migrar:         {sin_dias_para_migrar}")
    print(f"No encontrados en BD:         {no_encontrados}")
    print(f"Errores de procesamiento:     {errores}")
    print("="*70)

# --- Ejecución Principal ---
if __name__ == "__main__":
    print("MIGRADOR DE VACACIONES PENDIENTES")
    print("=" * 50)
    
    db_connection = crear_conexion_db()

    if db_connection:
        ruta_archivo_excel = 'formatos/VACACIONES-AGOSTO.xlsx'
        
        if os.path.exists(ruta_archivo_excel):
            print(f"Archivo encontrado: {ruta_archivo_excel}")
            migrar_vacaciones_desde_excel(ruta_archivo_excel, db_connection)
            db_connection.close()
            print("\nConexión cerrada.")
        else:
            print(f"ERROR: El archivo no se encuentra en la ruta: {ruta_archivo_excel}")
    else:
        print("Fallo en la conexión a la base de datos. No se puede continuar.")

    print("Proceso completado.")
