import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
import re
from datetime import datetime, timedelta, date

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
    if pd.isna(num_empleado):
        return None
    
    # Convertir a string para poder usar re.sub
    num_empleado_str = str(num_empleado)

    try:
        solo_digitos = re.sub(r'\D', '', num_empleado_str)
        if solo_digitos:
            return int(solo_digitos)
        else:
            return None
    except (ValueError, TypeError):
        return None

def obtener_empleado_por_ficha(cursor, ficha):
    """Busca un empleado por su número de ficha y retorna información completa."""
    if ficha is None:
        return None
    try:
        query = """SELECT personal_id, cedula, apenom, 
                          nomposicion_id, fecing, useruid, usuario_workflow, ficha
                   FROM nompersonal 
                   WHERE ficha = %s"""
        cursor.execute(query, (ficha,))
        resultado = cursor.fetchone()
        return resultado if resultado else None
    except Error as e:
        print(f"Error buscando ficha {ficha}: {e}")
        return None

def verificar_periodo_existente(cursor, cedula, fecha_inicio, fecha_fin):
    """Verifica si ya existe un período de vacaciones para las fechas dadas."""
    try:
        query = """SELECT id FROM periodos_vacaciones 
                   WHERE cedula = %s AND fini_periodo = %s AND ffin_periodo = %s AND tipo = 1"""
        cursor.execute(query, (cedula, fecha_inicio, fecha_fin))
        return cursor.fetchone() is not None
    except Error as e:
        print(f"Error verificando período existente: {e}")
        return False

def normalizar_fecha(fecha_input):
    """
    Convierte cualquier tipo de fecha (str, date, datetime) a datetime.
    Esta función resuelve el conflicto de tipos.
    """
    if fecha_input is None:
        return None
    
    if isinstance(fecha_input, str):
        try:
            return datetime.strptime(fecha_input, '%Y-%m-%d')
        except ValueError:
            try:
                return datetime.strptime(fecha_input, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                return None
    elif isinstance(fecha_input, date) and not isinstance(fecha_input, datetime):
        # Convertir date a datetime (hora 00:00:00)
        return datetime.combine(fecha_input, datetime.min.time())
    elif isinstance(fecha_input, datetime):
        return fecha_input
    else:
        return None

def crear_fecha_segura(anio, mes, dia):
    """Crea una fecha válida, ajustando el día si es necesario (por ejemplo, 29 de febrero en años no bisiestos)."""
    try:
        return datetime(anio, mes, dia)
    except ValueError:
        # Si el día no existe, usar el último día del mes
        if mes == 2:
            # Si es febrero, usar 28 o 29 según el año
            if (anio % 4 == 0 and (anio % 100 != 0 or anio % 400 == 0)):
                return datetime(anio, 2, 29)
            else:
                return datetime(anio, 2, 28)
        else:
            # Para otros meses, usar el último día del mes
            for d in range(31, 27, -1):
                try:
                    return datetime(anio, mes, d)
                except ValueError:
                    continue
        raise

def generar_periodos_historicos(cursor, empleado_info, dias_pendientes_totales):
    """
    Calcula y genera los períodos históricos de vacaciones basado en la fecha de ingreso
    y los días pendientes, distribuyéndolos en bloques de 30.
    """
    try:
        personal_id, cedula, nombre_completo, _, fecing, _, _, ficha = empleado_info
        
        if not fecing:
            return False, f"Empleado con ficha {ficha} no tiene fecha de ingreso (fecing)."

        dias_a_migrar = int(float(dias_pendientes_totales))
        fecha_actual = datetime.now()
        
        # Normalizar fecha_ingreso para evitar conflictos de tipos
        fecha_ingreso = normalizar_fecha(fecing)
        if fecha_ingreso is None:
            return False, f"Empleado con ficha {ficha} tiene fecha de ingreso inválida: {fecing}"

        # Determinar el último aniversario de trabajo que ya pasó
        ultimo_aniversario = crear_fecha_segura(fecha_actual.year, fecha_ingreso.month, fecha_ingreso.day)
        if ultimo_aniversario > fecha_actual:
            ultimo_aniversario = crear_fecha_segura(fecha_actual.year - 1, fecha_ingreso.month, fecha_ingreso.day)

        anio_fin_periodo = ultimo_aniversario.year
        periodos_creados = 0
        periodos_existentes = 0

        while dias_a_migrar > 0:
            # Usar crear_fecha_segura para ambas fechas del período
            print(f"Creando período para año {anio_fin_periodo}, mes={fecha_ingreso.month}, dia={fecha_ingreso.day} (ficha: {ficha})")

            try:
                # Fecha fin del período (ej: 14-05-2024)
                fecha_fin_periodo_dt = crear_fecha_segura(anio_fin_periodo, fecha_ingreso.month, fecha_ingreso.day) - timedelta(days=1)
                
                # Fecha inicio del período (ej: 15-05-2023) - usar crear_fecha_segura para el año anterior
                fecha_inicio_periodo_dt = crear_fecha_segura(anio_fin_periodo - 1, fecha_ingreso.month, fecha_ingreso.day)
                
            except Exception as e:
                print(f"❌ Error creando fechas para período {anio_fin_periodo} (ficha {ficha}): {e}")
                return False, f"Error creando fechas para período {anio_fin_periodo}: {e}"

            fecha_inicio_str = fecha_inicio_periodo_dt.strftime('%Y-%m-%d')
            fecha_fin_str = fecha_fin_periodo_dt.strftime('%Y-%m-%d')

            # Verificar que las fechas son lógicas (fin debe ser después de inicio)
            if fecha_fin_periodo_dt <= fecha_inicio_periodo_dt:
                print(f"❌ Error: Fecha fin ({fecha_fin_str}) no es posterior a fecha inicio ({fecha_inicio_str}) para ficha {ficha}")
                anio_fin_periodo -= 1
                if anio_fin_periodo < fecha_ingreso.year:
                    break
                continue

            if verificar_periodo_existente(cursor, cedula, fecha_inicio_str, fecha_fin_str):
                print(f"📋 Período {fecha_inicio_str} al {fecha_fin_str} ya existe para ficha {ficha}")
                anio_fin_periodo -= 1
                periodos_existentes += 1
                # Si el período más reciente ya existe, asumimos que no hay nada que migrar
                if anio_fin_periodo < fecha_ingreso.year:
                    break
                continue

            dias_este_periodo = min(dias_a_migrar, 30)
            descripcion = f"Saldo histórico migrado ({dias_este_periodo} de {dias_pendientes_totales}) - Período {fecha_inicio_periodo_dt.year}-{fecha_fin_periodo_dt.year}"
            
            insert_query = """INSERT INTO periodos_vacaciones 
                              (cedula, tipo, fini_periodo, ffin_periodo, asignados, saldo
                               estatus, observacion, fecha_creacion, usuario_creacion, fecha_efectivas
                               saldo_anterior, no_resolucion, fecha_resolucion)
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            
            cursor.execute(insert_query, (
                cedula, 1, fecha_inicio_str, fecha_fin_str,
                dias_este_periodo, dias_este_periodo,
                1, descripcion, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'MIGRACION_PYTHON',
                datetime.now().strftime('%Y-%m-%d'), 0, f'MIG-{ficha}-{fecha_inicio_periodo_dt.year}', datetime.now().strftime('%Y-%m-%d')
            ))
            
            print(f"✅ Período creado: {fecha_inicio_str} al {fecha_fin_str} con {dias_este_periodo} días (ficha {ficha})")
            periodos_creados += 1
            dias_a_migrar -= dias_este_periodo
            anio_fin_periodo -= 1

            # Evitar crear períodos antes de la fecha de ingreso
            if anio_fin_periodo < fecha_ingreso.year:
                print(f"⚠️ Se alcanzó el año de ingreso ({fecha_ingreso.year}), deteniendo creación de períodos para ficha {ficha}")
                break

        if periodos_creados > 0:
            return True, f"{dias_pendientes_totales} días distribuidos en {periodos_creados} período(s) histórico(s)."
        elif periodos_existentes > 0:
            return False, "Todos los períodos calculados ya existían en la base de datos."
        else:
            return False, "No se generaron períodos."

    except Error as e:
        return False, f"Error de base de datos: {e}"
    except Exception as e:
        return False, f"Error inesperado en cálculo de períodos: {e}"

def migrar_vacaciones_desde_excel(ruta_excel, connection):
    """Función principal para leer el Excel y migrar las vacaciones."""
    print(f"\n=== Iniciando Migración de Vacaciones desde: {ruta_excel} ===")
    
    try:
        df = pd.read_excel(ruta_excel, dtype=str)
    except FileNotFoundError:
        print(f"❌ ERROR: Archivo no encontrado en la ruta: {ruta_excel}")
        return
    except Exception as e:
        print(f"❌ ERROR: No se pudo leer el archivo Excel. Causa: {e}")
        return

    print(f"Filas totales en el archivo: {len(df)}")
    
    # Mapeo de columnas para mayor flexibilidad
    MAPEO_COLUMNAS = {
        'ficha': 'No. D mplado',
        'dias_pendientes': 'DIAS PENDIENTES A LA FECHA'
    }
    
    # Limpiar nombres de columnas en el DataFrame
    df.columns = df.columns.str.strip().str.replace(r'\s+', ' ', regex=True)
    print(f"Columnas detectadas: {df.columns.tolist()}")

    columnas_necesarias = list(MAPEO_COLUMNAS.values())
    columnas_faltantes = [col for col in columnas_necesarias if col not in df.columns]
    
    if columnas_faltantes:
        print(f"❌ ERROR: Columnas faltantes en el Excel: {columnas_faltantes}")
        return

    df_valido = df.dropna(subset=columnas_necesarias)
    print(f"Filas con datos suficientes para procesar: {len(df_valido)}")
    
    if df_valido.empty:
        print("No hay datos válidos para procesar. Finalizando.")
        return
    
    cursor = connection.cursor()
    migrados = 0
    errores = 0
    no_encontrados = 0
    sin_dias = 0
    ya_existentes = 0

    print(f"\n=== Iniciando procesamiento de {len(df_valido)} registros ===")

    for index, row in df_valido.iterrows():
        ficha_raw = row.get(MAPEO_COLUMNAS['ficha'])
        dias_pendientes_raw = row.get(MAPEO_COLUMNAS['dias_pendientes'])
        
        try:
            ficha = limpiar_ficha(ficha_raw)
            if ficha is None:
                print(f"⚠️ Fila {index+2}: 'No. D mplado' ('{ficha_raw}') inválido. SALTANDO.")
                errores += 1
                continue
            
            try:
                dias_pendientes = int(float(dias_pendientes_raw))
                if dias_pendientes <= 0:
                    sin_dias += 1
                    continue
            except (ValueError, TypeError, AttributeError):
                print(f"⚠️ Fila {index+2}: 'Días pendientes' ('{dias_pendientes_raw}') inválidos para ficha {ficha}. SALTANDO.")
                errores += 1
                continue

            empleado_info = obtener_empleado_por_ficha(cursor, ficha)
            if not empleado_info:
                print(f"❓ Fila {index+2}: Empleado con ficha {ficha} no encontrado en la BD. SALTANDO.")
                no_encontrados += 1
                continue
            
            nombre_completo = empleado_info[2]
            
            migrado, mensaje = generar_periodos_historicos(cursor, empleado_info, dias_pendientes)
            
            if migrado:
                print(f"✅ Ficha {ficha} ({nombre_completo}): {mensaje}")
                migrados += 1
            else:
                if "ya existían" in mensaje:
                    ya_existentes += 1
                    print(f"📋 Ficha {ficha} ({nombre_completo}): {mensaje}")
                else:
                    errores += 1
                    print(f"❌ Fila {index+2}: Error migrando ficha {ficha}. {mensaje}")

        except Exception as e:
            print(f"❌ Fila {index+2}: Error general inesperado procesando ficha {ficha_raw}: {e}")
            errores += 1
            continue

    try:
        if migrados > 0:
            connection.commit()
            print(f"\n✅ Transacción confirmada en la base de datos. {migrados} empleado(s) actualizado(s).")
        else:
            print("\nℹ️ No se realizaron cambios en la base de datos, no es necesario confirmar la transacción.")
    except Error as e:
        print(f"\n❌ Error al confirmar la transacción: {e}")
        connection.rollback()
    
    cursor.close()
    
    print("\n" + "="*70)
    print("=== RESUMEN DE LA MIGRACIÓN DE VACACIONES ===")
    print(f"📊 Registros en Excel con datos válidos: {len(df_valido)}")
    print(f"✅ Empleados con saldos migrados:       {migrados}")
    print(f"📋 Empleados con períodos ya existentes: {ya_existentes}")
    print(f"ℹ️ Empleados sin días pendientes:        {sin_dias}")
    print(f"❓ Empleados no encontrados en BD:       {no_encontrados}")
    print(f"❌ Registros con errores:                {errores}")
    print("="*70)

# --- Bloque de Ejecución Principal ---
if __name__ == "__main__":
    print("🚀 MIGRADOR DE VACACIONES PENDIENTES (Versión Histórica)")
    print("=" * 60)
    
    db_connection = crear_conexion_db()

    if db_connection:
        # Asegúrate de que esta ruta sea correcta
        ruta_archivo_excel = 'formatos/VACACIONES-JUNIO.xlsx'
        
        if not os.path.exists(ruta_archivo_excel):
            print(f"❌ El archivo no se encuentra en la ruta especificada: {ruta_archivo_excel}")
        else:
            print(f"📁 Archivo encontrado: {ruta_archivo_excel}")
            migrar_vacaciones_desde_excel(ruta_archivo_excel, db_connection)
            
        db_connection.close()
        print("\n🏁 Conexión a la base de datos cerrada.")
    else:
        print("❌ No se pudo establecer conexión con la base de datos.")

    print("🏁 Proceso de migración de vacaciones completado.")