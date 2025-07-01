import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
from decimal import Decimal, InvalidOperation

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
            print("✅ Conectado exitosamente a la base de datos MySQL.")
            return connection
    except Error as e:
        print(f"❌ Error al conectar a MySQL: {e}")
        return None
    return None

def limpiar_valor(valor, tipo='str'):
    """Limpia y convierte valores de Pandas, manejando NaNs."""
    if pd.isna(valor) or str(valor).strip().lower() == 'nan':
        return None
    
    valor_limpio = str(valor).strip()
    
    if tipo == 'int':
        try:
            return int(float(valor_limpio))
        except (ValueError, TypeError):
            return None
    elif tipo == 'decimal':
        try:
            return Decimal(valor_limpio)
        except InvalidOperation:
            return None
    return valor_limpio

def generar_partida_formateada(row):
    """Genera una cadena de partida presupuestaria formateada a partir de una fila."""
    partida_format_rules = {
        'codigo': 3, 'tipo_presupuesto': 1, 'programa': 1,
        'fuente': 3, 'subprograma': 2, 'actividad': 2, 'objeto_gasto': 3
    }
    partes_formateadas = []
    for col, padding in partida_format_rules.items():
        valor_limpio = limpiar_valor(row.get(col), 'int')
        valor_str = str(valor_limpio) if valor_limpio is not None else '0'
        partes_formateadas.append(valor_str.zfill(padding))
    return ".".join(partes_formateadas)

def migrar_partidas_cwprecue(cursor, df):
    """Limpia e inserta las partidas presupuestarias únicas en la tabla cwprecue."""
    print("\n--- Iniciando migración de partidas a `cwprecue` ---")
    
    # 1. Obtener todas las partidas únicas del DataFrame
    unique_partidas = {generar_partida_formateada(row) for index, row in df.iterrows()}
    
    if not unique_partidas:
        print("ℹ️ No se encontraron partidas para migrar a `cwprecue`.")
        return

    print(f"🔍 Se encontraron {len(unique_partidas)} partidas presupuestarias únicas.")

    try:
        # 2. Limpiar la tabla `cwprecue`
        print("🗑️  Limpiando la tabla `cwprecue`...")
        cursor.execute("TRUNCATE TABLE cwprecue")
        
        # 3. Preparar los datos para la inserción
        datos_para_insertar = [
            (partida, partida, 0, '') for partida in unique_partidas
        ]
        
        # 4. Insertar los datos en lote
        query_insert = """
            INSERT INTO cwprecue (CodCue, Denominacion, Tipocta, Tipopuc)
            VALUES (%s, %s, %s, %s)
        """
        cursor.executemany(query_insert, datos_para_insertar)
        print(f"✨ Se insertaron {cursor.rowcount} registros en `cwprecue`.")
        
    except Error as e:
        print(f"❌ Error durante la migración de `cwprecue`: {e}")
        raise # Re-lanza para que la transacción principal falle

def procesar_cargo(cursor, row):
    """Actualiza o inserta un registro en la tabla `nomcargos`."""
    cod_car = limpiar_valor(row.get('cargo_presupuestario'), 'int')
    if not cod_car:
        return

    des_car = limpiar_valor(row.get('desc_cargo'))
    sueldo = limpiar_valor(row.get('sueldo_planilla'), 'decimal')

    try:
        cursor.execute("SELECT cod_cargo FROM nomcargos WHERE cod_car = %s", (cod_car,))
        if cursor.fetchone():
            query = "UPDATE nomcargos SET des_car = %s, sueldo = %s WHERE cod_car = %s"
            values = (des_car, sueldo, cod_car)
            cursor.execute(query, values)
            print(f"  → Cargo actualizado: {cod_car} - {des_car}")
        else:
            query = "INSERT INTO nomcargos (cod_car, des_car, sueldo) VALUES (%s, %s, %s)"
            values = (cod_car, des_car, sueldo)
            cursor.execute(query, values)
            print(f"  → Cargo CREADO: {cod_car} - {des_car}")
    except Error as e:
        print(f"  ❌ Error procesando cargo {cod_car}: {e}")
        raise

def procesar_posicion(cursor, row):
    """Actualiza o inserta un registro en la tabla `nomposicion`."""
    nomposicion_id = limpiar_valor(row.get('posicion'), 'int')
    if not nomposicion_id:
        print("  ⚠️ Fila sin 'posicion', no se puede procesar `nomposicion`.")
        return

    partida_presupuestaria = generar_partida_formateada(row)
    sueldo_propuesto = limpiar_valor(row.get('sueldo_planilla'), 'decimal')
    
    sueldo_anual = sueldo_propuesto * 12 if sueldo_propuesto else None
    cargo_id = limpiar_valor(row.get('cargo_presupuestario'), 'int')
    descripcion_posicion = limpiar_valor(row.get('desc_cargo'))
    
    sueldo_2 = limpiar_valor(row.get('sueldo2'), 'decimal')
    mes_1 = limpiar_valor(row.get('mes1'), 'int')
    sueldo_3 = limpiar_valor(row.get('sueldo3'), 'decimal')
    mes_2 = limpiar_valor(row.get('mes2'), 'int')
    sueldo_4 = limpiar_valor(row.get('sueldo4'), 'decimal')
    mes_3 = limpiar_valor(row.get('mes3'), 'int')
    mes_4 = limpiar_valor(row.get('mes4'), 'int')

    try:
        cursor.execute("SELECT id FROM nomposicion WHERE nomposicion_id = %s", (nomposicion_id,))
        if cursor.fetchone():
            query = """
                UPDATE nomposicion SET 
                    descripcion_posicion = %s, sueldo_propuesto = %s, sueldo_anual = %s, partida = %s,
                    cargo_id = %s, mes_1 = %s, sueldo_2 = %s, mes_2 = %s, sueldo_3 = %s,
                    mes_3 = %s, sueldo_4 = %s, mes_4 = %s
                WHERE nomposicion_id = %s
            """
            values = (
                descripcion_posicion, sueldo_propuesto, sueldo_anual, partida_presupuestaria, cargo_id,
                mes_1, sueldo_2, mes_2, sueldo_3, mes_3, sueldo_4, mes_4, nomposicion_id
            )
            print(f"  ✓ Posición actualizada: {nomposicion_id} (Partida: {partida_presupuestaria})")
        else:
            query = """
                INSERT INTO nomposicion (
                    nomposicion_id, descripcion_posicion, sueldo_propuesto, sueldo_anual, partida, 
                    cargo_id, mes_1, sueldo_2, mes_2, sueldo_3, mes_3, sueldo_4, mes_4
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                nomposicion_id, descripcion_posicion, sueldo_propuesto, sueldo_anual, partida_presupuestaria,
                cargo_id, mes_1, sueldo_2, mes_2, sueldo_3, mes_3, sueldo_4, mes_4
            )
            print(f"  ✓ Posición CREADA: {nomposicion_id} (Partida: {partida_presupuestaria})")
        
        cursor.execute(query, values)
    except Error as e:
        print(f"  ❌ Error procesando posición {nomposicion_id}: {e}")
        raise

def migrar_estructura(ruta_excel, connection):
    """Función principal que orquesta la migración desde el archivo Excel."""
    print(f"\n🚀 Iniciando migración desde: {ruta_excel}")
    
    try:
        df = pd.read_excel(ruta_excel, dtype=str)
        print(f"📄 Archivo Excel leído. Se encontraron {len(df)} filas.")
    except FileNotFoundError:
        print(f"❌ ERROR: No se encontró el archivo en la ruta: {ruta_excel}")
        return
    except Exception as e:
        print(f"❌ ERROR: No se pudo leer el archivo Excel: {e}")
        return

    cursor = connection.cursor()
    
    try:
        # PRIMER PASO: Migrar las partidas únicas a `cwprecue`
        migrar_partidas_cwprecue(cursor, df)
        connection.commit() # Guardamos este paso
        
        # SEGUNDO PASO: Procesar cada fila para `nomcargos` y `nomposicion`
        print("\n--- Iniciando migración de Cargos y Posiciones ---")
        insertados_actualizados = 0
        errores = 0
        for index, row in df.iterrows():
            print(f"\nProcesando Fila {index + 2} del Excel...")
            try:
                procesar_cargo(cursor, row)
                procesar_posicion(cursor, row)
                connection.commit()
                insertados_actualizados += 1
                print(f"✅ Fila {index + 2} procesada y guardada.")
            except Error:
                print(f"ROLLBACK: Se revirtieron los cambios para la fila {index + 2} debido a un error.")
                connection.rollback()
                errores += 1
                continue
        
        print("\n" + "="*60)
        print("🏁 Migración de Cargos y Posiciones completada.")
        print(f"   - Filas procesadas con éxito: {insertados_actualizados}")
        print(f"   - Filas con errores (revertidas): {errores}")
        print("="*60)

    except Exception as e:
        print(f"❌ ERROR CRÍTICO durante la migración. Revirtiendo todos los cambios. Error: {e}")
        connection.rollback()
    finally:
        cursor.close()

# --- Bloque de Ejecución Principal ---
if __name__ == "__main__":
    db_connection = crear_conexion_db()

    if db_connection:
        ruta_archivo_excel = 'formatos/Estructura-Junio-2025.xlsx'
        migrar_estructura(ruta_archivo_excel, db_connection)
        db_connection.close()
        print("\n🔒 Conexión a la base de datos cerrada.")