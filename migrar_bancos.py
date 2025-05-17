import pandas as pd
import mysql.connector
import os
import re

# --- Configuración de la Base de Datos ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'aitsa_rrhh'
}

# --- Configuración del Archivo XLSX ---
XLSX_FILE_PATH = os.path.join('formatos', 'Listado_Empleado_InfoBanco.xlsx')

# Nombres esperados de las columnas en el XLSX
XLSX_COL_IDENTIFICACION = 'IDENTIFICACION'
XLSX_COL_BANCO = 'BANCO'
XLSX_COL_NO_CTA_ACH = 'NO_CTA_ACH'

def normalize_bank_name(name):
    """
    Normaliza agresivamente el nombre del banco para la comparación.
    Ej: "Banco Nacional de Panama S.A." -> "BANCONACIONALPANAMASA"
    """
    if not isinstance(name, str) or not name.strip():
        return ""
    text = name.upper().replace("S.A.", "SA").replace("S. A.", "SA")
    text = re.sub(r'[^A-Z0-9\s]', '', text) # Solo deja letras mayúsculas, números y espacios
    stopwords = ["DE", "DEL", "LA", "LOS", "LAS", "Y", "E", "AND", "THE"]
    words = text.split()
    filtered_words = [word for word in words if word not in stopwords]
    return "".join(filtered_words).strip()

def update_employee_bank_info():
    cnx = None
    cursor = None
    errors = []
    processed_rows = 0
    skipped_rows_missing_data = 0
    bank_not_found_rows = []
    print(f"Iniciando proceso de actualización de información bancaria...")
    try:
        cnx = mysql.connector.connect(**DB_CONFIG)
        cursor = cnx.cursor(buffered=True)
        bank_map = {}
        cursor.execute("SELECT cod_ban, des_ban FROM nombancos")
        for db_cod_ban, db_des_ban in cursor.fetchall():
            if db_des_ban:
                normalized_db_name = normalize_bank_name(db_des_ban)
                if normalized_db_name and normalized_db_name not in bank_map:
                    bank_map[normalized_db_name] = db_cod_ban
        if not bank_map:
            print("No se pudieron cargar bancos desde la base de datos.")
            return
        if not os.path.exists(XLSX_FILE_PATH):
            print(f"Archivo no encontrado: {XLSX_FILE_PATH}")
            return
        df = pd.read_excel(XLSX_FILE_PATH, engine='openpyxl')
        if df.empty:
            print(f"El archivo XLSX '{XLSX_FILE_PATH}' está vacío.")
            return
        required_cols = [XLSX_COL_IDENTIFICACION, XLSX_COL_BANCO, XLSX_COL_NO_CTA_ACH]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"Faltan columnas requeridas: {', '.join(missing_cols)}.")
            return
        batch_updates = []
        for i, row_data in df.iterrows():
            processed_rows += 1
            identificacion_raw = row_data.get(XLSX_COL_IDENTIFICACION)
            excel_banco_name_raw = row_data.get(XLSX_COL_BANCO)
            no_cta_ach_raw = row_data.get(XLSX_COL_NO_CTA_ACH)
            identificacion_val = str(identificacion_raw).strip() if pd.notna(identificacion_raw) else ""
            excel_banco_name_val = str(excel_banco_name_raw).strip() if pd.notna(excel_banco_name_raw) else ""
            no_cta_ach_val = str(no_cta_ach_raw).strip() if pd.notna(no_cta_ach_raw) else ""
            if identificacion_val.endswith(".0"):
                identificacion_val = identificacion_val[:-2]
            if not identificacion_val or not excel_banco_name_val or not no_cta_ach_val:
                skipped_rows_missing_data += 1
                continue
            normalized_excel_banco = normalize_bank_name(excel_banco_name_val)
            cod_banco_db = bank_map.get(normalized_excel_banco)
            if not cod_banco_db:
                bank_not_found_rows.append({
                    'fila': i+2,
                    'identificacion': identificacion_val,
                    'banco_original': excel_banco_name_val,
                    'banco_normalizado': normalized_excel_banco
                })
                continue
            batch_updates.append((cod_banco_db, no_cta_ach_val, identificacion_val))
        success_count = 0
        if batch_updates:
            query = "UPDATE nompersonal SET codbancob = %s, cuentacob = %s WHERE cedula = %s"
            try:
                cursor.executemany(query, batch_updates)
                cnx.commit()
                success_count = cursor.rowcount
                print(f"Actualización batch completada. Filas afectadas: {cursor.rowcount}")
            except mysql.connector.Error as db_err:
                cnx.rollback()
                print(f"Error en actualización batch: {db_err}")
        print_summary(processed_rows, success_count, skipped_rows_missing_data, bank_not_found_rows)
    except mysql.connector.Error as conn_err:
        print(f"Error de conexión o base de datos: {conn_err}")
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if cnx and cnx.is_connected():
            try:
                cnx.close()
            except:
                pass

def print_summary(processed_rows, success_count, skipped_rows, bank_not_found_rows):
    print("\n--- Resumen de Ejecución ---")
    print(f"Total de filas leídas del XLSX: {processed_rows}")
    if skipped_rows > 0:
        print(f"Filas omitidas por falta de datos esenciales: {skipped_rows}")
    print(f"Intentos de actualización batch: {success_count}")
    if bank_not_found_rows:
        print(f"\nBancos NO migrados (no encontrados en la BD): {len(bank_not_found_rows)}")
        for row in bank_not_found_rows:
            print(f"  Fila {row['fila']} | ID: {row['identificacion']} | Banco original: '{row['banco_original']}' | Normalizado: '{row['banco_normalizado']}'")
    print("--- Fin del Resumen ---")

if __name__ == '__main__':
    update_employee_bank_info()
    print("\nProceso de actualización finalizado.")