const mysql = require('mysql2/promise');
const xlsx = require('xlsx');
const dbConfig = require('./dbconfig');
require('dotenv').config();

(async () => {
  try {
    const connection = await mysql.createConnection(dbConfig);
    console.log("✅ Conexión exitosa a MySQL");

    // Leer el archivo Excel
    const workbookPersonal = xlsx.readFile('Personal_Al_23012025.xlsx');
    const sheetPersonal = workbookPersonal.Sheets[workbookPersonal.SheetNames[0]];

    // Mapeo de nombres de columnas
    const columnMapping = {
      Vicepresidencia: 'VP',
      Departamento: 'Departamento',
      Secciones: 'Seccion',
      Equipo: 'Equipo',
      Grupo: 'Grupo',
    };

    // Convertir el archivo Excel en JSON
    const personalData = xlsx.utils.sheet_to_json(sheetPersonal).map(row => {
      const mappedRow = {};
      for (const [oldKey, newKey] of Object.entries(columnMapping)) {
        mappedRow[newKey] = row[oldKey] || null;
      }
      mappedRow['Cedula'] = row['Cedula'] || null;
      return mappedRow;
    });

    console.log(`📌 Datos transformados: ${personalData.length} registros`);

    const niveles = [
      { nivel: 'VP', table: 'nomnivel1' },
      { nivel: 'Departamento', table: 'nomnivel2' },
      { nivel: 'Seccion', table: 'nomnivel3' },
      { nivel: 'Equipo', table: 'nomnivel4' },
      { nivel: 'Grupo', table: 'nomnivel5' },
    ];

    let actualizados = 0;
    let noActualizados = 0;

    for (const row of personalData) {
      if (!row.Cedula) {
        console.warn(`⚠️ Cédula vacía en el registro:`, row);
        continue;
      }

      console.log(`🔍 Procesando cédula: ${row.Cedula}`);

      const codorgs = {};
      for (const { nivel, table } of niveles) {
        if (!row[nivel]) {
          codorgs[nivel] = null; // Si no tiene valor, poner NULL
          continue;
        }

        const [rowsCheck] = await connection.execute(
          `SELECT codorg FROM ${table} WHERE TRIM(LOWER(descrip)) = TRIM(LOWER(?)) LIMIT 1`,
          [row[nivel]]
        );

        codorgs[nivel] = rowsCheck.length > 0 ? rowsCheck[0].codorg : null;
      }

      // Verificar si la cédula existe en nompersonal
      const [checkExisting] = await connection.execute(
        `SELECT cedula FROM nompersonal WHERE cedula = ?`,
        [row.Cedula]
      );

      if (checkExisting.length === 0) {
        console.warn(`⚠️ La cédula ${row.Cedula} no existe en nompersonal. Saltando...`);
        continue;
      }

      // Construir consulta de actualización
      const updates = [];
      const values = [];
      Object.entries(codorgs).forEach(([nivel, codorg], index) => {
        updates.push(`codnivel${index + 1} = ?`);
        values.push(codorg);
      });

      if (updates.length > 0) {
        values.push(row.Cedula);

        const query = `UPDATE nompersonal SET ${updates.join(', ')} WHERE cedula = ?`;
        const [updateResult] = await connection.execute(query, values);

        if (updateResult.affectedRows > 0) {
          console.log(`✅ Cédula ${row.Cedula} actualizada correctamente.`);
          actualizados++;
        } else {
          console.warn(`⚠️ No se actualizó la cédula ${row.Cedula}.`);
          noActualizados++;
        }
      }
    }

    await connection.end();
    console.log(`✅ Conexión cerrada. Proceso completado.`);
    console.log(`🔹 Total de registros actualizados: ${actualizados}`);
    console.log(`🔹 Total de registros no actualizados: ${noActualizados}`);
  } catch (error) {
    console.error("❌ Error al conectar o migrar datos:", error);
  }
})();
