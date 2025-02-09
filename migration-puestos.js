const mysql = require('mysql2/promise');
const xlsx = require('xlsx');
const dbConfig = require('./dbconfig');
require('dotenv').config();

(async () => {
  try {
    // Conectar a la base de datos
    const connection = await mysql.createConnection(dbConfig);
    console.log("Conexión exitosa a MySQL");

    // Leer el archivo Excel
    const workbook = xlsx.readFile('Personal_Al_23012025.xlsx');
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];

    // Convertir la hoja a JSON
    const data = xlsx.utils.sheet_to_json(sheet);
    console.log(`Datos cargados desde el Excel: ${data.length} registros`);

    // Limpiar la tabla 'nombancos'
    await connection.execute("DELETE FROM nombancos");
    console.log("Todos los registros de 'nombancos' han sido eliminados.");

    // Insertar bancos únicos en 'nombancos'
    const insertBancosQuery = "INSERT INTO nombancos (cod_ban, des_ban) VALUES (?, ?)";
    let codBan = 1;
    const bancosInsertados = new Set();
    for (const row of data) {
      const banco = row.Banco !== undefined ? String(row.Banco).trim() : null;
      if (banco && !bancosInsertados.has(banco)) {
        await connection.execute(insertBancosQuery, [codBan, banco]);
        bancosInsertados.add(banco);
        codBan++;
      }
    }
    console.log("Migración completada: Los bancos únicos se han insertado en 'nombancos'.");

    // Crear una tabla temporal para almacenar los datos del Excel
    await connection.execute(`
      CREATE TEMPORARY TABLE temp_personal (
        cedula VARCHAR(50),
        banco VARCHAR(255)
      )
    `);
    console.log("Tabla temporal 'temp_personal' creada.");

    // Insertar los datos en la tabla temporal en lotes
    const insertTempQuery = "INSERT INTO temp_personal (cedula, banco) VALUES ?";
    const tempData = data
      .map(row => [
        row.Cedula || null,
        row.Banco !== undefined ? String(row.Banco).trim() : null
      ])
      .filter(row => row[0] && row[1]);

    if (tempData.length > 0) {
      const batchSize = 1000;
      for (let i = 0; i < tempData.length; i += batchSize) {
        const batch = tempData.slice(i, i + batchSize);
        await connection.query(insertTempQuery, [batch]);
        console.log(`Insertado un lote de ${batch.length} registros en 'temp_personal'.`);
      }
    }

    const updateQuery = `
        UPDATE nompersonal np
        JOIN temp_personal tp ON np.cedula = tp.cedula
        JOIN nombancos nb ON tp.banco = nb.des_ban
        SET np.codbancob = nb.cod_ban
    `;
    
    const [updateResult] = await connection.execute(updateQuery);
    console.log(`Registros actualizados en 'nompersonal': ${updateResult.affectedRows}`);

    // Cerrar la conexión
    await connection.end();
    console.log("Conexión cerrada. Proceso completado con éxito.");
  } catch (error) {
    console.error("Error al conectar o migrar datos:", error);
  }
})();
