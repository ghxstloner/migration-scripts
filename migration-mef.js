const mysql = require('mysql2/promise');
const xlsx = require('xlsx');
const dbConfig = require('./dbconfig');

async function main() {
  let connection;
  try {
    console.log("Iniciando proceso...");
    
    connection = await mysql.createConnection({
        ...dbConfig,
        connectTimeout: 60000
    });
    console.log("Conexión establecida");    

    // 1. Borrar tablas si existen
    console.log("Borrando tablas existentes...");
    await connection.execute("DROP TABLE IF EXISTS cargos_mef");
    await connection.execute("DROP TABLE IF EXISTS codigos_cargo_mef");
    await connection.execute("DROP TABLE IF EXISTS posiciones_mef");
    console.log("Tablas borradas");

    // 2. Crear tablas nuevas
    console.log("Creando tablas nuevas...");
    await connection.execute(`
      CREATE TABLE posiciones_mef (
        id INT AUTO_INCREMENT PRIMARY KEY,
        posicionmef VARCHAR(10) UNIQUE NOT NULL
      ) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci
    `);

    await connection.execute(`
      CREATE TABLE codigos_cargo_mef (
        id INT AUTO_INCREMENT PRIMARY KEY,
        codigocargomef VARCHAR(10) UNIQUE NOT NULL
      ) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci
    `);

    await connection.execute(`
      CREATE TABLE cargos_mef (
        id INT AUTO_INCREMENT PRIMARY KEY,
        cargo VARCHAR(255) UNIQUE NOT NULL
      ) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci
    `);
    console.log("Tablas creadas");

    // 3. Leer Excel
    console.log("Leyendo archivo Excel...");
    const workbook = xlsx.readFile('Personal_Al_23012025.xlsx');
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const data = xlsx.utils.sheet_to_json(sheet);
    console.log(`Datos leídos del Excel: ${data.length} registros`);

    // 4. Crear tabla temporal
    console.log("Creando tabla temporal...");
    await connection.execute(`DROP TEMPORARY TABLE IF EXISTS temp_mef`);
    await connection.execute(`
      CREATE TEMPORARY TABLE temp_mef (
        posicionmef VARCHAR(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
        codigocargomef VARCHAR(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
        cargomef VARCHAR(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
        cedula VARCHAR(20) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci
      ) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci
    `);

    // 5. Insertar en temporal
    console.log("Insertando datos en tabla temporal...");
    const insertTemp = "INSERT INTO temp_mef (posicionmef, codigocargomef, cargomef, cedula) VALUES ?";
    const tempValues = data
      .filter(row => row.PosicionMEF && row.CodigoCargoMef && row.CargoMef && row.Cedula)
      .map(row => [
        row.PosicionMEF.toString().trim(),
        row.CodigoCargoMef.toString().trim(),
        row.CargoMef.trim(),
        row.Cedula.toString().trim()
      ]);
    
    await connection.query(insertTemp, [tempValues]);
    console.log(`Insertados ${tempValues.length} registros en temporal`);

    // 6. Insertar datos únicos en las tablas finales
    console.log("Insertando datos únicos en tablas finales...");
    
    await connection.beginTransaction();

    try {
        const [posResult] = await connection.execute(`
          INSERT INTO posiciones_mef (posicionmef)
          SELECT DISTINCT posicionmef FROM temp_mef
        `);
        console.log(`Insertadas ${posResult.affectedRows} posiciones MEF`);
        
        const [codResult] = await connection.execute(`
          INSERT INTO codigos_cargo_mef (codigocargomef)
          SELECT DISTINCT codigocargomef FROM temp_mef
        `);
        console.log(`Insertados ${codResult.affectedRows} códigos de cargo`);
        
        const [carResult] = await connection.execute(`
          INSERT INTO cargos_mef (cargo)
          SELECT DISTINCT cargomef FROM temp_mef
        `);
        console.log(`Insertados ${carResult.affectedRows} cargos`);

        // 7. Verificar datos antes de actualizar
        console.log("Verificando datos antes de actualizar...");
        const [testData] = await connection.execute(`
          SELECT 
            np.cedula,
            t.posicionmef,
            pm.id as id_posicion_mef,
            t.codigocargomef,
            ccm.id as id_codigo_cargo_mef,
            t.cargomef,
            cm.id as id_cargo_mef
          FROM nompersonal np
          INNER JOIN temp_mef t ON np.cedula = t.cedula
          INNER JOIN posiciones_mef pm ON t.posicionmef = pm.posicionmef
          INNER JOIN codigos_cargo_mef ccm ON t.codigocargomef = ccm.codigocargomef
          INNER JOIN cargos_mef cm ON t.cargomef = cm.cargo
          LIMIT 5
        `);
        
        console.log("Muestra de datos a actualizar:", testData);

        // 8. Deshabilitamos temporalmente las foreign keys
        await connection.execute('SET FOREIGN_KEY_CHECKS=0');

        console.log("Actualizando tabla nompersonal...");
        const [updateResult] = await connection.execute(`
            UPDATE nompersonal np
            INNER JOIN temp_mef t ON np.cedula = t.cedula
            INNER JOIN posiciones_mef pm ON t.posicionmef = pm.posicionmef
            INNER JOIN codigos_cargo_mef ccm ON t.codigocargomef = ccm.codigocargomef
            INNER JOIN cargos_mef cm ON t.cargomef = cm.cargo
            SET 
                np.id_posicion_mef = pm.id,
                np.id_codigo_cargo_mef = ccm.id,
                np.id_cargo_mef = cm.id
        `);
        console.log(`Actualizados ${updateResult.affectedRows} registros en nompersonal`);

        // Volvemos a habilitar las foreign keys
        await connection.execute('SET FOREIGN_KEY_CHECKS=1');

        await connection.commit();
        console.log("Transacción completada exitosamente");

    } catch (error) {
        await connection.rollback();
        throw error;
    }

  } catch (error) {
    console.error("Error en la ejecución:", error);
    if (error.sql) {
      console.error("SQL que causó el error:", error.sql);
    }
  } finally {
    if (connection) {
      await connection.end();
      console.log("Conexión cerrada");
    }
    process.exit(0);
  }
}

const timeout = setTimeout(() => {
  console.error("Timeout después de 120 segundos");
  process.exit(1);
}, 120000);

main().then(() => {
  clearTimeout(timeout);
  console.log("Proceso completado exitosamente");
}).catch(err => {
  clearTimeout(timeout);
  console.error("Error fatal:", err);
  process.exit(1);
});