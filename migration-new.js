const mysql = require('mysql2/promise');
const xlsx = require('xlsx');
const dbConfig = require('./dbconfig');
require('dotenv').config();

(async () => {
  try {
    const connection = await mysql.createConnection(dbConfig);
    console.log("Conexión exitosa a MySQL");

    // Leer el archivo Excel
    const workbook = xlsx.readFile('Personal_Al_23012025.xlsx');
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];
    const data = xlsx.utils.sheet_to_json(sheet);
    console.log(`Datos cargados desde el Excel: ${data.length} registros`);

    // Función para procesar centro_costos
    const procesarCentroCostos = async () => {
      console.log("\nProcesando Centro de Costos...");

      // Obtener valores únicos del Excel
      const centrosCosto = new Map();
      for (const row of data) {
        if (row.CentroCostos && row.Descripcion) {
          centrosCosto.set(row.CentroCostos, row.Descripcion);
        }
      }

      // Insertar o actualizar registros
      for (const [codigo, descripcion] of centrosCosto) {
        const [existing] = await connection.execute(
          'SELECT cod_cos FROM centro_costos WHERE cod_cos = ?',
          [codigo]
        );

        if (existing.length === 0) {
          await connection.execute(
            'INSERT INTO centro_costos (cod_cos, des_scos) VALUES (?, ?)',
            [codigo, descripcion]
          );
          console.log(`Insertado centro de costo: ${codigo}`);
        } else {
          await connection.execute(
            'UPDATE centro_costos SET des_scos = ? WHERE cod_cos = ?',
            [descripcion, codigo]
          );
          console.log(`Actualizado centro de costo: ${codigo}`);
        }
      }

      // Crear tabla temporal
      await connection.execute(`
        CREATE TEMPORARY TABLE IF NOT EXISTS temp_centro_costos (
          cedula VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
          cod_cos VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        )
      `);

      // Insertar en tabla temporal
      const insertTempQuery = "INSERT INTO temp_centro_costos (cedula, cod_cos) VALUES ?";
      const tempData = data
        .map(row => [
          row.Cedula || null,
          row.CentroCostos || null
        ])
        .filter(row => row[0] && row[1]);

      if (tempData.length > 0) {
        const batchSize = 1000;
        for (let i = 0; i < tempData.length; i += batchSize) {
          const batch = tempData.slice(i, i + batchSize);
          await connection.query(insertTempQuery, [batch]);
        }
      }

      // Actualizar nompersonal
      const [updateResult] = await connection.execute(`
        UPDATE nompersonal np
        JOIN temp_centro_costos tp ON np.cedula = tp.cedula
        SET np.cod_cos = tp.cod_cos
        WHERE tp.cod_cos IS NOT NULL
      `);

      console.log(`Registros actualizados en nompersonal: ${updateResult.affectedRows}`);
    };

    const procesarTabla = async (tableName, sourceField, codeField, descField, useAutoIncrement = true) => {
        console.log(`\nProcesando tabla ${tableName}...`);
        
        // Limpiar la tabla
        await connection.execute(`DELETE FROM ${tableName}`);
        console.log(`Tabla ${tableName} limpiada.`);
    
        // Insertar valores únicos
        let codigo = 1;
        const valoresInsertados = new Set();
        
        for (const row of data) {
            const valor = row[sourceField] !== undefined ? String(row[sourceField]).trim() : null;
            if (valor && !valoresInsertados.has(valor)) {
                const codPrefijo = tableName === 'aeropuertos' ? 'AER' : tableName.charAt(0).toUpperCase();
                const codFormatted = `${codPrefijo}${String(codigo).padStart(3, '0')}`;
                
                // Siempre usar la versión con código y descripción
                await connection.execute(
                    `INSERT INTO ${tableName} (${codeField}, ${descField}) VALUES (?, ?)`,
                    [codFormatted, valor]
                );
                
                valoresInsertados.add(valor);
                codigo++;
            }
        }
        console.log(`Valores únicos insertados en ${tableName}.`);
    
        // El resto del código sigue igual...
        // Crear tabla temporal
        await connection.execute(`
            CREATE TEMPORARY TABLE temp_${tableName} (
                cedula VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                valor VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            )
        `);
    
        // Insertar datos en tabla temporal
        const insertTempQuery = `INSERT INTO temp_${tableName} (cedula, valor) VALUES ?`;
        const tempData = data
            .map(row => [
                row.Cedula || null,
                row[sourceField] !== undefined ? String(row[sourceField]).trim() : null
            ])
            .filter(row => row[0] && row[1]);
    
        if (tempData.length > 0) {
            const batchSize = 1000;
            for (let i = 0; i < tempData.length; i += batchSize) {
                const batch = tempData.slice(i, i + batchSize);
                await connection.query(insertTempQuery, [batch]);
            }
        }
    
        // Actualizar nompersonal
        const updateField = tableName === 'centro_costos' ? 'cod_cos' :
                           tableName === 'aeropuertos' ? 'cod_aer' :
                           tableName === 'dias_periodo' ? 'cod_dia' :
                           tableName === 'tipos_periodo' ? 'cod_tip' :
                           tableName === 'jornadas' ? 'cod_jor' :
                           tableName === 'tipos_sueldo' ? 'cod_sue' :
                           'cod_sin';
    
        const updateQuery = tableName === 'centro_costos' ?
            `UPDATE nompersonal np
             JOIN temp_${tableName} tp ON np.cedula = tp.cedula
             JOIN ${tableName} t ON tp.valor = t.cod_cos
             SET np.${updateField} = t.cod_cos` :
            `UPDATE nompersonal np
             JOIN temp_${tableName} tp ON np.cedula = tp.cedula
             JOIN ${tableName} t ON tp.valor = t.${descField}
             SET np.${updateField} = t.${codeField}`;
    
        const [updateResult] = await connection.execute(updateQuery);
        console.log(`Registros actualizados en nompersonal: ${updateResult.affectedRows}`);
    };

    // Procesar las tablas
    //await procesarCentroCostos();

    await procesarTabla('aeropuertos', 'Aeropuerto', 'codigo', 'descripcion');
    await procesarTabla('dias_periodo', 'DiasPeriodo', 'cod_dia', 'des_dia', false);
    await procesarTabla('tipos_periodo', 'PeriodoTipo', 'cod_tip', 'des_tip', false);
    await procesarTabla('jornadas', 'Jornada', 'cod_jor', 'des_jor', false);
    await procesarTabla('tipos_sueldo', 'TipoSueldo', 'cod_sue', 'des_sue', false);
    await procesarTabla('sindicatos', 'Sindicato', 'cod_sin', 'des_sin', false);

    await connection.end();
    console.log("\nProceso completado con éxito.");
  } catch (error) {
    console.error("Error durante la migración:", error);
  }
})();