const mysql = require('mysql2/promise');
const xlsx = require('xlsx');

(async () => {
  const dbConfig = {
    host: '172.31.203.5',
    user: 'root',
    password: '4m4x0n14-41ts4',
    database: 'aitsa_rrhh'
  };

  try {
    const connection = await mysql.createConnection(dbConfig);
    console.log("Conexión exitosa a MySQL");

    const workbookEstructura = xlsx.readFile('EstructuraOrganizacional.xlsx');
    const sheetEstructura = workbookEstructura.Sheets[workbookEstructura.SheetNames[0]];
    const estructuraData = xlsx.utils.sheet_to_json(sheetEstructura);
    console.log(`Datos cargados desde el archivo de estructura: ${estructuraData.length} registros`);

    const workbookPersonal = xlsx.readFile('Personal_Al_23012025.xlsx');
    const sheetPersonal = workbookPersonal.Sheets[workbookPersonal.SheetNames[0]];
    const personalData = xlsx.utils.sheet_to_json(sheetPersonal);
    console.log(`Datos cargados desde el archivo de personal: ${personalData.length} registros`);

    const niveles = [
      { nivel: 'VP',           table: 'nomnivel1' },
      { nivel: 'Departamento', table: 'nomnivel2' },
      { nivel: 'Seccion',      table: 'nomnivel3' },
      { nivel: 'Equipo',       table: 'nomnivel4' },
      { nivel: 'Grupo',        table: 'nomnivel5' }
    ];

    const regexPrefijo = /^\d{1,3}(?:-\d{1,2})*\s+/;

    for (const { nivel, table } of niveles) {
      const uniqueValues = Array.from(
        new Set(estructuraData.map(row => row[nivel]).filter(Boolean))
      );

      for (const [index, value] of uniqueValues.entries()) {
        if (table === 'nomnivel5') {
          await connection.execute(
            `INSERT IGNORE INTO ${table} (codorg, descrip)
             VALUES (?, ?)`,
            [index + 1, value]
          );
        } else {
          const descCorta = value.replace(regexPrefijo, '').trim();
          await connection.execute(
            `INSERT IGNORE INTO ${table} (codorg, descrip, descripcion_corta)
             VALUES (?, ?, ?)`,
            [index + 1, value, descCorta]
          );
        }
      }
      console.log(`Migración completada para '${table}' con ${uniqueValues.length} registros.`);
    }

    await connection.execute(`
      CREATE TEMPORARY TABLE temp_personal (
        cedula VARCHAR(50),
        VP VARCHAR(255),
        Departamento VARCHAR(255),
        Seccion VARCHAR(255),
        Equipo VARCHAR(255),
        Grupo VARCHAR(255)
      )
    `);
    console.log("Tabla temporal 'temp_personal' creada.");

    const tempPersonalData = personalData
      .map(row => [
        row.Cedula || null,
        row.VP || null,
        row.Departamento || null,
        row.Seccion || null,
        row.Equipo || null,
        row.Grupo || null
      ])
      .filter(row => row[0]); // Excluye filas sin Cédula

    if (tempPersonalData.length > 0) {
      const batchSize = 1000;
      for (let i = 0; i < tempPersonalData.length; i += batchSize) {
        const batch = tempPersonalData.slice(i, i + batchSize);
        await connection.query(
          `INSERT INTO temp_personal (cedula, VP, Departamento, Seccion, Equipo, Grupo)
           VALUES ?`,
          [batch]
        );
        console.log(`Insertado un lote de ${batch.length} registros en 'temp_personal'.`);
      }
    }

    for (let i = 0; i < niveles.length; i++) {
      const { nivel, table } = niveles[i];
      const codnivel = `codnivel${i + 1}`;

      const query = `
        UPDATE temp_personal tp
        JOIN ${table} nn 
          ON tp.${nivel} = nn.descrip COLLATE utf8mb3_general_ci
        JOIN nompersonal np 
          ON tp.cedula = np.cedula
        SET np.${codnivel} = nn.codorg
      `;
      const [result] = await connection.execute(query);
      console.log(`Registros actualizados en 'nompersonal' para '${nivel}': ${result.affectedRows}`);
    }

    await connection.end();
    console.log("Conexión cerrada. Proceso completado con éxito.");
  } catch (error) {
    console.error("Error al conectar o migrar datos:", error);
  }
})();
