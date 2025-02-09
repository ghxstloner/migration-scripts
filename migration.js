const mysql = require('mysql2/promise');
const xlsx = require('xlsx');
const dbConfig = require('./dbconfig');
const cliProgress = require('cli-progress');
require('dotenv').config();

async function migrarBancos(connection, data) {
    console.log("\n=== Migrando Bancos ===");
    await connection.execute("DELETE FROM nombancos");
    
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

    await connection.execute(`
        CREATE TEMPORARY TABLE temp_bancos (
            cedula VARCHAR(50),
            banco VARCHAR(191)
        )
    `);

    const insertTempQuery = "INSERT INTO temp_bancos (cedula, banco) VALUES ?";
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
        }
    }

    const [updateResult] = await connection.execute(`
        UPDATE nompersonal np
        JOIN temp_bancos tp ON np.cedula = tp.cedula
        JOIN nombancos nb ON tp.banco = nb.des_ban
        SET np.codbancob = nb.cod_ban
    `);

    console.log(`Bancos actualizados: ${updateResult.affectedRows}`);
}

async function migrarMEF(connection, data) {
    console.log("\n=== Migrando MEF ===");
    
    await connection.execute('SET FOREIGN_KEY_CHECKS=0');

    await connection.execute("DROP TABLE IF EXISTS cargos_mef");
    await connection.execute("DROP TABLE IF EXISTS codigos_cargo_mef");
    await connection.execute("DROP TABLE IF EXISTS posiciones_mef");

    await connection.execute(`
        CREATE TABLE posiciones_mef (
            id INT AUTO_INCREMENT PRIMARY KEY,
            posicionmef VARCHAR(10) UNIQUE NOT NULL
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci
    `);

    await connection.execute(`
        CREATE TABLE codigos_cargo_mef (
            id INT AUTO_INCREMENT PRIMARY KEY,
            codigocargomef VARCHAR(10) UNIQUE NOT NULL
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci
    `);

    await connection.execute(`
        CREATE TABLE cargos_mef (
            id INT AUTO_INCREMENT PRIMARY KEY,
            cargo VARCHAR(191) UNIQUE NOT NULL
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci
    `);

    await connection.execute(`DROP TEMPORARY TABLE IF EXISTS temp_mef`);
    await connection.execute(`
        CREATE TEMPORARY TABLE temp_mef (
            posicionmef VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
            codigocargomef VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
            cargomef VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
            cedula VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci
    `);

    const insertTemp = "INSERT INTO temp_mef (posicionmef, codigocargomef, cargomef, cedula) VALUES ?";
    const tempValues = data
        .filter(row => row.PosicionMEF && row.CodigoCargoMef && row.CargoMef && row.Cedula)
        .map(row => [
            row.PosicionMEF.toString().trim(),
            row.CodigoCargoMef.toString().trim(),
            row.CargoMef.trim(),
            row.Cedula.toString().trim()
        ]);
    
    if (tempValues.length > 0) {
        await connection.query(insertTemp, [tempValues]);
    }

    await connection.beginTransaction();
    try {
        const [posResult] = await connection.execute(`
            INSERT INTO posiciones_mef (posicionmef)
            SELECT DISTINCT posicionmef FROM temp_mef
        `);
        
        const [codResult] = await connection.execute(`
            INSERT INTO codigos_cargo_mef (codigocargomef)
            SELECT DISTINCT codigocargomef FROM temp_mef
        `);
        
        const [carResult] = await connection.execute(`
            INSERT INTO cargos_mef (cargo)
            SELECT DISTINCT cargomef FROM temp_mef
        `);

        await connection.execute('SET FOREIGN_KEY_CHECKS=0');

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

        await connection.execute('SET FOREIGN_KEY_CHECKS=1');
        await connection.commit();

        console.log(`MEF actualizado - Posiciones: ${posResult.affectedRows}, Códigos: ${codResult.affectedRows}, Cargos: ${carResult.affectedRows}, Personal: ${updateResult.affectedRows}`);
    } catch (error) {
        await connection.rollback();
        throw error;
    }
}

async function migrarDeloitte(connection, data) {
    console.log("\n=== Migrando Deloitte ===");
    await connection.execute('SET FOREIGN_KEY_CHECKS=0');
    try {
      await connection.execute("DELETE FROM cargodeloitte");
      await connection.execute("DELETE FROM nivelcargo");
      await connection.execute("DELETE FROM rolcargo");

      const insertCargoQuery = "INSERT INTO cargodeloitte (id_cargo, nombre_cargo) VALUES (?, ?)";
      const insertNivelQuery = "INSERT INTO nivelcargo (id_nivel, nombre_nivel) VALUES (?, ?)";
      const insertRolQuery = "INSERT INTO rolcargo (id_rol, nombre_rol) VALUES (?, ?)";

      const cargosSet = new Set();
      const nivelesSet = new Set();
      const rolesSet = new Set();

      let cargoId = 1, nivelId = 1, rolId = 1;

      for (const row of data) {
          const cargo = row.CargoDeloitte?.trim();
          const nivel = row.NivelCargo?.trim();
          const rol = row.RolCargo?.trim();

          if (cargo && !cargosSet.has(cargo)) {
              await connection.execute(insertCargoQuery, [cargoId, cargo]);
              cargosSet.add(cargo);
              cargoId++;
          }

          if (nivel && !nivelesSet.has(nivel)) {
              await connection.execute(insertNivelQuery, [nivelId, nivel]);
              nivelesSet.add(nivel);
              nivelId++;
          }

          if (rol && !rolesSet.has(rol)) {
              await connection.execute(insertRolQuery, [rolId, rol]);
              rolesSet.add(rol);
              rolId++;
          }
      }

      await connection.execute(`DROP TEMPORARY TABLE IF EXISTS temp_cargos`);
      await connection.execute(`
          CREATE TEMPORARY TABLE temp_cargos (
              cedula VARCHAR(50),
              cargo VARCHAR(191),
              nivel VARCHAR(191),
              rol VARCHAR(191)
          )
      `);

      const insertTempQuery = "INSERT INTO temp_cargos (cedula, cargo, nivel, rol) VALUES ?";
      const tempData = data
          .map(row => [
              row.Cedula || null,
              row.CargoDeloitte?.trim() || null,
              row.NivelCargo?.trim() || null,
              row.RolCargo?.trim() || null
          ])
          .filter(row => row[0] && (row[1] || row[2] || row[3]));

      if (tempData.length > 0) {
          const batchSize = 1000;
          for (let i = 0; i < tempData.length; i += batchSize) {
              const batch = tempData.slice(i, i + batchSize);
              await connection.query(insertTempQuery, [batch]);
          }
      }

      const [updateResult] = await connection.execute(`
          INSERT INTO cargoempleado (id_empleado, id_cargo, id_nivel, id_rol, fecha_inicio)
          SELECT DISTINCT
              np.personal_id,
              CASE WHEN cd.id_cargo IS NULL THEN NULL ELSE cd.id_cargo END,
              CASE WHEN nc.id_nivel IS NULL THEN NULL ELSE nc.id_nivel END,
              CASE WHEN rc.id_rol IS NULL THEN NULL ELSE rc.id_rol END,
              CURRENT_DATE
          FROM nompersonal np
          INNER JOIN temp_cargos tc ON TRIM(np.cedula) = TRIM(tc.cedula)
          LEFT JOIN cargodeloitte cd ON TRIM(tc.cargo) = TRIM(cd.nombre_cargo)
          LEFT JOIN nivelcargo nc ON TRIM(tc.nivel) = TRIM(nc.nombre_nivel)
          LEFT JOIN rolcargo rc ON TRIM(tc.rol) = TRIM(rc.nombre_rol)
          WHERE np.personal_id IS NOT NULL
          AND (tc.cargo IS NOT NULL OR tc.nivel IS NOT NULL OR tc.rol IS NOT NULL)
      `);

      } catch (error) {
        // En caso de error, asegúrate de reactivar la validación
        await connection.execute('SET FOREIGN_KEY_CHECKS=1');
        await connection.rollback();
        throw error;
    } finally {
        // Por seguridad, asegúrate de que siempre se reactive la validación
        await connection.execute('SET FOREIGN_KEY_CHECKS=1');
    }
  }

async function migrarNiveles(connection, data) {
    console.log("\n=== Migrando Niveles ===");
    
    const niveles = [
        { nivel: 'VP', table: 'nomnivel1' },
        { nivel: 'Departamento', table: 'nomnivel2' },
        { nivel: 'Seccion', table: 'nomnivel3' },
        { nivel: 'Equipo', table: 'nomnivel4' },
        { nivel: 'Grupo', table: 'nomnivel5' }
    ];

    let actualizados = 0;
    let noActualizados = 0;

    for (const row of data) {
        if (!row.Cedula) continue;

        const codorgs = {};
        for (const { nivel, table } of niveles) {
            if (!row[nivel]) {
                codorgs[nivel] = null;
                continue;
            }

            const [rowsCheck] = await connection.execute(
                `SELECT codorg FROM ${table} WHERE TRIM(LOWER(descrip)) = TRIM(LOWER(?)) LIMIT 1`,
                [row[nivel]]
            );

            codorgs[nivel] = rowsCheck.length > 0 ? rowsCheck[0].codorg : null;
        }

        const [checkExisting] = await connection.execute(
            `SELECT cedula FROM nompersonal WHERE cedula = ?`,
            [row.Cedula]
        );

        if (checkExisting.length === 0) continue;

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
                actualizados++;
            } else {
                noActualizados++;
            }
        }
    }

    console.log(`Niveles actualizados: ${actualizados}, No actualizados: ${noActualizados}`);
}

async function migrarCentroCostos(connection, data) {
  console.log("\n=== Migrando Centro de Costos ===");

  const centrosCosto = new Map();
  for (const row of data) {
      if ((row.CentroCostos !== undefined && row.CentroCostos !== null && row.CentroCostos !== '') && 
          row.Descripcion) {
          centrosCosto.set(String(row.CentroCostos), row.Descripcion);
      }
  }

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
      } else {
          await connection.execute(
              'UPDATE centro_costos SET des_scos = ? WHERE cod_cos = ?',
              [descripcion, codigo]
          );
      }
  }

  await connection.execute(`
      CREATE TEMPORARY TABLE IF NOT EXISTS temp_centro_costos (
          cedula VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
          cod_cos VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
      )
  `);

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

  const [updateResult] = await connection.execute(`
      UPDATE nompersonal np
      JOIN temp_centro_costos tp ON np.cedula = tp.cedula
      SET np.cod_cos = tp.cod_cos
      WHERE tp.cod_cos IS NOT NULL
  `);

  console.log(`Centro de Costos actualizados: ${updateResult.affectedRows}`);
}

async function migrarTablasGenerales(connection, data) {
  console.log("\n=== Migrando Tablas Generales ===");

  const tablas = [
      { nombre: 'aeropuertos', campo: 'Aeropuerto', codigo: 'codigo', descripcion: 'descripcion', prefijo: 'AER' },
      { nombre: 'dias_periodo', campo: 'DiasPeriodo', codigo: 'cod_dia', descripcion: 'des_dia' },
      { nombre: 'tipos_periodo', campo: 'PeriodoTipo', codigo: 'cod_tip', descripcion: 'des_tip' },
      { nombre: 'jornadas', campo: 'Jornada', codigo: 'cod_jor', descripcion: 'des_jor' },
      { nombre: 'tipos_sueldo', campo: 'TipoSueldo', codigo: 'cod_sue', descripcion: 'des_sue' },
      { nombre: 'sindicatos', campo: 'Sindicato', codigo: 'cod_sin', descripcion: 'des_sin' }
  ];
  
  try {
    // Desactivar foreign keys al inicio
    await connection.execute('SET FOREIGN_KEY_CHECKS=0');

    for (const tabla of tablas) {
        console.log(`\nProcesando ${tabla.nombre}...`);
      
        await connection.execute(`DELETE FROM ${tabla.nombre}`);
        
        let codigo = 1;
        const valoresInsertados = new Set();
        
        for (const row of data) {
            const valor = row[tabla.campo] !== undefined ? String(row[tabla.campo]).trim() : null;
            if (valor && !valoresInsertados.has(valor)) {
                const codPrefijo = tabla.prefijo || tabla.nombre.charAt(0).toUpperCase();
                const codFormatted = `${codPrefijo}${String(codigo).padStart(3, '0')}`;
                
                await connection.execute(
                    `INSERT INTO ${tabla.nombre} (${tabla.codigo}, ${tabla.descripcion}) VALUES (?, ?)`,
                    [codFormatted, valor]
                );
                
                valoresInsertados.add(valor);
                codigo++;
            }
        }

        await connection.execute(`
            CREATE TEMPORARY TABLE temp_${tabla.nombre} (
                cedula VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                valor VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            )
        `);

        const insertTempQuery = `INSERT INTO temp_${tabla.nombre} (cedula, valor) VALUES ?`;
        const tempData = data
            .map(row => [
                row.Cedula || null,
                row[tabla.campo] !== undefined ? String(row[tabla.campo]).trim() : null
            ])
            .filter(row => row[0] && row[1]);

        if (tempData.length > 0) {
            const batchSize = 1000;
            for (let i = 0; i < tempData.length; i += batchSize) {
                const batch = tempData.slice(i, i + batchSize);
                await connection.query(insertTempQuery, [batch]);
            }
        }

        const campoUpdate = tabla.nombre === 'aeropuertos' ? 'cod_aer' :
                          tabla.nombre === 'dias_periodo' ? 'cod_dia' :
                          tabla.nombre === 'tipos_periodo' ? 'cod_tip' :
                          tabla.nombre === 'jornadas' ? 'cod_jor' :
                          tabla.nombre === 'tipos_sueldo' ? 'cod_sue' :
                          'cod_sin';

        const [updateResult] = await connection.execute(`
            UPDATE nompersonal np
            JOIN temp_${tabla.nombre} tp ON np.cedula = tp.cedula
            JOIN ${tabla.nombre} t ON tp.valor = t.${tabla.descripcion}
            SET np.${campoUpdate} = t.${tabla.codigo}
        `);

        console.log(`${tabla.nombre} actualizados: ${updateResult.affectedRows}`);
      }
  } catch (error) {
      throw error;
  } finally {
      // Siempre reactivar las foreign keys al final
      await connection.execute('SET FOREIGN_KEY_CHECKS=1');
  }
}

async function migrarPuestos(connection, personalData) {
  console.log("\n=== Migrando Puestos ===");
  
  // Deshabilitar foreign key checks
  await connection.execute('SET FOREIGN_KEY_CHECKS=0');
  
  try {
      // Verificar si existe la tabla puesto_aitsa
      const [tables] = await connection.execute(
          "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'puesto_aitsa'"
      );
      
      // Si la tabla no existe, crearla
      if (tables[0].count === 0) {
          await connection.execute(`
              CREATE TABLE puesto_aitsa (
                  id INT PRIMARY KEY AUTO_INCREMENT,
                  puesto VARCHAR(191) UNIQUE
              )
          `);
      }
      
      // Verificar si existe la columna id_puesto
      const checkColumn = await connection.execute(`
          SELECT COUNT(*) as exists_count 
          FROM information_schema.COLUMNS 
          WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'nompersonal' 
          AND COLUMN_NAME = 'id_puesto'
      `);

      const columnExists = checkColumn[0][0].exists_count > 0;

      if (!columnExists) {
          await connection.execute(`
              ALTER TABLE nompersonal
              ADD COLUMN id_puesto INT
          `);
      }

      // Leer el archivo de puestos
      const workbookPuestos = xlsx.readFile('Puestos_Trabajo.xlsx');
      const sheetPuestos = workbookPuestos.Sheets[workbookPuestos.SheetNames[0]];
      const puestosData = xlsx.utils.sheet_to_json(sheetPuestos);
      
      // Insertar puestos únicos
      const insertPuestoQuery = "INSERT IGNORE INTO puesto_aitsa (puesto) VALUES (?)";
      const puestosInsertados = new Set();
      
      for (const row of puestosData) {
          const puesto = row.Puesto !== undefined ? String(row.Puesto).trim() : null;
          if (puesto && !puestosInsertados.has(puesto)) {
              await connection.execute(insertPuestoQuery, [puesto]);
              puestosInsertados.add(puesto);
          }
      }
      
      // Crear tabla temporal para la actualización
      await connection.execute(`
          CREATE TEMPORARY TABLE temp_puestos (
              cedula VARCHAR(50),
              puesto VARCHAR(191)
          )
      `);
      
      // Insertar datos en la tabla temporal
      const insertTempQuery = "INSERT INTO temp_puestos (cedula, puesto) VALUES ?";
      const tempData = personalData
          .map(row => [
              row.Cedula || null,
              row.Puesto !== undefined ? String(row.Puesto).trim() : null
          ])
          .filter(row => row[0] && row[1]);
      
      // Insertar por lotes para mejor rendimiento
      if (tempData.length > 0) {
          const batchSize = 1000;
          for (let i = 0; i < tempData.length; i += batchSize) {
              const batch = tempData.slice(i, i + batchSize);
              await connection.query(insertTempQuery, [batch]);
          }
      }
      
      // Actualizar nompersonal con los IDs de los puestos
      const [updateResult] = await connection.execute(`
          UPDATE nompersonal np
          JOIN temp_puestos tp ON np.cedula = tp.cedula
          JOIN puesto_aitsa pa ON tp.puesto = pa.puesto
          SET np.id_puesto = pa.id
      `);

      // Intentar agregar la foreign key
      if (!columnExists) {
          await connection.execute(`
              ALTER TABLE nompersonal
              ADD CONSTRAINT fk_nompersonal_puesto
              FOREIGN KEY (id_puesto) REFERENCES puesto_aitsa(id)
          `);
      }
      
      console.log(`Puestos actualizados: ${updateResult.affectedRows}`);

  } catch (error) {
      console.error("Error durante la migración de puestos:", error);
      throw error;
  } finally {
      // Volver a habilitar foreign key checks
      await connection.execute('SET FOREIGN_KEY_CHECKS=1');
  }
}

async function migrarInformacionGeneral(connection, personalData) {
  console.log("\n=== Migrando Información General ===");

  // Crear tabla temporal para la actualización
  await connection.execute(`
      CREATE TEMPORARY TABLE temp_info_general (
          cedula VARCHAR(50),
          telefonos VARCHAR(100),
          telefono_celular VARCHAR(100),
          email VARCHAR(100),
          dv VARCHAR(10),
          fecnac DATE,
          fecha_resolucion_baja DATE,
          numero_carnet VARCHAR(50),
          codigo_carnet VARCHAR(50),
          cuenta_pago VARCHAR(50),
          isr_fijo_periodo DECIMAL(10,2),
          suesal DECIMAL(10,2),
          salario_diario DECIMAL(10,2),
          rata_x_hr DECIMAL(10,2),
          gastos_representacion DECIMAL(10,2),
          gasto_rep_diario DECIMAL(10,2),
          rata_hora_gasto_rep DECIMAL(10,2)
      )
  `);

  // Preparar los datos para inserción
  const tempData = personalData
      .map(row => {
          // Convertir fechas a formato MySQL
          const fechaNac = row.FechaNacimiento ? formatDate(row.FechaNacimiento) : null;
          const fechaBaja = row.FechaBaja ? formatDate(row.FechaBaja) : null;

          // Convertir valores numéricos, asegurando que sean números válidos
          const ISRFijo = parseFloat(row.ISRFijoPeriodo) || 0;
          const sueldoMensual = parseFloat(row.SueldoMensual) || 0;
          const sueldoDiario = parseFloat(row.SueldoDiario) || 0;
          const rataHora = parseFloat(row.RataHora) || 0;
          const gastosRep = parseFloat(row.GR) || 0;
          const gastoRepDiario = parseFloat(row.GastoRepresentacionDiario) || 0;
          const rataHoraGR = parseFloat(row.RataHoraGR) || 0;

          return [
              row.Cedula || null,
              row.Telefono || null,
              row.Telefono || null, // Teléfono celular
              row.eMail || null,
              row.DV || null,
              fechaNac,
              fechaBaja,
              row.Personal || null, // Número de carnet
              row.Personal || null, // Código de carnet
              row.CtaDinero || null,
              ISRFijo,
              sueldoMensual,
              sueldoDiario,
              rataHora,
              gastosRep,
              gastoRepDiario,
              rataHoraGR
          ];
      })
      .filter(row => row[0]); // Filtrar solo registros con cédula

  // Función auxiliar para formatear fechas
  function formatDate(date) {
      if (!date) return null;
      
      // Si es un número de Excel, convertirlo a fecha
      if (typeof date === 'number') {
          const excelDate = new Date((date - 25569) * 86400 * 1000);
          return excelDate.toISOString().split('T')[0];
      }
      
      // Si ya es una fecha en string, asegurarse que esté en formato YYYY-MM-DD
      try {
          const parts = date.split('/');
          if (parts.length === 3) {
              // Asumiendo formato DD/MM/YYYY
              return `${parts[2]}-${parts[1].padStart(2, '0')}-${parts[0].padStart(2, '0')}`;
          }
      } catch (e) {
          console.log(`Error al procesar fecha: ${date}`);
          return null;
      }
      
      return null;
  }

  // Insertar datos en lotes
  if (tempData.length > 0) {
      const batchSize = 1000;
      const insertTempQuery = `
          INSERT INTO temp_info_general (
              cedula, telefonos, telefono_celular, email, dv, fecnac,
              fecha_resolucion_baja, numero_carnet, codigo_carnet, cuenta_pago,
              isr_fijo_periodo, suesal, salario_diario, rata_x_hr,
              gastos_representacion, gasto_rep_diario, rata_hora_gasto_rep
          ) VALUES ?
      `;

      for (let i = 0; i < tempData.length; i += batchSize) {
          const batch = tempData.slice(i, i + batchSize);
          await connection.query(insertTempQuery, [batch]);
      }
  }

  // Actualizar la tabla nompersonal
  const [updateResult] = await connection.execute(`
      UPDATE nompersonal np
      JOIN temp_info_general tig ON np.cedula = tig.cedula
      SET 
          np.telefonos = tig.telefonos,
          np.TelefonoCelular = tig.telefono_celular,
          np.email = tig.email,
          np.dv = tig.dv,
          np.fecnac = tig.fecnac,
          np.fecha_resolucion_baja = tig.fecha_resolucion_baja,
          np.numero_carnet = tig.numero_carnet,
          np.codigo_carnet = tig.codigo_carnet,
          np.cuenta_pago = tig.cuenta_pago,
          np.ISRFijoPeriodo = tig.isr_fijo_periodo,
          np.suesal = tig.suesal,
          np.salario_diario = tig.salario_diario,
          np.rata_x_hr = tig.rata_x_hr,
          np.gastos_representacion = tig.gastos_representacion,
          np.gasto_rep_diario = tig.gasto_rep_diario,
          np.rata_hora_gasto_rep = tig.rata_hora_gasto_rep
  `);

  console.log(`Registros actualizados: ${updateResult.affectedRows}`);
}

async function main() {
  let connection;
  try {
      connection = await mysql.createConnection({
          ...dbConfig,
          connectTimeout: 60000
      });
      console.log("Conexión establecida");

      const workbook = xlsx.readFile('Personal_Al_23012025.xlsx');
      const sheet = workbook.Sheets[workbook.SheetNames[0]];
      const data = xlsx.utils.sheet_to_json(sheet);
      console.log(`Datos cargados: ${data.length} registros`);

      // Crear barra de progreso
      const progressBar = new cliProgress.SingleBar({
          format: 'Progreso |{bar}| {percentage}% || {value}/{total} Módulos || {currentTask}',
          barCompleteChar: '\u2588',
          barIncompleteChar: '\u2591',
          hideCursor: true
      });

      // Total de tareas a realizar
      const totalTasks = 8;
      progressBar.start(totalTasks, 0, { currentTask: 'Iniciando...' });

      // Función auxiliar para actualizar progreso
      const updateProgress = (taskNumber, taskName) => {
          progressBar.update(taskNumber, { currentTask: taskName });
      };

      // Ejecutar migraciones con actualizaciones de progreso
      updateProgress(1, 'Migrando Bancos...');
      await migrarBancos(connection, data);

      updateProgress(2, 'Migrando MEF...');
      await migrarMEF(connection, data);

      updateProgress(3, 'Migrando Deloitte...');
      await migrarDeloitte(connection, data);

      updateProgress(4, 'Migrando Niveles...');
      await migrarNiveles(connection, data);

      updateProgress(5, 'Migrando Centro de Costos...');
      await migrarCentroCostos(connection, data);

      updateProgress(6, 'Migrando Puestos...');
      await migrarPuestos(connection, data);
      
      updateProgress(7, 'Migrando Tablas Generales...');
      await migrarTablasGenerales(connection, data);

      updateProgress(8, 'Migrando Información General...');
      await migrarInformacionGeneral(connection, data);

      // Completar la barra de progreso
      progressBar.update(totalTasks, { currentTask: 'Completado!' });
      progressBar.stop();

      console.log("\n=== Migración completada exitosamente ===");
  } catch (error) {
      console.error("\nError en la migración:", error);
  } finally {
      if (connection) {
          await connection.end();
          console.log("Conexión cerrada");
      }
  }
}

main().catch(console.error);