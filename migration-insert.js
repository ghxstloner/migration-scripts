const mysql = require('mysql2/promise');
const xlsx = require('xlsx');
const dbConfig = require('./dbconfig');
const cliProgress = require('cli-progress');
require('dotenv').config();

function formatExcelDate(date) {
    if (!date) return null;
    
    if (typeof date === 'number') {
        const excelDate = new Date((date - 25569) * 86400 * 1000);
        return excelDate.toISOString().split('T')[0];
    }
    
    try {
        const parts = date.split('/');
        if (parts.length === 3) {
            return `${parts[2]}-${parts[1].padStart(2, '0')}-${parts[0].padStart(2, '0')}`;
        }
    } catch (e) {
        console.log(`Error al procesar fecha: ${date}`);
        return null;
    }
    
    return null;
}

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
        await connection.execute('SET FOREIGN_KEY_CHECKS=1');
        await connection.rollback();
        throw error;
    } finally {
        await connection.execute('SET FOREIGN_KEY_CHECKS=1');
    }
}

async function migrarNiveles(connection, data) {
    console.log("\n=== Migrando Niveles ===");
    let actualizados = 0;
    let noActualizados = 0;
    
    try {
        const columnMapping = {
            'Vicepresidencia': 'VP',
            'Departamento': 'Departamento',
            'Secciones': 'Seccion',
            'Equipo': 'Equipo',
            'Grupo': 'Grupo'
        };

        const mappedData = data.map(row => {
            const mappedRow = {};
            for (const [oldKey, newKey] of Object.entries(columnMapping)) {
                mappedRow[newKey] = row[oldKey] || null;
            }
            mappedRow['Cedula'] = row['Cedula'] || null;
            return mappedRow;
        });
        
        console.log(`📌 Datos transformados: ${mappedData.length} registros`);

        const niveles = [
            { nivel: 'VP', table: 'nomnivel1' },
            { nivel: 'Departamento', table: 'nomnivel2' },
            { nivel: 'Seccion', table: 'nomnivel3' },
            { nivel: 'Equipo', table: 'nomnivel4' },
            { nivel: 'Grupo', table: 'nomnivel5' }
        ];

        for (const row of mappedData) {
            if (!row.Cedula) {
                console.warn(`⚠️ Cédula vacía en el registro:`, row);
                continue;
            }

            console.log(`🔍 Procesando cédula: ${row.Cedula}`);

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

            if (checkExisting.length === 0) {
                console.warn(`⚠️ La cédula ${row.Cedula} no existe en nompersonal. Saltando...`);
                noActualizados++;
                continue;
            }

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

        console.log(`🔹 Total de registros actualizados: ${actualizados}`);
        console.log(`🔹 Total de registros no actualizados: ${noActualizados}`);
        
    } catch (error) {
        console.error("❌ Error durante la migración:", error);
        throw error; // Re-lanzamos el error para manejarlo en el nivel superior
    }
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
        { nombre: 'sindicatos', campo: 'Sindicato', codigo: 'cod_sin', descripcion: 'des_sin' },
        { nombre: 'nivelacademico', campo: 'NivelAcademco', codigo: 'id', descripcion: 'descripcion'}
    ];
    
    try {
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
                              tabla.nombre === 'nivelacademico' ? 'cod_niv' :
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
        await connection.execute('SET FOREIGN_KEY_CHECKS=1');
    }
}

async function migrarPuestos(connection, personalData) {
    console.log("\n=== Migrando Puestos ===");
    
    await connection.execute('SET FOREIGN_KEY_CHECKS=0');
    
    try {
        const [tables] = await connection.execute(
            "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'puesto_aitsa'"
        );
        
        if (tables[0].count === 0) {
            await connection.execute(`
                CREATE TABLE puesto_aitsa (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    puesto VARCHAR(191) UNIQUE
                )
            `);
        }
        
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

        const workbookPuestos = xlsx.readFile('Puestos_Trabajo.xlsx');
        const sheetPuestos = workbookPuestos.Sheets[workbookPuestos.SheetNames[0]];
        const puestosData = xlsx.utils.sheet_to_json(sheetPuestos);
        
        const insertPuestoQuery = "INSERT IGNORE INTO puesto_aitsa (puesto) VALUES (?)";
        const puestosInsertados = new Set();
        
        for (const row of puestosData) {
            const puesto = row.Puesto !== undefined ? String(row.Puesto).trim() : null;
            if (puesto && !puestosInsertados.has(puesto)) {
                await connection.execute(insertPuestoQuery, [puesto]);
                puestosInsertados.add(puesto);
            }
        }
        
        await connection.execute(`
            CREATE TEMPORARY TABLE temp_puestos (
                cedula VARCHAR(50),
                puesto VARCHAR(191)
            )
        `);
        
        const insertTempQuery = "INSERT INTO temp_puestos (cedula, puesto) VALUES ?";
        const tempData = personalData
            .map(row => [
                row.Cedula || null,
                row.Puesto !== undefined ? String(row.Puesto).trim() : null
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
            JOIN temp_puestos tp ON np.cedula = tp.cedula
            JOIN puesto_aitsa pa ON tp.puesto = pa.puesto
            SET np.id_puesto = pa.id
        `);

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
        await connection.execute('SET FOREIGN_KEY_CHECKS=1');
    }
}

async function insertarPersonal(connection, data) {
    console.log("\n=== Insertando Personal ===");
    
    await connection.execute(`
        CREATE TEMPORARY TABLE IF NOT EXISTS temp_personal (
            codigo_carnet VARCHAR(50),
            numero_carnet VARCHAR(50),
            estado VARCHAR(20),
            apellidos VARCHAR(100),
            apellido_materno VARCHAR(100),
            nombres VARCHAR(100),
            sexo VARCHAR(20),
            nacionalidad INT,
            fecnac DATE,
            lugarnac VARCHAR(100),
            cedula VARCHAR(50),
            seguro_social VARCHAR(50),
            estado_civil VARCHAR(20),
            dv VARCHAR(10),
            direccion TEXT,
            direccion2 VARCHAR(191),
            telefonos VARCHAR(50),
            TelefonoResidencial VARCHAR(50),
            TelefonoCelular VARCHAR(50),
            email VARCHAR(100),
            fecing DATE,
            fecha_resolucion_baja DATE,
            cuenta_pago VARCHAR(50),
            ISRFijoPeriodo DECIMAL(10,2),
            suesal DECIMAL(10,2),
            salario_diario DECIMAL(10,2),
            rata_x_hr DECIMAL(10,2),
            gastos_representacion DECIMAL(10,2),
            gasto_rep_diario DECIMAL(10,2),
            rata_hora_gasto_rep DECIMAL(10,2),
            ConceptoBaja VARCHAR(100),
            tipnom VARCHAR(3),
            apenom VARCHAR(200),
            foto VARCHAR(100)
        )
    `);

    const insertTempQuery = `
        INSERT INTO temp_personal (
            codigo_carnet, numero_carnet, estado, apellidos, apellido_materno,
            nombres, sexo, nacionalidad, fecnac, lugarnac, cedula, seguro_social,
            estado_civil, dv, direccion, direccion2, telefonos, TelefonoResidencial,
            TelefonoCelular, email, fecing, fecha_resolucion_baja, cuenta_pago,
            ISRFijoPeriodo, suesal, salario_diario, rata_x_hr, gastos_representacion,
            gasto_rep_diario, rata_hora_gasto_rep, ConceptoBaja, tipnom, apenom, foto
        ) VALUES ?
    `;

    const tempData = data
        .map(row => {
            // Manejar el tipnom según la categoría
            let tipnom;
            switch(row.Categoria?.trim()?.toUpperCase()) {
                case 'JUBILADOS':
                    tipnom = '002';
                    break;
                case 'PERMANENTES':
                    tipnom = '001';
                    break;
                case 'TRANSITORIOS':
                    tipnom = '003';
                    break;
                default:
                    tipnom = null;
            }

            // Construir apenom
            const apenom = `${row.ApellidoPaterno || ''} ${row.ApellidoMaterno || ''}, ${row.Nombre || ''}`.trim();

            // Construir ruta de foto
            const foto = row.Cedula ? `fotos/${row.Cedula}` : null;

            return [
                row.Personal || null,
                row.Personal || null,
                row.Estatus === 'BAJA' ? 'De Baja' : (row.Estatus === 'ALTA' ? 'Activo' : row.Estatus || null),
                row.ApellidoPaterno || null,
                row.ApellidoMaterno || null,
                row.Nombre || null,
                row.Sexo || null,
                row.Nacionalidad === 'Panamena' ? 1 : 2,
                formatExcelDate(row.FechaNacimiento),
                row.LugarNacimiento || null,
                row.Cedula || null,
                row.SeguroSocial || null,
                row.EstadoCivil || null,
                row.DV || null,
                row.Direccion || null,
                row.Barrio || null,
                row.Telefono || null,
                row.Telefono || null,
                row.Celular || null,
                row.eMail || null,
                formatExcelDate(row.FechaAntiguedad),
                formatExcelDate(row.FechaBaja),
                row.CtaDinero || null,
                parseFloat(row.ISRFijoPeriodo) || 0,
                parseFloat(row.SueldoMensual) || 0,
                parseFloat(row.SueldoDiario) || 0,
                parseFloat(row.RataHora) || 0,
                parseFloat(row.GR) || 0,
                parseFloat(row.GastoRepresentacionDiario) || 0,
                parseFloat(row.RataHoraGR) || 0,
                row.ConceptoBaja || null,
                tipnom,
                apenom,
                foto
            ];
        })
        .filter(row => row[10]); // Filtrar por cédula

    if (tempData.length > 0) {
        const batchSize = 1000;
        for (let i = 0; i < tempData.length; i += batchSize) {
            const batch = tempData.slice(i, i + batchSize);
            await connection.query(insertTempQuery, [batch]);
        }
    }

    const insertQuery = `
        INSERT INTO nompersonal (
            codigo_carnet, numero_carnet, estado, apellidos, apellido_materno,
            nombres, sexo, nacionalidad, fecnac, lugarnac, cedula, seguro_social,
            estado_civil, dv, direccion, direccion2, telefonos, TelefonoResidencial,
            TelefonoCelular, email, fecing, fecha_resolucion_baja, cuenta_pago,
            ISRFijoPeriodo, suesal, salario_diario, rata_x_hr, gastos_representacion,
            gasto_rep_diario, rata_hora_gasto_rep, ConceptoBaja, tipnom, apenom, foto
        )
        SELECT 
            codigo_carnet, numero_carnet, estado, apellidos, apellido_materno,
            nombres, sexo, nacionalidad, fecnac, lugarnac, cedula, seguro_social,
            estado_civil, dv, direccion, direccion2, telefonos, TelefonoResidencial,
            TelefonoCelular, email, fecing, fecha_resolucion_baja, cuenta_pago,
            ISRFijoPeriodo, suesal, salario_diario, rata_x_hr, gastos_representacion,
            gasto_rep_diario, rata_hora_gasto_rep, ConceptoBaja, tipnom, apenom, foto
        FROM temp_personal tp
        WHERE NOT EXISTS (
            SELECT 1 FROM nompersonal np WHERE np.cedula = tp.cedula
        )
    `;

    await connection.execute(insertQuery);
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

        const progressBar = new cliProgress.SingleBar({
            format: 'Progreso |{bar}| {percentage}% || {value}/{total} Módulos || {currentTask}',
            barCompleteChar: '\u2588',
            barIncompleteChar: '\u2591',
            hideCursor: true
        });

        const totalTasks = 8;
        progressBar.start(totalTasks, 0, { currentTask: 'Iniciando...' });

        try {
            progressBar.update(1, { currentTask: 'Insertando Personal...' });
            await insertarPersonal(connection, data);

            progressBar.update(2, { currentTask: 'Migrando Bancos...' });
            await migrarBancos(connection, data);

            progressBar.update(3, { currentTask: 'Migrando MEF...' });
            await migrarMEF(connection, data);

            progressBar.update(4, { currentTask: 'Migrando Deloitte...' });
            await migrarDeloitte(connection, data);

            progressBar.update(5, { currentTask: 'Migrando Niveles...' });
            await migrarNiveles(connection, data);

            progressBar.update(6, { currentTask: 'Migrando Centro de Costos...' });
            await migrarCentroCostos(connection, data);

            progressBar.update(7, { currentTask: 'Migrando Tablas Generales...' });
            await migrarTablasGenerales(connection, data);

            progressBar.update(8, { currentTask: 'Migrando Puestos...' });
            await migrarPuestos(connection, data);

            progressBar.update(totalTasks, { currentTask: 'Completado!' });
        } catch (error) {
            console.error('Error durante la migración:', error);
            throw error;
        }

        progressBar.stop();
        console.log("\n=== Migración completada exitosamente ===");
    } catch (error) {
        console.error("\nError en la migración:", error);
        throw error;
    } finally {
        if (connection) {
            await connection.end();
            console.log("Conexión cerrada");
        }
    }
}

main().catch(console.error);