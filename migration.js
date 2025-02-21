const mysql = require('mysql2/promise');
const xlsx = require('xlsx');
const dbConfig = require('./dbconfig');
const cliProgress = require('cli-progress');
const { mapParentesco } = require('./parentesco-utils');
require('dotenv').config();

function formatExcelDate(date) {
    if (!date) return null;
    if (typeof date === 'number') {
        return new Date((date - 25569) * 86400 * 1000).toISOString().split('T')[0];
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

function extractNumericValue(str) {
    if (!str) return null;
    const matches = str.match(/\d+/);
    return matches ? parseInt(matches[0], 10) : null;
}

async function migrarBancos(connection, data) {
    console.log("\n=== Migrando Bancos ===");
    await connection.execute("DELETE FROM nombancos");

    // Mapeo de bancos a rutas
    const rutasBancos = {
        'Banco Nacional Panama': '13',
        'BANISTMO S.A.': '26',
        'BANCO GENERAL': '71',
        'GLOBAL BANK': '1151',
        'BANISI, S.A.': '1614',
        'CAJA DE AHORROS': '770',
        'BANCO NACIONAL DE PANAMA': '13',
        'BANESCO': '1588',
        'MULTIBANK': '372',
        'BAC INTERNATIONAL BANK': '1384',
        'BANCO BAC DE PANAMA, S.A.': '1384',
        'SCOTIABANK': '424',
        'BANCO AZTECA': '1504',
        'SCOTIABANK TRANSFORMaNDOSE': '1504',
        'ST. GEORGES BANK': '1494',
        'MMG BANK': '1478',
        'METROBANK S.A': '1067',
        'MERCANTIL BANK': '1630',
        'METROBANK S.A.': '1067'
    };

    const insertBancosQuery = "INSERT INTO nombancos (cod_ban, des_ban, ruta) VALUES (?, ?, ?)";
    let codBan = 1;
    const bancosInsertados = new Set();

    for (const row of data) {
        const banco = row.Banco !== undefined ? String(row.Banco).trim() : null;
        if (banco && !bancosInsertados.has(banco)) {
            // Si no se encuentra una ruta específica, usar '0' como valor por defecto
            const ruta = rutasBancos[banco] || '0';

            // Registrar bancos sin ruta específica para revisión
            if (!rutasBancos[banco]) {
                console.log(`Advertencia: Banco sin ruta específica: ${banco}`);
            }

            await connection.execute(insertBancosQuery, [codBan, banco, ruta]);
            bancosInsertados.add(banco);
            codBan++;
        }
    }

    await connection.execute(`
        CREATE TEMPORARY TABLE temp_bancos (
            numero_carnet VARCHAR(50),
            banco VARCHAR(191)
        )
    `);

    const insertTempQuery = "INSERT INTO temp_bancos (numero_carnet, banco) VALUES ?";
    const tempData = data
        .map(row => [
            row.Personal || null,
            row.Banco !== undefined ? String(row.Banco).trim() : null
        ])
        .filter(row => row[0] && row[1]);

    if (tempData.length > 0) {
        const batchSize = 1000;
        for (let i = 0; i < tempData.length; i += batchSize) {
            await connection.query(insertTempQuery, [tempData.slice(i, i + batchSize)]);
        }
    }

    const [updateResult] = await connection.execute(`
        UPDATE nompersonal np
        JOIN temp_bancos tp ON np.numero_carnet = tp.numero_carnet 
        JOIN nombancos nb ON tp.banco = nb.des_ban
        SET np.codbancob = nb.cod_ban
    `);
}

async function migrarMEF(connection, data) {
    console.log("\n=== Iniciando Migración MEF ===");

    await connection.beginTransaction();

    try {
        await connection.execute('SET FOREIGN_KEY_CHECKS=0');

        await connection.execute("DROP TABLE IF EXISTS cargos_mef");
        await connection.execute("DROP TABLE IF EXISTS codigos_cargo_mef");
        await connection.execute("DROP TABLE IF EXISTS posiciones_mef");

        await connection.execute(`
             CREATE TABLE posiciones_mef (
                 id INT AUTO_INCREMENT PRIMARY KEY,
                 posicionmef VARCHAR(10) UNIQUE NOT NULL
             ) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci
         `);

        await connection.execute(`
             CREATE TABLE codigos_cargo_mef (
                 id INT AUTO_INCREMENT PRIMARY KEY,
                 codigocargomef VARCHAR(10) UNIQUE NOT NULL
             ) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci
         `);

        await connection.execute(`
             CREATE TABLE cargos_mef (
                 id INT AUTO_INCREMENT PRIMARY KEY,
                 cargo VARCHAR(191) UNIQUE NOT NULL
             ) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci
         `);

        await connection.execute(`DROP TEMPORARY TABLE IF EXISTS temp_mef`);
        await connection.execute(`
            CREATE TEMPORARY TABLE temp_mef (
                numero_carnet VARCHAR(50),
                posicionmef VARCHAR(10),
                codigocargomef VARCHAR(10),
                cargomef VARCHAR(191)
            ) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci
        `);

        const insertTemp = "INSERT INTO temp_mef (numero_carnet, posicionmef, codigocargomef, cargomef) VALUES ?";
        const tempValues = data
            .filter(row => row.Personal && row.PosicionMEF && row.CodigoCargoMef && row.CargoMef)
            .map(row => {
                const carnet = row.Personal.toString().trim().padEnd(10, ' ');
                return [
                    carnet,
                    row.PosicionMEF.toString().trim(),
                    row.CodigoCargoMef.toString().trim(),
                    row.CargoMef.trim()
                ];
            });

        if (tempValues.length > 0) {
            await connection.query(insertTemp, [tempValues]);
        }

        await connection.execute(`
            INSERT INTO posiciones_mef (posicionmef)
            SELECT DISTINCT posicionmef FROM temp_mef
        `);

        await connection.execute(`
            INSERT INTO codigos_cargo_mef (codigocargomef)
            SELECT DISTINCT codigocargomef FROM temp_mef
        `);

        await connection.execute(`
            INSERT INTO cargos_mef (cargo)
            SELECT DISTINCT cargomef FROM temp_mef
        `);

        const [updateResult] = await connection.execute(`
            UPDATE nompersonal np
            INNER JOIN temp_mef t ON TRIM(np.numero_carnet) = TRIM(t.numero_carnet)
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

        return updateResult.affectedRows;
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
                await connection.execute(insertCargoQuery, [cargoId++, cargo]);
                cargosSet.add(cargo);
            }

            if (nivel && !nivelesSet.has(nivel)) {
                await connection.execute(insertNivelQuery, [nivelId++, nivel]);
                nivelesSet.add(nivel);
            }

            if (rol && !rolesSet.has(rol)) {
                await connection.execute(insertRolQuery, [rolId++, rol]);
                rolesSet.add(rol);
            }
        }

        await connection.execute(`
           CREATE TEMPORARY TABLE temp_cargos (
               numero_carnet VARCHAR(50),
               cargo VARCHAR(191),
               nivel VARCHAR(191),
               rol VARCHAR(191)
           )
       `);

        const insertTempQuery = "INSERT INTO temp_cargos (numero_carnet, cargo, nivel, rol) VALUES ?";
        const tempData = data
            .map(row => [
                row.Personal || null,
                row.CargoDeloitte?.trim() || null,
                row.NivelCargo?.trim() || null,
                row.RolCargo?.trim() || null
            ])
            .filter(row => row[0] && (row[1] || row[2] || row[3]));

        if (tempData.length > 0) {
            for (let i = 0; i < tempData.length; i += 1000) {
                await connection.query(insertTempQuery, [tempData.slice(i, i + 1000)]);
            }
        }

        const [updateResult] = await connection.execute(`
           INSERT INTO cargoempleado (id_empleado, id_cargo, id_nivel, id_rol, fecha_inicio)
           SELECT DISTINCT
               np.personal_id,
               cd.id_cargo,
               nc.id_nivel,
               rc.id_rol,
               CURRENT_DATE
           FROM nompersonal np
           INNER JOIN temp_cargos tc ON np.numero_carnet = tc.numero_carnet
           LEFT JOIN cargodeloitte cd ON TRIM(tc.cargo) = TRIM(cd.nombre_cargo)
           LEFT JOIN nivelcargo nc ON TRIM(tc.nivel) = TRIM(nc.nombre_nivel)
           LEFT JOIN rolcargo rc ON TRIM(tc.rol) = TRIM(rc.nombre_rol)
           WHERE np.personal_id IS NOT NULL
           AND (tc.cargo IS NOT NULL OR tc.nivel IS NOT NULL OR tc.rol IS NOT NULL)
       `);
    } finally {
        await connection.execute('SET FOREIGN_KEY_CHECKS=1');
    }
}
async function migrarNiveles(connection, data) {
    console.log("\n=== Migrando Niveles ===");

    // Limpiar las tablas
    await connection.execute('SET FOREIGN_KEY_CHECKS=0');
    for (let i = 1; i <= 5; i++) {
        await connection.execute(`TRUNCATE TABLE nomnivel${i}`);
    }

    // Función auxiliar para extraer el código y la descripción corta
    function extractCodigoYDescripcion(valor) {
        if (!valor) return { codigo: null, descripcionCorta: null };
        const matches = valor.match(/^(\d+)\s+(.+)$/);
        if (!matches) return { codigo: null, descripcionCorta: valor };
        return {
            codigo: matches[1],
            descripcionCorta: matches[2]
        };
    }

    // Procesar nivel 1 (VP)
    const nivel1Set = new Set();
    const nivel1Map = new Map(); // Para almacenar codorg por descripción
    let counter1 = 1;

    for (const row of data) {
        const vp = row.VP?.trim();
        if (vp && !nivel1Set.has(vp)) {
            nivel1Set.add(vp);
            const { codigo, descripcionCorta } = extractCodigoYDescripcion(vp);

            await connection.execute(
                'INSERT INTO nomnivel1 (codorg, descrip, descripcion_corta) VALUES (?, ?, ?)',
                [counter1, vp, descripcionCorta]
            );

            nivel1Map.set(vp, counter1);
            counter1++;
        }
    }

    // Procesar nivel 2 (Departamento)
    const nivel2Set = new Set();
    const nivel2Map = new Map();
    let counter2 = 1;

    for (const row of data) {
        const dep = row.Departamento?.trim();
        if (dep && !nivel2Set.has(dep)) {
            nivel2Set.add(dep);
            const { codigo, descripcionCorta } = extractCodigoYDescripcion(dep);
            const vpParent = row.VP?.trim();
            const gerencia = vpParent ? nivel1Map.get(vpParent) : null;

            await connection.execute(
                'INSERT INTO nomnivel2 (codorg, descrip, gerencia, descripcion_corta) VALUES (?, ?, ?, ?)',
                [counter2, dep, gerencia, descripcionCorta]
            );

            nivel2Map.set(dep, counter2);
            counter2++;
        }
    }

    // Procesar nivel 3 (Sección)
    const nivel3Set = new Set();
    const nivel3Map = new Map();
    let counter3 = 1;

    for (const row of data) {
        const sec = row.Seccion?.trim();
        if (sec && !nivel3Set.has(sec)) {
            nivel3Set.add(sec);
            const { codigo, descripcionCorta } = extractCodigoYDescripcion(sec);
            const depParent = row.Departamento?.trim();
            const gerencia = depParent ? nivel2Map.get(depParent) : null;

            await connection.execute(
                'INSERT INTO nomnivel3 (codorg, descrip, gerencia, descripcion_corta) VALUES (?, ?, ?, ?)',
                [counter3, sec, gerencia, descripcionCorta]
            );

            nivel3Map.set(sec, counter3);
            counter3++;
        }
    }

    // Procesar nivel 4 (Equipo)
    const nivel4Set = new Set();
    const nivel4Map = new Map();
    let counter4 = 1;

    for (const row of data) {
        const eq = row.Equipo?.trim();
        if (eq && !nivel4Set.has(eq)) {
            nivel4Set.add(eq);
            const { codigo, descripcionCorta } = extractCodigoYDescripcion(eq);
            const secParent = row.Seccion?.trim();
            const gerencia = secParent ? nivel3Map.get(secParent) : null;

            await connection.execute(
                'INSERT INTO nomnivel4 (codorg, descrip, gerencia, descripcion_corta) VALUES (?, ?, ?, ?)',
                [counter4, eq, gerencia, descripcionCorta]
            );

            nivel4Map.set(eq, counter4);
            counter4++;
        }
    }

    // Procesar nivel 5 (Grupo)
    const nivel5Set = new Set();
    let counter5 = 1;

    for (const row of data) {
        const grp = row.Grupo?.trim();
        if (grp && !nivel5Set.has(grp)) {
            nivel5Set.add(grp);
            const { codigo, descripcionCorta } = extractCodigoYDescripcion(grp);
            const eqParent = row.Equipo?.trim();
            const gerencia = eqParent ? nivel4Map.get(eqParent) : null;

            await connection.execute(
                'INSERT INTO nomnivel5 (codorg, descrip, gerencia) VALUES (?, ?, ?)',
                [counter5, grp, gerencia]
            );

            counter5++;
        }
    }

    await connection.execute('SET FOREIGN_KEY_CHECKS=1');
}

async function migrarNivelesPersonal(connection, data) {
    console.log("\n=== Actualizando Niveles en Personal ===");

    await connection.execute('SET FOREIGN_KEY_CHECKS=0');

    try {
        // Crear tabla temporal para mapear empleados con sus niveles
        await connection.execute(`
            CREATE TEMPORARY TABLE temp_niveles_personal (
                numero_carnet VARCHAR(50),
                vp VARCHAR(191),
                departamento VARCHAR(191),
                seccion VARCHAR(191),
                equipo VARCHAR(191),
                grupo VARCHAR(191)
            )
        `);

        // Insertar datos en la tabla temporal
        const insertTempQuery = `
            INSERT INTO temp_niveles_personal 
            (numero_carnet, vp, departamento, seccion, equipo, grupo) 
            VALUES ?
        `;

        const tempData = data
            .map(row => [
                row.Personal?.toString().trim() || null,
                row.Vicepresidencia?.toString().trim() || null,
                row.Departamento?.toString().trim() || null,
                row.Secciones?.toString().trim() || null,
                row.Equipo?.toString().trim() || null,
                row.Grupo?.toString().trim() || null
            ])
            .filter(row => row[0]); // Solo registros con número de carnet

        if (tempData.length > 0) {
            for (let i = 0; i < tempData.length; i += 1000) {
                await connection.query(insertTempQuery, [tempData.slice(i, i + 1000)]);
            }
        }

        // Actualizar nompersonal con los códigos de nivel correspondientes
        const updateQuery = `
            UPDATE nompersonal np
            LEFT JOIN temp_niveles_personal tnp ON np.numero_carnet = tnp.numero_carnet
            LEFT JOIN nomnivel1 n1 ON tnp.vp = n1.descrip
            LEFT JOIN nomnivel2 n2 ON tnp.departamento = n2.descrip
            LEFT JOIN nomnivel3 n3 ON tnp.seccion = n3.descrip
            LEFT JOIN nomnivel4 n4 ON tnp.equipo = n4.descrip
            LEFT JOIN nomnivel5 n5 ON tnp.grupo = n5.descrip
            SET 
                np.codnivel1 = n1.codorg,
                np.codnivel2 = n2.codorg,
                np.codnivel3 = n3.codorg,
                np.codnivel4 = n4.codorg,
                np.codnivel5 = n5.codorg
            WHERE np.numero_carnet = tnp.numero_carnet
        `;

        const [result] = await connection.execute(updateQuery);
        console.log(`Registros actualizados: ${result.affectedRows}`);

        // Limpiar tabla temporal
        await connection.execute('DROP TEMPORARY TABLE IF EXISTS temp_niveles_personal');

    } catch (error) {
        console.error('Error en migrarNivelesPersonal:', error);
        throw error;
    } finally {
        await connection.execute('SET FOREIGN_KEY_CHECKS=1');
    }
}

async function migrarCentroCostos(connection, data) {
    console.log("\n=== Migrando Centro de Costos ===");

    const centrosCosto = new Map();
    for (const row of data) {
        if (row.CentroCostos && row.Descripcion) {
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
        CREATE TEMPORARY TABLE temp_centro_costos (
            numero_carnet VARCHAR(50),
            cod_cos VARCHAR(10)
        )
    `);

    const insertTempQuery = "INSERT INTO temp_centro_costos (numero_carnet, cod_cos) VALUES ?";
    const tempData = data
        .map(row => [
            row.Personal || null,
            row.CentroCostos || null
        ])
        .filter(row => row[0] && row[1]);

    if (tempData.length > 0) {
        for (let i = 0; i < tempData.length; i += 1000) {
            await connection.query(insertTempQuery, [tempData.slice(i, i + 1000)]);
        }
    }

    await connection.execute(`
        UPDATE nompersonal np
        JOIN temp_centro_costos tp ON np.numero_carnet = tp.numero_carnet
        SET np.cod_cos = tp.cod_cos
    `);
}
async function migrarTablasGenerales(connection, data) {
    console.log("\n=== Migrando Tablas Generales ===");
 
    const tablas = [
        { nombre: 'aeropuertos', campo: 'Aeropuerto', codigo: 'codigo', descripcion: 'descripcion', prefijo: 'AER', codigo_pps: 'codigo_pps' },
        { nombre: 'dias_periodo', campo: 'DiasPeriodo', codigo: 'cod_dia', descripcion: 'des_dia' },
        { nombre: 'tipos_periodo', campo: 'PeriodoTipo', codigo: 'cod_tip', descripcion: 'des_tip' },
        { nombre: 'jornadas', campo: 'Jornada', codigo: 'cod_jor', descripcion: 'des_jor' },
        { nombre: 'tipos_sueldo', campo: 'TipoSueldo', codigo: 'cod_sue', descripcion: 'des_sue' },
        { nombre: 'sindicatos', campo: 'Sindicato', codigo: 'cod_sin', descripcion: 'des_sin' },
        { nombre: 'nivelacademico', campo: 'NivelAcademico', codigo: 'id', descripcion: 'descripcion' }
    ];
 
    const jornadasAdicionales = [
        '8:00 - 4:30 p.m. Administrativo 40',
        '8:00- 4:30 p.m. de lunes a viernes 45 horas administrativo',
        'Sábados 8:00 a.m. a 1:00 p.m. y de 11:30 a.m. a 4:30 p.m.',
        '9:00 a.m. - 5:00 p.m. Panamá Pacífico',
        '45 ADM',
        '45 ROTATIVO',
        '48 ROTATIVO',
        '48 ADM',
        '40 ADM',
        '40 ROTATIVO'
    ];
 
    await connection.execute('SET FOREIGN_KEY_CHECKS=0');
 
    for (const tabla of tablas) {
        await connection.execute(`DELETE FROM ${tabla.nombre}`);
 
        let codigo = 1;
        const valoresInsertados = new Set();
 
        if (tabla.nombre === 'jornadas') {
            // Leer el archivo de jornadas
            const workbookJornadas = xlsx.readFile('JornadaLaboral.xlsx');
            const sheetJornadas = workbookJornadas.Sheets[workbookJornadas.SheetNames[0]];
            const jornadasData = xlsx.utils.sheet_to_json(sheetJornadas);

            // Primero las jornadas adicionales
            for (const jornada of jornadasAdicionales) {
                const codFormatted = `J${String(codigo).padStart(3, '0')}`;
                await connection.execute(
                    `INSERT INTO ${tabla.nombre} (
                        ${tabla.codigo}, 
                        ${tabla.descripcion}
                    ) VALUES (?, ?)`,
                    [codFormatted, jornada]
                );
                valoresInsertados.add(jornada);
                codigo++;
            }

            // Luego las jornadas del Excel
            for (const row of jornadasData) {
                if (row.Jornada && !valoresInsertados.has(row.Jornada)) {
                    const codFormatted = `J${String(codigo).padStart(3, '0')}`;
                    
                    // Convertir factorAusentismo de formato español a decimal
                    const factorAusentismo = row.FactorAusentismo ? 
                        parseFloat(row.FactorAusentismo.toString().replace(',', '.')) : 
                        null;

                    await connection.execute(
                        `INSERT INTO ${tabla.nombre} (
                            ${tabla.codigo}, 
                            ${tabla.descripcion},
                            tipo,
                            domingo,
                            lunes,
                            martes,
                            miercoles,
                            jueves,
                            viernes,
                            sabado,
                            descanso_rompe_rutina,
                            descansa_festivos,
                            festivo_rompe_rutina,
                            horas_promedio,
                            factor_ausentismo,
                            logico1,
                            logico2,
                            logico3,
                            logico4,
                            logico5,
                            horas_semana,
                            es_rotativo,
                            jornada_reducida,
                            horas_comida,
                            es_comida_nocturna,
                            jornada_nocturna,
                            tipo_jornada,
                            minutos
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                        [
                            codFormatted,
                            row.Jornada,
                            row.Tipo || null,
                            row.Domingo || null,
                            row.Lunes || null,
                            row.Martes || null,
                            row.Miercoles || null,
                            row.Jueves || null,
                            row.Viernes || null,
                            row.Sabado || null,
                            row.DescansoRompeRutina || null,
                            row.DescansaFestivos || null,
                            row.FestivoRompeRutina || null,
                            row.HorasPromedio || null,
                            factorAusentismo,
                            row.Logico1 || null,
                            row.Logico2 || null,
                            row.Logico3 || null,
                            row.Logico4 || null,
                            row.Logico5 || null,
                            row.HorasSemana || null,
                            row.EsRotativo || null,
                            row.JornadaReducida || null,
                            row.HorasComida || null,
                            row.EsComidaNocturna || null,
                            row.JornadaNocturna || null,
                            row.TipoJornada || null,
                            row.Minutos || null
                        ]
                    );
                    valoresInsertados.add(row.Jornada);
                    codigo++;
                }
            }
        } else {
            // Procesamiento normal para otras tablas
            for (const row of data) {
                const valor = row[tabla.campo]?.trim();
                if (valor && !valoresInsertados.has(valor)) {
                    const codPrefijo = tabla.prefijo || tabla.nombre.charAt(0).toUpperCase();
                    const codFormatted = `${codPrefijo}${String(codigo).padStart(3, '0')}`;
     
                    let codigoPPS = null;
                    if (tabla.nombre === 'aeropuertos') {
                        switch (valor) {
                            case 'AEROPUERTO INTERNACIONAL PANAMA PACIFICO':
                                codigoPPS = '4';
                                break;
                            case 'AEROPUERTO INTERNACIONAL ENRIQUE A. JIMENEZ':
                                codigoPPS = '3';
                                break;
                            case 'AEROPUERTO INTERNACIONAL SCARLETT MARTINEZ':
                                codigoPPS = '2';
                                break;
                            case 'AEROPUERTO INTERNACIONAL ENRIQUE MALEK':
                                codigoPPS = '1';
                                break;
                            default:
                                codigoPPS = '0';
                                break;
                        }
                    }
     
                    await connection.execute(
                        `INSERT INTO ${tabla.nombre} (${tabla.codigo}, ${tabla.descripcion}${codigoPPS ? `, ${tabla.codigo_pps}` : ''}) VALUES (?, ?${codigoPPS ? ', ?' : ''})`,
                        [codFormatted, valor, codigoPPS].filter(Boolean)
                    );
     
                    valoresInsertados.add(valor);
                    codigo++;
                }
            }
        }
 
        await connection.execute(`
            CREATE TEMPORARY TABLE temp_${tabla.nombre} (
                numero_carnet VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
                valor VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci
        `);
 
        const insertTempQuery = `INSERT INTO temp_${tabla.nombre} (numero_carnet, valor) VALUES ?`;
        const tempData = data
            .map(row => [
                row.Personal || null,
                row[tabla.campo]?.trim() || null
            ])
            .filter(row => row[0] && row[1]);
 
        if (tempData.length > 0) {
            for (let i = 0; i < tempData.length; i += 1000) {
                await connection.query(insertTempQuery, [tempData.slice(i, i + 1000)]);
            }
        }
 
        const campoUpdate = {
            'aeropuertos': 'cod_aer',
            'dias_periodo': 'cod_dia',
            'tipos_periodo': 'cod_tip',
            'jornadas': 'cod_jor',
            'tipos_sueldo': 'cod_sue',
            'nivelacademico': 'cod_niv',
            'sindicatos': 'cod_sin'
        }[tabla.nombre];
 
        await connection.execute(`
            UPDATE nompersonal np
            JOIN temp_${tabla.nombre} tp ON CONVERT(np.numero_carnet USING utf8mb4) COLLATE utf8mb4_0900_ai_ci = tp.numero_carnet
            JOIN ${tabla.nombre} t ON CONVERT(tp.valor USING utf8mb4) COLLATE utf8mb4_0900_ai_ci = CONVERT(t.${tabla.descripcion} USING utf8mb4) COLLATE utf8mb4_0900_ai_ci
            SET np.${campoUpdate} = t.${tabla.codigo}
        `);
    }
    await connection.execute('SET FOREIGN_KEY_CHECKS=1');
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

        if (!checkColumn[0][0].exists_count) {
            await connection.execute(`
                ALTER TABLE nompersonal
                ADD COLUMN id_puesto INT
            `);
        }

        const workbookPuestos = xlsx.readFile('Puestos_Trabajo.xlsx');
        const sheetPuestos = workbookPuestos.Sheets[workbookPuestos.SheetNames[0]];
        const puestosData = xlsx.utils.sheet_to_json(sheetPuestos);

        for (const row of puestosData) {
            const puesto = row.Puesto?.trim();
            if (puesto) {
                await connection.execute(
                    "INSERT IGNORE INTO puesto_aitsa (puesto) VALUES (?)",
                    [puesto]
                );
            }
        }

        await connection.execute(`
            CREATE TEMPORARY TABLE temp_puestos (
                numero_carnet VARCHAR(50),
                puesto VARCHAR(191)
            )
        `);

        const tempData = personalData
            .map(row => [
                row.Personal || null,
                row.Puesto?.trim() || null
            ])
            .filter(row => row[0] && row[1]);

        if (tempData.length > 0) {
            for (let i = 0; i < tempData.length; i += 1000) {
                await connection.query(
                    "INSERT INTO temp_puestos (numero_carnet, puesto) VALUES ?",
                    [tempData.slice(i, i + 1000)]
                );
            }
        }

        await connection.execute(`
            UPDATE nompersonal np
            JOIN temp_puestos tp ON np.numero_carnet = tp.numero_carnet
            JOIN puesto_aitsa pa ON tp.puesto = pa.puesto
            SET np.id_puesto = pa.id
        `);

        if (!checkColumn[0][0].exists_count) {
            await connection.execute(`
                ALTER TABLE nompersonal
                ADD CONSTRAINT fk_nompersonal_puesto
                FOREIGN KEY (id_puesto) REFERENCES puesto_aitsa(id)
            `);
        }

    } finally {
        await connection.execute('SET FOREIGN_KEY_CHECKS=1');
    }
}

async function insertarPersonal(connection, data) {
    console.log("\n=== Insertando Personal ===");

    await connection.execute(`
        CREATE TEMPORARY TABLE temp_personal (
            codigo_carnet VARCHAR(50),
            numero_carnet VARCHAR(50),
            ficha INT,
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
            sueldopro DECIMAL(10,2),
            salario_diario DECIMAL(10,2),
            rata_x_hr DECIMAL(10,2),
            gastos_representacion DECIMAL(10,2),
            gasto_rep_diario DECIMAL(10,2),
            rata_hora_gasto_rep DECIMAL(10,2),
            ConceptoBaja VARCHAR(100),
            tipnom VARCHAR(3),
            apenom VARCHAR(200),
            foto VARCHAR(100),
            nomposicion_id VARCHAR(4),
            codcargo VARCHAR(10),
            forcob VARCHAR(50),
            cuentacob VARCHAR(50),
            created_at TIMESTAMP,
            turno_id INT DEFAULT 1,
            fin_periodo DATE,
            tipo_empleado VARCHAR(50) DEFAULT 'Titular',
            observaciones TEXT,
            IdDepartamento INT,
            nomfuncion_id INT,
            usuario_workflow VARCHAR(50),
            usr_password VARCHAR(100),
            proyecto INT,
            Hijos INT,
            IdTipoSangre INT,
            EnfermedadesYAlergias VARCHAR(100),
            ContactoEmergencia VARCHAR(300),
            TelefonoEmergencia VARCHAR(100),
            ParentescoEmergencia INT,
            DireccionEmergencia VARCHAR(255),
            ContactoEmergencia2 VARCHAR(300),
            TelefonoEmergencia2 VARCHAR(100),
            ParentescoEmergencia2 INT,
            DireccionEmergencia2 VARCHAR(255),
            tipemp VARCHAR(50),
            fecharetiro DATE,
            tipo_funcionario INT,
            zona_economica VARCHAR(50)
        )
    `);

    function extractNumericValue(str) {
        if (!str) return null;
        const matches = str.match(/\d+/);
        return matches ? parseInt(matches[0], 10) : null;
    }

    function mapEstadoCivil(estado) {
        if (estado === undefined || estado === null) {
            return 'Soltero/a';
        }

        if (typeof estado === 'number') {
            return estado === 2 ? 'Soltero/a' : 'Soltero/a';
        }

        if (typeof estado !== 'string') {
            console.log(`Estado civil tipo inválido: ${typeof estado}, valor: ${estado}`);
            return 'Soltero/a';
        }

        const mapping = {
            '2': 'Soltero/a',
            'CASADA': 'Casado/a',
            'CASADO': 'Casado/a',
            'DIVORCIADA': 'Divorciado/a',
            'DIVORCIADO': 'Divorciado/a',
            'SEPARADA': 'Divorciado/a',
            'SOLTERA': 'Soltero/a',
            'SOLTERO': 'Soltero/a',
            'UNIDA': 'Unido',
            'UNIDO': 'Unido',
            'UNION LIBRE': 'Unido',
            'VIUDA': 'Viudo/a',
            'VIUDO': 'Viudo/a'
        };
        return mapping[estado.toUpperCase()] || 'Soltero/a';
    }

    function mapEstado(estado) {
        if (!estado) return null;

        const mapping = {
            'ALTA': 'Activo',
            'ASPIRANTE': 'Aspirante',
            'BAJA': 'De Baja'
        };

        const cleanEstado = estado.toString().trim().toUpperCase();
        return mapping[cleanEstado] || 'De Baja';
    }

    function formatPosicionMEF(posicion) {
        if (!posicion) return '9999';
        return posicion.toString().padStart(4, '0');
    }

    async function mapTipoSangre(tipo) {
        if (!tipo) return null;
        const [rows] = await connection.execute(
            'SELECT IdTipoSangre FROM tiposangre WHERE Descripcion = ?',
            [tipo.toString().trim()]
        );
        return rows[0]?.IdTipoSangre || null;
    }

    const insertTempQuery = `
        INSERT INTO temp_personal (
            codigo_carnet, numero_carnet, ficha, estado, apellidos, apellido_materno,
            nombres, sexo, nacionalidad, fecnac, lugarnac, cedula, seguro_social,
            estado_civil, dv, direccion, direccion2, telefonos, TelefonoResidencial,
            TelefonoCelular, email, fecing, fecha_resolucion_baja, cuenta_pago,
            ISRFijoPeriodo, suesal, sueldopro, salario_diario, rata_x_hr, gastos_representacion,
            gasto_rep_diario, rata_hora_gasto_rep, ConceptoBaja, tipnom, apenom, foto,
            nomposicion_id, codcargo, forcob, cuentacob, created_at, turno_id, 
            fin_periodo, tipo_empleado, observaciones, IdDepartamento, nomfuncion_id,
            usuario_workflow, usr_password, proyecto, Hijos, IdTipoSangre, EnfermedadesYAlergias,
            ContactoEmergencia, TelefonoEmergencia, ParentescoEmergencia, DireccionEmergencia,
            ContactoEmergencia2, TelefonoEmergencia2, ParentescoEmergencia2, DireccionEmergencia2,
            tipemp, fecharetiro, tipo_funcionario, zona_economica
        ) VALUES ?
    `;

    const fechaHoy = new Date().toISOString().split('T')[0];

    const tempData = await Promise.all(data.map(async row => {
        const fichaNum = extractNumericValue(row.Personal);
        const sueldoMensual = parseFloat(row.SueldoMensual) || 0;
        const tipnom = {
            'JUBILADOS': '002',
            'PERMANENTES': '001',
            'TRANSITORIOS': '003'
        }[row.Categoria?.trim()?.toUpperCase()] || null;

        const [departamentoId] = await connection.execute(
            'SELECT IdDepartamento FROM departamento WHERE Descripcion = ?',
            [row.Departamento ?? null]
        );

        const [funcionId] = await connection.execute(
            'SELECT nomfuncion_id FROM nomfuncion WHERE descripcion_funcion = ?',
            [row.Puesto ?? null]
        );

        const apenom = `${row.ApellidoPaterno || ''} ${row.ApellidoMaterno || ''}, ${row.Nombre || ''}`.trim();
        const foto = row.Cedula ? `fotos/${row.Cedula}.jpeg` : null;
        const tipoSangreId = await mapTipoSangre(row.TipoSangre);
        const parentescoContacto1 = await mapParentesco(connection, row.ParentescoContacto1);
        const parentescoContacto2 = await mapParentesco(connection, row.ParentescoContacto2);
        const formattedCarnet = (row.Personal || '').toString().trim().padEnd(10, ' ');

        return [
            formattedCarnet,  // codigo_carnet con padding
            formattedCarnet,
            fichaNum,
            mapEstado(row.Estatus),
            row.ApellidoPaterno || null,
            row.ApellidoMaterno || null,
            row.Nombre || null,
            row.Sexo || null,
            row.Nacionalidad === 'Panamena' ? 1 : 2,
            formatExcelDate(row.FechaNacimiento),
            row.LugarNacimiento || null,
            row.Cedula || null,
            row.SeguroSocial || null,
            mapEstadoCivil(row.EstadoCivil),
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
            sueldoMensual,
            sueldoMensual,
            parseFloat(row.SueldoDiario) || 0,
            parseFloat(row.RataHora) || 0,
            parseFloat(row.GR) || 0,
            parseFloat(row.GastoRepresentacionDiario) || 0,
            parseFloat(row.RataHoraGR) || 0,
            row.ConceptoBaja || null,
            tipnom,
            apenom,
            foto,
            formatPosicionMEF(row.PosicionMEF),
            row.CodigoCargoMef || null,
            row.FormaPago || null,
            row.PersonalCuenta || null,
            formatExcelDate(row.FechaAlta),
            1,
            formatExcelDate(row.FechaBaja),
            'Titular',
            `Migración del Excel ${fechaHoy}`,
            departamentoId[0]?.IdDepartamento || null,
            funcionId[0]?.nomfuncion_id || null,
            row.Cedula || null,
            'e10adc3949ba59abbe56e057f20f883e',
            1,
            row.Hijos || null,
            tipoSangreId || null,
            row.Enfermedades || null,
            row.NombreContacto1 || null,
            row.CelularContacto1 || null,
            parentescoContacto1 || null,
            row.DireccionPariente || null,
            row.NombreContacto2 || null,
            row.CelularContacto2 || null,
            parentescoContacto2 || null,
            row.DireccionPariente2 || null,
            "Fijo",
            '0000-00-00',
            1,
            1
        ];
    }));

    const filteredData = tempData.filter(row => row[0]);

    if (filteredData.length > 0) {
        for (let i = 0; i < filteredData.length; i += 1000) {
            await connection.query(insertTempQuery, [filteredData.slice(i, i + 1000)]);
        }
    }

    await connection.execute(`
        INSERT INTO nompersonal (
            codigo_carnet, numero_carnet, ficha, estado, apellidos, apellido_materno,
            nombres, sexo, nacionalidad, fecnac, lugarnac, cedula, seguro_social,
            estado_civil, dv, direccion, direccion2, telefonos, TelefonoResidencial,
            TelefonoCelular, email, fecing, fecha_resolucion_baja, cuenta_pago,
            ISRFijoPeriodo, suesal, sueldopro, salario_diario, rata_x_hr, gastos_representacion,
            gasto_rep_diario, rata_hora_gasto_rep, ConceptoBaja, tipnom, apenom, foto,
            nomposicion_id, codcargo, forcob, cuentacob, created_at, turno_id,
            fin_periodo, tipo_empleado, observaciones, IdDepartamento, nomfuncion_id,
            usuario_workflow, usr_password, proyecto, Hijos, IdTipoSangre, EnfermedadesYAlergias,
            ContactoEmergencia, TelefonoEmergencia, ParentescoEmergencia, DireccionEmergencia,
            ContactoEmergencia2, TelefonoEmergencia2, ParentescoEmergencia2, DireccionEmergencia2,
            tipemp, fecharetiro, tipo_funcionario, zona_economica
        )
        SELECT * FROM temp_personal tp
        WHERE NOT EXISTS (
            SELECT 1 FROM nompersonal np 
            WHERE TRIM(np.numero_carnet) = TRIM(tp.numero_carnet)
        )
    `);

    // Actualizar conficha con el mayor valor encontrado
    await connection.execute(`
        UPDATE nomempresa 
        SET conficha = (
            SELECT GREATEST(
                COALESCE((SELECT MAX(ficha) FROM nompersonal), 0),
                COALESCE(conficha, 0)
            )
        )
    `);
}

async function migrarFamiliares(connection, data) {
    console.log("\n=== Migrando Familiares ===");

    // Crear tabla temporal
    await connection.execute(`
        CREATE TEMPORARY TABLE temp_familiares (
            cedula VARCHAR(20),
            ficha VARCHAR(10),
            nombre VARCHAR(100),
            codpar INT,
            sexo VARCHAR(20),
            fecha_nac DATETIME,
            codgua INT DEFAULT 0,
            costo DECIMAL(10,2) DEFAULT 0,
            nacionalidad VARCHAR(1) DEFAULT 'N',
            afiliado TINYINT(1) DEFAULT 0,
            tipnom INT DEFAULT 1,
            cedula_beneficiario VARCHAR(20),
            apellido VARCHAR(100),
            niveledu VARCHAR(100) DEFAULT '',
            institucion VARCHAR(100) DEFAULT '',
            tallafranela VARCHAR(50) DEFAULT '',
            tallamono VARCHAR(50) DEFAULT '',
            fam_telf VARCHAR(15),
            fecha_beca DATE,
            beca INT DEFAULT 0,
            promedionota DECIMAL(10,2) DEFAULT 0,
            vive INT DEFAULT 1,
            discapacidad INT DEFAULT 0
        )
    `);

    function isDiscapacitado(valor) {
        if (!valor) return false;

        try {
            if (typeof valor === 'number') return false;

            const valorLimpio = valor.toString().trim().toUpperCase();

            return valorLimpio === 'SI';
        } catch (error) {
            console.log(`Error procesando valor de discapacidad: ${valor}`, error);
            return false;
        }
    }

    // Procesar familiares
    const familiares = [];
    for (const row of data) {
        // Procesar el primer beneficiario (sin número)
        if (row.Beneficiario) {
            const codpar = await mapParentesco(connection, row.Parentesco);
            const ficha = row.Personal ? parseInt(row.Personal.replace(/\D/g, ''), 10).toString() : '';

            familiares.push({
                cedula: row.Cedula,
                ficha: ficha,
                nombre: row.Beneficiario,
                codpar: codpar,
                cedula_beneficiario: row.Cedula1 || '',
                fecha_nac: formatExcelDate(row.BeneficiarioNacimiento),
                discapacidad: isDiscapacitado(row.Discapacidad1) ? 1 : 0
            });
        }

        // Procesar beneficiarios numerados (2-8)
        for (let i = 2; i <= 8; i++) {
            if (row[`Beneficiario${i}`]) {
                const codpar = await mapParentesco(connection, row[`Parentesco${i}`]);
                const ficha = row.Personal ? parseInt(row.Personal.replace(/\D/g, ''), 10).toString() : '';

                familiares.push({
                    cedula: row.Cedula,
                    ficha: ficha,
                    nombre: row[`Beneficiario${i}`],
                    codpar: codpar,
                    cedula_beneficiario: row[`Cedula${i}`] || '',
                    fecha_nac: formatExcelDate(row[`Beneficiario${i}Nacimiento`]),
                    discapacidad: isDiscapacitado(row[`Discapacidad${i}`]) ? 1 : 0
                });
            }
        }
    }

    // Insertar en lotes
    if (familiares.length > 0) {
        const insertQuery = `
            INSERT INTO temp_familiares 
            (cedula, ficha, nombre, codpar, cedula_beneficiario, fecha_nac, discapacidad)
            VALUES ?
        `;

        // Procesar por lotes de 1000
        for (let i = 0; i < familiares.length; i += 1000) {
            const batch = familiares.slice(i, i + 1000).map(f => [
                f.cedula,
                f.ficha,
                f.nombre,
                f.codpar,
                f.cedula_beneficiario,
                f.fecha_nac,
                f.discapacidad
            ]);
            await connection.query(insertQuery, [batch]);
        }
    }

    // Migrar a tabla final
    await connection.execute(`
        INSERT INTO nomfamiliares 
        (cedula, ficha, nombre, codpar, sexo, fecha_nac, codgua, costo, 
         nacionalidad, afiliado, tipnom, cedula_beneficiario, apellido, 
         niveledu, institucion, tallafranela, tallamono, fam_telf, 
         fecha_beca, beca, promedionota, vive, discapacidad)
        SELECT 
            cedula, ficha, nombre, codpar, sexo, fecha_nac, codgua, costo,
            nacionalidad, afiliado, tipnom, cedula_beneficiario, apellido,
            niveledu, institucion, tallafranela, tallamono, fam_telf,
            fecha_beca, beca, promedionota, vive, discapacidad
        FROM temp_familiares
        WHERE NOT EXISTS (
            SELECT 1 
            FROM nomfamiliares nf 
            WHERE nf.cedula = temp_familiares.cedula 
            AND nf.cedula_beneficiario = temp_familiares.cedula_beneficiario
        )
    `);

    // Limpiar tabla temporal
    await connection.execute('DROP TEMPORARY TABLE IF EXISTS temp_familiares');
}

async function main() {
    let connection;
    try {
        connection = await mysql.createConnection({
            ...dbConfig,
            connectTimeout: 60000
        });

        // Leer archivo de personal
        const workbookPersonal = xlsx.readFile('Personal_Al_06022025.xlsx');
        const sheetPersonal = workbookPersonal.Sheets[workbookPersonal.SheetNames[0]];
        const dataPersonal = xlsx.utils.sheet_to_json(sheetPersonal);

        // Leer archivo de estructura organizacional
        const workbookEstructura = xlsx.readFile('EstructuraOrganizacional.xlsx');
        const sheetEstructura = workbookEstructura.Sheets[workbookEstructura.SheetNames[0]];
        const dataEstructura = xlsx.utils.sheet_to_json(sheetEstructura);

        const progressBar = new cliProgress.SingleBar({
            format: 'Progreso |{bar}| {percentage}% || {value}/{total} Módulos || {currentTask}',
            barCompleteChar: '\u2588',
            barIncompleteChar: '\u2591',
            hideCursor: true
        });

        const totalTasks = 8; // Incrementado en 1 para incluir la estructura organizacional
        progressBar.start(totalTasks, 0, { currentTask: 'Iniciando...' });

        try {
            // Primero migrar la estructura organizacional
            progressBar.update(1, { currentTask: 'Migrando Estructura Organizacional...' });
            await migrarNiveles(connection, dataEstructura);

            progressBar.update(2, { currentTask: 'Insertando Personal...' });
            await insertarPersonal(connection, dataPersonal);

            progressBar.update(3, { currentTask: 'Migrando Bancos...' });
            await migrarBancos(connection, dataPersonal);

            progressBar.update(4, { currentTask: 'Migrando Centro de Costos...' });
            await migrarCentroCostos(connection, dataPersonal);

            progressBar.update(5, { currentTask: 'Migrando Tablas Generales...' });
            await migrarTablasGenerales(connection, dataPersonal);

            progressBar.update(6, { currentTask: 'Migrando Puestos...' });
            await migrarPuestos(connection, dataPersonal);

            progressBar.update(7, { currentTask: 'Migrando Familiares...' });
            await migrarFamiliares(connection, dataPersonal);

            progressBar.update(8, { currentTask: 'Actualizando Niveles en Personal...' });
            await migrarNivelesPersonal(connection, dataPersonal);

            progressBar.update(totalTasks, { currentTask: 'Completado!' });
        } catch (error) {
            console.error('Error durante la migración:', error);
            throw error;
        }

        progressBar.stop();
    } finally {
        if (connection) await connection.end();
    }
}

main().catch(console.error);