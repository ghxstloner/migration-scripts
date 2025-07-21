const mysql = require('mysql2/promise');
const xlsx = require('xlsx');
const dbConfig = require('./dbconfig');
const cliProgress = require('cli-progress');
require('dotenv').config();

function extractNumericValue(str) {
    if (!str) return null;
    const matches = str.match(/\d+/);
    return matches ? parseInt(matches[0], 10) : null;
}

// Función para normalizar texto (manejar acentos)
function normalizeText(text) {
    if (!text) return '';
    return text.toString().trim()
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .replace(/ñ/gi, 'n')
        .replace(/Ñ/gi, 'N')
        .replace(/[òóôõö]/gi, 'o')
        .replace(/[àáâãäå]/gi, 'a')
        .replace(/[èéêë]/gi, 'e')
        .replace(/[ìíîï]/gi, 'i')
        .replace(/[ùúûü]/gi, 'u')
        .toUpperCase();
}

function formatPosicionMEF(posicion) {
    if (!posicion) return '9999';
    return posicion.toString().padStart(4, '0');
}

async function actualizarEstructuraOrganizacional(connection, dataEstructura) {
    console.log("\n=== Actualizando Estructura Organizacional (SIN ELIMINAR) ===");

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

    await connection.execute('SET FOREIGN_KEY_CHECKS=0');

    // Obtener registros existentes para cada nivel
    const [existingNivel1] = await connection.execute('SELECT codorg, descrip FROM nomnivel1');
    const [existingNivel2] = await connection.execute('SELECT codorg, descrip FROM nomnivel2');
    const [existingNivel3] = await connection.execute('SELECT codorg, descrip FROM nomnivel3');
    const [existingNivel4] = await connection.execute('SELECT codorg, descrip FROM nomnivel4');
    const [existingNivel5] = await connection.execute('SELECT codorg, descrip FROM nomnivel5');

    // Crear mapas de registros existentes
    const existingNivel1Map = new Map();
    const existingNivel2Map = new Map();
    const existingNivel3Map = new Map();
    const existingNivel4Map = new Map();
    const existingNivel5Map = new Map();

    existingNivel1.forEach(item => existingNivel1Map.set(item.descrip, item.codorg));
    existingNivel2.forEach(item => existingNivel2Map.set(item.descrip, item.codorg));
    existingNivel3.forEach(item => existingNivel3Map.set(item.descrip, item.codorg));
    existingNivel4.forEach(item => existingNivel4Map.set(item.descrip, item.codorg));
    existingNivel5.forEach(item => existingNivel5Map.set(item.descrip, item.codorg));

    // Procesar nivel 1 (VP) - Solo insertar nuevos
    const nivel1Set = new Set();
    let maxCodorg1 = existingNivel1.length > 0 ? Math.max(...existingNivel1.map(n => n.codorg)) : 0;

    for (const row of dataEstructura) {
        const vp = row.VP?.trim();
        if (vp && !nivel1Set.has(vp) && !existingNivel1Map.has(vp)) {
            nivel1Set.add(vp);
            maxCodorg1++;
            const { codigo, descripcionCorta } = extractCodigoYDescripcion(vp);

            await connection.execute(
                'INSERT INTO nomnivel1 (codorg, descrip, descripcion_corta) VALUES (?, ?, ?)',
                [maxCodorg1, vp, descripcionCorta]
            );

            existingNivel1Map.set(vp, maxCodorg1);
        }
    }

    // Procesar nivel 2 (Departamento) - Solo insertar nuevos
    const nivel2Set = new Set();
    let maxCodorg2 = existingNivel2.length > 0 ? Math.max(...existingNivel2.map(n => n.codorg)) : 0;

    for (const row of dataEstructura) {
        const dep = row.Departamento?.trim();
        if (dep && !nivel2Set.has(dep) && !existingNivel2Map.has(dep)) {
            nivel2Set.add(dep);
            maxCodorg2++;
            const { codigo, descripcionCorta } = extractCodigoYDescripcion(dep);
            const vpParent = row.VP?.trim();
            const gerencia = vpParent ? existingNivel1Map.get(vpParent) : null;

            await connection.execute(
                'INSERT INTO nomnivel2 (codorg, descrip, gerencia, descripcion_corta) VALUES (?, ?, ?, ?)',
                [maxCodorg2, dep, gerencia, descripcionCorta]
            );

            existingNivel2Map.set(dep, maxCodorg2);
        }
    }

    // Procesar nivel 3 (Sección) - Solo insertar nuevos
    const nivel3Set = new Set();
    let maxCodorg3 = existingNivel3.length > 0 ? Math.max(...existingNivel3.map(n => n.codorg)) : 0;

    for (const row of dataEstructura) {
        const sec = row.Seccion?.trim();
        if (sec && !nivel3Set.has(sec) && !existingNivel3Map.has(sec)) {
            nivel3Set.add(sec);
            maxCodorg3++;
            const { codigo, descripcionCorta } = extractCodigoYDescripcion(sec);
            const depParent = row.Departamento?.trim();
            const gerencia = depParent ? existingNivel2Map.get(depParent) : null;

            await connection.execute(
                'INSERT INTO nomnivel3 (codorg, descrip, gerencia, descripcion_corta) VALUES (?, ?, ?, ?)',
                [maxCodorg3, sec, gerencia, descripcionCorta]
            );

            existingNivel3Map.set(sec, maxCodorg3);
        }
    }

    // Procesar nivel 4 (Equipo) - Solo insertar nuevos
    const nivel4Set = new Set();
    let maxCodorg4 = existingNivel4.length > 0 ? Math.max(...existingNivel4.map(n => n.codorg)) : 0;

    for (const row of dataEstructura) {
        const eq = row.Equipo?.trim();
        if (eq && !nivel4Set.has(eq) && !existingNivel4Map.has(eq)) {
            nivel4Set.add(eq);
            maxCodorg4++;
            const { codigo, descripcionCorta } = extractCodigoYDescripcion(eq);
            const secParent = row.Seccion?.trim();
            const gerencia = secParent ? existingNivel3Map.get(secParent) : null;

            await connection.execute(
                'INSERT INTO nomnivel4 (codorg, descrip, gerencia, descripcion_corta) VALUES (?, ?, ?, ?)',
                [maxCodorg4, eq, gerencia, descripcionCorta]
            );

            existingNivel4Map.set(eq, maxCodorg4);
        }
    }

    // Procesar nivel 5 (Grupo) - Solo insertar nuevos
    const nivel5Set = new Set();
    let maxCodorg5 = existingNivel5.length > 0 ? Math.max(...existingNivel5.map(n => n.codorg)) : 0;

    for (const row of dataEstructura) {
        const grp = row.Grupo?.trim();
        if (grp && !nivel5Set.has(grp) && !existingNivel5Map.has(grp)) {
            nivel5Set.add(grp);
            maxCodorg5++;
            const { codigo, descripcionCorta } = extractCodigoYDescripcion(grp);
            const eqParent = row.Equipo?.trim();
            const gerencia = eqParent ? existingNivel4Map.get(eqParent) : null;

            await connection.execute(
                'INSERT INTO nomnivel5 (codorg, descrip, gerencia) VALUES (?, ?, ?)',
                [maxCodorg5, grp, gerencia]
            );
        }
    }

    await connection.execute('SET FOREIGN_KEY_CHECKS=1');
    
    console.log(`✅ Estructura organizacional actualizada:`);
    console.log(`   Nivel 1 - Nuevos insertados: ${nivel1Set.size}`);
    console.log(`   Nivel 2 - Nuevos insertados: ${nivel2Set.size}`);
    console.log(`   Nivel 3 - Nuevos insertados: ${nivel3Set.size}`);
    console.log(`   Nivel 4 - Nuevos insertados: ${nivel4Set.size}`);
    console.log(`   Nivel 5 - Nuevos insertados: ${nivel5Set.size}`);
}

async function actualizarNivelesPersonal(connection, dataPersonal) {
    console.log("\n=== Actualizando Niveles en Personal ===");

    await connection.execute('SET FOREIGN_KEY_CHECKS=0');

    try {
        // Crear tabla temporal con la misma collation que las tablas principales
        await connection.execute(`
            CREATE TEMPORARY TABLE temp_niveles_personal (
                numero_carnet VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
                vp VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
                departamento VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
                seccion VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
                equipo VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
                grupo VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci
        `);

        // Insertar datos en la tabla temporal
        const insertTempQuery = `
            INSERT INTO temp_niveles_personal 
            (numero_carnet, vp, departamento, seccion, equipo, grupo) 
            VALUES ?
        `;

        const tempData = dataPersonal
            .map(row => [
                row.Personal?.toString().trim() || null,
                row.Vicepresidencia?.toString().trim() || null,
                row.Departamento?.toString().trim() || null,
                row.Secciones?.toString().trim() || null,
                row.Equipo?.toString().trim() || null,
                row.Grupo?.toString().trim() || null
            ])
            .filter(row => row[0]);

        if (tempData.length > 0) {
            for (let i = 0; i < tempData.length; i += 1000) {
                await connection.query(insertTempQuery, [tempData.slice(i, i + 1000)]);
            }
        }

        // Actualizar nompersonal - Query simplificado sin CONVERT
        const updateQuery = `
            UPDATE nompersonal np
            LEFT JOIN temp_niveles_personal tnp 
                ON TRIM(np.numero_carnet) = TRIM(tnp.numero_carnet)
            LEFT JOIN nomnivel1 n1 
                ON tnp.vp = n1.descrip
            LEFT JOIN nomnivel2 n2 
                ON tnp.departamento = n2.descrip
            LEFT JOIN nomnivel3 n3 
                ON tnp.seccion = n3.descrip
            LEFT JOIN nomnivel4 n4 
                ON tnp.equipo = n4.descrip
            LEFT JOIN nomnivel5 n5 
                ON tnp.grupo = n5.descrip
            SET 
                np.codnivel1 = n1.codorg,
                np.codnivel2 = n2.codorg,
                np.codnivel3 = n3.codorg,
                np.codnivel4 = n4.codorg,
                np.codnivel5 = n5.codorg
            WHERE tnp.numero_carnet IS NOT NULL
        `;

        const [result] = await connection.execute(updateQuery);
        console.log(`✅ Niveles actualizados: ${result.affectedRows} registros`);

        await connection.execute('DROP TEMPORARY TABLE IF EXISTS temp_niveles_personal');

    } catch (error) {
        console.error('Error en actualizarNivelesPersonal:', error);
        throw error;
    } finally {
        await connection.execute('SET FOREIGN_KEY_CHECKS=1');
    }
}

async function actualizarPuestos(connection, personalData) {
    console.log("\n=== Actualizando Puestos en nomfuncion ===");

    try {
        // Obtener puestos existentes
        const [existingPuestos] = await connection.execute(
            'SELECT nomfuncion_id, descripcion_funcion FROM nomfuncion ORDER BY nomfuncion_id'
        );
        
        // Crear mapa de puestos existentes (normalizado)
        const existingMap = new Map();
        const duplicatesMap = new Map();
        
        existingPuestos.forEach(p => {
            const normalized = normalizeText(p.descripcion_funcion);
            if (!duplicatesMap.has(normalized)) {
                duplicatesMap.set(normalized, []);
            }
            duplicatesMap.get(normalized).push({
                id: p.nomfuncion_id,
                original: p.descripcion_funcion
            });
        });
        
        // Usar el ID más antiguo para cada puesto normalizado
        duplicatesMap.forEach((variants, normalized) => {
            const oldest = variants.reduce((min, current) => 
                parseInt(current.id) < parseInt(min.id) ? current : min
            );
            existingMap.set(normalized, oldest);
        });

        // Recopilar puestos únicos del personal
        const puestosDelPersonal = new Set();
        personalData.forEach(row => {
            const puesto = row.Puesto?.toString().trim();
            if (puesto) {
                puestosDelPersonal.add(puesto.toUpperCase());
            }
        });

        // Insertar nuevos puestos
        const puestosParaInsertar = [];
        for (const puesto of puestosDelPersonal) {
            const puestoNormalizado = normalizeText(puesto);
            if (!existingMap.has(puestoNormalizado)) {
                puestosParaInsertar.push([puesto]);
            }
        }

        if (puestosParaInsertar.length > 0) {
            console.log(`💾 Insertando ${puestosParaInsertar.length} nuevos puestos`);
            
            const insertQuery = 'INSERT INTO nomfuncion (descripcion_funcion) VALUES ?';
            const batchSize = 1000;
            
            for (let i = 0; i < puestosParaInsertar.length; i += batchSize) {
                const batch = puestosParaInsertar.slice(i, i + batchSize);
                await connection.query(insertQuery, [batch]);
            }
        } else {
            console.log("✅ No hay nuevos puestos para insertar");
        }

        console.log("✅ Puestos actualizados");

    } catch (error) {
        console.error('Error en actualizarPuestos:', error);
        throw error;
    }
}

async function actualizarCamposEspecificos(connection, dataPersonal) {
    console.log("\n=== Actualizando Campos Específicos ===");

    try {
        // Mapear nomfuncion_id
        async function mapNomfuncionId(puesto) {
            if (!puesto) return null;
            
            const puestoNormalizado = normalizeText(puesto);
            
            const [allFunctions] = await connection.execute(
                'SELECT nomfuncion_id, descripcion_funcion FROM nomfuncion'
            );
            
            // Coincidencia exacta primero
            for (const func of allFunctions) {
                if (func.descripcion_funcion === puesto.toUpperCase()) {
                    return parseInt(func.nomfuncion_id) || null;
                }
            }
            
            // Coincidencia normalizada
            for (const func of allFunctions) {
                const funcNormalizada = normalizeText(func.descripcion_funcion);
                if (funcNormalizada === puestoNormalizado) {
                    return parseInt(func.nomfuncion_id) || null;
                }
            }
            
            return null;
        }

        // Crear tabla temporal con collation correcta
        await connection.execute(`
            CREATE TEMPORARY TABLE temp_actualizacion_campos (
                numero_carnet VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
                suesal DECIMAL(10,2),
                sueldopro DECIMAL(10,2),
                salario_diario DECIMAL(10,2),
                rata_x_hr DECIMAL(10,2),
                gastos_representacion DECIMAL(10,2),
                gasto_rep_diario DECIMAL(10,2),
                rata_hora_gasto_rep DECIMAL(10,2),
                cuentacob VARCHAR(50),
                forcob VARCHAR(50),
                nomposicion_id VARCHAR(4),
                nomfuncion_id INT,
                marca_reloj TINYINT DEFAULT 1,
                PRIMARY KEY (numero_carnet)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci
        `);

        // Preparar datos
        console.log("🔄 Procesando datos...");
        const tempData = await Promise.all(dataPersonal.map(async row => {
            const numeroCarnet = row.Personal?.toString().trim();
            if (!numeroCarnet) return null;

            const sueldoMensual = parseFloat(row.SueldoMensual) || 0;
            const sueldoDiario = parseFloat(row.SueldoDiario) || 0;
            const rataHora = parseFloat(row.RataHora) || 0;
            const gastoRep = parseFloat(row.GR) || 0;
            const gastoRepDiario = parseFloat(row.GastoRepresentacionDiario) || 0;
            const rataHoraGR = parseFloat(row.RataHoraGR) || 0;
            
            const funcionId = await mapNomfuncionId(row.Puesto);

            return [
                numeroCarnet,
                sueldoMensual,              // suesal
                sueldoMensual,              // sueldopro
                sueldoDiario,               // salario_diario
                rataHora,                   // rata_x_hr
                gastoRep,                   // gastos_representacion
                gastoRepDiario,             // gasto_rep_diario
                rataHoraGR,                 // rata_hora_gasto_rep
                row.PersonalCuenta || null, // cuentacob
                row.FormaPago || null,      // forcob
                formatPosicionMEF(row.PosicionMEF), // nomposicion_id
                funcionId,                  // nomfuncion_id
                1                           // marca_reloj
            ];
        }));

        const filteredData = tempData.filter(row => row !== null);

        // Insertar en tabla temporal
        console.log(`💾 Insertando ${filteredData.length} registros en tabla temporal...`);
        const batchSize = 1000;
        for (let i = 0; i < filteredData.length; i += batchSize) {
            const batch = filteredData.slice(i, i + batchSize);
            await connection.query(`
                INSERT INTO temp_actualizacion_campos (
                    numero_carnet, suesal, sueldopro, salario_diario, rata_x_hr,
                    gastos_representacion, gasto_rep_diario, rata_hora_gasto_rep,
                    cuentacob, forcob, nomposicion_id, nomfuncion_id, marca_reloj
                ) VALUES ?
            `, [batch]);
        }

        // Actualizar nompersonal - Sin CONVERT
        console.log("🔄 Actualizando nompersonal...");
        const [updateResult] = await connection.execute(`
            UPDATE nompersonal np
            JOIN temp_actualizacion_campos tac ON TRIM(np.numero_carnet) = TRIM(tac.numero_carnet)
            SET 
                np.suesal = tac.suesal,
                np.sueldopro = tac.sueldopro,
                np.salario_diario = tac.salario_diario,
                np.rata_x_hr = tac.rata_x_hr,
                np.gastos_representacion = tac.gastos_representacion,
                np.gasto_rep_diario = tac.gasto_rep_diario,
                np.rata_hora_gasto_rep = tac.rata_hora_gasto_rep,
                np.cuentacob = tac.cuentacob,
                np.forcob = tac.forcob,
                np.nomposicion_id = tac.nomposicion_id,
                np.nomfuncion_id = tac.nomfuncion_id,
                np.marca_reloj = tac.marca_reloj
        `);

        console.log(`✅ Personal actualizado: ${updateResult.affectedRows} registros`);

        // Verificaciones
        const [verificacion] = await connection.execute(`
            SELECT 
                COUNT(*) as total_actualizados,
                COUNT(np.nomfuncion_id) as con_funcion,
                COUNT(CASE WHEN np.marca_reloj = 1 THEN 1 END) as con_marca_reloj
            FROM nompersonal np
            JOIN temp_actualizacion_campos tac ON TRIM(np.numero_carnet) = TRIM(tac.numero_carnet)
        `);
        
        console.log(`📊 Verificación:`);
        console.log(`   Total actualizados: ${verificacion[0].total_actualizados}`);
        console.log(`   Con nomfuncion_id: ${verificacion[0].con_funcion}`);
        console.log(`   Con marca_reloj = 1: ${verificacion[0].con_marca_reloj}`);

        // Limpiar tabla temporal
        await connection.execute('DROP TEMPORARY TABLE temp_actualizacion_campos');

    } catch (error) {
        console.error('Error en actualizarCamposEspecificos:', error);
        throw error;
    }
}

async function main() {
    let connection;
    try {
        connection = await mysql.createConnection({
            ...dbConfig,
            connectTimeout: 60000
        });

        console.log("=== MIGRACIÓN DE ACTUALIZACIÓN ESPECÍFICA ===");
        console.log("Campos a actualizar:");
        console.log("- Salarios (suesal, sueldopro, salario_diario, rata_x_hr)");
        console.log("- Gastos de representación");
        console.log("- Estructura organizacional (nomnivel1-5) - SOLO NUEVOS");
        console.log("- cuentacob, forcob, nomposicion_id");
        console.log("- nomfuncion");
        console.log("- marca_reloj = 1");

        // Leer archivos
        console.log("\n📁 Leyendo archivos...");
        
        const workbookPersonal = xlsx.readFile('formatos/Personal_Al_18072025.xlsx');
        const sheetPersonal = workbookPersonal.Sheets[workbookPersonal.SheetNames[0]];
        const dataPersonal = xlsx.utils.sheet_to_json(sheetPersonal);
        console.log(`   Personal: ${dataPersonal.length} registros`);

        const workbookEstructura = xlsx.readFile('formatos/EstructuraOrganizacional.xlsx');
        const sheetEstructura = workbookEstructura.Sheets[workbookEstructura.SheetNames[0]];
        const dataEstructura = xlsx.utils.sheet_to_json(sheetEstructura);
        console.log(`   Estructura: ${dataEstructura.length} registros`);

        const progressBar = new cliProgress.SingleBar({
            format: 'Progreso |{bar}| {percentage}% || {value}/{total} Módulos || {currentTask}',
            barCompleteChar: '\u2588',
            barIncompleteChar: '\u2591',
            hideCursor: true
        });

        const totalTasks = 4;
        progressBar.start(totalTasks, 0, { currentTask: 'Iniciando...' });

        try {
            // 1. Actualizar estructura organizacional (sin eliminar)
            progressBar.update(1, { currentTask: 'Actualizando Estructura Organizacional (solo nuevos)...' });
            await actualizarEstructuraOrganizacional(connection, dataEstructura);

            // 2. Actualizar puestos
            progressBar.update(2, { currentTask: 'Actualizando Puestos...' });
            await actualizarPuestos(connection, dataPersonal);

            // 3. Actualizar niveles en personal
            progressBar.update(3, { currentTask: 'Actualizando Niveles en Personal...' });
            await actualizarNivelesPersonal(connection, dataPersonal);

            // 4. Actualizar campos específicos
            progressBar.update(4, { currentTask: 'Actualizando Campos Específicos...' });
            await actualizarCamposEspecificos(connection, dataPersonal);

            progressBar.update(totalTasks, { currentTask: 'Completado!' });

        } catch (error) {
            progressBar.stop();
            console.error('Error durante la migración:', error);
            throw error;
        }

        progressBar.stop();
        
        console.log("\n=== MIGRACIÓN COMPLETADA ===");
        console.log("✅ Estructura organizacional actualizada (solo nuevos niveles)");
        console.log("✅ Puestos actualizados en nomfuncion");
        console.log("✅ Niveles organizacionales actualizados");
        console.log("✅ Salarios y campos específicos actualizados");
        console.log("✅ Todos los registros con marca_reloj = 1");
        console.log("✅ NO se eliminaron registros existentes");

    } finally {
        if (connection) await connection.end();
    }
}

main().catch(console.error);