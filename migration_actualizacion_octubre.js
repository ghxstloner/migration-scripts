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

function formatPosicionMEF(posicion) {
    if (!posicion) return '9999';
    return posicion.toString().padStart(4, '0');
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

// Función para formatear nombres según formato Intelesis
function formatearNombresIntelesis(apellidoPaterno, apellidoMaterno, nombre) {
    if (!apellidoPaterno && !apellidoMaterno && !nombre) return null;
    
    // Limpiar y normalizar datos
    const apellidoP = (apellidoPaterno || '').toString().trim();
    const apellidoM = (apellidoMaterno || '').toString().trim();
    const nombreCompleto = (nombre || '').toString().trim();
    
    // Extraer partes del nombre
    const partesNombre = nombreCompleto.split(' ').filter(parte => parte.trim());
    
    let primerNombre = '';
    let segundoNombre = '';
    let inicialSegundoNombre = '';
    
    if (partesNombre.length > 0) {
        primerNombre = partesNombre[0];
    }
    if (partesNombre.length > 1) {
        segundoNombre = partesNombre[1];
        inicialSegundoNombre = segundoNombre.charAt(0).toUpperCase();
    }
    
    // Construir apellidos con inicial del segundo apellido
    let apellidosCompletos = apellidoP;
    if (apellidoM) {
        const inicialSegundoApellido = apellidoM.charAt(0).toUpperCase();
        apellidosCompletos += ` ${inicialSegundoApellido}`;
    }
    
    // Construir nombre completo con inicial del segundo nombre
    let nombreFormateado = primerNombre;
    if (inicialSegundoNombre) {
        nombreFormateado += ` ${inicialSegundoNombre}`;
    }
    
    // Construir apenom en formato: "Apellidos, Nombres"
    const apenom = `${apellidosCompletos}, ${nombreFormateado}`;
    
    return {
        apellidos: apellidosCompletos,
        apellido_materno: apellidoM,
        nombres: primerNombre,
        nombres2: segundoNombre,
        apenom: apenom
    };
}

// Función para mapear nivel académico
async function mapNivelAcademico(connection, nivelAcademico) {
    // Si el nivel es null, undefined, "NULL", o vacío, retornar null
    if (!nivelAcademico || nivelAcademico === 'NULL' || nivelAcademico.toString().trim() === '' || nivelAcademico.toString().trim() === 'NULL') {
        return null;
    }
    
    const nivel = nivelAcademico.toString().trim();
    
    // Primero buscar coincidencia exacta
    const [exactMatch] = await connection.execute(
        'SELECT IdNivelEducativo FROM niveleducativo WHERE Descripcion = ?',
        [nivel]
    );
    
    if (exactMatch.length > 0) {
        return exactMatch[0].IdNivelEducativo;
    }
    
    // Si no hay coincidencia exacta, buscar coincidencia normalizada
    const [allNiveles] = await connection.execute(
        'SELECT IdNivelEducativo, Descripcion FROM niveleducativo'
    );
    
    const nivelNormalizado = normalizeText(nivel);
    
    for (const n of allNiveles) {
        const descripcionNormalizada = normalizeText(n.Descripcion);
        if (descripcionNormalizada === nivelNormalizado) {
            return n.IdNivelEducativo;
        }
    }
    
    // Si no existe, crear nuevo nivel académico
    console.log(`🆕 Creando nuevo nivel académico: "${nivel}"`);
    const [result] = await connection.execute(
        'INSERT INTO niveleducativo (Descripcion) VALUES (?)',
        [nivel]
    );
    
    return result.insertId;
}

// Función para mapear niveles organizacionales
async function mapNivelOrganizacional(connection, nivel, tabla, campoDescripcion, ficha = null) {
    // Si el nivel es null, undefined, "NULL", o vacío, retornar null sin logs
    if (!nivel || nivel === 'NULL' || nivel.toString().trim() === '' || nivel.toString().trim() === 'NULL') {
        return null;
    }
    
    const nivelStr = nivel.toString().trim();
    
    // Buscar coincidencia exacta
    const [exactMatch] = await connection.execute(
        `SELECT codorg FROM ${tabla} WHERE ${campoDescripcion} = ?`,
        [nivelStr]
    );
    
    if (exactMatch.length > 0) {
        return exactMatch[0].codorg;
    }
    
    // Buscar coincidencia normalizada
    const [allNiveles] = await connection.execute(
        `SELECT codorg, ${campoDescripcion} FROM ${tabla}`
    );
    
    const nivelNormalizado = normalizeText(nivelStr);
    
    for (const n of allNiveles) {
        const descripcionNormalizada = normalizeText(n[campoDescripcion]);
        if (descripcionNormalizada === nivelNormalizado) {
            return n.codorg;
        }
    }
    
    // Log detallado cuando no se encuentra
    const fichaInfo = ficha ? ` (Ficha: ${ficha})` : '';
    console.log(`⚠️  NO ENCONTRADO en ${tabla}: "${nivelStr}"${fichaInfo}`);
    console.log(`   📋 Niveles disponibles en ${tabla}:`);
    
    // Mostrar algunos ejemplos de niveles existentes
    const ejemplos = allNiveles.slice(0, 5);
    ejemplos.forEach(n => {
        console.log(`      - ${n.codorg}: "${n[campoDescripcion]}"`);
    });
    
    if (allNiveles.length > 5) {
        console.log(`      ... y ${allNiveles.length - 5} más`);
    }
    
    return null;
}

// Función para limpiar niveleducativo de registros basura
async function limpiarNivelEducativo(connection) {
    console.log("\n=== Limpiando niveleducativo de registros basura ===");
    
    try {
        // Eliminar registros con "NULL" o vacíos
        const [result1] = await connection.execute(
            'DELETE FROM niveleducativo WHERE Descripcion = "NULL" OR Descripcion = "" OR Descripcion IS NULL'
        );
        console.log(`🗑️  Eliminados registros con NULL/vacíos: ${result1.affectedRows}`);
        
        // Encontrar y eliminar duplicados, manteniendo el ID más bajo
        const [duplicados] = await connection.execute(`
            SELECT Descripcion, COUNT(*) as cantidad, MIN(IdNivelEducativo) as id_mantener
            FROM niveleducativo 
            GROUP BY Descripcion 
            HAVING COUNT(*) > 1
        `);
        
        if (duplicados.length > 0) {
            console.log(`🔄 Encontrados ${duplicados.length} grupos de duplicados:`);
            
            for (const dup of duplicados) {
                console.log(`   "${dup.Descripcion}": ${dup.cantidad} registros (manteniendo ID ${dup.id_mantener})`);
                
                // Eliminar duplicados excepto el de menor ID
                const [result2] = await connection.execute(
                    'DELETE FROM niveleducativo WHERE Descripcion = ? AND IdNivelEducativo != ?',
                    [dup.Descripcion, dup.id_mantener]
                );
                console.log(`   ✅ Eliminados ${result2.affectedRows} duplicados`);
            }
        } else {
            console.log("✅ No se encontraron duplicados");
        }
        
        // Mostrar estado final
        const [estadoFinal] = await connection.execute(
            'SELECT COUNT(*) as total FROM niveleducativo'
        );
        console.log(`📊 Total registros en niveleducativo: ${estadoFinal[0].total}`);
        
    } catch (error) {
        console.error('Error en limpiarNivelEducativo:', error);
        throw error;
    }
}

async function actualizarNombres(connection, dataPersonal) {
    console.log("\n=== Actualizando Nombres (Formato Intelesis) ===");

    try {
        // Crear tabla temporal
        await connection.execute(`
            CREATE TEMPORARY TABLE temp_nombres (
                ficha INT,
                apellidos VARCHAR(100),
                apellido_materno VARCHAR(100),
                nombres VARCHAR(100),
                nombres2 VARCHAR(100),
                apenom VARCHAR(200),
                PRIMARY KEY (ficha)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci
        `);

        // Preparar datos
        console.log("🔄 Procesando nombres...");
        const tempData = dataPersonal.map(row => {
            const ficha = extractNumericValue(row.Personal);
            if (!ficha) return null;

            const nombresFormateados = formatearNombresIntelesis(
                row.ApellidoPaterno,
                row.ApellidoMaterno,
                row.Nombre
            );

            if (!nombresFormateados) return null;

            return [
                ficha,
                nombresFormateados.apellidos,
                nombresFormateados.apellido_materno,
                nombresFormateados.nombres,
                nombresFormateados.nombres2,
                nombresFormateados.apenom
            ];
        });

        const filteredData = tempData.filter(row => row !== null);

        // Insertar en tabla temporal
        console.log(`💾 Insertando ${filteredData.length} registros de nombres...`);
        const batchSize = 1000;
        for (let i = 0; i < filteredData.length; i += batchSize) {
            const batch = filteredData.slice(i, i + batchSize);
            await connection.query(`
                INSERT INTO temp_nombres (
                    ficha, apellidos, apellido_materno, nombres, nombres2, apenom
                ) VALUES ?
            `, [batch]);
        }

        // Actualizar nompersonal
        console.log("🔄 Actualizando nombres...");
        const [updateResult] = await connection.execute(`
            UPDATE nompersonal np
            JOIN temp_nombres tn ON np.ficha = tn.ficha
            SET 
                np.apellidos = tn.apellidos,
                np.apellido_materno = tn.apellido_materno,
                np.nombres = tn.nombres,
                np.nombres2 = tn.nombres2,
                np.apenom = tn.apenom
        `);

        console.log(`✅ Nombres actualizados: ${updateResult.affectedRows} registros`);

        // Verificaciones
        const [verificacion] = await connection.execute(`
            SELECT 
                COUNT(*) as total_actualizados,
                COUNT(np.apenom) as con_apenom,
                COUNT(CASE WHEN np.nombres2 IS NOT NULL AND np.nombres2 != '' THEN 1 END) as con_segundo_nombre
            FROM nompersonal np
            JOIN temp_nombres tn ON np.ficha = tn.ficha
        `);
        
        console.log(`📊 Verificación nombres:`);
        console.log(`   Total actualizados: ${verificacion[0].total_actualizados}`);
        console.log(`   Con apenom: ${verificacion[0].con_apenom}`);
        console.log(`   Con segundo nombre: ${verificacion[0].con_segundo_nombre}`);

        // Mostrar algunos ejemplos
        const [ejemplos] = await connection.execute(`
            SELECT np.ficha, np.apellidos, np.nombres, np.apenom 
            FROM nompersonal np
            JOIN temp_nombres tn ON np.ficha = tn.ficha
            LIMIT 5
        `);
        
        console.log(`\n📋 Ejemplos de nombres formateados:`);
        ejemplos.forEach(ej => {
            console.log(`   Ficha ${ej.ficha}: "${ej.apenom}"`);
        });

        // Limpiar tabla temporal
        await connection.execute('DROP TEMPORARY TABLE temp_nombres');

    } catch (error) {
        console.error('Error en actualizarNombres:', error);
        throw error;
    }
}

async function actualizarCamposSalariales(connection, dataPersonal) {
    console.log("\n=== Actualizando Campos Salariales y Académicos ===");

    try {
        // Crear tabla temporal
        await connection.execute(`
            CREATE TEMPORARY TABLE temp_campos_salariales (
                ficha INT,
                ISRFijoPeriodo DECIMAL(10,2),
                suesal DECIMAL(10,2),
                sueldopro DECIMAL(10,2),
                salario_diario DECIMAL(10,2),
                rata_x_hr DECIMAL(10,2),
                gastos_representacion DECIMAL(10,2),
                gasto_rep_diario DECIMAL(10,2),
                rata_hora_gasto_rep DECIMAL(10,2),
                IdNivelEducativo INT,
                nomposicion_id VARCHAR(4),
                PRIMARY KEY (ficha)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci
        `);

        // Preparar datos
        console.log("🔄 Procesando datos salariales y académicos...");
        const tempData = await Promise.all(dataPersonal.map(async row => {
            const ficha = extractNumericValue(row.Personal);
            if (!ficha) return null;

            const ISRFijoPeriodo = parseFloat(row.ISRFijoPeriodo) || 0;
            const sueldoMensual = parseFloat(row.SueldoMensual) || 0;
            const sueldoDiario = parseFloat(row.SueldoDiario) || 0;
            const rataHora = parseFloat(row.RataHora) || 0;
            const gastoRep = parseFloat(row.GR) || 0;
            const gastoRepDiario = parseFloat(row.GastoRepresentacionDiario) || 0;
            const rataHoraGR = parseFloat(row.RataHoraGR) || 0;
            
            const nivelAcademicoId = await mapNivelAcademico(connection, row.NivelAcademico);
            const nomposicionId = formatPosicionMEF(row.PosicionMEF);

            return [
                ficha,
                ISRFijoPeriodo,
                sueldoMensual,              // suesal
                sueldoMensual,               // sueldopro
                sueldoDiario,                // salario_diario
                rataHora,                    // rata_x_hr
                gastoRep,                    // gastos_representacion
                gastoRepDiario,             // gasto_rep_diario
                rataHoraGR,                 // rata_hora_gasto_rep
                nivelAcademicoId,           // IdNivelEducativo
                nomposicionId               // nomposicion_id
            ];
        }));

        const filteredData = tempData.filter(row => row !== null);

        // Insertar en tabla temporal
        console.log(`💾 Insertando ${filteredData.length} registros en tabla temporal...`);
        const batchSize = 1000;
        for (let i = 0; i < filteredData.length; i += batchSize) {
            const batch = filteredData.slice(i, i + batchSize);
            await connection.query(`
                INSERT INTO temp_campos_salariales (
                    ficha, ISRFijoPeriodo, suesal, sueldopro, salario_diario, rata_x_hr,
                    gastos_representacion, gasto_rep_diario, rata_hora_gasto_rep, IdNivelEducativo, nomposicion_id
                ) VALUES ?
            `, [batch]);
        }

        // Actualizar nompersonal
        console.log("🔄 Actualizando campos salariales y académicos...");
        const [updateResult] = await connection.execute(`
            UPDATE nompersonal np
            JOIN temp_campos_salariales tcs ON np.ficha = tcs.ficha
            SET 
                np.ISRFijoPeriodo = tcs.ISRFijoPeriodo,
                np.suesal = tcs.suesal,
                np.sueldopro = tcs.sueldopro,
                np.salario_diario = tcs.salario_diario,
                np.rata_x_hr = tcs.rata_x_hr,
                np.gastos_representacion = tcs.gastos_representacion,
                np.gasto_rep_diario = tcs.gasto_rep_diario,
                np.rata_hora_gasto_rep = tcs.rata_hora_gasto_rep,
                np.IdNivelEducativo = tcs.IdNivelEducativo,
                np.nomposicion_id = tcs.nomposicion_id
        `);

        console.log(`✅ Campos salariales y académicos actualizados: ${updateResult.affectedRows} registros`);

        // Verificaciones
        const [verificacion] = await connection.execute(`
            SELECT 
                COUNT(*) as total_actualizados,
                COUNT(np.IdNivelEducativo) as con_nivel_academico,
                COUNT(CASE WHEN np.suesal > 0 THEN 1 END) as con_sueldo,
                COUNT(CASE WHEN np.nomposicion_id IS NOT NULL AND np.nomposicion_id != '9999' THEN 1 END) as con_posicion_mef
            FROM nompersonal np
            JOIN temp_campos_salariales tcs ON np.ficha = tcs.ficha
        `);
        
        console.log(`📊 Verificación:`);
        console.log(`   Total actualizados: ${verificacion[0].total_actualizados}`);
        console.log(`   Con nivel académico: ${verificacion[0].con_nivel_academico}`);
        console.log(`   Con sueldo > 0: ${verificacion[0].con_sueldo}`);
        console.log(`   Con posición MEF válida: ${verificacion[0].con_posicion_mef}`);

        // Limpiar tabla temporal
        await connection.execute('DROP TEMPORARY TABLE temp_campos_salariales');

    } catch (error) {
        console.error('Error en actualizarCamposSalariales:', error);
        throw error;
    }
}

// Función para extraer código y descripción de departamentos
function extractCodigoYDescripcionDepartamento(departamento) {
    if (!departamento || departamento === 'NULL') return { codigo: null, descripcion: null };
    
    // Patrones para extraer código
    const patterns = [
        /^(\d+)\s+(.+)$/,                    // "204 Centro de Gestion Operativa"
        /^(X\s+\d+)\s+(.+)$/,                // "X 201 Dep Finanzas"
        /^([A-Za-z]+)$/                       // "Estacionamientos"
    ];
    
    for (const pattern of patterns) {
        const match = departamento.match(pattern);
        if (match) {
            return {
                codigo: match[1],
                descripcion: match[2] || match[1]
            };
        }
    }
    
    // Si no coincide con ningún patrón, usar el texto completo
    return {
        codigo: null,
        descripcion: departamento
    };
}

// Función para normalizar nombre de nivel (quitar prefijo X)
function normalizarNombreNivel(nombre) {
    if (!nombre) return null;
    
    // Si empieza con "X ", quitar el prefijo
    if (nombre.startsWith('X ')) {
        return nombre.substring(2).trim();
    }
    
    return nombre.trim();
}

// Función para insertar niveles organizacionales faltantes (nivel 3, 4, 5)
async function insertarNivelesOrganizacionalesFaltantes(connection, dataPersonal, nivel) {
    const tabla = `nomnivel${nivel}`;
    const campoNivel = nivel === 3 ? 'Secciones' : nivel === 4 ? 'Equipo' : 'Grupo';
    
    console.log(`\n=== INSERTANDO ${tabla.toUpperCase()} FALTANTES ===`);
    
    try {
        // Obtener niveles existentes
        const [nivelesExistentes] = await connection.execute(
            `SELECT codorg, descrip FROM ${tabla} ORDER BY codorg`
        );
        
        // Crear mapa de niveles existentes
        const nivelesMap = new Map();
        nivelesExistentes.forEach(n => {
            nivelesMap.set(n.descrip, n.codorg);
        });
        
        // Obtener el máximo código existente
        const maxCodorg = nivelesExistentes.length > 0 
            ? Math.max(...nivelesExistentes.map(n => n.codorg)) 
            : 0;
        
        // Recopilar niveles únicos del Excel
        const nivelesDelExcel = new Set();
        dataPersonal.forEach(row => {
            const nivelValue = row[campoNivel]?.toString().trim();
            if (nivelValue && nivelValue !== 'NULL') {
                nivelesDelExcel.add(nivelValue);
            }
        });
        
        // Encontrar niveles no existentes
        const nivelesParaInsertar = [];
        nivelesDelExcel.forEach(nivelValue => {
            // Primero verificar si existe tal como está
            if (nivelesMap.has(nivelValue)) {
                return; // Ya existe, no hacer nada
            }
            
            // Si empieza con "X ", verificar si existe sin el prefijo
            if (nivelValue.startsWith('X ')) {
                const nombreSinPrefijo = normalizarNombreNivel(nivelValue);
                if (nivelesMap.has(nombreSinPrefijo)) {
                    return; // Existe sin prefijo, no crear duplicado
                }
            }
            
            // Si no existe, agregarlo para crear
            nivelesParaInsertar.push(nivelValue);
        });
        
        if (nivelesParaInsertar.length === 0) {
            console.log(`✅ Todos los ${tabla} del Excel ya existen en la base de datos`);
            return;
        }
        
        console.log(`🔄 Insertando ${nivelesParaInsertar.length} ${tabla} faltantes...`);
        
        let nuevoCodorg = maxCodorg;
        const nivelesInsertados = [];
        
        for (const nivelValue of nivelesParaInsertar) {
            nuevoCodorg++;
            
            // Normalizar el nombre del nivel (quitar prefijo X si existe)
            const nivelNormalizado = normalizarNombreNivel(nivelValue);
            const { codigo, descripcion } = extractCodigoYDescripcionDepartamento(nivelNormalizado);
            
            // Determinar la gerencia padre basada en el nivel
            let gerencia = null;
            if (nivel === 3) {
                // Para nivel 3, buscar el departamento padre
                const departamentoPadre = dataPersonal.find(row => 
                    row.Secciones === nivelValue
                )?.Departamento;
                
                if (departamentoPadre) {
                    const [departamentoResult] = await connection.execute(
                        'SELECT codorg FROM nomnivel2 WHERE descrip = ?',
                        [departamentoPadre]
                    );
                    if (departamentoResult.length > 0) {
                        gerencia = departamentoResult[0].codorg;
                    }
                }
            } else if (nivel === 4) {
                // Para nivel 4, buscar la sección padre
                const seccionPadre = dataPersonal.find(row => 
                    row.Equipo === nivelValue
                )?.Secciones;
                
                if (seccionPadre) {
                    const [seccionResult] = await connection.execute(
                        'SELECT codorg FROM nomnivel3 WHERE descrip = ?',
                        [seccionPadre]
                    );
                    if (seccionResult.length > 0) {
                        gerencia = seccionResult[0].codorg;
                    }
                }
            } else if (nivel === 5) {
                // Para nivel 5, buscar el equipo padre
                const equipoPadre = dataPersonal.find(row => 
                    row.Grupo === nivelValue
                )?.Equipo;
                
                if (equipoPadre) {
                    const [equipoResult] = await connection.execute(
                        'SELECT codorg FROM nomnivel4 WHERE descrip = ?',
                        [equipoPadre]
                    );
                    if (equipoResult.length > 0) {
                        gerencia = equipoResult[0].codorg;
                    }
                }
            }
            
            // Solo incluir descripcion_corta para nomnivel2 (departamentos)
            if (nivel === 2) {
                await connection.execute(
                    `INSERT INTO ${tabla} (codorg, descrip, gerencia, descripcion_corta) VALUES (?, ?, ?, ?)`,
                    [nuevoCodorg, nivelNormalizado, gerencia, descripcion]
                );
            } else {
                await connection.execute(
                    `INSERT INTO ${tabla} (codorg, descrip, gerencia) VALUES (?, ?, ?)`,
                    [nuevoCodorg, nivelNormalizado, gerencia]
                );
            }
            
            nivelesInsertados.push({
                codorg: nuevoCodorg,
                descrip: nivelNormalizado,
                gerencia: gerencia
            });
            
            // Actualizar el mapa para evitar duplicados
            nivelesMap.set(nivelValue, nuevoCodorg);
            nivelesMap.set(nivelNormalizado, nuevoCodorg);
        }
        
        console.log(`✅ ${tabla} insertados: ${nivelesInsertados.length}`);
        console.log(`📋 ${tabla} creados:`);
        nivelesInsertados.forEach((nivel, index) => {
            const gerenciaInfo = nivel.gerencia ? `Gerencia: ${nivel.gerencia}` : 'Sin gerencia padre';
            console.log(`   ${index + 1}. ${nivel.codorg}: "${nivel.descrip}" (${gerenciaInfo})`);
        });
        
        return nivelesInsertados;
        
    } catch (error) {
        console.error(`Error en insertar${tabla}Faltantes:`, error);
        throw error;
    }
}

// Función para insertar departamentos faltantes
// Nota: Los departamentos con códigos 1000+ son históricos y no tienen gerencia asociada
// Se mantienen para reportería y historial de colaboradores
async function insertarDepartamentosFaltantes(connection, dataPersonal) {
    console.log("\n=== INSERTANDO DEPARTAMENTOS FALTANTES ===");
    
    try {
        // Obtener departamentos existentes
        const [departamentosExistentes] = await connection.execute(
            'SELECT codorg, descrip FROM nomnivel2 ORDER BY codorg'
        );
        
        // Crear mapa de departamentos existentes
        const departamentosMap = new Map();
        departamentosExistentes.forEach(d => {
            departamentosMap.set(d.descrip, d.codorg);
        });
        
        // Obtener el máximo código existente
        const maxCodorg = departamentosExistentes.length > 0 
            ? Math.max(...departamentosExistentes.map(d => d.codorg)) 
            : 0;
        
        // Recopilar departamentos únicos del Excel
        const departamentosDelExcel = new Set();
        dataPersonal.forEach(row => {
            const departamento = row.Departamento?.toString().trim();
            if (departamento && departamento !== 'NULL') {
                departamentosDelExcel.add(departamento);
            }
        });
        
        // Encontrar departamentos no existentes
        const departamentosParaInsertar = [];
        departamentosDelExcel.forEach(departamento => {
            // Primero verificar si existe tal como está
            if (departamentosMap.has(departamento)) {
                return; // Ya existe, no hacer nada
            }
            
            // Si empieza con "X ", verificar si existe sin el prefijo
            if (departamento.startsWith('X ')) {
                const nombreSinPrefijo = normalizarNombreNivel(departamento);
                if (departamentosMap.has(nombreSinPrefijo)) {
                    return; // Existe sin prefijo, no crear duplicado
                }
            }
            
            // Si no existe, agregarlo para crear
            departamentosParaInsertar.push(departamento);
        });
        
        if (departamentosParaInsertar.length === 0) {
            console.log("✅ Todos los departamentos del Excel ya existen en la base de datos");
            return;
        }
        
        console.log(`🔄 Insertando ${departamentosParaInsertar.length} departamentos faltantes...`);
        
        let nuevoCodorg = maxCodorg;
        const departamentosInsertados = [];
        
        for (const departamento of departamentosParaInsertar) {
            nuevoCodorg++;
            
            // Normalizar el nombre del departamento (quitar prefijo X si existe)
            const departamentoNormalizado = normalizarNombreNivel(departamento);
            const { codigo, descripcion } = extractCodigoYDescripcionDepartamento(departamentoNormalizado);
            
            // Determinar la gerencia (VP) basada en el código del departamento
            let gerencia = null;
            if (codigo) {
                const codigoNum = parseInt(codigo.replace(/[^\d]/g, ''));
                if (codigoNum >= 100 && codigoNum < 200) gerencia = 1; // VP General
                else if (codigoNum >= 200 && codigoNum < 300) gerencia = 2; // VP Finanzas
                else if (codigoNum >= 300 && codigoNum < 400) gerencia = 3; // VP Ingeniería
                else if (codigoNum >= 400 && codigoNum < 500) gerencia = 4; // VP Recursos Humanos
                else if (codigoNum >= 500 && codigoNum < 600) gerencia = 5; // VP Administración
                else if (codigoNum >= 600 && codigoNum < 700) gerencia = 6; // VP Planificación
                else if (codigoNum >= 700 && codigoNum < 800) gerencia = 7; // VP Comercial
                else if (codigoNum >= 800 && codigoNum < 900) gerencia = 8; // VP Tecnología
                else if (codigoNum >= 900 && codigoNum < 1000) gerencia = 9; // VP Mantenimiento
                // Los departamentos 1000+ no tienen gerencia (son históricos)
                // Se mantienen para reportería pero sin VP asociada
            }
            
            await connection.execute(
                'INSERT INTO nomnivel2 (codorg, descrip, gerencia, descripcion_corta) VALUES (?, ?, ?, ?)',
                [nuevoCodorg, departamentoNormalizado, gerencia, descripcion]
            );
            
            departamentosInsertados.push({
                codorg: nuevoCodorg,
                descrip: departamentoNormalizado,
                gerencia: gerencia
            });
            
            // Actualizar el mapa para evitar duplicados
            departamentosMap.set(departamento, nuevoCodorg);
            departamentosMap.set(departamentoNormalizado, nuevoCodorg);
        }
        
        console.log(`✅ Departamentos insertados: ${departamentosInsertados.length}`);
        console.log("📋 Departamentos creados:");
        departamentosInsertados.forEach((dep, index) => {
            const gerenciaInfo = dep.gerencia ? `Gerencia: ${dep.gerencia}` : 'Histórico (sin gerencia)';
            console.log(`   ${index + 1}. ${dep.codorg}: "${dep.descrip}" (${gerenciaInfo})`);
        });
        
        return departamentosInsertados;
        
    } catch (error) {
        console.error('Error en insertarDepartamentosFaltantes:', error);
        throw error;
    }
}

// Función para crear resumen de departamentos no encontrados
async function crearResumenDepartamentosNoEncontrados(connection, dataPersonal) {
    console.log("\n=== RESUMEN DE DEPARTAMENTOS NO ENCONTRADOS ===");
    
    try {
        // Obtener todos los departamentos existentes
        const [departamentosExistentes] = await connection.execute(
            'SELECT codorg, descrip FROM nomnivel2 ORDER BY descrip'
        );
        
        // Crear mapa de departamentos existentes
        const departamentosMap = new Map();
        departamentosExistentes.forEach(d => {
            departamentosMap.set(d.descrip, d.codorg);
        });
        
        // Recopilar departamentos únicos del Excel
        const departamentosDelExcel = new Set();
        dataPersonal.forEach(row => {
            const departamento = row.Departamento?.toString().trim();
            if (departamento) {
                departamentosDelExcel.add(departamento);
            }
        });
        
        // Encontrar departamentos no existentes
        const departamentosNoEncontrados = [];
        departamentosDelExcel.forEach(departamento => {
            if (!departamentosMap.has(departamento)) {
                departamentosNoEncontrados.push(departamento);
            }
        });
        
        if (departamentosNoEncontrados.length > 0) {
            console.log(`❌ DEPARTAMENTOS NO ENCONTRADOS (${departamentosNoEncontrados.length}):`);
            departamentosNoEncontrados.forEach((dep, index) => {
                console.log(`   ${index + 1}. "${dep}"`);
            });
            
            console.log(`\n📋 DEPARTAMENTOS EXISTENTES EN LA BASE DE DATOS:`);
            departamentosExistentes.forEach((dep, index) => {
                console.log(`   ${index + 1}. ${dep.codorg}: "${dep.descrip}"`);
            });
        } else {
            console.log("✅ Todos los departamentos del Excel existen en la base de datos");
        }
        
        return departamentosNoEncontrados;
        
    } catch (error) {
        console.error('Error en crearResumenDepartamentosNoEncontrados:', error);
        return [];
    }
}

async function actualizarEstructuraOrganizacional(connection, dataPersonal) {
    console.log("\n=== Actualizando Estructura Organizacional ===");

    try {
        // Crear tabla temporal
        await connection.execute(`
            CREATE TEMPORARY TABLE temp_estructura_personal (
                ficha INT,
                vp VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
                departamento VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
                seccion VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
                equipo VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
                grupo VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
                codnivel1 INT,
                codnivel2 INT,
                codnivel3 INT,
                codnivel4 INT,
                codnivel5 INT,
                PRIMARY KEY (ficha)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci
        `);

        // Preparar datos
        console.log("🔄 Procesando estructura organizacional...");
        const tempData = await Promise.all(dataPersonal.map(async row => {
            const ficha = extractNumericValue(row.Personal);
            if (!ficha) return null;

            const vp = row.Vicepresidencia?.toString().trim();
            const departamento = row.Departamento?.toString().trim();
            const seccion = row.Secciones?.toString().trim();
            const equipo = row.Equipo?.toString().trim();
            const grupo = row.Grupo?.toString().trim();

            // Mapear niveles organizacionales
            const codnivel1 = await mapNivelOrganizacional(connection, vp, 'nomnivel1', 'descrip', ficha);
            const codnivel2 = await mapNivelOrganizacional(connection, departamento, 'nomnivel2', 'descrip', ficha);
            const codnivel3 = await mapNivelOrganizacional(connection, seccion, 'nomnivel3', 'descrip', ficha);
            const codnivel4 = await mapNivelOrganizacional(connection, equipo, 'nomnivel4', 'descrip', ficha);
            const codnivel5 = await mapNivelOrganizacional(connection, grupo, 'nomnivel5', 'descrip', ficha);

            return [
                ficha,
                vp,
                departamento,
                seccion,
                equipo,
                grupo,
                codnivel1,
                codnivel2,
                codnivel3,
                codnivel4,
                codnivel5
            ];
        }));

        const filteredData = tempData.filter(row => row !== null);

        // Insertar en tabla temporal
        console.log(`💾 Insertando ${filteredData.length} registros en tabla temporal...`);
        const batchSize = 1000;
        for (let i = 0; i < filteredData.length; i += batchSize) {
            const batch = filteredData.slice(i, i + batchSize);
            await connection.query(`
                INSERT INTO temp_estructura_personal (
                    ficha, vp, departamento, seccion, equipo, grupo,
                    codnivel1, codnivel2, codnivel3, codnivel4, codnivel5
                ) VALUES ?
            `, [batch]);
        }

        // Actualizar nompersonal
        console.log("🔄 Actualizando estructura organizacional...");
        const [updateResult] = await connection.execute(`
            UPDATE nompersonal np
            JOIN temp_estructura_personal tep ON np.ficha = tep.ficha
            SET 
                np.codnivel1 = tep.codnivel1,
                np.codnivel2 = tep.codnivel2,
                np.codnivel3 = tep.codnivel3,
                np.codnivel4 = tep.codnivel4,
                np.codnivel5 = tep.codnivel5
        `);

        console.log(`✅ Estructura organizacional actualizada: ${updateResult.affectedRows} registros`);

        // Verificaciones
        const [verificacion] = await connection.execute(`
            SELECT 
                COUNT(*) as total_actualizados,
                COUNT(np.codnivel1) as con_nivel1,
                COUNT(np.codnivel2) as con_nivel2,
                COUNT(np.codnivel3) as con_nivel3,
                COUNT(np.codnivel4) as con_nivel4,
                COUNT(np.codnivel5) as con_nivel5
            FROM nompersonal np
            JOIN temp_estructura_personal tep ON np.ficha = tep.ficha
        `);
        
        console.log(`📊 Verificación estructura:`);
        console.log(`   Total actualizados: ${verificacion[0].total_actualizados}`);
        console.log(`   Con nivel 1: ${verificacion[0].con_nivel1}`);
        console.log(`   Con nivel 2: ${verificacion[0].con_nivel2}`);
        console.log(`   Con nivel 3: ${verificacion[0].con_nivel3}`);
        console.log(`   Con nivel 4: ${verificacion[0].con_nivel4}`);
        console.log(`   Con nivel 5: ${verificacion[0].con_nivel5}`);

        // Limpiar tabla temporal
        await connection.execute('DROP TEMPORARY TABLE temp_estructura_personal');

    } catch (error) {
        console.error('Error en actualizarEstructuraOrganizacional:', error);
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

        console.log("=== MIGRACIÓN DE ACTUALIZACIÓN OCTUBRE 2025 ===");
        console.log("Archivo: Personal_Al_20102025.xlsx");
        console.log("Campos a actualizar:");
        console.log("- Nombres: ApellidoPaterno, ApellidoMaterno, Nombre → formato Intelesis");
        console.log("- Campos salariales: ISRFijoPeriodo, SueldoMensual, SueldoDiario, RataHora");
        console.log("- Gastos de representación: GR, GastoRepresentacionDiario, RataHoraGR");
        console.log("- Nivel académico: NivelAcademico → niveleducativo");
        console.log("- Posición MEF: PosicionMEF → nomposicion_id (formato 4 dígitos)");
        console.log("- Estructura organizacional: Vicepresidencia, Departamento, Secciones, Equipo, Grupo");
        console.log("- Niveles faltantes: Se crearán automáticamente (departamentos, secciones, equipos, grupos)");
        console.log("- Validación inteligente: Verifica existencia sin prefijo 'X' antes de crear");

        // Leer archivo Excel
        console.log("\n📁 Leyendo archivo Excel...");
        
        const workbookPersonal = xlsx.readFile('formatos/Personal_Al_20102025.xlsx');
        const sheetPersonal = workbookPersonal.Sheets[workbookPersonal.SheetNames[0]];
        const dataPersonal = xlsx.utils.sheet_to_json(sheetPersonal);
        console.log(`   Personal: ${dataPersonal.length} registros`);

        // Mostrar columnas disponibles para verificación
        if (dataPersonal.length > 0) {
            console.log("\n📋 Columnas disponibles en el Excel:");
            const columnas = Object.keys(dataPersonal[0]);
            columnas.forEach((col, index) => {
                console.log(`   ${index + 1}. ${col}`);
            });
        }

        const progressBar = new cliProgress.SingleBar({
            format: 'Progreso |{bar}| {percentage}% || {value}/{total} Módulos || {currentTask}',
            barCompleteChar: '\u2588',
            barIncompleteChar: '\u2591',
            hideCursor: true
        });

        const totalTasks = 9;
        progressBar.start(totalTasks, 0, { currentTask: 'Iniciando migración...' });

        try {
            // 0. Limpiar niveleducativo de registros basura
            progressBar.update(1, { currentTask: 'Limpiando niveleducativo de registros basura...' });
            await limpiarNivelEducativo(connection);

            // 1. Insertar departamentos faltantes
            progressBar.update(2, { currentTask: 'Insertando departamentos faltantes...' });
            await insertarDepartamentosFaltantes(connection, dataPersonal);

            // 2. Insertar secciones faltantes (nivel 3)
            progressBar.update(3, { currentTask: 'Insertando secciones faltantes...' });
            await insertarNivelesOrganizacionalesFaltantes(connection, dataPersonal, 3);

            // 3. Insertar equipos faltantes (nivel 4)
            progressBar.update(4, { currentTask: 'Insertando equipos faltantes...' });
            await insertarNivelesOrganizacionalesFaltantes(connection, dataPersonal, 4);

            // 4. Insertar grupos faltantes (nivel 5)
            progressBar.update(5, { currentTask: 'Insertando grupos faltantes...' });
            await insertarNivelesOrganizacionalesFaltantes(connection, dataPersonal, 5);

            // 5. Actualizar nombres (formato Intelesis)
            progressBar.update(6, { currentTask: 'Actualizando nombres (formato Intelesis)...' });
            await actualizarNombres(connection, dataPersonal);

            // 6. Actualizar campos salariales y académicos
            progressBar.update(7, { currentTask: 'Actualizando campos salariales y académicos...' });
            await actualizarCamposSalariales(connection, dataPersonal);

            // 7. Actualizar estructura organizacional
            progressBar.update(8, { currentTask: 'Actualizando estructura organizacional...' });
            await actualizarEstructuraOrganizacional(connection, dataPersonal);

            // 8. Crear resumen de departamentos no encontrados
            progressBar.update(9, { currentTask: 'Creando resumen de departamentos no encontrados...' });
            await crearResumenDepartamentosNoEncontrados(connection, dataPersonal);

            progressBar.update(totalTasks, { currentTask: 'Migración completada!' });

        } catch (error) {
            progressBar.stop();
            console.error('Error durante la migración:', error);
            throw error;
        }

        progressBar.stop();
        
        console.log("\n=== MIGRACIÓN OCTUBRE 2025 COMPLETADA ===");
        console.log("✅ niveleducativo limpiado de registros basura y duplicados");
        console.log("✅ Departamentos faltantes insertados automáticamente");
        console.log("✅ Secciones faltantes insertadas automáticamente");
        console.log("✅ Equipos faltantes insertados automáticamente");
        console.log("✅ Grupos faltantes insertados automáticamente");
        console.log("✅ Nombres actualizados (formato Intelesis)");
        console.log("✅ Campos salariales actualizados");
        console.log("✅ Gastos de representación actualizados");
        console.log("✅ Niveles académicos mapeados correctamente");
        console.log("✅ Posiciones MEF actualizadas (formato 4 dígitos)");
        console.log("✅ Estructura organizacional actualizada");
        console.log("✅ Resumen de departamentos no encontrados generado");
        console.log("✅ Todos los campos del Excel procesados");

    } finally {
        if (connection) await connection.end();
    }
}

main().catch(console.error);
