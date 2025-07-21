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
    
    const dateStr = String(date).trim();
    
    try {
        if (dateStr.match(/^\d{4}-\d{2}-\d{2}(\s|T)/)) {
            return dateStr.split(/\s|T/)[0];
        }
        
        if (dateStr.includes('/')) {
            const parts = dateStr.split('/');
            if (parts.length === 3) {
                let day, month, year;
                
                if (parseInt(parts[0]) > 12) {
                    day = parts[0];
                    month = parts[1];
                    year = parts[2];
                } else {
                    if (parseInt(parts[1]) > 12) {
                        day = parts[0];
                        month = parts[1];
                        year = parts[2];
                    } else {
                        day = parts[0];
                        month = parts[1];
                        year = parts[2];
                    }
                }
                
                if (year.length === 2) {
                    const currentYear = new Date().getFullYear();
                    const century = Math.floor(currentYear / 100) * 100;
                    year = parseInt(year) + century;
                    if (year > currentYear + 80) {
                        year -= 100;
                    }
                }
                
                return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
            }
        }
        
        const timestamp = Date.parse(dateStr);
        if (!isNaN(timestamp)) {
            return new Date(timestamp).toISOString().split('T')[0];
        }
        
        return null;
    } catch (e) {
        console.log(`Error al procesar fecha: ${dateStr}`, e);
        return null;
    }
}

function extractNumericValue(str) {
    if (!str) return null;
    const matches = str.match(/\d+/);
    return matches ? parseInt(matches[0], 10) : null;
}

// Función para normalizar texto MEJORADA (manejar mejor acentos)
function normalizeText(text) {
    if (!text) return '';
    return text.toString().trim()
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "") // Quitar todos los acentos
        .replace(/ñ/gi, 'n')
        .replace(/Ñ/gi, 'N')
        .replace(/[òóôõö]/gi, 'o') // Normalizar todas las variantes de 'o'
        .replace(/[àáâãäå]/gi, 'a') // Normalizar todas las variantes de 'a'
        .replace(/[èéêë]/gi, 'e')   // Normalizar todas las variantes de 'e'
        .replace(/[ìíîï]/gi, 'i')   // Normalizar todas las variantes de 'i'
        .replace(/[ùúûü]/gi, 'u')   // Normalizar todas las variantes de 'u'
        .toUpperCase();
}

async function migrarPuestosOptimizado(connection, personalData) {
    console.log("\n=== Migrando Puestos de Trabajo a nomfuncion ===");

    try {
        // SOLO eliminar puesto_aitsa como solicitaste
        await connection.execute('DELETE FROM puesto_aitsa');
        
        // Leer archivo de puestos
        const workbookPuestos = xlsx.readFile('formatos/Puestos de Trabajo.xlsx');
        const sheetPuestos = workbookPuestos.Sheets[workbookPuestos.SheetNames[0]];
        const puestosData = xlsx.utils.sheet_to_json(sheetPuestos);

        console.log(`Procesando ${puestosData.length} puestos del archivo Excel`);

        // Obtener los puestos existentes en nomfuncion
        const [existingPuestos] = await connection.execute(
            'SELECT nomfuncion_id, descripcion_funcion FROM nomfuncion ORDER BY nomfuncion_id'
        );
        
        console.log(`\n📋 Puestos existentes en nomfuncion: ${existingPuestos.length}`);
        
        // Mostrar duplicados existentes con acentos
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
        
        // Mostrar duplicados
        let duplicatesFound = 0;
        duplicatesMap.forEach((variants, normalized) => {
            if (variants.length > 1) {
                duplicatesFound++;
                console.log(`🔄 Duplicado encontrado: "${normalized}"`);
                variants.forEach(v => {
                    console.log(`   ${v.id}: "${v.original}"`);
                });
            }
        });
        
        console.log(`📊 Total duplicados encontrados: ${duplicatesFound}`);

        // Crear mapa optimizado (usar el primer/más antiguo ID por cada puesto normalizado)
        const existingMap = new Map();
        duplicatesMap.forEach((variants, normalized) => {
            // Usar el que tenga menor ID (más antiguo)
            const oldest = variants.reduce((min, current) => 
                parseInt(current.id) < parseInt(min.id) ? current : min
            );
            existingMap.set(normalized, oldest);
        });

        // Procesar puestos del Excel
        const puestosParaInsertar = [];
        const puestosParaActualizar = [];
        
        // Recopilar puestos del archivo de puestos
        for (const row of puestosData) {
            const puesto = row.Puesto?.toString().trim();
            if (!puesto) continue;

            const puestoMayuscula = puesto.toUpperCase();
            const puestoNormalizado = normalizeText(puesto);
            
            if (existingMap.has(puestoNormalizado)) {
                // Ya existe pero puede necesitar actualización de acentos/ñ
                const existing = existingMap.get(puestoNormalizado);
                if (existing.original !== puestoMayuscula) {
                    puestosParaActualizar.push([puestoMayuscula, existing.id]);
                    console.log(`🔄 Actualizará: ${existing.id} "${existing.original}" → "${puestoMayuscula}"`);
                }
            } else {
                // No existe, agregarlo para inserción
                puestosParaInsertar.push([puestoMayuscula]);
                console.log(`➕ Nuevo puesto: "${puestoMayuscula}"`);
            }
        }

        // También recopilar puestos únicos del personal
        const puestosDelPersonal = new Set();
        personalData.forEach(row => {
            const puesto = row.Puesto?.toString().trim();
            if (puesto) {
                puestosDelPersonal.add(puesto);
            }
        });

        console.log(`\n📋 Puestos únicos encontrados en Personal: ${puestosDelPersonal.size}`);

        // Agregar puestos del personal que no estén ya considerados
        for (const puesto of puestosDelPersonal) {
            const puestoMayuscula = puesto.toUpperCase();
            const puestoNormalizado = normalizeText(puesto);
            
            if (!existingMap.has(puestoNormalizado)) {
                // Verificar si ya está en la lista para insertar
                const yaEnLista = puestosParaInsertar.some(p => 
                    normalizeText(p[0]) === puestoNormalizado
                );
                if (!yaEnLista) {
                    puestosParaInsertar.push([puestoMayuscula]);
                    console.log(`➕ Del personal: "${puestoMayuscula}"`);
                }
            }
        }

        // Insertar nuevos puestos en lotes
        if (puestosParaInsertar.length > 0) {
            console.log(`\n💾 Insertando ${puestosParaInsertar.length} nuevos puestos`);
            
            const insertQuery = 'INSERT INTO nomfuncion (descripcion_funcion) VALUES ?';
            const batchSize = 1000;
            
            for (let i = 0; i < puestosParaInsertar.length; i += batchSize) {
                const batch = puestosParaInsertar.slice(i, i + batchSize);
                await connection.query(insertQuery, [batch]);
            }
            
            // Actualizar el mapa con los nuevos puestos insertados
            const [newPuestos] = await connection.execute(
                'SELECT nomfuncion_id, descripcion_funcion FROM nomfuncion WHERE descripcion_funcion IN (?)',
                [puestosParaInsertar.map(p => p[0])]
            );
            
            newPuestos.forEach(p => {
                const normalized = normalizeText(p.descripcion_funcion);
                existingMap.set(normalized, {
                    id: p.nomfuncion_id,
                    original: p.descripcion_funcion
                });
            });
            
            console.log(`✅ Insertados y mapeados ${newPuestos.length} nuevos puestos`);
        }

        // Actualizar puestos existentes (acentos/ñ)
        if (puestosParaActualizar.length > 0) {
            console.log(`\n🔄 Actualizando ${puestosParaActualizar.length} puestos con acentos/ñ`);
            
            for (const [descripcion, id] of puestosParaActualizar) {
                await connection.execute(
                    'UPDATE nomfuncion SET descripcion_funcion = ? WHERE nomfuncion_id = ?',
                    [descripcion, id]
                );
                console.log(`   ✅ ${id}: "${descripcion}"`);
            }
        }

        console.log("\n✅ Puestos migrados exitosamente");

    } catch (error) {
        console.error('Error en migrarPuestosOptimizado:', error);
        throw error;
    }
}

async function migrarHorariosExactos(connection, data) {
    console.log("\n=== Migrando Horarios EXACTOS del Excel ===");

    try {
        // Obtener horarios existentes
        const [existingHorarios] = await connection.execute('SELECT cod_hor, des_hor FROM horarios ORDER BY cod_hor');
        console.log(`Horarios existentes: ${existingHorarios.length}`);

        // Crear mapa de horarios existentes (por descripción exacta)
        const horariosExistentesMap = new Map();
        existingHorarios.forEach(h => {
            horariosExistentesMap.set(h.des_hor.trim(), h.cod_hor);
        });

        // Obtener siguiente código disponible para horarios
        let nextCodHor = 1;
        if (existingHorarios.length > 0) {
            const maxCod = Math.max(...existingHorarios.map(h => parseInt(h.cod_hor) || 0));
            nextCodHor = maxCod + 1;
        }

        // Recopilar TODAS las jornadas únicas del personal
        const todasLasJornadas = new Set();
        const fichasEspeciales = [5202, 5203, 5204, 5205, 5206, 5207, 5208, 5210, 5212];
        
        data.forEach(row => {
            const ficha = extractNumericValue(row.Personal);
            const jornada = row.Jornada?.toString().trim();
            
            if (!jornada) return;
            
            // Incluir jornadas para:
            // 1. Fichas < 5200
            // 2. Fichas específicas >= 5200
            if (ficha < 5200 || fichasEspeciales.includes(ficha)) {
                todasLasJornadas.add(jornada);
            }
        });

        console.log(`\nJornadas únicas a procesar: ${todasLasJornadas.size}`);

        // Procesar cada jornada única
        const mapeoHorarios = new Map(); // jornada → cod_hor
        const nuevosHorarios = []; // Para insertar

        for (const jornada of todasLasJornadas) {
            if (horariosExistentesMap.has(jornada)) {
                // Ya existe exactamente
                const codHor = horariosExistentesMap.get(jornada);
                mapeoHorarios.set(jornada, codHor);
                console.log(`✅ "${jornada}" → ${codHor} (existente)`);
            } else {
                // Crear nuevo horario EXACTO
                mapeoHorarios.set(jornada, nextCodHor);
                nuevosHorarios.push([nextCodHor, jornada]);
                console.log(`🆕 "${jornada}" → NUEVO ${nextCodHor}`);
                nextCodHor++;
            }
        }

        // Insertar nuevos horarios
        if (nuevosHorarios.length > 0) {
            console.log(`\n💾 Insertando ${nuevosHorarios.length} nuevos horarios:`);
            
            for (const [codHor, descripcion] of nuevosHorarios) {
                await connection.execute(
                    'INSERT INTO horarios (cod_hor, des_hor) VALUES (?, ?)',
                    [codHor, descripcion]
                );
                console.log(`   ✅ ${codHor}: "${descripcion}"`);
            }
        }

        // Crear tabla temporal para actualizar personal
        await connection.execute(`
            CREATE TEMPORARY TABLE temp_horarios_exactos (
                numero_carnet VARCHAR(50),
                cod_hor INT,
                nuevo_estado VARCHAR(20)
            )
        `);

        // Preparar datos para actualizar
        const tempData = [];
        let procesados = 0;
        let excluidos = 0;
        
        for (const row of data) {
            const numeroCarnet = row.Personal?.toString().trim();
            const ficha = extractNumericValue(row.Personal);
            const jornada = row.Jornada?.toString().trim();
            const tipo = row.Tipo?.toString().trim().toLowerCase();
            const estadoActual = row.Estatus?.toString().trim().toUpperCase();
            
            if (!numeroCarnet) continue;
            
            let codHor = null;
            let nuevoEstado = null;
            
            // Determinar si debe procesarse
            const debeActualizarHorario = ficha < 5200 || fichasEspeciales.includes(ficha);
            
            if (debeActualizarHorario) {
                // Usar horario EXACTO del Excel
                if (jornada && mapeoHorarios.has(jornada)) {
                    codHor = mapeoHorarios.get(jornada);
                    procesados++;
                    console.log(`📅 Ficha ${ficha}: "${jornada}" → ${codHor}`);
                } else {
                    // Horario por defecto si no hay jornada
                    const horarioDefecto = existingHorarios.find(h => h.cod_hor == 9) || existingHorarios[0];
                    codHor = horarioDefecto ? horarioDefecto.cod_hor : 1;
                    procesados++;
                    console.log(`📅 Ficha ${ficha}: SIN JORNADA → ${codHor} (defecto)`);
                }
            } else {
                // NO tocar fichas >= 5200 que no estén en la lista especial
                excluidos++;
                if (ficha >= 5200) {
                    console.log(`⏭️  Ficha ${ficha}: conservando horario actual`);
                }
                continue;
            }
            
            // Determinar estado especial - PRIORIZAR "DE BAJA"
            if (estadoActual !== 'BAJA' && 
                tipo && (tipo.includes('jubilado') || (jornada && jornada.toLowerCase().includes('jubilado')))) {
                nuevoEstado = 'Jubilados';
            }
            
            tempData.push([numeroCarnet, codHor, nuevoEstado]);
        }

        console.log(`\n📊 Resumen de procesamiento:`);
        console.log(`   Procesados (horario actualizado): ${procesados}`);
        console.log(`   Excluidos (horario conservado): ${excluidos}`);

        // Insertar en tabla temporal
        if (tempData.length > 0) {
            await connection.query(
                'INSERT INTO temp_horarios_exactos (numero_carnet, cod_hor, nuevo_estado) VALUES ?',
                [tempData]
            );

            // Actualizar nompersonal con los horarios EXACTOS
            const [updateHorarioResult] = await connection.execute(`
                UPDATE nompersonal np
                JOIN temp_horarios_exactos the ON TRIM(np.numero_carnet) = TRIM(the.numero_carnet)
                SET np.cod_hor = the.cod_hor
                WHERE the.cod_hor IS NOT NULL
            `);

            console.log(`✅ Personal actualizado con horarios EXACTOS: ${updateHorarioResult.affectedRows} registros`);

            // Actualizar estados para jubilados (SOLO si no están de baja)
            const [updateEstadoResult] = await connection.execute(`
                UPDATE nompersonal np
                JOIN temp_horarios_exactos the ON TRIM(np.numero_carnet) = TRIM(the.numero_carnet)
                SET np.estado = the.nuevo_estado
                WHERE the.nuevo_estado IS NOT NULL 
                AND np.estado != 'De Baja'
            `);

            console.log(`✅ Estados actualizados para jubilados: ${updateEstadoResult.affectedRows} registros`);
        }

        // Limpiar tabla temporal
        await connection.execute('DROP TEMPORARY TABLE temp_horarios_exactos');

        console.log("\n✅ Horarios EXACTOS migrados exitosamente");
        console.log(`📊 Total de horarios mapeados: ${mapeoHorarios.size}`);
        console.log(`🆕 Nuevos horarios creados: ${nuevosHorarios.length}`);
        console.log(`🎯 Fichas procesadas: < 5200 + [5202,5203,5204,5205,5206,5207,5208,5210,5212]`);

    } catch (error) {
        console.error('Error en migrarHorariosExactos:', error);
        throw error;
    }
}

async function insertarPersonalCompleto(connection, data) {
    console.log("\n=== Insertando/Actualizando Personal Completo ===");

    try {
        // Crear tabla temporal con TODOS los campos originales
        await connection.execute(`
            CREATE TEMPORARY TABLE temp_personal_completo (
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
                zona_economica VARCHAR(50),
                barrio VARCHAR(100),
                calle VARCHAR(100),
                num_casa VARCHAR(50),
                id_pais INT DEFAULT 170,
                motivo_retiro VARCHAR(100),
                descripcion_pago VARCHAR(100),
                PRIMARY KEY (numero_carnet),
                INDEX idx_cedula (cedula),
                INDEX idx_ficha (ficha)
            ) ENGINE=InnoDB
        `);

        function mapEstadoCivil(estado) {
            if (estado === undefined || estado === null) {
                return 'Soltero/a';
            }

            if (typeof estado === 'number') {
                return estado === 2 ? 'Soltero/a' : 'Soltero/a';
            }

            if (typeof estado !== 'string') {
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
            if (!estado) return 'De Baja';

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

        // FUNCIÓN MEJORADA PARA MAPEAR NOMFUNCION_ID
        async function mapNomfuncionId(puesto) {
            if (!puesto) return null;
            
            const puestoNormalizado = normalizeText(puesto);
            
            // Consulta fresca para obtener todos los puestos actualizados
            const [allFunctions] = await connection.execute(
                'SELECT nomfuncion_id, descripcion_funcion FROM nomfuncion'
            );
            
            // Primero intentar coincidencia exacta
            for (const func of allFunctions) {
                if (func.descripcion_funcion === puesto.toUpperCase()) {
                    const id = func.nomfuncion_id.toString();
                    return parseInt(id) || null;
                }
            }
            
            // Si no hay coincidencia exacta, buscar con normalización
            for (const func of allFunctions) {
                const funcNormalizada = normalizeText(func.descripcion_funcion);
                if (funcNormalizada === puestoNormalizado) {
                    const id = func.nomfuncion_id.toString();
                    return parseInt(id) || null;
                }
            }
            
            console.log(`⚠️  No se encontró nomfuncion para: "${puesto}"`);
            return null;
        }

        const fechaHoy = new Date().toISOString().split('T')[0];

        // Preparar datos con TODOS los campos CORREGIDOS
        const tempData = await Promise.all(data.map(async row => {
            // FICHA: Usar extractNumericValue para extraer solo el número
            const fichaNum = extractNumericValue(row.Personal);
            
            // CORREGIR MAPEO DE SUELDOS
            const sueldoMensual = parseFloat(row.SueldoMensual) || 0;
            const sueldoDiario = parseFloat(row.SueldoDiario) || 0;
            const rataHora = parseFloat(row.RataHora) || 0;
            
            const tipnom = {
                'JUBILADOS': '002',
                'PERMANENTES': '001',
                'TRANSITORIOS': '003'
            }[row.Categoria?.trim()?.toUpperCase()] || null;

            const [departamentoId] = await connection.execute(
                'SELECT IdDepartamento FROM departamento WHERE Descripcion = ?',
                [row.Departamento ?? null]
            );

            // USAR FUNCIÓN MEJORADA PARA MAPEAR NOMFUNCION_ID
            const funcionId = await mapNomfuncionId(row.Puesto);

            const apenom = `${row.ApellidoPaterno || ''} ${row.ApellidoMaterno || ''}, ${row.Nombre || ''}`.trim();
            const foto = row.Cedula ? `fotos/${row.Cedula}.jpeg` : null;
            const tipoSangreId = await mapTipoSangre(row.TipoSangre);
            const parentescoContacto1 = await mapParentesco(connection, row.ParentescoContacto1);
            const parentescoContacto2 = await mapParentesco(connection, row.ParentescoContacto2);
            
            // NUMERO_CARNET: Usar solo el valor original sin padding para numero_carnet
            const numeroCarnet = (row.Personal || '').toString().trim();
            // CODIGO_CARNET: Con padding como antes
            const codigoCarnet = numeroCarnet.padEnd(10, ' ');

            return [
                codigoCarnet,  // codigo_carnet con padding
                numeroCarnet,  // numero_carnet sin padding
                fichaNum,      // ficha como número
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
                sueldoDiario,
                rataHora,
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
                funcionId,
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
                formatExcelDate(row.FechaBaja),
                1,
                1,
                row.Barrio || null,
                row.Calle || null,
                row.NumCasa || null,
                170,
                row.ConceptoBaja || null,
                'BNP - FONDO PLANILAS'
            ];
        }));

        const filteredData = tempData.filter(row => row[1]); // Filtrar por numero_carnet

        console.log(`\n📊 Personal a procesar: ${filteredData.length} registros`);
        console.log(`🔍 Registros con nomfuncion_id: ${filteredData.filter(row => row[46]).length}`);

        // Insertar en tabla temporal en lotes grandes
        const batchSize = 1000;
        for (let i = 0; i < filteredData.length; i += batchSize) {
            const batch = filteredData.slice(i, i + batchSize);
            await connection.query(`
                INSERT INTO temp_personal_completo (
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
                    tipemp, fecharetiro, tipo_funcionario, zona_economica, barrio, calle, num_casa, id_pais, motivo_retiro,
                    descripcion_pago
                ) VALUES ?
            `, [batch]);
        }

        // UPSERT: INSERT con ON DUPLICATE KEY UPDATE
        const [upsertResult] = await connection.execute(`
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
                tipemp, fecharetiro, tipo_funcionario, zona_economica, barrio, calle, num_casa, id_pais, motivo_retiro,
                descripcion_pago
            )
            SELECT * FROM temp_personal_completo
            ON DUPLICATE KEY UPDATE
                codigo_carnet = VALUES(codigo_carnet),
                estado = VALUES(estado),
                apellidos = VALUES(apellidos),
                apellido_materno = VALUES(apellido_materno),
                nombres = VALUES(nombres),
                sexo = VALUES(sexo),
                nacionalidad = VALUES(nacionalidad),
                fecnac = VALUES(fecnac),
                lugarnac = VALUES(lugarnac),
                cedula = VALUES(cedula),
                seguro_social = VALUES(seguro_social),
                estado_civil = VALUES(estado_civil),
                dv = VALUES(dv),
                direccion = VALUES(direccion),
                direccion2 = VALUES(direccion2),
                telefonos = VALUES(telefonos),
                TelefonoResidencial = VALUES(TelefonoResidencial),
                TelefonoCelular = VALUES(TelefonoCelular),
                email = VALUES(email),
                fecing = VALUES(fecing),
                fecha_resolucion_baja = VALUES(fecha_resolucion_baja),
                cuenta_pago = VALUES(cuenta_pago),
                suesal = VALUES(suesal),
                sueldopro = VALUES(sueldopro),
                salario_diario = VALUES(salario_diario),
                rata_x_hr = VALUES(rata_x_hr),
                nomfuncion_id = VALUES(nomfuncion_id),
                id_pais = VALUES(id_pais)
        `);

        console.log(`✅ Registros procesados (INSERT/UPDATE): ${upsertResult.affectedRows}`);

        // VERIFICAR MAPEO DE NOMFUNCION_ID
        const [verificacion] = await connection.execute(`
            SELECT 
                COUNT(*) as total,
                COUNT(nomfuncion_id) as con_funcion,
                COUNT(*) - COUNT(nomfuncion_id) as sin_funcion
            FROM nompersonal 
            WHERE numero_carnet IN (SELECT numero_carnet FROM temp_personal_completo)
        `);
        
        console.log(`📊 Verificación nomfuncion_id:`);
        console.log(`   Total: ${verificacion[0].total}`);
        console.log(`   Con función: ${verificacion[0].con_funcion}`);
        console.log(`   Sin función: ${verificacion[0].sin_funcion}`);

        // Actualizar conficha con el mayor valor encontrado
        await connection.execute(`
            UPDATE nomempresa 
            SET conficha = (
                SELECT GREATEST(
                    COALESCE((SELECT MAX(ficha) FROM nompersonal WHERE ficha IS NOT NULL), 0),
                    COALESCE(conficha, 0)
                )
            )
        `);

        // Limpiar tabla temporal
        await connection.execute('DROP TEMPORARY TABLE temp_personal_completo');

    } catch (error) {
        console.error('Error en insertarPersonalCompleto:', error);
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

        console.log("=== MIGRACIÓN EXACTA - HORARIOS Y PUESTOS PERFECTOS ===");

        // Leer archivos específicos
        console.log("Leyendo archivos...");
        
        // 1. Personal
        const workbookPersonal = xlsx.readFile('formatos/Personal_Al_18072025.xlsx');
        const sheetPersonal = workbookPersonal.Sheets[workbookPersonal.SheetNames[0]];
        const dataPersonal = xlsx.utils.sheet_to_json(sheetPersonal);
        console.log(`Personal: ${dataPersonal.length} registros`);

        const progressBar = new cliProgress.SingleBar({
            format: 'Progreso |{bar}| {percentage}% || {value}/{total} Módulos || {currentTask}',
            barCompleteChar: '\u2588',
            barIncompleteChar: '\u2591',
            hideCursor: true
        });

        const totalTasks = 3;
        progressBar.start(totalTasks, 0, { currentTask: 'Iniciando migración exacta...' });

        try {
            // 1. PRIMERO: Migrar puestos a nomfuncion (ANTES del personal)
            progressBar.update(1, { currentTask: 'Migrando Puestos (evitando duplicados)...' });
            await migrarPuestosOptimizado(connection, dataPersonal);

            // 2. SEGUNDO: Migrar personal completo (INSERT/UPDATE)
            progressBar.update(2, { currentTask: 'Migrando Personal Completo...' });
            await insertarPersonalCompleto(connection, dataPersonal);

            // 3. TERCERO: Migrar horarios EXACTOS del Excel
            progressBar.update(3, { currentTask: 'Migrando Horarios EXACTOS del Excel...' });
            await migrarHorariosExactos(connection, dataPersonal);

            progressBar.update(totalTasks, { currentTask: 'Migración exacta completada!' });
        } catch (error) {
            progressBar.stop();
            console.error('Error durante la migración:', error);
            throw error;
        }

        progressBar.stop();
        console.log("\n=== MIGRACIÓN EXACTA COMPLETADA ===");
        console.log("✅ Puestos migrados SIN duplicados (normalización mejorada)");
        console.log("✅ Personal completo migrado con nomfuncion_id correcto");
        console.log("✅ Horarios EXACTOS del Excel para fichas específicas");
        console.log("✅ Fichas < 5200: horario del Excel");
        console.log("✅ Fichas [5202,5203,5204,5205,5206,5207,5208,5210,5212]: horario del Excel");
        console.log("✅ Resto de fichas >= 5200: horarios conservados");
        console.log("✅ salario_diario y rata_x_hr corregidos");

    } finally {
        if (connection) await connection.end();
    }
}

main().catch(console.error);