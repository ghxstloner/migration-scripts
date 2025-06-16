const mysql = require('mysql2/promise');
const xlsx = require('xlsx');
const dbConfig = require('./dbconfig');
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

function mapNacionalidad(nacionalidad) {
    // Si está vacío, null, undefined, retorna null para la BD
    if (!nacionalidad || nacionalidad.toString().trim() === '') {
        return null;
    }
    
    const nacionalidadNormalizada = nacionalidad.toString()
        .trim()
        .toLowerCase()
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, ""); // Quita acentos
    
    const variantesPanamenas = [
        'panamena',
        'panameno',
        'panama'
    ];
    
    return variantesPanamenas.includes(nacionalidadNormalizada) ? 1 : 2;
}

function extractNumericValue(str) {
    if (!str) return null;
    const matches = str.match(/\d+/);
    return matches ? parseInt(matches[0], 10) : null;
}

async function migrarNacionalidadYUltimoPago(connection, data) {
    console.log("\n=== Migrando SOLO Nacionalidad y Ultimo Día Pagado POR FICHA ===");

    try {
        // Crear tabla temporal para los updates
        await connection.execute(`
            CREATE TEMPORARY TABLE temp_nacionalidad_pago (
                ficha INT,
                nacionalidad INT,
                ultimo_dia_pagado DATE,
                PRIMARY KEY (ficha)
            ) ENGINE=InnoDB
        `);

        // Preparar datos para actualizar
        const tempData = [];
        let procesados = 0;
        let conNacionalidad = 0;
        let conUltimoPago = 0;
        let sinFicha = 0;

        for (const row of data) {
            const ficha = extractNumericValue(row.Personal);
            if (!ficha) {
                sinFicha++;
                continue;
            }

            const nacionalidad = mapNacionalidad(row.Nacionalidad);
            const ultimoPago = formatExcelDate(row.UltimoPago);

            // Solo procesar si hay ficha
            tempData.push([
                ficha,
                nacionalidad,
                ultimoPago
            ]);

            procesados++;
            if (nacionalidad !== null) conNacionalidad++;
            if (ultimoPago !== null) conUltimoPago++;

            // Log de ejemplo para verificar
            if (procesados <= 10) {
                console.log(`📝 Ficha ${ficha}: Nacionalidad="${row.Nacionalidad}" → ${nacionalidad}, UltimoPago="${row.UltimoPago}" → ${ultimoPago}`);
            }
        }

        console.log(`\n📊 Estadísticas de procesamiento:`);
        console.log(`   Total procesados: ${procesados}`);
        console.log(`   Sin ficha (excluidos): ${sinFicha}`);
        console.log(`   Con nacionalidad: ${conNacionalidad}`);
        console.log(`   Con último pago: ${conUltimoPago}`);

        // Insertar en tabla temporal
        if (tempData.length > 0) {
            const batchSize = 1000;
            for (let i = 0; i < tempData.length; i += batchSize) {
                const batch = tempData.slice(i, i + batchSize);
                await connection.query(
                    'INSERT INTO temp_nacionalidad_pago (ficha, nacionalidad, ultimo_dia_pagado) VALUES ?',
                    [batch]
                );
            }

            console.log(`✅ Datos cargados en tabla temporal: ${tempData.length} registros`);

            // Actualizar nacionalidad (solo donde no sea NULL en temp)
            const [updateNacionalidadResult] = await connection.execute(`
                UPDATE nompersonal np
                JOIN temp_nacionalidad_pago tnp ON np.ficha = tnp.ficha
                SET np.nacionalidad = tnp.nacionalidad
                WHERE tnp.nacionalidad IS NOT NULL
            `);

            console.log(`✅ Nacionalidades actualizadas: ${updateNacionalidadResult.affectedRows} registros`);

            // Actualizar ultimo_dia_pagado (solo donde no sea NULL en temp)
            const [updatePagoResult] = await connection.execute(`
                UPDATE nompersonal np
                JOIN temp_nacionalidad_pago tnp ON np.ficha = tnp.ficha
                SET np.ultimo_dia_pagado = tnp.ultimo_dia_pagado
                WHERE tnp.ultimo_dia_pagado IS NOT NULL
            `);

            console.log(`✅ Últimos días pagados actualizados: ${updatePagoResult.affectedRows} registros`);

            // Estadísticas finales
            const [estadisticas] = await connection.execute(`
                SELECT 
                    COUNT(*) as total_bd,
                    COUNT(nacionalidad) as con_nacionalidad,
                    COUNT(ultimo_dia_pagado) as con_ultimo_pago
                FROM nompersonal 
                WHERE ficha IN (SELECT ficha FROM temp_nacionalidad_pago)
            `);

            console.log(`\n📈 Estadísticas finales en BD:`);
            console.log(`   Registros en BD: ${estadisticas[0].total_bd}`);
            console.log(`   Con nacionalidad: ${estadisticas[0].con_nacionalidad}`);
            console.log(`   Con último pago: ${estadisticas[0].con_ultimo_pago}`);
        }

        // Limpiar tabla temporal
        await connection.execute('DROP TEMPORARY TABLE temp_nacionalidad_pago');

        console.log("\n✅ Migración de nacionalidad y último pago completada");

    } catch (error) {
        console.error('Error en migrarNacionalidadYUltimoPago:', error);
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

        console.log("=== MIGRACIÓN: NACIONALIDAD Y ÚLTIMO DÍA PAGADO POR FICHA ===");

        // Leer archivo Excel
        console.log("Leyendo archivo Excel...");
        const workbookPersonal = xlsx.readFile('formatos/Personal_Al_09062025.xlsx');
        const sheetPersonal = workbookPersonal.Sheets[workbookPersonal.SheetNames[0]];
        const dataPersonal = xlsx.utils.sheet_to_json(sheetPersonal);
        console.log(`📄 Personal en Excel: ${dataPersonal.length} registros`);

        // Migrar solo nacionalidad y último pago
        await migrarNacionalidadYUltimoPago(connection, dataPersonal);

        console.log("\n=== MIGRACIÓN COMPLETADA ===");
        console.log("✅ JOIN por ficha: E00001 → 1, E00013 → 13");
        console.log("✅ Nacionalidades: Panameña/Panameño/Panama → 1, otros → 2, vacío → NULL");
        console.log("✅ Último día pagado: UltimoPago → ultimo_dia_pagado");

    } catch (error) {
        console.error('Error en main:', error);
    } finally {
        if (connection) await connection.end();
    }
}

main().catch(console.error);