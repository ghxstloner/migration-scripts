const mysql = require('mysql2/promise');
const xlsx = require('xlsx');
const cliProgress = require('cli-progress');
const dbConfig = require('./dbconfig');
require('dotenv').config();


async function procesarDivisionesPanama() {
    let connection;
    try {
        // Create database connection
        connection = await mysql.createConnection({
            ...dbConfig,
            multipleStatements: true,
            connectTimeout: 60000
        });

        console.log("=== Procesando Provincias, Distritos y Corregimientos de Panamá ===");
        
        // Read the Excel file
        const workbook = xlsx.readFile('formatos/Provincia-Distrito-Corregimiento.xlsx');
        const sheet = workbook.Sheets[workbook.SheetNames[0]];
        
        // Process data from the Excel sheet
        await processExcelData(connection, sheet);

        console.log("=== Proceso completado con éxito ===");
    } catch (error) {
        console.error("Error durante el proceso:", error);
    } finally {
        if (connection) await connection.end();
    }
}

async function processExcelData(connection, sheet) {
    // Initialize progress bar
    const progressBar = new cliProgress.SingleBar({
        format: 'Progreso |{bar}| {percentage}% || {currentTask}',
        barCompleteChar: '\u2588',
        barIncompleteChar: '\u2591',
        hideCursor: true
    });

    progressBar.start(100, 0, { currentTask: 'Preparando datos...' });

    try {
        // Clear existing data
        await connection.execute('SET FOREIGN_KEY_CHECKS=0');
        await connection.execute('TRUNCATE TABLE corregimientos');
        await connection.execute('TRUNCATE TABLE distritos');
        await connection.execute('TRUNCATE TABLE provincias');
        await connection.execute('SET FOREIGN_KEY_CHECKS=1');

        progressBar.update(10, { currentTask: 'Analizando datos del Excel...' });

        // Convert sheet to JSON with raw values
        const data = xlsx.utils.sheet_to_json(sheet, { 
            header: ["Prefijo_cédula_prov", "Provincia", "Num_Dis", "Distrito", "Num_Corr", "Corregimientos"],
            defval: null,
            raw: true
        });

        // Skip header row if present
        const startRow = data[0].Prefijo_cédula_prov === "Prefijo_cédula_prov" ? 1 : 0;

        // Extract and process the data
        const provincias = new Map();
        const distritos = new Map();
        const corregimientos = [];

        let currentPrefijo = null;
        let currentProvincia = null;
        let currentNumDis = null;
        let currentDistrito = null;

        progressBar.update(20, { currentTask: 'Extrayendo provincias, distritos y corregimientos...' });

        for (let i = startRow; i < data.length; i++) {
            const row = data[i];
            
            // Update provincia if available
            if (row.Prefijo_cédula_prov !== null && row.Prefijo_cédula_prov !== "") {
                currentPrefijo = String(row.Prefijo_cédula_prov).trim();
                currentProvincia = String(row.Provincia).trim();
                
                if (!provincias.has(currentPrefijo)) {
                    provincias.set(currentPrefijo, currentProvincia);
                }
            }
            
            // Update distrito if available
            if (row.Num_Dis !== null && row.Num_Dis !== "") {
                currentNumDis = String(row.Num_Dis).trim();
                currentDistrito = String(row.Distrito).trim();
                
                const distritoKey = `${currentPrefijo}_${currentNumDis}`;
                if (!distritos.has(distritoKey)) {
                    distritos.set(distritoKey, {
                        prefijoProvincia: currentPrefijo,
                        nombreProvincia: currentProvincia,
                        numero: currentNumDis,
                        nombre: currentDistrito
                    });
                }
            }
            
            // Process corregimiento if available
            if (row.Num_Corr !== null && row.Num_Corr !== "") {
                const numCorr = String(row.Num_Corr).trim();
                const nombreCorr = String(row.Corregimientos).trim();
                
                const distritoKey = `${currentPrefijo}_${currentNumDis}`;
                
                corregimientos.push({
                    distritoKey,
                    numero: numCorr,
                    nombre: nombreCorr,
                    nombreDistrito: currentDistrito,
                    nombreProvincia: currentProvincia
                });
            }
        }

        // Insert provincias
        progressBar.update(40, { currentTask: `Insertando ${provincias.size} provincias...` });
        const provinciaIds = await insertProvincias(connection, provincias);

        // Insert distritos
        progressBar.update(60, { currentTask: `Insertando ${distritos.size} distritos...` });
        const distritoIds = await insertDistritos(connection, distritos, provinciaIds);

        // Insert corregimientos
        progressBar.update(80, { currentTask: `Insertando ${corregimientos.length} corregimientos...` });
        await insertCorregimientos(connection, corregimientos, distritoIds);

        progressBar.update(100, { currentTask: 'Proceso completado!' });
    } catch (error) {
        console.error('Error durante el procesamiento:', error);
        throw error;
    } finally {
        progressBar.stop();
    }
}

async function insertProvincias(connection, provincias) {
    if (provincias.size === 0) return new Map();

    const currentTime = new Date().toISOString().slice(0, 19).replace('T', ' ');
    const provinciaIds = new Map();
    
    const insertQuery = `
        INSERT INTO provincias (descripcion, created_at, updated_at)
        VALUES (?, ?, ?)
    `;

    for (const [prefijo, nombre] of provincias.entries()) {
        const [result] = await connection.execute(insertQuery, [nombre, currentTime, currentTime]);
        provinciaIds.set(prefijo, result.insertId);
    }

    console.log(`Insertadas ${provincias.size} provincias`);
    return provinciaIds;
}

async function insertDistritos(connection, distritos, provinciaIds) {
    if (distritos.size === 0) return new Map();

    const currentTime = new Date().toISOString().slice(0, 19).replace('T', ' ');
    const distritoIds = new Map();

    const insertQuery = `
        INSERT INTO distritos (descripcion, provincia_id, created_at, updated_at)
        VALUES (?, ?, ?, ?)
    `;

    for (const [key, distrito] of distritos.entries()) {
        const provinciaId = provinciaIds.get(distrito.prefijoProvincia);
        
        if (provinciaId) {
            const [result] = await connection.execute(insertQuery, [
                distrito.nombre,
                provinciaId,
                currentTime,
                currentTime
            ]);
            
            distritoIds.set(key, result.insertId);
        } else {
            console.warn(`No se encontró la provincia con prefijo ${distrito.prefijoProvincia} para el distrito ${distrito.nombre}`);
        }
    }

    console.log(`Insertados ${distritoIds.size} distritos`);
    return distritoIds;
}

async function insertCorregimientos(connection, corregimientos, distritoIds) {
    if (corregimientos.length === 0) return;

    const currentTime = new Date().toISOString().slice(0, 19).replace('T', ' ');

    const insertQuery = `
        INSERT INTO corregimientos (descripcion, distrito_id, created_at, updated_at)
        VALUES (?, ?, ?, ?)
    `;

    const batchSize = 100;
    let insertedCount = 0;

    for (let i = 0; i < corregimientos.length; i += batchSize) {
        const batch = corregimientos.slice(i, Math.min(i + batchSize, corregimientos.length));
        
        for (const corr of batch) {
            const distritoId = distritoIds.get(corr.distritoKey);
            
            if (distritoId) {
                await connection.execute(insertQuery, [
                    corr.nombre,
                    distritoId,
                    currentTime,
                    currentTime
                ]);
                insertedCount++;
            } else {
                console.warn(`No se encontró el distrito con clave ${corr.distritoKey} (${corr.nombreDistrito}) para el corregimiento ${corr.nombre}`);
            }
        }
    }

    console.log(`Insertados ${insertedCount} corregimientos`);
}

// Run the main function
procesarDivisionesPanama().catch(console.error);