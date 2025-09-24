const mysql = require('mysql2/promise');
const xlsx = require('xlsx');
const dbConfig = require('./dbconfig');
require('dotenv').config();

function parseFichaFromClave(claveRaw) {
    if (!claveRaw) return null;
    const clave = claveRaw.toString().trim();
    // Esperado: E04989 -> 4989, E00003 -> 3
    const match = clave.match(/^[Ee]\s*(\d+)/);
    if (!match) return null;
    // El número puede venir con ceros a la izquierda
    const num = parseInt(match[1], 10);
    return isNaN(num) ? null : num;
}

function extractDeviceSNs(cellValue) {
    if (!cellValue) return [];
    const text = cellValue.toString();
    // Remover prefijos irrelevantes (Terminal 1:, Terminal 2:, Carga:, etc.) y capturar todos los SN
    // Un SN válido según ejemplos: MSD4234200055 (prefijo MSD + dígitos)
    const regex = /MSD\d+/g; // captura todos los MSD seguidos de dígitos
    const matches = text.match(regex);
    if (!matches) return [];
    // Normalizar: quitar espacios y duplicados preservando orden
    const seen = new Set();
    const result = [];
    for (const sn of matches) {
        const clean = sn.trim();
        if (clean && !seen.has(clean)) {
            seen.add(clean);
            result.push(clean);
        }
    }
    return result;
}

async function main() {
    let connection;
    try {
        connection = await mysql.createConnection({
            ...dbConfig,
            connectTimeout: 60000
        });

        console.log('=== MIGRACIÓN DE DISPOSITIVOS (personal_dispositivos) ===');
        console.log('Leyendo archivo formatos/DISPOSITIVOS_MARCACION.xlsx ...');

        const workbook = xlsx.readFile('formatos/DISPOSITIVOS_MARCACION.xlsx');
        const sheet = workbook.Sheets[workbook.SheetNames[0]];
        const rows = xlsx.utils.sheet_to_json(sheet, { defval: '' });

        console.log(`Registros en Excel: ${rows.length}`);

        // Determinar nombres posibles de columnas (tolerante a variaciones de acentos/espacios)
        // CLAVE y NÚMERO DE SERIE son las claves importantes según requerimiento.
        const guessColumn = (obj, candidates) => {
            for (const key of Object.keys(obj)) {
                const norm = key.toString().normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase().trim();
                for (const cand of candidates) {
                    const cn = cand.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase().trim();
                    if (norm === cn) return key; // devolver el nombre real en el archivo
                }
            }
            return null;
        };

        let claveCol = null;
        let serieCol = null;
        if (rows.length > 0) {
            claveCol = guessColumn(rows[0], ['CLAVE']);
            serieCol = guessColumn(rows[0], ['NÚMERO DE SERIE', 'NUMERO DE SERIE', 'NRO DE SERIE', 'SERIE', 'SERIAL']);
        }

        if (!claveCol || !serieCol) {
            throw new Error(`No se encontraron columnas requeridas. CLAVE: ${claveCol}, NÚMERO DE SERIE: ${serieCol}`);
        }

        // Preparar mapa de ficha -> personal_id para mejorar rendimiento (batch)
        const fichasSet = new Set();
        for (const row of rows) {
            const ficha = parseFichaFromClave(row[claveCol]);
            if (ficha !== null) fichasSet.add(ficha);
        }
        const fichas = Array.from(fichasSet);

        const personalMap = new Map(); // ficha -> personal_id
        if (fichas.length > 0) {
            // Dividir en lotes para evitar límites de parámetros
            const batchSize = 1000;
            for (let i = 0; i < fichas.length; i += batchSize) {
                const batch = fichas.slice(i, i + batchSize);
                const placeholders = batch.map(() => '?').join(',');
                const [res] = await connection.query(
                    `SELECT personal_id, ficha FROM nompersonal WHERE ficha IN (${placeholders})`,
                    batch
                );
                for (const r of res) {
                    personalMap.set(Number(r.ficha), Number(r.personal_id));
                }
            }
        }

        // Preparar mapa de DEVICE_SN -> DEVICE_ID (a partir de todos los SN detectados)
        const allSNSet = new Set();
        for (const row of rows) {
            const snList = extractDeviceSNs(row[serieCol]);
            snList.forEach(sn => allSNSet.add(sn));
        }
        const allSN = Array.from(allSNSet);

        const deviceMap = new Map(); // DEVICE_SN -> DEVICE_ID
        if (allSN.length > 0) {
            const batchSize = 1000;
            for (let i = 0; i < allSN.length; i += batchSize) {
                const batch = allSN.slice(i, i + batchSize);
                const placeholders = batch.map(() => '?').join(',');
                const [res] = await connection.query(
                    `SELECT DEVICE_ID, DEVICE_SN FROM profacex_device_info WHERE DEVICE_SN IN (${placeholders})`,
                    batch
                );
                for (const r of res) {
                    deviceMap.set(r.DEVICE_SN, Number(r.DEVICE_ID));
                }
            }
        }

        // Construir inserciones para personal_dispositivos
        const inserts = [];
        let missingPersonal = 0;
        let missingDevices = 0;

        for (const row of rows) {
            const ficha = parseFichaFromClave(row[claveCol]);
            if (ficha === null) continue;
            const personalId = personalMap.get(ficha);
            if (!personalId) {
                missingPersonal++;
                continue;
            }
            const snList = extractDeviceSNs(row[serieCol]);
            if (snList.length === 0) continue;
            for (const sn of snList) {
                const deviceId = deviceMap.get(sn);
                if (!deviceId) {
                    missingDevices++;
                    continue;
                }
                inserts.push([personalId, deviceId]);
            }
        }

        console.log(`Vínculos a insertar: ${inserts.length}`);
        if (missingPersonal > 0) console.warn(`Advertencia: ${missingPersonal} filas con CLAVE sin match en nompersonal (ficha).`);
        if (missingDevices > 0) console.warn(`Advertencia: ${missingDevices} dispositivos sin match en profacex_device_info (DEVICE_SN).`);

        await connection.execute('SET FOREIGN_KEY_CHECKS=0');
        try {
            console.log('Limpiando tabla personal_dispositivos (TRUNCATE)...');
            await connection.execute('TRUNCATE TABLE personal_dispositivos');

            if (inserts.length > 0) {
                const batchSize = 1000;
                const insertSql = 'INSERT INTO personal_dispositivos (personal_id, device_id) VALUES ?';
                for (let i = 0; i < inserts.length; i += batchSize) {
                    const batch = inserts.slice(i, i + batchSize);
                    await connection.query(insertSql, [batch]);
                }
            }
        } finally {
            await connection.execute('SET FOREIGN_KEY_CHECKS=1');
        }

        console.log('✅ Migración completada.');
    } catch (err) {
        console.error('Error en migracion_dispositivos:', err);
        process.exitCode = 1;
    } finally {
        if (connection) await connection.end();
    }
}

main();


