const mysql = require('mysql2/promise');
const xlsx = require('xlsx');
const fs = require('fs');
const path = require('path');
const dbConfig = require('./dbconfig');
require('dotenv').config();

const dryRun = !process.argv.includes('--apply');

const EXCEL_PATH = path.join(__dirname, 'formatos', 'listado_correos.xlsx');
const LOG_PATH = path.join(__dirname, 'log_actualizacion_correos_nompersonal.txt');

/**
 * Primera columna del Excel: quita ceros a la izquierda y convierte a entero (coincide con nompersonal.ficha).
 */
function normalizeFicha(raw) {
    if (raw === null || raw === undefined) return null;
    const s = String(raw).trim();
    if (!s) return null;
    const stripped = s.replace(/^0+/, '') || '0';
    const n = parseInt(stripped, 10);
    return Number.isFinite(n) ? n : null;
}

function normalizeEmail(raw) {
    if (raw === null || raw === undefined) return '';
    return String(raw).trim();
}

function emailsMatch(a, b) {
    return normalizeEmail(a).toLowerCase() === normalizeEmail(b).toLowerCase();
}

function loadWorkbookMap() {
    const workbook = xlsx.readFile(EXCEL_PATH);
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const rows = xlsx.utils.sheet_to_json(sheet, { defval: '' });

    /** @type {Map<number, { email: string, apellido: string, nombre: string, excelRow: number }>} */
    const byFicha = new Map();
    const duplicateKeys = [];
    const invalidFicha = [];

    rows.forEach((row, idx) => {
        const excelRow = idx + 2;
        const rawFicha = row.NUMERO_EMPLEADO;
        const ficha = normalizeFicha(rawFicha);
        const email = normalizeEmail(row.CORREO);
        const apellido = normalizeEmail(row.APELLIDO);
        const nombre = normalizeEmail(row.NOMBRE);

        if (ficha === null) {
            if (String(rawFicha).trim() !== '') {
                invalidFicha.push({ excelRow, rawFicha: String(rawFicha).trim() });
            }
            return;
        }

        if (byFicha.has(ficha)) {
            duplicateKeys.push({
                ficha,
                excelRow,
                previousEmail: byFicha.get(ficha).email,
                newEmail: email
            });
        }

        byFicha.set(ficha, { email, apellido, nombre, excelRow });
    });

    return { byFicha, duplicateKeys, totalExcelRows: rows.length, invalidFicha };
}

function formatPerson(meta) {
    const bits = [meta.apellido, meta.nombre].filter(Boolean);
    const label = bits.length ? bits.join(', ') : '';
    return label ? `ficha ${meta.ficha} (${label})` : `ficha ${meta.ficha}`;
}

function writeLog(lines) {
    const header = `=== Actualizacion nompersonal.email ${new Date().toISOString()} ===\nModo: ${dryRun ? 'DRY RUN' : 'APLICADO'}\n\n`;
    fs.writeFileSync(LOG_PATH, header + lines.join('\n') + '\n', 'utf8');
}

async function main() {
    let connection;

    const report = {
        invalidFicha: [],
        skippedEmptyEmail: [],
        notFound: [],
        ambiguousFicha: [],
        duplicateExcel: [],
        intact: [],
        /** Sustituyeron un correo que ya existía (valor distinto). */
        replaced: [],
        /** No tenían correo (NULL/vacío) y se asignó uno. */
        filledPreviouslyEmpty: []
    };

    try {
        if (!fs.existsSync(EXCEL_PATH)) {
            throw new Error(`No existe el archivo: ${EXCEL_PATH}`);
        }

        const { byFicha, duplicateKeys, totalExcelRows, invalidFicha } = loadWorkbookMap();
        report.duplicateExcel = duplicateKeys;
        report.invalidFicha = invalidFicha;

        connection = await mysql.createConnection({
            ...dbConfig,
            connectTimeout: 60000
        });

        if (!dryRun) {
            await connection.beginTransaction();
        }

        console.log('=== Sincronizacion de nompersonal.email ===');
        console.log(`Modo: ${dryRun ? 'DRY RUN (sin --apply no se escribe en BD)' : 'APLICAR CAMBIOS'}`);
        console.log(`Fuente: ${EXCEL_PATH}`);
        console.log(`Filas en hoja (con cabecera excluida en conteo logico): ${totalExcelRows}`);
        if (duplicateKeys.length) {
            console.log(`Aviso: ${duplicateKeys.length} fila(s) en Excel repiten NUMERO_EMPLEADO; se usa la ultima aparicion por ficha.`);
        }

        for (const [ficha, meta] of byFicha) {
            const excelEmail = meta.email;
            if (!excelEmail) {
                report.skippedEmptyEmail.push({ ficha, excelRow: meta.excelRow, apellido: meta.apellido, nombre: meta.nombre });
                continue;
            }

            const [dbRows] = await connection.query(
                `SELECT personal_id, ficha, email FROM nompersonal WHERE CAST(ficha AS UNSIGNED) = ?`,
                [ficha]
            );

            if (dbRows.length === 0) {
                report.notFound.push({ ficha, email: excelEmail, excelRow: meta.excelRow, apellido: meta.apellido, nombre: meta.nombre });
                continue;
            }

            if (dbRows.length > 1) {
                report.ambiguousFicha.push({
                    ficha,
                    count: dbRows.length,
                    personalIds: dbRows.map(r => r.personal_id)
                });
                continue;
            }

            const row = dbRows[0];
            const previous = row.email;

            if (emailsMatch(previous, excelEmail)) {
                report.intact.push({
                    ficha,
                    email: excelEmail,
                    excelRow: meta.excelRow,
                    apellido: meta.apellido,
                    nombre: meta.nombre
                });
                continue;
            }

            const hadEmailBefore = normalizeEmail(previous) !== '';

            if (!dryRun) {
                await connection.query('UPDATE nompersonal SET email = ? WHERE personal_id = ?', [excelEmail, row.personal_id]);
            }

            const entry = {
                ficha,
                excelRow: meta.excelRow,
                apellido: meta.apellido,
                nombre: meta.nombre,
                previousEmail: previous === null || previous === undefined ? '' : String(previous),
                newEmail: excelEmail
            };

            if (hadEmailBefore) {
                report.replaced.push(entry);
            } else {
                report.filledPreviouslyEmpty.push(entry);
            }
        }

        if (!dryRun) {
            await connection.commit();
        }

        const totalCambios = report.replaced.length + report.filledPreviouslyEmpty.length;

        console.log('\n--- Resumen ---');
        console.log(`Registros en Excel con ficha valida (unicos por ficha): ${byFicha.size}`);
        console.log(`Actualizaciones ${dryRun ? 'previstas' : 'aplicadas'} (correo distinto al de BD): ${totalCambios}`);
        console.log(`  - Reemplazo de correo existente: ${report.replaced.length}`);
        console.log(`  - Correo nuevo (antes vacio o NULL en BD): ${report.filledPreviouslyEmpty.length}`);
        console.log(`Sin cambio (correo en Excel igual al de BD): ${report.intact.length}`);
        console.log(`Omitidos (sin CORREO en Excel): ${report.skippedEmptyEmail.length}`);
        console.log(`NUMERO_EMPLEADO no valido en Excel: ${report.invalidFicha.length}`);
        console.log(`No encontrados en nompersonal: ${report.notFound.length}`);
        console.log(`Ficha ambigua en BD (>1 fila): ${report.ambiguousFicha.length}`);

        const logLines = [];
        logLines.push('--- Resumen ---');
        logLines.push(`Actualizaciones: ${totalCambios} (reemplazo: ${report.replaced.length}, sin correo antes: ${report.filledPreviouslyEmpty.length})`);
        logLines.push(`Intactos: ${report.intact.length}`);
        logLines.push(`Omitidos sin correo en Excel: ${report.skippedEmptyEmail.length}`);
        logLines.push(`NUMERO_EMPLEADO invalido: ${report.invalidFicha.length}`);
        logLines.push(`No encontrados: ${report.notFound.length}`);
        logLines.push('');

        logLines.push('--- Reemplazos (ya habia correo en BD) ---');
        for (const item of report.replaced) {
            logLines.push(`${formatPerson(item)} fila Excel ${item.excelRow}: "${item.previousEmail}" -> "${item.newEmail}"`);
        }

        logLines.push('');
        logLines.push('--- Asignados (antes sin correo en BD) ---');
        for (const item of report.filledPreviouslyEmpty) {
            logLines.push(`${formatPerson(item)} fila Excel ${item.excelRow}: (vacio) -> "${item.newEmail}"`);
        }

        logLines.push('');
        logLines.push('--- Intactos (mismo correo que en Excel) ---');
        for (const item of report.intact) {
            logLines.push(`${formatPerson(item)} fila Excel ${item.excelRow}: "${item.email}"`);
        }

        if (report.invalidFicha.length) {
            logLines.push('');
            logLines.push('--- NUMERO_EMPLEADO no interpretable como entero ---');
            for (const item of report.invalidFicha) {
                logLines.push(`fila Excel ${item.excelRow}: "${item.rawFicha}"`);
            }
        }

        if (report.notFound.length) {
            logLines.push('');
            logLines.push('--- No encontrados en nompersonal ---');
            for (const item of report.notFound) {
                logLines.push(`ficha ${item.ficha} fila Excel ${item.excelRow} correo "${item.email}"`);
            }
        }

        if (report.duplicateExcel.length) {
            logLines.push('');
            logLines.push('--- Duplicados de NUMERO_EMPLEADO en Excel (ultima fila gana) ---');
            for (const d of report.duplicateExcel) {
                logLines.push(`ficha ${d.ficha} fila ${d.excelRow} (antes "${d.previousEmail}" -> ahora "${d.newEmail}")`);
            }
        }

        writeLog(logLines);
        console.log(`\nDetalle escrito en: ${LOG_PATH}`);
        console.log(`\n${dryRun ? 'Revise el reporte y ejecute con node sincronizar_correos_nompersonal.js --apply para aplicar.' : 'Sincronizacion de correos completada.'}`);
    } catch (error) {
        if (connection && !dryRun) {
            await connection.rollback();
        }
        console.error('Error sincronizando correos:', error);
        process.exitCode = 1;
    } finally {
        if (connection) {
            await connection.end();
        }
    }
}

main();
