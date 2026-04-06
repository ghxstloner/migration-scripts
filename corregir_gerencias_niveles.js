const mysql = require('mysql2/promise');
const xlsx = require('xlsx');
const dbConfig = require('./dbconfig');
require('dotenv').config();

const dryRun = !process.argv.includes('--apply');

function normalizeText(text) {
    if (text === null || text === undefined) return '';
    return text.toString().trim()
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/ñ/gi, 'n')
        .replace(/Ñ/gi, 'N')
        .replace(/[òóôõö]/gi, 'o')
        .replace(/[àáâãäå]/gi, 'a')
        .replace(/[èéêë]/gi, 'e')
        .replace(/[ìíîï]/gi, 'i')
        .replace(/[ùúûü]/gi, 'u')
        .replace(/\s+/g, ' ')
        .toUpperCase();
}

function sanitizeValue(value) {
    if (value === null || value === undefined) return null;
    const str = value.toString().trim();
    if (!str || normalizeText(str) === 'NULL') return null;
    return str;
}

function loadWorkbook(path, mapping) {
    const workbook = xlsx.readFile(path);
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    return xlsx.utils.sheet_to_json(sheet, { defval: '' }).map(mapping);
}

function buildUniqueParentMap(sources, childKey, parentKey) {
    const relations = new Map();

    for (const source of sources) {
        for (const row of source.rows) {
            const child = sanitizeValue(row[childKey]);
            const parent = sanitizeValue(row[parentKey]);
            if (!child || !parent) continue;

            const childNorm = normalizeText(child);
            const parentNorm = normalizeText(parent);

            if (!relations.has(childNorm)) {
                relations.set(childNorm, {
                    child,
                    parents: new Map()
                });
            }

            const entry = relations.get(childNorm);
            if (!entry.parents.has(parentNorm)) {
                entry.parents.set(parentNorm, {
                    parent,
                    sources: new Set()
                });
            }

            entry.parents.get(parentNorm).sources.add(source.name);
        }
    }

    const unique = new Map();
    const conflicts = [];

    for (const [childNorm, entry] of relations.entries()) {
        if (entry.parents.size === 1) {
            const [[parentNorm, parentEntry]] = entry.parents.entries();
            unique.set(childNorm, {
                child: entry.child,
                parent: parentEntry.parent,
                parentNorm,
                sources: [...parentEntry.sources].sort()
            });
        } else {
            conflicts.push({
                child: entry.child,
                parents: [...entry.parents.values()]
                    .map(parentEntry => ({
                        parent: parentEntry.parent,
                        sources: [...parentEntry.sources].sort()
                    }))
                    .sort((a, b) => a.parent.localeCompare(b.parent))
            });
        }
    }

    return { unique, conflicts };
}

function buildRowLookup(rows) {
    const lookup = new Map();

    for (const row of rows) {
        const normalized = normalizeText(row.descrip);
        if (!normalized) continue;

        if (!lookup.has(normalized)) {
            lookup.set(normalized, []);
        }

        lookup.get(normalized).push(row);
    }

    return lookup;
}

async function loadTable(connection, tableName) {
    const [rows] = await connection.execute(
        `SELECT codorg, descrip, gerencia FROM ${tableName} ORDER BY codorg`
    );

    return {
        rows,
        lookup: buildRowLookup(rows)
    };
}

function resolveSingleRow(lookup, value) {
    const normalized = normalizeText(value);
    const matches = lookup.get(normalized) || [];

    if (matches.length === 1) {
        return { row: matches[0] };
    }

    if (matches.length > 1) {
        return {
            error: `Hay ${matches.length} registros con descrip "${value}"`
        };
    }

    return {
        error: `No existe registro con descrip "${value}"`
    };
}

async function corregirNivel(connection, config, sources, parentTable, childTable) {
    const { unique, conflicts } = buildUniqueParentMap(sources, config.childKey, config.parentKey);
    const parentData = await loadTable(connection, parentTable);
    const childData = await loadTable(connection, childTable);

    const stats = {
        table: childTable,
        evaluated: 0,
        updates: 0,
        unchanged: 0,
        skipped: 0,
        workbookConflicts: conflicts,
        skippedDetails: [],
        updatesDetails: []
    };

    for (const { child, parent, sources: relationSources } of unique.values()) {
        stats.evaluated += 1;

        const childResolved = resolveSingleRow(childData.lookup, child);
        if (childResolved.error) {
            stats.skipped += 1;
            stats.skippedDetails.push(`${childTable}: ${childResolved.error}`);
            continue;
        }

        const parentResolved = resolveSingleRow(parentData.lookup, parent);
        if (parentResolved.error) {
            stats.skipped += 1;
            stats.skippedDetails.push(`${childTable}: padre no resuelto para "${child}" -> "${parent}" (${parentResolved.error})`);
            continue;
        }

        const childRow = childResolved.row;
        const parentRow = parentResolved.row;

        if (Number(childRow.gerencia) === Number(parentRow.codorg)) {
            stats.unchanged += 1;
            continue;
        }

        if (!dryRun) {
            await connection.execute(
                `UPDATE ${childTable} SET gerencia = ? WHERE codorg = ?`,
                [parentRow.codorg, childRow.codorg]
            );
        }

        stats.updates += 1;
        stats.updatesDetails.push({
            codorg: childRow.codorg,
            descrip: childRow.descrip,
            from: childRow.gerencia,
            to: parentRow.codorg,
            parent: parentRow.descrip,
            sources: relationSources
        });
    }

    return stats;
}

function printStats(stats) {
    console.log(`\n=== ${stats.table} ===`);
    console.log(`Evaluados: ${stats.evaluated}`);
    console.log(`Cambios ${dryRun ? 'detectados' : 'aplicados'}: ${stats.updates}`);
    console.log(`Sin cambios: ${stats.unchanged}`);
    console.log(`Omitidos: ${stats.skipped}`);

    if (stats.workbookConflicts.length > 0) {
        console.log(`Conflictos en Excel (${stats.workbookConflicts.length}):`);
        for (const conflict of stats.workbookConflicts) {
            const parents = conflict.parents
                .map(parent => `${parent.parent} [${parent.sources.join(', ')}]`)
                .join(' | ');
            console.log(` - "${conflict.child}" tiene multiples padres: ${parents}`);
        }
    }

    if (stats.updatesDetails.length > 0) {
        console.log(`Cambios ${dryRun ? 'previstos' : 'realizados'}:`);
        for (const change of stats.updatesDetails) {
            console.log(` - ${change.codorg} "${change.descrip}": ${change.from ?? 'NULL'} -> ${change.to} (${change.parent}) [${change.sources.join(', ')}]`);
        }
    }

    if (stats.skippedDetails.length > 0) {
        console.log('Omitidos por ambiguedad o faltantes:');
        for (const detail of stats.skippedDetails) {
            console.log(` - ${detail}`);
        }
    }
}

async function main() {
    let connection;

    try {
        const sources = [
            {
                name: 'EstructuraOrganizacional.xlsx',
                rows: loadWorkbook('formatos/EstructuraOrganizacional.xlsx', row => ({
                    VP: row.VP,
                    Departamento: row.Departamento,
                    Seccion: row.Seccion,
                    Equipo: row.Equipo,
                    Grupo: row.Grupo
                }))
            },
            {
                name: 'Personal_Al_20102025.xlsx',
                rows: loadWorkbook('formatos/Personal_Al_20102025.xlsx', row => ({
                    VP: row.Vicepresidencia,
                    Departamento: row.Departamento,
                    Seccion: row.Secciones,
                    Equipo: row.Equipo,
                    Grupo: row.Grupo
                }))
            }
        ];

        connection = await mysql.createConnection({
            ...dbConfig,
            connectTimeout: 60000
        });

        console.log('=== Correccion de gerencias organizacionales ===');
        console.log(`Modo: ${dryRun ? 'DRY RUN' : 'APLICAR CAMBIOS'}`);
        console.log('Fuentes: formatos/EstructuraOrganizacional.xlsx + formatos/Personal_Al_20102025.xlsx');
        console.log('No se toca nompersonal; solo nomnivel2..nomnivel5.');

        const configs = [
            { childKey: 'Departamento', parentKey: 'VP', childTable: 'nomnivel2', parentTable: 'nomnivel1' },
            { childKey: 'Seccion', parentKey: 'Departamento', childTable: 'nomnivel3', parentTable: 'nomnivel2' },
            { childKey: 'Equipo', parentKey: 'Seccion', childTable: 'nomnivel4', parentTable: 'nomnivel3' },
            { childKey: 'Grupo', parentKey: 'Equipo', childTable: 'nomnivel5', parentTable: 'nomnivel4' }
        ];

        for (const config of configs) {
            const stats = await corregirNivel(
                connection,
                config,
                sources,
                config.parentTable,
                config.childTable
            );
            printStats(stats);
        }

        console.log(`\n${dryRun ? 'Revise el detalle y luego ejecute con --apply para actualizar.' : 'Correccion completada.'}`);
    } catch (error) {
        console.error('Error corrigiendo gerencias:', error);
        process.exitCode = 1;
    } finally {
        if (connection) {
            await connection.end();
        }
    }
}

main();
