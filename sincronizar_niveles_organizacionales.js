const mysql = require('mysql2/promise');
const xlsx = require('xlsx');
const fs = require('fs');
const dbConfig = require('./dbconfig');
require('dotenv').config();

const dryRun = !process.argv.includes('--apply');

const LEVELS = [
    {
        level: 1,
        table: 'nomnivel1',
        field: 'VP',
        childTable: 'nomnivel2',
        childFk: 'gerencia',
        personalFk: 'codnivel1'
    },
    {
        level: 2,
        table: 'nomnivel2',
        field: 'Departamento',
        parentField: 'VP',
        parentCandidates: [{ field: 'VP', level: 1 }],
        childTable: 'nomnivel3',
        childFk: 'gerencia',
        personalFk: 'codnivel2'
    },
    {
        level: 3,
        table: 'nomnivel3',
        field: 'Seccion',
        parentField: 'Departamento',
        parentCandidates: [{ field: 'Departamento', level: 2 }, { field: 'VP', level: 1 }],
        childTable: 'nomnivel4',
        childFk: 'gerencia',
        personalFk: 'codnivel3'
    },
    {
        level: 4,
        table: 'nomnivel4',
        field: 'Equipo',
        parentField: 'Seccion',
        parentCandidates: [{ field: 'Seccion', level: 3 }, { field: 'Departamento', level: 2 }, { field: 'VP', level: 1 }],
        childTable: 'nomnivel5',
        childFk: 'gerencia',
        personalFk: 'codnivel4'
    },
    {
        level: 5,
        table: 'nomnivel5',
        field: 'Grupo',
        parentField: 'Equipo',
        parentCandidates: [{ field: 'Equipo', level: 4 }, { field: 'Seccion', level: 3 }, { field: 'Departamento', level: 2 }, { field: 'VP', level: 1 }],
        childTable: null,
        childFk: null,
        personalFk: 'codnivel5'
    }
];

const SOURCE_DEFINITIONS = [
    {
        name: 'EstructuraOrganizacional.xlsx',
        path: 'formatos/EstructuraOrganizacional.xlsx',
        map(row) {
            return {
                VP: row.VP,
                Departamento: row.Departamento,
                Seccion: row.Seccion,
                Equipo: row.Equipo,
                Grupo: row.Grupo
            };
        }
    },
    {
        name: 'Personal_Al_20102025.xlsx',
        path: 'formatos/Personal_Al_20102025.xlsx',
        map(row) {
            return {
                VP: row.Vicepresidencia,
                Departamento: row.Departamento,
                Seccion: row.Secciones,
                Equipo: row.Equipo,
                Grupo: row.Grupo
            };
        }
    }
];

const RELATION_OVERRIDES = {
    nomnivel5: {
        [normalizeOverrideKey('20400 Centro de Gestion Operativa')]: {
            parentLevel: 2,
            parentValue: '204 Centro de Gestion Operativa'
        },
        [normalizeOverrideKey('20500 Depto.Plataforma y Estacionamiento')]: {
            parentLevel: 2,
            parentValue: '205 Plataforma y Estacionamiento'
        },
        [normalizeOverrideKey('50200 Depto.Recursos Financieros')]: {
            parentLevel: 2,
            parentValue: '502 Recursos Financieros'
        },
        [normalizeOverrideKey('50300 Depto.Recursos Logísticos')]: {
            parentLevel: 2,
            parentValue: '503 Recursos Log¡sticos'
        }
    }
};

function normalizeOverrideKey(value) {
    return value
        .toString()
        .trim()
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/ñ/gi, 'n')
        .replace(/Ñ/gi, 'N')
        .replace(/¡/g, 'i')
        .replace(/[òóôõö]/gi, 'o')
        .replace(/[àáâãäå]/gi, 'a')
        .replace(/[èéêë]/gi, 'e')
        .replace(/[ìíîï]/gi, 'i')
        .replace(/[ùúûü]/gi, 'u')
        .replace(/\s+/g, ' ')
        .toUpperCase();
}

function stripSourcePrefix(value) {
    if (value === null || value === undefined) return null;
    const str = value.toString().trim();
    if (str.startsWith('X ')) {
        return str.substring(2).trim();
    }
    return str;
}

function normalizeText(text) {
    if (text === null || text === undefined) return '';

    return stripSourcePrefix(text)
        .toString()
        .trim()
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
    const stripped = stripSourcePrefix(value);
    if (stripped === null || stripped === undefined) return null;

    const str = stripped.toString().trim();
    if (!str) return null;
    if (normalizeText(str) === 'NULL') return null;

    return str;
}

function hasAccents(value) {
    return /[\u00C0-\u017F]/.test(value);
}

function sourcePriority(sourceName) {
    return sourceName === 'EstructuraOrganizacional.xlsx' ? 2 : 1;
}

function shortDescription(value) {
    const cleaned = sanitizeValue(value);
    if (!cleaned) return null;

    const patterns = [
        /^\d+(?:-\d+)*(?:-\d+)*(?:-\d+)?\s+(.+)$/,
        /^\d+\s+(.+)$/,
        /^([A-Za-z].+)$/
    ];

    for (const pattern of patterns) {
        const match = cleaned.match(pattern);
        if (match) {
            return (match[1] || cleaned).trim();
        }
    }

    return cleaned;
}

function loadWorkbookRows() {
    const sources = [];

    for (const sourceDef of SOURCE_DEFINITIONS) {
        const workbook = xlsx.readFile(sourceDef.path);
        const sheet = workbook.Sheets[workbook.SheetNames[0]];
        const rows = xlsx.utils.sheet_to_json(sheet, { defval: '' }).map(sourceDef.map);
        sources.push({
            name: sourceDef.name,
            rows
        });
    }

    return sources;
}

function rebuildStateFromRows(rows) {
    const byKey = new Map();
    const byComposite = new Map();
    let maxCodorg = 0;

    for (const row of rows) {
        const key = normalizeText(row.descrip);
        if (!byKey.has(key)) {
            byKey.set(key, []);
        }
        byKey.get(key).push(row);

        if (row.gerencia !== null && row.gerencia !== undefined && row.gerencia !== '') {
            byComposite.set(`${key}::${row.gerencia}`, row);
        }

        maxCodorg = Math.max(maxCodorg, Number(row.codorg));
    }

    return { rows, byKey, byComposite, maxCodorg };
}

async function refreshState(connection, table, levelState) {
    if (dryRun) {
        return rebuildStateFromRows(levelState.rows);
    }

    return loadStateForLevel(connection, table);
}

function buildSourceModel(sources) {
    const nodesByLevel = new Map();
    const relationsByLevel = new Map();

    for (const config of LEVELS) {
        nodesByLevel.set(config.level, new Map());
        if (config.level > 1) {
            relationsByLevel.set(config.level, new Map());
        }
    }

    for (const source of sources) {
        for (const row of source.rows) {
            for (const config of LEVELS) {
                const rawValue = sanitizeValue(row[config.field]);
                if (!rawValue) continue;

                const key = normalizeText(rawValue);
                const variants = nodesByLevel.get(config.level);

                if (!variants.has(key)) {
                    variants.set(key, {
                        key,
                        variants: new Map()
                    });
                }

                const entry = variants.get(key);
                if (!entry.variants.has(rawValue)) {
                    entry.variants.set(rawValue, {
                        value: rawValue,
                        count: 0,
                        sources: new Set()
                    });
                }

                const variant = entry.variants.get(rawValue);
                variant.count += 1;
                variant.sources.add(source.name);
            }

            for (const config of LEVELS.filter(item => item.level > 1)) {
                const child = sanitizeValue(row[config.field]);
                if (!child) continue;

                let parent = null;
                let parentLevel = null;

                for (const candidate of config.parentCandidates) {
                    const candidateValue = sanitizeValue(row[candidate.field]);
                    if (candidateValue) {
                        parent = candidateValue;
                        parentLevel = candidate.level;
                        break;
                    }
                }

                if (!parent || !parentLevel) continue;

                const childKey = normalizeText(child);
                const parentKey = normalizeText(parent);
                const relations = relationsByLevel.get(config.level);

                if (!relations.has(childKey)) {
                    relations.set(childKey, {
                        child,
                        parentOptions: new Map()
                    });
                }

                const relation = relations.get(childKey);
                const optionKey = `${parentLevel}:${parentKey}`;

                if (!relation.parentOptions.has(optionKey)) {
                    relation.parentOptions.set(optionKey, {
                        key: parentKey,
                        optionKey,
                        value: parent,
                        parentLevel,
                        count: 0,
                        sources: new Set()
                    });
                }

                const parentOption = relation.parentOptions.get(optionKey);
                parentOption.count += 1;
                parentOption.sources.add(source.name);
            }
        }
    }

    const canonicalNodes = new Map();
    const canonicalRelations = new Map();
    const relationConflicts = new Map();

    for (const config of LEVELS) {
        const nodes = nodesByLevel.get(config.level);
        const canonicalMap = new Map();

        for (const [key, entry] of nodes.entries()) {
            const variants = [...entry.variants.values()];
            variants.sort((a, b) => {
                if (Number(hasAccents(b.value)) !== Number(hasAccents(a.value))) {
                    return Number(hasAccents(b.value)) - Number(hasAccents(a.value));
                }
                const aSource = Math.max(...[...a.sources].map(sourcePriority));
                const bSource = Math.max(...[...b.sources].map(sourcePriority));
                if (bSource !== aSource) return bSource - aSource;
                if (b.count !== a.count) return b.count - a.count;
                return a.value.localeCompare(b.value, 'es');
            });

            canonicalMap.set(key, {
                key,
                value: variants[0].value,
                variants: variants.map(item => ({
                    value: item.value,
                    count: item.count,
                    sources: [...item.sources].sort()
                }))
            });
        }

        canonicalNodes.set(config.level, canonicalMap);
    }

    for (const config of LEVELS.filter(item => item.level > 1)) {
        const relations = relationsByLevel.get(config.level);
        const canonicalMap = new Map();
        const conflicts = [];
        const overrides = RELATION_OVERRIDES[config.table] || {};

        for (const [childKey, relation] of relations.entries()) {
            if (overrides[childKey]) {
                canonicalMap.set(childKey, {
                    childKey,
                    childValue: relation.child,
                    parentKey: normalizeText(overrides[childKey].parentValue),
                    parentValue: overrides[childKey].parentValue,
                    parentLevel: overrides[childKey].parentLevel,
                    count: null,
                    sources: ['OVERRIDE']
                });
                continue;
            }

            const options = [...relation.parentOptions.values()].sort((a, b) => {
                if (b.parentLevel !== a.parentLevel) return b.parentLevel - a.parentLevel;
                const aSource = Math.max(...[...a.sources].map(sourcePriority));
                const bSource = Math.max(...[...b.sources].map(sourcePriority));
                if (bSource !== aSource) return bSource - aSource;
                if (b.count !== a.count) return b.count - a.count;
                return a.value.localeCompare(b.value, 'es');
            });

            const bestLevel = options[0].parentLevel;
            const bestLevelOptions = options.filter(option => option.parentLevel === bestLevel);

            if (bestLevelOptions.length === 1) {
                canonicalMap.set(childKey, {
                    childKey,
                    childValue: relation.child,
                    parentKey: bestLevelOptions[0].key,
                    parentValue: bestLevelOptions[0].value,
                    parentLevel: bestLevelOptions[0].parentLevel,
                    count: bestLevelOptions[0].count,
                    sources: [...bestLevelOptions[0].sources].sort()
                });
            } else {
                const bestOption = chooseBestConflictOption(bestLevelOptions);

                if (bestOption) {
                    canonicalMap.set(childKey, {
                        childKey,
                        childValue: relation.child,
                        parentKey: bestOption.key,
                        parentValue: bestOption.value,
                        parentLevel: bestOption.parentLevel,
                        count: bestOption.count,
                        sources: [...bestOption.sources].sort(),
                        autoResolved: true,
                        autoResolvedReason: `mayoria ${bestOption.count}/${bestLevelOptions.reduce((sum, option) => sum + option.count, 0)}`
                    });
                } else {
                    conflicts.push({
                        childKey,
                        childValue: relation.child,
                        parents: bestLevelOptions.map(option => ({
                            key: option.key,
                            value: option.value,
                            parentLevel: option.parentLevel,
                            count: option.count,
                            sources: [...option.sources].sort()
                        }))
                    });
                }
            }
        }

        canonicalRelations.set(config.level, canonicalMap);
        relationConflicts.set(config.level, conflicts);
    }

    return {
        canonicalNodes,
        canonicalRelations,
        relationConflicts
    };
}

async function loadTableData(connection, table) {
    const [rows] = await connection.execute(`SELECT codorg, descrip, gerencia FROM ${table} ORDER BY codorg`);
    return rows;
}

async function countReferences(connection, config, codorg) {
    let childRefs = 0;
    let personalRefs = 0;

    if (config.childTable) {
        const [rows] = await connection.execute(
            `SELECT COUNT(*) AS total FROM ${config.childTable} WHERE ${config.childFk} = ?`,
            [codorg]
        );
        childRefs = Number(rows[0].total || 0);
    }

    const [personalRows] = await connection.execute(
        `SELECT COUNT(*) AS total FROM nompersonal WHERE ${config.personalFk} = ?`,
        [codorg]
    );
    personalRefs = Number(personalRows[0].total || 0);

    return { childRefs, personalRefs, totalRefs: childRefs + personalRefs };
}

async function resolveDuplicates(connection, config, canonicalMap, state, report) {
    const rows = await loadTableData(connection, config.table);
    const grouped = new Map();

    for (const row of rows) {
        const key = config.level === 1
            ? normalizeText(row.descrip)
            : `${normalizeText(row.descrip)}::${row.gerencia ?? ''}`;
        if (!grouped.has(key)) {
            grouped.set(key, []);
        }
        grouped.get(key).push(row);
    }

    for (const [key, group] of grouped.entries()) {
        if (group.length === 1) continue;

        const canonical = canonicalMap.get(normalizeText(group[0].descrip));
        const rowsWithRefs = [];

        for (const row of group) {
            const refs = await countReferences(connection, config, row.codorg);
            rowsWithRefs.push({ row, refs });
        }

        rowsWithRefs.sort((a, b) => {
            if (b.refs.totalRefs !== a.refs.totalRefs) return b.refs.totalRefs - a.refs.totalRefs;
            if (Number(canonical && b.row.descrip === canonical.value) !== Number(canonical && a.row.descrip === canonical.value)) {
                return Number(canonical && b.row.descrip === canonical.value) - Number(canonical && a.row.descrip === canonical.value);
            }
            return a.row.codorg - b.row.codorg;
        });

        const keeper = rowsWithRefs[0];
        const duplicates = rowsWithRefs.slice(1);

        report.duplicates.push({
            table: config.table,
            normalizedKey: key,
            keeper: {
                codorg: keeper.row.codorg,
                descrip: keeper.row.descrip,
                gerencia: keeper.row.gerencia,
                refs: keeper.refs
            },
            duplicates: duplicates.map(item => ({
                codorg: item.row.codorg,
                descrip: item.row.descrip,
                gerencia: item.row.gerencia,
                refs: item.refs
            }))
        });

        if (canonical && keeper.row.descrip !== canonical.value) {
            report.descriptionUpdates.push({
                table: config.table,
                codorg: keeper.row.codorg,
                from: keeper.row.descrip,
                to: canonical.value,
                reason: 'canonizar duplicado'
            });

            if (!dryRun) {
                await connection.execute(
                    `UPDATE ${config.table} SET descrip = ? WHERE codorg = ?`,
                    [canonical.value, keeper.row.codorg]
                );
            }

            keeper.row.descrip = canonical.value;

            const stateRow = state[config.level].rows.find(row => Number(row.codorg) === Number(keeper.row.codorg));
            if (stateRow) {
                stateRow.descrip = canonical.value;
            }
        }

        for (const duplicate of duplicates) {
            if (config.childTable && duplicate.refs.childRefs > 0) {
                report.childRepoints.push({
                    childTable: config.childTable,
                    fromCodorg: duplicate.row.codorg,
                    toCodorg: keeper.row.codorg,
                    count: duplicate.refs.childRefs
                });

                if (!dryRun) {
                    await connection.execute(
                        `UPDATE ${config.childTable} SET ${config.childFk} = ? WHERE ${config.childFk} = ?`,
                        [keeper.row.codorg, duplicate.row.codorg]
                    );
                }
            }

            if (duplicate.refs.personalRefs === 0) {
                report.deletedDuplicates.push({
                    table: config.table,
                    codorg: duplicate.row.codorg,
                    descrip: duplicate.row.descrip
                });

                if (!dryRun) {
                    await connection.execute(
                        `DELETE FROM ${config.table} WHERE codorg = ?`,
                        [duplicate.row.codorg]
                    );
                }
            } else {
                report.blockedDuplicates.push({
                    table: config.table,
                    codorg: duplicate.row.codorg,
                    descrip: duplicate.row.descrip,
                    personalRefs: duplicate.refs.personalRefs
                });
            }
        }
    }

    if (dryRun) {
        const deleted = new Set(report.deletedDuplicates.filter(item => item.table === config.table).map(item => item.codorg));
        state[config.level].rows = state[config.level].rows.filter(row => !deleted.has(row.codorg));
    }

    state[config.level] = await refreshState(connection, config.table, state[config.level]);
}

async function consolidateRowsToExpectedParent(connection, config, key, expectedParentCodorg, state, report) {
    const rows = state[config.level].byKey.get(key) || [];
    if (rows.length <= 1) {
        return rows[0] || null;
    }

    const rowsWithRefs = [];
    for (const row of rows) {
        const refs = await countReferences(connection, config, row.codorg);
        rowsWithRefs.push({ row, refs });
    }

    rowsWithRefs.sort((a, b) => {
        if (b.refs.totalRefs !== a.refs.totalRefs) return b.refs.totalRefs - a.refs.totalRefs;
        if (Number(b.row.gerencia === expectedParentCodorg) !== Number(a.row.gerencia === expectedParentCodorg)) {
            return Number(b.row.gerencia === expectedParentCodorg) - Number(a.row.gerencia === expectedParentCodorg);
        }
        return a.row.codorg - b.row.codorg;
    });

    const keeper = rowsWithRefs[0];

    if (Number(keeper.row.gerencia) !== Number(expectedParentCodorg)) {
        report.parentUpdates.push({
            table: config.table,
            codorg: keeper.row.codorg,
            descrip: keeper.row.descrip,
            from: keeper.row.gerencia,
            to: expectedParentCodorg,
            parent: `codorg ${expectedParentCodorg}`
        });

        if (!dryRun) {
            await connection.execute(
                `UPDATE ${config.table} SET gerencia = ? WHERE codorg = ?`,
                [expectedParentCodorg, keeper.row.codorg]
            );
        }

        keeper.row.gerencia = expectedParentCodorg;
    }

    for (const item of rowsWithRefs.slice(1)) {
        if (config.childTable && item.refs.childRefs > 0) {
            report.childRepoints.push({
                childTable: config.childTable,
                fromCodorg: item.row.codorg,
                toCodorg: keeper.row.codorg,
                count: item.refs.childRefs
            });

            if (!dryRun) {
                await connection.execute(
                    `UPDATE ${config.childTable} SET ${config.childFk} = ? WHERE ${config.childFk} = ?`,
                    [keeper.row.codorg, item.row.codorg]
                );
            }
        }

        if (item.refs.personalRefs === 0) {
            report.deletedDuplicates.push({
                table: config.table,
                codorg: item.row.codorg,
                descrip: item.row.descrip
            });

            if (!dryRun) {
                await connection.execute(
                    `DELETE FROM ${config.table} WHERE codorg = ?`,
                    [item.row.codorg]
                );
            }
        } else {
            report.blockedDuplicates.push({
                table: config.table,
                codorg: item.row.codorg,
                descrip: item.row.descrip,
                personalRefs: item.refs.personalRefs
            });
        }
    }

    state[config.level] = await refreshState(connection, config.table, state[config.level]);
    return rowByParentFromState(state[config.level], key, expectedParentCodorg) || singleRowFromState(state[config.level], key);
}

async function loadStateForLevel(connection, table) {
    const rows = await loadTableData(connection, table);
    return rebuildStateFromRows(rows);
}

function singleRowFromState(levelState, key) {
    const rows = levelState.byKey.get(key) || [];
    if (rows.length === 1) return rows[0];
    return null;
}

function rowByParentFromState(levelState, key, parentCodorg) {
    return levelState.byComposite.get(`${key}::${parentCodorg}`) || null;
}

async function resolveExpectedRow(connection, level, key, sourceModel, state, report) {
    if (level === 1) {
        const rows = state[level].byKey.get(key) || [];
        if (rows.length === 1) return rows[0];
        return null;
    }

    const config = LEVELS.find(item => item.level === level);
    const relation = sourceModel.canonicalRelations.get(level).get(key);
    if (!relation) {
        return null;
    }

    const parentLevel = relation.parentLevel || (level - 1);
    const parentRow = await resolveExpectedRow(connection, parentLevel, relation.parentKey, sourceModel, state, report);
    if (!parentRow) {
        return null;
    }

    let row = rowByParentFromState(state[level], key, parentRow.codorg);
    if (row) return row;

    const rowsByName = state[level].byKey.get(key) || [];
    if (rowsByName.length > 1) {
        row = await consolidateRowsToExpectedParent(connection, config, key, parentRow.codorg, state, report);
        if (row) return row;
    }

    if (rowsByName.length === 1) {
        return rowsByName[0];
    }

    return null;
}

async function insertLevelRow(connection, config, state, report, descrip, parentCodorg, reason) {
    state[config.level].maxCodorg += 1;
    const codorg = state[config.level].maxCodorg;

    report.inserts.push({
        table: config.table,
        codorg,
        descrip,
        gerencia: parentCodorg,
        reason
    });

    if (!dryRun) {
        if (config.level === 5) {
            await connection.execute(
                'INSERT INTO nomnivel5 (codorg, descrip, gerencia) VALUES (?, ?, ?)',
                [codorg, descrip, parentCodorg]
            );
        } else {
            await connection.execute(
                `INSERT INTO ${config.table} (codorg, descrip, gerencia, descripcion_corta) VALUES (?, ?, ?, ?)`,
                [codorg, descrip, parentCodorg, shortDescription(descrip)]
            );
        }
    }

    state[config.level].rows.push({ codorg, descrip, gerencia: parentCodorg });
    state[config.level] = await refreshState(connection, config.table, state[config.level]);
    return rowByParentFromState(state[config.level], normalizeText(descrip), parentCodorg);
}

async function ensureConflictRows(connection, config, conflicts, state, report) {
    for (const conflict of conflicts) {
        const childKey = normalizeText(conflict.childValue);
        const childRows = state[config.level].byKey.get(childKey) || [];

        report.conflicts.push({
            table: config.table,
            child: conflict.childValue,
            parents: conflict.parents.map(parent => ({
                value: parent.value,
                parentLevel: parent.parentLevel,
                count: parent.count,
                sources: parent.sources
            }))
        });

        for (const parent of conflict.parents) {
            const parentLevel = parent.parentLevel || (config.level - 1);
            const parentRow = singleRowFromState(state[parentLevel], normalizeText(parent.value));

            if (!parentRow) {
                report.unresolved.push({
                    table: config.table,
                    descrip: conflict.childValue,
                    reason: `padre no disponible para desdoblar: ${parent.value}`
                });
                continue;
            }

            let childRow = rowByParentFromState(state[config.level], childKey, parentRow.codorg);

            if (!childRow) {
                const existingSameParent = childRows.find(row => Number(row.gerencia) === Number(parentRow.codorg));
                childRow = existingSameParent || await insertLevelRow(
                    connection,
                    config,
                    state,
                    report,
                    conflict.childValue,
                    parentRow.codorg,
                    'desdoble por multiples padres'
                );
            }

            if (childRow && childRow.descrip !== conflict.childValue) {
                report.descriptionUpdates.push({
                    table: config.table,
                    codorg: childRow.codorg,
                    from: childRow.descrip,
                    to: conflict.childValue,
                    reason: 'canonizar desdoble'
                });

                if (!dryRun) {
                    await connection.execute(
                        `UPDATE ${config.table} SET descrip = ? WHERE codorg = ?`,
                        [conflict.childValue, childRow.codorg]
                    );
                }

                childRow.descrip = conflict.childValue;
                state[config.level] = await refreshState(connection, config.table, state[config.level]);
            }
        }
    }
}

async function ensureLevelRows(connection, config, sourceModel, state, report) {
    const canonicalNodes = sourceModel.canonicalNodes.get(config.level);
    const canonicalRelations = sourceModel.canonicalRelations.get(config.level);
    const conflicts = sourceModel.relationConflicts.get(config.level) || [];

    if (conflicts.length > 0) {
        await ensureConflictRows(connection, config, conflicts, state, report);
    }

    for (const [key, node] of canonicalNodes.entries()) {
        let row = config.level === 1 ? singleRowFromState(state[config.level], key) : null;

        if (!row && config.level === 1) {
            state[config.level].maxCodorg += 1;
            const codorg = state[config.level].maxCodorg;

            report.inserts.push({
                table: config.table,
                codorg,
                descrip: node.value,
                gerencia: null
            });

            if (!dryRun) {
                await connection.execute(
                    'INSERT INTO nomnivel1 (codorg, descrip, descripcion_corta) VALUES (?, ?, ?)',
                    [codorg, node.value, shortDescription(node.value)]
                );
            }

            row = { codorg, descrip: node.value, gerencia: null };
            state[config.level].rows.push(row);
            state[config.level].byKey.set(key, [row]);
            continue;
        }

        if (!row && config.level > 1) {
            const relation = canonicalRelations.get(key);

            if (!relation) {
                continue;
            }

            const targetParentLevel = relation.parentLevel || (config.level - 1);
            const parentRow = await resolveExpectedRow(connection, targetParentLevel, relation.parentKey, sourceModel, state, report);
            if (!parentRow) {
                report.unresolved.push({
                    table: config.table,
                    descrip: node.value,
                    reason: `padre no disponible: ${relation.parentValue}`
                });
                continue;
            }

            row = rowByParentFromState(state[config.level], key, parentRow.codorg);
            if (!row) {
                const rowsByName = state[config.level].byKey.get(key) || [];
                if (rowsByName.length === 1) {
                    row = rowsByName[0];
                }
            }
            if (!row) {
                row = await insertLevelRow(connection, config, state, report, node.value, parentRow.codorg, 'faltante segun Excel');
            }
        }
    }

    state[config.level] = await refreshState(connection, config.table, state[config.level]);
}

async function synchronizeDescriptionsAndParents(connection, config, sourceModel, state, report) {
    const canonicalNodes = sourceModel.canonicalNodes.get(config.level);
    const canonicalRelations = sourceModel.canonicalRelations.get(config.level);

    for (const [key, node] of canonicalNodes.entries()) {
        const rows = state[config.level].byKey.get(key) || [];
        if (rows.length === 0) continue;

        for (const row of rows) {
            if (row.descrip === node.value) continue;

            report.descriptionUpdates.push({
                table: config.table,
                codorg: row.codorg,
                from: row.descrip,
                to: node.value,
                reason: 'canonizar descripcion'
            });

            if (!dryRun) {
                await connection.execute(
                    `UPDATE ${config.table} SET descrip = ? WHERE codorg = ?`,
                    [node.value, row.codorg]
                );
            }

            row.descrip = node.value;
        }

        if (config.level === 1) {
            continue;
        }

        const relation = canonicalRelations.get(key);
        if (!relation) continue;

        const targetParentLevel = relation.parentLevel || (config.level - 1);
        const parentRow = await resolveExpectedRow(connection, targetParentLevel, relation.parentKey, sourceModel, state, report);
        if (!parentRow) {
            report.unresolved.push({
                table: config.table,
                descrip: node.value,
                reason: `padre no disponible para ajustar gerencia: ${relation.parentValue}`
            });
            continue;
        }

        let row = rowByParentFromState(state[config.level], key, parentRow.codorg);
        if (!row) {
            const rowsByName = state[config.level].byKey.get(key) || [];
            if (rowsByName.length > 1) {
                row = await consolidateRowsToExpectedParent(connection, config, key, parentRow.codorg, state, report);
            } else if (rowsByName.length === 1) {
                row = rowsByName[0];
            }
        }
        if (!row) continue;

        if (relation.autoResolved) {
            const alreadyLogged = report.autoResolvedConflicts.some(item => item.table === config.table && item.child === row.descrip);
            if (!alreadyLogged) {
                report.autoResolvedConflicts.push({
                    table: config.table,
                    child: row.descrip,
                    parent: relation.parentValue,
                    parentLevel: relation.parentLevel,
                    reason: relation.autoResolvedReason
                });
            }
        }

        if (Number(row.gerencia) !== Number(parentRow.codorg)) {
            report.parentUpdates.push({
                table: config.table,
                codorg: row.codorg,
                descrip: row.descrip,
                from: row.gerencia,
                to: parentRow.codorg,
                parent: parentRow.descrip
            });

            if (!dryRun) {
                await connection.execute(
                    `UPDATE ${config.table} SET gerencia = ? WHERE codorg = ?`,
                    [parentRow.codorg, row.codorg]
                );
            }

            row.gerencia = parentRow.codorg;
        }
    }

    state[config.level] = await refreshState(connection, config.table, state[config.level]);
}

function printSection(title, items, formatter) {
    if (items.length === 0) return;
    console.log(`\n${title}: ${items.length}`);
    for (const item of items) {
        console.log(` - ${formatter(item)}`);
    }
}

function chooseBestConflictOption(options) {
    if (!options || options.length === 0) return null;

    const sorted = [...options].sort((a, b) => {
        if (b.parentLevel !== a.parentLevel) return b.parentLevel - a.parentLevel;
        if (b.count !== a.count) return b.count - a.count;
        const aSource = Math.max(...[...a.sources].map(sourcePriority));
        const bSource = Math.max(...[...b.sources].map(sourcePriority));
        if (bSource !== aSource) return bSource - aSource;
        return a.value.localeCompare(b.value, 'es');
    });

    const best = sorted[0];
    const sameLevel = sorted.filter(option => option.parentLevel === best.parentLevel);
    const second = sameLevel[1];

    if (sameLevel.length === 1) return best;
    if (best.count >= 3 && (!second || best.count >= second.count * 2)) return best;

    return null;
}

async function syncNompersonalNivel4FromNivel5(connection, report) {
    const sql = `
        SELECT
            np.personal_id,
            np.ficha,
            np.numero_carnet,
            np.apenom,
            np.estado,
            np.codnivel3,
            np.codnivel4,
            np.codnivel5,
            n5.descrip AS nivel5_descrip,
            n5.gerencia AS expected_codnivel4,
            n4.descrip AS nivel4_descrip,
            n4.gerencia AS expected_codnivel3,
            EXISTS(
                SELECT 1
                FROM expediente e
                WHERE e.personal_id = np.personal_id
                  AND (
                    e.codnivel1_nuevo IS NOT NULL OR
                    e.codnivel2_nuevo IS NOT NULL OR
                    e.codnivel3_nuevo IS NOT NULL OR
                    e.codnivel4_nuevo IS NOT NULL OR
                    e.codnivel5_nuevo IS NOT NULL
                  )
            ) AS has_expediente_change
        FROM nompersonal np
        INNER JOIN nomnivel5 n5 ON n5.codorg = np.codnivel5
        LEFT JOIN nomnivel4 n4 ON n4.codorg = n5.gerencia
        WHERE COALESCE(np.estado, '') <> 'De Baja'
          AND np.codnivel5 IS NOT NULL
          AND np.codnivel5 <> ''
          AND (np.codnivel4 IS NULL OR np.codnivel4 = '')
        ORDER BY np.ficha
    `;

    const [rows] = await connection.execute(sql);

    for (const row of rows) {
        if (Number(row.has_expediente_change) === 1) {
            report.personalSkippedByExpediente.push({
                ficha: row.ficha,
                numero_carnet: row.numero_carnet,
                apenom: row.apenom,
                nivel5: row.nivel5_descrip
            });
            continue;
        }

        if (!row.expected_codnivel4) {
            report.personalSkippedNoParent.push({
                ficha: row.ficha,
                numero_carnet: row.numero_carnet,
                apenom: row.apenom,
                nivel5: row.nivel5_descrip,
                reason: 'nivel5 sin gerencia en nomnivel5'
            });
            continue;
        }

        if (row.codnivel3 && row.expected_codnivel3 && Number(row.codnivel3) !== Number(row.expected_codnivel3)) {
            report.personalSkippedMismatch.push({
                ficha: row.ficha,
                numero_carnet: row.numero_carnet,
                apenom: row.apenom,
                nivel5: row.nivel5_descrip,
                nivel4: row.nivel4_descrip,
                codnivel3_actual: row.codnivel3,
                codnivel3_esperado: row.expected_codnivel3
            });
            continue;
        }

        report.personalLevel4Updates.push({
            personal_id: row.personal_id,
            ficha: row.ficha,
            numero_carnet: row.numero_carnet,
            apenom: row.apenom,
            from: row.codnivel4,
            to: row.expected_codnivel4,
            nivel4: row.nivel4_descrip,
            nivel5: row.nivel5_descrip
        });

        if (!dryRun) {
            await connection.execute(
                'UPDATE nompersonal SET codnivel4 = ? WHERE personal_id = ?',
                [row.expected_codnivel4, row.personal_id]
            );
        }
    }
}

function getLevelByTable(tableName) {
    const config = LEVELS.find(item => item.table === tableName);
    return config ? config.level : null;
}

async function loadActivePersonnelCounts(connection) {
    const countsByLevel = new Map();

    for (const config of LEVELS) {
        const [rows] = await connection.execute(
            `SELECT ${config.personalFk} AS codorg, COUNT(*) AS total FROM nompersonal WHERE COALESCE(estado, '') <> 'De Baja' AND ${config.personalFk} IS NOT NULL AND ${config.personalFk} <> '' GROUP BY ${config.personalFk}`
        );

        const levelMap = new Map();
        for (const row of rows) {
            levelMap.set(String(row.codorg), Number(row.total));
        }

        countsByLevel.set(config.level, levelMap);
    }

    return countsByLevel;
}

function sumActivePersonnel(rows, countsMap) {
    if (!rows || rows.length === 0 || !countsMap) return 0;
    return rows.reduce((sum, row) => sum + (countsMap.get(String(row.codorg)) || 0), 0);
}

async function annotateConflictsWithActivePersonnel(connection, state, report) {
    const activeCounts = await loadActivePersonnelCounts(connection);

    for (const conflict of report.conflicts) {
        const childLevel = getLevelByTable(conflict.table);
        const childRows = childLevel ? (state[childLevel].byKey.get(normalizeText(conflict.child)) || []) : [];

        conflict.childLevel = childLevel;
        conflict.childCodorgs = childRows.map(row => row.codorg);
        conflict.childActivePersonnel = sumActivePersonnel(childRows, activeCounts.get(childLevel));

        for (const parent of conflict.parents) {
            const parentLevel = parent.parentLevel || (childLevel ? childLevel - 1 : null);
            const parentRows = parentLevel ? (state[parentLevel].byKey.get(normalizeText(parent.value)) || []) : [];

            parent.parentLevel = parentLevel;
            parent.codorgs = parentRows.map(row => row.codorg);
            parent.activePersonnel = sumActivePersonnel(parentRows, activeCounts.get(parentLevel));
        }
    }
}

function writeManualReviewLog(report) {
    const lines = [];
    lines.push('REPORTE DE REVISION MANUAL');
    lines.push(`Modo: ${dryRun ? 'DRY RUN' : 'APLICAR CAMBIOS'}`);
    lines.push(`Fecha: ${new Date().toISOString()}`);
    lines.push('');

    lines.push(`Conflictos desdoblados por multiples padres: ${report.conflicts.length}`);
    for (const item of report.conflicts) {
        lines.push(`- ${item.table} | ${item.child} | nivel hijo ${item.childLevel ?? 'N/D'} | personal activo asociado ${item.childActivePersonnel ?? 0}`);
        for (const parent of item.parents) {
            lines.push(`  * ${parent.value} | nivel padre ${parent.parentLevel ?? 'N/D'} | apariciones ${parent.count} | personal activo asociado ${parent.activePersonnel ?? 0} | fuentes ${parent.sources.join(', ')}`);
        }
    }

    lines.push('');
    lines.push(`Pendientes por resolver: ${report.unresolved.length}`);
    for (const item of report.unresolved) {
        lines.push(`- ${item.table} | ${item.descrip} | ${item.reason}`);
    }

    lines.push('');
    lines.push(`Conflictos auto-resueltos por mayoria: ${report.autoResolvedConflicts.length}`);
    for (const item of report.autoResolvedConflicts) {
        lines.push(`- ${item.table} | ${item.child} -> ${item.parent} | nivel padre ${item.parentLevel} | razon ${item.reason}`);
    }

    lines.push('');
    lines.push(`Nompersonal omitidos por expediente: ${report.personalSkippedByExpediente.length}`);
    for (const item of report.personalSkippedByExpediente) {
        lines.push(`- ficha ${item.ficha} | ${item.numero_carnet} | ${item.apenom} | ${item.nivel5}`);
    }

    lines.push('');
    lines.push(`Nompersonal omitidos por inconsistencia jerarquica: ${report.personalSkippedMismatch.length}`);
    for (const item of report.personalSkippedMismatch) {
        lines.push(`- ficha ${item.ficha} | ${item.numero_carnet} | ${item.apenom} | nivel5 ${item.nivel5} | nivel4 esperado ${item.nivel4} | codnivel3 actual ${item.codnivel3_actual} | codnivel3 esperado ${item.codnivel3_esperado}`);
    }

    lines.push('');
    lines.push(`Nompersonal omitidos por padre faltante: ${report.personalSkippedNoParent.length}`);
    for (const item of report.personalSkippedNoParent) {
        lines.push(`- ficha ${item.ficha} | ${item.numero_carnet} | ${item.apenom} | ${item.nivel5} | ${item.reason}`);
    }

    fs.writeFileSync('log_revision_manual_niveles.txt', `${lines.join('\n')}\n`, 'utf8');
}

function printReport(report) {
    console.log('\n=== REPORTE ===');
    printSection('Inserciones', report.inserts, item => `${item.table} ${item.codorg} "${item.descrip}" -> gerencia ${item.gerencia ?? 'NULL'}`);
    printSection('Ajustes de gerencia', report.parentUpdates, item => `${item.table} ${item.codorg} "${item.descrip}": ${item.from ?? 'NULL'} -> ${item.to} (${item.parent})`);
    printSection('Ajustes de descripcion', report.descriptionUpdates, item => `${item.table} ${item.codorg}: "${item.from}" -> "${item.to}" (${item.reason})`);
    printSection('Ajustes en nompersonal.codnivel4', report.personalLevel4Updates, item => `ficha ${item.ficha} ${item.apenom}: ${item.from ?? 'NULL'} -> ${item.to} (${item.nivel4})`);
    printSection('Conflictos auto-resueltos', report.autoResolvedConflicts, item => `${item.table} "${item.child}" -> ${item.parent} (${item.reason})`);
    printSection('Reapuntes de hijos', report.childRepoints, item => `${item.childTable}: ${item.count} registros ${item.fromCodorg} -> ${item.toCodorg}`);
    printSection('Duplicados eliminables/eliminados', report.deletedDuplicates, item => `${item.table} ${item.codorg} "${item.descrip}"`);
    printSection('Duplicados bloqueados por nompersonal', report.blockedDuplicates, item => `${item.table} ${item.codorg} "${item.descrip}" con ${item.personalRefs} referencias en nompersonal`);
    printSection('Conflictos desdoblados', report.conflicts, item => `${item.table} "${item.child}" (activos: ${item.childActivePersonnel ?? 0}) => ${item.parents.map(parent => `${parent.value} [nivel ${parent.parentLevel ?? 'N/D'}, activos ${parent.activePersonnel ?? 0}]`).join(' | ')}`);
    printSection('Pendientes por resolver', report.unresolved, item => `${item.table} "${item.descrip}": ${item.reason}`);
    printSection('Nompersonal omitidos por expediente', report.personalSkippedByExpediente, item => `ficha ${item.ficha} ${item.apenom} (${item.nivel5})`);
    printSection('Nompersonal omitidos por inconsistencia', report.personalSkippedMismatch, item => `ficha ${item.ficha} ${item.apenom} (${item.nivel5})`);
    printSection('Nompersonal omitidos por padre faltante', report.personalSkippedNoParent, item => `ficha ${item.ficha} ${item.apenom}: ${item.reason}`);
    console.log('\nLog de revision manual: log_revision_manual_niveles.txt');
}

async function main() {
    let connection;

    const report = {
        inserts: [],
        parentUpdates: [],
        descriptionUpdates: [],
        childRepoints: [],
        deletedDuplicates: [],
        blockedDuplicates: [],
        duplicates: [],
        conflicts: [],
        unresolved: [],
        autoResolvedConflicts: [],
        personalLevel4Updates: [],
        personalSkippedByExpediente: [],
        personalSkippedMismatch: [],
        personalSkippedNoParent: []
    };

    try {
        const sources = loadWorkbookRows();
        const sourceModel = buildSourceModel(sources);

        connection = await mysql.createConnection({
            ...dbConfig,
            connectTimeout: 60000
        });

        if (!dryRun) {
            await connection.beginTransaction();
        }

        console.log('=== Sincronizacion de nomnivel1..5 ===');
        console.log(`Modo: ${dryRun ? 'DRY RUN' : 'APLICAR CAMBIOS'}`);
        console.log('Fuentes: formatos/EstructuraOrganizacional.xlsx + formatos/Personal_Al_20102025.xlsx');
        console.log('Corrige jerarquias nomnivel1..5 y completa nompersonal.codnivel4 cuando sea seguro.');

        const state = {};
        for (const config of LEVELS) {
            state[config.level] = await loadStateForLevel(connection, config.table);
        }

        for (const config of LEVELS) {
            await resolveDuplicates(connection, config, sourceModel.canonicalNodes.get(config.level), state, report);
        }

        for (const config of LEVELS) {
            await ensureLevelRows(connection, config, sourceModel, state, report);
            await synchronizeDescriptionsAndParents(connection, config, sourceModel, state, report);
        }

        await syncNompersonalNivel4FromNivel5(connection, report);
        await annotateConflictsWithActivePersonnel(connection, state, report);

        if (!dryRun) {
            await connection.commit();
        }

        printReport(report);
        writeManualReviewLog(report);
        console.log(`\n${dryRun ? 'Revise el reporte y ejecute con --apply para aplicar.' : 'Sincronizacion completada.'}`);
    } catch (error) {
        if (connection && !dryRun) {
            await connection.rollback();
        }
        console.error('Error sincronizando niveles organizacionales:', error);
        process.exitCode = 1;
    } finally {
        if (connection) {
            await connection.end();
        }
    }
}

main();
