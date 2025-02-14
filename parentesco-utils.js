// Mapping global de parentescos básicos
const parentescoMap = {
    'madre': 1,
    'padre': 2,
    'hijo': 3,
    'hija': 3,
    'hijo(a)': 3,
    'conyuge': 4,
    'conyugue': 4,
    'esposo': 4,
    'esposa': 4,
    'pareja': 4,
    'concubino': 5,
    'concubina': 5,
    'nieto': 6,
    'nieta': 6,
    'hijastra': 6,
    'hijastro': 6,
    'nieto(a)': 6,
    'vacías': 8,
    'vacias': 8,
    '(vacías)': 8,
    '(vacias)': 8,
    'sin información': 8
};

// Función global para limpiar strings de parentesco
const cleanParentescoString = (str) => {
    if (!str || typeof str !== 'string') {
        return null;
    }
    return str.replace(/\d+/g, '')
             .replace(/[()]/g, '')
             .replace(/\s+/g, ' ')
             .trim()
             .toLowerCase();
};

// Función global para mapear parentescos
const mapParentesco = async (connection, parentesco) => {
    if (!parentesco) return 8;
    
    const cleaned = cleanParentescoString(parentesco);
    let codorg = parentescoMap[cleaned];
    
    if (!codorg && cleaned) {
        // Determinar el formato normalizado para nuevos parentescos
        let descripcion;
        if (['hermano', 'hermana'].includes(cleaned)) {
            descripcion = 'Hermano/a';
        } else if (['sobrino', 'sobrina'].includes(cleaned)) {
            descripcion = 'Sobrino/a';
        } else if (['tio', 'tia'].includes(cleaned)) {
            descripcion = 'Tio/a';
        } else if (['abuelo', 'abuela'].includes(cleaned)) {
            descripcion = 'Abuelo/a';
        }

        if (descripcion) {
            // Verificar si ya existe
            const [existing] = await connection.execute(
                'SELECT codorg FROM nomparentescos WHERE LOWER(descrip) = LOWER(?)',
                [descripcion]
            );

            if (existing.length > 0) {
                codorg = existing[0].codorg;
            } else {
                // Obtener nuevo código y insertar
                const [rows] = await connection.execute('SELECT MAX(codorg) as maxCod FROM nomparentescos');
                const newCodorg = (rows[0].maxCod || 0) + 1;
                
                await connection.execute(
                    'INSERT INTO nomparentescos (codorg, descrip) VALUES (?, ?)',
                    [newCodorg, descripcion]
                );
                
                console.log(`Nuevo parentesco agregado: ${descripcion} con código ${newCodorg}`);
                codorg = newCodorg;
            }
        }
    }
    
    return codorg || 8;
};

module.exports = {
    parentescoMap,
    cleanParentescoString,
    mapParentesco
};