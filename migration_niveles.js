const mysql = require('mysql2/promise');
const xlsx = require('xlsx');
const dbConfig = require('./dbconfig');

async function actualizarCodigoNivel() {
    let connection;
    try {
        connection = await mysql.createConnection(dbConfig);
        
        // Leer el archivo Excel
        const workbook = xlsx.readFile('formatos/Personal_al_2025-03-21.xlsx');
        const sheet = workbook.Sheets[workbook.SheetNames[0]];
        const data = xlsx.utils.sheet_to_json(sheet);

        await connection.execute('SET FOREIGN_KEY_CHECKS=0');
        
        // Limpiar y poblar tabla nivelacademico
        await connection.execute('TRUNCATE nivelacademico');
        
        let codigo = 1;
        const valoresInsertados = new Set();
        
        for (const row of data) {
            const valor = row.NivelAcademico?.trim();
            if (valor && !valoresInsertados.has(valor)) {
                const codFormatted = `N${String(codigo).padStart(3, '0')}`;
                
                await connection.execute(
                    'INSERT INTO nivelacademico (id, descripcion) VALUES (?, ?)',
                    [codFormatted, valor]
                );
                
                valoresInsertados.add(valor);
                codigo++;
            }
        }
        
        // Crear tabla temporal
        await connection.execute(`
            CREATE TEMPORARY TABLE temp_nivelacademico (
                numero_carnet VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
                valor VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci
        `);
        
        // Insertar datos temporales
        const insertTempQuery = 'INSERT INTO temp_nivelacademico (numero_carnet, valor) VALUES ?';
        const tempData = data
            .map(row => [
                row.Personal || null,
                row.NivelAcademico?.trim() || null
            ])
            .filter(row => row[0] && row[1]);
        
        if (tempData.length > 0) {
            for (let i = 0; i < tempData.length; i += 1000) {
                await connection.query(insertTempQuery, [tempData.slice(i, i + 1000)]);
            }
        }
        
        // Actualizar cod_niv en nompersonal
        const [result] = await connection.execute(`
            UPDATE nompersonal np
            JOIN temp_nivelacademico tp ON CONVERT(np.numero_carnet USING utf8mb4) COLLATE utf8mb4_0900_ai_ci = tp.numero_carnet
            JOIN nivelacademico t ON CONVERT(tp.valor USING utf8mb4) COLLATE utf8mb4_0900_ai_ci = CONVERT(t.descripcion USING utf8mb4) COLLATE utf8mb4_0900_ai_ci
            SET np.cod_niv = t.id
        `);
        
        await connection.execute('SET FOREIGN_KEY_CHECKS=1');
        
        console.log(`Actualizados ${result.affectedRows} registros con código de nivel académico`);
        
    } catch (error) {
        console.error('Error:', error);
        throw error;
    } finally {
        if (connection) await connection.end();
    }
}

actualizarCodigoNivel().catch(console.error);