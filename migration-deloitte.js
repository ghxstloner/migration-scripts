const mysql = require('mysql2/promise');
const xlsx = require('xlsx');
const dbConfig = require('./dbconfig');
require('dotenv').config();

(async () => {
 try {
   const connection = await mysql.createConnection(dbConfig);
   

   const workbook = xlsx.readFile('Personal_Al_23012025.xlsx');
   const sheetName = workbook.SheetNames[0];
   const sheet = workbook.Sheets[sheetName];
   const data = xlsx.utils.sheet_to_json(sheet);

   // Limpiar tablas
   await connection.execute("DELETE FROM cargodeloitte");
   await connection.execute("DELETE FROM nivelcargo"); 
   await connection.execute("DELETE FROM rolcargo");
   


   // Insertar cargos únicos
   const insertCargoQuery = "INSERT INTO cargodeloitte (id_cargo, nombre_cargo) VALUES (?, ?)";
   const insertNivelQuery = "INSERT INTO nivelcargo (id_nivel, nombre_nivel) VALUES (?, ?)";
   const insertRolQuery = "INSERT INTO rolcargo (id_rol, nombre_rol) VALUES (?, ?)";

   const cargosSet = new Set();
   const nivelesSet = new Set();
   const rolesSet = new Set();

   let cargoId = 1, nivelId = 1, rolId = 1;

   for (const row of data) {
     const cargo = row.CargoDeloitte?.trim();
     const nivel = row.NivelCargo?.trim();
     const rol = row.RolCargo?.trim();

     if (cargo && !cargosSet.has(cargo)) {
       await connection.execute(insertCargoQuery, [cargoId, cargo]);
       
       cargosSet.add(cargo);
       cargoId++;
     }

     if (nivel && !nivelesSet.has(nivel)) {
       await connection.execute(insertNivelQuery, [nivelId, nivel]);
       
       nivelesSet.add(nivel);
       nivelId++;
     }

     if (rol && !rolesSet.has(rol)) {
       await connection.execute(insertRolQuery, [rolId, rol]);
       
       rolesSet.add(rol);
       rolId++;
     }
   }

   // Crear tabla temporal
   await connection.execute(`DROP TEMPORARY TABLE IF EXISTS temp_cargos`);
   await connection.execute(`
     CREATE TEMPORARY TABLE temp_cargos (
       cedula VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
       cargo VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
       nivel VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
       rol VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci
     )
   `);

   // Insertar en tabla temporal
   const insertTempQuery = "INSERT INTO temp_cargos (cedula, cargo, nivel, rol) VALUES ?";
   const tempData = data
   .map(row => [
     row.Cedula || null,
     row.CargoDeloitte?.trim() || null,
     row.NivelCargo?.trim() || null,
     row.RolCargo?.trim() || null
   ])
   .filter(row => row[0] && (row[1] || row[2] || row[3]));

   if (tempData.length > 0) {
     const batchSize = 1000;
     for (let i = 0; i < tempData.length; i += batchSize) {
       const batch = tempData.slice(i, i + batchSize);
       await connection.query(insertTempQuery, [batch]);
     }
   }

   // Verificar datos en tabla temporal
   const [tempCheck] = await connection.execute('SELECT * FROM temp_cargos LIMIT 5');
   

   // Verificar cédula específica
   const [specificCheck] = await connection.execute(
     'SELECT * FROM temp_cargos WHERE cedula = ?',
     ['8-945-1418']
   );
   

   // Verificar join con nompersonal
   const [joinCheck] = await connection.execute(`
     SELECT np.cedula, np.personal_id, tc.cargo, tc.nivel, tc.rol
     FROM nompersonal np
     INNER JOIN temp_cargos tc ON np.cedula = tc.cedula
     LIMIT 5
   `);
   
   const insertCargoEmpleadoQuery = `
   INSERT INTO cargoempleado (id_empleado, id_cargo, id_nivel, id_rol, fecha_inicio)
   SELECT DISTINCT
     np.personal_id,
     CASE WHEN cd.id_cargo IS NULL THEN NULL ELSE cd.id_cargo END,
     CASE WHEN nc.id_nivel IS NULL THEN NULL ELSE nc.id_nivel END,
     CASE WHEN rc.id_rol IS NULL THEN NULL ELSE rc.id_rol END,
     CURRENT_DATE
   FROM nompersonal np
   INNER JOIN temp_cargos tc ON TRIM(np.cedula) = TRIM(tc.cedula)
   LEFT JOIN cargodeloitte cd ON TRIM(tc.cargo) = TRIM(cd.nombre_cargo)
   LEFT JOIN nivelcargo nc ON TRIM(tc.nivel) = TRIM(nc.nombre_nivel)
   LEFT JOIN rolcargo rc ON TRIM(tc.rol) = TRIM(rc.nombre_rol)
   WHERE np.personal_id IS NOT NULL
   AND (tc.cargo IS NOT NULL OR tc.nivel IS NOT NULL OR tc.rol IS NOT NULL)
   `;

   const [insertResult] = await connection.execute(insertCargoEmpleadoQuery);
   

   await connection.end();
   
 } catch (error) {
   console.error("Error:", error);
   console.error("Stack:", error.stack);
 }
})();