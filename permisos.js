/**
 * Script para resetear permisos de usuarios basados en su rol asignado
 * 
 * Este script:
 * 1. Elimina todos los permisos actuales de cada usuario
 * 2. Asigna los permisos según el rol asignado a cada usuario
 */

const mysql = require('mysql2/promise');
const xlsx = require('xlsx');
const cliProgress = require('cli-progress');
const { mapParentesco } = require('./parentesco-utils');
require('dotenv').config();

// Configuración de la base de datos
const dbConfig = {
  host: 'localhost',
  user: 'root',
  password: 'root',
  database: 'aitsa_configuracion'
};

async function resetearPermisos() {
  console.log('Iniciando reseteo de permisos de usuarios...');
  
  let connection;
  
  try {
    // Conectar a la base de datos
    connection = await mysql.createConnection(dbConfig);
    console.log('Conexión establecida');
    
    // Iniciar transacción
    await connection.beginTransaction();
    
    // 1. Obtener usuarios con rol asignado
    const [usuarios] = await connection.execute(`
      SELECT coduser, id_rol, descrip 
      FROM nomusuarios 
      WHERE id_rol IS NOT NULL
    `);
    
    console.log(`Se encontraron ${usuarios.length} usuarios con roles asignados`);
    
    let totalPermisosEliminados = 0;
    let totalPermisosAsignados = 0;
    
    // Procesar cada usuario
    for (const usuario of usuarios) {
      const idUsuario = usuario.coduser;
      const idRol = usuario.id_rol;
      const nombreUsuario = usuario.descrip;
      
      console.log(`\nProcesando usuario: ${nombreUsuario} (ID: ${idUsuario}, Rol: ${idRol})`);
      
      // 2. Eliminar todos los permisos actuales del usuario
      const [resultadoEliminar] = await connection.execute(`
        DELETE FROM usuario_permisos 
        WHERE id_usuario = ?
      `, [idUsuario]);
      
      const permisosEliminados = resultadoEliminar.affectedRows;
      totalPermisosEliminados += permisosEliminados;
      console.log(`- Se eliminaron ${permisosEliminados} permisos anteriores`);
      
      // 3. Obtener permisos del rol asignado
      const [permisosRol] = await connection.execute(`
        SELECT id_permiso 
        FROM rol_permisos 
        WHERE id_rol = ?
      `, [idRol]);
      
      if (permisosRol.length === 0) {
        console.log(`- El rol ${idRol} no tiene permisos asignados`);
        continue;
      }
      
      // 4. Asignar los permisos del rol al usuario
      const valoresInsert = permisosRol.map(p => [idUsuario, p.id_permiso]);
      
      if (valoresInsert.length > 0) {
        const [resultadoInsertar] = await connection.query(`
          INSERT INTO usuario_permisos (id_usuario, id_permiso) 
          VALUES ?
        `, [valoresInsert]);
        
        totalPermisosAsignados += resultadoInsertar.affectedRows;
        console.log(`- Se asignaron ${resultadoInsertar.affectedRows} permisos nuevos`);
      }
    }
    
    // Confirmar transacción
    await connection.commit();
    
    console.log('\n===== RESUMEN =====');
    console.log(`Usuarios procesados: ${usuarios.length}`);
    console.log(`Total permisos eliminados: ${totalPermisosEliminados}`);
    console.log(`Total permisos asignados: ${totalPermisosAsignados}`);
    console.log('Reseteo de permisos completado con éxito');
    
  } catch (error) {
    // Revertir cambios en caso de error
    if (connection) {
      await connection.rollback();
    }
    console.error('Error durante el reseteo de permisos:', error.message);
  } finally {
    // Cerrar conexión
    if (connection) {
      await connection.end();
    }
  }
}

// Ejecutar el script
resetearPermisos().catch(console.error);