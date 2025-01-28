const mysql = require('mysql2/promise');
const xlsx = require('xlsx');

(async () => {
    const dbConfig = {
        host: '172.31.203.5',
        user: 'root',
        password: '4m4x0n14-41ts4',
        database: 'aitsa_rrhh'
      };

  try {
    const connection = await mysql.createConnection(dbConfig);
    console.log("Conexión exitosa a MySQL");

    // Leer el archivo Excel
    const workbook = xlsx.readFile('Personal_Al_23012025.xlsx');
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];
    const rows = xlsx.utils.sheet_to_json(sheet);
    console.log(`Datos cargados desde el Excel: ${rows.length} registros`);

    if (rows.length === 0) {
      console.error("El Excel no contiene registros.");
      return;
    }

    // Procesar fila por fila
    for (const row of rows) {
      // Validar que Cedula exista en la fila
      if (!row['Cedula']) {
        console.warn(`Fila ignorada: falta 'Cedula'. Datos: ${JSON.stringify(row)}`);
        continue;
      }

      // Verificar existencia antes de actualizar
      const [existingRecord] = await connection.execute(
        'SELECT cedula FROM nompersonal WHERE cedula = ?',
        [row['Cedula']]
      );

      if (existingRecord.length === 0) {
        console.warn(`Cedula no encontrada en la base de datos: ${row['Cedula']}`);
        continue;
      }

      console.log(`Actualizando registro para 'Cedula': ${row['Cedula']}`);

      // Construir consulta de actualización
      const query = `
        UPDATE nompersonal
        SET forcob = ?,
            banco_sucursal = ?,
            cuentacob = ?,
            cuenta_pago = ?,
            sindicato = ?,
            dias_periodo = ?,
            tipo_periodo = ?,
            jornada = ?,
            tipo_sueldo = ?,
            ISRFijoPeriodo = ?,
            suesal = ?,
            salario_diario = ?,
            rata_x_hr = ?,
            gastos_representacion = ?,
            gasto_rep_diario = ?,
            rata_hora_gasto_rep = ?,
            aeropuerto = ?,
            zona_economica = 1
        WHERE cedula = ?
      `;

      // Crear arreglo de valores para la consulta
      const values = [
        row['FormaPago'] ?? null,
        row['Banco'] ?? null,
        row['PersonalCuenta'] ?? null,
        row['CtaDinero'] ?? null,
        row['Sindicato'] ?? null,
        row['DiasPeriodo'] ?? null,
        row['PeriodoTipo'] ?? null,
        row['Jornada'] ?? null,
        row['TipoSueldo'] ?? null,
        row['ISRFijoPeriodo'] ?? null,
        row['SueldoMensual'] ?? null,
        row['SueldoDiario'] ?? null,
        row['RataHora'] ?? null,
        row['GR'] ?? null,
        row['GastoRepresentacionDiario'] ?? null,
        row['RataHoraGR'] ?? null,
        row['Aeropuerto'] ?? null,
        row['Cedula'], // Clave primaria para identificar la fila
      ];

      // Log para depurar
      console.log('Query:', query);
      console.log('Values:', values);

      // Ejecutar la consulta
      const [result] = await connection.execute(query, values);

      if (result.affectedRows === 0) {
        console.warn(`No se encontró ningún registro para 'Cedula': ${row['Cedula']}`);
      } else {
        console.log(`Registro actualizado para 'Cedula': ${row['Cedula']}`);
      }
    }

    console.log("Actualización completada exitosamente.");
    await connection.end();
  } catch (error) {
    console.error("Error durante la migración:", error);
  }
})();