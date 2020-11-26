const _ = require('lodash');
const stream = require('stream');
const driverBase = require('../frontend/driver');
const Analyser = require('./Analyser');
const mysql = require('mysql');

function extractColumns(fields) {
  if (fields)
    return fields.map((col) => ({
      columnName: col.name,
    }));
  return null;
}

/** @type {import('dbgate-types').EngineDriver} */
const driver = {
  ...driverBase,
  analyserClass: Analyser,

  async connect( { server, port, user, password, database }) {
    const connection = mysql.createConnection({
      host: server,
      port,
      user,
      password,
      database,
    });
    connection._database_name = database;
    return connection;
  },
  async query(connection, sql) {
    if (sql == null) {
      return {
        rows: [],
        columns: [],
      };
    }
    return new Promise((resolve, reject) => {
      connection.query(sql, function (error, results, fields) {
        if (error) reject(error);
        resolve({ rows: results, columns: extractColumns(fields) });
      });
    });
  },
  async stream(connection, sql, options) {
    const query = connection.query(sql);

    // const handleInfo = (info) => {
    //   const { message, lineNumber, procName } = info;
    //   options.info({
    //     message,
    //     line: lineNumber,
    //     procedure: procName,
    //     time: new Date(),
    //     severity: 'info',
    //   });
    // };

    const handleEnd = (result) => {
      // console.log('RESULT', result);
      options.done(result);
    };

    const handleRow = (row) => {
      options.row(row);
    };

    const handleFields = (columns) => {
      console.log('FIELDS', columns[0].name);
      options.recordset(extractColumns(columns));
    };

    const handleError = (error) => {
      console.log('ERROR', error);
      const { message, lineNumber, procName } = error;
      options.info({
        message,
        line: lineNumber,
        procedure: procName,
        time: new Date(),
        severity: 'error',
      });
    };

    query.on('error', handleError).on('fields', handleFields).on('result', handleRow).on('end', handleEnd);

    return query;
  },
  async readQuery(connection, sql, structure) {
    const query = connection.query(sql);

    const pass = new stream.PassThrough({
      objectMode: true,
      highWaterMark: 100,
    });

    query
      .on('error', (err) => {
        console.error(err);
        pass.end();
      })
      .on('fields', (fields) => pass.write(structure || { columns: extractColumns(fields) }))
      .on('result', (row) => pass.write(row))
      .on('end', () => pass.end());

    return pass;
  },
  async getVersion(connection) {
    const { rows } = await this.query(connection, "show variables like 'version'");
    const version = rows[0].Value;
    return { version };
  },
  async listDatabases(connection) {
    const { rows } = await this.query(connection, 'show databases');
    return rows.map((x) => ({ name: x.Database }));
  },
  async writeTable(pool, name, options) {
    // @ts-ignore
    return createBulkInsertStreamBase(this, stream, pool, name, options);
  },
};

module.exports = driver;
