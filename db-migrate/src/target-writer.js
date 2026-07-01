/**
 * Target database writer module
 * Supports writing to MySQL and SQL Server
 */

'use strict';

class TargetWriter {
  /**
   * @param {object} config - target database config { type: 'mysql'|'mssql', ... }
   */
  constructor(config) {
    this.config = config;
    this.type = config.type; // 'mysql' or 'mssql'
    this.connection = null;
  }

  /**
   * Connect to the target database
   */
  async connect() {
    if (this.type === 'mysql') {
      return this._connectMysql();
    } else if (this.type === 'mssql') {
      return this._connectMssql();
    }
    throw new Error(`Unsupported target type: ${this.type}`);
  }

  async _connectMysql() {
    const mysql = require('mysql2/promise');
    const conn = await mysql.createConnection({
      host: this.config.host || 'localhost',
      port: this.config.port || 3306,
      user: this.config.user,
      password: this.config.password,
      database: this.config.database,
      multipleStatements: true,
    });
    this.connection = conn;
    return conn;
  }

  async _connectMssql() {
    const sql = require('mssql');
    const pool = await sql.connect({
      server: this.config.host || 'localhost',
      port: this.config.port || 1433,
      user: this.config.user,
      password: this.config.password,
      database: this.config.database,
      options: {
        encrypt: this.config.encrypt || false,
        trustServerCertificate: this.config.trustServerCertificate || true,
      },
      requestTimeout: this.config.requestTimeout || 60000,
      pool: { max: 10, min: 0, idleTimeoutMillis: 30000 },
    });
    this.connection = pool;
    return pool;
  }

  /**
   * Drop a table if it exists
   * @param {string} tableName
   * @param {string} dbPrefix
   */
  async dropTable(tableName, dbPrefix) {
    const fullTableName = dbPrefix ? `${dbPrefix}_${tableName}` : tableName;

    if (this.type === 'mysql') {
      await this.connection.query(`DROP TABLE IF EXISTS \`${fullTableName}\``);
    } else {
      try {
        await this.connection.request().query(`DROP TABLE IF EXISTS [${fullTableName}]`);
      } catch (err) {
        // ignore
      }
    }
  }

  /**
   * Create a table in the target database
   * @param {string} tableName
   * @param {object} tableDef - table definition from schema.js
   * @param {string} dbPrefix - optional prefix for table name (e.g. for DAQ databases)
   * @param {boolean} dropIfExists - drop the table first if it exists
   */
  async createTable(tableName, tableDef, dbPrefix, dropIfExists = false) {
    const fullTableName = dbPrefix ? `${dbPrefix}_${tableName}` : tableName;

    if (dropIfExists) {
      await this.dropTable(tableName, dbPrefix);
    }

    if (this.type === 'mysql') {
      return this._createTableMysql(fullTableName, tableDef);
    } else {
      return this._createTableMssql(fullTableName, tableDef);
    }
  }

  async _createTableMysql(tableName, tableDef) {
    const columns = tableDef.columns.map(col => {
      let def = `\`${col.name}\` ${col.mysqlType}`;
      // Don't add NOT NULL for nullable columns
      return def;
    });

    let pk = '';
    if (tableDef.primaryKey && !tableDef.autoIncrement) {
      pk = `, PRIMARY KEY (\`${tableDef.primaryKey}\`)`;
    } else if (tableDef.primaryKey && tableDef.autoIncrement) {
      // auto increment column is already primary key
      pk = `, PRIMARY KEY (\`${tableDef.primaryKey}\`)`;
    }

    const sql = `CREATE TABLE IF NOT EXISTS \`${tableName}\` (\n  ${columns.join(',\n  ')}${pk}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`;

    await this.connection.query(sql);
  }

  async _createTableMssql(tableName, tableDef) {
    const columns = tableDef.columns.map(col => {
      let def = `[${col.name}] ${col.mssqlType}`;
      return def;
    });

    let pk = '';
    if (tableDef.primaryKey) {
      pk = `, CONSTRAINT [PK_${tableName}] PRIMARY KEY ([${tableDef.primaryKey}])`;
    }

    const sql = `IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='${tableName}' AND xtype='U')\nCREATE TABLE [${tableName}] (\n  ${columns.join(',\n  ')}${pk}\n)`;

    await this.connection.request().query(sql);
  }

  /**
   * Insert rows into a table (UPSERT)
   * @param {string} tableName
   * @param {Array} rows
   * @param {object} tableDef
   * @param {string} dbPrefix
   * @param {number} batchSize
   */
  async insertRows(tableName, rows, tableDef, dbPrefix, batchSize = 500) {
    if (!rows || rows.length === 0) return 0;

    const fullTableName = dbPrefix ? `${dbPrefix}_${tableName}` : tableName;
    let totalInserted = 0;

    // Process in batches
    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);

      if (this.type === 'mysql') {
        await this._insertBatchMysql(fullTableName, batch, tableDef);
      } else {
        await this._insertBatchMssql(fullTableName, batch, tableDef);
      }
      totalInserted += batch.length;
    }

    return totalInserted;
  }

  async _insertBatchMysql(tableName, rows, tableDef) {
    if (rows.length === 0) return;

    const columns = tableDef.columns.map(c => `\`${c.name}\``).join(', ');
    const placeholders = tableDef.columns.map(() => '?').join(', ');
    const valuesClause = rows.map(() => `(${placeholders})`).join(', ');

    // Use REPLACE INTO for upsert (requires primary key)
    let sql;
    if (tableDef.primaryKey) {
      sql = `REPLACE INTO \`${tableName}\` (${columns}) VALUES ${valuesClause}`;
    } else {
      sql = `INSERT INTO \`${tableName}\` (${columns}) VALUES ${valuesClause}`;
    }

    const flatValues = [];
    for (const row of rows) {
      for (const col of tableDef.columns) {
        flatValues.push(this._formatValue(row[col.name], col));
      }
    }

    await this.connection.query(sql, flatValues);
  }

  async _insertBatchMssql(tableName, rows, tableDef) {
    if (rows.length === 0) return;

    const sql = require('mssql');
    const table = new sql.Table(tableName);
    table.create = false;

    for (const col of tableDef.columns) {
      const typeInfo = this._getMssqlType(col);
      table.columns.add(col.name, typeInfo.type, { nullable: true, ...typeInfo.options });
    }

    for (const row of rows) {
      const values = tableDef.columns.map(col => this._formatValue(row[col.name], col));
      table.rows.add(...values);
    }

    const request = this.connection.request();
    await request.bulk(table);
  }

  _getMssqlType(col) {
    const sql = require('mssql');
    // BCP bulk insert requires the type constructor (class), NOT an instance.
    // For variable-length types, pass length via options.
    const typeMap = {
      'INT': { type: sql.Int },
      'INT IDENTITY(1,1)': { type: sql.Int },
      'INT AUTO_INCREMENT': { type: sql.Int },
      'BIGINT': { type: sql.BigInt },
      'VARCHAR(100)': { type: sql.NVarChar, options: { length: 100 } },
      'VARCHAR(255)': { type: sql.NVarChar, options: { length: 255 } },
      'NVARCHAR(100)': { type: sql.NVarChar, options: { length: 100 } },
      'NVARCHAR(255)': { type: sql.NVarChar, options: { length: 255 } },
      'NVARCHAR(MAX)': { type: sql.NVarChar, options: { length: 'max' } },
      'TEXT': { type: sql.NVarChar, options: { length: 'max' } },
      'DATETIME': { type: sql.DateTime2 },
      'DATETIME2': { type: sql.DateTime2 },
    };
    return typeMap[col.mssqlType] || { type: sql.NVarChar, options: { length: 'max' } };
  }

  /**
   * Format value for insertion
   */
  _formatValue(value, col) {
    if (value === undefined || value === null) {
      return null;
    }
    // Integer types
    if (col.mysqlType === 'INT' || col.mysqlType === 'BIGINT' ||
        col.mysqlType === 'INT AUTO_INCREMENT') {
      const num = parseInt(value);
      return isNaN(num) ? null : num;
    }
    return value;
  }

  /**
   * Truncate a table before migration
   * @param {string} tableName
   * @param {string} dbPrefix
   */
  async truncateTable(tableName, dbPrefix) {
    const fullTableName = dbPrefix ? `${dbPrefix}_${tableName}` : tableName;

    if (this.type === 'mysql') {
      try {
        await this.connection.query(`SET FOREIGN_KEY_CHECKS = 0`);
        await this.connection.query(`TRUNCATE TABLE \`${fullTableName}\``);
        await this.connection.query(`SET FOREIGN_KEY_CHECKS = 1`);
      } catch (err) {
        // Table might not exist
        if (!err.message.includes("doesn't exist")) {
          throw err;
        }
      }
    } else {
      try {
        await this.connection.request().query(`TRUNCATE TABLE [${fullTableName}]`);
      } catch (err) {
        // Table might not exist
        if (!err.message.includes('Cannot find')) {
          throw err;
        }
      }
    }
  }

  /**
   * Test connection
   * @returns {boolean}
   */
  async testConnection() {
    try {
      if (this.type === 'mysql') {
        const [rows] = await this.connection.query('SELECT 1 as test');
        return rows && rows.length > 0;
      } else {
        const result = await this.connection.request().query('SELECT 1 as test');
        return result && result.recordset && result.recordset.length > 0;
      }
    } catch (err) {
      throw err;
    }
  }

  /**
   * Close the connection
   */
  async close() {
    if (this.connection) {
      if (this.type === 'mysql') {
        await this.connection.end();
      } else {
        await this.connection.close();
      }
      this.connection = null;
    }
  }
}

module.exports = TargetWriter;
