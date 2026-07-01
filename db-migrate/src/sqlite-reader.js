/**
 * SQLite reader module
 * Reads data from FUXA SQLite databases using better-sqlite3 (synchronous, fast)
 */

'use strict';

const path = require('path');
const fs = require('fs');

let Database;
try {
  Database = require('better-sqlite3');
} catch (e) {
  console.error('better-sqlite3 not installed. Run: npm install');
  process.exit(1);
}

class SqliteReader {
  /**
   * @param {string} appdataDir - FUXA _appdata directory path
   */
  constructor(appdataDir) {
    this.appdataDir = appdataDir;
    this.connections = {};
  }

  /**
   * Open a SQLite database file
   * @param {string} dbFile - database file name
   * @returns {object} better-sqlite3 Database instance
   */
  open(dbFile) {
    const dbPath = path.resolve(this.appdataDir, dbFile);
    if (!fs.existsSync(dbPath)) {
      return null;
    }
    if (!this.connections[dbPath]) {
      this.connections[dbPath] = new Database(dbPath, { readonly: true, fileMustExist: true });
    }
    return this.connections[dbPath];
  }

  /**
   * Read all rows from a table
   * @param {string} dbFile - database file name
   * @param {string} tableName - table to read
   * @returns {Array} rows
   */
  readAll(dbFile, tableName) {
    const db = this.open(dbFile);
    if (!db) {
      return [];
    }
    try {
      const rows = db.prepare(`SELECT * FROM [${tableName}]`).all();
      return rows;
    } catch (err) {
      if (err.message.includes('no such table')) {
        return [];
      }
      throw err;
    }
  }

  /**
   * Read row count from a table
   * @param {string} dbFile
   * @param {string} tableName
   * @returns {number}
   */
  count(dbFile, tableName) {
    const db = this.open(dbFile);
    if (!db) {
      return 0;
    }
    try {
      const result = db.prepare(`SELECT COUNT(*) as cnt FROM [${tableName}]`).get();
      return result ? result.cnt : 0;
    } catch (err) {
      if (err.message.includes('no such table')) {
        return 0;
      }
      throw err;
    }
  }

  /**
   * Get table names in a database
   * @param {string} dbFile
   * @returns {string[]}
   */
  getTableNames(dbFile) {
    const db = this.open(dbFile);
    if (!db) {
      return [];
    }
    const rows = db.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").all();
    return rows.map(r => r.name);
  }

  /**
   * Check if database file exists
   * @param {string} dbFile
   * @returns {boolean}
   */
  exists(dbFile) {
    return fs.existsSync(path.resolve(this.appdataDir, dbFile));
  }

  /**
   * Find DAQ database files by prefix
   * @param {string} prefix - e.g. 'daq-data_' or 'daq-map_'
   * @returns {string[]} list of file names
   */
  findDaqFiles(prefix) {
    const result = [];
    try {
      const files = fs.readdirSync(this.appdataDir);
      for (const file of files) {
        if (file.startsWith(prefix) && file.endsWith('.db')) {
          result.push(file);
        }
      }
      // Also check archive folder
      const archiveDir = path.join(this.appdataDir, 'archive');
      if (fs.existsSync(archiveDir)) {
        const archiveFiles = fs.readdirSync(archiveDir);
        for (const file of archiveFiles) {
          if (file.startsWith(prefix) && file.endsWith('.db')) {
            result.push(path.join('archive', file));
          }
        }
      }
    } catch (err) {
      // ignore
    }
    return result;
  }

  /**
   * Read all rows from a table with pagination (for large datasets)
   * @param {string} dbFile
   * @param {string} tableName
   * @param {number} batchSize
   * @param {function} callback - called with each batch of rows
   */
  readAllBatched(dbFile, tableName, batchSize, callback) {
    const db = this.open(dbFile);
    if (!db) return;

    try {
      const countResult = db.prepare(`SELECT COUNT(*) as cnt FROM [${tableName}]`).get();
      const total = countResult ? countResult.cnt : 0;

      let offset = 0;
      while (offset < total) {
        const rows = db.prepare(`SELECT * FROM [${tableName}] LIMIT ? OFFSET ?`).all(batchSize, offset);
        if (rows.length === 0) break;
        callback(rows, offset, total);
        offset += batchSize;
      }
    } catch (err) {
      if (!err.message.includes('no such table')) {
        throw err;
      }
    }
  }

  /**
   * Close all database connections
   */
  closeAll() {
    for (const dbPath of Object.keys(this.connections)) {
      try {
        this.connections[dbPath].close();
      } catch (e) {
        // ignore
      }
    }
    this.connections = {};
  }
}

module.exports = SqliteReader;
