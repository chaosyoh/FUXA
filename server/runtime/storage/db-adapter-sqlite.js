/**
 * SQLite database adapter.
 * Wraps the sqlite3 async-callback API with a Promise-based interface.
 */

'use strict';

const sqlite3 = require('sqlite3').verbose();
const sqlDialect = require('./sql-dialect');

class SqliteAdapter {
    constructor() {
        this.db = null;
        this.engine = 'sqlite';
    }

    /**
     * Initialize the SQLite connection.
     * @param {object} config
     * @param {string} config.dbFile  path to .db file
     * @param {object} config.logger  logger instance
     * @param {string} config.moduleName  module name for logging
     * @returns {Promise<boolean>} true if db file already existed
     */
    init(config) {
        const { dbFile, logger, moduleName } = config;
        const fs = require('fs');
        const dbfileExist = fs.existsSync(dbFile);

        return new Promise((resolve, reject) => {
            this.db = new sqlite3.Database(dbFile, (err) => {
                if (err) {
                    if (logger) logger.error(`${moduleName}.adapter.connect failed! ${err}`);
                    reject(err);
                    return;
                }
                if (logger) logger.info(`${moduleName}.connected-to ${dbFile} database`, true);
            });
            resolve(dbfileExist);
        });
    }

    /**
     * Execute a write SQL statement.
     * @param {string} sql
     * @param {Array} params
     * @returns {Promise<{changes: number, lastID: number}>}
     */
    run(sql, params = []) {
        return new Promise((resolve, reject) => {
            this.db.run(sql, params, function (err) {
                if (err) {
                    reject(err);
                } else {
                    resolve({ changes: this.changes, lastID: this.lastID });
                }
            });
        });
    }

    /**
     * Execute a read SQL statement, return all rows.
     * @param {string} sql
     * @param {Array} params
     * @returns {Promise<Array>}
     */
    all(sql, params = []) {
        return new Promise((resolve, reject) => {
            this.db.all(sql, params, (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows || []);
                }
            });
        });
    }

    /**
     * Execute a read SQL statement, return single row.
     * @param {string} sql
     * @param {Array} params
     * @returns {Promise<Object|undefined>}
     */
    get(sql, params = []) {
        return new Promise((resolve, reject) => {
            this.db.get(sql, params, (err, row) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(row);
                }
            });
        });
    }

    /**
     * Execute multiple SQL statements at once (DDL, batch DML).
     * @param {string} sql  semicolon-separated statements
     * @returns {Promise<void>}
     */
    exec(sql) {
        return new Promise((resolve, reject) => {
            this.db.exec(sql, (err) => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }

    /**
     * Create a prepared statement (for batch writes in currentstorage).
     * @param {string} sql
     * @returns {{run: Function, finalize: Function}}
     */
    prepare(sql) {
        const stmt = this.db.prepare(sql);
        return {
            run: (...args) => new Promise((resolve, reject) => {
                stmt.run(...args, function (err) {
                    if (err) reject(err);
                    else resolve({ changes: this.changes, lastID: this.lastID });
                });
            }),
            finalize: () => new Promise((resolve, reject) => {
                stmt.finalize((err) => {
                    if (err) reject(err);
                    else resolve();
                });
            }),
        };
    }

    /**
     * Generate the correct upsert SQL for SQLite.
     * @param {string} table
     * @param {string[]} columns
     * @returns {string}
     */
    upsertSql(table, columns) {
        return sqlDialect.upsertSql('sqlite', table, columns);
    }

    /**
     * Get the correct table name for the current engine.
     * @param {string} module  e.g. 'alarms'
     * @param {string} role    e.g. 'runtime', 'chronicle'
     * @returns {string}
     */
    getTableName(module, role) {
        return sqlDialect.getTableName('sqlite', module, role);
    }

    /**
     * Quote an identifier if needed.
     * @param {string} name
     * @returns {string}
     */
    quoteId(name) {
        return sqlDialect.quoteIdentifier('sqlite', name);
    }

    /**
     * Check if an error is a duplicate column name error.
     * @param {Error} err
     * @returns {boolean}
     */
    isDuplicateColumnError(err) {
        return sqlDialect.isDuplicateColumnError(err, 'sqlite');
    }

    /**
     * Generate ALTER TABLE ADD column SQL.
     * @param {string} table
     * @param {string} column
     * @param {string} type
     * @returns {string}
     */
    addColumnSql(table, column, type) {
        return sqlDialect.addColumnSql('sqlite', table, column, type);
    }

    /**
     * Get the START TRANSACTION statement.
     * @returns {string}
     */
    beginTransaction() {
        return sqlDialect.beginTransaction('sqlite');
    }

    /**
     * Close the database connection.
     */
    close() {
        if (this.db) {
            this.db.close();
            this.db = null;
        }
    }
}

module.exports = SqliteAdapter;
