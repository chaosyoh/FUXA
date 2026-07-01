/**
 * Knex-based database adapter for MySQL and SQL Server (MSSQL).
 * Replaces the separate db-adapter-mysql.js and db-adapter-mssql.js
 * with a unified implementation powered by Knex.js query builder.
 *
 * Knex handles all dialect differences (parameter binding, identifier quoting,
 * upsert syntax, transaction control) transparently.
 */

'use strict';

const sqlDialect = require('./sql-dialect');

// Singleton Knex instance (lazily created, shared across all modules)
let _knex = null;
let _knexEngine = null;

// Track active transactions per adapter instance (WeakMap keyed by adapter)
const _activeTransactions = new WeakMap();

/**
 * Get or create the shared Knex instance.
 * @param {string} engine  'mysql' | 'mssql'
 * @param {object} config  database connection config from settings
 * @returns {object} knex instance
 */
function getKnexInstance(engine, config) {
    if (!_knex || _knexEngine !== engine) {
        const knex = require('knex');

        let knexConfig;
        if (engine === 'mysql') {
            knexConfig = {
                client: 'mysql2',
                connection: {
                    host: config.host || 'localhost',
                    port: config.port || 3306,
                    user: config.user || 'root',
                    password: config.password || '',
                    database: config.database || 'fuxa',
                    charset: 'utf8mb4',
                },
                pool: {
                    min: 0,
                    max: config.poolMax || 10,
                },
            };
        } else if (engine === 'mssql') {
            knexConfig = {
                client: 'mssql',
                connection: {
                    server: config.host || 'localhost',
                    port: config.port || 1433,
                    user: config.user || 'sa',
                    password: config.password || '',
                    database: config.database || 'fuxa',
                    options: {
                        encrypt: config.encrypt || false,
                        trustServerCertificate: config.trustServerCertificate !== false,
                    },
                },
                pool: {
                    min: 0,
                    max: config.poolMax || 10,
                },
            };
        } else {
            throw new Error(`KnexAdapter does not support engine: ${engine}`);
        }

        _knex = knex(knexConfig);
        _knexEngine = engine;
    }
    return _knex;
}

/**
 * Destroy the shared Knex instance (for shutdown).
 */
async function destroyKnex() {
    if (_knex) {
        await _knex.destroy();
        _knex = null;
        _knexEngine = null;
    }
}

/**
 * Normalize the result from knex.raw() for write queries (INSERT/UPDATE/DELETE).
 *
 * Knex behavior by engine:
 * - MySQL:  returns [ResultSetHeader, fields]  where ResultSetHeader.affectedRows
 * - MSSQL:  returns []  (empty array, tedious emits no row events for writes)
 *
 * @param {*} result  raw result from knex.raw()
 * @param {string} engine  'mysql' | 'mssql'
 * @returns {{ changes: number, lastID: number }}
 */
function _normalizeRunResult(result, engine) {
    if (engine === 'mysql' && Array.isArray(result)) {
        const header = result[0];
        // ResultSetHeader has affectedRows; rows array does not
        if (header && typeof header.affectedRows !== 'undefined') {
            return {
                changes: header.affectedRows || 0,
                lastID: header.insertId || 0,
            };
        }
    }
    return { changes: 0, lastID: 0 };
}

/**
 * Normalize the result from knex.raw() for SELECT queries.
 *
 * Knex behavior by engine:
 * - MySQL:  returns [rows, fields]  (mysql2 driver format)
 * - MSSQL:  returns rows array directly (via processResponse default case)
 *
 * For MySQL we must extract result[0] (the rows), otherwise we return
 * the entire [rows, fields] tuple which breaks downstream iteration.
 *
 * @param {*} result  raw result from knex.raw()
 * @param {string} engine  'mysql' | 'mssql'
 * @returns {Array}
 */
function _normalizeRows(result, engine) {
    if (engine === 'mysql') {
        // MySQL: result is [rows, fields]; rows is always at index 0
        if (Array.isArray(result) && result.length >= 1 && Array.isArray(result[0])) {
            return result[0];
        }
        return [];
    }
    // MSSQL: result is the rows array directly
    if (Array.isArray(result)) {
        return result;
    }
    return [];
}

/**
 * Knex-based adapter for MySQL and MSSQL.
 * Implements the same interface as MysqlAdapter / MssqlAdapter.
 */
class KnexAdapter {
    /**
     * @param {object} knexInstance  shared knex instance
     * @param {string} engine        'mysql' | 'mssql'
     */
    constructor(knexInstance, engine) {
        this.knex = knexInstance;
        this.engine = engine;
    }

    /**
     * Initialize the adapter.
     * @param {object} config
     * @param {object} config.logger
     * @param {string} config.moduleName
     * @returns {Promise<boolean>}
     */
    async init(config) {
        const { logger, moduleName } = config;
        try {
            // Test the connection
            await this.knex.raw('SELECT 1 AS test');
            if (logger) {
                const label = this.engine === 'mysql' ? 'MySQL' : 'SQL Server';
                logger.info(`${moduleName}.connected-to ${label} database (via knex)`, true);
            }
        } catch (err) {
            if (logger) logger.error(`${moduleName}.adapter.connect failed! ${err}`);
            throw err;
        }
        return false;
    }

    /**
     * Execute a write SQL statement.
     * Intercepts transaction control statements (BEGIN/COMMIT/ROLLBACK)
     * and routes them through Knex's transaction API.
     *
     * @param {string} sql    SQL with ? placeholders
     * @param {Array}  params parameter values
     * @returns {Promise<{changes: number, lastID: number}>}
     */
    async run(sql, params = []) {
        const trimmed = sql.trim();

        // Intercept transaction control statements
        if (/^(START\s+TRANSACTION|BEGIN\s+TRANSACTION)/i.test(trimmed)) {
            const trx = await this.knex.transaction();
            _activeTransactions.set(this, trx);
            return { changes: 0, lastID: 0 };
        }
        if (/^COMMIT/i.test(trimmed)) {
            const trx = _activeTransactions.get(this);
            if (trx) {
                await trx.commit();
                _activeTransactions.delete(this);
            }
            return { changes: 0, lastID: 0 };
        }
        if (/^ROLLBACK/i.test(trimmed)) {
            const trx = _activeTransactions.get(this);
            if (trx) {
                await trx.rollback();
                _activeTransactions.delete(this);
            }
            return { changes: 0, lastID: 0 };
        }

        // Use active transaction if one exists, otherwise use main knex instance
        const executor = _activeTransactions.get(this) || this.knex;
        const result = params.length === 0
            ? await executor.raw(trimmed)
            : await executor.raw(trimmed, params);
        return _normalizeRunResult(result, this.engine);
    }

    /**
     * Execute a read SQL statement, return all rows.
     * @param {string} sql
     * @param {Array}  params
     * @returns {Promise<Array>}
     */
    async all(sql, params = []) {
        const executor = _activeTransactions.get(this) || this.knex;
        const result = params.length === 0
            ? await executor.raw(sql)
            : await executor.raw(sql, params);
        return _normalizeRows(result, this.engine);
    }

    /**
     * Execute a read SQL statement, return single row.
     * @param {string} sql
     * @param {Array}  params
     * @returns {Promise<Object|undefined>}
     */
    async get(sql, params = []) {
        const rows = await this.all(sql, params);
        return rows.length > 0 ? rows[0] : undefined;
    }

    /**
     * Execute multiple SQL statements sequentially (DDL, batch DML).
     * For non-SQLite engines, this is typically a no-op since tables are
     * pre-created via db-migrate.
     *
     * @param {string} sql  semicolon-separated statements
     * @returns {Promise<void>}
     */
    async exec(sql) {
        const statements = sqlDialect.splitStatements(sql);
        const executor = _activeTransactions.get(this) || this.knex;
        for (const stmt of statements) {
            await executor.raw(stmt);
        }
    }

    /**
     * Batch write using a Knex transaction.
     * More efficient than manual BEGIN/COMMIT as it uses Knex's native
     * transaction support for both MySQL and MSSQL.
     *
     * @param {string} sql         SQL with ? placeholders
     * @param {Array<Array>} paramsList  array of parameter arrays
     * @returns {Promise<{changes: number}>}
     */
    async batchRun(sql, paramsList) {
        let totalChanges = 0;
        await this.knex.transaction(async (trx) => {
            for (const params of paramsList) {
                const result = params.length === 0
                    ? await trx.raw(sql)
                    : await trx.raw(sql, params);
                totalChanges += _normalizeRunResult(result, this.engine).changes;
            }
        });
        return { changes: totalChanges };
    }

    /**
     * Generate the correct upsert SQL for the current engine.
     * @param {string} table
     * @param {string[]} columns
     * @returns {string}
     */
    upsertSql(table, columns) {
        return sqlDialect.upsertSql(this.engine, table, columns);
    }

    /**
     * Get the correct table name for the current engine.
     * @param {string} module
     * @param {string} role
     * @returns {string}
     */
    getTableName(module, role) {
        return sqlDialect.getTableName(this.engine, module, role);
    }

    /**
     * Quote an identifier if it's a reserved word.
     * @param {string} name
     * @returns {string}
     */
    quoteId(name) {
        return sqlDialect.quoteIdentifier(this.engine, name);
    }

    /**
     * Check if an error is a duplicate column name error.
     * @param {Error} err
     * @returns {boolean}
     */
    isDuplicateColumnError(err) {
        return sqlDialect.isDuplicateColumnError(err, this.engine);
    }

    /**
     * Generate ALTER TABLE ADD column SQL.
     * @param {string} table
     * @param {string} column
     * @param {string} type
     * @returns {string}
     */
    addColumnSql(table, column, type) {
        return sqlDialect.addColumnSql(this.engine, table, column, type);
    }

    /**
     * Get the correct BEGIN/START TRANSACTION statement for the engine.
     * Note: actual transaction management is handled transparently in run().
     * @returns {string}
     */
    beginTransaction() {
        return sqlDialect.beginTransaction(this.engine);
    }

    /**
     * Close - does NOT destroy the shared Knex instance.
     * Pool lifecycle is managed by db-adapter.js factory via closePool().
     */
    close() {
        // Clean up any lingering transaction reference
        _activeTransactions.delete(this);
    }
}

module.exports = {
    KnexAdapter,
    getKnexInstance,
    destroyKnex,
};
