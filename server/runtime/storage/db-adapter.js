/**
 * Database adapter factory.
 * Creates the appropriate adapter based on settings.dbType:
 *   - 'sqlite' (default): uses db-adapter-sqlite.js (multi-file mode)
 *   - 'mysql':  uses KnexAdapter with mysql2 driver
 *   - 'mssql':  uses KnexAdapter with mssql (tedious) driver
 *
 * Knex.js handles all dialect differences (parameter binding, identifier
 * quoting, upsert syntax, transactions) for MySQL and MSSQL.
 */

'use strict';

const SqliteAdapter = require('./db-adapter-sqlite');
const sqlDialect = require('./sql-dialect');
const { KnexAdapter, getKnexInstance, destroyKnex } = require('./db-adapter-knex');

/**
 * Create a database adapter for the specified engine.
 * @param {object} settings  application settings (must have dbType and mysql/mssql)
 * @param {object} logger    application logger
 * @returns {SqliteAdapter|KnexAdapter}
 */
function createAdapter(settings, logger) {
    const engine = (settings.dbType || 'sqlite').toLowerCase();

    if (engine === 'mysql') {
        const knexInstance = getKnexInstance('mysql', settings.mysql || {});
        return new KnexAdapter(knexInstance, 'mysql');
    }

    if (engine === 'mssql') {
        const knexInstance = getKnexInstance('mssql', settings.mssql || {});
        return new KnexAdapter(knexInstance, 'mssql');
    }

    // Default: SQLite
    return new SqliteAdapter();
}

/**
 * Get the current database engine type from settings.
 * @param {object} settings
 * @returns {string} 'sqlite' | 'mysql' | 'mssql'
 */
function getEngine(settings) {
    return (settings.dbType || 'sqlite').toLowerCase();
}

/**
 * Get the DDL statements for a module.
 * Only used by SQLite (non-SQLite databases are pre-created via db-migrate).
 * @param {string} engine  'sqlite' | 'mysql' | 'mssql'
 * @param {string} module  module name (e.g. 'project', 'users', 'alarms')
 * @returns {string[]} array of CREATE TABLE statements
 */
function getDDL(engine, module) {
    if (sqlDialect.DDL[module]) {
        return sqlDialect.DDL[module](engine);
    }
    return [];
}

/**
 * Close all shared connection pools.
 * Should be called during application shutdown.
 */
async function closePool() {
    await destroyKnex();
}

module.exports = {
    createAdapter,
    getEngine,
    getDDL,
    closePool,
};
