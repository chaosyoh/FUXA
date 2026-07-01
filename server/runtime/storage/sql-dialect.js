/**
 * SQL dialect helpers and DDL definitions for multi-database support.
 * Handles differences between SQLite, MySQL, and SQL Server (MSSQL).
 */

'use strict';

/**
 * Table name mapping for modules that have naming conflicts
 * when consolidated into a single shared database.
 * SQLite uses separate files so no conflict; MySQL/MSSQL use the renamed tables.
 */
const TABLE_NAMES = {
    alarms: {
        runtime:   { sqlite: 'alarms',   mysql: 'alarms_runtime',   mssql: 'alarms_runtime' },
        chronicle: { sqlite: 'chronicle', mysql: 'alarms_chronicle', mssql: 'alarms_chronicle' },
    },
    notifications: {
        chronicle: { sqlite: 'chronicle', mysql: 'notifications_chronicle', mssql: 'notifications_chronicle' },
    },
};

/**
 * Get the correct table name for a module/table-role/engine combination.
 * @param {string} engine  'sqlite' | 'mysql' | 'mssql'
 * @param {string} module  e.g. 'alarms', 'notifications'
 * @param {string} role    e.g. 'runtime', 'chronicle'
 * @returns {string} table name
 */
function getTableName(engine, module, role) {
    if (TABLE_NAMES[module] && TABLE_NAMES[module][role]) {
        return TABLE_NAMES[module][role][engine] || TABLE_NAMES[module][role]['sqlite'];
    }
    // Default: role IS the table name (most modules)
    return role;
}

/**
 * MySQL reserved words that need backtick quoting.
 */
const MYSQL_RESERVED = new Set([
    'groups', 'order', 'group', 'select', 'where', 'from', 'table',
    'index', 'key', 'rank', 'rows', 'system',
]);

/**
 * SQL Server reserved words that need bracket quoting.
 */
const MSSQL_RESERVED = new Set([
    'groups', 'order', 'group', 'select', 'where', 'from', 'table',
    'index', 'key', 'rows', 'user', 'system', 'rule', 'check',
]);

/**
 * Quote an identifier (column/table name) if it's a reserved word.
 * @param {string} engine  'sqlite' | 'mysql' | 'mssql'
 * @param {string} name    identifier
 * @returns {string}
 */
function quoteIdentifier(engine, name) {
    if (engine === 'mysql' && MYSQL_RESERVED.has(name.toLowerCase())) {
        return '`' + name + '`';
    }
    if (engine === 'mssql' && MSSQL_RESERVED.has(name.toLowerCase())) {
        return '[' + name + ']';
    }
    return name;
}

/**
 * Generate the correct upsert (INSERT OR REPLACE) SQL for the engine.
 * @param {string} engine    'sqlite' | 'mysql' | 'mssql'
 * @param {string} table     table name
 * @param {string[]} columns column names
 * @returns {string} SQL with ? placeholders
 */
function upsertSql(engine, table, columns) {
    const placeholders = columns.map(() => '?').join(', ');
    const colList = columns.map(c => quoteIdentifier(engine, c)).join(', ');
    if (engine === 'mysql') {
        const updateParts = columns.map(c => {
            const qc = quoteIdentifier(engine, c);
            return `${qc}=VALUES(${qc})`;
        }).join(', ');
        return `INSERT INTO ${table} (${colList}) VALUES(${placeholders}) ON DUPLICATE KEY UPDATE ${updateParts}`;
    }
    if (engine === 'mssql') {
        // SQL Server: use MERGE statement (trailing ; is required)
        const pk = quoteIdentifier(engine, columns[0]);
        const updateParts = columns.slice(1).map(c => {
            const qc = quoteIdentifier(engine, c);
            return `target.${qc}=source.${qc}`;
        }).join(', ');
        const srcPlaceholders = columns.map(() => '?').join(', ');
        const srcColAliases = columns.map(c => quoteIdentifier(engine, c)).join(', ');
        const insertVals = columns.map(c => `source.${quoteIdentifier(engine, c)}`).join(', ');
        return `MERGE INTO ${table} AS target USING (SELECT ${srcPlaceholders}) AS source(${srcColAliases}) ON target.${pk}=source.${pk} WHEN MATCHED THEN UPDATE SET ${updateParts} WHEN NOT MATCHED THEN INSERT (${colList}) VALUES(${insertVals});`;
    }
    return `INSERT OR REPLACE INTO ${table} (${colList}) VALUES(${placeholders})`;
}

/**
 * DDL definitions for each module's tables.
 * Only SQLite DDL is provided here; MySQL and MSSQL tables are pre-created
 * via the db-migrate tool and do not require runtime DDL execution.
 * Each function returns an array of CREATE TABLE statements for SQLite.
 */
const DDL = {
    /**
     * Project storage - 10 KV tables (devices has extra columns)
     */
    project() {
        const kv = (name) =>
            `CREATE TABLE IF NOT EXISTS ${name} (name TEXT PRIMARY KEY, value TEXT)`;
        const kvTables = ['general', 'views', 'devicesSecurity', 'texts', 'alarms',
                          'notifications', 'scripts', 'reports', 'locations'];
        const stmts = kvTables.map(t => kv(t));
        stmts.push(`CREATE TABLE IF NOT EXISTS devices (name TEXT PRIMARY KEY, value TEXT, connection TEXT, cntid TEXT, cntpwd TEXT)`);
        return stmts;
    },

    /**
     * Users storage - users + roles tables
     */
    users() {
        return [
            `CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, fullname TEXT, password TEXT, groups INTEGER, info TEXT)`,
            `CREATE TABLE IF NOT EXISTS roles (name TEXT PRIMARY KEY, value TEXT)`,
        ];
    },

    /**
     * Alarm storage - alarms_runtime + alarms_chronicle (table names differ by engine)
     */
    alarms(engine) {
        const runtimeTable = getTableName(engine, 'alarms', 'runtime');
        const chronicleTable = getTableName(engine, 'alarms', 'chronicle');
        return [
            `CREATE TABLE IF NOT EXISTS ${runtimeTable} (nametype TEXT PRIMARY KEY, type TEXT, status TEXT, ontime INTEGER, offtime INTEGER, acktime INTEGER)`,
            `CREATE TABLE IF NOT EXISTS ${chronicleTable} (Sn INTEGER, nametype TEXT, type TEXT, status TEXT, text TEXT, grp TEXT, ontime INTEGER, offtime INTEGER, acktime INTEGER, userack TEXT, PRIMARY KEY(Sn AUTOINCREMENT))`,
        ];
    },

    /**
     * Notification storage - notifications_chronicle (table name differs by engine)
     */
    notifications(engine) {
        const chronicleTable = getTableName(engine, 'notifications', 'chronicle');
        return [
            `CREATE TABLE IF NOT EXISTS ${chronicleTable} (Sn INTEGER, id TEXT, name TEXT, type TEXT, receiver TEXT, text TEXT, notifytime INTEGER, notifytype TEXT, PRIMARY KEY(Sn AUTOINCREMENT))`,
        ];
    },

    /**
     * API Keys storage - single KV table
     */
    apikeys() {
        return [
            `CREATE TABLE IF NOT EXISTS apikeys (name TEXT PRIMARY KEY, value TEXT)`,
        ];
    },

    /**
     * Scheduler storage
     */
    scheduler() {
        return [
            `CREATE TABLE IF NOT EXISTS schedulers (id TEXT PRIMARY KEY, data TEXT NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)`,
        ];
    },

    /**
     * Current tag readings storage
     */
    currentValues() {
        return [
            `CREATE TABLE IF NOT EXISTS currentValues (tagId TEXT PRIMARY KEY, deviceId TEXT, value TEXT)`,
        ];
    },
};

/**
 * Split a multi-statement SQL string into individual statements.
 * Used for MySQL/MSSQL which cannot execute multiple statements in one call.
 * @param {string} sql  semicolon-separated SQL
 * @returns {string[]} array of non-empty statements
 */
function splitStatements(sql) {
    return sql.split(';')
        .map(s => s.trim())
        .filter(s => s.length > 0);
}

/**
 * Check if an error is a "duplicate column name" error from ALTER TABLE.
 * @param {Error} err
 * @param {string} engine
 * @returns {boolean}
 */
function isDuplicateColumnError(err, engine) {
    if (!err) return false;
    if (engine === 'mysql') {
        return err.code === 'ER_DUP_FIELDNAME' || (err.errno === 1060);
    }
    if (engine === 'mssql') {
        // SQL Server error 2705: Column names in each table must be unique
        return err.number === 2705 || (err.message && err.message.includes('already an object named'));
    }
    return err.message && err.message.includes('duplicate column name');
}

/**
 * Generate the correct ALTER TABLE ADD column SQL for the engine.
 * MSSQL uses `ADD col type` (no COLUMN keyword); SQLite/MySQL use `ADD COLUMN`.
 * @param {string} engine  'sqlite' | 'mysql' | 'mssql'
 * @param {string} table   table name
 * @param {string} column  column name
 * @param {string} type    column type
 * @returns {string}
 */
function addColumnSql(engine, table, column, type) {
    const col = quoteIdentifier(engine, column);
    if (engine === 'mssql') {
        // Map SQLite/MySQL types to MSSQL equivalents
        const mssqlType = (type.toUpperCase() === 'TEXT') ? 'NVARCHAR(MAX)' : type;
        return `ALTER TABLE [${table}] ADD ${col} ${mssqlType}`;
    }
    return `ALTER TABLE ${table} ADD COLUMN ${col} ${type}`;
}

/**
 * Get the correct BEGIN/START TRANSACTION statement for the engine.
 * @param {string} engine
 * @returns {string}
 */
function beginTransaction(engine) {
    if (engine === 'mssql') return 'BEGIN TRANSACTION';
    if (engine === 'mysql') return 'START TRANSACTION';
    // SQLite uses BEGIN or BEGIN TRANSACTION
    return 'BEGIN TRANSACTION';
}

module.exports = {
    getTableName,
    quoteIdentifier,
    upsertSql,
    addColumnSql,
    beginTransaction,
    DDL,
    splitStatements,
    isDuplicateColumnError,
};
