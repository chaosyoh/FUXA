/**
 *  Module to manage the project datastore in a database
 *  Table: 'general', 'views', 'devices', 'chart', 'texts', 'alarms', 'notifications', 'scripts', 'reports', 'locations'
 */

'use strict';

const path = require('path');
const dbAdapter = require('../storage/db-adapter');

var settings        // Application settings
var logger;         // Application logger
var adapter;        // Database adapter
var _txQueue = Promise.resolve();  // Serialization queue to prevent concurrent transactions

function _ensureValidTable(table) {
    const tables = Object.values(TableType);
    if (!tables.includes(table)) {
        throw new Error(`invalid table '${table}'`);
    }
    return table;
}

/**
 * Init and bind the database resource
 * @param {*} _settings
 * @param {*} _log
 */
function init(_settings, _log) {
    settings = _settings;
    logger = _log;

    return _bind();
}

/**
 * Bind the database resource by create the table if not exist
 */
async function _bind() {
    try {
        const engine = dbAdapter.getEngine(settings);
        adapter = dbAdapter.createAdapter(settings, logger);
        var dbfileExist;
        if (engine === 'sqlite') {
            var dbfile = path.join(settings.workDir, 'project.fuxap.db');
            dbfileExist = await adapter.init({ dbFile: dbfile, logger, moduleName: 'prjstorage' });
        } else {
            await adapter.init({ logger, moduleName: 'prjstorage' });
            // For non-SQLite engines, check if data already exists in DB
            // (KnexAdapter.init() always returns false since there's no file to check)
            try {
                const checkSql = engine === 'mssql'
                    ? 'SELECT TOP 1 name FROM general'
                    : 'SELECT name FROM general LIMIT 1';
                const rows = await adapter.all(checkSql);
                dbfileExist = rows && rows.length > 0;
            } catch (e) {
                dbfileExist = false;
            }
        }
        const ddl = dbAdapter.getDDL(engine, 'project');
        if (engine === 'sqlite') {
            await adapter.exec(ddl.join('; '));
        }
        if (logger) logger.info(`prjstorage._bind: engine=${engine}, dataExists=${dbfileExist}`, true);
        return dbfileExist;
    } catch (err) {
        logger.error(`prjstorage.bind failed! ${err}`);
        throw err;
    }
}

/**
 * Set default project value in database
 */
function setDefault() {
    return new Promise(function (resolve, reject) {
        var scs = [];
        scs.push({ table: TableType.GENERAL, name: 'version', value: '1.00' });
        // Use device id '0' as the key instead of 'server' to avoid duplication
        scs.push({ table: TableType.DEVICES, name: '0', value: { 'id': '0', 'name': 'FUXA Server', 'type': 'FuxaServer', 'enabled': true, 'tags': {}, 'property': {} } });
        setSections(scs).then(() => {
            resolve();
        }).catch(function (err) {
            reject(err);
        });
    });
}

/**
 * Insert the list of values in database tables, if exist replace the value of name(key)
 * The section contains the name of table, name(key) and value
 * @param {*} sections
 */
function setSections(sections) {
    // Enqueue to serialize concurrent calls and avoid nested transactions
    _txQueue = _txQueue.then(async function () {
        try {
            await adapter.run(adapter.beginTransaction());
            for (var i = 0; i < sections.length; i++) {
                var table = _ensureValidTable(sections[i].table);
                var value = JSON.stringify(sections[i].value);
                const sql = adapter.upsertSql(table, ['name', 'value']);
                await adapter.run(sql, [sections[i].name, value]);
            }
            await adapter.run('COMMIT');
        } catch (err) {
            try {
                await adapter.run('ROLLBACK');
            } catch (_) {}
            logger.error(`prjstorage.set failed! ${err}`);
            throw err;
        }
    });
    return _txQueue;
}

/**
 * Insert the values in database table, if exist replace the value of name(key)
 * The section contains the name of table, name(key) and value
 * @param {*} section
 */
function setSection(section) {
    const sections = [section];
    return setSections(sections);
}

/**
 * Return all values of table with this name
 * If name is null return all values in table
 * @param {*} table
 * @param {*} name
 */
async function getSection(table, name) {
    try {
        var safeTable = _ensureValidTable(table);
        var sql = `SELECT name, value FROM ${safeTable}`;
        var params = [];
        if (name) {
            sql += " WHERE name = ?";
            params.push(name);
        }
        return await adapter.all(sql, params);
    } catch (err) {
        throw err;
    }
}

/**
 * Delete the values in database table
 * The section contains the name of table, name(key)
 * @param {*} section
 */
async function deleteSection(section) {
    try {
        var table = _ensureValidTable(section.table);
        await adapter.run(`DELETE FROM ${table} WHERE name = ?`, [section.name]);
    } catch (err) {
        throw err;
    }
}

/**
 * Close the database
 */
function close() {
    if (adapter) {
        adapter.close();
    }
}

/**
 * Clear all table in database
 */
async function clearAll() {
    try {
        const tables = ['general', 'views', 'devices', 'texts', 'alarms',
                       'notifications', 'scripts', 'reports', 'locations'];
        for (const table of tables) {
            await adapter.run(`DELETE FROM ${table}`);
        }
        return true;
    } catch (err) {
        logger.error(`prjstorage.clear failed! ${err}`);
        throw err;
    }
}

/**
 * Database Table
 */
const TableType = {
    GENERAL: 'general',
    DEVICES: 'devices',
    VIEWS: 'views',
    DEVICESSECURITY: 'devicesSecurity',
    TEXTS: 'texts',
    ALARMS: 'alarms',
    NOTIFICATIONS: 'notifications',
    SCRIPTS: 'scripts',
    REPORTS: 'reports',
    LOCATIONS: 'locations',
}


module.exports = {
    init: init,
    close: close,
    clearAll: clearAll,
    getSection: getSection,
    setSections: setSections,
    setSection: setSection,
    deleteSection: deleteSection,
    setDefault: setDefault,
    TableType: TableType,
};
