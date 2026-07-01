/**
 * Module to manage scheduler configuration storage
 */

'use strict';

const path = require('path');
const dbAdapter = require('../storage/db-adapter');

var settings;
var logger;
var runtime;
var adapter;

var TABLE_SCHEDULERS = 'schedulers';

function init(_settings, _log, _runtime) {
    settings = _settings;
    logger = _log;
    runtime = _runtime;
    
    return _createDB().catch(err => {
        logger.error('scheduler-storage init error: ' + err);
        throw err;
    });
}

async function _createDB() {
    const engine = dbAdapter.getEngine(settings);
    adapter = dbAdapter.createAdapter(settings, logger);
    if (engine === 'sqlite') {
        const dbPath = path.join(settings.workDir, 'scheduler.db');
        await adapter.init({ dbFile: dbPath, logger, moduleName: 'scheduler-storage' });
    } else {
        await adapter.init({ logger, moduleName: 'scheduler-storage' });
    }
    const ddl = dbAdapter.getDDL(engine, 'scheduler');
    if (engine === 'sqlite') {
        await adapter.exec(ddl.join('; '));
    }
}

async function getSchedulerData(schedulerId) {
    if (!adapter) {
        throw new Error('Scheduler database not initialized');
    }
    try {
        const sql = `SELECT data FROM ${TABLE_SCHEDULERS} WHERE id = ?`;
        const row = await adapter.get(sql, [schedulerId]);
        if (row) {
            return JSON.parse(row.data);
        }
        return null;
    } catch (err) {
        logger.error('scheduler-storage get error: ' + err);
        throw err;
    }
}

async function setSchedulerData(schedulerId, data) {
    if (!adapter) {
        throw new Error('Scheduler database not initialized');
    }
    try {
        const jsonData = JSON.stringify(data);
        const sql = adapter.upsertSql(TABLE_SCHEDULERS, ['id', 'data', 'updated_at']);
        const now = new Date().toISOString().slice(0, 19).replace('T', ' ');
        const result = await adapter.run(sql, [schedulerId, jsonData, now]);
        return { changes: result.changes };
    } catch (err) {
        logger.error('scheduler-storage set error: ' + err);
        throw err;
    }
}

async function getAllSchedulers() {
    if (!adapter) {
        throw new Error('Scheduler database not initialized');
    }
    try {
        const sql = `SELECT id, data FROM ${TABLE_SCHEDULERS}`;
        const rows = await adapter.all(sql);
        return rows.map(row => ({
            id: row.id,
            data: JSON.parse(row.data)
        }));
    } catch (err) {
        logger.error('scheduler-storage get all error: ' + err);
        throw err;
    }
}

async function deleteSchedulerData(schedulerId) {
    if (!adapter) {
        throw new Error('Scheduler database not initialized');
    }
    try {
        const sql = `DELETE FROM ${TABLE_SCHEDULERS} WHERE id = ?`;
        const result = await adapter.run(sql, [schedulerId]);
        return { changes: result.changes };
    } catch (err) {
        logger.error('scheduler-storage delete error: ' + err);
        throw err;
    }
}

function close() {
    if (adapter) {
        adapter.close();
    }
}

module.exports = {
    init: init,
    getSchedulerData: getSchedulerData,
    setSchedulerData: setSchedulerData,
    getAllSchedulers: getAllSchedulers,
    deleteSchedulerData: deleteSchedulerData,
    close: close
};
