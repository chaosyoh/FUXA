/**
 *  Module to manage the alarms in a database
 *  Table: 'alarms' (runtime), 'chronicle' (history)
 */

'use strict';

const path = require('path');
const dbAdapter = require('../storage/db-adapter');

var settings        // Application settings
var logger;         // Application logger
var adapter;        // Database adapter

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
            var dbfile = path.join(settings.workDir, 'alarms.fuxap.db');
            dbfileExist = await adapter.init({ dbFile: dbfile, logger, moduleName: 'alarmsstorage' });
        } else {
            dbfileExist = await adapter.init({ logger, moduleName: 'alarmsstorage' });
        }
        const ddl = dbAdapter.getDDL(engine, 'alarms');
        if (engine === 'sqlite') {
            await adapter.exec(ddl.join('; '));
        }
        return dbfileExist;
    } catch (err) {
        logger.error('alarmsstorage.failed-to-bind: ' + err);
        throw err;
    }
}

/**
 * Get table names (differ between SQLite and MySQL for conflict resolution)
 */
function _alarmsTable() {
    return adapter.getTableName('alarms', 'runtime');
}
function _chronicleTable() {
    return adapter.getTableName('alarms', 'chronicle');
}

/**
 * Clear all Alarms from table
 */
async function clearAlarms(all) {
    const alarmsTable = _alarmsTable();
    const chronicleTable = _chronicleTable();
    try {
        await adapter.run(`DELETE FROM ${alarmsTable}`);
        if (all) {
            await adapter.run(`DELETE FROM ${chronicleTable}`);
        }
    } catch (err) {
        throw err;
    }
}

/**
 * Clear Alarms history
 */
async function clearAlarmsHistory(dtlimit) {
    const chronicleTable = _chronicleTable();
    var sql = `DELETE FROM ${chronicleTable} WHERE ontime < ?`;
    return await adapter.run(sql, [dtlimit.getTime()]);
}

/**
 * Return the Alarms list
 */
async function getAlarms() {
    if (!adapter) {
        throw new Error('alarmsstorage not initialized');
    }
    const alarmsTable = _alarmsTable();
    return await adapter.all(`SELECT * FROM ${alarmsTable}`);
}

/**
 * Return the Alarms history
 */
async function getAlarmsHistory(from, to) {
    if (!adapter) {
        throw new Error('alarmsstorage not initialized');
    }
    const chronicleTable = _chronicleTable();
    var start = from || 0;
    var end = to || Number.MAX_SAFE_INTEGER;
    var sql = `SELECT * FROM ${chronicleTable} WHERE ontime BETWEEN ? and ? ORDER BY ontime DESC`;
    return await adapter.all(sql, [start, end]);
}

/**
 * Set alarm value in database
 */
async function setAlarms(alarms) {
    if (!alarms || !alarms.length) {
        return;
    }
    const alarmsTable = _alarmsTable();
    const chronicleTable = _chronicleTable();
    try {
        await adapter.run(adapter.beginTransaction());
        for (const alr of alarms) {
            let grp = alr.subproperty.group || '';
            let status = alr.status || '';
            let userack = alr.userack || '';
            const alarmId = alr.getId();

            // Upsert into alarms runtime table
            const upsertAlarm = adapter.upsertSql(alarmsTable, ['nametype', 'type', 'status', 'ontime', 'offtime', 'acktime']);
            await adapter.run(upsertAlarm, [alarmId, alr.type, status, alr.ontime, alr.offtime, alr.acktime]);

            // Upsert into chronicle table
            if (adapter.engine === 'sqlite') {
                // SQLite: INSERT OR REPLACE with subquery to match existing Sn
                await adapter.run(
                    `INSERT OR REPLACE INTO ${chronicleTable} (Sn, nametype, type, status, text, grp, ontime, offtime, acktime, userack) VALUES ((SELECT Sn from ${chronicleTable} WHERE ontime = ? AND nametype = ?), ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [alr.ontime, alarmId, alarmId, alr.type, status, alr.subproperty.text, grp, alr.ontime, alr.offtime, alr.acktime, userack]
                );
            } else {
                // MySQL: two-step approach (can't use subquery on same table in INSERT)
                const existing = await adapter.get(
                    `SELECT Sn FROM ${chronicleTable} WHERE ontime = ? AND nametype = ?`,
                    [alr.ontime, alarmId]
                );
                if (existing) {
                    await adapter.run(
                        `UPDATE ${chronicleTable} SET type=?, status=?, text=?, grp=?, offtime=?, acktime=?, userack=? WHERE Sn=?`,
                        [alr.type, status, alr.subproperty.text, grp, alr.offtime, alr.acktime, userack, existing.Sn]
                    );
                } else {
                    await adapter.run(
                        `INSERT INTO ${chronicleTable} (nametype, type, status, text, grp, ontime, offtime, acktime, userack) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                        [alarmId, alr.type, status, alr.subproperty.text, grp, alr.ontime, alr.offtime, alr.acktime, userack]
                    );
                }
            }

            if (alr.toremove) {
                await adapter.run(`DELETE FROM ${alarmsTable} WHERE nametype = ?`, [alarmId]);
            }
        }
        await adapter.run('COMMIT');
    } catch (err) {
        try {
            await adapter.run('ROLLBACK');
        } catch (_) {}
        logger.error('alarmsstorage.failed-to-set: ' + err);
        throw err;
    }
}

/**
 * Remove alarm from database
 */
async function removeAlarm(alarm) {
    try {
        const alarmsTable = _alarmsTable();
        await adapter.run(`DELETE FROM ${alarmsTable} WHERE nametype = ?`, [alarm.getId()]);
    } catch (err) {
        logger.error('alarmsstorage.failed-to-remove: ' + err);
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

module.exports = {
    init: init,
    close: close,
    getAlarms: getAlarms,
    getAlarmsHistory: getAlarmsHistory,
    setAlarms: setAlarms,
    clearAlarms: clearAlarms,
    clearAlarmsHistory: clearAlarmsHistory,
    removeAlarm: removeAlarm
};
