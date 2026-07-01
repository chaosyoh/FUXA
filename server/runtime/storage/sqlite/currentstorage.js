'use strict';

const path = require('path');
const dbAdapter = require('../db-adapter');
const writeInterval = 5000;

function CurrentTagReadings(_settings, _log) {

    const settings = _settings;     // Application settings
    const logger = _log;            // Application logger
    var adapter = null;             // Database adapter
    const dataQueue = new Map();    // Tags map

    /**
     * Bind the database resource by create the table if not exist
     */
    var _bind = async function () {
        const engine = dbAdapter.getEngine(settings);
        adapter = dbAdapter.createAdapter(settings, logger);
        var dbfileExist;
        if (engine === 'sqlite') {
            var dbfile = path.join(settings.dbDir, 'currentTagReadings.db');
            dbfileExist = await adapter.init({ dbFile: dbfile, logger, moduleName: 'currentstorage' });
        } else {
            dbfileExist = await adapter.init({ logger, moduleName: 'currentstorage' });
        }
        const ddl = dbAdapter.getDDL(engine, 'currentValues');
        if (engine === 'sqlite') {
            await adapter.exec(ddl.join('; '));
        }
        return dbfileExist;
    }

    /**
     * Write dataQueue in database
     */
    var _writeValues = async function() {
        if (dataQueue.size > 0) {
            if (adapter.engine === 'sqlite') {
                // SQLite: use prepared statements for performance
                const stmt = adapter.prepare("INSERT OR REPLACE INTO currentValues (tagId, deviceId, value) VALUES (?, ?, ?)");
                for (const [tagid, tag] of dataQueue) {
                    await stmt.run(tagid, tag.deviceId, tag.value);
                }
                await stmt.finalize();
            } else {
                // MySQL: use transaction-wrapped batch
                const sql = adapter.upsertSql('currentValues', ['tagId', 'deviceId', 'value']);
                const paramsList = [];
                for (const [tagid, tag] of dataQueue) {
                    paramsList.push([tagid, tag.deviceId, tag.value]);
                }
                await adapter.batchRun(sql, paramsList);
            }
            dataQueue.clear();
        }
    }

    /**
     * Insert the list of values in database tables, if exist replace the value
     * @param {*} tags [{ tagid, deviceId, value}]
     */
    this.setValues = function (tags) {
        for (const tag of tags) {
            dataQueue.set(tag.id, tag);
        }
    }

    /**
     * Get a list of values per device
     * @param {*} deviceId
     */
    this.getValuesByDeviceId = async function (deviceId) {
        var sql = "SELECT tagId, value FROM currentValues WHERE deviceId = ?";
        const rows = await adapter.all(sql, [deviceId]);
        return rows.map(row => ({ id: row.tagId, value: row.value }));
    }

    /**
     * Close the database
     */
    this.close = function () {
        if (adapter) {
            adapter.close();
        }
    }

    /**
     * Clear all table in database
     */
    this.clearAll = async function () {
        if (!adapter) {
            throw new Error('currentstorage.clear failed! (adapter not initialized)');
        }
        var sql = "DELETE FROM currentValues";
        await adapter.run(sql);
        dataQueue.clear();
        return true;
    }

    this.ready = _bind().then(result => {
        logger.info('currentstorage init successful!', true);
        setInterval(async () => {
            try {
                if (adapter) {
                    await _writeValues();
                }
            } catch (error) {
                logger.error(`currentstorage.writeValues failed! ${error}`);
            }
        }, writeInterval);
        return result;
    }).catch(function (err) {
        logger.error(`currentstorage.failed-to-init ${err}`);
        throw err;
    });
}

module.exports = {
    create: function (data, logger) {
        return new CurrentTagReadings(data, logger);
    },
};
