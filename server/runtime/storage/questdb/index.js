'use strict'

const { Pool, types } = require('pg');
const { Sender } = require('@questdb/nodejs-client');
let utils = require('../../utils');

// QuestDB stores all timestamps in UTC, but pg parses TIMESTAMP (without timezone)
// using local timezone by default. Override to interpret as UTC.
types.setTypeParser(1114, (str) => new Date(str + '+00'));

function QuestDB(_settings, _log, _currentStorage) {
    let settings = _settings;               // Application settings
    const logger = _log;                    // Application logger
    const currentStorage = _currentStorage; // Database to set the last value (current)
    let pool = null;
    let sender = null;
    const tableName = getTableName();
    let writeQueue = Promise.resolve();
    let initPromise = Promise.resolve();

    this.setCall = function (_fncGetProp) {
        fncGetTagProp = _fncGetProp;
        return this.addDaqValues;
    }
    var fncGetTagProp = null;

    this.init = async function () {
        try {
            const config = getQueryClientConfig();
            pool = new Pool(getQueryClientConfig());
            sender = Sender.fromConfig(getIngestConfigString());
            await ensureSchema();
            logger.info('QuestDB connected');
        } catch (error) {
            logger.error(`questdb-init failed! ${error}`);
        }
    }

    this.addDaqValues = function (tagsValues, deviceName, deviceId) {
        var dataToRestore = [];
        var rowsToWrite = [];

        for (const tagid in tagsValues) {
            let tag = tagsValues[tagid];
            if (!tag.daq || utils.isNullOrUndefined(tag.value) || Number.isNaN(tag.value)) {
                if (tag.daq && tag.daq.restored) {
                    dataToRestore.push({ id: tag.id, deviceId: deviceId, value: tag.value });
                }
                if (tag.daq && !tag.daq.enabled) {
                    continue;
                }
            }

            rowsToWrite.push({
                tagid,
                value: tag.value,
                timestamp: tag.timestamp || Date.now(),
            });
        }

        if (rowsToWrite.length) {
            writeQueue = writeQueue.then(async () => {
                await initPromise;
                if (!sender) {
                    return;
                }

                for (const row of rowsToWrite) {
                    const parsedValue = normalizeValue(row.value);
                    let line = sender
                        .table(tableName)
                        .symbol('tag_id', row.tagid)
                        .stringColumn('tag_value', parsedValue);           
                    await line.at(Number(row.timestamp), 'ms');
                }
                await sender.flush();
            }).catch((error) => {
                logger.error(`questdb-addDaqValues failed! ${error}`);
            });
        }

        if (dataToRestore.length && currentStorage) {
            currentStorage.setValues(dataToRestore);
        }
    }

    this.getDaqValue = function (tagid, fromts, tots) {
        return new Promise(function (resolve, reject) {
            initPromise.then(() => {
                if (!pool) {
                    resolve([]);
                    return;
                }

                const query = `SELECT dt, tag_value FROM ${tableName} WHERE tag_id = $1 AND dt >= $2 AND dt < $3 ORDER BY dt`;
                //console.log(`[${new Date().toISOString()}] QuestDB query: SELECT dt, tag_value FROM ${tableName} WHERE tag_id = '${tagid}' AND dt >= ${new Date(fromts).toISOString()} AND dt < ${new Date(tots).toISOString()} ORDER BY dt`)
                const params = [tagid, new Date(fromts), new Date(tots)];

                pool.query(query, params)
                    .then((result) => {
                        let data = [];
                        console.log(`[${new Date().toISOString()}] QuestDB query result rows:`, result.rows)
                        result.rows.forEach((row) => {
                            const value = !utils.isNullOrUndefined(row.tag_value) ? Number(row.tag_value) : row.tag_value;
                            data.push({ dt: new Date(row.dt).getTime(), value });
                        });
                        resolve(data)
                    })
                    .catch((error) => {
                        logger.error(`questdb-getDaqValue failed! ${error}`)
                        reject(error)
                    })
            }).catch((error) => {
                logger.error(`questdb-getDaqValue failed! ${error}`)
                reject(error)
            });
        })
    }

    this.close = function () {
        if (sender) {
            sender.close().catch((error) => {
                logger.error(`questdb-close sender failed! ${error}`);
            });
            sender = null;
        }
        if (pool) {
            pool.end().catch((error) => {
                logger.error(`questdb-close pool failed! ${error}`);
            });
            pool = null;
        }
    }

    this.getDaqMap = function (tagid) {
        var dummy = {};
        dummy[tagid] = true;
        return dummy;
    }

    async function ensureSchema() {
        if (!pool) {
            return;
        }
        await pool.query(`CREATE TABLE IF NOT EXISTS ${tableName} (
            dt TIMESTAMP,
            tag_id SYMBOL,
            tag_value STRING
        ) TIMESTAMP(dt) PARTITION BY DAY`);
    }

    function getIngestConfigString() {
        return settings.daqstore.configurationString || 'http::addr=localhost:9000;';
    }

    function getQueryClientConfig() {
        return {
            host: settings.daqstore.host || '127.0.0.1',
            port: 8812, // Standard port
            database: 'qdb', // Standard database
            user: settings.daqstore.credentials?.username || 'admin',
            password: settings.daqstore.credentials?.password || 'quest',
            max: 10, // Pool with 10 connections
            idleTimeoutMillis: 30000, // 30s
        };
    }

    function getTableName() {
        const name = (settings.daqstore.tableName || 'meters').trim();
        if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
            return name.toLowerCase();
        }
        logger.warn(`questdb invalid tableName "${name}", fallback to meters`);
        return 'meters';
    }

    function normalizeValue(value) {
        if (utils.isNullOrUndefined(value)) {
            return null;
        }
        if (utils.isBoolean(value)) {
            return value ? '1' : '0'
        }
        return String(value);
    }

    function normalizeUnsPath(value) {
        if (utils.isNullOrUndefined(value)) {
            return null;
        }
        const normalized = String(value).trim();
        return normalized.length ? normalized : null;
    }

    initPromise = this.init();
}

module.exports = {
    create: function (data, logger, currentStorage) {
        return new QuestDB(data, logger, currentStorage);
    }
};
