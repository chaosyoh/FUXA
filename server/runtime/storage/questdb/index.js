'use strict'
let fetch;
import('node-fetch').then(module => {
    fetch = module.default;
    // 使用 fetch 进行操作
}).catch(err => {
    console.error('Failed to load node-fetch', err);
});
let utils = require('../../utils');

function QuestDB(_settings, _log, _currentStorage) {
    let settings = _settings;               // Application settings
    const logger = _log;                      // Application logger
    const currentStorage = _currentStorage;  // Database to set the last value (current)
    const HOST = settings.daqstore.url || "http://localhost:9000";
    this.setCall = function (_fncGetProp) {
        fncGetTagProp = _fncGetProp;
        return this.addDaqValues;
    }
    var fncGetTagProp = null;

    this.init = function () {
        logger.info("QuestDB connected")
    }

    this.addDaqValues = async function (tagsValues, deviceName, deviceId) {
        var dataToRestore = [];
        let values = [];
        for (const tagid in tagsValues) {
            let tag = tagsValues[tagid];
            if (!tag.daq || utils.isNullOrUndefined(tag.value) || Number.isNaN(tag.value)) {
                if (tag.daq.restored) {
                    dataToRestore.push({ id: tag.id, deviceId: deviceId, value: tag.value });
                }
                if (!tag.daq.enabled) {
                    continue;
                }
            }
            values.push(`(now(),'${tagid}','${tag.value}')`);
        }
        let query = '';
        for (let i = 1; i <= values.length; i++) {
            query += values[i - 1];
            if (i % 200 === 0 || i === values.length) {
                query = "INSERT INTO meters VALUES" + query;
                try {
                    let res = await fetch(`${HOST}/exec?query=${encodeURIComponent(query)}`);
                    if (res.status !== 200) {
                        logger.error(`QuestDB daq addValue error`);
                    }
                    // else {
                    //     logger.info(`QuestDB daq addValue success`);
                    // }
                }
                catch (err) {
                    logger.error(`QuestDB daq addValue error ${err}`);
                }
                query = '';
            }
            else {
                query += ',';
            }
        }
        if (dataToRestore.length && currentStorage) {
            currentStorage.setValues(dataToRestore);
        }
    }

    this.getDaqValue = function (tagid, fromts, tots) {
        return new Promise(function (resolve, reject) {
            let data = []

            let start = new Date(fromts)
            let end = new Date(tots)
            let query = `SELECT CAST(dt as BIGINT)/1000 as dt, tag_value
                          FROM meters
                            WHERE tag_id = '${tagid}' 
                            and dt >= '${start.toISOString()}'
                            and dt < '${end.toISOString()}' `;
            //add by J, the tagid is missed in the sql, should be one of the filter condition
            fetch(`${HOST}/exec?query=${encodeURIComponent(query)}`).then(res => {
                if (res.status !== 200) {
                    logger.error(`QuestDB daq getValue error`);
                }
                res.json().then(json => {
                    if (json.dataset && json.dataset.length) {
                        json.dataset.forEach(row => {
                            data.push({ dt: row[0], value: row[1] })
                        });
                    }
                    resolve(data)
                });
            }).catch((error) => {
                logger.error(`QuestDB-getDaqValue failed! ${error}`)
                reject(error)
            })

        })
    }

    this._getData = function (query) {
        return new Promise(function (resolve, reject) {
            let data = []
            fetch(`${HOST}/exec?query=${encodeURIComponent(query)}`).then(res => {
                res.json().then(json => {
                    if (json.dataset && json.dataset.length) {
                        json.dataset.forEach(row => {
                            data.push({ dt: row[0], value: row[1] })
                        });
                    }
                    resolve(data)
                });
            }).catch((error) => {
                logger.error(`QuestDB-getDaqValue failed! ${error}`)
                reject(error)
            })

        })
    }

    this.close = function () {
        //do noting
    }

    this.getDaqMap = function (tagid) {
        var dummy = {};
        dummy[tagid] = true;
        return dummy;
    }

    this.init();
}

module.exports = {
    create: function (data, logger, currentStorage) {
        return new QuestDB(data, logger, currentStorage);
    }
};