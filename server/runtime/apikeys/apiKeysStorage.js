/**
 *  Module to manage the apikeys in a database
 *  Table: 'apikeys'
 */

'use strict';

const path = require('path');
const dbAdapter = require('../storage/db-adapter');

var settings        // Application settings
var logger;         // Application logger
var adapter;        // Database adapter (sqlite or mysql)

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
            var dbfile = path.join(settings.workDir, 'apikeys.fuxap.db');
            dbfileExist = await adapter.init({ dbFile: dbfile, logger, moduleName: 'apiKeysStorage' });
        } else {
            dbfileExist = await adapter.init({ logger, moduleName: 'apiKeysStorage' });
        }
        const ddl = dbAdapter.getDDL(engine, 'apikeys');
        if (engine === 'sqlite') {
            await adapter.exec(ddl.join('; '));
        }
        return dbfileExist;
    } catch (err) {
        logger.error(`apiKeysStorage.bind failed! ${err}`);
        throw err;
    }
}

/**
 * Return the ApiKeys list
 */
async function getApiKeys() {
    try {
        var sql = "SELECT value FROM apikeys";
        return await adapter.all(sql);
    } catch (err) {
        throw err;
    }
}

/**
 * Set ApiKeys value in database
 */
async function setApiKeys(apiKeys) {
    try {
        const sql = adapter.upsertSql('apikeys', ['name', 'value']);
        for (var i = 0; i < apiKeys.length; i++) {
            const apiKey = apiKeys[i];
            var value = JSON.stringify(apiKey);
            await adapter.run(sql, [apiKey.id, value]);
        }
    } catch (err) {
        logger.error(`apiKeysStorage.set apikeys failed! ${err}`);
        throw err;
    }
}

/**
 * Remove ApiKeys from database
 */
async function removeApiKeys(apiKeys) {
    try {
        const sql = "DELETE FROM apikeys WHERE name = ?";
        for (var i = 0; i < apiKeys.length; i++) {
            const apiKey = apiKeys[i];
            await adapter.run(sql, [apiKey.id]);
        }
    } catch (err) {
        logger.error(`apiKeysStorage.remove apikeys failed! ${err}`);
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
    getApiKeys: getApiKeys,
    setApiKeys: setApiKeys,
    removeApiKeys: removeApiKeys
};
