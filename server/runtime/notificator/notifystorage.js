/**
 *  Module to manage the notifications in a database
 *  Table: 'chronicle'
 */

'use strict';

const path = require('path');
const dbAdapter = require('../storage/db-adapter');

var settings            // Application settings
var logger;             // Application logger
var adapter;            // Database adapter

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
            var dbfile = path.join(settings.workDir, 'notifications.fuxap.db');
            dbfileExist = await adapter.init({ dbFile: dbfile, logger, moduleName: 'notifystorage' });
        } else {
            dbfileExist = await adapter.init({ logger, moduleName: 'notifystorage' });
        }
        const ddl = dbAdapter.getDDL(engine, 'notifications');
        if (engine === 'sqlite') {
            await adapter.exec(ddl.join('; '));
        }
        return dbfileExist;
    } catch (err) {
        logger.error('notifystorage.failed-to-bind: ' + err);
        throw err;
    }
}

/**
 * Get the chronicle table name (differs between SQLite and MySQL)
 */
function _chronicleTable() {
    return adapter.getTableName('notifications', 'chronicle');
}

/**
 * Clear all Notifications from table
 */
async function clearNotifications(all) {
    const table = _chronicleTable();
    try {
        await adapter.run(`DELETE FROM ${table}`);
    } catch (err) {
        throw err;
    }
}

/**
 * Return the Notifications list
 */
async function getNotifications() {
    if (!adapter) {
        throw new Error('notifystorage not initialized');
    }
    const table = _chronicleTable();
    return await adapter.all(`SELECT * FROM ${table}`);
}

/**
 * Return the Notifications history
 */
async function getNotificationsHistory(from, to) {
    if (!adapter) {
        throw new Error('notifystorage not initialized');
    }
    const table = _chronicleTable();
    var sql = `SELECT * FROM ${table} ORDER BY notifytime DESC`;
    return await adapter.all(sql);
}

/**
 * Set Notifications value in database
 */
async function setNotification(notification) {
    if (!notification) {
        return;
    }
    try {
        const table = _chronicleTable();
        const sql = adapter.upsertSql(table, ['id', 'name', 'type', 'receiver', 'text', 'notifytime', 'notifytype']);
        await adapter.run(sql, [
            notification.id,
            notification.name,
            notification.type,
            notification.receiver,
            notification.text,
            notification.notifytime,
            notification.notifytype
        ]);
    } catch (err) {
        logger.error('notifystorage.failed-to-set: ' + err);
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
 * Remove Notification from database
 */
async function removeNotification(notification) {
    try {
        const table = _chronicleTable();
        await adapter.run(`DELETE FROM ${table} WHERE id = ?`, [notification.id]);
    } catch (err) {
        logger.error('notificationsstorage.failed-to-remove: ' + err);
        throw err;
    }
}

module.exports = {
    init: init,
    close: close,
    getNotifications: getNotifications,
    getNotificationsHistory: getNotificationsHistory,
    setNotification: setNotification,
    clearNotifications: clearNotifications,
    removeNotification: removeNotification
};
