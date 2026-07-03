/**
 *  Module to manage the users in a database
 *  Table: 'users', 'roles'
 */

'use strict';

const path = require('path');
const dbAdapter = require('../storage/db-adapter');
const bcrypt = require('bcryptjs');

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
            var dbfile = path.join(settings.workDir, 'users.fuxap.db');
            dbfileExist = await adapter.init({ dbFile: dbfile, logger, moduleName: 'usrstorage' });
        } else {
            await adapter.init({ logger, moduleName: 'usrstorage' });
            // For non-SQLite engines, check if data already exists in DB
            try {
                const checkSql = engine === 'mssql'
                    ? 'SELECT TOP 1 username FROM users'
                    : 'SELECT username FROM users LIMIT 1';
                const rows = await adapter.all(checkSql);
                dbfileExist = rows && rows.length > 0;
            } catch (e) {
                dbfileExist = false;
            }
        }
        const ddl = dbAdapter.getDDL(engine, 'users');
        const groupsCol = adapter.quoteId('groups');
        const columnsToAdd = [
            { name: 'fullname', type: 'TEXT' },
            { name: 'info', type: 'TEXT' }
        ];
        if (engine === 'sqlite') {
            await adapter.exec(ddl.join('; '));
            // Check for schema migration (add missing columns) - SQLite only
            await _checkUpdate(columnsToAdd);
        }
        return dbfileExist;
    } catch (err) {
        logger.error(`usrstorage.bind failed! ${err}`);
        throw err;
    }
}

async function _checkUpdate(columnsToAdd) {
    for (const column of columnsToAdd) {
        const { name, type } = column;
        try {
            await adapter.run(adapter.addColumnSql('users', name, type));
        } catch (err) {
            if (!adapter.isDuplicateColumnError(err)) {
                logger.error(`usrstorage._checkUpdate error! ${err}`);
            }
        }
    }
}

/**
 * Set default users value in database (administrator)
 */
async function setDefault() {
    try {
        const groupsCol = adapter.quoteId('groups');
        const sql = adapter.upsertSql('users', ['username', 'fullname', 'password', groupsCol]);
        const params = ['admin', 'Administrator Account', bcrypt.hashSync('123456', 10), -1];
        await adapter.run(sql, params);
    } catch (err) {
        logger.error(`usrstorage.set failed! ${err}`);
        throw err;
    }
}

/**
 * Return the Users list
 */
async function getUsers(user) {
    const groupsCol = adapter.quoteId('groups');
    var sql = `SELECT username, fullname, password, ${groupsCol}, info FROM users`;
    var params = [];
    if (user && user.username) {
        sql += " WHERE username = ?";
        params = [user.username];
    }
    return await adapter.all(sql, params);
}

/**
 * Set user value in database
 */
async function setUser(usr, fullname, pwd, groups, info) {
    try {
        const groupsCol = adapter.quoteId('groups');
        const data = await getUsers({ username: usr });
        const exist = data && data.length;
        let sql = '';
        let params = [];
        if (pwd) {
            const hashedPwd = bcrypt.hashSync(pwd, 10);
            if (exist) {
                sql = `UPDATE users SET password = ?, info = ?, ${groupsCol} = ?, fullname = ? WHERE username = ?`;
                params = [hashedPwd, info, groups, fullname, usr];
            } else {
                sql = adapter.upsertSql('users', ['username', 'fullname', 'password', groupsCol, 'info']);
                params = [usr, fullname, hashedPwd, groups, info];
            }
        } else if (exist) {
            sql = `UPDATE users SET ${groupsCol} = ?, info = ?, fullname = ? WHERE username = ?`;
            params = [groups, info, fullname, usr];
        } else {
            sql = adapter.upsertSql('users', ['username', 'fullname', groupsCol, 'info']);
            params = [usr, fullname, groups, info];
        }
        await adapter.run(sql, params);
    } catch (err) {
        logger.error(`usrstorage.set failed! ${err}`);
        throw err;
    }
}

/**
 * Remove user from database
 */
async function removeUser(usr) {
    try {
        await adapter.run("DELETE FROM users WHERE username = ?", [usr]);
    } catch (err) {
        logger.error(`usrstorage.remove failed! ${err}`);
        throw err;
    }
}

/**
 * Return the Roles list
 */
async function getRoles() {
    return await adapter.all("SELECT value FROM roles");
}

/**
 * Set roles value in database
 */
async function setRoles(roles) {
    try {
        const sql = adapter.upsertSql('roles', ['name', 'value']);
        for (const role of roles) {
            const value = JSON.stringify(role);
            await adapter.run(sql, [role.id, value]);
        }
    } catch (err) {
        logger.error(`usrstorage.set role failed! ${err}`);
        throw err;
    }
}

/**
 * Remove roles from database
 */
async function removeRoles(roles) {
    try {
        const sql = "DELETE FROM roles WHERE name = ?";
        for (const role of roles) {
            await adapter.run(sql, [role.id]);
        }
    } catch (err) {
        logger.error(`usrstorage.remove role failed! ${err}`);
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
    setDefault: setDefault,
    getUsers: getUsers,
    setUser: setUser,
    removeUser: removeUser,
    getRoles: getRoles,
    setRoles: setRoles,
    removeRoles: removeRoles
};
