/**
 * FUXA SQLite databases schema definitions
 * 
 * Each database entry contains:
 *   - dbFile: SQLite file name (relative to FUXA _appdata directory)
 *   - tables: array of table definitions with column info and SQL types for MySQL/SQL Server
 */

const DATABASES = [
  {
    name: 'project',
    dbFile: 'project.fuxap.db',
    tables: [
      {
        name: 'general',
        primaryKey: 'name',
        columns: [
          { name: 'name', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'value', sqliteType: 'TEXT', mysqlType: 'LONGTEXT', mssqlType: 'NVARCHAR(MAX)' },
        ],
      },
      {
        name: 'views',
        primaryKey: 'name',
        columns: [
          { name: 'name', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'value', sqliteType: 'TEXT', mysqlType: 'LONGTEXT', mssqlType: 'NVARCHAR(MAX)' },
        ],
      },
      {
        name: 'devices',
        primaryKey: 'name',
        columns: [
          { name: 'name', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'value', sqliteType: 'TEXT', mysqlType: 'LONGTEXT', mssqlType: 'NVARCHAR(MAX)' },
          { name: 'connection', sqliteType: 'TEXT', mysqlType: 'TEXT', mssqlType: 'NVARCHAR(MAX)' },
          { name: 'cntid', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'cntpwd', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
        ],
      },
      {
        name: 'devicesSecurity',
        primaryKey: 'name',
        columns: [
          { name: 'name', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'value', sqliteType: 'TEXT', mysqlType: 'LONGTEXT', mssqlType: 'NVARCHAR(MAX)' },
        ],
      },
      {
        name: 'texts',
        primaryKey: 'name',
        columns: [
          { name: 'name', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'value', sqliteType: 'TEXT', mysqlType: 'LONGTEXT', mssqlType: 'NVARCHAR(MAX)' },
        ],
      },
      {
        name: 'alarms',
        primaryKey: 'name',
        columns: [
          { name: 'name', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'value', sqliteType: 'TEXT', mysqlType: 'LONGTEXT', mssqlType: 'NVARCHAR(MAX)' },
        ],
      },
      {
        name: 'notifications',
        primaryKey: 'name',
        columns: [
          { name: 'name', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'value', sqliteType: 'TEXT', mysqlType: 'LONGTEXT', mssqlType: 'NVARCHAR(MAX)' },
        ],
      },
      {
        name: 'scripts',
        primaryKey: 'name',
        columns: [
          { name: 'name', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'value', sqliteType: 'TEXT', mysqlType: 'LONGTEXT', mssqlType: 'NVARCHAR(MAX)' },
        ],
      },
      {
        name: 'reports',
        primaryKey: 'name',
        columns: [
          { name: 'name', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'value', sqliteType: 'TEXT', mysqlType: 'LONGTEXT', mssqlType: 'NVARCHAR(MAX)' },
        ],
      },
      {
        name: 'locations',
        primaryKey: 'name',
        columns: [
          { name: 'name', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'value', sqliteType: 'TEXT', mysqlType: 'LONGTEXT', mssqlType: 'NVARCHAR(MAX)' },
        ],
      },
    ],
  },
  {
    name: 'users',
    dbFile: 'users.fuxap.db',
    tables: [
      {
        name: 'users',
        primaryKey: 'username',
        columns: [
          { name: 'username', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'fullname', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'password', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'groups', sqliteType: 'INTEGER', mysqlType: 'INT', mssqlType: 'INT' },
          { name: 'info', sqliteType: 'TEXT', mysqlType: 'TEXT', mssqlType: 'NVARCHAR(MAX)' },
        ],
      },
      {
        name: 'roles',
        primaryKey: 'name',
        columns: [
          { name: 'name', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'value', sqliteType: 'TEXT', mysqlType: 'LONGTEXT', mssqlType: 'NVARCHAR(MAX)' },
        ],
      },
    ],
  },
  {
    name: 'apikeys',
    dbFile: 'apikeys.fuxap.db',
    tables: [
      {
        name: 'apikeys',
        primaryKey: 'name',
        columns: [
          { name: 'name', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'value', sqliteType: 'TEXT', mysqlType: 'LONGTEXT', mssqlType: 'NVARCHAR(MAX)' },
        ],
      },
    ],
  },
  {
    name: 'notifications',
    dbFile: 'notifications.fuxap.db',
    tables: [
      {
        name: 'chronicle',
        targetTable: 'notifications_chronicle',  // MySQL shared-db: avoid conflict with alarms.chronicle
        primaryKey: 'Sn',
        autoIncrement: true,
        columns: [
          { name: 'Sn', sqliteType: 'INTEGER', mysqlType: 'INT AUTO_INCREMENT', mssqlType: 'INT IDENTITY(1,1)' },
          { name: 'id', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'name', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'type', sqliteType: 'TEXT', mysqlType: 'VARCHAR(100)', mssqlType: 'NVARCHAR(100)' },
          { name: 'receiver', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'text', sqliteType: 'TEXT', mysqlType: 'TEXT', mssqlType: 'NVARCHAR(MAX)' },
          { name: 'notifytime', sqliteType: 'INTEGER', mysqlType: 'BIGINT', mssqlType: 'BIGINT' },
          { name: 'notifytype', sqliteType: 'TEXT', mysqlType: 'VARCHAR(100)', mssqlType: 'NVARCHAR(100)' },
        ],
      },
    ],
  },
  {
    name: 'alarms',
    dbFile: 'alarms.fuxap.db',
    tables: [
      {
        name: 'alarms',
        targetTable: 'alarms_runtime',  // MySQL shared-db: avoid conflict with project.alarms
        primaryKey: 'nametype',
        columns: [
          { name: 'nametype', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'type', sqliteType: 'TEXT', mysqlType: 'VARCHAR(100)', mssqlType: 'NVARCHAR(100)' },
          { name: 'status', sqliteType: 'TEXT', mysqlType: 'VARCHAR(100)', mssqlType: 'NVARCHAR(100)' },
          { name: 'ontime', sqliteType: 'INTEGER', mysqlType: 'BIGINT', mssqlType: 'BIGINT' },
          { name: 'offtime', sqliteType: 'INTEGER', mysqlType: 'BIGINT', mssqlType: 'BIGINT' },
          { name: 'acktime', sqliteType: 'INTEGER', mysqlType: 'BIGINT', mssqlType: 'BIGINT' },
        ],
      },
      {
        name: 'chronicle',
        targetTable: 'alarms_chronicle',  // MySQL shared-db: avoid conflict with notifications.chronicle
        primaryKey: 'Sn',
        autoIncrement: true,
        columns: [
          { name: 'Sn', sqliteType: 'INTEGER', mysqlType: 'INT AUTO_INCREMENT', mssqlType: 'INT IDENTITY(1,1)' },
          { name: 'nametype', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'type', sqliteType: 'TEXT', mysqlType: 'VARCHAR(100)', mssqlType: 'NVARCHAR(100)' },
          { name: 'status', sqliteType: 'TEXT', mysqlType: 'VARCHAR(100)', mssqlType: 'NVARCHAR(100)' },
          { name: 'text', sqliteType: 'TEXT', mysqlType: 'TEXT', mssqlType: 'NVARCHAR(MAX)' },
          { name: 'grp', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'ontime', sqliteType: 'INTEGER', mysqlType: 'BIGINT', mssqlType: 'BIGINT' },
          { name: 'offtime', sqliteType: 'INTEGER', mysqlType: 'BIGINT', mssqlType: 'BIGINT' },
          { name: 'acktime', sqliteType: 'INTEGER', mysqlType: 'BIGINT', mssqlType: 'BIGINT' },
          { name: 'userack', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
        ],
      },
    ],
  },
  {
    name: 'scheduler',
    dbFile: 'scheduler.db',
    tables: [
      {
        name: 'schedulers',
        primaryKey: 'id',
        columns: [
          { name: 'id', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'data', sqliteType: 'TEXT', mysqlType: 'LONGTEXT', mssqlType: 'NVARCHAR(MAX)' },
          { name: 'created_at', sqliteType: 'DATETIME', mysqlType: 'DATETIME', mssqlType: 'DATETIME2' },
          { name: 'updated_at', sqliteType: 'DATETIME', mysqlType: 'DATETIME', mssqlType: 'DATETIME2' },
        ],
      },
    ],
  },
  {
    name: 'currentTagReadings',
    dbFile: 'currentTagReadings.db',
    tables: [
      {
        name: 'currentValues',
        primaryKey: 'tagId',
        columns: [
          { name: 'tagId', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'deviceId', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'value', sqliteType: 'TEXT', mysqlType: 'TEXT', mssqlType: 'NVARCHAR(MAX)' },
        ],
      },
    ],
  },
];

/**
 * DAQ databases (per-device, dynamic file names).
 * These are handled separately since there can be multiple files per device.
 */
const DAQ_DATABASES = [
  {
    name: 'daq-map',
    dbPrefix: 'daq-map_',
    tables: [
      {
        name: 'data',
        primaryKey: 'mapid',
        autoIncrement: true,
        columns: [
          { name: 'mapid', sqliteType: 'INTEGER', mysqlType: 'INT AUTO_INCREMENT', mssqlType: 'INT IDENTITY(1,1)' },
          { name: 'id', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'name', sqliteType: 'TEXT', mysqlType: 'VARCHAR(255)', mssqlType: 'NVARCHAR(255)' },
          { name: 'type', sqliteType: 'TEXT', mysqlType: 'VARCHAR(100)', mssqlType: 'NVARCHAR(100)' },
        ],
      },
    ],
  },
  {
    name: 'daq-data',
    dbPrefix: 'daq-data_',
    tables: [
      {
        name: 'data',
        primaryKey: null, // No primary key, high-volume data
        columns: [
          { name: 'dt', sqliteType: 'INTEGER', mysqlType: 'BIGINT', mssqlType: 'BIGINT' },
          { name: 'id', sqliteType: 'INTEGER', mysqlType: 'INT', mssqlType: 'INT' },
          { name: 'value', sqliteType: 'TEXT', mysqlType: 'TEXT', mssqlType: 'NVARCHAR(MAX)' },
        ],
      },
    ],
  },
];

module.exports = { DATABASES, DAQ_DATABASES };
