namespace Fuxa.DbMigrate.Models;

/// <summary>
/// Column definition with type mappings for MySQL and SQL Server
/// </summary>
public class ColumnDef
{
    public string Name { get; init; } = string.Empty;
    public string SqliteType { get; init; } = string.Empty;
    public string MysqlType { get; init; } = string.Empty;
    public string MssqlType { get; init; } = string.Empty;
}

/// <summary>
/// Table definition with columns and primary key info
/// </summary>
public class TableDef
{
    public string Name { get; init; } = string.Empty;

    /// <summary>Target table name (if different from source, e.g. to avoid name conflicts)</summary>
    public string? TargetTable { get; init; }

    public string? PrimaryKey { get; init; }
    public bool AutoIncrement { get; init; }
    public List<ColumnDef> Columns { get; init; } = [];
}

/// <summary>
/// Database definition: one SQLite file with multiple tables
/// </summary>
public class DatabaseDef
{
    public string Name { get; init; } = string.Empty;
    public string DbFile { get; init; } = string.Empty;
    public List<TableDef> Tables { get; init; } = [];
}

/// <summary>
/// DAQ database definition: dynamic file names matched by prefix
/// </summary>
public class DaqDatabaseDef
{
    public string Name { get; init; } = string.Empty;
    public string DbPrefix { get; init; } = string.Empty;
    public List<TableDef> Tables { get; init; } = [];
}

/// <summary>
/// All FUXA database schema definitions
/// </summary>
public static class SchemaDefinitions
{
    public static readonly List<DatabaseDef> Databases =
    [
        new()
        {
            Name = "project",
            DbFile = "project.fuxap.db",
            Tables =
            [
                KvTable("general"),
                KvTable("views"),
                new()
                {
                    Name = "devices",
                    PrimaryKey = "name",
                    Columns =
                    [
                        new() { Name = "name", SqliteType = "TEXT", MysqlType = "VARCHAR(255)", MssqlType = "NVARCHAR(255)" },
                        new() { Name = "value", SqliteType = "TEXT", MysqlType = "LONGTEXT", MssqlType = "NVARCHAR(MAX)" },
                        new() { Name = "connection", SqliteType = "TEXT", MysqlType = "TEXT", MssqlType = "NVARCHAR(MAX)" },
                        new() { Name = "cntid", SqliteType = "TEXT", MysqlType = "VARCHAR(255)", MssqlType = "NVARCHAR(255)" },
                        new() { Name = "cntpwd", SqliteType = "TEXT", MysqlType = "VARCHAR(255)", MssqlType = "NVARCHAR(255)" },
                    ]
                },
                KvTable("devicesSecurity"),
                KvTable("texts"),
                KvTable("alarms"),
                KvTable("notifications"),
                KvTable("scripts"),
                KvTable("reports"),
                KvTable("locations"),
            ]
        },
        new()
        {
            Name = "users",
            DbFile = "users.fuxap.db",
            Tables =
            [
                new()
                {
                    Name = "users",
                    PrimaryKey = "username",
                    Columns =
                    [
                        new() { Name = "username", SqliteType = "TEXT", MysqlType = "VARCHAR(255)", MssqlType = "NVARCHAR(255)" },
                        new() { Name = "fullname", SqliteType = "TEXT", MysqlType = "VARCHAR(255)", MssqlType = "NVARCHAR(255)" },
                        new() { Name = "password", SqliteType = "TEXT", MysqlType = "VARCHAR(255)", MssqlType = "NVARCHAR(255)" },
                        new() { Name = "groups", SqliteType = "INTEGER", MysqlType = "INT", MssqlType = "INT" },
                        new() { Name = "info", SqliteType = "TEXT", MysqlType = "TEXT", MssqlType = "NVARCHAR(MAX)" },
                    ]
                },
                KvTable("roles"),
            ]
        },
        new()
        {
            Name = "apikeys",
            DbFile = "apikeys.fuxap.db",
            Tables = [KvTable("apikeys")]
        },
        new()
        {
            Name = "notifications",
            DbFile = "notifications.fuxap.db",
            Tables =
            [
                new()
                {
                    Name = "chronicle",
                    TargetTable = "notifications_chronicle",
                    PrimaryKey = "Sn",
                    AutoIncrement = true,
                    Columns =
                    [
                        new() { Name = "Sn", SqliteType = "INTEGER", MysqlType = "INT AUTO_INCREMENT", MssqlType = "INT IDENTITY(1,1)" },
                        new() { Name = "id", SqliteType = "TEXT", MysqlType = "VARCHAR(255)", MssqlType = "NVARCHAR(255)" },
                        new() { Name = "name", SqliteType = "TEXT", MysqlType = "VARCHAR(255)", MssqlType = "NVARCHAR(255)" },
                        new() { Name = "type", SqliteType = "TEXT", MysqlType = "VARCHAR(100)", MssqlType = "NVARCHAR(100)" },
                        new() { Name = "receiver", SqliteType = "TEXT", MysqlType = "VARCHAR(255)", MssqlType = "NVARCHAR(255)" },
                        new() { Name = "text", SqliteType = "TEXT", MysqlType = "TEXT", MssqlType = "NVARCHAR(MAX)" },
                        new() { Name = "notifytime", SqliteType = "INTEGER", MysqlType = "BIGINT", MssqlType = "BIGINT" },
                        new() { Name = "notifytype", SqliteType = "TEXT", MysqlType = "VARCHAR(100)", MssqlType = "NVARCHAR(100)" },
                    ]
                },
            ]
        },
        new()
        {
            Name = "alarms",
            DbFile = "alarms.fuxap.db",
            Tables =
            [
                new()
                {
                    Name = "alarms",
                    TargetTable = "alarms_runtime",
                    PrimaryKey = "nametype",
                    Columns =
                    [
                        new() { Name = "nametype", SqliteType = "TEXT", MysqlType = "VARCHAR(255)", MssqlType = "NVARCHAR(255)" },
                        new() { Name = "type", SqliteType = "TEXT", MysqlType = "VARCHAR(100)", MssqlType = "NVARCHAR(100)" },
                        new() { Name = "status", SqliteType = "TEXT", MysqlType = "VARCHAR(100)", MssqlType = "NVARCHAR(100)" },
                        new() { Name = "ontime", SqliteType = "INTEGER", MysqlType = "BIGINT", MssqlType = "BIGINT" },
                        new() { Name = "offtime", SqliteType = "INTEGER", MysqlType = "BIGINT", MssqlType = "BIGINT" },
                        new() { Name = "acktime", SqliteType = "INTEGER", MysqlType = "BIGINT", MssqlType = "BIGINT" },
                    ]
                },
                new()
                {
                    Name = "chronicle",
                    TargetTable = "alarms_chronicle",
                    PrimaryKey = "Sn",
                    AutoIncrement = true,
                    Columns =
                    [
                        new() { Name = "Sn", SqliteType = "INTEGER", MysqlType = "INT AUTO_INCREMENT", MssqlType = "INT IDENTITY(1,1)" },
                        new() { Name = "nametype", SqliteType = "TEXT", MysqlType = "VARCHAR(255)", MssqlType = "NVARCHAR(255)" },
                        new() { Name = "type", SqliteType = "TEXT", MysqlType = "VARCHAR(100)", MssqlType = "NVARCHAR(100)" },
                        new() { Name = "status", SqliteType = "TEXT", MysqlType = "VARCHAR(100)", MssqlType = "NVARCHAR(100)" },
                        new() { Name = "text", SqliteType = "TEXT", MysqlType = "TEXT", MssqlType = "NVARCHAR(MAX)" },
                        new() { Name = "grp", SqliteType = "TEXT", MysqlType = "VARCHAR(255)", MssqlType = "NVARCHAR(255)" },
                        new() { Name = "ontime", SqliteType = "INTEGER", MysqlType = "BIGINT", MssqlType = "BIGINT" },
                        new() { Name = "offtime", SqliteType = "INTEGER", MysqlType = "BIGINT", MssqlType = "BIGINT" },
                        new() { Name = "acktime", SqliteType = "INTEGER", MysqlType = "BIGINT", MssqlType = "BIGINT" },
                        new() { Name = "userack", SqliteType = "TEXT", MysqlType = "VARCHAR(255)", MssqlType = "NVARCHAR(255)" },
                    ]
                },
            ]
        },
        new()
        {
            Name = "scheduler",
            DbFile = "scheduler.db",
            Tables =
            [
                new()
                {
                    Name = "schedulers",
                    PrimaryKey = "id",
                    Columns =
                    [
                        new() { Name = "id", SqliteType = "TEXT", MysqlType = "VARCHAR(255)", MssqlType = "NVARCHAR(255)" },
                        new() { Name = "data", SqliteType = "TEXT", MysqlType = "LONGTEXT", MssqlType = "NVARCHAR(MAX)" },
                        new() { Name = "created_at", SqliteType = "DATETIME", MysqlType = "DATETIME", MssqlType = "DATETIME2" },
                        new() { Name = "updated_at", SqliteType = "DATETIME", MysqlType = "DATETIME", MssqlType = "DATETIME2" },
                    ]
                }
            ]
        },
        new()
        {
            Name = "currentTagReadings",
            DbFile = "currentTagReadings.db",
            Tables =
            [
                new()
                {
                    Name = "currentValues",
                    PrimaryKey = "tagId",
                    Columns =
                    [
                        new() { Name = "tagId", SqliteType = "TEXT", MysqlType = "VARCHAR(255)", MssqlType = "NVARCHAR(255)" },
                        new() { Name = "deviceId", SqliteType = "TEXT", MysqlType = "VARCHAR(255)", MssqlType = "NVARCHAR(255)" },
                        new() { Name = "value", SqliteType = "TEXT", MysqlType = "TEXT", MssqlType = "NVARCHAR(MAX)" },
                    ]
                }
            ]
        },
    ];

    /// <summary>
    /// DAQ databases (per-device, dynamic file names matched by prefix)
    /// </summary>
    public static readonly List<DaqDatabaseDef> DaqDatabases =
    [
        new()
        {
            Name = "daq-map",
            DbPrefix = "daq-map_",
            Tables =
            [
                new()
                {
                    Name = "data",
                    PrimaryKey = "mapid",
                    AutoIncrement = true,
                    Columns =
                    [
                        new() { Name = "mapid", SqliteType = "INTEGER", MysqlType = "INT AUTO_INCREMENT", MssqlType = "INT IDENTITY(1,1)" },
                        new() { Name = "id", SqliteType = "TEXT", MysqlType = "VARCHAR(255)", MssqlType = "NVARCHAR(255)" },
                        new() { Name = "name", SqliteType = "TEXT", MysqlType = "VARCHAR(255)", MssqlType = "NVARCHAR(255)" },
                        new() { Name = "type", SqliteType = "TEXT", MysqlType = "VARCHAR(100)", MssqlType = "NVARCHAR(100)" },
                    ]
                }
            ]
        },
        new()
        {
            Name = "daq-data",
            DbPrefix = "daq-data_",
            Tables =
            [
                new()
                {
                    Name = "data",
                    PrimaryKey = null,
                    Columns =
                    [
                        new() { Name = "dt", SqliteType = "INTEGER", MysqlType = "BIGINT", MssqlType = "BIGINT" },
                        new() { Name = "id", SqliteType = "INTEGER", MysqlType = "INT", MssqlType = "INT" },
                        new() { Name = "value", SqliteType = "TEXT", MysqlType = "TEXT", MssqlType = "NVARCHAR(MAX)" },
                    ]
                }
            ]
        },
    ];

    /// <summary>Helper: create a simple key-value table (name TEXT PK, value TEXT)</summary>
    private static TableDef KvTable(string name) => new()
    {
        Name = name,
        PrimaryKey = "name",
        Columns =
        [
            new() { Name = "name", SqliteType = "TEXT", MysqlType = "VARCHAR(255)", MssqlType = "NVARCHAR(255)" },
            new() { Name = "value", SqliteType = "TEXT", MysqlType = "LONGTEXT", MssqlType = "NVARCHAR(MAX)" },
        ]
    };
}
