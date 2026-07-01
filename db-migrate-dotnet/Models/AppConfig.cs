namespace Fuxa.DbMigrate.Models;

/// <summary>
/// Root configuration bound from appsettings.json
/// </summary>
public class AppConfig
{
    public MigrationConfig Migration { get; set; } = new();
    public TargetConfig Target { get; set; } = new();
}

/// <summary>
/// Migration options
/// </summary>
public class MigrationConfig
{
    /// <summary>FUXA _appdata directory path</summary>
    public string AppdataDir { get; set; } = string.Empty;

    /// <summary>
    /// Migration mode:
    ///   "preview"  - scan and show summary only (default)
    ///   "dry-run"  - preview migration without writing data
    ///   "run"      - execute migration
    /// </summary>
    public string Mode { get; set; } = "preview";

    /// <summary>Batch size for large inserts</summary>
    public int BatchSize { get; set; } = 1000;

    /// <summary>Include DAQ time-series data migration</summary>
    public bool IncludeDaq { get; set; }

    /// <summary>Truncate target tables before insert</summary>
    public bool Truncate { get; set; } = true;

    /// <summary>Specific databases to migrate (empty = all)</summary>
    public List<string> Databases { get; set; } = [];
}

/// <summary>
/// Target database connection configuration
/// </summary>
public class TargetConfig
{
    /// <summary>Database type: "MySql" or "SqlServer"</summary>
    public string DbType { get; set; } = "MySql";

    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 3306;
    public string User { get; set; } = "root";
    public string Password { get; set; } = string.Empty;
    public string Database { get; set; } = "fuxa";

    /// <summary>SQL Server specific: encrypt connection</summary>
    public bool Encrypt { get; set; }

    /// <summary>SQL Server specific: trust server certificate</summary>
    public bool TrustServerCertificate { get; set; } = true;
}
