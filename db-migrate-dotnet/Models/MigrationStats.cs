namespace Fuxa.DbMigrate.Models;

/// <summary>
/// Migration result statistics
/// </summary>
public class MigrationStats
{
    public int Databases { get; set; }
    public int Tables { get; set; }
    public long Rows { get; set; }
    public int Skipped { get; set; }
    public List<string> Errors { get; set; } = [];
    public List<MigratedTableInfo> MigratedTables { get; set; } = [];
}

/// <summary>
/// Info about a single migrated table
/// </summary>
public class MigratedTableInfo
{
    public string Database { get; set; } = string.Empty;
    public string Table { get; set; } = string.Empty;
    public long Rows { get; set; }
}

/// <summary>
/// Summary info for preview display
/// </summary>
public class DatabaseSummary
{
    public string Name { get; set; } = string.Empty;
    public string DbFile { get; set; } = string.Empty;
    public List<TableSummary> Tables { get; set; } = [];
}

/// <summary>
/// Table row count summary
/// </summary>
public class TableSummary
{
    public string Name { get; set; } = string.Empty;
    public long Rows { get; set; }
}
