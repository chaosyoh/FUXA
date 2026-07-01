using SqlSugar;

namespace Core.Entity;

[SugarTable("apikeys")]
public class ApiKey
{
    [SugarColumn(ColumnName = "id", IsPrimaryKey = true)]
    public string Id { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "name")]
    public string Name { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "key")]
    public string Key { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "groups")]
    public int Groups { get; set; } = -1;

    [SugarColumn(ColumnName = "expire")]
    public long? Expire { get; set; }

    [SugarColumn(ColumnName = "info")]
    public string? Info { get; set; }
}
