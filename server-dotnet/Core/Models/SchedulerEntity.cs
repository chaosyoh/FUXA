using SqlSugar;

namespace Core.Models;

[SugarTable("schedulers")]
public class SchedulerEntity
{
    [SugarColumn(IsPrimaryKey = true, ColumnName = "id")]
    public string Id { get; set; } = string.Empty;

    [SugarColumn(ColumnDataType = "longtext", ColumnName = "data")]
    public string Data { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "created_at")]
    public DateTime CreatedAt { get; set; }

    [SugarColumn(ColumnName = "updated_at")]
    public DateTime UpdatedAt { get; set; }
}
