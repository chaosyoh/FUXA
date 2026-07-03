using SqlSugar;

namespace Core.Models;

[SugarTable("alarms_runtime")]
public class AlarmRecord
{
    [SugarColumn(IsPrimaryKey = true, ColumnName = "nametype")]
    public string Nametype { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "type")]
    public string Type { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "status")]
    public string Status { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "ontime")]
    public long Ontime { get; set; }

    [SugarColumn(ColumnName = "offtime")]
    public long Offtime { get; set; }

    [SugarColumn(ColumnName = "acktime")]
    public long Acktime { get; set; }
}
