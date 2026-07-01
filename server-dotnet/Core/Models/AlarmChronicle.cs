using SqlSugar;

namespace Core.Models;

[SugarTable("chronicle")]
public class AlarmChronicle
{
    [SugarColumn(IsPrimaryKey = true, IsIdentity = true, ColumnName = "Sn")]
    public int Sn { get; set; }

    [SugarColumn(ColumnName = "nametype")]
    public string Nametype { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "type")]
    public string Type { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "status")]
    public string Status { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "text")]
    public string Text { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "grp")]
    public string Grp { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "ontime")]
    public long Ontime { get; set; }

    [SugarColumn(ColumnName = "offtime")]
    public long Offtime { get; set; }

    [SugarColumn(ColumnName = "acktime")]
    public long Acktime { get; set; }

    [SugarColumn(ColumnName = "userack")]
    public string Userack { get; set; } = string.Empty;
}
