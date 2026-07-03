using SqlSugar;

namespace Core.Models;

[SugarTable("notifications_chronicle")]
public class NotificationChronicle
{
    [SugarColumn(IsPrimaryKey = true, IsIdentity = true, ColumnName = "Sn")]
    public int Sn { get; set; }

    [SugarColumn(ColumnName = "id")]
    public string Id { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "name")]
    public string Name { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "type")]
    public string Type { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "receiver")]
    public string Receiver { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "text")]
    public string Text { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "notifytime")]
    public long Notifytime { get; set; }

    [SugarColumn(ColumnName = "notifytype")]
    public string Notifytype { get; set; } = string.Empty;
}
