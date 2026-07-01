using SqlSugar;
using System.Text.Json.Serialization;

namespace Core.Models;

/// <summary>
/// Tag 分组（文件夹）
/// </summary>
[SugarTable(TableName = "SCADA_TagGroup", TableDescription = "SCADA变量分组")]
public class TagGroup
{
    /// <summary>
    /// 主键Id
    /// </summary>
    [SugarColumn(IsPrimaryKey = true, Length = 20)]
    public string Id { get; set; } = string.Empty;

    /// <summary>
    /// 父组ID，空字符串表示根级别
    /// </summary>
    [SugarColumn(Length = 20, IsNullable = true)]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? ParentId { get; set; }

    /// <summary>
    /// 所属设备ID
    /// </summary>
    [SugarColumn(Length = 20)]
    public string DeviceId { get; set; } = string.Empty;

    /// <summary>
    /// 组名称
    /// </summary>
    [SugarColumn(Length = 255)]
    public string Name { get; set; } = string.Empty;
}
