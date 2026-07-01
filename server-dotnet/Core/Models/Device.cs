using SqlSugar;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Core.Models;

/// <summary>
/// SCADA设备
/// </summary>
[SugarTable(TableName = "SCADA_Device", TableDescription = "SCADA设备")]
public class Device
{
    /// <summary>
    /// 主键Id
    /// </summary>
    [SugarColumn(IsPrimaryKey = true, Length = 20)]
    public string Id { get; set; } = string.Empty;
    /// <summary>
    /// 名称
    /// </summary>
    [SugarColumn(Length = 255)]
    public string Name { get; set; } = string.Empty;
    /// <summary>
    /// 
    /// </summary>
    public bool Enabled { get; set; }
    /// <summary>
    /// 
    /// </summary>
    [SugarColumn(IsJson = true, Length = 255)]
    public DeviceProperty Property { get; set; } = new DeviceProperty();
    /// <summary>
    /// 
    /// </summary>
    [SugarColumn(Length = 255)]
    public string Type { get; set; } = string.Empty;
    /// <summary>
    /// 
    /// </summary>
    public int Polling { get; set; }
    /// <summary>
    /// 是否启用降级重连模式
    /// </summary>
    [SugarColumn(IsNullable = true)]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public bool? DegradeEnabled { get; set; } = true;
    /// <summary>
    /// 进入降级模式前的重试次数
    /// </summary>
    [SugarColumn(IsNullable = true)]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? DegradeRetryCount { get; set; } = 2;
    /// <summary>
    /// 降级周期（秒），降级后每个周期仅尝试一次重连
    /// </summary>
    [SugarColumn(IsNullable = true)]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? DegradePeriod { get; set; } = 60;
    /// <summary>
    /// 
    /// </summary>
    [SugarColumn(IsIgnore = true)]
    public Dictionary<string, Tag> Tags { get; set; } = new Dictionary<string, Tag>();
    /// <summary>
    /// Tag 分组（文件夹）
    /// </summary>
    [SugarColumn(IsIgnore = true)]
    public Dictionary<string, TagGroup> TagGroups { get; set; } = new Dictionary<string, TagGroup>();
    /// <summary>
    /// Device folder ID, empty or null means root level
    /// </summary>
    [SugarColumn(Length = 50, IsNullable = true)]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? FolderId { get; set; }
}
/// <summary>
/// 
/// </summary>
public class DeviceProperty
{
    /// <summary>
    /// 
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Address { get; set; }
    /// <summary>
    /// 
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Port { get; set; }
    /// <summary>
    /// 
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Slot { get; set; }
    /// <summary>
    /// 
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Rack { get; set; }
    /// <summary>
    /// CPU type for Siemens S7 (S7200, S7200Smart, S7300, S7400, S71200, S71500)
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? CpuType { get; set; }
    /// <summary>
    /// 
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? SlaveId { get; set; }
    /// <summary>
    /// 
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Baudrate { get; set; }
    /// <summary>
    /// 
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Databits { get; set; }
    /// <summary>
    /// 
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Stopbits { get; set; }
    /// <summary>
    /// Serial parity for ModbusRTU (none, even, odd, mark, space)
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Parity { get; set; }
    /// <summary>
    /// MQTT ClientId
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? ClientId { get; set; }
    /// <summary>
    /// MQTT/OPCUA Username
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Username { get; set; }
    /// <summary>
    /// MQTT/OPCUA Password
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Password { get; set; }
    /// <summary>
    /// HTTP/WebAPI GET URL
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? GetUrl { get; set; }
    /// <summary>
    /// HTTP/WebAPI POST URL
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? PostUrl { get; set; }
}

