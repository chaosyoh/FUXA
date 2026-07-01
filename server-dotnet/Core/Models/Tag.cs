using Core.Utils;
using SqlSugar;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Core.Models;

[SugarTable(TableName = "SCADA_Tag", TableDescription = "SCADA变量")]
public class Tag
{
    /// <summary>
    /// 主键Id
    /// </summary>
    [SugarColumn(IsPrimaryKey = true, Length = 20)]
    public string Id { get; set; } = string.Empty;
    /// <summary>
    /// 
    /// </summary>
    [SugarColumn(Length = 20)]
    public string DeviceId { get; set; } = string.Empty;
    /// <summary>
    /// 变量名称
    /// </summary>
    [SugarColumn(Length = 255)]
    public string Name { get; set; } = string.Empty;
    /// <summary>
    /// 
    /// </summary>
    [SugarColumn(Length = 255)]
    public string Label { get; set; } = string.Empty;
    /// <summary>
    /// 变量类型
    /// </summary>
    [SugarColumn(Length = 255)]
    public string Type { get; set; } = string.Empty;
    /// <summary>
    /// 地址
    /// </summary>
    /// <remarks>modbus/webapi使用</remarks>
    [SugarColumn(Length = 255)]
    public string Memaddress { get; set; } = string.Empty;
    /// <summary>
    /// 地址
    /// </summary>
    [SugarColumn(Length = 255)]
    public string Address { get; set; } = string.Empty;
    /// <summary>
    /// 
    /// </summary>
    /// <remarks>modbus使用</remarks>
    public int? Divisor { get; set; }
    /// <summary>
    /// 
    /// </summary>
    [SugarColumn(Length = 255)]
    public string Options { get; set; } = string.Empty;
    /// <summary>
    /// 
    /// </summary>
    [SugarColumn(IsNullable = true, Length = 255)]
    public string? Format { get; set; }
    /// <summary>
    /// 
    /// </summary>
    [SugarColumn(IsJson = true, Length = 255)]
    public Daq Daq { get; set; } = new Daq();
    /// <summary>
    /// 初始值
    /// </summary>
    [SugarColumn(Length = 255)]
    public string Init { get; set; } = string.Empty;
    /// <summary>
    /// 缩放设置
    /// </summary>
    [SugarColumn(IsJson = true, Length = 500, IsNullable = true)]
    [JsonConverter(typeof(ScaleConverter))]
    public Scale? Scale { get; set; }

    /// <summary>
    /// 缩放读函数
    /// </summary>
    [SugarColumn(Length = 500, IsNullable = true)]
    public string? ScaleReadFunction { get; set; }
    /// <summary>
    /// 
    /// </summary>
    [SugarColumn(Length = 500, IsNullable = true)]
    public string? ScaleReadParams { get; set; }
    /// <summary>
    /// 缩放写函数
    /// </summary>
    [SugarColumn(Length = 500, IsNullable = true)]
    public string? ScaleWriteFunction { get; set; }
    /// <summary>
    /// 
    /// </summary>
    [SugarColumn(Length = 500, IsNullable = true)]
    public string? ScaleWriteParams { get; set; }
    /// <summary>
    /// 描述
    /// </summary>
    [SugarColumn(Length = 500, IsNullable = true)]
    public string? Description { get; set; }
    /// <summary>
    /// 死区设置
    /// </summary>
    [SugarColumn(IsJson = true, Length = 500, IsNullable = true)]
    [JsonConverter(typeof(TagDeadbandConverter))]
    public TagDeadband? Deadband { get; set; }
    /// <summary>
    /// 系统标签类型（如设备连接状态）
    /// </summary>
    /// <remarks>前端 TagSystemType 枚举: 1=deviceConnectionStatus</remarks>
    [SugarColumn(IsNullable = true)]
    public int? SysType { get; set; }
    /// <summary>
    /// 
    /// </summary>
    /// <remarks>GPIO使用</remarks>
    [SugarColumn(Length = 500, IsNullable = true)]
    public string? Direction { get; set; }
    /// <summary>
    /// 
    /// </summary>
    /// <remarks>GPIO使用</remarks>
    [SugarColumn(Length = 500, IsNullable = true)]
    public string? Edge { get; set; }

    /// <summary>
    /// 标签所属分组ID，可空表示根级别
    /// </summary>
    [SugarColumn(Length = 20, IsNullable = true)]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? GroupId { get; set; }

    /// <summary>
    /// 读写权限: rw=读写(默认), ro=只读
    /// </summary>
    [SugarColumn(Length = 10, IsNullable = true)]
    public string? Access { get; set; } = "rw";


    /// <summary>
    /// 计算变量的算术表达式，如 "{DeviceA.Tag1} + {DeviceB.Tag2} * 2"
    /// </summary>
    [SugarColumn(Length = 2000, IsNullable = true)]
    public string? Expression { get; set; }

    [SugarColumn(IsIgnore = true)]
    public object? Value { get; set; }

    /// <summary>
    /// 最近一次读取的时间戳（Unix毫秒），运行时使用，不持久化
    /// </summary>
    [SugarColumn(IsIgnore = true)]
    [JsonIgnore(Condition = JsonIgnoreCondition.Always)]
    public long Timestamp { get; set; }

    //[SugarColumn(IsIgnore = true)]
    //public Device Device { get; set; }

}

/// <summary>
/// 
/// </summary>
public class Daq
{
    /// <summary>
    /// 
    /// </summary>
    public bool Enabled { get; set; }
    /// <summary>
    /// 
    /// </summary>
    public bool Changed { get; set; }
    /// <summary>
    /// 
    /// </summary>
    public int Interval { get; set; }
}

/// <summary>
/// 缩放设置
/// </summary>
public class Scale
{
    /// <summary>
    /// 缩放模式
    /// </summary>
    public string Mode { get; set; } = string.Empty;
    /// <summary>
    /// 原始低
    /// </summary>
    public decimal RawLow { get; set; }
    /// <summary>
    /// 原始高
    /// </summary>
    public decimal RawHigh { get; set; }
    /// <summary>
    /// 缩放低
    /// </summary>
    public decimal ScaledLow { get; set; }
    /// <summary>
    /// 缩放高
    /// </summary>
    public decimal ScaledHigh { get; set; }
    /// <summary>
    /// 日期格式
    /// </summary>
    public string DateTimeFormat { get; set; } = string.Empty;
    /// <summary>
    /// 
    /// </summary>
    public string ReadExpression { get; set; } = string.Empty;
    /// <summary>
    /// 
    /// </summary>
    public string WriteExpression { get; set; } = string.Empty;
}

/// <summary>
/// 死区设置
/// </summary>
public class TagDeadband
{
    /// <summary>
    /// 值
    /// </summary>
    public decimal Value { get; set; }
    /// <summary>
    /// 模式
    /// </summary>
    public string Mode { set; get; } = string.Empty;
}

