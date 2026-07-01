namespace Core.Models;

/// <summary>
/// 报警联动动作容器（对应 Node.js alarm.actions）
/// </summary>
public class AlarmSubActions
{
    public bool Enabled { get; set; }
    public List<AlarmAction> Values { get; set; } = new();
}

/// <summary>
/// 单个联动动作定义（对应 Node.js alarm.actions.values[i]）
/// </summary>
public class AlarmAction
{
    public string Type { get; set; } = string.Empty;
    public string? Actparam { get; set; }
    public string? VariableId { get; set; }
    public object? Actoptions { get; set; }
    public int? Checkdelay { get; set; }
    public double? Min { get; set; }
    public double? Max { get; set; }
    public int? Timedelay { get; set; }
}

/// <summary>
/// 客户端联动动作 DTO（广播给前端）
/// </summary>
public class AlarmActionDto
{
    public string Type { get; set; } = string.Empty;
    public string? Params { get; set; }
    public object? Options { get; set; }
}
