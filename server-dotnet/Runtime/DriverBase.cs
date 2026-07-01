using Core.Const;
using Core.Models;
using Microsoft.AspNetCore.SignalR;
using System.Text.Json;

namespace Runtime;
/// <summary>
/// 驱动基类
/// </summary>
public abstract class DeviceBase : IDevice
{
    protected bool connected { get; set; } = false;

    protected bool monitored { get; set; } = false;

    protected bool working { get; set; } = false;

    private int _overloading = 0;

    private Device? data { get; set; }
    public Device Data
    {
        get
        {
            if (data == null) throw new NullReferenceException("请先调用Load方法加载驱动设置");
            return data;
        }
    }

    protected DateTime lastReadTimestamp = DateTime.MinValue;

    protected string Status { get; set; } = DeviceStatus.Off;

    public readonly IHubContext<DataHub> HubCtx;

    private Action<Dictionary<string, Tag>, string>? _addDaqFnc;
    private Func<string, string, Task<object?>>? _getPropertyFnc;

    public DeviceBase(IHubContext<DataHub> hubCtx)
    {
        HubCtx = hubCtx;
    }

    public virtual void Load(Device data)
    {
        this.data = data;
    }

    public void BindAddDaq(Action<Dictionary<string, Tag>, string> fnc)
    {
        _addDaqFnc = fnc;
    }

    public void BindGetProperty(Func<string, string, Task<object?>> fnc)
    {
        _getPropertyFnc = fnc;
    }

    protected void AddDaq()
    {
        // Set timestamp on all tags before DAQ and push
        var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        if (data?.Tags != null)
        {
            foreach (var tag in data.Tags.Values)
                tag.Timestamp = ts;
        }
        _addDaqFnc?.Invoke(Data.Tags, Data.Id);
    }

    protected Task<object?> GetDeviceProperty(string query, string name)
    {
        return _getPropertyFnc?.Invoke(query, name) ?? Task.FromResult<object?>(null);
    }

    /// <summary>
    /// 过载保护检查。调用 CheckWorking(true) 开始工作，CheckWorking(false) 结束工作。
    /// 返回 true 表示可以继续（或已达到过载阈值需强制断开）；返回 false 表示应跳过本轮。
    /// </summary>
    protected bool CheckWorking(bool check)
    {
        if (check && working)
        {
            _overloading++;
            if (_overloading >= 3) return true; // 过载达阈值，需强制断开
            return false; // 跳过本轮
        }
        working = check;
        if (!check) _overloading = 0;
        return true;
    }

    /// <summary>
    /// 清除所有变量值
    /// </summary>
    protected virtual void ClearVarsValue()
    {
        if (data?.Tags == null) return;
        foreach (var tag in data.Tags.Values)
            tag.Value = null;
    }

    public abstract Task<bool> Connect();

    public abstract Task<bool> Disconnect();

    public abstract Task<bool> Polling();

    public string GetStatus()
    {
        return Status;
    }

    public Daq? GetTagDaqSettings(string id)
    {
        if (Data.Tags.TryGetValue(id, out var tag))
        {
            return tag.Daq;
        }
        return null;
    }

    public Tag? GetTagProperty(string id)
    {
        Data.Tags.TryGetValue(id, out var tag);
        return tag;
    }

    public object? GetValue(string id)
    {
        if (Data.Tags.TryGetValue(id, out var tag))
        {
            return tag.Value;
        }
        return null;
    }

    public Dictionary<string, Tag> GetValues()
    {
        return Data.Tags;
    }

    public bool IsConnected()
    {
        return connected;
    }

    public DateTime LastReadTimestamp()
    {
        return lastReadTimestamp;
    }

    public void SetTagDaqSettings(string id, Daq daq)
    {
        Data.Tags.TryGetValue(id, out Tag? tag);
        if (tag != null) tag.Daq = daq;
    }

    public abstract Task<bool> SetValue(string id, object value);

    /// <summary>
    /// 在写入设备前对值进行预处理：
    /// 1. 将 JsonElement 解包为 .NET 原生类型
    /// 2. 应用反缩放（linear 模式或 expression 模式）
    /// 各驱动的 SetValue 方法在获取 tag 后应调用此方法替代单独的 UnwrapJsonValue。
    /// </summary>
    public static object PrepareWriteValue(object value, Tag tag)
    {
        // Step 1: Unwrap JsonElement
        value = UnwrapJsonValue(value);

        if (tag == null || value == null) return value!;

        try
        {
            // Step 2: Reverse scaling (convert user-facing scaled value back to raw device value)
            if (tag.Scale != null)
            {
                if (tag.Scale.Mode == "linear")
                {
                    // Reverse of: scaled = (scaledHigh - scaledLow) * (raw - rawLow) / (rawHigh - rawLow) + scaledLow
                    // => raw = rawLow + (rawHigh - rawLow) * (scaled - scaledLow) / (scaledHigh - scaledLow)
                    var scaledValue = (decimal)Convert.ToDouble(value);
                    var rawValue = tag.Scale.RawLow +
                        (tag.Scale.RawHigh - tag.Scale.RawLow) * (scaledValue - tag.Scale.ScaledLow) /
                        (tag.Scale.ScaledHigh - tag.Scale.ScaledLow);
                    value = (double)rawValue;
                }
                else if (tag.Scale.Mode == "expression" && !string.IsNullOrEmpty(tag.Scale.WriteExpression))
                {
                    // Simple expression evaluation: replace {value} placeholder with actual value
                    // Supports basic math expressions like "{value} * 10 + 5"
                    value = EvaluateSimpleExpression(tag.Scale.WriteExpression, Convert.ToDouble(value));
                }
            }
        }
        catch
        {
            // If scaling fails, proceed with original unwrapped value
        }

        return value;
    }

    /// <summary>
    /// 将 System.Text.Json 反序列化产生的 JsonElement 解包为 .NET 原生类型。
    /// </summary>
    protected static object UnwrapJsonValue(object value)
    {
        if (value is JsonElement je)
        {
            return je.ValueKind switch
            {
                JsonValueKind.Number => je.TryGetInt64(out var l)
                    ? (l is >= int.MinValue and <= int.MaxValue ? (object)(int)l : l)
                    : je.GetDouble(),
                JsonValueKind.String => je.GetString()!,
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Null => null!,
                _ => value
            };
        }
        return value;
    }

    /// <summary>
    /// 简单表达式求值：支持 {value} 占位符和基本数学运算 (+, -, *, /)
    /// 例如: "{value} * 10 + 5", "({value} - 32) * 5 / 9"
    /// </summary>
    private static double EvaluateSimpleExpression(string expression, double value)
    {
        // Replace {value} placeholder with actual number
        var expr = expression.Replace("{value}", value.ToString(System.Globalization.CultureInfo.InvariantCulture));

        // Use DataTable.Compute for basic math expression evaluation
        var dt = new System.Data.DataTable();
        var result = dt.Compute(expr, string.Empty);
        return Convert.ToDouble(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    public async Task NotifyStatus(string status)
    {
        Status = status;
        if (HubCtx == null) return;
        await HubCtx.Clients.All.SendCoreAsync(IoEventTypes.DEVICE_STATUS, [new {
            Data.Id,
            status,
        }]);
    }
}
