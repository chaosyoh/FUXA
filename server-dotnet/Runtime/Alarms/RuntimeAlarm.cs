using Core.Const;
using Core.Models;

namespace Runtime.Alarms;

/// <summary>
/// Runtime alarm instance with state machine: VOID -> ON -> (OFF or ACK)
/// Mirrors the Node.js Alarm function behavior
/// </summary>
public class RuntimeAlarm
{
    public const string ID_SEPARATOR = "^~^";

    public string Name { get; set; } = string.Empty;
    public string Type { get; set; } = string.Empty;
    public AlarmSubProperty? SubProperty { get; set; }
    public AlarmProperty? TagProperty { get; set; }
    public AlarmAction? ActionConfig { get; set; }
    public string Status { get; set; } = AlarmStatusEnum.VOID;
    public long Ontime { get; set; }
    public long Offtime { get; set; }
    public long Acktime { get; set; }
    public long LastCheck { get; set; }
    public bool ToRemove { get; set; }
    public string Userack { get; set; } = string.Empty;

    public string GetId() => $"{Name}{ID_SEPARATOR}{Type}";

    /// <summary>
    /// Check alarm condition and transition state
    /// Returns true if state changed
    /// </summary>
    public bool Check(long time, double? value)
    {
        if (SubProperty == null || SubProperty.Enabled != true) return false;
        if (value == null) return false;

        // Checkdelay 防抖：距上次检查未超过 checkdelay 秒则跳过
        if (SubProperty.Checkdelay is > 0 && LastCheck > 0
            && LastCheck + (SubProperty.Checkdelay.Value * 1000L) > time)
        {
            return false;
        }

        var oldStatus = Status;
        var isActive = EvaluateCondition(value.Value);

        switch (Status)
        {
            case AlarmStatusEnum.VOID:
                if (isActive)
                {
                    // Timedelay 持续确认
                    if (SubProperty.Timedelay is > 0)
                    {
                        if (Ontime == 0)
                        {
                            // 首次检测到条件为真，记录待确认时间
                            Ontime = time;
                            LastCheck = time;
                            return false;
                        }
                        if (Ontime + (SubProperty.Timedelay.Value * 1000L) > time)
                        {
                            // 延时未到，继续等待
                            LastCheck = time;
                            return false;
                        }
                    }
                    // 延时满足（或无延时），正式转为 ON
                    Status = AlarmStatusEnum.ON;
                    Ontime = Ontime > 0 ? Ontime : time; // 保留首次检测时间
                    Offtime = 0;
                    Acktime = 0;
                }
                else
                {
                    // 条件不成立，重置待确认时间
                    Ontime = 0;
                }
                break;

            case AlarmStatusEnum.ON:
                if (!isActive)
                {
                    if (SubProperty.Ackmode == AlarmAckModeEnum.Float || Acktime > 0)
                    {
                        Status = AlarmStatusEnum.VOID;
                        Offtime = time;
                        ToRemove = true;
                    }
                    else
                    {
                        Status = AlarmStatusEnum.OFF;
                        Offtime = time;
                    }
                }
                else if (Acktime > 0)
                {
                    Status = AlarmStatusEnum.ACK;
                }
                break;

            case AlarmStatusEnum.OFF:
                if (isActive)
                {
                    Status = AlarmStatusEnum.ON;
                    Ontime = time;
                    Offtime = 0;
                    Acktime = 0;
                    Userack = string.Empty;
                }
                else if (Acktime > 0 || Type == AlarmTypeConst.ACTION)
                {
                    // 已确认或 ACTION 类型自动清除
                    ToRemove = true;
                }
                break;

            case AlarmStatusEnum.ACK:
                if (!isActive)
                {
                    Status = AlarmStatusEnum.VOID;
                    Offtime = time;
                    ToRemove = true;
                }
                break;
        }

        LastCheck = time;
        return oldStatus != Status;
    }

    /// <summary>
    /// Acknowledge this alarm
    /// </summary>
    public bool SetAck(string? userId)
    {
        if (Status == AlarmStatusEnum.ON)
        {
            if (SubProperty?.Ackmode == AlarmAckModeEnum.AckActive)
            {
                Status = AlarmStatusEnum.ACK;
                Acktime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                Userack = userId ?? string.Empty;
                return true;
            }
        }
        else if (Status == AlarmStatusEnum.OFF)
        {
            Status = AlarmStatusEnum.VOID;
            Acktime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            Userack = userId ?? string.Empty;
            ToRemove = true;
            return true;
        }
        return false;
    }

    public int GetToAck()
    {
        if (SubProperty?.Ackmode == AlarmAckModeEnum.Float)
            return -1;
        if (SubProperty?.Ackmode == AlarmAckModeEnum.AckPassive && Status == AlarmStatusEnum.OFF)
            return 1;
        if (SubProperty?.Ackmode == AlarmAckModeEnum.AckActive && (Status == AlarmStatusEnum.OFF || Status == AlarmStatusEnum.ON))
            return 1;
        return 0;
    }

    public AlarmRecord ToAlarmRecord()
    {
        return new AlarmRecord
        {
            Nametype = GetId(),
            Type = Type,
            Status = Status,
            Ontime = Ontime,
            Offtime = Offtime,
            Acktime = Acktime,
        };
    }

    public AlarmChronicle ToChronicle()
    {
        return new AlarmChronicle
        {
            Nametype = GetId(),
            Type = Type,
            Status = Status,
            Text = SubProperty?.Text ?? string.Empty,
            Grp = SubProperty?.Group ?? string.Empty,
            Ontime = Ontime,
            Offtime = Offtime,
            Acktime = Acktime,
            Userack = Userack,
        };
    }

    public AlarmValueDto ToValueDto()
    {
        return new AlarmValueDto
        {
            Name = GetId(),
            Type = Type,
            Status = Status,
            Ontime = Ontime,
            Offtime = Offtime,
            Acktime = Acktime,
            Text = SubProperty?.Text ?? string.Empty,
            Group = SubProperty?.Group ?? string.Empty,
            Toack = GetToAck(),
            Bkcolor = SubProperty?.bkcolor ?? string.Empty,
            Color = SubProperty?.color ?? string.Empty,
        };
    }

    private bool EvaluateCondition(double value)
    {
        if (SubProperty == null) return false;

        return Type switch
        {
            AlarmTypeConst.HIGH_HIGH => SubProperty.Max.HasValue && value >= SubProperty.Max.Value,
            AlarmTypeConst.HIGH => SubProperty.Max.HasValue && value >= SubProperty.Max.Value,
            AlarmTypeConst.LOW => SubProperty.Min.HasValue && value <= SubProperty.Min.Value,
            AlarmTypeConst.INFO => SubProperty.Min.HasValue && value == SubProperty.Min.Value,
            AlarmTypeConst.ACTION => SubProperty.Min.HasValue && SubProperty.Max.HasValue
                && value >= SubProperty.Min.Value && value <= SubProperty.Max.Value,
            _ => false,
        };
    }

    /// <summary>
    /// Restore state from a persisted AlarmRecord
    /// </summary>
    public void RestoreFrom(AlarmRecord record)
    {
        Status = record.Status;
        Ontime = record.Ontime;
        Offtime = record.Offtime;
        Acktime = record.Acktime;
    }
}
