namespace Core.Models;

public class SchedulerData
{
    public SchedulerSettings? Settings { get; set; }
    public Dictionary<string, List<ScheduleEntry>>? Schedules { get; set; }
}

public class SchedulerSettings
{
    public List<SchedulerDevice>? Devices { get; set; }
}

public class SchedulerDevice
{
    public string Name { get; set; } = string.Empty;
    public string VariableId { get; set; } = string.Empty;
    public List<DeviceAction>? DeviceActions { get; set; }
}

public class DeviceAction
{
    public string? OnSetValue { get; set; }
    public string? OnToggleValue { get; set; }
    public string? OnRunScript { get; set; }
}

public class ScheduleEntry
{
    public string? Id { get; set; }
    public string? Label { get; set; }
    public string? StartTime { get; set; }
    public string? EndTime { get; set; }
    public int? Duration { get; set; }
    public List<bool>? Days { get; set; }
    public List<bool>? Months { get; set; }
    public List<int>? DaysOfMonth { get; set; }
    public bool? MonthMode { get; set; }
    public bool? Recurring { get; set; }
    public bool? EventMode { get; set; }
    public bool? Disabled { get; set; }
}
