using Core.Models;

namespace Runtime.Alarms;

public interface IAlarmService
{
    Task Start();
    Task Stop();
    void Reset();
    Task Tick();
    AlarmStatus GetAlarmsStatus();
    List<AlarmValueDto> GetAlarmsValues(AlarmFilter? filter);
    Task<List<AlarmChronicle>> GetAlarmsHistory(long from, long to);
    Task SetAlarmAck(string? alarmName, string? userId);
    Task ClearAlarms(bool all);
    Task CheckRetention();

    event Action? OnAlarmsStatusChanged;
}
