using Core.Models;

namespace Runtime.Alarms;

public interface IAlarmStorage
{
    Task<List<AlarmRecord>> GetAlarms();
    Task<List<AlarmChronicle>> GetAlarmsHistory(long from, long to);
    Task SetAlarms(List<AlarmRecord> toUpdate, List<AlarmChronicle> toChronicle, List<string> toRemove);
    Task ClearAlarms(bool all);
    Task ClearAlarmsHistory(long dtLimit);
    void Close();
}
