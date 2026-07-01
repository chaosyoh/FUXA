using Core.Const;
using Core.Models;
using Microsoft.Extensions.Logging;
using Runtime.Project;
using System.Collections.Concurrent;

namespace Runtime.Alarms;

public class AlarmService : IAlarmService
{
    private readonly ILogger<AlarmService> _logger;
    private readonly IAlarmStorage _storage;
    private readonly IProjectService _project;

    private readonly ConcurrentDictionary<string, List<RuntimeAlarm>> _alarms = new();
    private bool _working;
    private string _status = "INIT";

    public event Action? OnAlarmsStatusChanged;

    public AlarmService(ILogger<AlarmService> logger, IAlarmStorage storage, IProjectService project)
    {
        _logger = logger;
        _storage = storage;
        _project = project;
    }

    public Task Start()
    {
        _status = "LOAD";
        _logger.LogInformation("AlarmService starting");
        return Task.CompletedTask;
    }

    public Task Stop()
    {
        _status = "INIT";
        _alarms.Clear();
        _logger.LogInformation("AlarmService stopped");
        return Task.CompletedTask;
    }

    public void Reset()
    {
        _alarms.Clear();
        _status = "LOAD";
    }

    public async Task Tick()
    {
        if (_working) return;
        _working = true;
        try
        {
            if (_status == "LOAD")
            {
                await LoadAlarms();
                _status = "IDLE";
            }
            else if (_status == "IDLE")
            {
                await CheckAlarms();
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "AlarmService tick error");
        }
        finally
        {
            _working = false;
        }
    }

    private async Task LoadAlarms()
    {
        _alarms.Clear();
        var projectData = _project.GetProject();

        // Build alarm matrix from project alarm definitions
        foreach (var alarmDef in projectData.Alarms)
        {
            if (alarmDef.Property == null || string.IsNullOrEmpty(alarmDef.Property.VariableId))
                continue;

            var variableId = alarmDef.Property.VariableId;
            var alarmList = _alarms.GetOrAdd(variableId, _ => new List<RuntimeAlarm>());

            AddAlarmType(alarmList, alarmDef.Name, AlarmTypeConst.HIGH_HIGH, alarmDef.Highhigh, alarmDef.Property);
            AddAlarmType(alarmList, alarmDef.Name, AlarmTypeConst.HIGH, alarmDef.High, alarmDef.Property);
            AddAlarmType(alarmList, alarmDef.Name, AlarmTypeConst.LOW, alarmDef.Low, alarmDef.Property);
            AddAlarmType(alarmList, alarmDef.Name, AlarmTypeConst.INFO, alarmDef.Info, alarmDef.Property);
        }

        // Restore persisted alarm states
        try
        {
            var persisted = await _storage.GetAlarms();
            foreach (var record in persisted)
            {
                foreach (var alarmList in _alarms.Values)
                {
                    var alarm = alarmList.Find(a => a.GetId() == record.Nametype);
                    if (alarm != null)
                    {
                        alarm.RestoreFrom(record);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to restore alarm states");
        }

        _logger.LogInformation("Loaded {Count} alarm groups", _alarms.Count);
    }

    private void AddAlarmType(List<RuntimeAlarm> list, string name, string type, AlarmSubProperty? sub, AlarmProperty tagProp)
    {
        if (sub == null || sub.Enabled != true) return;
        list.Add(new RuntimeAlarm
        {
            Name = name,
            Type = type,
            SubProperty = sub,
            TagProperty = tagProp,
        });
    }

    private async Task CheckAlarms()
    {
        var projectData = _project.GetProject();
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var toUpdate = new List<AlarmRecord>();
        var toChronicle = new List<AlarmChronicle>();
        var toRemove = new List<string>();
        var changed = false;

        foreach (var kvp in _alarms)
        {
            var variableId = kvp.Key;
            if (!projectData.Tags.TryGetValue(variableId, out var tag)) continue;

            double? tagValue = null;
            if (tag.Value != null)
            {
                if (tag.Value is bool boolVal)
                    tagValue = boolVal ? 1.0 : 0.0;
                else if (double.TryParse(tag.Value.ToString(), out var dv))
                    tagValue = dv;
            }

            foreach (var alarm in kvp.Value)
            {
                var stateChanged = alarm.Check(now, tagValue);
                if (stateChanged)
                {
                    changed = true;
                    if (alarm.ToRemove)
                    {
                        toRemove.Add(alarm.GetId());
                        toChronicle.Add(alarm.ToChronicle());
                        alarm.ToRemove = false;
                        alarm.Status = AlarmStatusEnum.VOID;
                        alarm.Ontime = 0;
                        alarm.Offtime = 0;
                        alarm.Acktime = 0;
                        alarm.Userack = string.Empty;
                    }
                    else
                    {
                        toUpdate.Add(alarm.ToAlarmRecord());
                        toChronicle.Add(alarm.ToChronicle());
                    }
                }
            }
        }

        if (toUpdate.Count > 0 || toChronicle.Count > 0 || toRemove.Count > 0)
        {
            try
            {
                await _storage.SetAlarms(toUpdate, toChronicle, toRemove);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to persist alarm changes");
            }
        }

        if (changed)
        {
            OnAlarmsStatusChanged?.Invoke();
        }
    }

    public AlarmStatus GetAlarmsStatus()
    {
        var status = new AlarmStatus();
        foreach (var alarmList in _alarms.Values)
        {
            foreach (var alarm in alarmList)
            {
                if (alarm.Status == AlarmStatusEnum.VOID) continue;
                switch (alarm.Type)
                {
                    case AlarmTypeConst.HIGH_HIGH: status.Highhigh++; break;
                    case AlarmTypeConst.HIGH: status.High++; break;
                    case AlarmTypeConst.LOW: status.Low++; break;
                    case AlarmTypeConst.INFO: status.Info++; break;
                }
            }
        }
        return status;
    }

    public List<AlarmValueDto> GetAlarmsValues(AlarmFilter? filter)
    {
        var result = new List<AlarmValueDto>();
        foreach (var alarmList in _alarms.Values)
        {
            foreach (var alarm in alarmList)
            {
                if (alarm.Status == AlarmStatusEnum.VOID) continue;
                if (alarm.Type == AlarmTypeConst.INFO) continue;

                var dto = alarm.ToValueDto();

                // Apply filter
                if (filter != null)
                {
                    if (filter.Priority != null && filter.Priority.Count > 0 && !filter.Priority.Contains(dto.Type))
                        continue;
                    if (!string.IsNullOrEmpty(filter.Text) && !dto.Text.Contains(filter.Text, StringComparison.OrdinalIgnoreCase))
                        continue;
                    if (!string.IsNullOrEmpty(filter.Group) && dto.Group != filter.Group)
                        continue;
                }

                result.Add(dto);
            }
        }
        return result;
    }

    public Task<List<AlarmChronicle>> GetAlarmsHistory(long from, long to)
    {
        return _storage.GetAlarmsHistory(from, to);
    }

    public async Task SetAlarmAck(string? alarmName, string? userId)
    {
        var toUpdate = new List<AlarmRecord>();
        var toChronicle = new List<AlarmChronicle>();
        var toRemove = new List<string>();

        foreach (var alarmList in _alarms.Values)
        {
            foreach (var alarm in alarmList)
            {
                if (!string.IsNullOrEmpty(alarmName) && alarm.Name != alarmName) continue;
                if (alarm.GetToAck() <= 0) continue;

                if (alarm.SetAck(userId))
                {
                    if (alarm.ToRemove)
                    {
                        toRemove.Add(alarm.GetId());
                        toChronicle.Add(alarm.ToChronicle());
                        alarm.ToRemove = false;
                        alarm.Status = AlarmStatusEnum.VOID;
                        alarm.Ontime = 0;
                        alarm.Offtime = 0;
                        alarm.Acktime = 0;
                        alarm.Userack = string.Empty;
                    }
                    else
                    {
                        toUpdate.Add(alarm.ToAlarmRecord());
                        toChronicle.Add(alarm.ToChronicle());
                    }
                }
            }
        }

        if (toUpdate.Count > 0 || toChronicle.Count > 0 || toRemove.Count > 0)
        {
            await _storage.SetAlarms(toUpdate, toChronicle, toRemove);
            OnAlarmsStatusChanged?.Invoke();
        }
    }

    public async Task ClearAlarms(bool all)
    {
        await _storage.ClearAlarms(all);
        foreach (var alarmList in _alarms.Values)
        {
            foreach (var alarm in alarmList)
            {
                alarm.Status = AlarmStatusEnum.VOID;
                alarm.Ontime = 0;
                alarm.Offtime = 0;
                alarm.Acktime = 0;
                alarm.Userack = string.Empty;
            }
        }
        OnAlarmsStatusChanged?.Invoke();
    }

    public async Task CheckRetention()
    {
        var settings = Core.Settings.AppSettings.GetSettings();
        if (settings.Alarms.RetentionType == "days" && settings.Alarms.RetentionDays > 0)
        {
            var dtLimit = DateTimeOffset.UtcNow.AddDays(-settings.Alarms.RetentionDays).ToUnixTimeMilliseconds();
            await _storage.ClearAlarmsHistory(dtLimit);
        }
    }
}
