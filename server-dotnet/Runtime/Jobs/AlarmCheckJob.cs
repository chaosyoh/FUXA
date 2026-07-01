using Quartz;
using Runtime.Alarms;

namespace Runtime.Jobs;

[DisallowConcurrentExecution]
public class AlarmCheckJob : IJob
{
    private readonly IAlarmService _alarmService;

    public AlarmCheckJob(IAlarmService alarmService)
    {
        _alarmService = alarmService;
    }

    public async Task Execute(IJobExecutionContext context)
    {
        await _alarmService.Tick();
    }
}
