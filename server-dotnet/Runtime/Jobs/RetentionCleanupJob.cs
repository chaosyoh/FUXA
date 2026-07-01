using Quartz;
using Runtime.Alarms;
using Runtime.Notificator;

namespace Runtime.Jobs;

[DisallowConcurrentExecution]
public class RetentionCleanupJob : IJob
{
    private readonly IAlarmService _alarmService;
    private readonly INotificatorService _notificatorService;

    public RetentionCleanupJob(IAlarmService alarmService, INotificatorService notificatorService)
    {
        _alarmService = alarmService;
        _notificatorService = notificatorService;
    }

    public async Task Execute(IJobExecutionContext context)
    {
        await _alarmService.CheckRetention();
    }
}
