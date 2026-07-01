using Quartz;
using Runtime.Notificator;

namespace Runtime.Jobs;

[DisallowConcurrentExecution]
public class NotifyCheckJob : IJob
{
    private readonly INotificatorService _notificatorService;

    public NotifyCheckJob(INotificatorService notificatorService)
    {
        _notificatorService = notificatorService;
    }

    public async Task Execute(IJobExecutionContext context)
    {
        await _notificatorService.Tick();
    }
}
