using Core.Const;
using Core.Quartz;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Hosting;
using Runtime;
using Runtime.Alarms;
using Runtime.Jobs;
using Runtime.Notificator;
using Runtime.Project;

namespace Server;

public class HostService : IHostedService
{
    public ILogger<HostService> _logger;
    private readonly IProjectService _projectService;
    private readonly IJobManager _jobManager;
    private readonly IAlarmService _alarmService;
    private readonly INotificatorService _notificatorService;
    private readonly IHubContext<DataHub> _hubContext;

    public HostService(
        ILogger<HostService> logger,
        IProjectService projectService,
        IJobManager jobManager,
        IAlarmService alarmService,
        INotificatorService notificatorService,
        IHubContext<DataHub> hubContext)
    {
        _logger = logger;
        _projectService = projectService;
        _jobManager = jobManager;
        _alarmService = alarmService;
        _notificatorService = notificatorService;
        _hubContext = hubContext;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("FUXA Start");
        _logger.LogInformation("GetProjectData");
        await _projectService.Load();

        // Subscribe to alarm status changes for proactive broadcast
        _alarmService.OnAlarmsStatusChanged += BroadcastAlarmsStatus;

        // Start alarm service
        await _alarmService.Start();

        // Start notification service
        await _notificatorService.Start();

        // Register periodic jobs
        _jobManager.StartJob<SendTagValueJob>("InsertLogJob", "InsertLogJob", 1);
        _jobManager.StartJob<TagToSaveJob>("TagToSaveJob>", "TagToSaveJob", 5);
        _jobManager.StartJob<SaveDaqJob>("SaveDaqJob>", "SaveDaqJob", 5);
        _jobManager.StartJob<HeartbeatJob>("HeartbeatJob", "HeartbeatJob", 10);
        _jobManager.StartJob<AlarmCheckJob>("AlarmCheckJob", "AlarmCheckJob", 1);
        _jobManager.StartJob<NotifyCheckJob>("NotifyCheckJob", "NotifyCheckJob", 20);
        _jobManager.StartJob<RetentionCleanupJob>("RetentionCleanupJob", "RetentionCleanupJob", 3600);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _alarmService.OnAlarmsStatusChanged -= BroadcastAlarmsStatus;
        _logger.LogInformation("FUXA end!");
        return Task.CompletedTask;
    }

    private void BroadcastAlarmsStatus()
    {
        try
        {
            var status = _alarmService.GetAlarmsStatus();
            _ = _hubContext.Clients.All.SendCoreAsync(IoEventTypes.ALARMS_STATUS, [status]);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to broadcast alarm status");
        }
    }
}
