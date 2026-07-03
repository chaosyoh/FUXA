using Core.Const;
using Core.Quartz;
using Microsoft.AspNetCore.SignalR;
using Runtime;
using Runtime.Alarms;
using Runtime.ApiKeys;
using Runtime.Jobs;
using Runtime.Notificator;
using Runtime.Project;
using Runtime.Scheduler;
using Runtime.Storage;
using Runtime.Users;

namespace Server;

public class HostService : IHostedService
{
    public ILogger<HostService> _logger;
    private readonly ProjectStorage _projectStorage;
    private readonly AlarmStorage _alarmStorage;
    private readonly NotifyStorage _notifyStorage;
    private readonly SchedulerStorage _schedulerStorage;
    private readonly ApiKeyStorage _apiKeyStorage;
    private readonly UserService _userService;
    private readonly Currentstorage _currentstorage;
    private readonly QuestDb _questDb;
    private readonly ProjectService _projectService;
    private readonly IJobManager _jobManager;
    private readonly AlarmService _alarmService;
    private readonly NotificatorService _notificatorService;
    private readonly IHubContext<DataHub> _hubContext;

    public HostService(
        ILogger<HostService> logger,
        ProjectStorage projectStorage,
        AlarmStorage alarmStorage,
        NotifyStorage notifyStorage,
        SchedulerStorage schedulerStorage,
        ApiKeyStorage apiKeyStorage,
        UserService userService,
        Currentstorage currentstorage,
        QuestDb questDb,
        ProjectService projectService,
        IJobManager jobManager,
        AlarmService alarmService,
        NotificatorService notificatorService,
        IHubContext<DataHub> hubContext)
    {
        _logger = logger;
        _projectStorage = projectStorage;
        _alarmStorage = alarmStorage;
        _notifyStorage = notifyStorage;
        _schedulerStorage = schedulerStorage;
        _apiKeyStorage = apiKeyStorage;
        _userService = userService;
        _currentstorage = currentstorage;
        _questDb = questDb;
        _projectService = projectService;
        _jobManager = jobManager;
        _alarmService = alarmService;
        _notificatorService = notificatorService;
        _hubContext = hubContext;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("FUXA Start");

        // Initialize all database tables
        _logger.LogInformation("Initializing database tables...");

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
