using Core.Models;
using Microsoft.AspNetCore.SignalR;
using Quartz;
using Runtime;
using Runtime.Project;
using System.Threading.Channels;

namespace Server;

[DisallowConcurrentExecution]
public class SendTagValueJob : IJob
{
    private readonly IHubContext<DataHub> _hubCtx;

    private readonly ILogger<SendTagValueJob> _logger;

    private IProjectService _project;

    private TagSubscribeService _subscribeService;

    public SendTagValueJob(IHubContext<DataHub> hubCtx, ILogger<SendTagValueJob> logger,IProjectService project,TagSubscribeService subscribeService)
    {
        _hubCtx = hubCtx;
        _logger = logger;
        _project = project;
        _subscribeService = subscribeService;
    }

    public async Task Execute(IJobExecutionContext context)
    {
        var projectData = _project.GetProject();
        var subscriptions = _subscribeService.GetSubscriptions();
        foreach (var kvp in subscriptions)
        {
            var connectionId = kvp.Key;
            var tagsId = kvp.Value;
            var client = _hubCtx.Clients.Client(connectionId);
            var tags = new List<Tag>();
            foreach (var tag in tagsId)
            {
                if (projectData.Tags.TryGetValue(tag, out var t))
                {
                    tags.Add(t);
                }
            }
            await client.SendCoreAsync("device-values", [new
            {
                values = tags.Select(x=> new {
                    x.Id,
                    x.Value,
                    timestamp = x.Timestamp,
                }).ToList()
            }]);
        }
    }
}

