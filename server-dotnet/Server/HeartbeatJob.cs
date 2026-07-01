using Microsoft.AspNetCore.SignalR;
using Quartz;
using Runtime;

namespace Server;

public class HeartbeatJob : IJob
{
    private IHubContext<DataHub> _hubCtx;

    public HeartbeatJob(IHubContext<DataHub> hubCtx)
    {
        _hubCtx = hubCtx;
    }

    public async Task Execute(IJobExecutionContext context)
    {
        await _hubCtx.Clients.All.SendCoreAsync("heartbeat", ["FUXA server is alive!"]);
    }
}

