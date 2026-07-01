using Microsoft.Extensions.Logging;
using Quartz;
using Runtime.Storage;
using System.Threading.Channels;

namespace Runtime.Jobs;

[DisallowConcurrentExecution]
public class SaveDaqJob : IJob
{
    private ILogger<SaveDaqJob> _logger;
    private ChannelReader<Meters> _reader;

    public SaveDaqJob(ILogger<SaveDaqJob> logger, ChannelReader<Meters> reader)
    {
        _logger = logger;
        _reader = reader;
    }

    public async Task Execute(IJobExecutionContext context)
    {
        var count = _reader.Count;
        var list = new List<Meters>();
        for (int i = 0; i < count; i++)
        {
            var data = await _reader.ReadAsync();
            if (data is not null)
            {
                list.Add(data);
            }
        }
        if (list.Count == 0) return;
        //_logger.LogInformation("插入归档数据{count}条", list.Count);
    }
}

