using Microsoft.Extensions.Logging;
using Quartz;
using Runtime.Project;
using Runtime.Storage;
using System.Threading.Channels;

namespace Runtime.Jobs;

[DisallowConcurrentExecution]
public class TagToSaveJob : IJob
{
    private ILogger<TagToSaveJob> _logger;

    private ChannelWriter<Meters> _writer;

    private IProjectService _project;

    public TagToSaveJob(ILogger<TagToSaveJob> logger, ChannelWriter<Meters> writer, IProjectService project)
    {
        _logger = logger;
        _writer = writer;
        _project = project;
    }

    public async Task Execute(IJobExecutionContext context)
    {
        var dic = _project.GetArchiveDic();
        var now = DateTime.Now;
        foreach (var kvp in dic)
        {
            foreach (var tag in kvp.Value)
            {
                await _writer.WriteAsync(new Meters
                {
                    Dt = now,
                    Tag_Id = tag.Id,
                    Tag_Value = tag.Value?.ToString(),
                });
            }
        }
    }
}

