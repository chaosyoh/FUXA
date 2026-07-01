using Microsoft.Extensions.Logging;
using Runtime.Project;
using System.Collections.Concurrent;

namespace Runtime;

public class TagSubscribeService
{
    private static readonly ConcurrentDictionary<string, List<string>> _tagSubscriptions = new();

    private IProjectService _project;

    private ILogger<TagSubscribeService> _logger;


    public TagSubscribeService(IProjectService project, ILogger<TagSubscribeService> logger)
    {
        _project = project;
        _logger = logger;
    }

    public void Subscribe(string connectionId, List<string> tagsId)
    {
        // 将当前连接加入到对应设备的组中
        //await Groups.AddToGroupAsync(Context.ConnectionId, deviceId);

        // 记录订阅关系（可选，用于后续管理）
        if (_tagSubscriptions.ContainsKey(connectionId))
        {
            _tagSubscriptions[connectionId] = tagsId;
            _logger.LogInformation("客户端重 {ConnectionId} 重新订阅变量", connectionId);
        }
        else
        {
            _tagSubscriptions.TryAdd(connectionId, tagsId);
            _logger.LogInformation("客户端 {ConnectionId} 订阅了变量", connectionId);
        }
    }

    /// <summary>
    /// 客户端取消订阅
    /// </summary>
    public void UnSubscribe(string connectionId, List<string> tagsId)
    {
        if (_tagSubscriptions.TryGetValue(connectionId, out var list))
        {
            _tagSubscriptions[connectionId] = tagsId;
            foreach (var id in tagsId)
            {
                if (list.Contains(id))
                {
                    list.Remove(id);
                }
            }
            _logger.LogInformation("客户端 {ConnectionId} 取消订阅变量", connectionId);
        }
    }

    /// <summary>
    /// 客户端取消订阅
    /// </summary>
    public void UnSubscribe(string connectionId)
    {
        if (_tagSubscriptions.ContainsKey(connectionId))
        {
            _tagSubscriptions.Remove(connectionId, out var _);
            _logger.LogInformation("客户端 {ConnectionId} 断开连接，清理订阅关系", connectionId);
        }
    }

    public ICollection<KeyValuePair<string, List<string>>> GetSubscriptions()
    {
        return _tagSubscriptions;
    }
}

