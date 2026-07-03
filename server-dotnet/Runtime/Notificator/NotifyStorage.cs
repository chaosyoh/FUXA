using Core.Models;
using Microsoft.Extensions.Logging;
using SqlSugar;

namespace Runtime.Notificator;

public class NotifyStorage
{
    private readonly ILogger<NotifyStorage> _logger;
    private readonly ISqlSugarClient _db;

    public NotifyStorage(ILogger<NotifyStorage> logger, ISqlSugarClient db)
    {
        _logger = logger;
        _db = db;
    }

    public Task<List<NotificationChronicle>> GetNotificationsHistory(long from, long to)
    {
        return _db.Queryable<NotificationChronicle>()
            .Where(x => x.Notifytime >= from && x.Notifytime <= to)
            .OrderByDescending(x => x.Notifytime)
            .ToListAsync();
    }

    public async Task SetNotification(NotificationChronicle notification)
    {
        await _db.Insertable(notification).ExecuteCommandAsync();
    }

    public async Task ClearNotifications(bool all)
    {
        await _db.Deleteable<NotificationChronicle>().ExecuteCommandAsync();
    }

    public void Close()
    {
        _db.Close();
    }
}
