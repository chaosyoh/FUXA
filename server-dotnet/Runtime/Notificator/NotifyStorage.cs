using Core.Models;
using Core.Settings;
using Microsoft.Extensions.Logging;
using Runtime.Storage;
using SqlSugar;

namespace Runtime.Notificator;

public class NotifyStorage : INotifyStorage
{
    private readonly ILogger<NotifyStorage> _logger;
    private readonly ISqlSugarClient _db;
    private readonly string _tableName;

    public NotifyStorage(ILogger<NotifyStorage> logger, ISqlSugarProvider provider)
    {
        _logger = logger;
        _db = provider.GetClient("NotifyStorage");
        // SQLite: "chronicle" (separate db, no conflict); MySQL: "notifications_chronicle" (shared db)
        _tableName = provider.IsSqlite ? "chronicle" : "notifications_chronicle";
        try
        {
            _db.CodeFirst.InitTables<NotificationChronicle>();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "notification storage initialization failed!");
        }
    }

    public Task<List<NotificationChronicle>> GetNotificationsHistory(long from, long to)
    {
        return _db.Queryable<NotificationChronicle>().AS(_tableName)
            .Where(x => x.Notifytime >= from && x.Notifytime <= to)
            .OrderByDescending(x => x.Notifytime)
            .ToListAsync();
    }

    public async Task SetNotification(NotificationChronicle notification)
    {
        await _db.Insertable(notification).AS(_tableName).ExecuteCommandAsync();
    }

    public async Task ClearNotifications(bool all)
    {
        await _db.Deleteable<NotificationChronicle>().AS(_tableName).ExecuteCommandAsync();
    }

    public void Close()
    {
        _db.Close();
    }
}
