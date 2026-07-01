using Core.Models;

namespace Runtime.Notificator;

public interface INotifyStorage
{
    Task<List<NotificationChronicle>> GetNotificationsHistory(long from, long to);
    Task SetNotification(NotificationChronicle notification);
    Task ClearNotifications(bool all);
    void Close();
}
