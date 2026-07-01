using Core.Models;
using Core.Settings;

namespace Runtime.Notificator;

public interface INotificatorService
{
    Task Start();
    Task Stop();
    void Reset();
    Task Tick();
    Task SendMail(MailMsg msg, StmpSettings? smtpOverride);
    Task ClearNotifications(bool all);
}
