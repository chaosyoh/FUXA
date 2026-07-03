using Core.Models;
using Core.Settings;
using Microsoft.Extensions.Logging;
using Runtime.Alarms;
using Runtime.Project;
using System.Net;
using System.Net.Mail;

namespace Runtime.Notificator;

public class NotificatorService : INotificatorService
{
    private readonly ILogger<NotificatorService> _logger;
    private readonly NotifyStorage _storage;
    private readonly IProjectService _project;
    private readonly IAlarmService _alarmService;

    private string _status = "INIT";
    private bool _working;
    private AlarmStatus? _previousStatus;

    public NotificatorService(
        ILogger<NotificatorService> logger,
        NotifyStorage storage,
        IProjectService project,
        IAlarmService alarmService)
    {
        _logger = logger;
        _storage = storage;
        _project = project;
        _alarmService = alarmService;
    }

    public Task Start()
    {
        _status = "LOAD";
        _alarmService.OnAlarmsStatusChanged += OnAlarmChanged;
        _logger.LogInformation("NotificatorService started");
        return Task.CompletedTask;
    }

    public Task Stop()
    {
        _status = "INIT";
        _alarmService.OnAlarmsStatusChanged -= OnAlarmChanged;
        _logger.LogInformation("NotificatorService stopped");
        return Task.CompletedTask;
    }

    public void Reset()
    {
        _status = "LOAD";
        _previousStatus = null;
    }

    private void OnAlarmChanged()
    {
        // Force a check on next tick
    }

    public async Task Tick()
    {
        if (_working) return;
        _working = true;
        try
        {
            if (_status == "LOAD")
            {
                _previousStatus = _alarmService.GetAlarmsStatus();
                _status = "IDLE";
            }
            else if (_status == "IDLE")
            {
                await CheckNotifications();
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "NotificatorService tick error");
        }
        finally
        {
            _working = false;
        }
    }

    private async Task CheckNotifications()
    {
        var currentStatus = _alarmService.GetAlarmsStatus();
        if (_previousStatus == null)
        {
            _previousStatus = currentStatus;
            return;
        }

        var hasChange = currentStatus.Highhigh != _previousStatus.Highhigh
            || currentStatus.High != _previousStatus.High
            || currentStatus.Low != _previousStatus.Low
            || currentStatus.Info != _previousStatus.Info;

        if (!hasChange)
        {
            _previousStatus = currentStatus;
            return;
        }

        var projectData = _project.GetProject();
        foreach (var config in projectData.Notifications)
        {
            try
            {
                if (!config.Enabled) continue;

                var alarms = _alarmService.GetAlarmsValues(null);
                if (alarms.Count == 0) continue;
                var matchingAlarms = alarms.Where(a =>
                    config.Subscriptions.ContainsKey(a.Type) && config.Subscriptions[a.Type]
                ).ToList();

                if (matchingAlarms.Count == 0) continue;

                var text = string.Join("\n", matchingAlarms.Select(a => $"[{a.Type.ToUpper()}] {a.Text}"));
                var settings = AppSettings.GetSettings();

                if (config.Type == "email" && !string.IsNullOrEmpty(config.Receiver))
                {
                    var msg = new MailMsg
                    {
                        To = config.Receiver,
                        Subject = $"FUXA Alarm Notification - {matchingAlarms.Count} active alarm(s)",
                        Text = text,
                    };
                    await SendMail(msg, null);

                    await _storage.SetNotification(new NotificationChronicle
                    {
                        Id = config.Id,
                        Name = config.Name,
                        Type = config.Type,
                        Receiver = config.Receiver,
                        Text = text,
                        Notifytime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                        Notifytype = "email",
                    });
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process notification");
            }
        }

        _previousStatus = currentStatus;
    }

    public async Task SendMail(MailMsg msg, StmpSettings? smtpOverride)
    {
        var settings = smtpOverride ?? AppSettings.GetSettings().Stmp;
        if (string.IsNullOrEmpty(settings.Host))
        {
            _logger.LogWarning("SMTP host not configured");
            return;
        }

        try
        {
            using var client = new SmtpClient(settings.Host, settings.Port);
            if (!string.IsNullOrEmpty(settings.Username))
            {
                client.Credentials = new NetworkCredential(settings.Username, settings.Password);
            }
            client.EnableSsl = settings.Port == 465 || settings.Port == 587;

            var mailMessage = new System.Net.Mail.MailMessage();
            mailMessage.From = new MailAddress(msg.From ?? settings.Mailsender);
            mailMessage.To.Add(msg.To);
            mailMessage.Subject = msg.Subject;
            mailMessage.Body = !string.IsNullOrEmpty(msg.Html) ? msg.Html : msg.Text;
            mailMessage.IsBodyHtml = !string.IsNullOrEmpty(msg.Html);

            await client.SendMailAsync(mailMessage);
            _logger.LogInformation("Email sent to {To}", msg.To);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send email to {To}", msg.To);
            throw;
        }
    }

    public Task ClearNotifications(bool all)
    {
        return _storage.ClearNotifications(all);
    }
}
