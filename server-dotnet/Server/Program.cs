using Core.Models;
using Core.Quartz;
using Core.Settings;
using Microsoft.AspNetCore.Cors.Infrastructure;
using Quartz;
using Quartz.Impl;
using Quartz.Spi;
using Runtime;
using Runtime.Alarms;
using Runtime.ApiKeys;
using Runtime.Jobs;
using Runtime.Notificator;
using Runtime.Plugins;
using Runtime.Project;
using Runtime.Resources;
using Runtime.Scheduler;
using Runtime.Scripts;
using Runtime.Storage;
using Runtime.Users;
using Server;
using Server.Services;
using SqlSugar;
using System.Reflection;
using System.Threading.Channels;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
var services = builder.Services;
var appSettings = AppSettings.Initialize(builder.Configuration);
services.AddSingleton(appSettings);

// SqlSugar provider & shared client
services.AddSingleton<Runtime.Storage.SqlSugarProvider>();
services.AddSingleton(sp => sp.GetRequiredService<Runtime.Storage.SqlSugarProvider>().GetClient());

services.AddSignalR(options =>
{
    // Server sends ping every 15s to keep connection alive
    options.KeepAliveInterval = TimeSpan.FromSeconds(15);
    // Server waits 60s for client response before considering disconnected
    options.ClientTimeoutInterval = TimeSpan.FromSeconds(60);
});
services.AddControllers();
services.AddCors(option =>
{
    option.AddPolicy("any", policy =>
    {
        policy.SetIsOriginAllowed(origin => true)
          .AllowAnyHeader()
          .AllowAnyMethod()
          .AllowCredentials();
    });
});
// 创建一个无界Channel，用于在采集器和处理器之间传递数据
var channel = Channel.CreateUnbounded<Tag>();
// 将Channel的Reader和Writer注册为单例，方便各处注入
builder.Services.AddSingleton(channel.Reader);
builder.Services.AddSingleton(channel.Writer);

var daqChannel = Channel.CreateUnbounded<Meters>();
builder.Services.AddSingleton(daqChannel.Reader);
builder.Services.AddSingleton(daqChannel.Writer);

// HTTP Client for webhook notifications
services.AddHttpClient();

// Storage classes (no interfaces, direct concrete types)
services.AddSingleton<ProjectStorage>();
services.AddSingleton<Currentstorage>();
services.AddSingleton<QuestDb>();
services.AddSingleton<AlarmStorage>();
services.AddSingleton<NotifyStorage>();
services.AddSingleton<SchedulerStorage>();
services.AddSingleton<ApiKeyStorage>();
services.AddSingleton<UserService>();

// Service classes
services.AddSingleton<ProjectService>();
services.AddSingleton<IProjectService>(sp => sp.GetRequiredService<ProjectService>());
services.AddSingleton<TagSubscribeService>();
services.AddSingleton<DaqStorageService>();
services.AddSingleton<AlarmService>();
services.AddSingleton<IAlarmService>(sp => sp.GetRequiredService<AlarmService>());
services.AddSingleton<NotificatorService>();
services.AddSingleton<INotificatorService>(sp => sp.GetRequiredService<NotificatorService>());

// Script service
services.AddSingleton<IScriptService, ScriptService>();

// Plugin service
services.AddSingleton<IPluginService, PluginService>();

// Resource service
services.AddSingleton<IResourceService, ResourceService>();

// Device XLS import/export service
services.AddSingleton<DeviceXlsService>();

// KepServer converter service
services.AddSingleton<KepserverConverterService>();

// Hosted services
services.AddHostedService<HostService>();
services.AddSingleton<DeviceManager>();
services.AddHostedService<DeviceManager>(sp => sp.GetRequiredService<DeviceManager>());
services.AddSingleton<IDeviceRegistry>(sp => sp.GetRequiredService<DeviceManager>());

// Quartz job scheduling
services.AddSingleton<IJobManager, JobManager>();
services.AddSingleton<ISchedulerFactory, StdSchedulerFactory>();
services.AddSingleton<IJobFactory, IOCJobFactory>();
services.AddSingleton<SendTagValueJob>();
services.AddSingleton<TagToSaveJob>();
services.AddSingleton<SaveDaqJob>();
services.AddSingleton<HeartbeatJob>();
services.AddSingleton<AlarmCheckJob>();
services.AddSingleton<NotifyCheckJob>();
services.AddSingleton<RetentionCleanupJob>();
builder.WebHost.UseUrls($"http://*:{appSettings.UiPort}");
var app = builder.Build();
// Configure the HTTP request pipeline.
app.UseRouting();
app.UseAuthorization();
app.MapControllers();
app.MapHub<DataHub>("/DataHub", (options) =>
{
});
app.UseCors("any");

app.Run();
