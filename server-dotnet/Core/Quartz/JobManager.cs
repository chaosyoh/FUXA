using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Quartz;
using Quartz.Spi;
using System;
using System.Collections.Generic;
using System.Reflection.Metadata;
using System.Text;

namespace Core.Quartz;

public class JobManager : IJobManager
{
    private readonly ILogger<JobManager> _logger;
    private readonly IScheduler _scheduler;
    private readonly IServiceProvider _serviceProvider;
    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="logger"></param>
    /// <param name="serviceProvider"></param>
    /// <param name="jobFactory"></param>
    public JobManager(ILogger<JobManager> logger, IServiceProvider serviceProvider, IJobFactory jobFactory)
    {
        _logger = logger;
        _serviceProvider = serviceProvider;
        _scheduler = GetScheduler().Result;
        _scheduler.JobFactory = jobFactory;
        _scheduler.Start();
    }

    private Task<IScheduler> GetScheduler()
    {
        try
        {
            ISchedulerFactory factory = _serviceProvider.GetRequiredService<ISchedulerFactory>();
            return factory.GetScheduler();
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex);
            throw;
        }
    }

    /// <summary>
    /// 开始（定时）任务
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="name">任务名</param>
    /// <param name="group">任务所在组</param>
    /// <param name="descriptionForJob">任务藐视</param>
    /// <param name="descriptionForTrigger">监听描述</param>
    /// <param name="cronSchedule">
    /// 条件表达式，由七个子表达式组成，这些子表达式用空格隔开
    /// 秒 分 时 日 月 星期几 年（可选）。
    /// “*”字符用于选择该项的所有值，例如分为 * ，表示每一分钟
    /// “/”字符可用于指定值的增量，例如秒为 0/5 ，表示从第 0 秒开始，每 5 秒执行一次。
    /// “?”字符用于月、日、星期几，表示无指定值。
    /// “L”字符用于日、星期几，表示最后一天。
    /// “W”字符用于日，例如日为 15W ，表示最接近 15 号的 工作日 。
    /// “#”字符用于指定该月的“第 n 个”XXX 工作日。例如，“星期几”字段中的值“6#3”或“FRI#3”表示“该月的第三个星期五”。
    /// </param>
    public async void StartJob<T>(string name, string group, string cronSchedule, string? descriptionForJob = null, string? descriptionForTrigger = null) where T : IJob
    {
        try
        {
            //指定具体执行的任务Job
            IJobDetail job = JobBuilder.Create<T>()
            .WithIdentity(name, group)
            .WithDescription(descriptionForJob is null ? "" : descriptionForJob)
            .Build();


            //设置触发条件
            ITrigger trigger = TriggerBuilder.Create()
            .WithIdentity(name, group)
            .WithDescription(descriptionForTrigger is null ? "" : descriptionForTrigger)
            .WithCronSchedule(cronSchedule, x => x
            .WithMisfireHandlingInstructionFireAndProceed())
            .Build();

            if (!_scheduler.CheckExists(job.Key).Result)
            {
                await _scheduler.ScheduleJob(job, trigger);
            }
            else
            {
                await _scheduler.DeleteJob(job.Key);
                await _scheduler.ScheduleJob(job, trigger);
            }
            //if (!scheduler.CheckExists(Job.Key).Result)
            //    await scheduler.ScheduleJob(Job, Trigger, cts.Token);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "创建定时器时出现异常，异常为：{msg}", ex.Message);
            return;
        }
    }

    /// <summary>
    /// 开始（定时）任务
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="name">定时任务名称</param>
    /// <param name="group">定时任务组</param>
    /// <param name="sec">任务所在组</param>
    public async void StartJob<T>(string name, string group, int sec) where T : IJob
    {
        try
        {
            //指定具体执行的任务Job
            IJobDetail job = JobBuilder.Create<T>()
            .WithIdentity(name, group)
            .Build();


            //设置触发条件
            ITrigger trigger = TriggerBuilder.Create()
            .WithIdentity(name, group)
            .WithSimpleSchedule(x => x
                .WithIntervalInSeconds(sec)
                .RepeatForever())
            .Build();

            if (!_scheduler.CheckExists(job.Key).Result)
            {
                await _scheduler.ScheduleJob(job, trigger);
            }
            else
            {
                await _scheduler.DeleteJob(job.Key);
                await _scheduler.ScheduleJob(job, trigger);
            }
            //if (!scheduler.CheckExists(Job.Key).Result)
            //    await scheduler.ScheduleJob(Job, Trigger, cts.Token);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "创建定时器时出现异常，异常为：{msg}", ex.Message);
            return;
        }
    }

    /// <summary>
    /// 删除定时任务
    /// </summary>
    /// <param name="name"></param>
    /// <param name="group"></param>
    public async void DeleteJob(string name, string group)
    {
        try
        {
            JobKey jobKey = new(name, group);
            await _scheduler.DeleteJob(jobKey);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "删除定时器时出现异常，异常为：{msg}", ex.Message);
        }
    }

}

