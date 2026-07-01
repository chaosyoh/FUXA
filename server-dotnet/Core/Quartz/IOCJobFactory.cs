using Quartz;
using Quartz.Spi;
using System;
using System.Collections.Generic;
using System.Text;

namespace Core.Quartz
{
    /// <summary>
    /// 
    /// </summary>
    public class IOCJobFactory : IJobFactory
    {
        private readonly IServiceProvider _serviceProvider;
        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="serviceProvider"></param>
        public IOCJobFactory(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="bundle"></param>
        /// <param name="scheduler"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public IJob NewJob(TriggerFiredBundle bundle, IScheduler scheduler)
        {
            var o = _serviceProvider.GetService(bundle.JobDetail.JobType);
            if (_serviceProvider.GetService(bundle.JobDetail.JobType) is not IJob job)
            {
                throw new Exception("获取定时任务失败");
            }
            return job;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="job"></param>
        public void ReturnJob(IJob job)
        {
            if (job is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }
    }
}
