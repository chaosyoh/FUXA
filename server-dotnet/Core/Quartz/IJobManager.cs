using Quartz;

namespace Core.Quartz
{
    public interface IJobManager
    {
        void DeleteJob(string name, string group);
        void StartJob<T>(string name, string group, int sec) where T : IJob;
        void StartJob<T>(string name, string group, string cronSchedule, string? descriptionForJob = null, string? descriptionForTrigger = null) where T : IJob;
    }
}