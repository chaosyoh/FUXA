using Core.Models;

namespace Runtime.Scheduler;

public interface ISchedulerStorage
{
    Task<SchedulerData?> GetSchedulerData(string schedulerId);
    Task SetSchedulerData(string schedulerId, SchedulerData data);
    Task<Dictionary<string, SchedulerData>> GetAllSchedulers();
    Task DeleteSchedulerData(string schedulerId);
    void Close();
}
