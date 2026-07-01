namespace Core.Models.Requests;

public class SchedulerSaveRequest
{
    public string Id { get; set; } = string.Empty;
    public SchedulerData? Data { get; set; }
}
