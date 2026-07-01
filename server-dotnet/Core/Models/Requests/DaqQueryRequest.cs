namespace Core.Models.Requests;

public class DaqQueryRequest
{
    public List<string> Sids { get; set; } = new();
    public long From { get; set; }
    public long To { get; set; }
}
