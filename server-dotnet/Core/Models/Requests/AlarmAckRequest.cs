namespace Core.Models.Requests;

public class AlarmAckRequest
{
    public AlarmAckParams? Params { get; set; }
}

public class AlarmAckParams
{
    public string? Name { get; set; }
    public string? SubProperty { get; set; }
}
