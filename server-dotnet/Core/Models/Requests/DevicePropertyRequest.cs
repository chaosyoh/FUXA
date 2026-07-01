namespace Core.Models.Requests;

public class DevicePropertyRequest
{
    public DevicePropertyParams? Params { get; set; }
}

public class DevicePropertyParams
{
    public string? Query { get; set; }
    public string? Name { get; set; }
    public object? Value { get; set; }
}
