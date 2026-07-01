namespace Core.Models.Requests;

public class RunSysFunctionRequest
{
    public RunSysFunctionParams? Params { get; set; }
}

public class RunSysFunctionParams
{
    public string FunctionName { get; set; } = string.Empty;
    public object? Parameters { get; set; }
}
