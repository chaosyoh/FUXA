namespace Core.Models.Requests;

public class RunScriptRequest
{
    public RunScriptParams? Params { get; set; }
}

public class RunScriptParams
{
    public object? Script { get; set; }
    public bool? ToLogEvent { get; set; }
}
