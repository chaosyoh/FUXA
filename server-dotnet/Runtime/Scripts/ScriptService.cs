using Microsoft.Extensions.Logging;

namespace Runtime.Scripts;

public class ScriptService : IScriptService
{
    private readonly ILogger<ScriptService> _logger;

    public ScriptService(ILogger<ScriptService> logger)
    {
        _logger = logger;
    }

    public Task<object?> RunScript(object script, bool toLogEvent)
    {
        _logger.LogWarning("Script execution is not yet implemented in .NET runtime");
        return Task.FromResult<object?>(null);
    }

    public Task<object?> RunSysFunction(string functionName, object? parameters)
    {
        _logger.LogWarning("System function '{FunctionName}' is not yet implemented in .NET runtime", functionName);
        return Task.FromResult<object?>(null);
    }

    public bool SysFunctionExist(string functionName)
    {
        return false;
    }

    public bool IsAuthorised(object script, int permission)
    {
        return true;
    }
}
