namespace Runtime.Scripts;

public interface IScriptService
{
    Task<object?> RunScript(object script, bool toLogEvent);
    Task<object?> RunSysFunction(string functionName, object? parameters);
    bool SysFunctionExist(string functionName);
    bool IsAuthorised(object script, int permission);
}
