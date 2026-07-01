using Core.Models.Requests;
using Microsoft.AspNetCore.Mvc;
using Runtime.Scripts;

namespace Server.Controllers;

[ApiController]
public class ScriptController : ControllerBase
{
    private readonly ILogger<ScriptController> _logger;
    private readonly IScriptService _scriptService;

    public ScriptController(ILogger<ScriptController> logger, IScriptService scriptService)
    {
        _logger = logger;
        _scriptService = scriptService;
    }

    [HttpPost]
    [Route("/api/runscript")]
    public async Task<IActionResult> RunScript([FromBody] RunScriptRequest? req)
    {
        if (req?.Params?.Script == null)
        {
            return BadRequest(new { error = "invalid_request", message = "Missing script" });
        }

        try
        {
            var result = await _scriptService.RunScript(req.Params.Script, req.Params.ToLogEvent ?? false);
            return Ok(result);
        }
        catch (NotSupportedException ex)
        {
            return StatusCode(501, new { error = "not_implemented", message = ex.Message });
        }
    }

    [HttpPost]
    [Route("/api/runSysFunction")]
    public async Task<IActionResult> RunSysFunction([FromBody] RunSysFunctionRequest? req)
    {
        if (req?.Params == null || string.IsNullOrEmpty(req.Params.FunctionName))
        {
            return BadRequest(new { error = "invalid_request", message = "Missing function name" });
        }

        if (!_scriptService.SysFunctionExist(req.Params.FunctionName))
        {
            return NotFound(new { error = "not_found", message = $"System function '{req.Params.FunctionName}' not found" });
        }

        try
        {
            var result = await _scriptService.RunSysFunction(req.Params.FunctionName, req.Params.Parameters);
            return Ok(result);
        }
        catch (NotSupportedException ex)
        {
            return StatusCode(501, new { error = "not_implemented", message = ex.Message });
        }
    }
}
