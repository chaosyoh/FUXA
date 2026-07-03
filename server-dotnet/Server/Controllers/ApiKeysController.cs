using Core.Entity;
using Core.Models.Requests;
using Microsoft.AspNetCore.Mvc;
using Runtime.ApiKeys;

namespace Server.Controllers;

[ApiController]
public class ApiKeysController : ControllerBase
{
    private readonly ILogger<ApiKeysController> _logger;
    private readonly ApiKeyStorage _apiKeyStorage;

    public ApiKeysController(ILogger<ApiKeysController> logger, ApiKeyStorage apiKeyStorage)
    {
        _logger = logger;
        _apiKeyStorage = apiKeyStorage;
    }

    [HttpGet]
    [Route("/api/apikeys")]
    public async Task<IActionResult> GetApiKeys()
    {
        try
        {
            var result = await _apiKeyStorage.GetApiKeys();
            return Ok(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "api get apikeys failed");
            return BadRequest(new { error = "unexpected_error", message = ex.Message });
        }
    }

    [HttpPost]
    [Route("/api/apikeys")]
    public async Task<IActionResult> SetApiKeys([FromBody] ApiKeySaveRequest? req)
    {
        if (req?.Params == null || req.Params.Count == 0)
        {
            return BadRequest(new { error = "invalid_request", message = "Missing apikeys params" });
        }

        try
        {
            await _apiKeyStorage.SetApiKeys(req.Params);
            return Ok();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "api post apikeys failed");
            return BadRequest(new { error = "unexpected_error", message = ex.Message });
        }
    }

    [HttpDelete]
    [Route("/api/apikeys")]
    public async Task<IActionResult> RemoveApiKeys()
    {
        var apikeysStr = Request.Query["apikeys"].FirstOrDefault();
        if (string.IsNullOrEmpty(apikeysStr))
        {
            return BadRequest(new { error = "invalid_request", message = "Missing apikeys" });
        }

        try
        {
            var apiKeys = System.Text.Json.JsonSerializer.Deserialize<List<ApiKey>>(apikeysStr, Core.Utils.JsonHelper.Default);
            if (apiKeys != null && apiKeys.Count > 0)
            {
                await _apiKeyStorage.RemoveApiKeys(apiKeys);
            }
            return Ok();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "api delete apikeys failed");
            return BadRequest(new { error = "unexpected_error", message = ex.Message });
        }
    }
}
