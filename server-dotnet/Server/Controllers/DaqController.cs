using Core.Models.Requests;
using Core.Utils;
using Microsoft.AspNetCore.Mvc;
using Runtime.Project;
using Runtime.Storage;
using System.Text.Json;

namespace Server.Controllers;

[ApiController]
public class DaqController : ControllerBase
{
    private readonly ILogger<DaqController> _logger;
    private readonly DaqStorageService _daqService;
    private readonly IProjectService _projectService;

    public DaqController(ILogger<DaqController> logger, DaqStorageService daqService, IProjectService projectService)
    {
        _logger = logger;
        _daqService = daqService;
        _projectService = projectService;
    }

    [HttpGet]
    [Route("/api/daq")]
    public async Task<IActionResult> GetDaq()
    {
        var queryStr = Request.Query["query"].FirstOrDefault();
        if (string.IsNullOrEmpty(queryStr))
        {
            return BadRequest(new { error = "invalid_request", message = "Missing query parameter" });
        }

        var query = JsonSerializer.Deserialize<DaqQueryRequest>(queryStr, JsonHelper.Default);
        if (query == null || query.Sids == null || query.Sids.Count == 0)
        {
            return BadRequest(new { error = "invalid_request", message = "Invalid query format" });
        }

        // If from == to, return current values
        if (query.From == query.To)
        {
            var projectData = _projectService.GetProject();
            var currentValues = new Dictionary<string, object?>();
            foreach (var sid in query.Sids)
            {
                if (projectData.Tags.TryGetValue(sid, out var tag))
                {
                    currentValues[sid] = tag.Value;
                }
                else
                {
                    currentValues[sid] = null;
                }
            }
            return Ok(currentValues);
        }

        // Query historical data
        var result = await _daqService.GetNodesValues(query.Sids, query.From, query.To);
        return Ok(result);
    }
}
