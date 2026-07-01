using Core.Models;
using Core.Models.Requests;
using Microsoft.AspNetCore.Mvc;
using Runtime.Scheduler;

namespace Server.Controllers;

[ApiController]
public class SchedulerController : ControllerBase
{
    private readonly ILogger<SchedulerController> _logger;
    private readonly ISchedulerStorage _schedulerStorage;

    public SchedulerController(ILogger<SchedulerController> logger, ISchedulerStorage schedulerStorage)
    {
        _logger = logger;
        _schedulerStorage = schedulerStorage;
    }

    [HttpGet]
    [Route("/api/scheduler")]
    public async Task<IActionResult> GetScheduler()
    {
        var id = Request.Query["id"].FirstOrDefault();
        if (string.IsNullOrEmpty(id))
        {
            return BadRequest(new { error = "Missing scheduler id parameter" });
        }

        var data = await _schedulerStorage.GetSchedulerData(id);
        if (data == null)
        {
            return Ok(new { schedules = new Dictionary<string, object>() });
        }
        return Ok(data);
    }

    [HttpPost]
    [Route("/api/scheduler")]
    public async Task<IActionResult> SaveScheduler([FromBody] SchedulerSaveRequest? req)
    {
        if (req == null || string.IsNullOrEmpty(req.Id) || req.Data == null)
        {
            return BadRequest(new { error = "invalid_request", message = "Missing id or data" });
        }

        await _schedulerStorage.SetSchedulerData(req.Id, req.Data);
        return Ok(new { result = "ok" });
    }

    [HttpDelete]
    [Route("/api/scheduler")]
    public async Task<IActionResult> DeleteScheduler()
    {
        var id = Request.Query["id"].FirstOrDefault();
        if (string.IsNullOrEmpty(id))
        {
            return BadRequest(new { error = "invalid_request", message = "Missing id" });
        }

        await _schedulerStorage.DeleteSchedulerData(id);
        return Ok(new { result = "ok" });
    }
}
