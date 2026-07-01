using Core.Models;
using Core.Models.Requests;
using Core.Utils;
using Microsoft.AspNetCore.Mvc;
using Runtime.Alarms;
using System.Globalization;
using System.Text.Json;

namespace Server.Controllers;

[ApiController]
public class AlarmController : ControllerBase
{
    private readonly IAlarmService _alarmService;
    private readonly ILogger<AlarmController> _logger;

    public AlarmController(IAlarmService alarmService, ILogger<AlarmController> logger)
    {
        _alarmService = alarmService;
        _logger = logger;
    }

    [Route("/api/alarms")]
    [HttpGet, HttpPost]
    public IActionResult Alarms()
    {
        AlarmFilter? filter = null;
        var filterStr = Request.Query["filter"].FirstOrDefault();
        if (!string.IsNullOrEmpty(filterStr))
        {
            filter = JsonSerializer.Deserialize<AlarmFilter>(filterStr, JsonHelper.Default);
        }
        var result = _alarmService.GetAlarmsValues(filter);
        return Ok(result);
    }

    [Route("/api/alarmsHistory")]
    [HttpGet, HttpPost]
    public async Task<IActionResult> AlarmsHistory()
    {
        // Node.js uses "start"/"end", keep backward compat with "from"/"to"
        var fromStr = Request.Query["start"].FirstOrDefault() ?? Request.Query["from"].FirstOrDefault();
        var toStr = Request.Query["end"].FirstOrDefault() ?? Request.Query["to"].FirstOrDefault();
        var from = long.TryParse(fromStr, out var f) ? f : 0;
        var to = long.TryParse(toStr, out var t) ? t : DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var chronicles = await _alarmService.GetAlarmsHistory(from, to);
        // Map to match Node.js response format: parse "name^~^type" from nametype, map grp→group
        var result = chronicles.Select(c =>
        {
            var ids = c.Nametype.Split("^~^");
            return new
            {
                name = ids.Length > 0 ? ids[0] : c.Nametype,
                type = ids.Length > 1 ? ids[1] : c.Type,
                status = c.Status,
                text = c.Text,
                group = c.Grp,
                ontime = c.Ontime,
                offtime = c.Offtime,
                acktime = c.Acktime,
                userack = c.Userack,
            };
        });
        return Ok(result);
    }

    [Route("/api/alarmack")]
    [HttpPost]
    public async Task<IActionResult> AlarmAck([FromBody] AlarmAckRequest? req)
    {
        await _alarmService.SetAlarmAck(req?.Params?.Name, null);
        return Ok();
    }

    [Route("/api/alarmsClear")]
    [HttpPost]
    public async Task<IActionResult> ClearAlarms()
    {
        await _alarmService.ClearAlarms(false);
        _alarmService.Reset();
        return Ok();
    }

    [HttpGet]
    [Route("/api/getAlarms")]
    public IActionResult GetAlarms()
    {
        AlarmFilter? filter = null;
        var filterStr = Request.Query["filter"].FirstOrDefault();
        if (!string.IsNullOrEmpty(filterStr))
        {
            filter = JsonSerializer.Deserialize<AlarmFilter>(filterStr, JsonHelper.Default);
        }

        var values = _alarmService.GetAlarmsValues(filter);
        var result = values.Select(x => new
        {
            time = DateTimeOffset.FromUnixTimeMilliseconds(x.Ontime)
                .ToOffset(TimeSpan.FromHours(8))
                .ToString("yyyy/M/d HH:mm:ss", CultureInfo.InvariantCulture),
            equipment = x.Group,
            description = x.Text,
            level = x.Type switch
            {
                "highhigh" => "\u7d27\u6025",
                "high" => "\u91cd\u8981",
                "low" => "\u4e00\u822c",
                _ => "\u4fe1\u606f"
            }
        });
        return Ok(result);
    }
}
