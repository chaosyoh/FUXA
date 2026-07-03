using Core.Models;
using Microsoft.Extensions.Logging;
using SqlSugar;

namespace Runtime.Alarms;

public class AlarmStorage
{
    private readonly ILogger<AlarmStorage> _logger;
    private readonly ISqlSugarClient _db;

    public AlarmStorage(ILogger<AlarmStorage> logger, ISqlSugarClient db)
    {
        _logger = logger;
        _db = db;
    }

    public void InitTables()
    {
        try
        {
            _db.CodeFirst.InitTables<AlarmRecord>();
            _db.CodeFirst.InitTables<AlarmChronicle>();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "alarm storage table initialization failed!");
        }
    }

    public Task<List<AlarmRecord>> GetAlarms()
    {
        return _db.Queryable<AlarmRecord>().ToListAsync();
    }

    public Task<List<AlarmChronicle>> GetAlarmsHistory(long from, long to)
    {
        return _db.Queryable<AlarmChronicle>()
            .Where(x => x.Ontime >= from && x.Ontime <= to)
            .OrderByDescending(x => x.Ontime)
            .ToListAsync();
    }

    public async Task SetAlarms(List<AlarmRecord> toUpdate, List<AlarmChronicle> toChronicle, List<string> toRemove)
    {
        foreach (var alarm in toUpdate)
        {
            var sql = "INSERT INTO alarms_runtime (nametype, type, status, ontime, offtime, acktime) " +
                      "VALUES(@nametype, @type, @status, @ontime, @offtime, @acktime) " +
                      "ON DUPLICATE KEY UPDATE type=VALUES(type), status=VALUES(status), " +
                      "ontime=VALUES(ontime), offtime=VALUES(offtime), acktime=VALUES(acktime)";
            await _db.Ado.ExecuteCommandAsync(sql, new
            {
                nametype = alarm.Nametype,
                type = alarm.Type,
                status = alarm.Status,
                ontime = alarm.Ontime,
                offtime = alarm.Offtime,
                acktime = alarm.Acktime
            });
        }

        foreach (var chronicle in toChronicle)
        {
            var sql = "INSERT INTO alarms_chronicle (nametype, type, status, text, grp, ontime, offtime, acktime, userack) " +
                      "VALUES(@nametype, @type, @status, @text, @grp, @ontime, @offtime, @acktime, @userack)";
            await _db.Ado.ExecuteCommandAsync(sql, new
            {
                nametype = chronicle.Nametype,
                type = chronicle.Type,
                status = chronicle.Status,
                text = chronicle.Text,
                grp = chronicle.Grp,
                ontime = chronicle.Ontime,
                offtime = chronicle.Offtime,
                acktime = chronicle.Acktime,
                userack = chronicle.Userack
            });
        }

        if (toRemove.Count > 0)
        {
            await _db.Deleteable<AlarmRecord>()
                .Where(x => toRemove.Contains(x.Nametype)).ExecuteCommandAsync();
        }
    }

    public async Task ClearAlarms(bool all)
    {
        await _db.Deleteable<AlarmRecord>().ExecuteCommandAsync();
        if (all)
        {
            await _db.Deleteable<AlarmChronicle>().ExecuteCommandAsync();
        }
    }

    public async Task ClearAlarmsHistory(long dtLimit)
    {
        await _db.Deleteable<AlarmChronicle>()
            .Where(x => x.Ontime < dtLimit).ExecuteCommandAsync();
    }

    public void Close()
    {
        _db.Close();
    }
}
