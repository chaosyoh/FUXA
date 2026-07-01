using Core.Models;
using Core.Settings;
using Microsoft.Extensions.Logging;
using Runtime.Storage;
using SqlSugar;

namespace Runtime.Alarms;

public class AlarmStorage : IAlarmStorage
{
    private readonly ILogger<AlarmStorage> _logger;
    private readonly ISqlSugarClient _db;
    private readonly string _alarmsTable;
    private readonly string _chronicleTable;

    public AlarmStorage(ILogger<AlarmStorage> logger, ISqlSugarProvider provider)
    {
        _logger = logger;
        _db = provider.GetClient("AlarmStorage");
        // SQLite: separate db files, no name conflict; MySQL: shared db, need distinct names
        _alarmsTable = provider.IsSqlite ? "alarms" : "alarms_runtime";
        _chronicleTable = provider.IsSqlite ? "chronicle" : "alarms_chronicle";
        try
        {
            // Use CodeFirst to create tables with dynamic names
            // For SQLite, the entity attributes map directly.
            // For MySQL, we need to create tables with different names.
            if (provider.IsSqlite)
            {
                _db.CodeFirst.InitTables<AlarmRecord>();
                _db.CodeFirst.InitTables<AlarmChronicle>();
            }
            else
            {
                // Create tables with MySQL-specific names using raw DDL
                _db.Ado.ExecuteCommand(@"CREATE TABLE IF NOT EXISTS alarms_runtime (
                    nametype VARCHAR(255) PRIMARY KEY,
                    type TEXT,
                    status TEXT,
                    ontime BIGINT,
                    offtime BIGINT,
                    acktime BIGINT)");
                _db.Ado.ExecuteCommand(@"CREATE TABLE IF NOT EXISTS alarms_chronicle (
                    Sn INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    nametype TEXT,
                    type TEXT,
                    status TEXT,
                    text TEXT,
                    grp TEXT,
                    ontime BIGINT,
                    offtime BIGINT,
                    acktime BIGINT,
                    userack TEXT)");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "alarm storage initialization failed!");
        }
    }

    public Task<List<AlarmRecord>> GetAlarms()
    {
        return _db.Queryable<AlarmRecord>().AS(_alarmsTable).ToListAsync();
    }

    public Task<List<AlarmChronicle>> GetAlarmsHistory(long from, long to)
    {
        return _db.Queryable<AlarmChronicle>().AS(_chronicleTable)
            .Where(x => x.Ontime >= from && x.Ontime <= to)
            .OrderByDescending(x => x.Ontime)
            .ToListAsync();
    }

    public async Task SetAlarms(List<AlarmRecord> toUpdate, List<AlarmChronicle> toChronicle, List<string> toRemove)
    {
        var isSqlite = _db.CurrentConnectionConfig.DbType == DbType.Sqlite;

        foreach (var alarm in toUpdate)
        {
            string sql;
            if (isSqlite)
            {
                sql = $"INSERT OR REPLACE INTO {_alarmsTable} (nametype, type, status, ontime, offtime, acktime) VALUES(@nametype, @type, @status, @ontime, @offtime, @acktime)";
            }
            else
            {
                sql = $"INSERT INTO {_alarmsTable} (nametype, type, status, ontime, offtime, acktime) VALUES(@nametype, @type, @status, @ontime, @offtime, @acktime) ON DUPLICATE KEY UPDATE type=VALUES(type), status=VALUES(status), ontime=VALUES(ontime), offtime=VALUES(offtime), acktime=VALUES(acktime)";
            }
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
            var sql = $"INSERT INTO {_chronicleTable} (nametype, type, status, text, grp, ontime, offtime, acktime, userack) VALUES(@nametype, @type, @status, @text, @grp, @ontime, @offtime, @acktime, @userack)";
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
            await _db.Deleteable<AlarmRecord>().AS(_alarmsTable)
                .Where(x => toRemove.Contains(x.Nametype)).ExecuteCommandAsync();
        }
    }

    public async Task ClearAlarms(bool all)
    {
        await _db.Ado.ExecuteCommandAsync($"DELETE FROM {_alarmsTable}");
        if (all)
        {
            await _db.Ado.ExecuteCommandAsync($"DELETE FROM {_chronicleTable}");
        }
    }

    public async Task ClearAlarmsHistory(long dtLimit)
    {
        await _db.Deleteable<AlarmChronicle>().AS(_chronicleTable)
            .Where(x => x.Ontime < dtLimit).ExecuteCommandAsync();
    }

    public void Close()
    {
        _db.Close();
    }
}
