using Core.Const;
using Core.Extensions;
using Core.Settings;
using Microsoft.Extensions.Logging;
using SqlSugar;
using System.Collections.Concurrent;

namespace Runtime.Storage;

public class DaqStorageService : IDaqStorageService
{
    private readonly ILogger<DaqStorageService> _logger;
    private readonly ICurrentstorage _currentstorage;
    private readonly IStorage _storage;
    public DaqStorageService(ILogger<DaqStorageService> logger, ICurrentstorage currentstorage, IStorage storage)
    {
        _logger = logger;
        _currentstorage = currentstorage;
        _storage = storage;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="tagids"></param>
    /// <param name="fromts"></param>
    /// <param name="tots"></param>
    /// <returns></returns>
    public async Task<Dictionary<string, List<DaqValue>>> GetNodesValues(List<string> tagids, long fromts, long tots)
    {
        var dbfncs = new ConcurrentDictionary<string, List<DaqValue>>();
        foreach (var tagid in tagids)
        {
            var start = fromts.UnixTimeStampToDateTime();
            var end = tots.UnixTimeStampToDateTime();
            var daqValues = await _storage.GetDaqValue(tagid, start, end);
            dbfncs.TryAdd(tagid, daqValues);
        }
        ;
        foreach (var value in dbfncs.Values)
        {
            value.Insert(0, new DaqValue { Dt = DateTime.Now, Value = "" });
            value.Add(new DaqValue { Dt = DateTime.Now, Value = "" });
        }
        return dbfncs.ToDictionary(x => x.Key, x => x.Value);
    }

    public Task<List<DaqValue>> GetNodeValues(string tagid, long fromts, long tots)
    {
        var start = fromts.UnixTimeStampToDateTime();
        var end = tots.UnixTimeStampToDateTime();
        return _storage.GetDaqValue(tagid, start, end);
    }

    public async Task CheckRetention()
    {
        try
        {
            var settings = AppSettings.GetSettings();
            var retention = settings.DaqStore.Retention;
            if (string.IsNullOrEmpty(retention) || retention == DaqStoreRetentionType.None)
            {
                return;
            }

            var days = GetRetentionDays(retention);
            if (days <= 0) return;

            var cutoff = DateTime.Now.AddDays(-days);
            var deleted = await _storage.DeleteBefore(cutoff);
            if (deleted > 0)
            {
                _logger.LogInformation("DAQ retention check: deleted {Count} records older than {Cutoff:yyyy-MM-dd}", deleted, cutoff);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DAQ retention check failed");
        }
    }

    private static int GetRetentionDays(string retention)
    {
        return retention switch
        {
            DaqStoreRetentionType.Day1 => 1,
            DaqStoreRetentionType.Days2 => 2,
            DaqStoreRetentionType.Days3 => 3,
            DaqStoreRetentionType.Days7 => 7,
            DaqStoreRetentionType.Days14 => 14,
            DaqStoreRetentionType.Days30 => 30,
            DaqStoreRetentionType.Days90 => 90,
            DaqStoreRetentionType.Year1 => 365,
            DaqStoreRetentionType.Year3 => 365 * 3,
            DaqStoreRetentionType.Tear5 => 365 * 5,
            _ => 365,
        };
    }


    private DbType GetDbType(string type)
    {
        type = type.ToLower();
        switch (type)
        {
            case "mysql":
                return DbType.MySql;
            case "sqlserver":
                return DbType.SqlServer;
            case "sqlite":
                return DbType.Sqlite;
            case "questdb":
                return DbType.QuestDB;
            default:
                return DbType.Sqlite;
        }
    }


}
