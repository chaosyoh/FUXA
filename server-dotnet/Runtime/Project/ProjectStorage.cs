using Core.Const;
using Core.Models;
using Core.Utils;
using Microsoft.Extensions.Logging;
using SqlSugar;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Runtime.Project;

public class ProjectStorage
{
    private readonly ILogger<ProjectStorage> _logger;
    private readonly ISqlSugarClient _db;
    private static JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private static readonly Type[] _tableEntityTypes = new[]
    {
        typeof(GeneralRow), typeof(ViewRow), typeof(DeviceRow), typeof(DevicesSecurityRow),
        typeof(TextRow), typeof(ProjectAlarmRow), typeof(ProjectNotificationRow),
        typeof(ScriptRow), typeof(ReportRow), typeof(LocationRow),
    };

    public ProjectStorage(ILogger<ProjectStorage> logger, ISqlSugarClient db)
    {
        _logger = logger;
        _db = db;
    }

    public void InitTables()
    {
        try
        {
            _db.CodeFirst.InitTables(_tableEntityTypes);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "project storage table initialization failed!");
        }
    }

    public async Task SetDefault()
    {
        var sections = new List<SqlSection>();
        sections.Add(new SqlSection
        {
            Table = TableType.GENERAL,
            Name = "Version",
            Value = "1.0.0"
        });
        sections.Add(new SqlSection
        {
            Table = TableType.DEVICES,
            Name = "Server",
            Value = new
            {
                Id = "0",
                Name = "FUXA Server",
                Type = "FuxaServer",
                Property = new { }
            }
        });
        await SetSections(sections);
    }

    public async Task SetSections(List<SqlSection> sections)
    {
        foreach (var section in sections)
        {
            var value = SerializeValue(section.Value, _jsonOptions);
            var sql = $"INSERT INTO {section.Table} (name, value) VALUES(@name, @value) ON DUPLICATE KEY UPDATE value = VALUES(value)";
            await _db.Ado.ExecuteCommandAsync(sql, new { name = section.Name, value });
        }
    }

    public async Task SetSection(SqlSection section)
    {
        var value = SerializeValue(section.Value, _jsonOptions);
        var sql = $"INSERT INTO {section.Table} (name, value) VALUES(@name, @value) ON DUPLICATE KEY UPDATE value = VALUES(value)";
        await _db.Ado.ExecuteCommandAsync(sql, new { name = section.Name, value });
    }

    public Task<List<RowData>> GetSection(string table, string? name = null)
    {
        return _db.Queryable<RowData>().AS(table)
            .WhereIF(!string.IsNullOrEmpty(name), x => x.Name == name)
            .ToListAsync();
    }

    public Task DeleteSection(SqlSection section)
    {
        return _db.Deleteable<RowData>().AS(section.Table)
            .Where(x => x.Name == section.Name)
            .ExecuteCommandAsync();
    }

    public async Task ClearAll()
    {
        var tables = new[] {
            TableType.GENERAL, TableType.VIEWS, TableType.DEVICES,
            TableType.DEVICESSECURITY, TableType.TEXTS, TableType.ALARMS,
            TableType.NOTIFICATIONS, TableType.SCRIPTS, TableType.REPORTS,
            TableType.LOCATIONS
        };
        foreach (var tableName in tables)
        {
            await _db.Ado.ExecuteCommandAsync($"DELETE FROM {tableName}");
        }
    }

    private static string SerializeValue(object? value, JsonSerializerOptions options)
    {
        // [FromBody] object? is bound as System.Text.Json.JsonElement by ASP.NET Core.
        if (value is JsonElement je)
            return je.GetRawText();
        return JsonSerializer.Serialize(value, options);
    }
}
