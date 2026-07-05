using Core.Models;
using Microsoft.Extensions.Logging;
using SqlSugar;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace Runtime.Project;

public class ProjectStorage
{
    private readonly ILogger<ProjectStorage> _logger;
    private readonly ISqlSugarClient _db;

    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private static readonly Type[] _tableEntityTypes =
    [
        typeof(GeneralRow), typeof(ViewRow), typeof(DeviceRow), typeof(DevicesSecurityRow),
        typeof(TextRow), typeof(ProjectAlarmRow), typeof(ProjectNotificationRow),
        typeof(ScriptRow), typeof(ReportRow), typeof(LocationRow),
    ];

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
        await UpsertRow(new GeneralRow { Name = "Version", Value = "\"1.0.0\"" });
        await UpsertRow(new DeviceRow
        {
            Name = "Server",
            Value = JsonSerializer.Serialize(new
            {
                Id = "0",
                Name = "FUXA Server",
                Type = "FuxaServer",
                Property = new { }
            }, _jsonOptions)
        });
    }

    #region Typed CRUD operations

    /// <summary>
    /// Read all rows from a typed table, optionally filtered by name.
    /// </summary>
    public Task<List<T>> GetRows<T>(string? name = null) where T : RowData, new()
    {
        return _db.Queryable<T>()
            .WhereIF(!string.IsNullOrEmpty(name), x => x.Name == name)
            .ToListAsync();
    }

    /// <summary>
    /// Upsert a single row using its PK (Name). Database-agnostic.
    /// </summary>
    public Task<int> UpsertRow<T>(T row) where T : RowData, new()
    {
        return _db.Storageable(new[] { row }).ExecuteCommandAsync();
    }

    /// <summary>
    /// Batch upsert rows using their PKs (Name). Database-agnostic.
    /// </summary>
    public Task<int> UpsertRows<T>(List<T> rows) where T : RowData, new()
    {
        if (rows.Count == 0) return Task.FromResult(0);
        return _db.Storageable(rows).ExecuteCommandAsync();
    }

    /// <summary>
    /// Delete a single row by name from a typed table.
    /// </summary>
    public Task<int> DeleteRow<T>(string name) where T : RowData, new()
    {
        return _db.Deleteable<T>()
            .Where(x => x.Name == name)
            .ExecuteCommandAsync();
    }

    #endregion

    public async Task ClearAll()
    {
        await _db.Deleteable<GeneralRow>().ExecuteCommandAsync();
        await _db.Deleteable<ViewRow>().ExecuteCommandAsync();
        await _db.Deleteable<DeviceRow>().ExecuteCommandAsync();
        await _db.Deleteable<DevicesSecurityRow>().ExecuteCommandAsync();
        await _db.Deleteable<TextRow>().ExecuteCommandAsync();
        await _db.Deleteable<ProjectAlarmRow>().ExecuteCommandAsync();
        await _db.Deleteable<ProjectNotificationRow>().ExecuteCommandAsync();
        await _db.Deleteable<ScriptRow>().ExecuteCommandAsync();
        await _db.Deleteable<ReportRow>().ExecuteCommandAsync();
        await _db.Deleteable<LocationRow>().ExecuteCommandAsync();
    }

    #region Serialization helpers

    /// <summary>
    /// Serialize any value (JsonElement, JsonNode, or object) to a JSON string for storage.
    /// </summary>
    public static string SerializeValue(object? value)
    {
        if (value is JsonElement je)
            return je.GetRawText();
        if (value is JsonNode jn)
            return jn.ToJsonString();
        if (value is string s)
            return s;
        return JsonSerializer.Serialize(value, _jsonOptions);
    }

    #endregion
}
