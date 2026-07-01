using SqlSugar;

namespace Runtime.Storage;

/// <summary>
/// Provides SqlSugar client instances for storage modules.
/// In SQLite mode, returns per-module SqlSugarScope (each connected to a separate .db file).
/// In MySQL mode, returns a shared SqlSugarScope (all modules share one database).
/// </summary>
public interface ISqlSugarProvider
{
    /// <summary>
    /// Get a SqlSugar client for the specified module.
    /// </summary>
    /// <param name="moduleName">Module name (e.g. "ProjectStorage", "AlarmStorage")</param>
    /// <returns>ISqlSugarClient instance</returns>
    ISqlSugarClient GetClient(string moduleName);

    /// <summary>
    /// Whether the current database engine is SQLite.
    /// Used by storage modules to apply engine-specific logic.
    /// </summary>
    bool IsSqlite { get; }
}
