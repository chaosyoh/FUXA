using Fuxa.DbMigrate.Models;

namespace Fuxa.DbMigrate.Services;

/// <summary>
/// Migration orchestrator: reads from SQLite and writes to target database
/// </summary>
public class Migrator : IDisposable
{
    private readonly AppConfig _config;
    private readonly MigratorOptions _options;
    private readonly SqliteReader _reader;
    private readonly TargetWriter _writer;
    private readonly MigrationStats _stats = new();
    private bool _disposed;

    public Migrator(AppConfig config, MigratorOptions options)
    {
        _config = config;
        _options = options;
        _reader = new SqliteReader(config.Migration.AppdataDir);
        _writer = new TargetWriter(config.Target);
    }

    /// <summary>
    /// Get a summary of all databases and their row counts (for preview)
    /// </summary>
    public List<DatabaseSummary> GetSummary()
    {
        var summary = new List<DatabaseSummary>();

        foreach (var dbDef in SchemaDefinitions.Databases)
        {
            if (!_reader.Exists(dbDef.DbFile))
                continue;

            var existingTables = _reader.GetTableNames(dbDef.DbFile);
            var tables = new List<TableSummary>();

            foreach (var tableDef in dbDef.Tables)
            {
                if (existingTables.Contains(tableDef.Name))
                {
                    var count = _reader.Count(dbDef.DbFile, tableDef.Name);
                    tables.Add(new TableSummary { Name = tableDef.Name, Rows = count });
                }
            }

            summary.Add(new DatabaseSummary
            {
                Name = dbDef.Name,
                DbFile = dbDef.DbFile,
                Tables = tables,
            });
        }

        // DAQ databases
        if (_options.IncludeDaq)
        {
            foreach (var daqDef in SchemaDefinitions.DaqDatabases)
            {
                var daqFiles = _reader.FindDaqFiles(daqDef.DbPrefix);
                if (daqFiles.Count == 0) continue;

                var tables = new List<TableSummary>();
                foreach (var file in daqFiles)
                {
                    foreach (var tableDef in daqDef.Tables)
                    {
                        var count = _reader.Count(file, tableDef.Name);
                        tables.Add(new TableSummary { Name = $"{file}:{tableDef.Name}", Rows = count });
                    }
                }

                summary.Add(new DatabaseSummary
                {
                    Name = daqDef.Name,
                    DbFile = $"{daqDef.DbPrefix}*.db ({daqFiles.Count} files)",
                    Tables = tables,
                });
            }
        }

        return summary;
    }

    /// <summary>
    /// Run the migration
    /// </summary>
    public async Task<MigrationStats> RunAsync()
    {
        Log("Connecting to target database...");

        if (!_options.DryRun)
        {
            _writer.Connect();
            Log("Connected to target database.");

            await _writer.TestConnectionAsync();
            Log("Target database connection verified.");
        }
        else
        {
            Log("[DRY RUN] No data will be written.");
        }

        // Migrate standard databases
        foreach (var dbDef in SchemaDefinitions.Databases)
        {
            if (_options.Databases.Count > 0 && !_options.Databases.Contains(dbDef.Name))
                continue;

            if (!_reader.Exists(dbDef.DbFile))
            {
                Log($"  Skipping '{dbDef.Name}' - file not found: {dbDef.DbFile}");
                _stats.Skipped++;
                continue;
            }

            await MigrateDatabaseAsync(dbDef);
        }

        // Migrate DAQ databases if requested
        if (_options.IncludeDaq)
        {
            await MigrateDaqDatabasesAsync();
        }

        return _stats;
    }

    /// <summary>
    /// Migrate a single database
    /// </summary>
    private async Task MigrateDatabaseAsync(DatabaseDef dbDef)
    {
        Log($"\nMigrating database: {dbDef.Name} ({dbDef.DbFile})");
        _stats.Databases++;

        var existingTables = _reader.GetTableNames(dbDef.DbFile);

        foreach (var tableDef in dbDef.Tables)
        {
            if (!existingTables.Contains(tableDef.Name))
            {
                Log($"  Table '{tableDef.Name}' not found, skipping.");
                _stats.Skipped++;
                continue;
            }

            var rowCount = _reader.Count(dbDef.DbFile, tableDef.Name);
            var targetTable = tableDef.TargetTable ?? tableDef.Name;

            if (_options.DryRun)
            {
                _stats.Tables++;
                _stats.Rows += rowCount;
                _stats.MigratedTables.Add(new MigratedTableInfo
                {
                    Database = dbDef.Name,
                    Table = $"{tableDef.Name} -> {targetTable}",
                    Rows = rowCount
                });
                continue;
            }

            try
            {
                // Create target table (drop first if truncate enabled)
                await _writer.CreateTableAsync(targetTable, tableDef, null, _options.Truncate);

                long inserted = 0;
                if (rowCount > 0)
                {
                    var rows = _reader.ReadAll(dbDef.DbFile, tableDef.Name);
                    inserted = await _writer.InsertRowsAsync(targetTable, rows, tableDef, null, _options.BatchSize);
                    _stats.Rows += inserted;
                }

                var mapping = tableDef.TargetTable != null ? $" ({tableDef.Name} -> {targetTable})" : "";
                Log($"    {dbDef.Name}.{tableDef.Name}{mapping} -> {inserted} rows");
                _stats.Tables++;
                _stats.MigratedTables.Add(new MigratedTableInfo
                {
                    Database = dbDef.Name,
                    Table = targetTable,
                    Rows = inserted
                });

                _options.OnProgress?.Invoke(dbDef.Name, targetTable, inserted, rowCount);
            }
            catch (Exception ex)
            {
                var errMsg = $"Error migrating {dbDef.Name}.{tableDef.Name}: {ex.Message}";
                Log($"    ERROR: {errMsg}");
                _stats.Errors.Add(errMsg);
            }
        }
    }

    /// <summary>
    /// Migrate DAQ (time-series) databases
    /// </summary>
    private async Task MigrateDaqDatabasesAsync()
    {
        Log("\nMigrating DAQ databases...");

        foreach (var daqDef in SchemaDefinitions.DaqDatabases)
        {
            var daqFiles = _reader.FindDaqFiles(daqDef.DbPrefix);

            if (daqFiles.Count == 0)
            {
                Log($"  No DAQ files found for prefix '{daqDef.DbPrefix}'.");
                continue;
            }

            Log($"  Found {daqFiles.Count} files for '{daqDef.Name}'.");

            foreach (var daqFile in daqFiles)
            {
                var baseName = Path.GetFileNameWithoutExtension(daqFile);
                var tablePrefix = System.Text.RegularExpressions.Regex.Replace(baseName, @"[^a-zA-Z0-9_]", "_");

                Log($"  Processing: {daqFile}");

                foreach (var tableDef in daqDef.Tables)
                {
                    var rowCount = _reader.Count(daqFile, tableDef.Name);

                    if (_options.DryRun)
                    {
                        _stats.Tables++;
                        _stats.Rows += rowCount;
                        _stats.MigratedTables.Add(new MigratedTableInfo
                        {
                            Database = daqDef.Name,
                            Table = $"{tablePrefix}_{tableDef.Name}",
                            Rows = rowCount
                        });
                        continue;
                    }

                    try
                    {
                        await _writer.CreateTableAsync(tableDef.Name, tableDef, tablePrefix, _options.Truncate);

                        long totalInserted = 0;
                        if (rowCount > 0)
                        {
                            _reader.ReadAllBatched(daqFile, tableDef.Name, _options.BatchSize, async (batch, offset, total) =>
                            {
                                var inserted = await _writer.InsertRowsAsync(tableDef.Name, batch, tableDef, tablePrefix, _options.BatchSize);
                                totalInserted += inserted;
                                Log($"    Batch: {offset + batch.Count}/{total} rows");
                            });
                            _stats.Rows += totalInserted;
                        }

                        Log($"    {daqDef.Name}.{tablePrefix}_{tableDef.Name} -> {totalInserted} rows");
                        _stats.Tables++;
                        _stats.MigratedTables.Add(new MigratedTableInfo
                        {
                            Database = daqDef.Name,
                            Table = $"{tablePrefix}_{tableDef.Name}",
                            Rows = totalInserted
                        });
                    }
                    catch (Exception ex)
                    {
                        var errMsg = $"Error migrating DAQ {daqFile}.{tableDef.Name}: {ex.Message}";
                        Log($"    ERROR: {errMsg}");
                        _stats.Errors.Add(errMsg);
                    }
                }
            }
        }
    }

    private static void Log(string msg) => Console.WriteLine(msg);

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _reader.Dispose();
        _writer.Dispose();
        GC.SuppressFinalize(this);
    }
}

/// <summary>
/// Runtime options for a migration run
/// </summary>
public class MigratorOptions
{
    public bool DryRun { get; set; }
    public bool IncludeDaq { get; set; }
    public int BatchSize { get; set; } = 1000;
    public bool Truncate { get; set; } = true;
    public List<string> Databases { get; set; } = [];
    public Action<string, string, long, long>? OnProgress { get; set; }
}
