using Microsoft.Data.Sqlite;

namespace Fuxa.DbMigrate.Services;

/// <summary>
/// Reads data from FUXA SQLite databases using Microsoft.Data.Sqlite
/// </summary>
public class SqliteReader : IDisposable
{
    private readonly string _appdataDir;
    private readonly Dictionary<string, SqliteConnection> _connections = [];
    private bool _disposed;

    public SqliteReader(string appdataDir)
    {
        _appdataDir = appdataDir;
    }

    /// <summary>
    /// Open (or reuse) a connection to a SQLite database file
    /// </summary>
    private SqliteConnection? Open(string dbFile)
    {
        var dbPath = Path.GetFullPath(Path.Combine(_appdataDir, dbFile));
        if (!File.Exists(dbPath))
            return null;

        if (_connections.TryGetValue(dbPath, out var existing))
            return existing;

        var conn = new SqliteConnection($"Data Source={dbPath};Mode=ReadOnly");
        conn.Open();
        _connections[dbPath] = conn;
        return conn;
    }

    /// <summary>
    /// Check if database file exists
    /// </summary>
    public bool Exists(string dbFile)
    {
        var dbPath = Path.GetFullPath(Path.Combine(_appdataDir, dbFile));
        return File.Exists(dbPath);
    }

    /// <summary>
    /// Get table names in a database
    /// </summary>
    public List<string> GetTableNames(string dbFile)
    {
        var conn = Open(dbFile);
        if (conn == null) return [];

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name";
        var names = new List<string>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            names.Add(reader.GetString(0));
        }
        return names;
    }

    /// <summary>
    /// Count rows in a table
    /// </summary>
    public long Count(string dbFile, string tableName)
    {
        var conn = Open(dbFile);
        if (conn == null) return 0;

        try
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT COUNT(*) FROM [{tableName}]";
            var result = cmd.ExecuteScalar();
            return result != null ? Convert.ToInt64(result) : 0;
        }
        catch (SqliteException ex) when (ex.Message.Contains("no such table"))
        {
            return 0;
        }
    }

    /// <summary>
    /// Read all rows from a table as a list of dictionaries
    /// </summary>
    public List<Dictionary<string, object?>> ReadAll(string dbFile, string tableName)
    {
        var conn = Open(dbFile);
        if (conn == null) return [];

        try
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT * FROM [{tableName}]";
            var rows = new List<Dictionary<string, object?>>();
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                }
                rows.Add(row);
            }
            return rows;
        }
        catch (SqliteException ex) when (ex.Message.Contains("no such table"))
        {
            return [];
        }
    }

    /// <summary>
    /// Read rows in batches (for large DAQ datasets)
    /// </summary>
    public void ReadAllBatched(string dbFile, string tableName, int batchSize,
        Action<List<Dictionary<string, object?>>, int, long> callback)
    {
        var conn = Open(dbFile);
        if (conn == null) return;

        try
        {
            long total;
            using (var countCmd = conn.CreateCommand())
            {
                countCmd.CommandText = $"SELECT COUNT(*) FROM [{tableName}]";
                var result = countCmd.ExecuteScalar();
                total = result != null ? Convert.ToInt64(result) : 0;
            }

            int offset = 0;
            while (offset < total)
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT * FROM [{tableName}] LIMIT @limit OFFSET @offset";
                cmd.Parameters.AddWithValue("@limit", batchSize);
                cmd.Parameters.AddWithValue("@offset", offset);

                var batch = new List<Dictionary<string, object?>>();
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    var row = new Dictionary<string, object?>();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    }
                    batch.Add(row);
                }

                if (batch.Count == 0) break;
                callback(batch, offset, total);
                offset += batchSize;
            }
        }
        catch (SqliteException ex) when (!ex.Message.Contains("no such table"))
        {
            throw;
        }
    }

    /// <summary>
    /// Find DAQ database files by prefix
    /// </summary>
    public List<string> FindDaqFiles(string prefix)
    {
        var result = new List<string>();

        try
        {
            foreach (var file in Directory.GetFiles(_appdataDir, $"{prefix}*.db"))
            {
                result.Add(Path.GetFileName(file));
            }

            // Also check archive folder
            var archiveDir = Path.Combine(_appdataDir, "archive");
            if (Directory.Exists(archiveDir))
            {
                foreach (var file in Directory.GetFiles(archiveDir, $"{prefix}*.db"))
                {
                    result.Add(Path.Combine("archive", Path.GetFileName(file)));
                }
            }
        }
        catch
        {
            // ignore
        }

        return result;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        foreach (var conn in _connections.Values)
        {
            try { conn.Close(); conn.Dispose(); } catch { /* ignore */ }
        }
        _connections.Clear();

        GC.SuppressFinalize(this);
    }
}
