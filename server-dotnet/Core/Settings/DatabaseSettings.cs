namespace Core.Settings;

public class DatabaseSettings
{
    /// <summary>
    /// Database engine type: "sqlite" (default) or "mysql"
    /// </summary>
    public string Type { get; set; } = "sqlite";

    /// <summary>
    /// Connection string for non-SQLite databases.
    /// MySQL example: "server=localhost;Database=fuxa;Uid=root;Pwd=secret;CharSet=utf8mb4;"
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;
}
