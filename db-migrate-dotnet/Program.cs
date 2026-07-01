using Fuxa.DbMigrate.Models;
using Fuxa.DbMigrate.Services;
using Microsoft.Extensions.Configuration;

// =============================================
// FUXA Database Migration Tool (.NET Edition)
// =============================================

Console.ForegroundColor = ConsoleColor.Cyan;
Console.WriteLine("\n=== FUXA Database Migration Tool (.NET) ===\n");
Console.ResetColor();

// Resolve config file path from --config argument (optional)
var configFile = "appsettings.json";
for (int i = 0; i < args.Length - 1; i++)
{
    if (args[i] is "--config" or "-c")
    {
        configFile = args[i + 1];
        break;
    }
}

// Resolve config path: try current dir first, then exe dir as fallback
var configPath = Path.GetFullPath(configFile);
if (!File.Exists(configPath))
{
    var exeDir = AppContext.BaseDirectory;
    var fallbackPath = Path.GetFullPath(Path.Combine(exeDir, configFile));
    if (File.Exists(fallbackPath))
        configPath = fallbackPath;
}
if (!File.Exists(configPath))
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.Error.WriteLine($"Config file not found: {configPath}");
    Console.ResetColor();
    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.Error.WriteLine("Create an appsettings.json file. See appsettings.example.json for reference.");
    Console.ResetColor();
    WaitAndExit();
    return 1;
}

Console.WriteLine($"Config: {configPath}");

var configuration = new ConfigurationBuilder()
    .SetBasePath(Path.GetDirectoryName(configPath)!)
    .AddJsonFile(Path.GetFileName(configPath), optional: false)
    .Build();

var appConfig = new AppConfig();
configuration.Bind(appConfig);

// Validate
if (string.IsNullOrWhiteSpace(appConfig.Migration.AppdataDir))
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.Error.WriteLine("Error: Migration:AppdataDir is required in config.");
    Console.ResetColor();
    WaitAndExit();
    return 1;
}

if (!Directory.Exists(appConfig.Migration.AppdataDir))
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.Error.WriteLine($"Error: AppdataDir does not exist: {appConfig.Migration.AppdataDir}");
    Console.ResetColor();
    WaitAndExit();
    return 1;
}

if (string.IsNullOrWhiteSpace(appConfig.Target.DbType))
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.Error.WriteLine("Error: Target:DbType is required in config.");
    Console.ResetColor();
    WaitAndExit();
    return 1;
}

// Parse mode from config
var mode = appConfig.Migration.Mode.ToLowerInvariant();
var isDryRun = mode is "dry-run" or "dryrun";
var isRun = mode == "run";

// Build migrator options from config
var options = new MigratorOptions
{
    DryRun = isDryRun,
    IncludeDaq = appConfig.Migration.IncludeDaq,
    BatchSize = appConfig.Migration.BatchSize > 0 ? appConfig.Migration.BatchSize : 1000,
    Truncate = appConfig.Migration.Truncate,
    Databases = appConfig.Migration.Databases,
};

using var migrator = new Migrator(appConfig, options);

// Step 1: Show summary
Console.ForegroundColor = ConsoleColor.White;
Console.WriteLine("\nScanning FUXA databases...\n");
Console.ResetColor();

var summary = migrator.GetSummary();

if (summary.Count == 0)
{
    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.WriteLine($"No FUXA databases found in: {appConfig.Migration.AppdataDir}");
    Console.ResetColor();
    WaitAndExit();
    return 0;
}

long totalRows = 0;
foreach (var db in summary)
{
    Console.ForegroundColor = ConsoleColor.White;
    Console.Write($"  {db.Name}");
    Console.ForegroundColor = ConsoleColor.DarkGray;
    Console.WriteLine($" ({db.DbFile})");
    Console.ResetColor();

    foreach (var table in db.Tables)
    {
        Console.ForegroundColor = ConsoleColor.Green;
        Console.Write($"    {table.Name}: {table.Rows,8} rows");
        Console.ResetColor();
        Console.WriteLine();
        totalRows += table.Rows;
    }
}

Console.ForegroundColor = ConsoleColor.White;
Console.WriteLine($"\n  Total: {totalRows} rows to migrate");
Console.ForegroundColor = ConsoleColor.Magenta;
Console.WriteLine($"  Target: {appConfig.Target.DbType.ToUpper()} -> {appConfig.Target.Host}:{appConfig.Target.Port}/{appConfig.Target.Database}");
Console.ResetColor();

// Show current mode
Console.Write("  Mode:   ");
if (isRun)
{
    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine("RUN (execute migration)");
}
else if (isDryRun)
{
    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.WriteLine("DRY-RUN (preview only)");
}
else
{
    Console.ForegroundColor = ConsoleColor.Cyan;
    Console.WriteLine("PREVIEW (scan only)");
}
Console.ResetColor();

if (!isRun && !isDryRun)
{
    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.WriteLine("\nSet \"Mode\" to \"dry-run\" to preview, or \"run\" to execute migration.\n");
    Console.ResetColor();
    WaitAndExit();
    return 0;
}

if (isDryRun)
{
    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.WriteLine("\n[DRY RUN] No data was written. Set \"Mode\" to \"run\" to execute migration.\n");
    Console.ResetColor();
    WaitAndExit();
    return 0;
}

// Step 2: Execute migration
Console.WriteLine("\nStarting migration...");
var result = await migrator.RunAsync();

// Print results
Console.ForegroundColor = ConsoleColor.Cyan;
Console.WriteLine("\n=== Migration Complete ===\n");
Console.ResetColor();

Console.WriteLine($"  Databases processed: {result.Databases}");
Console.WriteLine($"  Tables migrated:     {result.Tables}");
Console.ForegroundColor = ConsoleColor.Green;
Console.WriteLine($"  Rows inserted:       {result.Rows}");
Console.ResetColor();
Console.ForegroundColor = ConsoleColor.Yellow;
Console.WriteLine($"  Skipped:             {result.Skipped}");
Console.ResetColor();

// Print detailed table list
if (result.MigratedTables.Count > 0)
{
    Console.ForegroundColor = ConsoleColor.White;
    Console.WriteLine("\n  Migrated tables:");
    Console.ResetColor();

    var lastDb = "";
    foreach (var item in result.MigratedTables)
    {
        if (item.Database != lastDb)
        {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine($"\n    [{item.Database}]");
            Console.ResetColor();
            lastDb = item.Database;
        }

        if (item.Rows > 0)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"      {item.Table}: {item.Rows,8} rows");
        }
        else
        {
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.WriteLine($"      {item.Table}: {item.Rows,8} rows");
        }
        Console.ResetColor();
    }
}

if (result.Errors.Count > 0)
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine($"\n  Errors ({result.Errors.Count}):");
    foreach (var err in result.Errors)
    {
        Console.WriteLine($"    - {err}");
    }
    Console.ResetColor();
    WaitAndExit();
    return 1;
}
else
{
    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine("\n  Migration completed successfully!\n");
    Console.ResetColor();
    WaitAndExit();
    return 0;
}

static void WaitAndExit()
{
    Console.WriteLine("Press any key to exit...");
    Console.ReadKey(true);
}
