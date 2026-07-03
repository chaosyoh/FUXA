using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;

namespace Core.Settings;

public class AppSettings
{
    private static AppSettings? _instance;

    public static AppSettings GetSettings()
    {
        return _instance ?? throw new InvalidOperationException("AppSettings has not been initialized. Call AppSettings.Initialize(configuration) first.");
    }

    private string rootDir { get; set; } = string.Empty;

    /// <summary>
    /// Initialize AppSettings from ASP.NET Core IConfiguration (appsettings.json "Fuxa" section).
    /// Must be called once during startup before any GetSettings() call.
    /// </summary>
    public static AppSettings Initialize(IConfiguration configuration)
    {
        var instance = new AppSettings();
        instance.SetupDirectories();
        instance.LoadFromConfiguration(configuration);
        _instance = instance;
        return instance;
    }

    private AppSettings() { }

    /// <summary>
    /// Create all required runtime directories.
    /// </summary>
    private void SetupDirectories()
    {
        rootDir = AppDomain.CurrentDomain.BaseDirectory;
        AppDir = rootDir;
        if (string.IsNullOrEmpty(WorkDir))
        {
            WorkDir = Path.Combine(rootDir, "_appdata");
        }
        else
        {
            WorkDir = Path.Combine(rootDir, WorkDir);
        }
        DbDir = Path.Combine(rootDir, "_db");
        if (!Directory.Exists(DbDir)) Directory.CreateDirectory(DbDir);
        LogDir = Path.Combine(rootDir, "_logs");
        if (!Directory.Exists(LogDir)) Directory.CreateDirectory(LogDir);
        PackageDir = Path.Combine(rootDir, "_pkg");
        if (!Directory.Exists(PackageDir)) Directory.CreateDirectory(PackageDir);
        UploadFileDir = Path.Combine(WorkDir, "_upload");
        if (!Directory.Exists(UploadFileDir)) Directory.CreateDirectory(UploadFileDir);
        ImagesFileDir = Path.Combine(rootDir, "_images");
        if (!Directory.Exists(ImagesFileDir)) Directory.CreateDirectory(ImagesFileDir);
        WidgetsFileDir = Path.Combine(rootDir, "_widgets");
        if (!Directory.Exists(WidgetsFileDir)) Directory.CreateDirectory(WidgetsFileDir);
        ReoprtsDir = Path.Combine(rootDir, "_reports");
        if (!Directory.Exists(ReoprtsDir)) Directory.CreateDirectory(ReoprtsDir);
        WebcamSnapShotsDir = Path.Combine(rootDir, "_webcam_snapshots");
        if (!Directory.Exists(WebcamSnapShotsDir)) Directory.CreateDirectory(WebcamSnapShotsDir);
        HttpUploadFileStatic = "resources";
        Environment = System.Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? "prod";
    }

    /// <summary>
    /// Bind the "Fuxa" section from IConfiguration to all settings properties.
    /// If the section is missing or empty, defaults are preserved.
    /// </summary>
    private void LoadFromConfiguration(IConfiguration configuration)
    {
        var section = configuration.GetSection("Fuxa");
        if (section.Exists())
        {
            section.Bind(this);
        }
        UserSettingsFile = Path.Combine(WorkDir, "appsettings.json");
    }
    #region settings content
    public double Version { get; set; } = 1.4;

    public string Language { get; set; } = "en";

    public bool HideEditorOnboarding { get; set; } = false;

    public int UiPort { get; set; } = 1881;

    public string LogApiLevel { get; set; } = "tiny";

    public bool DaqEnabled { get; set; } = true;

    public int DaqTokenizer { get; set; } = 24;

    public LogsSettings Logs { get; set; } = new LogsSettings();

    public bool BroadcastAll { get; set; } = false;

    public List<string> AllowedOrigins { get; set; } = new List<string>
    {
        "http://localhost", "http://127.0.0.1", "http://192.168.*", "http://10.*", "http://localhost:4200"
    };

    public bool SecureEnabled { get; set; } = false;

    public string TokenExpiresIn { get; set; } = "1h";

    public bool EnableRefreshCookieAuth { get; set; } = false;

    public string RefreshTokenExpiresIn { get; set; } = "7d";

    public bool SecureOnlyEditor { get; set; } = false;

    public string SecretCode { get; set; } = string.Empty;

    public int HeartbeatIntervalSec { get; set; } = 10;

    public bool WebcamSnapShotsCleanup { get; set; } = false;

    public int WebcamSnapShotsRetain { get; set; } = 7;

    public bool SwaggerEnabled { get; set; } = false;

    public bool NodeRedEnabled { get; set; } = false;

    public string NodeRedAuthMode { get; set; } = "secure";

    public bool NodeRedUnsafeModules { get; set; } = false;

    public StmpSettings Stmp { get; set; } = new StmpSettings();

    public AlarmSettings Alarms { get; set; } = new AlarmSettings();
    public DaqStore DaqStore { get; set; } = new DaqStore();
    public DatabaseSettings Database { get; set; } = new DatabaseSettings();

    public bool LogFull { get; set; } = false;

    public bool UserRole { get; set; } = false;
    #endregion

    #region path content

    public string LogDir { get; set; } = string.Empty;
    public string WorkDir { get; set; } = string.Empty;
    public string AppDir { get; set; } = string.Empty;
    public string PackageDir { get; set; } = string.Empty;
    public string UploadFileDir { get; set; } = string.Empty;
    public string ImagesFileDir { get; set; } = string.Empty;
    public string WidgetsFileDir { get; set; } = string.Empty;
    public string ReoprtsDir { get; set; } = string.Empty;
    public string DbDir { get; set; } = string.Empty;
    public string WebcamSnapShotsDir { get; set; } = string.Empty;
    public string UserSettingsFile { get; set; } = string.Empty;
    public string Environment { get; set; } = string.Empty;
    public string HttpStatic { get; set; } = string.Empty;

    public string HttpUploadFileStatic { get; set; } = string.Empty;
    #endregion

    /// <summary>
    /// Merge user settings into the in-memory singleton (like Node.js mergeUserSettings)
    /// </summary>
    public void MergeUserSettings(Settings userSettings)
    {
        Language = userSettings.Language;
        HideEditorOnboarding = userSettings.HideEditorOnboarding;
        BroadcastAll = userSettings.BroadcastAll;
        SecureEnabled = userSettings.SecureEnabled;
        SecretCode = userSettings.SecretCode;
        LogFull = userSettings.LogFull;
        UserRole = userSettings.UserRole;
        NodeRedEnabled = userSettings.NodeRedEnabled;
        NodeRedAuthMode = userSettings.NodeRedAuthMode;
        NodeRedUnsafeModules = userSettings.NodeRedUnsafeModules;
        SwaggerEnabled = userSettings.SwaggerEnabled;
        EnableRefreshCookieAuth = userSettings.EnableRefreshCookieAuth;
        RefreshTokenExpiresIn = userSettings.RefreshTokenExpiresIn;
        TokenExpiresIn = userSettings.TokenExpiresIn;
        Stmp = userSettings.Stmp;
        DaqStore = userSettings.DaqStore;
        Database = userSettings.Database;
        Alarms = userSettings.Alarms;
        Logs = userSettings.Logs;
    }
}

