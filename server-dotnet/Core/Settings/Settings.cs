using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Settings;

public class Settings
{
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
    public string WebcamSnapShotsDir { get; set; } = "_webcam_snapshots";
    public bool WebcamSnapShotsCleanup { get; set; } = false;
    public int WebcamSnapShotsRetain { get; set; } = 7;
    public bool SwaggerEnabled { get; set; } = false;
    public bool NodeRedEnabled { get; set; } = false;
    public string NodeRedAuthMode { get; set; } = "secure";
    public bool NodeRedUnsafeModules { get; set; } = false;
    public bool LogFull { get; set; } = false;
    public bool UserRole { get; set; } = false;
    public StmpSettings Stmp { get; set; } = new StmpSettings();
    public DaqStore DaqStore { get; set; } = new DaqStore();
    public AlarmSettings Alarms { get; set; } = new AlarmSettings();
    public DatabaseSettings Database { get; set; } = new DatabaseSettings();
}

public class LogsSettings
{
    public string Retention { get; set; } = "none";
}
