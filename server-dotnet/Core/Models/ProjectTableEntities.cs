using SqlSugar;

namespace Core.Models;

/// <summary>
/// Thin subclasses of RowData for CodeFirst table initialization.
/// Each class maps to one of the 10 project tables via [SugarTable].
/// </summary>
[SugarTable("general")]
public class GeneralRow : RowData { }

[SugarTable("views")]
public class ViewRow : RowData { }

[SugarTable("devices")]
public class DeviceRow : RowData { }

[SugarTable("devicesSecurity")]
public class DevicesSecurityRow : RowData { }

[SugarTable("texts")]
public class TextRow : RowData { }

[SugarTable("alarms")]
public class ProjectAlarmRow : RowData { }

[SugarTable("notifications")]
public class ProjectNotificationRow : RowData { }

[SugarTable("scripts")]
public class ScriptRow : RowData { }

[SugarTable("reports")]
public class ReportRow : RowData { }

[SugarTable("locations")]
public class LocationRow : RowData { }
