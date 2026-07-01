namespace Core.Models;

public class Alarm
{
    public string Name { get; set; } = string.Empty;
    public AlarmProperty? Property { get; set; }
    public AlarmSubProperty? Highhigh { get; set; }
    public AlarmSubProperty? High { get; set; }
    public AlarmSubProperty? Low { get; set; }
    public AlarmSubProperty? Info { get; set; }
    public AlarmSubActions? Actions { get; set; }
    public string? Value { get; set; }
}
