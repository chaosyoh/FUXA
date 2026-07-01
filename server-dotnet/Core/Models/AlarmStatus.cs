namespace Core.Models;

public class AlarmStatus
{
    public int Highhigh { get; set; }
    public int High { get; set; }
    public int Low { get; set; }
    public int Info { get; set; }
    public List<AlarmActionDto> Actions { get; set; } = new();
}
