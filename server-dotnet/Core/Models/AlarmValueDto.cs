namespace Core.Models;

public class AlarmValueDto
{
    public string Name { get; set; } = string.Empty;
    public string Type { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public long Ontime { get; set; }
    public long Offtime { get; set; }
    public long Acktime { get; set; }
    public string Text { get; set; } = string.Empty;
    public string Group { get; set; } = string.Empty;
    public int Toack { get; set; }
    public string Bkcolor { get; set; } = string.Empty;
    public string Color { get; set; } = string.Empty;
}
