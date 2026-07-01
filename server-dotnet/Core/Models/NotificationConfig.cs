namespace Core.Models;

public class NotificationConfig
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string Type { get; set; } = string.Empty;
    public string Receiver { get; set; } = string.Empty;
    public int Delay { get; set; }
    public int Interval { get; set; }
    public bool Enabled { get; set; }
    public string Text { get; set; } = string.Empty;
    public Dictionary<string, bool> Subscriptions { get; set; } = new();
    public int Mode { get; set; }
}
