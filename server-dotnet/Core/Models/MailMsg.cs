namespace Core.Models;

public class MailMsg
{
    public string? From { get; set; }
    public string To { get; set; } = string.Empty;
    public string Subject { get; set; } = string.Empty;
    public string Text { get; set; } = string.Empty;
    public string? Html { get; set; }
    public List<string>? Attachments { get; set; }
}
