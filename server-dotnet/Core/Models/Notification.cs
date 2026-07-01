using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace Core.Models;

public class Notification
{
    public string Id { get; set; } = string.Empty;

    public string Name { get; set; } = string.Empty;

    public string Receiver { get; set; } = string.Empty;

    public int Delay { get; set; } = 1;

    public int Interval { get; set; } = 1;

    public bool Enabled { get; set; } = true;

    public string Text { get; set; } = string.Empty;

    public string Type { get; set; } = string.Empty;

    public Dictionary<string, bool> Subscriptions { get; set; } = [];

    public JsonElement Options { get; set; } 

    public NotificationMode Mode { get; set; } = NotificationMode.All;

}

public enum NotificationMode
{
    All = 0,
    Single = 1,
}