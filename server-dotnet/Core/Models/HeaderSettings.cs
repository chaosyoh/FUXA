using System;
using System.Collections.Generic;
using System.Text;

namespace Core.Models;

public class HeaderSettings
{
    public string Title { get; set; } = string.Empty;

    public string Alarms { get; set; } = string.Empty;

    public string Infos { get; set; } = string.Empty;

    public string Bkcolor { get; set; } = string.Empty;

    public string Fgcolor { get; set; } = string.Empty;

    public string FontFamily { get; set; } = string.Empty;

    public double FontSize { get; set; }

    public List<HeaderItem> Items { get; set; } = [];

    public string ItemsAnchor { get; set; } = string.Empty;

    public string LoginInfo { get; set; } = string.Empty;

    public string DateTimeDisplay { get; set; } = string.Empty;

    public string Language { get; set; } = string.Empty;
}

public class HeaderItem
{
    public string Id { get; set; } = string.Empty;

    public string Type { get; set; } = string.Empty;
    public string Icon { get; set; } = string.Empty;
    public string Image { get; set; } = string.Empty;
    public string Bkcolor { get; set; } = string.Empty;
    public string Fgcolor { get; set; } = string.Empty;

    public double MarginLeft { get; set; }
    public double MarginRight { get; set; }


}

public class GaugeProperty
{
    public string VariableId { get; set; } = string.Empty;
    public string VariableValue { get; set; } = string.Empty;
    public int Bitmask { get; set; }
    public int Permission { get; set; }
    public PermissionRole PermissionRoles { get; set; } = new PermissionRole();

    public bool Readonly { get; set; }
    public string Text { get; set; } = string.Empty;
}

public class PermissionRole
{
    public string[] Show { get; set; } = [];

    public string[] Enabled { get; set; } = [];

}