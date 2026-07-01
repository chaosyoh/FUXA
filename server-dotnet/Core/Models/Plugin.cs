using System;
using System.Collections.Generic;
using System.Text;

namespace Core.Models;

public class Plugin
{
    public string Name { get; set; } = string.Empty;

    public string Type {  get; set; } = string.Empty;

    public string Version {  get; set; } = string.Empty;

    public string Current {  get; set; } = string.Empty;

    public string Status {  get; set; } = string.Empty;

    public bool Pkg {  get; set; }

    public bool Dinamic { get; set; }

    public string Group { get; set; } = string.Empty;
}

