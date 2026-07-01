using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace Core.Models;

public class Graph
{
    public string Id { get; set; } = string.Empty;

    public string Name { get; set; } = string.Empty;

    public GraphType Type { get; set; }

    public JsonElement Property { get; set; }

    public List<GraphSource> Sources { get; set; } = [];

}


public class GraphSource
{
    public string Device { get; set; } = string.Empty;

    public string Id { get; set; } = string.Empty;

    public string Name { get; set; } = string.Empty;

    public string Label { get; set; } = string.Empty;

    public string Color { get; set; } = string.Empty;

    public string? Fill { get; set; }
}

public enum GraphType
{
    Bar = 0,
    Pie = 1,
}