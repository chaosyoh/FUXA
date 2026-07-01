namespace Core.Models.Requests;

public class UploadResourceRequest
{
    public UploadResourceData Resource { get; set; } = new();
    public string? Destination { get; set; }
}

public class UploadResourceData
{
    public string Name { get; set; } = string.Empty;
    public string Data { get; set; } = string.Empty;
    public string Type { get; set; } = string.Empty;
    public string? FullPath { get; set; }
}
