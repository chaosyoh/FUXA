namespace Core.Models.Requests;

public class SetTagValueRequest
{
    public List<TagValueItem> Tags { get; set; } = new();
}

public class TagValueItem
{
    public string Id { get; set; } = string.Empty;
    public object? Value { get; set; }
}
