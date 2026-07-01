namespace Core.Models.Requests;

public class ReportsQueryFilter
{
    public string? Name { get; set; }
    public int? Count { get; set; }
}

public class ReportBuildRequest
{
    public object? Params { get; set; }
}

public class ReportRemoveFileRequest
{
    public ReportRemoveParams? Params { get; set; }
}

public class ReportRemoveParams
{
    public string? FileName { get; set; }
}
