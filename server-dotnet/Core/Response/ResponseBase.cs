using System;
using System.Collections.Generic;
using System.Text;

namespace Core.Response;

public class ResponseBase<T>
{
    public string Status { get; set; } = string.Empty;

    public string? Message { get; set; }

    public T? Data { get; set; }

}

