using SqlSugar;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Runtime.Storage;

[SugarTable("meters")]
public class Meters
{
    public DateTime Dt { get; set; }

    public string Tag_Id { get; set; } = string.Empty;

    public string? Tag_Value { get; set; }
}

public enum QualityCode
{
    Good = 0,
    Bad = 1,
}

