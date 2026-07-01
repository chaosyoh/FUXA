using SqlSugar;
using System;
using System.Collections.Generic;
using System.Text;

namespace Core.Entity;

[SugarTable("Roles")]
public class Role
{
    [SugarColumn(ColumnName = "name", IsPrimaryKey = true)]
    public string Name { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "value")]
    public string Value { get; set; } = string.Empty;
}

