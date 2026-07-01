using SqlSugar;
using System;
using System.Collections.Generic;
using System.Text;

namespace Core.Entity;

[SugarTable("users")]
public class User
{
    [SugarColumn(ColumnName = "username", IsPrimaryKey = true)]
    public string Username { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "fullname")]
    public string FullName { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "password")]
    public string Password { get; set; } = string.Empty;

    [SugarColumn(ColumnName = "groups")]
    public int Groups { get; set; } = -1;

    [SugarColumn(ColumnName = "info")]
    public string? Info { get; set; }
}

