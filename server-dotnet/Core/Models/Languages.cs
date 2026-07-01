using System;
using System.Collections.Generic;
using System.Text;

namespace Core.Models;

public class Languages
{
    public Language Default { get; set; } = new Language();

    public List<Language> Options { get; set; } = new List<Language>();


}

public class Language
{

    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
}