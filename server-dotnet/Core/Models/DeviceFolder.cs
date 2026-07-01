using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Models;

/// <summary>
/// Device folder for tree organization
/// </summary>
public class DeviceFolder
{
    /// <summary>
    /// Folder ID, GUID with df_ prefix
    /// </summary>
    public string Id { get; set; } = string.Empty;

    /// <summary>
    /// Parent folder ID, empty string means root level
    /// </summary>
    public string ParentId { get; set; } = string.Empty;

    /// <summary>
    /// Folder display name
    /// </summary>
    public string Name { get; set; } = string.Empty;
}
