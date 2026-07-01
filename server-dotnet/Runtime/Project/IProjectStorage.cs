
using Core.Models;

namespace Runtime.Project;

public interface IProjectStorage
{
    Task ClearAll();
    Task DeleteSection(SqlSection section);
    Task<List<RowData>> GetSection(string table, string? name = null);
    Task SetDefault();
    Task SetSections(List<SqlSection> sections);
    Task SetSection(SqlSection section);
}