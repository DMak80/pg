namespace PgWorker.Core.DI;

[AttributeUsage(AttributeTargets.Class)]
public class ConfigAttribute(string? name = null) : Attribute
{
    public string? Name { get; set; } = name;
}