using System.Globalization;
namespace PgWorker.NodeTools;

// Минимальный парсер параметров. Формат фиксированный: --name value и
// позиционные значения после опций. Значения могут содержать запятые/URL.
public sealed class CommandLine(IReadOnlyList<string> args)
{
    private readonly IReadOnlyList<string> _args = args;

    public IReadOnlyList<string> Positionals =>
        [.. _args.Where((_, i) => !IsOptionName(i) && !IsOptionValue(i))];

    public string? Value(string name)
    {
        var index = _args.ToList().FindIndex(a => a == $"--{name}");
        return index >= 0 && index + 1 < _args.Count ? _args[index + 1] : null;
    }

    public int Int(string name, int fallback)
        => int.TryParse(Value(name), CultureInfo.InvariantCulture, out var value) ? value : fallback;

    public double Double(string name, double fallback)
        => double.TryParse(Value(name), CultureInfo.InvariantCulture, out var value) ? value : fallback;

    public IReadOnlyList<string> List(string name, string fallback)
        => (Value(name) ?? fallback)
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    public bool Has(string name) => _args.Contains($"--{name}");

    private bool IsOptionName(int index) =>
        _args[index].StartsWith("--", StringComparison.Ordinal);

    private bool IsOptionValue(int index)
    {
        for (var i = index - 1; i >= 0; i--)
        {
            if (!IsOptionName(i))
                continue;
            return i + 1 == index;
        }

        return false;
    }
}
