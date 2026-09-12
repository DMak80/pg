using System.Globalization;

namespace PgWorker.Backups;

/// <summary>Строка timeline-history: родитель и точка переключения (LSN «X/Y»).</summary>
public sealed record WalHistoryEntry(uint ParentTli, string SwitchLsn);

/// <summary>Строгий парсер <c>.history</c> (arch/19 §3, t04): строки
/// «parentTLI switchWALLSN [reason]» (tab-разделение, как пишет PostgreSQL);
/// последняя запись файла &lt;newTLI&gt;.history описывает сам newTLI — ответвление
/// от parentTLI в switchWALLSN. Мусорные строки пропускаются; ни одной
/// валидной → null (строгий разбор невозможен — фолбэк на эвристику t03).</summary>
public static class WalHistory
{
    public static IReadOnlyList<WalHistoryEntry>? Parse(string content)
    {
        List<WalHistoryEntry>? entries = null;
        foreach (var line in content.Split('\n'))
        {
            var fields = line.Trim('\r', ' ').Split('\t', ' ');
            if (fields.Length < 2)
                continue;
            if (!uint.TryParse(fields[0], NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var parent))
                continue;
            if (!IsLsn(fields[1]))
                continue;
            (entries ??= []).Add(new WalHistoryEntry(parent, fields[1]));
        }

        return entries is { Count: > 0 } list ? list : null;
    }

    // LSN — строго «X/Y»: обе половины hex (ненулевой длины).
    private static bool IsLsn(string value)
    {
        var parts = value.Split('/');
        return parts.Length == 2
               && parts[0].Length is > 0 and <= 8
               && parts[1].Length is > 0 and <= 8
               && ulong.TryParse(parts[0], NumberStyles.HexNumber, CultureInfo.InvariantCulture, out _)
               && ulong.TryParse(parts[1], NumberStyles.HexNumber, CultureInfo.InvariantCulture, out _);
    }
}
