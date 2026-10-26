namespace PgWorker.WalReceiver.Operations;

/// <summary>Однострочная причина из вывода child-процесса/исключения (JSON-
/// безопасность статусов: error — всегда одна строка, spec t26 §3.2).</summary>
internal static class OpText
{
    public static string FirstLine(string text, string fallback)
    {
        var line = text.Split('\n')
            .Select(l => l.Trim().Trim('"'))
            .FirstOrDefault(l => l.Length > 0);
        return string.IsNullOrEmpty(line) ? fallback : line;
    }

    public static string OneLine(string text)
        => text.Split('\n')[0].Trim();
}
