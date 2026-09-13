using System.Text.RegularExpressions;

namespace PgWorker.Backups.Restore;

// Разбор backup_label полного бэкапа (t05 §3.6): стартовый WAL-сегмент цепочки
// — sed-эквивалент прецедента entrypoint t02 (START WAL LOCATION: <lsn>
// (file <segment>)). Нет совпадения → null (битый/чужой формат).
public static partial class BackupLabel
{
    [GeneratedRegex(@"^START WAL LOCATION: .*\((file [0-9A-Fa-f]+)\)", RegexOptions.Multiline)]
    private static partial Regex StartWalLine();

    /// <summary>Имя стартового WAL-сегмента из текста backup_label; null — строка
    /// START WAL LOCATION отсутствует/битая.</summary>
    public static string? WalStartSegment(string backupLabelRaw)
    {
        var match = StartWalLine().Match(backupLabelRaw);
        return match.Success ? match.Groups[1].Value["file ".Length..] : null;
    }
}
