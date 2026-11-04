using System.Globalization;

namespace PgWorker.Backups;

/// <summary>Ключ .partial-версии незакрытого сегмента (arch/19 §3):
/// wal/&lt;segment&gt;.&lt;endLsn&gt;.partial — segment 24 hex строчных, endLsn РОВНО
/// 16 hex строчных (LSN конца валидного префикса): фиксированная ширина даёт
/// лексикографический порядок суффиксов = числовому — лучший выбирается
/// без числового парсинга. Финалы и .history — не парсятся (null).</summary>
public static class WalPartialName
{
    public const string Suffix = ".partial";

    /// <summary>Ключ объекта по имени сегмента (24 hex) и LSN конца префикса.</summary>
    public static string Key(string segmentName, ulong endLsn)
        => $"{segmentName}.{endLsn:x16}{Suffix}";

    /// <summary>Разбор имени объекта: (сегмент, EndLsn) либо null (не .partial).</summary>
    public static (WalFileName Segment, ulong EndLsn)? TryParse(string name)
    {
        // <24hex>.<16hex>.partial — ровно 49 символов.
        if (name.Length != 24 + 1 + 16 + Suffix.Length
            || name[24] != '.'
            || !name.EndsWith(Suffix, StringComparison.Ordinal))
            return null;
        if (WalFileName.TryParse(name[..24]) is not { } segment)
            return null;
        return ulong.TryParse(name.AsSpan(25, 16), NumberStyles.HexNumber,
                   CultureInfo.InvariantCulture, out var endLsn)
            ? (segment, endLsn)
            : null;
    }

    /// <summary>Узкий S3-префикс листинга всех версий сегмента: "&lt;segmentName&gt;.".</summary>
    public static string SegmentPrefixOf(string segmentName)
        => segmentName + ".";
}
