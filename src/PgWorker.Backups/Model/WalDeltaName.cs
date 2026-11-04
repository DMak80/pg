using System.Globalization;

namespace PgWorker.Backups;

/// <summary>Имя дельта-куска незакрытого сегмента (arch/pgnode/03 §2, spec §4.1):
/// wal/&lt;segment&gt;.partial_&lt;start:x16&gt;_&lt;end:x16&gt;_&lt;life:x8&gt;_&lt;role&gt;_&lt;md5&gt; —
/// start/end — абсолютные LSN (полузакрытый [start,end), размер = end−start),
/// life — номер жизни цепочки (u32, 8 hex), роль источника m|r на момент
/// выкладки, md5 байтов куска (32 hex). Вырожденный диапазон start &gt;= end
/// запрещён конструированием (Key бросает, TryParse → null — spec №29).
/// Все ширины фиксированы — сортировка имён сегмента = порядок по start;
/// t14-версии wal/&lt;segment&gt;.&lt;endLsn&gt;.partial этим парсером не разбираются
/// (TryParse null — безномерный legacy переходного окна). Чистые функции.</summary>
public static class WalDeltaName
{
    public const string PrefixLiteral = ".partial_";

    /// <summary>Кусок: сегмент, границы [Start,End), номер жизни, роль, md5-hex.</summary>
    public sealed record Piece(
        WalFileName Segment, ulong Start, ulong End, uint ChainLife, bool IsMaster, string Md5Hex);

    /// <summary>Ключ объекта: сегмент + суффикс с фиксированными ширинами;
    /// start &gt;= end — ArgumentException (пустые/перевёрнутые куски не существуют).</summary>
    public static string Key(
        WalFileName segment, ulong start, ulong end, uint chainLife, bool isMaster, string md5Hex)
    {
        if (start >= end)
            throw new ArgumentException("delta piece must have start < end (half-open range)");
        return $"{segment.Name}{PrefixLiteral}{start:x16}_{end:x16}_{chainLife:x8}_{(isMaster ? "m" : "r")}_{md5Hex}";
    }

    /// <summary>Разбор имени объекта: кусок либо null (финал/t14-версия/мусор/
    /// вырожденный диапазон).</summary>
    public static Piece? TryParse(string name)
    {
        // <24hex>.partial_<16hex>_<16hex>_<8hex>_<m|r>_<32hex>
        // = 24 + 9 + 16 + 1 + 16 + 1 + 8 + 1 + 1 + 1 + 32 = 110 символов.
        const int Total = 24 + 9 + 16 + 1 + 16 + 1 + 8 + 1 + 1 + 1 + 32;
        if (name.Length != Total)
            return null;
        if (!name.AsSpan(24, PrefixLiteral.Length).SequenceEqual(PrefixLiteral))
            return null;
        var span = name.AsSpan();
        if (span[24 + 9 + 16] != '_' || span[24 + 9 + 16 + 1 + 16] != '_'
            || span[24 + 9 + 16 + 1 + 16 + 1 + 8] != '_' || span[24 + 9 + 16 + 1 + 16 + 1 + 8 + 1 + 1] != '_')
            return null;
        var inv = CultureInfo.InvariantCulture;
        if (!ulong.TryParse(span.Slice(24 + 9, 16), NumberStyles.HexNumber, inv, out var start)
            || !ulong.TryParse(span.Slice(24 + 9 + 1 + 16, 16), NumberStyles.HexNumber, inv, out var end)
            || !uint.TryParse(span.Slice(24 + 9 + 16 + 1 + 16 + 1, 8), NumberStyles.HexNumber, inv, out var life))
            return null;
        if (start >= end)
            return null; // вырожденный диапазон — битый кусок
        var role = span[24 + 9 + 16 + 1 + 16 + 1 + 8 + 1];
        if (role is not ('m' or 'r'))
            return null;
        var md5 = span.Slice(24 + 9 + 16 + 1 + 16 + 1 + 8 + 1 + 1 + 1, 32);
        foreach (var c in md5)
            if (!Uri.IsHexDigit(c) || char.IsUpper(c))
                return null;
        if (WalFileName.TryParse(name[..24]) is not { } segment)
            return null;
        return new Piece(segment, start, end, life, role == 'm', new string(md5));
    }

    /// <summary>Покрытие дельта-кусков ТЕКУЩЕГО номера жизни по именам (без
    /// байтов; arch/pgnode/processes/g-wal-delta-assembly.md, spec §4.2/§4.5):
    /// база start=0 задаёт начальное покрытие (max end среди баз), затем куски
    /// start ≤ coverage &lt; end расширяют его жадно (сортировка по start);
    /// 0 — базы нет (покрытие жизни не начато). Куски чужих номеров не
    /// участвуют; финалы и t14-версии этим парсером не разбираются вовсе.</summary>
    public static ulong CoverageOf(IEnumerable<string> names, uint chainLife)
    {
        var pieces = new List<Piece>();
        foreach (var name in names)
            if (TryParse(name) is { } piece && piece.ChainLife == chainLife)
                pieces.Add(piece);
        pieces.Sort((a, b) => a.Start.CompareTo(b.Start));

        ulong coverage = 0;
        foreach (var piece in pieces)
            if (piece.Start == 0 && piece.End > coverage)
                coverage = piece.End; // база жизни
        bool extended;
        do
        {
            extended = false;
            foreach (var piece in pieces)
                if (piece.Start <= coverage && piece.End > coverage)
                {
                    coverage = piece.End;
                    extended = true;
                }
        }
        while (extended);

        return coverage;
    }

    /// <summary>Узкий S3-префикс листинга всего кусочного семейства сегмента
    /// (куски и t14-версии): "&lt;segmentName&gt;.".</summary>
    public static string SegmentPrefixOf(string segmentName)
        => segmentName + ".";
}
