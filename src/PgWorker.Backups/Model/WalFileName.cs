using System.Globalization;

namespace PgWorker.Backups;

/// <summary>Имя WAL-сегмента — 24 hex-символа TLI(8)+log(8)+seg(8) (arch/19 §3).
/// `.partial` — незакрытый сегмент (в цепочку не входит); `NNNNNNNN.history` —
/// timeline-история (загружается обязательно). Чистые функции, без состояния.</summary>
public readonly record struct WalFileName(uint Tli, uint Log, uint Seg)
{
    /// <summary>Стандартный размер WAL-сегмента (16 MiB) — ноды Spilo канона arch/14.</summary>
    public const long SegmentBytes = 16L * 1024 * 1024;

    /// <summary>Сегментов в одном log-файле (seg 0x00..0xFF).</summary>
    public const uint SegsPerLog = 0x100;

    /// <summary>Каноническое имя (lowercase hex).</summary>
    public string Name => $"{Tli:x8}{Log:x8}{Seg:x8}";

    /// <summary>Разобрать имя объекта: строго 24 hex-символа (без суффиксов);
    /// `.partial`/`.history`/мусор → null.</summary>
    public static WalFileName? TryParse(string name)
        => name.Length == 24 && uint.TryParse(
               name.AsSpan(0, 8), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var tli)
           && uint.TryParse(
               name.AsSpan(8, 8), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var log)
           && uint.TryParse(
               name.AsSpan(16, 8), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var seg)
            ? new WalFileName(tli, log, seg)
            : null;

    /// <summary>Незакрытый сегмент (pg_receivewal дописывает) — не грузится агентом.</summary>
    public static bool IsPartial(string name)
        => name.EndsWith(".partial", StringComparison.Ordinal);

    /// <summary>Timeline-история `NNNNNNNN.history` → TLI; иначе null.</summary>
    public static uint? TryParseHistory(string name)
        => name.Length == 8 + ".history".Length
           && name.EndsWith(".history", StringComparison.Ordinal)
           && uint.TryParse(
               name.AsSpan(0, 8), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var tli)
            ? tli
            : null;

    /// <summary>LSN 'X/Y' (pg_current_wal_lsn) → имя сегмента позиции записи мастера
    /// (lag-зонд arch/19 §3): segId = lsn / 16MB; log = segId / 256; seg = segId % 256.</summary>
    public static WalFileName FromLsn(uint tli, string lsn)
    {
        var parts = lsn.Split('/');
        var value = (ulong.Parse(parts[0], NumberStyles.HexNumber, CultureInfo.InvariantCulture) << 32)
            | ulong.Parse(parts[1], NumberStyles.HexNumber, CultureInfo.InvariantCulture);
        var segId = value / (ulong)SegmentBytes;
        return new WalFileName(tli, (uint)(segId / SegsPerLog), (uint)(segId % SegsPerLog));
    }

    /// <summary>Следующий сегмент: seg+1; при seg=0xFF → log+1, seg=0 (arch/19 §3).</summary>
    public WalFileName Next()
        => Seg < 0xFF
            ? this with { Seg = Seg + 1 }
            : this with { Log = Log + 1, Seg = 0 };

    /// <summary>Расстояние в сегментах (other − this по log*256+seg); TLI не участвует
    /// (лаг — про позицию записи, timeline-скачки учитывает контроль цепочки).</summary>
    public long DistanceTo(WalFileName other)
        => (long)(other.Log * SegsPerLog + other.Seg) - (long)(Log * SegsPerLog + Seg);
}
