using PgWorker.Backups;
using Shared.Core;

namespace PgWorker.PgNode;

/// <summary>Читатель позиции слота на источнике (arch/pgnode/03-wal-receiver.md): пустой префикс
/// S3 — старт от restart_lsn. Реализация Npgsql — Task 5.</summary>
public interface ISlotPositionReader
{
    /// <summary>restart_lsn слота из pg_replication_slots; слот отсутствует → Failed.</summary>
    Task<Result<ulong>> ReadRestartLsnAsync(string slot, CancellationToken ct);
}

/// <summary>Хвост S3: стартовая LSN (конец последнего сегмента непрерывной
/// части цепочки) + имя хвостового сегмента (TLI из имени — вход tli-процедуры
/// при 58P01).</summary>
public sealed record WalTail(ulong Position, string SegmentName);

/// <summary>Резолв стартовой позиции приёмника от хвоста S3 (arch/backups/02-wal-archive.md §1):
/// list префикса → минимальный сегмент как chain_start → WalChain.Check:
/// непрерывная цепочка — старт с конца последнего сегмента; дыра — старт с конца
/// непрерывной части (повторная доставка выше дыры идемпотентна по имени);
/// пустой префикс — null (старт от restart_lsn слота). Хвост — на непрерывной
/// части от минимального сегмента: откатанная restore-ветка остаётся её
/// продолжением (так новые сегменты живой ветки архивируются от точки форка —
/// цепочка непрерывна для восстановления старого полного). Хвост, который
/// оказался впереди позиции записи сервера (restore вернул LSN назад),
/// резолвится отдельно — <see cref="ResolveLiveTailAsync"/>.</summary>
public static class S3TailResolver
{
    public static Task<Result<WalTail?>> ResolveTailAsync(
        IBackupS3 s3, string cluster, string shard, CancellationToken ct)
        => ResolveAsync(s3, cluster, shard, restartFromNewestTli: false, ct);

    /// <summary>Хвост ЖИВОЙ ветки: проверка перезапускается от первого сегмента
    /// максимального TLI (CheckWithRestart — правило контроля цепочки). Вход —
    /// хвост основного резолва впереди позиции записи сервера (откат restore:
    /// сегменты прежней ветки выше точки форка остаются в wal/, а живая ветка
    /// пишет ниже); без сегментов живой ветки — null (старт от restart_lsn).</summary>
    public static Task<Result<WalTail?>> ResolveLiveTailAsync(
        IBackupS3 s3, string cluster, string shard, CancellationToken ct)
        => ResolveAsync(s3, cluster, shard, restartFromNewestTli: true, ct);

    private static async Task<Result<WalTail?>> ResolveAsync(
        IBackupS3 s3, string cluster, string shard, bool restartFromNewestTli,
        CancellationToken ct)
    {
        var listed = await s3.ListWalAsync(cluster, shard, ct: ct);
        if (!listed.IsSuccess)
            return Result<WalTail?>.Failed(listed.Error!);

        var segments = listed.Value
            .Select(o => WalFileName.TryParse(o.Name))
            .OfType<WalFileName>()
            .OrderBy(s => s.Name, StringComparer.Ordinal)
            .ToList();
        if (segments.Count == 0)
            return Result<WalTail?>.Success(null);

        var names = listed.Value.Select(o => o.Name).ToList();
        var chain = restartFromNewestTli
            ? WalChain.CheckWithRestart(segments[0], names)
            : WalChain.Check(segments[0], names);
        if (chain.LastSegment is not { } last)
            return Result<WalTail?>.Success(null); // цепочка не закреплена — старт от слота

        return Result<WalTail?>.Success(new WalTail(EndLsn(last), last.Name));
    }

    /// <summary>LSN конца сегмента: (позиция + 1) × 16 MiB — первая непокрытая
    /// сегментом позиция (старт следующего).</summary>
    public static ulong EndLsn(WalFileName segment)
        => (ulong)(segment.Log * WalFileName.SegsPerLog + segment.Seg + 1) << 24;
}
