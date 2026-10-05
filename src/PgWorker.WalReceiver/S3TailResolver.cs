using PgWorker.Backups;
using Shared.Core;

namespace PgWorker.WalReceiver;

/// <summary>Читатель позиции слота на источнике (t27, arch/19 §3.1): пустой префикс
/// S3 — старт от restart_lsn. Реализация Npgsql — Task 5.</summary>
public interface ISlotPositionReader
{
    /// <summary>restart_lsn слота из pg_replication_slots; слот отсутствует → Failed.</summary>
    Task<Result<ulong>> ReadRestartLsnAsync(string slot, CancellationToken ct);
}

/// <summary>Резолв стартовой позиции приёмника от хвоста S3 (t27, arch/19 §3):
/// list префикса → минимальный сегмент как chain_start → WalChain.Check:
/// непрерывная цепочка — старт с конца последнего сегмента; дыра — старт с конца
/// непрерывной части (повторная доставка выше дыры идемпотентна по имени);
/// пустой префикс — null (старт от restart_lsn слота).</summary>
public static class S3TailResolver
{
    public static async Task<Result<ulong?>> ResolveTailAsync(
        IBackupS3 s3, string cluster, string shard, CancellationToken ct)
    {
        var listed = await s3.ListWalAsync(cluster, shard, ct: ct);
        if (!listed.IsSuccess)
            return Result<ulong?>.Failed(listed.Error!);

        var segments = listed.Value
            .Select(o => WalFileName.TryParse(o.Name))
            .OfType<WalFileName>()
            .OrderBy(s => s.Name, StringComparer.Ordinal)
            .ToList();
        if (segments.Count == 0)
            return Result<ulong?>.Success(null);

        var names = listed.Value.Select(o => o.Name).ToList();
        var chain = WalChain.Check(segments[0], names);
        if (chain.LastSegment is not { } last)
            return Result<ulong?>.Success(null); // цепочка не закреплена — старт от слота

        return Result<ulong?>.Success(EndLsn(last));
    }

    /// <summary>LSN конца сегмента: (позиция + 1) × 16 MiB — первая непокрытая
    /// сегментом позиция (старт следующего).</summary>
    public static ulong EndLsn(WalFileName segment)
        => (ulong)(segment.Log * WalFileName.SegsPerLog + segment.Seg + 1) << 24;
}
