using PgWorker.Core;
using PgWorker.Etcd.Client;

namespace PgWorker.Provisioning.Snapshots;

/// <summary>
/// Снапшоты etcd (задача 22; P12, spec §7): /v3/snapshot/save → файл
/// snapshot-<yyyyMMdd-HHmmss>.db в каталоге тома. Ретеншн: держим последние
/// RetentionFiles файлов (старейшие сверх лимита удаляются). Снимает
/// SnapshotLoop (лидер) и процессы в точках изменений (до/после).
/// Обслуживание (MaintainAsync): compact кластера + последовательная
/// дефрагментация каждой ноды — не чаще раза в MaintenanceIntervalMin.
/// </summary>
public sealed class SnapshotJob(
    IEtcdGateway etcd,
    string[] endpoints,
    string dir,
    int retentionFiles = 10,
    int maintenanceIntervalMin = 60)
{
    // Локальное время последнего обслуживания (compact + defrag).
    // Статичная переменная: переживает реконструкцию SnapshotJob (singleton),
    // protects от частых тиков SnapshotLoop.
    private static DateTimeOffset? _lastMaintenanceUtc;

    // Сброс состояния обслуживания (только для тестов).
    internal static void ResetMaintenanceState() => _lastMaintenanceUtc = null;

    public async Task<Result<string>> TakeAsync(CancellationToken ct)
    {
        // Failover: первый успешный endpoint.
        byte[]? data = null;
        Exception? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.SnapshotSaveAsync(endpoint, ct);
            if (result.IsSuccess)
            {
                data = result.Value;
                break;
            }

            last = result.Error!;
        }

        if (data is null)
            return Result<string>.Failed(last ?? new ApplicationException("нет etcd-endpoints"));

        try
        {
            Directory.CreateDirectory(dir);
            var path = Path.Combine(dir,
                $"snapshot-{DateTimeOffset.UtcNow:yyyyMMdd-HHmmss}.db");
            await File.WriteAllBytesAsync(path, data, ct);
            ApplyRetention();
            return Result<string>.Success(path);
        }
        catch (Exception e)
        {
            return Result<string>.Failed(new ApplicationException($"снапшот не записан в {dir}: {e.Message}", e));
        }
    }

    /// <summary>
    /// Обслуживание etcd: compact (сжатие истории) + defrag (дефрагментация
    /// каждой ноды строго последовательно). Выполняется не чаще раза в
    /// MaintenanceIntervalMin — время последней процедуры хранится в статичной
    /// переменной _lastMaintenanceUtc.
    /// </summary>
    public async Task<Result> MaintainAsync(CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow;
        if (_lastMaintenanceUtc is { } last && now - last < TimeSpan.FromMinutes(maintenanceIntervalMin))
            return Result.Success();

        // 1. Получить текущую ревизию (failover по endpoints).
        long? revision = null;
        Exception? lastErr = null;
        foreach (var endpoint in endpoints)
        {
            var status = await etcd.StatusAsync(endpoint, ct);
            if (status.IsSuccess)
            {
                revision = status.Value;
                break;
            }

            lastErr = status.Error!;
        }

        if (revision is null)
            return Result.Failed(lastErr ?? new ApplicationException("нет etcd-endpoints"));

        // 2. Compact — кластерная операция, достаточно на одном endpoint.
        var compacted = false;
        foreach (var endpoint in endpoints)
        {
            var compact = await etcd.CompactAsync(endpoint, revision.Value, ct);
            if (compact.IsSuccess)
            {
                compacted = true;
                break;
            }

            lastErr = compact.Error!;
        }

        if (!compacted)
            return Result.Failed(lastErr!);

        // 3. Defragment — строго последовательно на каждой ноде.
        foreach (var endpoint in endpoints)
        {
            var defrag = await etcd.DefragmentAsync(endpoint, ct);
            if (!defrag.IsSuccess)
                return defrag;
        }

        _lastMaintenanceUtc = now;
        return Result.Success();
    }

    // Ретеншн: только последние retentionFiles файлов (имена — таймштампы,
    // лексикографическая сортировка = хронологическая).
    private void ApplyRetention()
    {
        var files = Directory.GetFiles(dir, "snapshot-*.db")
            .OrderByDescending(f => f, StringComparer.Ordinal)
            .ToList();
        foreach (var stale in files.Skip(retentionFiles))
            File.Delete(stale);
    }
}
