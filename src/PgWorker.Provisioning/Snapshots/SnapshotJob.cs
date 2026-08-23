using PgWorker.Core;
using PgWorker.Etcd.Client;

namespace PgWorker.Provisioning.Snapshots;

/// <summary>
/// Снапшоты etcd (задача 22; P12, spec §7): /v3/snapshot/save → файл
/// snapshot-<yyyyMMdd-HHmmss>.db в каталоге тома. Ретеншн: держим последние
/// RetentionFiles файлов (старейшие сверх лимита удаляются). Снимает
/// SnapshotLoop (лидер) и процессы в точках изменений (до/после).
/// </summary>
public sealed class SnapshotJob(
    IEtcdGateway etcd,
    string[] endpoints,
    string dir,
    int retentionFiles = 10)
{
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
