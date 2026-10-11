using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.Logging;
using OwnS3.Replication.Inbound;
using OwnS3.Storage;
using OwnS3.Replication.Lww;
using OwnS3.Replication.Transport;

namespace OwnS3.Replication.Reconcile;

// Reconcile / backfill — гарантийный контур сходимости (t35, глава 07 §9): приёмник
// сверяет СВОЙ листинг с листингом соседа и закрывает дыры фетчами. Механизм:
// backfill состава бакетов (листинг без bucket) → постраничный ListAsync соседа →
// LWW-сравнение (ReadKeyState) → старее/отсутствует → fetch-задача в очередь источника;
// новее → ничего. Прогресс-маркер reconcile/<source>.marker — персист после каждой
// страницы; параллельный проход по одному соседу не создаётся.
public sealed class ReconcileRunner(ReplicationTargets targets, IReplicaStore store, ReplicaApplier applier,
    FetchQueueRegistry queues, IReplicationTransport transport, string reconcileDir,
    ILogger<ReconcileRunner> logger, ReplicationMetrics metrics)
{
    private readonly ConcurrentDictionary<string, long> _processed = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, byte> _running = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, Task> _runs = new(StringComparer.Ordinal);

    // Один проход по соседу; идущий — await (повторный Trigger не порождает второй).
    public async Task RunForSourceAsync(string sourceName, CancellationToken ct)
    {
        if (!_running.TryAdd(sourceName, 0))
        {
            await (_runs.TryGetValue(sourceName, out var existing) ? existing : Task.CompletedTask);
            return;
        }
        var run = RunCoreAsync(sourceName, ct);
        _runs[sourceName] = run;
        try
        {
            await run;
        }
        finally
        {
            _running.TryRemove(sourceName, out _);
            _runs.TryRemove(sourceName, out _);
        }
    }

    // Идёт ли reconcile по соседу (статус/повторный триггер).
    public bool IsRunning(string sourceName) => _running.ContainsKey(sourceName);

    // Админ/событие: внеплановый проход (fire-and-forget, параллельный не создаётся).
    public void Trigger(string sourceName, CancellationToken ct) =>
        _ = Task.Run(() => RunForSourceAsync(sourceName, ct), ct);

    // Прогресс (метрика reconcile.position, глава 07 §10).
    public long ProcessedKeys(string sourceName) =>
        _processed.TryGetValue(sourceName, out var v) ? v : 0;

    private async Task RunCoreAsync(string sourceName, CancellationToken ct)
    {
        var target = targets.Snapshot.FirstOrDefault(t => t.Name == sourceName);
        if (target is null)
            return;
        logger.LogInformation("Reconcile по соседу {Source}: старт", sourceName);

        // Backfill состава бакетов (§9): у соседа есть — у нас нет → create с CreationDate соседа
        var neighborBuckets = await transport.ListingBucketsAsync(target, ct);
        foreach (var (name, creationUnixMs) in neighborBuckets)
        {
            applier.ApplyBucketCreate(name, creationUnixMs);
        }

        var processed = 0L;
        foreach (var (bucket, _) in neighborBuckets)
        {
            // Постраничный проход от маркера (рестарт продолжает, §9)
            var marker = ReadMarker(sourceName, bucket);
            while (!ct.IsCancellationRequested)
            {
                var page = await transport.ListAsync(target, bucket, marker, maxKeys: 1000, ct);
                foreach (var entry in page.Entries)
                {
                    processed++;
                    var local = store.ReadKeyState(bucket, entry.Key);
                    var localMod = local?.ModTime.ToUnixTimeMilliseconds() ?? -1;
                    if (entry.ModTime.ToUnixTimeMilliseconds() > localMod)
                    {
                        // старее/отсутствует → fetch-задача в очередь источника (§9;
                        // идемпотентно — фетчер применит по LWW)
                        queues.ForSource(sourceName).TryEnqueue(new FetchTask(bucket, entry.Key,
                            entry.ModTime.ToUnixTimeMilliseconds()));
                    }
                    // новее → ничего: наш вариант выиграет при взаимной доставке (§9)
                }
                metrics.SetReconcilePosition(sourceName, processed);
                marker = page.NextMarker;
                WriteMarker(sourceName, bucket, marker);
                if (marker is null)
                    break; // страница без nextMarker — листинг исчерпан
            }
            ClearMarker(sourceName, bucket);
        }

        _processed[sourceName] = processed;
        logger.LogInformation("Reconcile по соседу {Source}: завершён (ключей обработано: {Processed})",
            sourceName, processed);
    }

    // Маркер прогресса: "<bucket>|<marker>" — однострочный файл per-source (§9).
    private string MarkerPath(string sourceName) => Path.Combine(reconcileDir, sourceName + ".marker");

    private (string Bucket, string? Marker)? ReadAll(string sourceName)
    {
        if (!File.Exists(MarkerPath(sourceName)))
            return null;
        var parts = File.ReadAllText(MarkerPath(sourceName)).Split('|', 2);
        return (parts[0], parts.Length > 1 && parts[1].Length > 0 ? parts[1] : null);
    }

    private string? ReadMarker(string sourceName, string bucket)
    {
        var all = ReadAll(sourceName);
        return all?.Bucket == bucket ? all?.Marker : null; // чужой бакет в маркере — с нуля
    }

    private void WriteMarker(string sourceName, string bucket, string? marker)
    {
        Directory.CreateDirectory(reconcileDir);
        var tmp = MarkerPath(sourceName) + ".tmp";
        File.WriteAllText(tmp, $"{bucket}|{marker ?? string.Empty}", Encoding.UTF8);
        File.Move(tmp, MarkerPath(sourceName), overwrite: true);
    }

    private void ClearMarker(string sourceName, string bucket)
    {
        var all = ReadAll(sourceName);
        if (all?.Bucket == bucket && File.Exists(MarkerPath(sourceName)))
            File.Delete(MarkerPath(sourceName)); // проход по бакету завершён
    }
}
