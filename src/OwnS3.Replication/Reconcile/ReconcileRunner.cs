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

    // Ожидание свободного слота fetch-очереди при переполнении (§9): очередь
    // обычно дренируется приёмником за секунды — проход ждёт до бюджета, и лишь
    // пережившее бюджет переполнение приостанавливает проход (маркер сохранён).
    // Шаг опроса 250 мс; тесты (InternalsVisibleTo) задают малый бюджет.
    public TimeSpan QueueWaitBudget { get; internal init; } = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan QueueWaitPoll = TimeSpan.FromMilliseconds(250);

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

        // Ретрай транспорта: сосед на момент триггера может быть ещё недоступен
        // (бэкофф 5 с → 60 с, до ~5 мин) — сбой не роняет проход (глава 07 §9)
        IReadOnlyList<(string Name, long CreationUnixMs)> neighborBuckets;
        var backoff = TimeSpan.FromSeconds(5);
        while (true)
        {
            var listResult = await transport.ListingBucketsAsync(target, ct);
            if (listResult.IsSuccess)
            {
                neighborBuckets = listResult.Value;
                break;
            }
            logger.LogWarning(listResult.Error, "Reconcile {Source}: сосед недоступен — ретрай через {Backoff}",
                sourceName, backoff);
            await Task.Delay(backoff, ct);
            backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2, TimeSpan.FromSeconds(60).Ticks));
        }

        // Backfill состава бакетов (§9): у соседа есть — у нас нет → create с CreationDate
        // соседа; отказ create не роняет проход (прецедент ретрай-механики §9)
        foreach (var (name, creationUnixMs) in neighborBuckets)
        {
            var created = applier.ApplyBucketCreate(name, creationUnixMs);
            if (!created.IsSuccess)
                logger.LogWarning(created.Error, "Reconcile {Source}: create бакета {Bucket} не удалён",
                    sourceName, name);
        }

        var processed = 0L;
        foreach (var (bucket, _) in neighborBuckets)
        {
            // Постраничный проход от маркера (рестарт продолжает, §9)
            var marker = ReadMarker(sourceName, bucket);
            while (!ct.IsCancellationRequested)
            {
                ReplicationListingResponse page;
                var pageBackoff = TimeSpan.FromSeconds(5);
                while (true)
                {
                    var listResult = await transport.ListAsync(target, bucket, marker, maxKeys: 1000, ct);
                    if (listResult.IsSuccess)
                    {
                        page = listResult.Value;
                        break;
                    }
                    logger.LogWarning(listResult.Error,
                        "Reconcile {Source}: листинг {Bucket} не удался — ретрай через {Backoff}",
                        sourceName, bucket, pageBackoff);
                    await Task.Delay(pageBackoff, ct);
                    pageBackoff = TimeSpan.FromTicks(Math.Min(pageBackoff.Ticks * 2, TimeSpan.FromSeconds(60).Ticks));
                }
                foreach (var entry in page.Entries)
                {
                    processed++; // обработан (любой исход — поставлен/skip)
                    var localLookup = store.ReadKeyState(bucket, entry.Key);
                    var local = localLookup.IsSuccess
                        ? localLookup.Value is ReplicaKeyLookup.Found rf ? rf.State : null
                        : throw localLookup.Error!;
                    var localMod = local?.ModTime.ToUnixTimeMilliseconds() ?? -1;
                    // Решение через LWW-компаратор (та же таблица, что у приёмника —
                    // глава 07 §7): IncomingWins — включая sha-тай-брейк при равных
                    // modTime (иначе reconcile не чинит потерянное tie-уведомление)
                    var (outcome, _) = LwwComparator.Compare(
                        entry.ModTime.ToUnixTimeMilliseconds(), entry.ContentSha256Hex,
                        localMod, local?.ContentSha256Hex ?? string.Empty);
                    if (outcome != LwwOutcome.IncomingWins)
                        continue; // новее/Identical → ничего: наш вариант выиграет (§9)
                    // fetch-задача в очередь источника (§9; идемпотентно — фетчер
                    // применит по LWW). Переполнение очереди: короткий ожид ёмкости
                    // (обычно дренируется за секунды); пережило бюджет — маркер НЕ
                    // двигается и проход приостанавливается — задача не теряется
                    // (at-least-once, как 429 в inbox); продолжение — следующий
                    // триггер/периодический
                    var fetchTask = new FetchTask(bucket, entry.Key,
                        entry.ModTime.ToUnixTimeMilliseconds());
                    var queue = queues.ForSource(sourceName);
                    if (!queue.TryEnqueue(fetchTask))
                    {
                        var waited = TimeSpan.Zero;
                        var enqueued = false;
                        while (waited < QueueWaitBudget && !ct.IsCancellationRequested)
                        {
                            await Task.Delay(QueueWaitPoll, ct);
                            waited += QueueWaitPoll;
                            if (queue.TryEnqueue(fetchTask))
                            {
                                enqueued = true;
                                break;
                            }
                        }
                        if (!enqueued)
                        {
                            logger.LogWarning(
                                "Reconcile {Source}: fetch-очередь переполнена — проход приостановлен на {Bucket}/{Key}, маркер сохранён (ретрай следующим проходом)",
                                sourceName, bucket, entry.Key);
                            WriteMarker(sourceName, bucket, marker);
                            return;
                        }
                    }
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
