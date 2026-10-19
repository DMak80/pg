using AdminPanel.Core;
using AdminPanel.Etcd;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Probes.S3;

// Фоновый S3-инвентарь-тик (t08, adminpanel/02 §2.5, spec §4.3): раз в
// IntervalSec — health + ListBuckets + ОДИН полный list-v2 bucket постранично →
// S3Inventory.Build → атомарная замена в сторе (KV-тик refresher'а не
// блокируется). Сбой любого шага: ConsecutiveFailures +1, прежний инвентарь
// живёт (устаревающий, штамп UpdatedAtUnix); успех — счётчик 0.
// Нет пула в /s3/clusters/ — грань выключена: ExecuteAsync завершается сразу.
public sealed class S3InventoryLoop(
    IS3StorageClient client,
    IS3InventoryStore store,
    IOptions<BackupsS3Options> options,
    IS3SnapshotReader s3Snapshot,
    TimeProvider time,
    ILogger<S3InventoryLoop> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (s3Snapshot.Current is null || S3TargetsResolver.Resolve(s3Snapshot.Current) is null)
        {
            logger.LogInformation(
                "AdminPanel:Backups: S3 backups pool not found in /s3/clusters/ — facet disabled, inventory tick not started");
            return;
        }

        // Fail-fast старта уже отсёк IntervalSec <= 0 при настроенной грани;
        // fallback — на случай программной сборки без валидации.
        var seconds = options.Value.IntervalSec;
        if (seconds <= 0)
            seconds = 60;

        // Первый тик сразу (образец ProbeOrchestrator/SnapshotRefresher), далее по периоду.
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(seconds));
        do
        {
            try
            {
                await RunOnceAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }

    // Ядро тика — публично для unit/integration-тестов без хоста.
    public async Task RunOnceAsync(CancellationToken ct)
    {
        var config = options.Value;

        // Цели грани — из снапшота S3-домена (arch/adminpanel/02 §12.4): клиент
        // перенацеливается КАЖДЫМ тиком (ротация кредов — следующий тик); пула
        // нет — грань не настроена, no-op, клиент не зовётся.
        var current = s3Snapshot.Current;
        var targets = current is null ? null : S3TargetsResolver.Resolve(current);
        client.UpdateTargets(targets);
        if (targets is null)
            return;

        // Маркер configured до первого тика (Решение 4): API различает «не
        // настроено» (S3Storage null) и «настроено, инвентарь ещё собирается».
        if (store.Current is null)
        {
            store.Replace(new S3StorageInfo(
                Configured: true, targets.Endpoints, config.S3.Bucket,
                Health: null, Buckets: [], UsedBytes: 0, ObjectCount: 0,
                Clusters: [], ForeignPrefixes: [], UpdatedAtUnix: 0,
                ConsecutiveFailures: 0, LastError: null));
        }

        var at = time.GetUtcNow().ToUnixTimeSeconds();

        // (1) health — факт доступности на ЭТОТ тик: реальный клиент не бросает
        // (пробы гасят исключения в S3Health); AC3: Health в сторе обновляется
        // КАЖДЫМ тиком — у остановленного хранилища apiOk/liveOk=false, при этом
        // инвентарь остаётся прежним (устаревающим, штамп не растёт).
        S3Health health;
        try
        {
            health = await client.GetHealthAsync(ct);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            FailTick(e.Message, health: null, targets);
            return;
        }

        try
        {
            // (2) buckets (3) полный list-v2 постранично (4) агрегация.
            var buckets = await client.ListBucketsAsync(ct);
            if (!buckets.IsSuccess)
                throw new ApplicationException(buckets.Error!.Message, buckets.Error);

            var objects = new List<S3InventoryObject>();
            string? token = null;
            do
            {
                var page = await client.ListPageAsync(null, token, 1000, ct);
                if (!page.IsSuccess)
                    throw new ApplicationException(page.Error!.Message, page.Error);
                objects.AddRange(page.Value.Items.Select(i =>
                    new S3InventoryObject(i.Key, i.SizeBytes, i.LastModifiedUnix)));
                token = page.Value.NextContinuationToken;
            }
            while (token is not null);

            var inventory = S3Inventory.Build(objects);
            logger.LogInformation(
                "AdminPanel:Backups: S3 inventory updated — {Clusters} clusters, {Bytes} bytes, {Objects} objects",
                inventory.Clusters.Count, inventory.UsedBytes, inventory.ObjectCount);

            store.Replace(new S3StorageInfo(
                Configured: true, targets.Endpoints, config.S3.Bucket,
                health, buckets.Value, inventory.UsedBytes, inventory.ObjectCount,
                inventory.Clusters, inventory.ForeignPrefixes, at,
                ConsecutiveFailures: 0, LastError: null));
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            FailTick(e.Message, health, targets);
        }
    }

    // Сбойный тик: счётчик +1, Health — факт текущей пробы (AC3; null — проба
    // сама бросила), данные/штамп прежние (аналог FailTick refresher'а); маркер
    // Решения 4 (стор был пуст) остаётся с UpdatedAtUnix=0.
    private void FailTick(string message, S3Health? health, S3StorageTargets targets)
    {
        var previous = store.Current;
        logger.LogWarning(
            "AdminPanel:Backups: S3 inventory tick failed ({Failures} in a row): {Error}",
            (previous?.ConsecutiveFailures ?? 0) + 1, message);
        store.Replace(
            previous is null
                ? new S3StorageInfo(
                    Configured: true, targets.Endpoints, options.Value.S3.Bucket,
                    Health: health, Buckets: [], UsedBytes: 0, ObjectCount: 0,
                    Clusters: [], ForeignPrefixes: [], UpdatedAtUnix: 0,
                    ConsecutiveFailures: 1, LastError: message)
                : previous with
                {
                    Health = health ?? previous.Health,
                    ConsecutiveFailures = previous.ConsecutiveFailures + 1,
                    LastError = message,
                });
    }
}
