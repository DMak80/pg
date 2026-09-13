using AdminPanel.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Probes.S3;

// Фоновый инвентарь-тик MinIO (t08, adminpanel/02 §2.5, spec §4.3): раз в
// IntervalSec — health + ListBuckets + ОДИН полный list-v2 bucket постранично →
// MinioInventory.Build → атомарная замена в сторе (KV-тик refresher'а не
// блокируется). Сбой любого шага: ConsecutiveFailures +1, прежний инвентарь
// живёт (устаревающий, штамп UpdatedAtUnix); успех — счётчик 0.
// Пустой Endpoint — грань выключена: ExecuteAsync завершается сразу (AC1).
public sealed class MinioInventoryLoop(
    IMinioS3 client,
    IMinioInventoryStore store,
    IOptions<MinioOptions> options,
    TimeProvider time,
    ILogger<MinioInventoryLoop> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!options.Value.IsConfigured)
        {
            logger.LogInformation(
                "AdminPanel:Backups:S3:Endpoint не задан — MinIO-грань выключена, инвентарь-тик не запускается");
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
        if (!config.IsConfigured)
            return; // выключенная грань — no-op, клиент не зовётся (AC1)

        // Маркер configured до первого тика (Решение 4): API различает «не
        // настроено» (MinioStorage null) и «настроено, инвентарь ещё собирается».
        if (store.Current is null)
        {
            store.Replace(new MinioStorageInfo(
                Configured: true, config.S3.Endpoint, config.S3.Bucket,
                Health: null, Buckets: [], UsedBytes: 0, ObjectCount: 0,
                Clusters: [], ForeignPrefixes: [], UpdatedAtUnix: 0,
                ConsecutiveFailures: 0, LastError: null));
        }

        var at = time.GetUtcNow().ToUnixTimeSeconds();
        try
        {
            // (1) health (2) buckets (3) полный list-v2 постранично (4) агрегация.
            var health = await client.GetHealthAsync(ct);
            var buckets = await client.ListBucketsAsync(ct);
            if (!buckets.IsSuccess)
                throw new ApplicationException(buckets.Error!.Message, buckets.Error);

            var objects = new List<MinioObject>();
            string? token = null;
            do
            {
                var page = await client.ListPageAsync(null, token, 1000, ct);
                if (!page.IsSuccess)
                    throw new ApplicationException(page.Error!.Message, page.Error);
                objects.AddRange(page.Value.Items.Select(i =>
                    new MinioObject(i.Key, i.SizeBytes, i.LastModifiedUnix)));
                token = page.Value.NextContinuationToken;
            }
            while (token is not null);

            var inventory = MinioInventory.Build(objects);
            logger.LogInformation(
                "AdminPanel:Backups: инвентарь MinIO обновлён — {Clusters} кластеров, {Bytes} байт, {Objects} объектов",
                inventory.Clusters.Count, inventory.UsedBytes, inventory.ObjectCount);

            store.Replace(new MinioStorageInfo(
                Configured: true, config.S3.Endpoint, config.S3.Bucket,
                health, buckets.Value, inventory.UsedBytes, inventory.ObjectCount,
                inventory.Clusters, inventory.ForeignPrefixes, at,
                ConsecutiveFailures: 0, LastError: null));
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            // Сбой тика: счётчик +1, прежние данные/штамп живут (аналог FailTick
            // refresher'а); маркер Решения 4 (стор был пуст) остаётся с UpdatedAtUnix=0.
            var previous = store.Current;
            logger.LogWarning(e, "AdminPanel:Backups: инвентарь-тик MinIO не удался ({Failures} подряд)",
                (previous?.ConsecutiveFailures ?? 0) + 1);
            store.Replace(previous is null
                ? new MinioStorageInfo(
                    Configured: true, config.S3.Endpoint, config.S3.Bucket,
                    Health: null, Buckets: [], UsedBytes: 0, ObjectCount: 0,
                    Clusters: [], ForeignPrefixes: [], UpdatedAtUnix: 0,
                    ConsecutiveFailures: 1, LastError: e.Message)
                : previous with
                {
                    ConsecutiveFailures = previous.ConsecutiveFailures + 1,
                    LastError = e.Message,
                });
        }
    }
}
