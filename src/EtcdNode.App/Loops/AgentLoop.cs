using EtcdNode.App;
using EtcdNode.Core;
using Shared.Core;
using Shared.Etcd.Client;
using Shared.Etcd.Coordination;
using Shared.Etcd.Maintenance;

namespace EtcdNode.App.Loops;

// Единственный цикл агента EtcdNode (arch/etcdnode/processes/c-snapshot-export.md,
// arch/etcdnode/01-agent.md §2/§4): лидер ClaimStore исполняет тик C1–C5
// (доводка → решение с фильтром важности → снятие со СВОЕГО localhost →
// выгрузка → обслуживание контура → повторный расчёт отставания); не-лидер —
// попытка захвата и сон скана. Координация localhost-only: отказ своего etcd
// у лидера — тик работу не исполняет, статус НЕ пишется и лидерство НЕ
// релизится (ветка F) — lease гаснет по TTL ≤ 15 с, работу пишет новый лидер.
internal sealed class AgentLoop(
    EtcdNodeOptions options,
    ClaimStore claims,
    IEtcdGateway etcd,
    TimeProvider clock,
    ILogger<AgentLoop> logger,
    Shared.Metrics.Worker.WorkerMetricsInstrumentation metrics) : BackgroundService
{
    // Свой узел — всегда первый (координация/снятие/статус, arch/etcdnode/01-agent.md §2).
    private const string Localhost = "http://localhost:2379";

    // Локальный том слепков СВОЕГО узла (volume etcdw-node-<k>-snapshots).
    private const string SnapshotsDir = "/snapshots";

    // Сон тика здорового контура (детекция изменений каждый тик); watchdog
    // у агента нет — пульсирующий сон не нужен.
    private static readonly TimeSpan ScanInterval = TimeSpan.FromSeconds(5);

    // Эталонная карта key→mod_revision последней успешной выгрузки (память
    // процесса лидера, arch/etcdnode/02-snapshots.md §3): нет карты (старт/
    // смена лидера) — первый тик внеочередной (RPO-консервативно).
    private Dictionary<string, long>? _baseline;
    private long? _baselineRevision;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            // Постановка instance-ключа живости + фоновый keepalive-цикл 5 с
            // (без него lease лидерства не продлевается и ключи живости не
            // ставятся, ClaimStore.StartAsync).
            await claims.StartAsync(stoppingToken);
            while (!stoppingToken.IsCancellationRequested)
            {
                if (!claims.IsLeader)
                {
                    var became = await claims.TryBecomeLeaderAsync(stoppingToken);
                    if (became.IsSuccess && became.Value)
                        logger.LogInformation("etcdnode leadership acquired: {InstanceId}", claims.InstanceId);
                }

                if (claims.IsLeader)
                {
                    try
                    {
                        // Cancellation-гвард лидерства (паттерн S5): потеря
                        // leader-lease прерывает тик и сон тика.
                        using var leaderLease = CancellationTokenSource.CreateLinkedTokenSource(
                            stoppingToken, claims.LeaderToken);
                        var behind = await TickLeaderAsync(leaderLease.Token);

                        // C5-сон: отставание/FAILED — RetryIntervalSec (RPO-окно
                        // транзиента закрывается минутами), здоровый контур —
                        // короткий скан.
                        var sleep = behind
                            ? TimeSpan.FromSeconds(options.Snapshots.RetryIntervalSec)
                            : ScanInterval;
                        await Task.Delay(sleep, leaderLease.Token);
                    }
                    catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
                    {
                        // Потеря leader-lease прервала тик: не ошибка — цикл
                        // продолжит как не-лидер и пере-захватит ≤ TTL + тик скана.
                        logger.LogWarning("etcdnode leadership lost mid-tick");
                    }
                }
                else
                {
                    // Не лидер: сон скана до следующей попытки захвата.
                    await Task.Delay(ScanInterval, stoppingToken);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // штатная остановка host'а
        }
        finally
        {
            // Ревок lease'ов (живость/лидерство) при штатной остановке агента.
            await claims.DisposeAsync();
        }
    }

    // Тик лидера C1–C5. Возврат — отставание выгрузки (сон RetryIntervalSec).
    private async Task<bool> TickLeaderAsync(CancellationToken ct)
    {
        // Список endpoints тика (localhost первым + client-грани членов из
        // member list): статус/экспорт — failover по нему; перечитывается
        // каждый тик — актуален без отдельной конфигурации.
        var endpoints = await TickEndpointsAsync(ct);
        var snapshotOpts = options.Snapshots;

        // Экспортёр per-тик: список endpoints обновляется каждый тик.
        var exporter = new SnapshotExporter(
            etcd, endpoints,
            new EtcdBackupTargetResolver(etcd, endpoints),
            t => new EtcdSnapshotS3(
                t.Endpoints[0], t.Bucket, t.AccessKey, t.SecretKey,
                options.S3CaPemPath, TimeSpan.FromSeconds(snapshotOpts.TimeoutSec)),
            snapshotOpts.RetentionObjects, snapshotOpts.TimeoutSec,
            claims.InstanceId, snapshotOpts.IntervalMin);

        // C1: доводка отстающей выгрузки ДО снятия (транзиент цели не
        // растягивает RPO-окно); отказ — считаем отставание есть.
        var catchUp = await exporter.CatchUpAsync(SnapshotsDir, ct);
        var behind = catchUp.IsSuccess ? catchUp.Value : true;

        // C2: решение снятия — чистое расписание с фильтром важности.
        var latest = SnapshotExporter.LatestLocalFile(SnapshotsDir);
        var lastTakeUnix = EtcdSnapshotStatus.TakenUnixFromName(
            latest is null ? "" : Path.GetFileName(latest));
        var status = await exporter.ReadStatusAsync(ct);
        var lastUploadedUnix = status.IsSuccess ? status.Value?.LastUploadedUnix : null;
        var statusPayload = await etcd.StatusAsync(Localhost, ct);
        var currentRevision = statusPayload.IsSuccess && statusPayload.Value?.Revision is { } revision
            ? (long)revision
            : 0;

        // Дифф карты — ТОЛЬКО при росте revision (дешёвый гейт тика):
        // нет эталона — важно; range не получился — консервативно важно
        // (карты нет — сравнивать нечем, снимаем).
        Dictionary<string, long>? currentMap = null;
        var diffImportant = true;
        if (_baseline is not null && currentRevision <= _baselineRevision)
            diffImportant = false;
        else if (_baseline is not null)
        {
            currentMap = await ReadMapAsync(ct);
            diffImportant = currentMap is null || ChangeMapDiff.IsImportant(_baseline, currentMap);
        }

        var take = SnapshotSchedule.ShouldTake(
            clock.GetUtcNow().ToUnixTimeSeconds(),
            lastTakeUnix, lastUploadedUnix,
            currentRevision, _baselineRevision, diffImportant,
            snapshotOpts.IntervalMin, snapshotOpts.MinIntervalSec);

        if (take)
        {
            // C3: снятие — SnapshotJob с ровно ОДНИМ endpoint (свой узел,
            // решение №13): compact+defrag тут не выполняется.
            var shot = await new SnapshotJob(etcd, [Localhost], SnapshotsDir,
                snapshotOpts.RetentionFiles, snapshotOpts.MaintenanceIntervalMin).TakeAsync(ct);
            if (!shot.IsSuccess)
            {
                // Ветка F (arch/etcdnode/01-agent.md §2): свой etcd недоступен —
                // работу НЕ исполняем, статус НЕ пишется и лидерство НЕ
                // релизится: координация localhost-only, lease гаснет по
                // TTL ≤ 15 с — работу и статус пишет новый лидер.
                logger.LogWarning(shot.Error, "etcd snapshot failed (own etcd): {Message}",
                    shot.Error!.Message);
                return true; // короткий сон RetryIntervalSec
            }

            metrics.SnapshotTaken(clock.GetUtcNow());
            logger.LogInformation("etcd snapshot taken: {Path}", shot.Value);

            var data = await File.ReadAllBytesAsync(shot.Value, ct);
            var exported = await exporter.ExportAsync(
                Path.GetFileName(shot.Value), data,
                statusPayload.IsSuccess && statusPayload.Value?.Revision is { } takenRevision
                    ? (long?)takenRevision
                    : null,
                ct);
            if (exported.IsSuccess)
            {
                // Эталон = карта этого тика; дифф не выполнялся (плановый/
                // тишь по revision) — берём range заново после выгрузки.
                // Отказ range — эталон остаётся прежним (консервативно).
                _baseline = currentMap ?? await ReadMapAsync(ct) ?? _baseline;
                if (_baseline is not null)
                    _baselineRevision = currentRevision;
            }
            else
            {
                // Отказ выгрузки — домен статус-ключа (FAILED пишет экспортёр),
                // НЕ цикла: снятие прошло, слепок в локальном томе, цикл здоров.
                logger.LogError(exported.Error, "etcd snapshot export failed: {Message}",
                    exported.Error!.Message);
            }

            // C4: обслуживание контура — ОТДЕЛЬНЫЙ вызов SnapshotJob с полным
            // списком endpoints тика (compact+defrag ПО ВСЕМ членам, канон
            // arch/etcdnode/02-snapshots.md §5); не чаще MaintenanceIntervalMin;
            // отказ — warning, тик не фейлит.
            var maintenance = await new SnapshotJob(etcd, endpoints, SnapshotsDir,
                snapshotOpts.RetentionFiles, snapshotOpts.MaintenanceIntervalMin).MaintainAsync(ct);
            if (!maintenance.IsSuccess)
                logger.LogWarning(maintenance.Error, "etcd maintenance failed: {Message}",
                    maintenance.Error!.Message);
        }

        // C5: повторный расчёт отставания ПОСЛЕ снятия — первый транзиент мог
        // увести статус в FAILED уже после расчёта «до»; без него цикл ушёл бы
        // в длинный сон с невыгруженным слепком.
        var fresh = await exporter.ReadStatusAsync(ct);
        var freshLatest = SnapshotExporter.LatestLocalFile(SnapshotsDir);
        behind = !fresh.IsSuccess
                 || EtcdSnapshotStatus.IsBehind(
                     fresh.Value,
                     EtcdSnapshotStatus.TakenUnixFromName(
                         freshLatest is null ? "" : Path.GetFileName(freshLatest)));

        metrics.LoopTick("snapshot", ok: true);
        return behind;
    }

    // Список endpoints тика: localhost первым + client-грани членов из
    // member list (имена сети контура); отказ member-list — только localhost.
    private async Task<string[]> TickEndpointsAsync(CancellationToken ct)
    {
        var members = await etcd.MemberListAsync(Localhost, ct);
        if (!members.IsSuccess)
            return [Localhost];
        var memberEndpoints = members.Value
            .Where(m => m.Name is { Length: > 0 })
            .Select(m => $"http://{m.Name}:2379")
            .Distinct(StringComparer.Ordinal);
        return memberEndpoints.Prepend(Localhost).ToArray();
    }

    // Полная карта key→mod_revision (range корня одним запросом; kanon
    // arch/etcdnode/02-snapshots.md §3); отказ — null.
    private async Task<Dictionary<string, long>?> ReadMapAsync(CancellationToken ct)
    {
        var range = await etcd.RangeAsync(Localhost, "/", ct);
        return range.IsSuccess
            ? range.Value.ToDictionary(kv => kv.Key, kv => (long)kv.ModRevision)
            : null;
    }
}
