using Microsoft.Extensions.Logging;
using PgWorker.Backups.Drill;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Docker.Drivers;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;

namespace PgWorker.Backups.Process;

/// <summary>
/// RestoreDrillProcess — тиковая машина планового тестового восстановления
/// (reliability t02, arch/19 §3.6) под клэймом &lt;C&gt; (Op backup-drill):
/// супервиз активного drill-джоба pgw-backup-drill-&lt;C&gt;-&lt;X&gt;-&lt;id&gt; (фазы
/// downloading|recovering из логов; exit-код+result-JSON — истина итога),
/// доводимый снос контура фазой cleaning (journal-before-rm; переживает
/// рестарт воркера — идемпотентная доводка по детерминированным именам),
/// отбор «один шард за проход, наименее свежий по дриллу» (DrillPlanner),
/// валидация кандидата — путь restore-заявки t05 (манифест + WalChain),
/// запуск джоба; таймаут-бюджет DrillTimeoutSec → FAILED drill-timeout,
/// RUNNING без контейнера старше бюджета → FAILED drill-vanished.
/// Control — «старт postgres + выход из recovery» (restored_to_lsn);
/// SQL-проверок данных нет. Состояние — один etcd-ключ
/// /pgworker/backups/&lt;C&gt;/&lt;X&gt;/drill (перезаписывается).
/// </summary>
public sealed class RestoreDrillProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ShardEndpoints shardEndpoints,
    IBackupS3 s3,
    ClaimStore claims,
    WorkJournal journal,
    BackupsRuntimeOptions options,
    TimeProvider time,
    ILogger<RestoreDrillProcess> logger)
{
    public const string Op = "backup-drill";

    public async Task<Result<ProcessOutcome>> TickAsync(
        ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;
        _ = s3; // валидация кандидата (манифест/WalChain) — Task 7

        // Guard 1: клэйм наш (мутации /pgworker/backups/* — только держатель).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"{Op} {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // Guard 2: только Active-кластер. Backups:Enabled=false — НЕ выход:
        // блокируются только новые запуски (флаг — в отборе); супервиз
        // активного дрилла и доводка сноса продолжаются (стоп-семантика).
        if (snap.Config.State != ClusterState.Active)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var mine = backups.FirstOrDefault(b => b.Cluster == cluster)
                   ?? new ClusterBackups(cluster, null, new Dictionary<string, ShardBackups>());
        var nowUnix = time.GetUtcNow().ToUnixTimeSeconds();

        // Активности дрилла кластера (по одному за тик, старейший started_unix):
        // RUNNING — супервиз; терминальный с phase=cleaning — доводка сноса
        // (краш-рекавери). Была активность → Done (отбор — следующим тиком).
        var activities = mine.Shards
            .Where(p => p.Value.Drill is { } d
                        && (d.State == DrillStatus.Running || d.Phase == "cleaning"))
            .Select(p => (Shard: p.Key, Drill: p.Value.Drill!))
            .OrderBy(p => p.Drill.StartedUnix)
            .ToList();

        if (activities.Count > 0)
        {
            var (shard, drill) = activities[0];
            if (drill.State == DrillStatus.Running)
                await SuperviseRunningAsync(snap, shard, drill, nowUnix, ct);
            else
                await FinishCleanupAsync(cluster, shard, drill, null, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // ── Супервиз RUNNING-дрилла (arch/19 §3.6 п.2) ──

    private async Task SuperviseRunningAsync(
        ClusterSnapshot snap, string shard, DrillState drill, long nowUnix, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;
        var containerName = BackupNames.DrillContainerName(cluster, shard, drill.Id);

        var engine = await EngineForShardAsync(snap, shard, ct);
        if (engine is null)
        {
            await journal.WritePhaseAsync(cluster, Op, $"transient/{shard}/{drill.Id}",
                claims.InstanceId, "docker-хост не резолвится — статус не трогаем", ct);
            return; // transient — следующий тик
        }

        var prefix = BackupNames.DrillContainerName(cluster, shard, string.Empty);
        var listed = await engine.ListContainersAsync(prefix, all: true, ct);
        if (!listed.IsSuccess)
        {
            await journal.WritePhaseAsync(cluster, Op, $"transient/{shard}/{drill.Id}",
                claims.InstanceId, "docker list недоступен — статус не трогаем", ct);
            return; // transport-сбой list → transient (тик повторит)
        }
        var container = listed.Value.FirstOrDefault(c =>
            c.Names.Any(n => n.TrimStart('/') == containerName));

        // Контейнера нет при успешном list — takeover-аномалия (arch/19 §3.6):
        // возраст < бюджета — transient-ожидание (create подтверждён, а
        // start/list моргнул); старше — FAILED drill-vanished (+ снос volume).
        if (container is null)
        {
            var age = nowUnix - drill.StartedUnix;
            if (SupervisionTimeouts.IsTimedOut(drill.StartedUnix, nowUnix, options.DrillTimeoutSec))
            {
                var vanished = drill with
                {
                    State = DrillStatus.Failed,
                    FinishedUnix = nowUnix,
                    Error = $"drill-vanished: контейнер отсутствует, возраст {age} с > {options.DrillTimeoutSec} с",
                };
                var put = await PutDrillAsync(cluster, shard, vanished, ct);
                if (!put.IsSuccess)
                    return; // transient — статус не сменился
                await FinishCleanupAsync(cluster, shard, vanished, engine, ct);
            }

            return; // младше бюджета — ждём (Task 7: vanish-бюджет)
        }

        // Бюджет активного дрилла — возраст RUNNING-ключа (etcd-факт + часы):
        // исчерпан → FAILED drill-timeout → снос (kill+rm через cleaning).
        if (SupervisionTimeouts.IsTimedOut(drill.StartedUnix, nowUnix, options.DrillTimeoutSec))
        {
            var timedOut = drill with
            {
                State = DrillStatus.Failed,
                FinishedUnix = nowUnix,
                Error = $"drill-timeout: {nowUnix - drill.StartedUnix} с > {options.DrillTimeoutSec} с",
            };
            var put = await PutDrillAsync(cluster, shard, timedOut, ct);
            if (!put.IsSuccess)
                return; // transient — статус не сменился
            await FinishCleanupAsync(cluster, shard, timedOut, engine, ct);
            return;
        }

        if (container.State is "created")
        {
            await engine.StartContainerAsync(containerName, ct); // создан, но не стартован
            return;
        }

        if (container.State is "running" or "restarting")
        {
            // Фаза джоба (downloading|recovering — протокол t05) — в ключ при
            // изменении; иначе ждём (InProgress — тик не блокируется).
            var logs = await engine.GetContainerLogsAsync(containerName, 200, ct);
            if (!logs.IsSuccess)
                return; // transient — фаза недоступна
            var phase = DrillJobLog.Parse(logs.Value).Phase;
            if (phase is { } p && p != drill.Phase)
                await PutDrillAsync(cluster, shard, drill with { Phase = p }, ct); // fail — тик повторит
            return;
        }

        if (container.State != "exited")
            return; // прочие состояния — вне протокола супервиза

        // Итог: exit-код + result-JSON (истина итога, arch/19 §3.5-образец).
        var inspect = await engine.InspectContainerAsync(containerName, ct);
        if (!inspect.IsSuccess)
            return; // transient — итог недоступен
        var exitCode = inspect.Value.ExitCode ?? -1;
        var exitLogs = await engine.GetContainerLogsAsync(containerName, 200, ct);
        if (!exitLogs.IsSuccess)
            return; // transient — итог недоступен
        var result = DrillJobLog.Parse(exitLogs.Value);

        DrillState terminal;
        if (exitCode == 0 && result.Result is { Ok: true })
        {
            // старт postgres + выход из recovery доказаны (restored_to_lsn).
            terminal = drill with
            {
                State = DrillStatus.Succeeded,
                FinishedUnix = nowUnix,
                RestoredToLsn = result.Result.RestoredToLsn,
            };
        }
        else
        {
            terminal = drill with
            {
                State = DrillStatus.Failed,
                FinishedUnix = nowUnix,
                Error = result.Result is { Ok: false, Error: { } err } ? err : $"exit {exitCode}",
            };
        }

        var putTerminal = await PutDrillAsync(cluster, shard, terminal, ct);
        if (!putTerminal.IsSuccess)
            return; // transient — статус не сменился, вердикт следующим тиком
        await FinishCleanupAsync(cluster, shard, terminal, engine, ct);
    }

    // ── Доводимый снос контура после терминального исхода (все пути сходятся) ──

    private async Task FinishCleanupAsync(
        string cluster, string shard, DrillState drill, IDockerEngine? engine, CancellationToken ct)
    {
        if (engine is null)
        {
            // cleanup-путь краш-рекавери без snap (тик активностей): хост —
            // первый из таблицы Docker:Hosts (drill-джоб мог жить на любом).
            var hosts = await driver.GetHostsAsync(ct);
            var first = hosts.IsSuccess ? hosts.Value.FirstOrDefault() : null;
            engine = first is null ? null : driver.EngineFor(first.Name);
            if (engine is null)
                return; // transient — ключ остаётся как есть, следующий тик
        }

        var containerName = BackupNames.DrillContainerName(cluster, shard, drill.Id);

        // journal-before-manipulations + видимая фаза сноса в etcd (spec §2.5):
        // «что происходит» — ДО rm.
        if (drill.Phase != "cleaning")
        {
            await journal.WritePhaseAsync(cluster, Op, $"drill-cleanup/{shard}/{drill.Id}",
                claims.InstanceId, null, ct);
            var putCleaning = await PutDrillAsync(cluster, shard, drill with
            {
                Phase = "cleaning",
                FinishedUnix = drill.FinishedUnix ?? NowUnix(),
            }, ct);
            if (!putCleaning.IsSuccess)
                return; // transient — фаза не записана, снос следующим тиком
        }

        // rm контейнера (force) и volume — одно имя, 404 = ок (идемпотентность).
        var rmContainer = await engine.RemoveContainerAsync(containerName, force: true, ct);
        var rmVolume = await engine.RemoveVolumeAsync(
            BackupNames.DrillVolumeName(cluster, shard, drill.Id), ct);

        // Подтверждение: контейнера нет (повторный list пуст) → чистый итог;
        // list-fail / rm не прошёл / не пусто → ключ остаётся с cleaning,
        // следующий тик повторяет (идемпотентно, переживает рестарт воркера).
        var confirm = await engine.ListContainersAsync(containerName, all: true, ct);
        if (!confirm.IsSuccess || confirm.Value.Count > 0 || !rmVolume.IsSuccess)
            return;

        var clean = drill with { Phase = null };
        var put = await PutDrillAsync(cluster, shard, clean, ct);
        if (!put.IsSuccess)
            return; // cleaning-фаза осталась — следующий тик перепишет (идемпотентно)
        await journal.WritePhaseAsync(cluster, Op,
            $"{(drill.State == DrillStatus.Succeeded ? "drill-done" : "drill-failed")}/{shard}/{drill.Id}",
            claims.InstanceId, null, ct);
        logger.LogInformation("{Op} {cluster}/{shard}: дрилл {id} — {state}, контур снесён",
            Op, cluster, shard, drill.Id, drill.State);
    }

    // Docker-хост джоба: первая нода шарда из portalloc (node-факт); шард/ноды
    // исчезли → fallback первый хост таблицы Docker:Hosts (образец verify).
    private async Task<IDockerEngine?> EngineForShardAsync(
        ClusterSnapshot snap, string shard, CancellationToken ct)
    {
        var addresses = await shardEndpoints.ReadPortAllocAsync(snap.Config.Cluster, ct);
        var firstNode = snap.Shards
            .FirstOrDefault(s => s.Name == shard)?.Nodes
            .Select(n => n.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .FirstOrDefault();
        if (addresses.IsSuccess && firstNode is not null
            && addresses.Value.TryGetValue($"{shard}/{firstNode}", out var addr))
            return driver.EngineFor(addr.Host);

        var hosts = await driver.GetHostsAsync(ct); // таблица Docker:Hosts (канон)
        var first = hosts.IsSuccess ? hosts.Value.FirstOrDefault() : null;
        return first is null ? null : driver.EngineFor(first.Name);
    }

    // Failover-put ключа дрилла (первый успешный endpoint — образец verify).
    private async Task<Result> PutDrillAsync(string cluster, string shard, DrillState drill, CancellationToken ct)
    {
        var value = DrillStatusJson.Serialize(drill);
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.PutAsync(endpoint, BackupNames.DrillKey(cluster, shard), value, null, ct);
            if (result.IsSuccess)
                return result;
        }

        return Result.Failed(new ApplicationException($"etcd put drill {cluster}/{shard} — все endpoints недоступны"));
    }

    private long NowUnix() => time.GetUtcNow().ToUnixTimeSeconds();
}
