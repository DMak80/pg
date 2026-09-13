using Microsoft.Extensions.Logging;
using PgWorker.Backups.Job;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Docker.Drivers;
using PgWorker.Docker.Engine;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;

namespace PgWorker.Backups;

/// <summary>
/// BackupVerifyProcess — тиковая машина проверки полных бэкапов (t04, arch/19
/// §5/§8) под клэймом &lt;C&gt;: due-резолв кандидатов (PENDING от on_create /
/// периодика по verify.interval_sec; FAILED терминален) → цепочечная проверка
/// list-ами S3 + строгий LSN-разбор .history (WalChain.CheckRange + WalHistory,
/// без скачивания сегментов) → ephemeral verify-джоб pgw-backup-verify-&lt;C&gt;-&lt;X&gt;-&lt;id&gt;
/// (inline bash mc cp + pg_verifybackup, протокол t02: result-JSON + exit-код)
/// на docker-хосте источника. Тик не блокируется на длинную проверку: итог —
/// супервизом следующих тиков (задача 9). Transient (S3/docker/скачивание) —
/// статус кандидата не трогаем; permanent (дыра цепочки, нет wal_start) — FAILED
/// сразу, джоб checksums не запускается (вердикт уже определён).
/// </summary>
public sealed class BackupVerifyProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ShardEndpoints shardEndpoints,
    IBackupS3 s3,
    ClaimStore claims,
    WorkJournal journal,
    BackupsRuntimeOptions options,
    TimeProvider time,
    ILogger<BackupVerifyProcess> logger,
    Action<string, string, string>? verifyObserver = null) // (cluster, shard, result: ok|failed|transient)
{
    private const string Op = "backup-verify";

    public async Task<Result<ProcessOutcome>> TickAsync(
        ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Guard 1: клэйм наш (мутации /pgworker/backups/* — только держатель).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"{Op} {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // Guard 2: Backups:Enabled=false → no-op (идущие ephemeral-джобы не убиваем).
        if (!options.Enabled)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        // Guard 3: только Active-кластер.
        if (snap.Config.State != ClusterState.Active)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var mine = backups.FirstOrDefault(b => b.Cluster == cluster)
                   ?? new ClusterBackups(cluster, null, new Dictionary<string, ShardBackups>());
        var intervalSec = mine.Policy?.VerifyIntervalSec ?? options.VerifyIntervalSec;
        var nowUnix = time.GetUtcNow().ToUnixTimeSeconds();

        // Адреса нод один раз на тик (portalloc) — хост джоба (node-факт).
        var addresses = await shardEndpoints.ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return Result<ProcessOutcome>.Failed(addresses.Error!);

        foreach (var shard in snap.Shards.Where(s => !s.ToRemove))
        {
            // шард без ключей полных — пропускается (dsn не нужен: verify чисто по S3)
            if (!mine.Shards.TryGetValue(shard.Name, out var shardBackups) || shardBackups.Full.Count == 0)
                continue;

            try
            {
                await TickShardAsync(cluster, shard.Name, shardBackups, addresses.Value, intervalSec, nowUnix, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                // ошибка шарда не роняет остальные (arch/17)
                logger.LogError(ex, "{Op} {cluster}/{shard}: сбой тика шарда", Op, cluster, shard.Name);
                await journal.WritePhaseAsync(cluster, Op, "shard-error", claims.InstanceId,
                    $"{shard.Name}: {ex.Message}", ct);
            }
        }

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    private async Task TickShardAsync(
        string cluster, string shard, ShardBackups shardBackups,
        IReadOnlyDictionary<string, NodeAddress> addresses, long intervalSec, long nowUnix, CancellationToken ct)
    {
        var fulls = shardBackups.Full;

        // (1) Супервиз verify-контейнеров шарда: есть хоть один (running/exited) —
        //     обрабатываем и ЗАВЕРШАЕМ тик шарда — новых не стартуем (инвариант
        //     одного джоба, spec §3.3 п.1). Лист — движок первого хоста таблицы
        //     Docker:Hosts; сбой → transient (тик повторит).
        var shardPrefix = BackupNames.VerifyContainerName(cluster, shard, string.Empty);
        var hostsForList = await driver.GetHostsAsync(ct);
        IDockerEngine? listEngine = hostsForList.IsSuccess && hostsForList.Value.Count > 0
            ? driver.EngineFor(hostsForList.Value[0].Name)
            : null;
        if (listEngine is null)
            return; // хостов нет/движок не резолвится — transient
        var alive = await listEngine.ListContainersAsync(shardPrefix, all: true, ct);
        if (!alive.IsSuccess)
            return; // transport-сбой list → transient (тик повторит)
        if (alive.Value.Count > 0)
        {
            foreach (var container in alive.Value)
            {
                var name = container.Names
                    .Select(n => n.TrimStart('/'))
                    .FirstOrDefault(n => n.StartsWith(shardPrefix, StringComparison.Ordinal));
                if (name is not null)
                    await SuperviseAsync(
                        listEngine, cluster, shard, name, shardPrefix, container.State, shardBackups, nowUnix, ct);
            }

            return; // супервиз-тик: новых джобов не стартуем
        }

        // (2) due-резолв: PENDING-очередь раньше периодики; по одному за тик.
        //     verify FAILED — терминален: не попадает ни в один список.
        var pending = fulls
            .Where(f => f.State == FullBackupStatus.Completed
                        && f.Verify is { State: BackupVerifyStatus.Pending }
                        // t07: checked_unix после verify-таймаута — квота попытки:
                        // повторный due через verify.interval_sec (лив-лок немедленных
                        // перезапусков исключён); null — on_create, due сразу;
                        // interval <= 0 — таймаутнувшийся кандидат не перезапускается
                        // автоматически (периодика выключена).
                        && (f.Verify.CheckedUnix is null
                            || intervalSec > 0 && nowUnix - f.Verify.CheckedUnix.Value > intervalSec))
            .OrderBy(f => f.StartedUnix)
            .ToList();
        var periodic = intervalSec > 0
            ? fulls
                .Where(f => f.State == FullBackupStatus.Completed
                            && (f.Verify is null
                                || f.Verify is { State: BackupVerifyStatus.Ok, CheckedUnix: var c }
                                && nowUnix - (c ?? 0) > intervalSec))
                .OrderBy(f => f.Verify?.CheckedUnix ?? 0)
                .ToList()
            : [];
        var candidate = pending.FirstOrDefault() ?? periodic.FirstOrDefault();
        if (candidate is null)
            return; // нет due — нечего проверять

        var id = candidate.Id;

        // (3) wal_start_segment отсутствует у COMPLETED — аномалия: permanent FAILED.
        if (string.IsNullOrEmpty(candidate.WalStartSegment))
        {
            await WritePermanentAsync(cluster, shard, candidate,
                "нет wal_start_segment — набор не.archiveable (нет точки старта цепочки)", nowUnix, ct);
            return;
        }

        // (4) Цепочка: list wal/ + list набора + строгие TLI-переходы (по содержимому
        //     history при наличии). Transient list/GET → шард-skip, статус не трогаем.
        var walList = await s3.ListAsync(cluster, shard, "wal/", ct: ct);
        if (!walList.IsSuccess)
        {
            observe(cluster, shard, "transient");
            return;
        }

        var setList = await s3.ListAsync(cluster, shard, $"full/{id}/pg_wal/", ct: ct);
        if (!setList.IsSuccess)
        {
            observe(cluster, shard, "transient");
            return;
        }

        var start = WalFileName.TryParse(candidate.WalStartSegment);
        if (start is null)
        {
            await WritePermanentAsync(cluster, shard, candidate,
                $"битый wal_start_segment {candidate.WalStartSegment}", nowUnix, ct);
            return;
        }

        var setSegments = setList.Value
            .Select(o => WalFileName.TryParse(o.Name))
            .OfType<WalFileName>()
            .ToList();
        if (setSegments.Count == 0)
        {
            await WritePermanentAsync(cluster, shard, candidate,
                "в наборе нет WAL-сегментов pg_wal", nowUnix, ct);
            return;
        }

        var endSegment = setSegments
            .OrderByDescending(s => (s.Tli, s.Log, s.Seg))
            .First();

        // Содержимое history для переходов диапазона (tli > start.Tli && tli <= end.Tli).
        var historyContents = new Dictionary<uint, string>();
        foreach (var obj in walList.Value)
        {
            if (WalFileName.TryParseHistory(obj.Name) is not { } tli
                || tli <= start.Value.Tli || tli > endSegment.Tli)
                continue;
            var content = await s3.GetObjectAsync(cluster, shard, $"wal/{obj.Name}", ct);
            if (!content.IsSuccess)
            {
                observe(cluster, shard, "transient");
                return;
            }

            historyContents[tli] = content.Value;
        }

        var check = WalChain.CheckRange(
            start.Value, endSegment, walList.Value.Select(o => o.Name), historyContents);
        if (!check.IsContinuous)
        {
            await WritePermanentAsync(cluster, shard, candidate,
                check.GapError ?? "цепочка WAL не проходит проверку", nowUnix, ct);
            return;
        }

        // (5) Джоб: docker-хост источника (node-факт portalloc; fallback — первый
        //     хост таблицы Docker:Hosts с journal-фактом выбора).
        var (engine, fallbackHost) = await EngineForShardAsync(cluster, shard, addresses, $"{shard}/{candidate.Node}", ct);
        if (engine is null)
        {
            observe(cluster, shard, "transient"); // хост не известен — следующий тик
            return;
        }

        var spec = VerifyJobSpec.Build(options, cluster, shard, id);
        var jobName = BackupNames.VerifyContainerName(cluster, shard, id);
        var created = await engine.CreateContainerAsync(spec, jobName, ct);
        if (!created.IsSuccess)
        {
            observe(cluster, shard, "transient");
            return;
        }

        var started = await engine.StartContainerAsync(jobName, ct);
        if (!started.IsSuccess)
        {
            observe(cluster, shard, "transient"); // PENDING остаётся — идемпотентный перезапуск
            return;
        }

        // journal-факты: started — факт запуска; при fallback-пути резюме тика —
        // выбор fallback-хоста (журнал — единый ключ последней фазы: резюме пишем
        // ПОСЛЕДНИМ, чтобы факт выбора не затирался started).
        await journal.WritePhaseAsync(cluster, Op, $"started/{shard}/{id}", claims.InstanceId, null, ct);
        if (fallbackHost is not null)
            await journal.WritePhaseAsync(cluster, Op, $"engine-fallback/{shard}",
                claims.InstanceId, fallbackHost, ct);
        logger.LogInformation("{Op} {cluster}/{shard}: verify-джоб {job} запущен (id {id})",
            Op, cluster, shard, jobName, id);
    }

    // Супервиз одного verify-контейнера шарда (spec §3.2): orphan/stale → снос
    // без итога; running → ждать; created → старт; exited → итог по exit-коду и
    // result-JSON (фаза verify/нет результата с ok:false+phase=verify → permanent;
    // download/мусор → transient: снос, статус не трогаем); итог OK/FAILED —
    // полная перезапись ключа + журнал + снос джоба.
    private async Task SuperviseAsync(
        IDockerEngine engine, string cluster, string shard, string containerName, string shardPrefix,
        string state, ShardBackups shardBackups, long nowUnix, CancellationToken ct)
    {
        var id = containerName[shardPrefix.Length..];
        var full = shardBackups.Full.FirstOrDefault(f => f.Id == id);

        // orphan/stale (ключ ушёл — deprovisioning-гонка; внешний FAILED): итог НЕ
        // пишем, объект сносим. verify null|PENDING|OK — легитимный кандидат джоба.
        if (full is null
            || full.State != FullBackupStatus.Completed
            || full.Verify is { State: BackupVerifyStatus.Failed })
        {
            await CleanupJobAsync(engine, containerName, ct);
            return;
        }

        if (state is "running" or "restarting")
        {
            // t07 (arch/19 §6): возраст running-джоба — docker-факт StartedAt
            // (в etcd-статусе кандидата времени запуска нет). Бюджет исчерпан →
            // kill+rm; вердикт FAILED НЕ ставим (данные не виноваты) — попытка
            // зачитывается checked_unix=now: следующий due через verify.interval_sec,
            // лив-лок немедленных перезапусков исключён.
            var inspectForAge = await engine.InspectContainerAsync(containerName, ct);
            if (inspectForAge.IsSuccess
                && inspectForAge.Value.StartedAtUnix is { } started
                && SupervisionTimeouts.IsTimedOut(started, nowUnix, options.JobVerifyTimeoutSec))
            {
                var quota = full.Verify is null
                    ? new BackupVerify(BackupVerifyStatus.Pending, nowUnix)
                    : full.Verify with { CheckedUnix = nowUnix };
                var putQuota = await PutAsync(BackupNames.FullKey(cluster, shard, full.Id),
                    BackupStatusJson.Serialize(full with { Verify = quota }), ct);
                if (!putQuota.IsSuccess)
                    return; // transient — статус не сменился, снесём следующим тиком
                await journal.WritePhaseAsync(cluster, Op, $"verify-timeout/{shard}/{id}",
                    claims.InstanceId, $"verify-джоб старше {options.JobVerifyTimeoutSec} с — kill, попытка зачтена", ct);
                await CleanupJobAsync(engine, containerName, ct);
            }

            return; // ждём — проверка в работе
        }
        if (state == "created")
        {
            await engine.StartContainerAsync(containerName, ct); // создан, но не стартован
            return;
        }

        if (state != "exited")
            return; // прочие состояния — вне протокола супервиза

        var inspect = await engine.InspectContainerAsync(containerName, ct);
        if (!inspect.IsSuccess)
            return; // transient — итог недоступен
        var exitCode = inspect.Value.ExitCode ?? -1;
        var logs = await engine.GetContainerLogsAsync(containerName, 200, ct);
        if (!logs.IsSuccess)
            return; // transient — итог недоступен
        var result = VerifyLog.Parse(logs.Value);

        if (exitCode == 0 && result is { Ok: true })
        {
            // exit 0 + ok:true → verify OK: проверка прошла (SHA256 + manifest)
            var updated = full with { Verify = new BackupVerify(BackupVerifyStatus.Ok, nowUnix) };
            var put = await PutAsync(BackupNames.FullKey(cluster, shard, full.Id),
                BackupStatusJson.Serialize(updated), ct);
            if (!put.IsSuccess)
                return; // transient — статус не сменился, снесём джоб следующим тиком
            await journal.WritePhaseAsync(cluster, Op, $"verified-ok/{shard}/{id}",
                claims.InstanceId, null, ct);
            observe(cluster, shard, "ok");
            await CleanupJobAsync(engine, containerName, ct);
            return;
        }

        if (result is { Ok: false, Phase: "verify" or null, Error: { } err })
        {
            // ненулевой pg_verifybackup (phase=verify) → permanent FAILED: данные плохие
            var failed = full with { Verify = new BackupVerify(BackupVerifyStatus.Failed, nowUnix, err) };
            var put = await PutAsync(BackupNames.FullKey(cluster, shard, full.Id),
                BackupStatusJson.Serialize(failed), ct);
            if (!put.IsSuccess)
                return;
            await journal.WritePhaseAsync(cluster, Op, $"verify-failed/{shard}/{id}",
                claims.InstanceId, err, ct);
            observe(cluster, shard, "failed");
            await CleanupJobAsync(engine, containerName, ct);
            return;
        }

        // иначе (phase=download / exit без результата) → transient: снос, статус
        // НЕ меняем (PENDING остаётся — ретрай следующим тиком, spec §3.1)
        await journal.WritePhaseAsync(cluster, Op, $"download-retry/{shard}/{id}",
            claims.InstanceId, result is { Error: { } transientError } ? transientError : "download failed", ct);
        observe(cluster, shard, "transient");
        await CleanupJobAsync(engine, containerName, ct);
    }

    // Снос ephemeral verify-джоба: контейнер + volume (имена совпадают; 404 = успех).
    private async Task CleanupJobAsync(IDockerEngine engine, string containerName, CancellationToken ct)
    {
        await engine.RemoveContainerAsync(containerName, force: true, ct);
        await engine.RemoveVolumeAsync(containerName, ct);
    }

    // Хост джоба (spec §3.2): хост ноды-источника полного (node-факт) из
    // portalloc; узел исчез из portalloc → ПЕРВЫЙ хост таблицы Docker:Hosts
    // (driver.GetHostsAsync — plain-драйвер отдаёт конфиг-таблицу) → EngineFor;
    // выбор fallback-хоста — journal-факт (phase "engine-fallback/<X>").
    private async Task<(IDockerEngine? Engine, string? FallbackHost)> EngineForShardAsync(
        string cluster, string shard,
        IReadOnlyDictionary<string, NodeAddress> addresses, string nodeKey, CancellationToken ct)
    {
        if (addresses.TryGetValue(nodeKey, out var addr))
            return (driver.EngineFor(addr.Host), null);

        var hosts = await driver.GetHostsAsync(ct); // таблица Docker:Hosts (канон)
        var first = hosts.IsSuccess ? hosts.Value.FirstOrDefault() : null;
        if (first is null)
            return (null, null);
        return (driver.EngineFor(first.Name), first.Name);
    }

    // permanent FAILED: полная перезапись ключа статуса (BackupStatusJson), журнал
    // и наблюдатель; checked_unix — время вердикта.
    private async Task WritePermanentAsync(
        string cluster, string shard, FullBackupState full, string error, long nowUnix, CancellationToken ct)
    {
        var updated = full with
        {
            Verify = new BackupVerify(BackupVerifyStatus.Failed, nowUnix, error),
        };
        var put = await PutAsync(BackupNames.FullKey(cluster, shard, full.Id),
            BackupStatusJson.Serialize(updated), ct);
        if (!put.IsSuccess)
        {
            observe(cluster, shard, "transient"); // put не прошёл — статус остался прежним
            return;
        }

        await journal.WritePhaseAsync(cluster, Op, $"verify-failed/{shard}/{full.Id}",
            claims.InstanceId, error, ct);
        observe(cluster, shard, "failed");
    }

    // Failover-put как BackupProcess.PutAsync (первый успешный endpoint).
    private async Task<Result> PutAsync(string key, string value, CancellationToken ct)
    {
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.PutAsync(endpoint, key, value, null, ct);
            if (result.IsSuccess)
                return result;
        }

        return Result.Failed(new ApplicationException($"etcd put {key} — все endpoints недоступны"));
    }

    private void observe(string cluster, string shard, string result)
        => verifyObserver?.Invoke(cluster, shard, result);
}
