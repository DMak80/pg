using Microsoft.Extensions.Logging;
using PgWorker.Backups.Job;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Docker.Drivers;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;

namespace PgWorker.Backups;

/// <summary>
/// BackupVerifyProcess — тиковая машина проверки полных бэкапов (t04/t26,
/// arch/19 §5/§8) под клэймом &lt;C&gt;: due-резолв кандидатов (PENDING от
/// on_create / периодика по verify.interval_sec; FAILED терминален) →
/// цепочечная проверка list-ами S3 + строгий LSN-разбор .history
/// (WalChain.CheckRange + WalHistory, без скачивания сегментов) →
/// tool-контейнер pgw-backup-verify-&lt;C&gt;-&lt;X&gt;-&lt;id&gt; (образ ноды в tool-режиме,
/// pg_verifybackup по API приёмника) на docker-хосте источника. Тик не
/// блокируется на длинную проверку: итог — поллинг GET /api/ops/verify
/// следующих тиков. Transient (S3/docker/скачивание) — статус кандидата не
/// трогаем; permanent (дыра цепочки, нет wal_start, pg_verifybackup ≠ 0) —
/// FAILED сразу, checksums не запускается (вердикт уже определён).
/// </summary>
public sealed class BackupVerifyProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ShardEndpoints shardEndpoints,
    BackupS3Factory s3Factory,
    IWalReceiverApiClient receiver,
    ClaimStore claims,
    WorkJournal journal,
    BackupsRuntimeOptions options,
    TimeProvider time,
    ILogger<BackupVerifyProcess> logger,
    Action<string, string, string>? verifyObserver = null, // (cluster, shard, result: ok|failed|transient)
    TimeSpan? watchdogWindow = null, // период S3-пульса — полокна (образец сверстников)
    Shared.Core.Hosting.ILoopProgress? progress = null) // S3-вызовы — пульс S3Pulse (arch/14 §6)
{
    private const string Op = "backup-verify";

    /// <summary>Окно бута tool-контейнера (arch/19 §5): приложение .NET
    /// self-contained слушает API не мгновенно после docker start (под
    /// нагрузкой начальной фазы кластера наблюд state refusal десятки секунд);
    /// транспортный отказ GET дольше окна — застрявший бут: ограниченный
    /// рецикл «снос + пересоздание» вместо ожидания возрастного бюджета
    /// Job:VerifyTimeoutSec. С запасом покрывает наблюдавшийся бут под
    /// нагрузкой (~20 с) и много меньше любой операции verify.</summary>
    private const int ToolBootWindowSec = 60;

    private TimeSpan WatchdogWindow => watchdogWindow ?? TimeSpan.FromSeconds(15);

    public async Task<Result<ProcessOutcome>> TickAsync(
        ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Guard 1: клэйм наш (мутации /pgworker/backups/* — только держатель).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"{Op} {cluster}: claim is not ours (or lost) — mutations are forbidden"));

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

        // t36 (arch/19 §5): пул чтения per-cluster — точечные GET/листинги с
        // перебором ReadAll. null/Failed — transient: journal, ретраи тиком
        // (кандидаты остаются PENDING/OK, статусы не трогаем).
        var poolRead = await s3Factory.ResolveAsync(cluster, ct);
        if (!poolRead.IsSuccess || poolRead.Value is not { } poolCtx)
        {
            await journal.WritePhaseAsync(cluster, Op, "waiting-backup-pool", claims.InstanceId,
                poolRead.IsSuccess ? "policy without s3 section — pool not selected" : poolRead.Error!.Message, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }
        await using var s3 = s3Factory.Create(poolCtx);

        foreach (var shard in snap.Shards.Where(s => !s.ToRemove))
        {
            // шард без ключей полных — пропускается (dsn не нужен: verify чисто по S3)
            if (!mine.Shards.TryGetValue(shard.Name, out var shardBackups) || shardBackups.Full.Count == 0)
                continue;

            try
            {
                await TickShardAsync(cluster, shard.Name, shardBackups, addresses.Value, intervalSec, nowUnix, poolCtx, s3, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                // ошибка шарда не роняет остальные (arch/17)
                logger.LogError(ex, "{Op} {cluster}/{shard}: shard tick failed", Op, cluster, shard.Name);
                await journal.WritePhaseAsync(cluster, Op, "shard-error", claims.InstanceId,
                    $"{shard.Name}: {ex.Message}", ct);
            }
        }

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    private async Task TickShardAsync(
        string cluster, string shard, ShardBackups shardBackups,
        IReadOnlyDictionary<string, NodeAddress> addresses, long intervalSec, long nowUnix,
        BackupPoolContext pool, PoolBackupS3 s3, CancellationToken ct)
    {
        var fulls = shardBackups.Full;

        // (1) Супервиз verify-контейнеров шарда: контейнер ищется ОБХОДОМ по
        //     всем хостам Docker:Hosts (симметрия чистке BackupJobsCleaner):
        //     он живёт на хосте ноды-источника полного (или fallback-хосте) —
        //     на мульти-хосте лист только hosts[0] его не видит → вечный
        //     transient-цикл супервиза. Найденный контейнер супервизится НА
        //     СВОЁМ хосте (движок и host — от хоста, где он найден; адрес API —
        //     инспект публикации этого движка). Есть хоть один — обрабатываем
        //     и ЗАВЕРШАЕМ тик шарда — новых не стартуем (инвариант одного
        //     джоба, spec §3.3 п.1). Сбой list хоста → transient (тик повторит).
        var shardPrefix = BackupNames.VerifyContainerName(cluster, shard, string.Empty);
        var hostsForList = await driver.GetHostsAsync(ct);
        if (!hostsForList.IsSuccess || hostsForList.Value.Count == 0)
            return; // хостов нет/таблица недоступна — transient (тик повторит)
        var superviseAny = false;
        foreach (var hostInfo in hostsForList.Value)
        {
            var listEngine = driver.EngineFor(hostInfo.Name);
            if (listEngine is null)
                continue; // хост не резолвится — ответ других хостов достаточен
            var alive = await listEngine.ListContainersAsync(shardPrefix, all: true, ct);
            if (!alive.IsSuccess)
                return; // transport-сбой list → transient (тик повторит)
            // Диагностика жизненного цикла tool-контейнеров: что видит листинг
            // (имя:состояние) — вместе с reason в CleanupJobAsync замыкает
            // картину «кто и когда сносит контейнер».
            if (alive.Value.Count > 0)
                logger.LogInformation(
                    "{Op} {cluster}/{shard}: supervise list on {Host} — {Containers}",
                    Op, cluster, shard, hostInfo.Name,
                    string.Join(", ", alive.Value.Select(c =>
                        $"{c.Names.FirstOrDefault()?.TrimStart('/') ?? "?"}:{c.State}")));
            foreach (var container in alive.Value)
            {
                var name = container.Names
                    .Select(n => n.TrimStart('/'))
                    .FirstOrDefault(n => n.StartsWith(shardPrefix, StringComparison.Ordinal));
                if (name is null)
                    continue;
                superviseAny = true;
                await SuperviseAsync(
                    listEngine, cluster, shard, name, shardPrefix, container.State,
                    shardBackups, nowUnix, hostInfo.Name, ct);
            }
        }

        if (superviseAny)
            return; // супервиз-тик: новых джобов не стартуем

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
                "no wal_start_segment — set is not archiveable (no chain start point)", nowUnix, ct);
            return;
        }

        // (4) Цепочка: list wal/ + list набора + строгие TLI-переходы (по содержимому
        //     history при наличии). Transient list/GET → шард-skip, статус не трогаем.
        //     Листинги — UNION по всем нодам пула (t02, arch/19 §5 «где данные
        //     есть»): полный заливается на первую живую ноду момента заливки —
        //     соло-лист другой ноды даёт ложный gap и ложное «пусто» в наборе.
        var walList = await S3Pulse.CallAsync(progress, token => s3.ListWalUnionAsync(cluster, shard, ct: token), ct, WatchdogWindow / 2);
        if (!walList.IsSuccess)
        {
            observe(cluster, shard, "transient");
            return;
        }

        var setList = await S3Pulse.CallAsync(progress, token => s3.ListUnionAsync(cluster, shard, $"full/{id}/pg_wal/", ct: token), ct, WatchdogWindow / 2);
        if (!setList.IsSuccess)
        {
            observe(cluster, shard, "transient");
            return;
        }

        var start = WalFileName.TryParse(candidate.WalStartSegment);
        if (start is null)
        {
            await WritePermanentAsync(cluster, shard, candidate,
                $"invalid wal_start_segment {candidate.WalStartSegment}", nowUnix, ct);
            return;
        }

        var setSegments = setList.Value
            .Select(o => WalFileName.TryParse(o.Name))
            .OfType<WalFileName>()
            .ToList();
        if (setSegments.Count == 0)
        {
            await WritePermanentAsync(cluster, shard, candidate,
                "set contains no pg_wal WAL segments", nowUnix, ct);
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
            var content = await S3Pulse.CallAsync(progress, token => s3.GetObjectAsync(cluster, shard, $"wal/{obj.Name}", token), ct, WatchdogWindow / 2);
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
                check.GapError ?? "WAL chain check failed", nowUnix, ct);
            return;
        }

        // (5) Tool-контейнер checksums (t26, spec §3.5): docker-хост источника
        //     (node-факт portalloc; fallback — первый хост Docker:Hosts с
        //     journal-фактом) + create/start + команда StartVerifyAsync
        //     (S3-контекст read-списка — секреты только в POST, Р4).
        var (engine, engineHost, fallbackHost) = await EngineForShardAsync(
            cluster, shard, addresses, $"{shard}/{candidate.Node}", ct);
        if (engine is null)
        {
            observe(cluster, shard, "transient"); // хост не известен — следующий тик
            return;
        }

        // Порт API tool-контейнера: прод — зонд свободного динамического
        // (правило AGENTS.md); контур (ContourNetwork задан) — без публикации
        // (адрес — alias имени в сети контура, ForTool contourMode).
        var contourMode = options.ContourNetwork is { Length: > 0 };
        int? hostPort = null;
        if (!contourMode)
        {
            var probe = Tool.FreePortProbe.Allocate();
            if (!probe.IsSuccess)
            {
                observe(cluster, shard, "transient");
                return;
            }
            hostPort = probe.Value;
        }
        var jobName = BackupNames.VerifyContainerName(cluster, shard, id);
        var spec = Tool.ToolJobSpec.Build(options, cluster, jobName,
            BackupNames.VerifyVolumeName(cluster, shard, id), "/staging", hostPort);
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

        // Адрес API tool-контейнера: контур — alias сети контура; прод —
        // инспект публикации :8080 (динамический host-порт).
        string receiverUrl;
        if (contourMode)
        {
            receiverUrl = WalReceiverAddress.ForTool(jobName, engineHost, null, contourMode: true);
        }
        else
        {
            var inspect = await engine.InspectContainerAsync(jobName, ct);
            if (!inspect.IsSuccess)
            {
                observe(cluster, shard, "transient"); // порт не увидели — снос супервизом
                return;
            }
            var published = inspect.Value.Ports
                .FirstOrDefault(p => p.ContainerPort == 8080)?.HostPort;
            if (published is null)
            {
                observe(cluster, shard, "transient");
                return;
            }
            receiverUrl = WalReceiverAddress.ForTool(jobName, engineHost, published, contourMode: false);
        }

        // Команда запуска с ограниченным ретраем (гонка бута tool-контейнера):
        // приложение .NET self-contained слушает :8080 не мгновенно после
        // docker start (наблюдались секунды под нагрузкой начальной фазы
        // кластера) — POST в окне бута даёт connection refused; без ретрая
        // контейнер-сирота ждал бы супервиз, и цикл «create → refused →
        // снос» не сходился бы, пока бут дольше тика. POST у приёмника 404
        // не возвращает (202/409) — первый же ответ проснувшегося приложения
        // запускает операцию, сходимость гарантирована в окне ретрая;
        // исчерпание — transient (контейнер жив, супервиз продолжит по ветке
        // окна бута, arch/19 §5). ~10 с — много меньше окна бута и бюджетов
        // зависших тиков.
        ReceiverReply launch = default!;
        for (var attempt = 1; ; attempt++)
        {
            launch = await receiver.StartVerifyAsync(receiverUrl, new StartVerifyRequest(
                id,
                new S3Context(
                    Endpoints: [.. pool.ReadAll],
                    AccessKey: pool.AccessKey,
                    SecretKey: pool.SecretKey,
                    Bucket: options.S3Bucket,
                    Prefix: $"{cluster}/{shard}",
                    CaPem: BackupCaPem.ReadIfConfigured(options.S3CaBundlePath))), ct);
            if (launch.IsAccepted || launch.IsConflict || !launch.IsTransient || attempt >= 5)
                break;
            await Task.Delay(TimeSpan.FromSeconds(1), ct);
        }
        if (!launch.IsAccepted && !launch.IsConflict)
        {
            // Транспортный отказ/5xx команды: контейнер-сирота без операции —
            // супервиз отправит её по ветке окна бута (refused дольше
            // ToolBootWindowSec → ограниченный рецикл, arch/19 §5), PENDING
            // остаётся.
            observe(cluster, shard, "transient");
            return;
        }

        // journal-факты: started — факт запуска; при fallback-пути резюме тика —
        // выбор fallback-хоста (журнал — единый ключ последней фазы: резюме пишем
        // ПОСЛЕДНИМ, чтобы факт выбора не затирался started).
        await journal.WritePhaseAsync(cluster, Op, $"started/{shard}/{id}", claims.InstanceId, null, ct);
        if (fallbackHost is not null)
            await journal.WritePhaseAsync(cluster, Op, $"engine-fallback/{shard}",
                claims.InstanceId, fallbackHost, ct);
        logger.LogInformation("{Op} {cluster}/{shard}: verify tool container {job} started (id {id})",
            Op, cluster, shard, jobName, id);
    }

    // Супервиз одного verify-tool-контейнера шарда (t26, spec §3.5): orphan/
    // stale → снос без итога; created → старт; итог — ТОЛЬКО по API
    // (GetVerifyAsync): succeeded ⇒ OK+checked_unix; failed+downloading
    // (МАРКЕР ТРАНЗИЕНТА) ⇒ снос, статус не трогаем (PENDING, ретрай тиком);
    // failed+verifying ⇒ FAILED+error; IsNotFound (операция vanished — рестарт
    // приёмника) ⇒ transient: снос, PENDING-ретрай; транспортный отказ
    // (StatusCode 0 — приложение не слушает) ⇒ окно бута ToolBootWindowSec от
    // StartedAt: в окне — ожидание, дольше — ограниченный рецикл «снос +
    // пересоздание» (arch/19 §5); 5xx и running — возрастной
    // бюджет Job:VerifyTimeoutSec по StartedAtUnix инспекта ⇒ kill+rm,
    // checked_unix=now (вердикт НЕ ставится).
    private async Task SuperviseAsync(
        IDockerEngine engine, string cluster, string shard, string containerName, string shardPrefix,
        string state, ShardBackups shardBackups, long nowUnix, string host, CancellationToken ct)
    {
        var id = containerName[shardPrefix.Length..];
        var full = shardBackups.Full.FirstOrDefault(f => f.Id == id);

        // orphan/stale (ключ ушёл — deprovisioning-гонка; внешний FAILED): итог НЕ
        // пишем, объект сносим. verify null|PENDING|OK — легитимный кандидат.
        if (full is null
            || full.State != FullBackupStatus.Completed
            || full.Verify is { State: BackupVerifyStatus.Failed })
        {
            await CleanupJobAsync(engine, containerName, "orphan-stale", ct);
            return;
        }

        if (state == "created")
        {
            await engine.StartContainerAsync(containerName, ct); // создан, но не стартован
            return;
        }

        // Итог — по API приёмника tool-контейнера (адрес: контур — alias,
        // прод — инспект публикации :8080; host — хост контейнера).
        var receiverUrl = await ToolReceiverUrlAsync(engine, host, containerName, ct);
        if (receiverUrl is not null)
        {
            var reply = await receiver.GetVerifyAsync(receiverUrl, ct);
            if (reply.IsNotFound)
            {
                // Контейнер жив, операции нет (рестарт приёмника) — transient:
                // снос, PENDING остаётся, следующий тик создаст заново.
                await journal.WritePhaseAsync(cluster, Op, $"download-retry/{shard}/{id}",
                    claims.InstanceId, "verify operation vanished — container recreated", ct);
                observe(cluster, shard, "transient");
                await CleanupJobAsync(engine, containerName, "not-found-recreate", ct);
                return;
            }
            if (reply.StatusCode == 0)
            {
                // Транспортный отказ GET (приложение ещё не слушает API — бут,
                // либо процесс замолчал): окно бута (arch/19 §5). В окне —
                // transient-ожидание; отказ дольше окна — застрявший бут:
                // ограниченный рецикл «снос + пересоздание» (симметрия
                // IsNotFound-ветки), PENDING остаётся — до возрастного бюджета
                // отказ-сирота не живёт. Ветка применяется в любом состоянии
                // контейнера (running/exited — молчащее приложение одинаково).
                var inspectBoot = await engine.InspectContainerAsync(containerName, ct);
                if (inspectBoot.IsSuccess
                    && inspectBoot.Value.StartedAtUnix is { } bootStarted
                    && SupervisionTimeouts.IsTimedOut(bootStarted, nowUnix, ToolBootWindowSec))
                {
                    await journal.WritePhaseAsync(cluster, Op, $"boot-recycle/{shard}/{id}",
                        claims.InstanceId,
                        $"tool container refuses API for over {ToolBootWindowSec} s — recycled (boot window)", ct);
                    observe(cluster, shard, "transient");
                    await CleanupJobAsync(engine, containerName, "boot-refused-recycle", ct);
                }
                return; // в окне бута — ждём; рецикл следующим тиком
            }
            if (!reply.IsTransient && reply.Status is { } snapshot)
            {
                switch (snapshot.State)
                {
                    case "succeeded":
                    {
                        // verify OK: checksums прошли (SHA256 + manifest).
                        var updated = full with { Verify = new BackupVerify(BackupVerifyStatus.Ok, nowUnix) };
                        var put = await PutAsync(BackupNames.FullKey(cluster, shard, full.Id),
                            BackupStatusJson.Serialize(updated), ct);
                        if (!put.IsSuccess)
                            return; // transient — статус не сменился, снесём следующим тиком
                        await journal.WritePhaseAsync(cluster, Op, $"verified-ok/{shard}/{id}",
                            claims.InstanceId, null, ct);
                        logger.LogInformation("{Op} {cluster}/{shard}: verify {id} OK", Op, cluster, shard, id);
                        observe(cluster, shard, "ok");
                        await CleanupJobAsync(engine, containerName, "after-ok", ct);
                        return;
                    }
                    case "failed" when snapshot.Phase == "downloading":
                    {
                        // МАРКЕР ТРАНЗИЕНТА: сбой скачивания — статус не трогаем
                        // (PENDING), снос — следующий тик создаст заново.
                        await journal.WritePhaseAsync(cluster, Op, $"download-retry/{shard}/{id}",
                            claims.InstanceId, snapshot.Error ?? "download failed", ct);
                        logger.LogWarning(
                            "{Op} {cluster}/{shard}: verify {id} download failed: {Error}",
                            Op, cluster, shard, id, snapshot.Error ?? "download failed");
                        observe(cluster, shard, "transient");
                        await CleanupJobAsync(engine, containerName, "download-retry", ct);
                        return;
                    }
                    case "failed":
                    {
                        // PERMANENT (phase=verifying): pg_verifybackup ≠ 0 — данные плохие.
                        var err = snapshot.Error ?? "pg_verifybackup failed";
                        var failed = full with { Verify = new BackupVerify(BackupVerifyStatus.Failed, nowUnix, err) };
                        var put = await PutAsync(BackupNames.FullKey(cluster, shard, full.Id),
                            BackupStatusJson.Serialize(failed), ct);
                        if (!put.IsSuccess)
                            return;
                        await journal.WritePhaseAsync(cluster, Op, $"verify-failed/{shard}/{id}",
                            claims.InstanceId, err, ct);
                        logger.LogError("{Op} {cluster}/{shard}: verify {id} FAILED: {Error}", Op, cluster, shard, id, err);
                        observe(cluster, shard, "failed");
                        await CleanupJobAsync(engine, containerName, "verify-failed", ct);
                        return;
                    }
                }
                // running — проверка в работе: проваливаемся в возрастной бюджет.
            }
        }

        if (state is "running" or "restarting")
        {
            // t07 (arch/19 §6): возраст running-контейнера — docker-факт
            // StartedAt (в etcd-статусе кандидата времени запуска нет). Бюджет
            // исчерпан → kill+rm; вердикт FAILED НЕ ставим (данные не виноваты)
            // — попытка зачитывается checked_unix=now: следующий due через
            // verify.interval_sec, лив-лок немедленных перезапусков исключён.
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
                    claims.InstanceId, $"verify container older than {options.JobVerifyTimeoutSec} s — killed, attempt counted", ct);
                await CleanupJobAsync(engine, containerName, "after-ok", ct);
            }

            return; // ждём — проверка в работе
        }
    }

    // Адрес API tool-контейнера verify (spec §3.1): контур (ContourNetwork) —
    // alias имени :8080 в сети контура; прод — инспект публикации :8080
    // (динамический host-порт). Инспект не удался — null (transient тика).
    private async Task<string?> ToolReceiverUrlAsync(
        IDockerEngine engine, string host, string containerName, CancellationToken ct)
    {
        if (options.ContourNetwork is { Length: > 0 })
            return WalReceiverAddress.ForTool(containerName, host, null, contourMode: true);
        var inspect = await engine.InspectContainerAsync(containerName, ct);
        if (!inspect.IsSuccess)
            return null;
        var published = inspect.Value.Ports
            .FirstOrDefault(p => p.ContainerPort == 8080)?.HostPort;
        return published is null
            ? null
            : WalReceiverAddress.ForTool(containerName, host, published, contourMode: false);
    }

    // Снос ephemeral verify-джоба: контейнер + volume (имена совпадают; 404 = успех).
    // reason — диагностика жизненного цикла (кто и за что снёс контейнер;
    // docker-вызовы движка в логе воркера не видны).
    private async Task CleanupJobAsync(IDockerEngine engine, string containerName, string reason, CancellationToken ct)
    {
        logger.LogInformation("{Op}: cleanup {Container} — {Reason}", Op, containerName, reason);
        await engine.RemoveContainerAsync(containerName, force: true, ct);
        await engine.RemoveVolumeAsync(containerName, ct);
    }

    // Хост джоба (spec §3.2): хост ноды-источника полного (node-факт) из
    // portalloc; узел исчез из portalloc → ПЕРВЫЙ хост таблицы Docker:Hosts
    // (driver.GetHostsAsync — plain-драйвер отдаёт конфиг-таблицу) → EngineFor;
    // выбор fallback-хоста — journal-факт (phase "engine-fallback/<X>").
    private async Task<(IDockerEngine? Engine, string Host, string? FallbackHost)> EngineForShardAsync(
        string cluster, string shard,
        IReadOnlyDictionary<string, NodeAddress> addresses, string nodeKey, CancellationToken ct)
    {
        if (addresses.TryGetValue(nodeKey, out var addr))
            return (driver.EngineFor(addr.Host), addr.Host, null);

        var hosts = await driver.GetHostsAsync(ct); // таблица Docker:Hosts (канон)
        var first = hosts.IsSuccess ? hosts.Value.FirstOrDefault() : null;
        if (first is null)
            return (null, string.Empty, null);
        return (driver.EngineFor(first.Name), first.Name, first.Name);
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
        logger.LogError("{Op} {cluster}/{shard}: verify {full.Id} FAILED: {Error}", Op, cluster, shard, full.Id, error);
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

        return Result.Failed(new ApplicationException($"etcd put {key} — all endpoints unavailable"));
    }

    private void observe(string cluster, string shard, string result)
        => verifyObserver?.Invoke(cluster, shard, result);
}
