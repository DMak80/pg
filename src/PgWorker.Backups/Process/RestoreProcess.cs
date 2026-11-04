using System.Globalization;
using Microsoft.Extensions.Logging;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using Shared.Etcd.Client;
using PgWorker.Backups.Job;
using PgWorker.Etcd.Backups;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Sql;

namespace PgWorker.Backups.Process;

// RestoreProcess (arch/19 §3.5): машина восстановления шарда из бэкапа.
// Поддерживается ТОЛЬКО в Mode=Plain (подсистема бэкапов plain-only с /;
// swarm-драйвер джобы бэкапов не исполняет). Тик под клэймом <C>: активная
// заявка (максимум одна на шард) проводится по фазам
// PLANNED (валидация: усыновление/полный/manifest/цепочка) →
// RUNNING (демонтаж + ephemeral restore-джоб, Task 9) →
// REJOINING (EnsureNode + Patroni-пробы, Task 10) →
// COMPLETED (del wal-ключа → планировщик переснимает полный).
// permanent/transient: валидационные отказы — permanent-FAILED (повтор заявки
// оператором); docker/S3-транспортные отказы — transient (статус не меняем,
// следующий тик повторит). Takeover: всё состояние — в etcd-статусе заявки;
// in-memory только диагностика ожиданий (Task 10).
public sealed class RestoreProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    BackupS3Factory s3Factory,
    IWalReceiverApiClient receiver,
    ClaimStore claims,
    WorkJournal journal,
    BackupsRuntimeOptions options,
    InstallSecrets secrets,
    EtcdEndpoints etcdEndpoints,
    IClusterSecretEnsurer appSecret,
    PgWorker.Provisioning.Processes.INodeBootstrapDelivery bootstrap,
    ShardProbe probe,
    ISqlExecutor db,
    ThresholdsOptions thresholds,
    TimeProvider time,
    PgtuneInputsFactory pgtune,
    PgtuneSettings pgtuneSettings,
    ILogger<RestoreProcess>? logger = null,
    Action<string, string, string>? restoreObserver = null, // (cluster, shard, ok|failed), arch/18 §2.7
    TimeSpan? watchdogWindow = null,
    Shared.Core.Hosting.ILoopProgress? progress = null) // аудит долгих фаз: итерации поллинга дают Mark (spec §1.2 п.4)
{
    private const string Op = "backup-restore";

    // Окно проверки watchdog для поллинга create-вызовов (дефолт — продовые 15 c).
    private TimeSpan WatchdogWindow => watchdogWindow ?? TimeSpan.FromSeconds(15);

    public async Task<Result<ProcessOutcome>> TickAsync(
        ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)
    {
        // Каркас (Task 8): параметры фаз RUNNING/REJOINING (демонтаж/джоб/rejoin,
        // Tasks 9–10) — discard до реализации фаз, сигнатура финальная.
        _ = driver; _ = secrets; _ = etcdEndpoints; _ = appSecret; _ = probe; _ = thresholds;

        var cluster = snap.Config.Cluster;

        // Мутации /pgworker/backups/* — только держатель клэйма (arch/19 §4).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"backup-restore {cluster}: claim is not ours (or lost) — mutations are forbidden"));

        // G0: подсистема выключена — процесс не исполняется (врезка ReconcileLoop
        // тоже гвардит, но процесс самодостаточен при прямом вызове).
        if (!options.Enabled)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        if (snap.Config.State != ClusterState.Active)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var mine = backups.FirstOrDefault(b => b.Cluster == cluster);
        var actives = mine?.Shards
            .SelectMany(kv => kv.Value.Restores.Select(r => (Shard: kv.Key, Op: r)))
            .Where(p => p.Op.State is RestoreStatus.Planned or RestoreStatus.Running or RestoreStatus.Rejoining)
            .OrderBy(p => p.Op.Id).ThenBy(p => p.Shard).ToList() ?? [];
        if (actives.Count == 0)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        // Процессный гвард «максимум один активный на шард» (§3.1): API-гвард
        // (txn 409) обычно не допускает дублей, но ручная запись/гонка могут —
        // младшие дубли того же шарда гасим permanent-FAILED, старейший
        // исполняется.
        var perShard = new List<(string Shard, RestoreOperationState Op)>();
        foreach (var active in actives)
        {
            var existing = perShard.FirstOrDefault(p => p.Shard == active.Shard);
            if (existing.Shard is null)
                perShard.Add(active);
            else
                await FailPermanentAsync(cluster, active.Shard, active.Op,
                    error: $"duplicate request: oldest {existing.Op.Id} is active", ct);
        }

        // Каждый шард делает ОДИН шаг за тик — шаги разных шардов идут
        // ОТДЕЛЬНЫМИ ТАСКАМИ и тик ждёт их все (Task.WhenAll): шарды независимы
        // (свои ноды/тома/S3-префиксы). Прежний последовательный обход отдавал
        // время одного шарда другому (второй restore стартовал на 31 с позже).
        var steps = new List<Task<Result<ProcessOutcome>>>();
        foreach (var (shardName, op) in perShard)
        {
            var shard = snap.Shards.FirstOrDefault(s => s.Name == shardName);
            if (shard is null)
                continue; // шард убрали — заявку закроет remove-shard ветка
            steps.Add(RunShardStepAsync(cluster, snap, shard, op, mine, ct));
        }

        var last = Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        foreach (var outcome in await Task.WhenAll(steps))
            if (!outcome.IsSuccess)
                last = outcome;
        return last;
    }

    // Один шаг одного шарда в тике (выполняется отдельной таской; исключение
    // шарда не роняет тик и другие шарды — §3.4).
    private async Task<Result<ProcessOutcome>> RunShardStepAsync(
        string cluster, ClusterSnapshot snap, ShardSpec shard, RestoreOperationState op,
        ClusterBackups? mine, CancellationToken ct)
    {
        try
        {
            return op.State switch
            {
                RestoreStatus.Planned => await ValidateAsync(snap, shard, op, mine, ct),
                RestoreStatus.Running => await RunAsync(snap, shard, op, ct),      // Task 9
                RestoreStatus.Rejoining => await RejoinAsync(snap, shard, op, ct), // Task 10
                _ => Result<ProcessOutcome>.Success(ProcessOutcome.Done),
            };
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger?.LogError(ex, "backup-restore {Cluster}/{Shard}: {Message}", cluster, shard.Name, ex.Message);
            await journal.WritePhaseAsync(cluster, Op, "crashed", claims.InstanceId, ex.Message, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }
    }

    //  (arch/19 §4.2/§3.5): пул чтения restore — source_pool DR-заявки
    // (чужой пул, CreateForPoolAsync) ?? собственный пул кластера (policy.s3).
    private async Task<Result<(PoolBackupS3 Client, BackupPoolContext Context)>> ResolveReadPoolAsync(
        string cluster, RestoreOperationState op, CancellationToken ct)
    {
        if (op.SourcePool is { Length: > 0 } sourcePool)
            return await s3Factory.CreateForPoolAsync(sourcePool, ct);
        var resolved = await s3Factory.ResolveAsync(cluster, ct);
        if (!resolved.IsSuccess)
            return Result<(PoolBackupS3, BackupPoolContext)>.Failed(resolved.Error!);
        if (resolved.Value is not { } ctx)
            return Result<(PoolBackupS3, BackupPoolContext)>.Failed(new ApplicationException(
                $"s3-pool: cluster {cluster} policy has no s3 section — pool not selected"));
        return (s3Factory.Create(ctx), ctx);
    }

    // ── PLANNED: валидация заявки (все шаги идемпотентны) ──

    private async Task<Result<ProcessOutcome>> ValidateAsync(
        ClusterSnapshot snap, ShardSpec shard, RestoreOperationState op,
        ClusterBackups? mine, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Живая сверка клэйма перед фазой заявки (arch/17 S5).
        if (!await ClaimAliveAsync(cluster, shard.Name, op, ct))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"{Op} {cluster}: claim lost before validate of {shard.Name}/{op.Id}"));

        //  (arch/19 §5): пул чтения — source_pool DR-заявки ?? собственный
        // пул policy.s3. null/Failed — transient: journal, ретраи тиком.
        var poolRead = await ResolveReadPoolAsync(cluster, op, ct);
        if (!poolRead.IsSuccess)
        {
            await journal.WritePhaseAsync(cluster, Op, "waiting-backup-pool", claims.InstanceId,
                poolRead.Error!.Message, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }
        // (клиент, контекст): клиент — IAsyncDisposable, контекст — списки env.
        var (readS3, poolCtx) = poolRead.Value;
        // Клиент — кэш фабрики (breaker переживает тики) — НЕ диспоузим.
        var s3 = readS3;

        // 1. Усыновлённые (object) ноды — ручной путь, restore plain-only.
        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return await TransientAsync(cluster, $"portalloc-unavailable/{shard.Name}/{op.Id}",
                addresses.Error!.Message, ct);
        var objectNode = addresses.Value.Keys
            .Where(k => k.StartsWith($"{shard.Name}/", StringComparison.Ordinal))
            .Any(k => addresses.Value[k].Object is { Length: > 0 });
        if (objectNode)
        {
            await FailPermanentAsync(cluster, shard.Name, op,
                "restore of adopted shards is not supported (manual path — docs/backup-restore.md)", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        // 2. Source-префикс («<srcC>/<srcX>»; default — собственный).
        var parts = op.Source.Split('/', 2);
        if (parts.Length != 2 || parts[0].Length == 0 || parts[1].Length == 0)
        {
            await FailPermanentAsync(cluster, shard.Name, op,
                $"malformed source '{op.Source}'", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }
        var (srcC, srcX) = (parts[0], parts[1]);
        var ownSource = srcC == cluster && srcX == shard.Name;

        // 3. backup_id: заявка → DR-кандидат (orphan S3) → свой свежий
        //    COMPLETED (etcd) → DR-list S3.
        //    Orphan-полные (S3 без etcd-статуса) — объекты инкарнации, чьи
        //    статусы стёр deprovision (D2). Пересозданный кластер успевает
        //    снять СВОЙ полный (статус есть): в DR он не источник — newest
        //    обязан браться из orphan-набора, иначе restore восстановит пустой
        //    кластер из собственного свежего снимка (факт E2E). Статусы —
        //    по шарду-ИСТОЧНИКУ: source-override между шардами того же
        //    кластера (DR shard1→shard2) подчиняется тому же правилу.
        FullBackupState? ownFresh = null;
        var statusIds = new HashSet<string>(StringComparer.Ordinal);
        var backupId = op.BackupId;
        if (backupId.Length == 0 && srcC == cluster
            && mine?.Shards.TryGetValue(srcX, out var srcSb) == true)
        {
            foreach (var f in srcSb.Full)
                statusIds.Add(f.Id);
            if (ownSource)
                ownFresh = srcSb.Full.Where(f => f.State == FullBackupStatus.Completed)
                    .OrderByDescending(f => f.Id, StringComparer.Ordinal).FirstOrDefault();
        }
        if (backupId.Length == 0 && statusIds.Count > 0)
        {
            var listed = await S3Pulse.CallAsync(progress, token => s3.ListFullsAsync(srcC, srcX, ct: token), ct, WatchdogWindow / 2);
            if (!listed.IsSuccess)
                return await TransientAsync(cluster, $"s3-unavailable/{shard.Name}/{op.Id}",
                    listed.Error!.Message, ct);
            var orphans = listed.Value.Where(id => !statusIds.Contains(id)).ToList();
            if (orphans.Count > 0)
                backupId = orphans[^1]; // новейший полный погибшей инкарнации
        }
        if (backupId.Length == 0 && ownFresh is { } fresh)
            backupId = fresh.Id;
        if (backupId.Length == 0)
        {
            // DR-ветка (source-override или etcd-статусов нет): новейший = max Id
            // (id — сортируемая метка времени, Ordinal).
            var fulls = await S3Pulse.CallAsync(progress, token => s3.ListFullsAsync(srcC, srcX, ct: token), ct, WatchdogWindow / 2);
            if (!fulls.IsSuccess)
                return await TransientAsync(cluster, $"s3-unavailable/{shard.Name}/{op.Id}",
                    fulls.Error!.Message, ct);
            if (fulls.Value.Count == 0)
            {
                await FailPermanentAsync(cluster, shard.Name, op,
                    $"no full backups found in {srcC}/{srcX}", ct);
                return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
            }
            backupId = fulls.Value[^1];
        }

        // 3.5. Номер жизни выбранного полного (t15, arch/backups/05 §1):
        // full-статус по id (етcd); DR-orphan (статуса нет или поля нет) —
        // null — fetch-wal фильтр по номеру не активирует.
        FullBackupState? selectedFull = null;
        if (mine?.Shards.TryGetValue(srcX, out var lifeSb) == true)
            selectedFull = lifeSb.Full.FirstOrDefault(f => f.Id == backupId);
        var chainLife = selectedFull?.ChainLife;

        // 4. Факт целостности кандидата: upload (mc cp --recursive) не
        // атомарен и не гарантирует «манифест последним» — упавший на середине
        // джоб оставляет частичный префикс full/<id>/, а при DR etcd-статусов
        // нет; инвариант «префикс ⇔ манифест» механикой НЕ обеспечивается →
        // частичный кандидат отсеивается проверкой манифеста.
        var manifest = await S3Pulse.CallAsync(progress, token => s3.DownloadTextAsync(srcC, srcX, $"full/{backupId}/backup_manifest", token), ct, WatchdogWindow / 2);
        if (!manifest.IsSuccess)
        {
            await FailPermanentAsync(cluster, shard.Name, op,
                $"full {backupId} has no backup_manifest (incomplete/corrupt)", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        // 5. Стартовая точка WAL: свой свежий полный → etcd-статус; иначе
        // backup_label из S3 (BackupLabel.WalStartSegment — sed-эквивалент).
        string? walStart = ownFresh?.WalStartSegment;
        if (walStart is not { Length: > 0 })
        {
            var label = await S3Pulse.CallAsync(progress, token => s3.DownloadTextAsync(srcC, srcX, $"full/{backupId}/backup_label", token), ct, WatchdogWindow / 2);
            walStart = label.IsSuccess ? Restore.BackupLabel.WalStartSegment(label.Value) : null;
            if (walStart is null)
            {
                await FailPermanentAsync(cluster, shard.Name, op,
                    $"full/{backupId}: backup_label unavailable/corrupt", ct);
                return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
            }
        }
        var chainStart = WalFileName.TryParse(walStart);
        if (chainStart is null)
        {
            await FailPermanentAsync(cluster, shard.Name, op,
                $"full/{backupId}: wal_start '{walStart}' cannot be parsed", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        // 6. Непрерывность WAL-цепочки от стартовой точки (WalChain):
        // дыра → permanent с границами; S3-отказ → transient (статус не меняем).
        var wal = await S3Pulse.CallAsync(progress, token => s3.ListWalAsync(srcC, srcX, ct: token), ct, WatchdogWindow / 2);
        if (!wal.IsSuccess)
            return await TransientAsync(cluster, $"s3-unavailable/{shard.Name}/{op.Id}",
                wal.Error!.Message, ct);
        var chain = WalChain.Check(chainStart.Value, wal.Value.Select(o => o.Name));
        if (!chain.IsContinuous)
        {
            await FailPermanentAsync(cluster, shard.Name, op,
                chain.GapError ?? "WAL chain gap", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        // 7. Успех: RUNNING (journal-before-manipulations — демонтаж в Task 9
        // начинается только после видимого RUNNING).
        var running = op with
        {
            State = RestoreStatus.Running,
            BackupId = backupId,
            StartedUnix = NowUnix(),
            ChainLife = chainLife, // t15: номер жизни выбранного полного → fetch-wal
        };
        var put = await PutStatusAsync(cluster, shard.Name, running, ct);
        if (!put.IsSuccess)
            return Result<ProcessOutcome>.Failed(put.Error!);
        await journal.WritePhaseAsync(cluster, Op, $"validated/{shard.Name}/{op.Id}", claims.InstanceId, null, ct);
        return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
    }

    // ── RUNNING: демонтаж + restore-операция WalReceiver первой ноды (§3.5) ──

    private async Task<Result<ProcessOutcome>> RunAsync(
        ClusterSnapshot snap, ShardSpec shard, RestoreOperationState op, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Живая сверка клэйма перед фазой демонтажа/восстановления (arch/17 S5).
        if (!await ClaimAliveAsync(cluster, shard.Name, op, ct))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"{Op} {cluster}: claim lost before run of {shard.Name}/{op.Id}"));

        // Первая нода — её WalReceiver исполняет restore-операцию (arch/19 §3.5);
        // адрес из portalloc (null → permanent: заявка не исполнима).
        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return await TransientAsync(cluster, $"portalloc-unavailable/{shard.Name}/{op.Id}",
                addresses.Error!.Message, ct);
        var first = shard.Nodes.Count > 0 ? shard.Nodes.Min(n => n.Name) : null;
        if (first is null
            || !addresses.Value.TryGetValue($"{shard.Name}/{first}", out var addr))
        {
            // portalloc рассинхронизирован с декларацией — заявка не исполнима
            await FailPermanentAsync(cluster, shard.Name, op,
                $"node {first ?? "?"} of shard {shard.Name} not found in portalloc", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        //  (arch/19 §6): статусный бюджет заявки (downloading/демонтаж могут
        // transient-циклиться без ноды — возраст считаем от started_unix).
        // Канон допускает «restore часами»; сутки — явный завис. REJOINING не
        // таймаутим (бюджеты Patroni-проб — свои); RecoveryTimeoutSec — бюджет
        // ВНУТРИ операции приёмника (§3.5). Снос первой ноды — вместе с
        // volume (restore-операция уходит с нодой); best-effort (transient):
        // статус FAILED важнее.
        var opStarted = op.StartedUnix ?? NowUnix();
        if (SupervisionTimeouts.IsTimedOut(opStarted, NowUnix(), options.JobRestoreTimeoutSec))
        {
            await driver.RemoveNodeAsync(cluster, shard.Name, first, ct);
            await FailPermanentAsync(cluster, shard.Name, op,
                $"restore-job-timeout: {NowUnix() - opStarted}s > {options.JobRestoreTimeoutSec}", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        // Цель: "time:<RFC3339>" → PG-формат recovery_target_time (парсер не
        // принимает ни «T»-сепаратор ISO-8601, ни суффикс «Z» — приняты «пробел»
        // и «+00:00»); latest → null (ЕДИНОЕ ПРАВИЛО контракта: latest = null,
        // пустая строка приёмнику не передаётся). Битое время (ручная запись
        // мимо API) — permanent FAILED.
        string? targetTime = null;
        if (op.Target.StartsWith("time:", StringComparison.Ordinal))
        {
            if (!DateTimeOffset.TryParse(op.Target["time:".Length..],
                    CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal,
                    out var targetTs))
            {
                await FailPermanentAsync(cluster, shard.Name, op,
                    $"target_time is not RFC3339: {op.Target["time:".Length..]}", ct);
                return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
            }

            targetTime = targetTs.ToUniversalTime().ToString(
                "yyyy-MM-dd HH:mm:sszzz", CultureInfo.InvariantCulture);
        }

        // Адрес приёмника первой ноды (arch/19 §2/§3.5) — alias:8080 в общей
        // сети воркера и нод.
        var receiverUrl = WalReceiverAddress.ForNode(cluster, shard.Name, first, addr.Object);

        // ── Фазы RUNNING (в поле Phase заявки; фазы операции приёмника
        // downloading/recovering перезаписывают их при поллинге) ──
        // null/"" → ДЕМОНТАЖ (безусловный: живой шард обязан быть снесён ДО
        //   restore — данные переписываются; идемпотентен при takeover-повторе) →
        //   Phase="demolished".
        // "demolished" → EnsureNode первой ноды (щит "restore-in-progress"
        //   удерживает Patroni от bootstrap пустого PGDATA) + POST /api/ops/restore
        //   → Phase="launching". 409 (чужая ops-операция — verify, arch/19 §3.5)
        //   — transient: фаза остаётся "demolished", повтор следующим тиком.
        //   Краш между POST и put фазы — редкое окно: повторный POST получает
        //   409, пока операция жива, и доезжает до перезапуска операции после
        //   её терминала (резюмов нет — скачивание заново, идемпотентно).
        // "launching"/"downloading"/"recovering" → только GET-поллинг: повтор
        //   команды на каждом тике супервиза НЕБЕЗОПАСЕН — когда операция
        //   приёмника достигла терминала, 409-гвард принимает новую команду
        //   (202) РАНЬШЕ, чем тот же тик успевает прочитать GET succeeded, —
        //   операция перезапускается вечно до бюджетного таймаута. Команду
        //   повторяют ТОЛЬКО ветки супервиза vanished/failed+downloading —
        //   сбросом Phase обратно в "demolished" (ensure no-op на живой ноде).
        switch (op.Phase)
        {
            case null or "":
                return await DemolishStepAsync(cluster, shard, op, ct);

            case "demolished":
                return await LaunchStepAsync(snap, shard, op, first, addr, receiverUrl,
                    addresses.Value, targetTime, ct);

            default:
                return await SuperviseRestoreOpAsync(
                    snap, shard, op, first, receiverUrl, addresses.Value, ct);
        }
    }

    // Хронометраж шагов restore-процесса (диагностика «почему restore долгий»,
    // вкл. параллельные заявки: демонтаж/ensure/tool-доставка/гейты). Лог —
    // только для шагов >2 с: молчание восстановлено для нормального хода.
    private async Task<T> TimedStepAsync<T>(
        string cluster, string shard, string step, Func<Task<T>> call)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var result = await call();
        sw.Stop();
        if (sw.Elapsed > TimeSpan.FromSeconds(2))
            logger?.LogWarning(
                "backup-restore {Cluster}/{Shard}: step {Step} took {Seconds:F1}s",
                cluster, shard, step, sw.Elapsed.TotalSeconds);
        return result;
    }

    private async Task<Result> TimedResultStepAsync(
        string cluster, string shard, string step, Func<Task<Result>> call)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var result = await call();
        sw.Stop();
        if (sw.Elapsed > TimeSpan.FromSeconds(2))
            logger?.LogWarning(
                "backup-restore {Cluster}/{Shard}: step {Step} took {Seconds:F1}s",
                cluster, shard, step, sw.Elapsed.TotalSeconds);
        return result;
    }

    // Шаг демонтажа (Phase null/""): снос нод шарда с data-volume + чистка
    // HA-scope + щит (см. DemolishAsync); факт — Phase="demolished" (тик
    // запуска продолжается следующим тиком — takeover между шагами безопасен:
    // демонтаж идемпотентен).
    private async Task<Result<ProcessOutcome>> DemolishStepAsync(
        string cluster, ShardSpec shard, RestoreOperationState op, CancellationToken ct)
    {
        var demolished = await TimedResultStepAsync(cluster, shard.Name, "demolish",
            () => DemolishAsync(cluster, shard, op, ct));
        if (!demolished.IsSuccess)
            return Result<ProcessOutcome>.Failed(demolished.Error!);

        // t15 (spec §4.6, решение №32б): ПЕРВЫЙ переход в demolished — инкремент
        // жизни цепочки (причина restore), идемпотентно на заявку (маркер времени
        // started_unix). Порядок обязателен: bump ДО записи фазы. Put фазы упал →
        // повтор пропустит bump по гейту времени и доедет put; bump упал → фаза
        // ещё не записана, повторный тик снова войдёт в демонтаж (идемпотентен).
        var life = await new WalLifeStore(etcd, endpoints).BumpAsync(
            cluster, shard.Name, WalLifeReason.Restore, startedUnix: op.StartedUnix, ct);
        if (!life.IsSuccess)
            return Result<ProcessOutcome>.Failed(life.Error!);

        var put = await PutStatusAsync(cluster, shard.Name, op with { Phase = "demolished" }, ct);
        if (!put.IsSuccess)
            return Result<ProcessOutcome>.Failed(put.Error!);
        return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
    }

    // Шаг запуска (Phase "demolished"): EnsureNode первой ноды + команда
    // restore её приёмнику.
    private async Task<Result<ProcessOutcome>> LaunchStepAsync(
        ClusterSnapshot snap, ShardSpec shard, RestoreOperationState op, string first,
        NodeAddress addr, string receiverUrl, IReadOnlyDictionary<string, NodeAddress> addresses,
        string? targetTime, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // BACKUP_ID — резолвнутый валидацией полный (op.BackupId), НЕ id
        // заявки: качается full/<backup_id>/.
        if (op.BackupId.Length == 0)
        {
            await FailPermanentAsync(cluster, shard.Name, op,
                "backup_id is not recorded in the request (expected after validation)", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        // Подъём первой ноды (EnsureNode — идемпотентен: живой контейнер с
        // планом портов не трогает). Щит initialize="restore-in-progress"
        // (DemolishAsync) удерживает Patroni от bootstrap пустого PGDATA —
        // постгрес Patroni не поднимает, PGDATA ноды свободен для операции
        // (arch/19 §3.5). Поллинг create/start — arch/14 §6 инвариант.
        var ensured = await TimedResultStepAsync(cluster, shard.Name, "ensure-first",
            () => EnsureShardNodeAsync(snap, shard, first, addr, addresses, ct));
        if (!ensured.IsSuccess)
            return await TransientAsync(cluster, $"docker-unavailable/{shard.Name}/{op.Id}",
                ensured.Error!.Message, ct);

        // t05 tool-доставка (arch/14 §4, spec §3.6-3а/§8-22 is_tool_start):
        // нода restore НЕ запускает компоненты — агент готовит параметры
        // (tmpfs-рендер по bootstrap-ключу, is_tool_start=true) и переводит
        // машину в ready (components=prepared). БЕЗ этого старт spilo после
        // restore упал бы fail-fast по пустой tmpfs (env-секретов больше нет).
        // Transient-ретрай тиком: delivery идемпотентен (put при изменении,
        // handshake повторно — ок-пинок); машина агента bounded (рендер).
        var toolCreds = await appSecret.EnsureAsync(cluster, snap.Config, ct);
        if (!toolCreds.IsSuccess)
            return await TransientAsync(cluster, $"bootstrap-tool/{shard.Name}/{first}",
                toolCreds.Error!.Message, ct);
        var toolTopology = Topology(cluster, shard.Name, addresses);
        var toolTuning = pgtune.Create(await ReadShardResourcesAsync(cluster, shard.Name, ct));
        var toolDelivery = await TimedStepAsync(cluster, shard.Name, "tool-delivery",
            () => bootstrap.EnsureAsync(
                toolTopology, first, addr, secrets, toolTuning, pgtuneSettings.ExcludeParams,
                snap.Config.SyncStrict, toolCreds.Value.RestPassword, isToolStart: true, ct));
        if (!toolDelivery.Delivered)
            return await TransientAsync(cluster, $"bootstrap-tool/{shard.Name}/{first}",
                toolDelivery.Detail ?? "bootstrap delivery is not complete (waiting)", ct);
        var toolStatusRead = await ReadStatusAsync(cluster, shard.Name, first, ct);
        if (!toolStatusRead.IsSuccess)
            return await TransientAsync(cluster, $"bootstrap-tool/{shard.Name}/{first}",
                toolStatusRead.Error!.Message, ct);
        if (toolStatusRead.Value?.State == "start_failed")
        {
            await FailPermanentAsync(cluster, shard.Name, op,
                "agent reported start_failed (tool parameters): " +
                (toolStatusRead.Value.Error ?? "no diagnostics"), ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }
        if (toolStatusRead.Value?.State != "ready")
            return await TransientAsync(cluster, $"bootstrap-tool/{shard.Name}/{first}",
                $"agent machine is {toolStatusRead.Value?.State ?? "absent"} (waiting for prepared)", ct);

        // Команда запуска restore-операции: backup_id / target_time (null =
        // latest) / recovery_timeout_sec / S3-контекст source_pool ?? своего
        // пула (prefix — DR-источник). Секреты — только в POST-теле (Р4).
        var poolRead = await ResolveReadPoolAsync(cluster, op, ct);
        if (!poolRead.IsSuccess)
        {
            await journal.WritePhaseAsync(cluster, Op, "waiting-backup-pool", claims.InstanceId,
                poolRead.Error!.Message, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }
        var poolCtx = poolRead.Value.Context;

        var launch = await TimedStepAsync(cluster, shard.Name, "start-restore",
            () => receiver.StartRestoreAsync(
                receiverUrl,
                new StartRestoreRequest(
                    BackupId: op.BackupId,
                    Id: null, // restore — не drill
                    TargetTime: targetTime,
                    RecoveryTimeoutSec: options.RestoreRecoveryTimeoutSec,
                    S3: new S3Context(
                        Endpoints: [.. poolCtx.ReadAll],
                        AccessKey: poolCtx.AccessKey,
                        SecretKey: poolCtx.SecretKey,
                        Bucket: options.S3Bucket,
                        Prefix: $"{SrcCluster(op)}/{SrcShard(op)}",
                        CaPem: BackupCaPem.ReadIfConfigured(options.S3CaBundlePath)),
                    // t15: номер жизни выбранного полного → fetch-wal фильтрует
                    // куски по нему (DR-orphan — null, фильтр не активен).
                    ChainLife: op.ChainLife), ct));
        if (launch.IsConflict)
        {
            // 409 — в общем ops-реестре приёмника ЧУЖАЯ ops-операция (verify
            // свежего полного; одна ops-операция на процесс, arch/19 §5).
            // Снимок 409 не различает свою/чужую — принять 409 за «свою живую»
            // НЕЛЬЗЯ: verify затирает снимки общего реестра, и воркер уходит в
            // REJOINING от чужого succeeded БЕЗ system_id (щит-заполнитель
            // навсегда запирает bootstrap, firstReady недостижим). Запуск
            // откладывается: фаза остаётся «demolished», повтор следующим
            // тиком до освобождения реестра (arch/19 §3.5).
            return await TransientAsync(cluster, $"receiver-busy/{shard.Name}/{op.Id}",
                "restore launch deferred: the receiver ops-registry is busy (verify/drill in flight)", ct);
        }
        if (!launch.IsAccepted)
        {
            // Транспортный отказ/5xx команды — transient: статус не меняем
            // (фаза "demolished" остаётся — команда повторится), следующий тик
            // повторит POST.
            return await TransientAsync(cluster, $"receiver-unavailable/{shard.Name}/{op.Id}",
                launch.Error, ct);
        }

        // Команда принята — фаза "launching": следующий тик поллит GET
        // (перезапуск команды запрещён — см. комментарий свитча фаз).
        var putPhase = await PutStatusAsync(cluster, shard.Name, op with { Phase = "launching" }, ct);
        if (!putPhase.IsSuccess)
            return Result<ProcessOutcome>.Failed(putPhase.Error!);
        return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
    }

    // Статусная машина агента ноды (t05 tool-доставка): null-факт — не сбой
    // (агент стартует) — вызывающий решает (transient-ретрай тиком).
    private async Task<Result<PgNodeStatusFact?>> ReadStatusAsync(
        string cluster, string shard, string node, CancellationToken ct)
    {
        Result<Kv?> last = default;
        foreach (var endpoint in endpoints)
        {
            var read = await etcd.GetAsync(endpoint,
                PgNodeStatusFact.StatusKey(cluster, shard, node), ct);
            if (!read.IsSuccess)
            {
                last = read;
                continue;
            }

            if (read.Value is null)
                return Result<PgNodeStatusFact?>.Success(null);
            return Result<PgNodeStatusFact?>.Success(
                PgNodeStatusFact.Parse(read.Value.Value) is { } fact
                    ? Result<PgNodeStatusFact?>.Success(fact).Value
                    : null);
        }

        return Result<PgNodeStatusFact?>.Failed(
            last.Error ?? new ApplicationException("etcd endpoints are not set"));
    }

    // EnsureNode одной ноды шарда (тик запуска restore/rejoin): идемпотентно,
    // поллинг create/start (arch/14 §6 инвариант), бюджет — PatroniBootSec.
    // Креды (P1.5-образец): ensured тройка — env ноды; REST-пара — ключ
    // etcd (arch/14 §5 I ETCD-FIRST).
    private async Task<Result> EnsureShardNodeAsync(
        ClusterSnapshot snap, ShardSpec shard, string node, NodeAddress addr,
        IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;
        var creds = await appSecret.EnsureAsync(cluster, snap.Config, ct);
        if (!creds.IsSuccess)
            return creds;
        var topology = Topology(cluster, shard.Name, addresses);
        var resources = await ReadShardResourcesAsync(cluster, shard.Name, ct);
        var work = await journal.ReadAsync(cluster, ct);
        if (!work.IsSuccess)
            return work;
        // arch/14 §2.1: тюнинг — пересчёт от заявки на КАЖДОМ EnsureNode-пути.
        var tuning = pgtune.Create(resources);
        return await LongCallPolling.EnsureAsync(
            $"create/start of node {node}",
            token => driver.EnsureNodeAsync(
                topology, node, addr, etcdEndpoints, resources, tuning,
                snap.Config.SyncStrict, token),
            progress, logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<RestoreProcess>.Instance,
            TimeSpan.FromTicks(Math.Max(TimeSpan.TicksPerSecond, WatchdogWindow.Ticks / 2)),
            TimeSpan.FromSeconds(thresholds.PatroniBootSec), ct);
    }

    // Супервиз restore-операции приёмника первой ноды (§3.5): итог — ТОЛЬКО
    // по API (GetRestoreAsync).
    private async Task<Result<ProcessOutcome>> SuperviseRestoreOpAsync(
        ClusterSnapshot snap, ShardSpec shard, RestoreOperationState op, string first,
        string receiverUrl, IReadOnlyDictionary<string, NodeAddress> addresses,
        CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;
        var reply = await receiver.GetRestoreAsync(receiverUrl, ct);

        if (reply.IsNotFound)
        {
            // Нода жива, операции нет (рестарт приёмника) — transient: фаза
            // обратно в "demolished" (без повторного демонтажа: volume уже
            // чистый, EnsureNode no-op) — командой повтором следующим тиком;
            // резюмов нет — скачивание заново (Р9).
            var putReset = await PutStatusAsync(cluster, shard.Name, op with { Phase = "demolished" }, ct);
            if (!putReset.IsSuccess)
                return Result<ProcessOutcome>.Failed(putReset.Error!);
            return await TransientAsync(cluster, $"receiver-vanished/{shard.Name}/{op.Id}",
                "restore operation vanished — command re-sent on next tick", ct);
        }

        if (reply.IsTransient)
            return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress); // ждём

        var status = reply.Status;
        if (status is null)
            return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);

        switch (status.State)
        {
            case "running":
            {
                // Фаза операции → Phase статуса etcd (панель видит
                // downloading/recovering, arch/19 §4); вместе с фазой — факт
                // её перехода PhaseUnix (стенка наката — от перехода).
                var current = op;
                if (status.Phase is { Length: > 0 } phase && phase != op.Phase)
                {
                    current = op with { Phase = phase, PhaseUnix = NowUnix() };
                    var putPhase = await PutStatusAsync(cluster, shard.Name, current, ct);
                    if (!putPhase.IsSuccess)
                        return Result<ProcessOutcome>.Failed(putPhase.Error!);
                }

                // Стенка наката — ТОЛЬКО в фазе recovering и от ПЕРЕХОДА в неё
                // (PhaseUnix): скачивание набора (downloading) recovery-бюджет
                // не съедает — долгий download на здоровой операции больше не
                // даёт premature FAILED «recovery budget exhausted»; он под
                // статусным бюджетом заявки Job:RestoreTimeoutSec (выше) и
                // transient-семантикой download-retry. Внутри recovering
                // приёмник держит свой бюджет RecoveryTimeoutSec — воркерская
                // стенка (запас 60 c) — backstop зависшего приёмника. Ключ без
                // PhaseUnix (в момент обновления воркера) — от started_unix
                // (прежнее поведение).
                var recoveringUnix = current.PhaseUnix ?? current.StartedUnix;
                if (current.Phase == "recovering" && recoveringUnix is { } wallFrom)
                {
                    var budgetSec = options.RestoreRecoveryTimeoutSec + 60;
                    if (NowUnix() - wallFrom > budgetSec)
                    {
                        await driver.RemoveNodeAsync(cluster, shard.Name, first, ct);
                        await FailPermanentAsync(cluster, shard.Name, op,
                            $"recovery budget exhausted ({budgetSec}s)", ct);
                        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
                    }
                }

                // §8-28 (arch/19 §3.5) — ПАРАЛЛЕЛЬНАЯ ДОСТАВКА rejoin-нод: пока
                // джоба восстанавливает первичку (running), rejoin-ноды шарда
                // поднимаются (EnsureNode, новый volume) и получают bootstrap-
                // ключи/хендшейк БЕЗ ожидания конца восстановления: их Patroni
                // легитимно держит initializing до initialize/лидера (щит
                // демонтажа не даёт initdb). Первичка — в собственном порядке
                // (REJOINING-путь ниже), параллельный путь её не трогает.
                // Такты — те же transient-ретраи: EnsureNode идемпотентен,
                // повторный хендшейк — Nop для машины не в not_initialized.
                await RejoinPreflightAsync(cluster, shard, snap, op, ct);

                return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress); // жив — ждём
            }

            case "succeeded":
            {
                // SUCCESS: REJOINING; RestoredToLsn/SystemId — из статуса API.
                // Рестарта ноды НЕТ: restore-операция сама вернула Patroni
                // (start spilo в finally) уже с валидным initialize и
                // остановленной промоутнутой базой — Patroni поднимает её и
                // доводит recovery до промоута. Volume первой ноды содержит
                // восстановленный PGDATA — НЕ трогаем.
                var rejoining = op with
                {
                    State = RestoreStatus.Rejoining,
                    Phase = null,
                    RestoredToLsn = status.RestoredToLsn,
                    SystemId = status.SystemId,
                };
                var put = await PutStatusAsync(cluster, shard.Name, rejoining, ct);
                if (!put.IsSuccess)
                    return Result<ProcessOutcome>.Failed(put.Error!);
                await journal.WritePhaseAsync(cluster, Op, $"restored/{shard.Name}/{op.Id}",
                    claims.InstanceId, null, ct);
                return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
            }

            case "cancelled":
            {
                // Операторская отмена (POST /api/ops/cancel приёмника —
                // внешний канал мимо воркера; снимок state=cancelled отличает
                // её от vanished): всё как до старта операции — первая нода
                // сносится вместе с data-volume (до-restore «пустое»
                // состояние демонтажа), щит initialize снимается (заполнитель
                // «restore-in-progress» навсегда запер бы bootstrap пустых
                // нод), запись заявки УДАЛЯЕТСЯ — операции нет, записи нет;
                // никакого FAILED (arch/19 §3.5).
                await driver.RemoveNodeAsync(cluster, shard.Name, first, ct);
                var initKey = $"/service/{cluster}-{shard.Name}/initialize";
                var init = await etcd.GetAsync(endpoints[0], initKey, ct);
                if (init.IsSuccess && init.Value?.Value == "restore-in-progress")
                {
                    var delShield = await etcd.DeleteAsync(endpoints[0], initKey, prefix: false, ct);
                    if (!delShield.IsSuccess)
                        logger?.LogError(
                            "backup-restore {Cluster}/{Shard}/{Id}: initialize placeholder was not deleted — {Error}",
                            cluster, shard.Name, op.Id, delShield.Error?.Message);
                }
                var delOp = await etcd.DeleteAsync(endpoints[0],
                    BackupNames.RestoreKey(cluster, shard.Name, op.Id), prefix: false, ct);
                if (!delOp.IsSuccess)
                    return Result<ProcessOutcome>.Failed(delOp.Error!);
                await journal.WritePhaseAsync(cluster, Op,
                    $"cancelled-by-operator/{shard.Name}/{op.Id}", claims.InstanceId,
                    "restore operation cancelled by operator — request removed, shard back to pre-restore state", ct);
                logger?.LogInformation(
                    "backup-restore {Cluster}/{Shard}/{Id}: cancelled by operator — request removed, shard back to pre-restore state",
                    cluster, shard.Name, op.Id);
                return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
            }

            case "failed" when status.Phase == "downloading":
            {
                // МАРКЕР ТРАНЗИЕНТА: сбой скачивания — статус не трогаем
                // (RUNNING остаётся), фаза обратно в "demolished" (EnsureNode
                // no-op — нода уже наша); следующий тик повторит POST (операция
                // начнётся заново — резюмов нет; недокачанные файлы набора
                // перезаписываются — набор детерминирован).
                var putReset = await PutStatusAsync(cluster, shard.Name, op with { Phase = "demolished" }, ct);
                if (!putReset.IsSuccess)
                    return Result<ProcessOutcome>.Failed(putReset.Error!);
                return await TransientAsync(cluster, $"download-retry/{shard.Name}/{op.Id}",
                    status.Error ?? "download failed", ct);
            }

            case "failed":
            {
                // PERMANENT (phase=recovering): причина из статуса; volume
                // первой ноды удаляется вместе с нодой (мог остаться битым —
                // restoration = конец, шард остаётся разобранным, разбор по
                // runbook).
                var error = status.Error ?? "restore operation failed";
                await driver.RemoveNodeAsync(cluster, shard.Name, first, ct);
                await FailPermanentAsync(cluster, shard.Name, op, error, ct);
                return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
            }

            default:
                return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress); // незнакомое — ждём
        }
    }

    // Демонтаж шарда под restore (§3.4, идемпотентно — каждый шаг 404 = ок):
    // агенты бэкапов сносятся первыми (WAL-агент на снесённом мастере не живёт),
    // ноды удаляются с data-volume («погиб диск»), HA-scope Patroni чистится
    // (Д3-образец ProvisioningProcess.ResetScopeAsync; дубль осознан — прецедент
    // кодовой базы); request_* НЕ трогаем (заявка ресурсов — декларация).
    private async Task<Result> DemolishAsync(
        string cluster, ShardSpec shard, RestoreOperationState op, CancellationToken ct)
    {
        // Живая сверка клэйма перед сносом нод шарда (arch/17 S5).
        if (!await ClaimAliveAsync(cluster, shard.Name, op, ct))
            return Result.Failed(new ApplicationException(
                $"{Op} {cluster}: claim lost before demolish of {shard.Name}/{op.Id}"));

        var agents = await driver.RemoveBackupAgentsAsync(cluster, shard.Name, ct);
        if (!agents.IsSuccess)
            return agents;

        // Ноды демонтируются НЕЗАВИСИМО (свои контейнеры/тома) — отдельные таски,
        // шаг ждёт все: последовательный снос суммировал docker-время нод.
        async Task<Result> RemoveOneAsync(NodeSpec node)
        {
            // REBUILDING — видимый статус демонтажа (панель/гварды), Removing не трогаем.
            if (node.State != NodeState.Removing)
            {
                var put = await etcd.PutAsync(endpoints[0],
                    $"/clusters/{cluster}/shards/{shard.Name}/nodes/{node.Name}/state",
                    "REBUILDING", null, ct);
                if (!put.IsSuccess)
                    return put;
            }
            return await driver.RemoveNodeAsync(cluster, shard.Name, node.Name, ct);
        }
        foreach (var removed in await Task.WhenAll(shard.Nodes.Select(RemoveOneAsync)))
            if (!removed.IsSuccess)
                return removed;

        var scope = $"{cluster}-{shard.Name}";
        // status чистим ОБЯЗАТЕЛЬНО: в нём optime прошлой жизни лидера; без чистки
        // восстановленная нода (LSN = точке restore, ниже старого optime при PITR-откате)
        // «отстаёт» больше maximum_lag_on_failover и навечно отказывается избираться
        // failover чистим ОБЯЗАТЕЛЬНО: остаточный маркер (надзор успел ускорить
        // failover в окне демонтажа — гвард на шаге мутации, arch/19 §3.5) с
        // scheduled_at в прошлом для Patroni новой жизни — «accelerated failover»
        // на неинициализированного кандидата: восстановленная нода не лидерится
        // (первыйReady-гейт rejoin недостижим).
        // Ключи HA-scope независимы — удаляются параллельно (таски шага).
        var scopeDeletes = await Task.WhenAll(
            new[] { "initialize", "leader", "sync", "status", "failover" }
                .Select(key => etcd.DeleteAsync(
                    endpoints[0], $"/service/{scope}/{key}", prefix: false, ct)));
        foreach (var del in scopeDeletes)
            if (!del.IsSuccess)
                return del;
        foreach (var prefix in new[] { $"/service/{scope}/optime/", $"/service/{scope}/members/" })
        {
            var del = await etcd.DeleteAsync(endpoints[0], prefix, prefix: true, ct);
            if (!del.IsSuccess)
                return del;
        }

        // Щит против re-bootstrap (инцидент E2E-гейта, 2026-09-13): в окне
        // демонтаж→джоба→rejoin (~15–30 c) пустая нода, поднятая кем-то параллельно
        // (супервиз/гонка тиков), без initialize успевает initdb'нуться и стать
        // лидером НОВОГО пустого кластера — восстановленная навечно получает
        // «system ID mismatch». Заполнитель закрывает бутстрап; в rejoin'е его
        // перезапишет настоящий system_id восстановленного PGDATA.
        var shield = await etcd.PutAsync(
            endpoints[0], $"/service/{scope}/initialize", "restore-in-progress", null, ct);
        if (!shield.IsSuccess)
            return shield;

        return await journal.WritePhaseAsync(cluster, Op, $"demolished/{shard.Name}/{op.Id}",
            claims.InstanceId, null, ct);
    }

    // Source «<srcC>/<srcX>»: компоненты для env спеки (валидация PLANNED уже
    // проверила формат).
    private static string SrcCluster(RestoreOperationState op)
        => op.Source.Split('/', 2)[0];

    private static string SrcShard(RestoreOperationState op)
        => op.Source.Split('/', 2)[1];

    // ── REJOINING: ensure нод + Patroni-пробы + COMPLETED (§3.4, AC4) ──

    // Трекер ожидания Patroni (диагностика takeover: состояние в etcd-статусе
    // заявки, трекер — только бюджет ожидания; прецедент _patroniWaitSince).
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, long> _rejoinWaitSince = new();

    // §8-28 (arch/19 §3.5): параллельная доставка rejoin-нод в фазе running —
    // EnsureNode (новый volume) + bootstrap-ключи + команда доставки БЕЗ
    // ожидания конца восстановления первички. Идемпотентно: EnsureNode не
    // трогает живой контейнер, EnsureAsync ставит ключ при изменении,
    // повторный хендшейк — Nop для машины не в not_initialized. Ошибки узла —
    // transient-лог (следующий тик повторит); тик не роняют.
    private async Task RejoinPreflightAsync(
        string cluster, ShardSpec shard, ClusterSnapshot snap,
        RestoreOperationState op, CancellationToken ct)
    {
        // Живая сверка клэйма перед МУТАЦИЯМИ preflight (arch/17 S5, образец
        // Demolish/Rejoin): EnsureNode/put/хендшейк без клэйма — мутации
        // потерявшего клэйм инстанса.
        if (!await ClaimAliveAsync(cluster, shard.Name, op, ct))
        {
            await journal.WritePhaseAsync(cluster, Op,
                "rejoin-preflight/claim-lost", claims.InstanceId,
                "claim lost before rejoin-preflight — no mutations", ct);
            return;
        }

        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
        {
            await journal.WritePhaseAsync(cluster, Op, "rejoin-preflight/portalloc",
                claims.InstanceId, addresses.Error!.Message, ct);
            return;
        }

        var topology = Topology(cluster, shard.Name, addresses.Value);
        var ordered = shard.Nodes
            .Where(n => n.Name != shard.Nodes.Select(m => m.Name).OrderBy(m => m, StringComparer.Ordinal).First())
            .Select(n => n.Name).ToList(); // все, кроме первички (её порядок — собственный)
        var creds = await appSecret.EnsureAsync(cluster, snap.Config, ct);
        if (!creds.IsSuccess)
        {
            await journal.WritePhaseAsync(cluster, Op, "rejoin-preflight/creds",
                claims.InstanceId, creds.Error!.Message, ct);
            return;
        }

        foreach (var node in ordered)
        {
            if (!addresses.Value.TryGetValue($"{shard.Name}/{node}", out var addr))
                continue;
            var resources = await ReadShardResourcesAsync(cluster, shard.Name, ct);
            var tuning = pgtune.Create(resources);
            var ensured = await driver.EnsureNodeAsync(
                topology, node, addr, etcdEndpoints, resources, tuning,
                snap.Config.SyncStrict, ct);
            if (!ensured.IsSuccess)
            {
                await journal.WritePhaseAsync(cluster, Op,
                    $"rejoin-preflight/ensure/{shard.Name}/{node}",
                    claims.InstanceId, ensured.Error!.Message, ct);
                continue;
            }

            var delivery = await bootstrap.EnsureAsync(
                topology, node, addr, secrets, tuning, pgtuneSettings.ExcludeParams,
                snap.Config.SyncStrict, creds.Value.RestPassword, isToolStart: false, ct);
            if (delivery.Transient || !delivery.Delivered)
                await journal.WritePhaseAsync(cluster, Op,
                    $"rejoin-preflight/delivery/{shard.Name}/{node}",
                    claims.InstanceId,
                    delivery.Detail ?? "bootstrap delivery is not complete (waiting)", ct);
        }
    }

    private async Task<Result<ProcessOutcome>> RejoinAsync(
        ClusterSnapshot snap, ShardSpec shard, RestoreOperationState op, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;
        var waitKey = $"{cluster}/{shard.Name}/{op.Id}";

        // Живая сверка клэйма перед фазой реинтеграции нод (arch/17 S5).
        if (!await ClaimAliveAsync(cluster, shard.Name, op, ct))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"{Op} {cluster}: claim lost before rejoin of {shard.Name}/{op.Id}"));

        // Креды (P1.5-копия): ensured тройка — env нод (mover/bucket_admin —
        // канонические ключи etcd).
        var creds = await appSecret.EnsureAsync(cluster, snap.Config, ct);
        if (!creds.IsSuccess)
            return Result<ProcessOutcome>.Failed(creds.Error!);

        // Адреса/топология/заявка ресурсов (portalloc жив — restore его не трогал).
        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return await TransientAsync(cluster, $"portalloc-unavailable/{shard.Name}/{op.Id}",
                addresses.Error!.Message, ct);
        var topology = Topology(cluster, shard.Name, addresses.Value);
        var resources = await ReadShardResourcesAsync(cluster, shard.Name, ct);

        // Первая нода — на volume с восстановленным PGDATA (джоб уже
        // записал его в pgw-<C>-<X>-<n>-data; docker смонтирует существующий).
        var ordered = shard.Nodes.Select(n => n.Name).OrderBy(n => n, StringComparer.Ordinal).ToList();
        var first = ordered[0];
        if (!addresses.Value.TryGetValue($"{shard.Name}/{first}", out var firstAddr))
        {
            await FailPermanentAsync(cluster, shard.Name, op,
                $"node {first} of shard {shard.Name} not found in portalloc", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }
        // Щит initialize (см. DemolishAsync): заполнитель «restore-in-progress»
        // перезаписывается system_id восстановленного PGDATA ДО ensure первой
        // ноды — patroni восстановленной ноды видит совпадение и лидерится, а
        // пустые ноды в гонке initdb'нуться не могут. Старый образ джоба не
        // присылает system_id — щит остаётся заполнителем (только warn).
        if (op.SystemId is { Length: > 0 } sysid)
        {
            var putShield = await etcd.PutAsync(
                endpoints[0], $"/service/{cluster}-{shard.Name}/initialize", sysid, null, ct);
            if (!putShield.IsSuccess)
                return await TransientAsync(cluster, $"etcd-unavailable/{shard.Name}/{op.Id}",
                    putShield.Error!.Message, ct);
        }
        else
        {
            logger?.LogWarning(
                "backup-restore {Cluster}/{Shard}/{Op}: system_id not received from the job — initialize keeps the placeholder",
                cluster, shard.Name, op.Id);
        }

        // Санитизация failover-маркера (прецедент NodeSupervisor — «хвост
        // failover-first ускорения»): демонтаж чистит HA-scope с failover
        // (arch/19 §3.5), а гвард надзора на шаге мутации закрывает гонку
        // «долгий тик надзора, начавшийся до заявки»; маркер, всё же
        // положенный в окне демонтаж→rejoin, для Patroni восстановленной
        // ноды — «accelerated failover» на неинициализированного кандидата:
        // она не лидерится, firstReady ждёт бюджет впустую. Удаление на
        // КАЖДОМ тике rejoin (идемпотентно, отсутствие ключа = успех) —
        // лечит и поздние маркеры; P2.2-проба ниже читает scope после чистки.
        var delFailover = await etcd.DeleteAsync(endpoints[0],
            $"/service/{cluster}-{shard.Name}/failover", prefix: false, ct);
        if (!delFailover.IsSuccess)
            return Result<ProcessOutcome>.Failed(delFailover.Error!);

        // Поллинг create/start (arch/14 §6 инвариант поллинга): одиночный вызов
        // таймаутом короче окна проверки (половина), идемпотентный повтор;
        // бюджет — существующий порог PatroniBootSec. : bootstrap
        // восстановленной ноды несёт strict кластера.
        // REST-пара restore-пути — ключ etcd (arch/14 §5 I ETCD-FIRST).
        var work = await journal.ReadAsync(cluster, ct);
        if (!work.IsSuccess)
            return await TransientAsync(cluster, $"docker-unavailable/{shard.Name}/{op.Id}",
                work.Error!.Message, ct);
        var rejoinTuning = pgtune.Create(resources);
        var firstEnsure = await TimedStepAsync(cluster, shard.Name, "rejoin-ensure-first",
            () => LongCallPolling.EnsureAsync(
                $"create/start of node {first}",
                token => driver.EnsureNodeAsync(
                    topology, first, firstAddr, etcdEndpoints, resources, rejoinTuning,
                    snap.Config.SyncStrict, token),
                progress, logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<RestoreProcess>.Instance,
                TimeSpan.FromTicks(Math.Max(TimeSpan.TicksPerSecond, WatchdogWindow.Ticks / 2)),
                TimeSpan.FromSeconds(thresholds.PatroniBootSec), ct));
        if (!firstEnsure.IsSuccess)
            return await TransientAsync(cluster, $"docker-unavailable/{shard.Name}/{op.Id}",
                firstEnsure.Error!.Message, ct);

        // t05 bootstrap-доставка rejoin-нод (spec §3.6-3а): EnsureNode создаёт
        // контейнер БЕЗ секретов в env — агент ждёт пина хендшейка. Надзор шард
        // в restore пропускает («restore in progress») — доставку делает
        // restore-процесс, иначе ноды навсегда ждут handshake и Patroni-состав
        // не сходится (rejoin-wait выгорает бюджет). Тюнинг null: bootstrap-ключ
        // ноды в etcd жив (provisioning) — рендер не нужен, ensure повторяет его.
        var rejoinDelivery = await TimedStepAsync(cluster, shard.Name, "rejoin-delivery-first",
            () => bootstrap.EnsureAsync(
                topology, first, firstAddr, secrets, rejoinTuning, pgtuneSettings.ExcludeParams,
                snap.Config.SyncStrict, creds.Value.RestPassword, isToolStart: false, ct));
        if (!rejoinDelivery.Delivered)
            return await TransientAsync(cluster, $"bootstrap-rejoin/{shard.Name}/{first}",
                rejoinDelivery.Detail ?? "bootstrap delivery is not complete (waiting)", ct);

        // Идентифицирующая Patroni-проба (P2.2-образец): лидер восстановленного
        // шарда должен быть ВЫБРАН (role primary) до подъёма реплик. state=running
        // недостаточно: джоба промоутила PG до Patroni, «running» бывает мгновенно,
        // до победы в выборах — ранний подъём реплик ломает выборы навечно
        // (не-избранный лидер уходит в following, «not the healthiest»).
        var members = await probe.GetClusterAsync(firstAddr, ct);
        var firstReady = members.IsSuccess
                         && members.Value.Any(m => m.Name == first
                             && m.Role is "leader" or "primary" or "standby_leader"
                             && m.State is "running" or "streaming");
        if (!firstReady)
            return await RejoinWaitAsync(cluster, shard.Name, op, waitKey, firstAddr, ct);

        // Остальные ноды — чистыми (драйвер создаст volume, реплики догоняются
        // pg_basebackup от лидера). Реплики НЕЗАВИСИМЫ: ensure + bootstrap-
        // доставка каждой — отдельная таска, тик ждёт все (Task.WhenAll);
        // последовательный обход суммировал их время.
        async Task<Result> RejoinReplicaAsync(string node, NodeAddress addr)
        {
            // Поллинг create/start реплики — тот же инвариант (arch/14 §6).
            var ensured = await TimedStepAsync(cluster, shard.Name, $"rejoin-ensure-{node}",
                () => LongCallPolling.EnsureAsync(
                    $"create/start of node {node}",
                    token => driver.EnsureNodeAsync(
                        topology, node, addr, etcdEndpoints, resources, rejoinTuning,
                        snap.Config.SyncStrict, token),
                    progress, logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<RestoreProcess>.Instance,
                    TimeSpan.FromTicks(Math.Max(TimeSpan.TicksPerSecond, WatchdogWindow.Ticks / 2)),
                    TimeSpan.FromSeconds(thresholds.PatroniBootSec), ct));
            if (!ensured.IsSuccess)
                return Result.Failed(ensured.Error!);

            // t05 bootstrap-доставка реплики rejoin (см. выше — надзор шард в
            // restore пропускает): пин хендшейка → агент стартует компоненты →
            // реплика догоняет лидера.
            var replicaDelivery = await TimedStepAsync(cluster, shard.Name, $"rejoin-delivery-{node}",
                () => bootstrap.EnsureAsync(
                    topology, node, addr, secrets, rejoinTuning, pgtuneSettings.ExcludeParams,
                    snap.Config.SyncStrict, creds.Value.RestPassword, isToolStart: false, ct));
            return replicaDelivery.Delivered
                ? Result.Success()
                : Result.Failed(new ApplicationException(
                    replicaDelivery.Detail ?? "bootstrap delivery is not complete (waiting)"));
        }

        var replicaSteps = new List<Task<Result>>();
        foreach (var node in ordered.Skip(1))
        {
            if (!addresses.Value.TryGetValue($"{shard.Name}/{node}", out var addr))
            {
                await FailPermanentAsync(cluster, shard.Name, op,
                    $"node {node} of shard {shard.Name} not found in portalloc", ct);
                return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
            }
            replicaSteps.Add(RejoinReplicaAsync(node, addr));
        }
        foreach (var replica in await Task.WhenAll(replicaSteps))
            if (!replica.IsSuccess)
                return await TransientAsync(cluster, $"bootstrap-rejoin/{shard.Name}/{op.Id}",
                    replica.Error!.Message, ct);

        // Пробы всех нод (тот же трекер-бюджет). Контракт (решение, 2026-09-12):
        // COMPLETED ждёт ЛИДЕР running/streaming + реплики, УСПЕШНО СТАРТОВАВШИЕ
        // синхронизацию — "creating replica" (pg_basebackup от лидера пошёл) или
        // уже готовые ("running"/"streaming"). Полного окончания basebackup ждёт
        // не заявка, а Patroni (свой retry) — rejoin на нём не висит. Patroni 4.x
        // у healthy-реплики отдаёт state "streaming" (инцидент E2E-гейта).
        var clusterState = await probe.GetClusterAsync(firstAddr, ct);
        var allStarted = clusterState.IsSuccess
                         && ordered.All(n => clusterState.Value.Any(
                             m => m.Name == n
                                  && m.State is "running" or "streaming" or "creating replica"));
        if (!allStarted)
            return await RejoinWaitAsync(cluster, shard.Name, op, waitKey, firstAddr, ct);

        // SQL-гейт мастера (arch/19 §3.5): Patroni-пробы живы и в
        // переходном окне рестарта postmaster (57P01 у клиентов), а COMPLETED
        // обязан означать закрытое окно — после него планировщик полных
        // немедленно стартует пересъём, тесты/панель читают. Проба фактического
        // постгреса ПЕРВОЙ ноды (firstReady выше уже требует её лидерство):
        // pg_is_in_recovery()=false закрывает и crash recovery после рестарта
        // (Patroni /primary там уже 200). Запрос БЕЗ ::text — Npgsql-скаляр
        // PG-boolean приходит boxed-bool и сравнивается с false: строковый
        // контракт «"f"» не сработал на PG18 (boolean::text там «false», а не
        // «f», как в PG≤17) — осознанное уточнение spec §4.2 по фактам
        // прогона E2E (решение пользователя, 2026-09-15).
        var adminDsn = ShardEndpoints.AdminDsn(firstAddr, snap.Config.DbName, secrets);
        var masterProbe = await TimedStepAsync(cluster, shard.Name, "sql-gate",
            () => db.ExecuteScalarAsync(adminDsn, "SELECT pg_is_in_recovery()", ct));
        if (masterProbe is not { IsSuccess: true, Value: false })
            return await MasterSqlWaitAsync(cluster, shard.Name, op, waitKey, firstAddr,
                masterProbe.IsSuccess ? "pg_is_in_recovery != false" : masterProbe.Error!.Message, ct);

        // COMPLETED (AC4): ноды RUNNING; wal-ключ шарда удаляется — сброс цепочки,
        // планировщик немедленно переснимает полный; мастер-ключ обновит сам
        // лидер (lease-скрипт P11) — RestoreProcess его не пишет.
        foreach (var node in ordered)
        {
            var put = await etcd.PutAsync(endpoints[0],
                $"/clusters/{cluster}/shards/{shard.Name}/nodes/{node}/state", "RUNNING", null, ct);
            if (!put.IsSuccess)
                return Result<ProcessOutcome>.Failed(put.Error!);
        }

        var completed = op with
        {
            State = RestoreStatus.Completed,
            FinishedUnix = NowUnix(),
        };
        var putDone = await PutStatusAsync(cluster, shard.Name, completed, ct);
        if (!putDone.IsSuccess)
            return Result<ProcessOutcome>.Failed(putDone.Error!);
        restoreObserver?.Invoke(cluster, shard.Name, "ok"); // COMPLETED зафиксирован в etcd
        var delWal = await etcd.DeleteAsync(endpoints[0],
            $"/pgworker/backups/{cluster}/{shard.Name}/wal", prefix: false, ct);
        if (!delWal.IsSuccess)
            return Result<ProcessOutcome>.Failed(delWal.Error!);
        await journal.WritePhaseAsync(cluster, Op, $"done/{shard.Name}/{op.Id}", claims.InstanceId, null, ct);
        _rejoinWaitSince.TryRemove($"{waitKey}/sql", out _);
        _rejoinWaitSince.TryRemove(waitKey, out _);
        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // Бюджет ожидания Patroni (PatroniBootSec): не готово → InProgress; исчерпан
    // → permanent-FAILED (трекер снять — новая заявка получает полный бюджет).
    // Телеметрия (гейт 2026-09-13): каждый 10-й тик пишет warn с живым
    // снимком пробы — журнал воркера обязан объяснять «почему так долго» без
    // перезапуска теста (в терминальном FAILED он приходит слишком поздно).
    private async Task<Result<ProcessOutcome>> RejoinWaitAsync(
        string cluster, string shard, RestoreOperationState op, string waitKey,
        NodeAddress? firstAddr, CancellationToken ct)
    {
        var now = NowUnix();
        var since = _rejoinWaitSince.GetOrAdd(waitKey, now);
        var waited = now - since;
        if (waited > thresholds.PatroniBootSec)
        {
            _rejoinWaitSince.TryRemove(waitKey, out _);
            // Диагностика в причине: снимок Patroni-модели на момент фейла —
            // кто лидер, в каком состоянии ноды (иначе фейл нем и флейки нечему учить)
            var snapshot = "";
            if (firstAddr is { } addr)
            {
                var diag = await probe.GetClusterAsync(addr, ct);
                snapshot = diag.IsSuccess
                    ? "; members=" + string.Join(",",
                        diag.Value.Select(m => $"{m.Name}:{m.Role}:{m.State}"))
                    : "; probe=" + diag.Error!.Message;
            }
            await FailPermanentAsync(cluster, shard, op,
                $"Patroni did not come up within {thresholds.PatroniBootSec}s{snapshot}", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        // Каждый 10-й тик (тик ≈ 1 c): живой снимок пробы в журнал воркера.
        if (waited > 0 && waited % 10 == 0)
        {
            var progress = "";
            if (firstAddr is { } addr)
            {
                var diag = await probe.GetClusterAsync(addr, ct);
                progress = diag.IsSuccess
                    ? "members=" + string.Join(",", diag.Value.Select(m => $"{m.Name}:{m.Role}:{m.State}"))
                    : "probe=" + diag.Error!.Message;
            }

            logger?.LogWarning(
                "backup-restore {Cluster}/{Shard}/{Op}: rejoin-wait {Waited}s/{Budget}s, probe {Addr}: {Progress}",
                cluster, shard, op.Id, waited, thresholds.PatroniBootSec, firstAddr?.ToString() ?? "-", progress);
        }

        return await TransientAsync(cluster, $"rejoin-wait/{shard}/{op.Id}", null, ct);
    }

    // Бюджет SQL-ожидания мастера : образец RejoinWaitAsync — тот же
    // трекер _rejoinWaitSince (суффикс «/sql» отделяет от Patroni-ожидания),
    // тот же бюджет PatroniBootSec; телеметрия каждые ~10 тиков — окно обязано
    // объясняться журналом без перезапуска. Никаких мутаций до готовности
    // (TransientAsync — тик повторит пробу). Бюджет исчерпан → permanent-FAILED
    // со снимком Patroni-диагностики (по образцу RejoinWaitAsync).
    private async Task<Result<ProcessOutcome>> MasterSqlWaitAsync(
        string cluster, string shard, RestoreOperationState op, string waitKey,
        NodeAddress? firstAddr, string? lastError, CancellationToken ct)
    {
        var sqlKey = $"{waitKey}/sql";
        var now = NowUnix();
        var since = _rejoinWaitSince.GetOrAdd(sqlKey, now);
        var waited = now - since;
        if (waited > thresholds.PatroniBootSec)
        {
            _rejoinWaitSince.TryRemove(sqlKey, out _);
            var snapshot = "";
            if (firstAddr is { } addr)
            {
                var diag = await probe.GetClusterAsync(addr, ct);
                snapshot = diag.IsSuccess
                    ? "; members=" + string.Join(",",
                        diag.Value.Select(m => $"{m.Name}:{m.Role}:{m.State}"))
                    : "; probe=" + diag.Error!.Message;
            }
            await FailPermanentAsync(cluster, shard, op,
                $"master did not accept SQL within {thresholds.PatroniBootSec}s (probe: {lastError ?? "-"}){snapshot}", ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        if (waited > 0 && waited % 10 == 0)
            logger?.LogWarning(
                "backup-restore {Cluster}/{Shard}/{Op}: master-sql-wait {Waited}s/{Budget}s, addr {Addr}: {Error}",
                cluster, shard, op.Id, waited, thresholds.PatroniBootSec,
                firstAddr?.ToString() ?? "-", lastError ?? "-");

        return await TransientAsync(cluster, $"master-sql-wait/{shard}/{op.Id}", lastError, ct);
    }

    // Приватная копия ProvisioningProcess.Topology (прецедент кодовой базы).
    private static ShardTopology Topology(
        string cluster, string shard, IReadOnlyDictionary<string, NodeAddress> addresses)
        => new(cluster, shard, $"{cluster}-{shard}",
            addresses
                .Where(p => p.Key.StartsWith($"{shard}/", StringComparison.Ordinal))
                .ToDictionary(p => p.Key.Split('/')[1], p => p.Value));

    // Упрощённая копия ProvisioningProcess.ReadShardResourcesAsync: заявка
    // request_cpu/request_mem scope; нет/битые → null (лимиты не заданы).
    private async Task<NodeResources?> ReadShardResourcesAsync(
        string cluster, string shard, CancellationToken ct)
    {
        var scope = $"{cluster}-{shard}";
        var cpu = await etcd.GetAsync(endpoints[0], $"/service/{scope}/request_cpu", ct);
        if (!cpu.IsSuccess)
            return null;
        var mem = await etcd.GetAsync(endpoints[0], $"/service/{scope}/request_mem", ct);
        return mem.IsSuccess ? NodeResourcesParser.Parse(cpu.Value?.Value, mem.Value?.Value) : null;
    }

    // ── Хелперы etcd/журнала ──

    private long NowUnix() => time.GetUtcNow().ToUnixTimeSeconds();

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await etcd.GetAsync(endpoints[0], $"/pgworker/portalloc/{cluster}", ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        if (result.Value is not { } kv)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>());
        return Portalloc.Parse(cluster, kv.Value);
    }

    // failover-Put статуса заявки (по образцу BackupProcess.PutAsync).
    private async Task<Result> PutStatusAsync(
        string cluster, string shard, RestoreOperationState state, CancellationToken ct)
    {
        var value = Restore.RestoreStatusJson.Serialize(state);
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.PutAsync(endpoint, BackupNames.RestoreKey(cluster, shard, state.Id),
                value, null, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints are not set");
    }

    // permanent-отказ: FAILED + finished_unix + причина + журнал (повтор заявки — оператор).
    private async Task FailPermanentAsync(
        string cluster, string shard, RestoreOperationState op, string error, CancellationToken ct)
    {
        logger?.LogWarning("backup-restore {Cluster}/{Shard}/{Id}: FAILED — {Error}", cluster, shard, op.Id, error);
        // Щит демонтажа снимается при permanent-FAILED ДО rejoin: шард разобран,
        // заполненный «restore-in-progress» навсегда запер бы bootstrap пустых
        // нод при пересоздании шарда. В REJOINING+ там уже настоящий system_id
        // восстановленного volume — не трогаем (ноды поднимет rejoin-путь).
        if (op.State is RestoreStatus.Planned or RestoreStatus.Running)
        {
            var initKey = $"/service/{cluster}-{shard}/initialize";
            var init = await etcd.GetAsync(endpoints[0], initKey, ct);
            if (init.IsSuccess && init.Value?.Value == "restore-in-progress")
            {
                var del = await etcd.DeleteAsync(endpoints[0], initKey, prefix: false, ct);
                if (!del.IsSuccess)
                    logger?.LogError("backup-restore {Cluster}/{Shard}/{Id}: initialize placeholder was not deleted — {Error}",
                        cluster, shard, op.Id, del.Error?.Message);
            }
        }
        var put = await PutStatusAsync(cluster, shard, op with
        {
            State = RestoreStatus.Failed,
            FinishedUnix = NowUnix(),
            Error = error,
        }, ct);
        if (!put.IsSuccess)
            logger?.LogError("backup-restore {Cluster}/{Shard}/{Id}: FAILED status was not written — {Error}",
                cluster, shard, op.Id, put.Error?.Message);
        else
            restoreObserver?.Invoke(cluster, shard, "failed"); // терминальный исход зафиксирован в etcd
        await journal.WritePhaseAsync(cluster, Op, $"failed/{shard}/{op.Id}", claims.InstanceId, error, ct);
    }

    // transient-отказ: статус не меняем, запись журнала, следующий тик повторит.
    // Живая сверка клэйма перед мутациями фазы restore (arch/17 S5): IsMine на
    // входе тика не защищает длинную фазу от takeover. false — ключа нет/чужой/
    // etcd недоступен: мутации не начинаются (P9), журнал несёт claim-lost/<фазу>.
    private async Task<bool> ClaimAliveAsync(
        string cluster, string shard, RestoreOperationState op, CancellationToken ct)
    {
        var alive = await claims.EnsureClaimAliveAsync(cluster, ct);
        if (alive is { IsSuccess: true, Value: true })
            return true;

        await journal.WritePhaseAsync(cluster, Op, $"claim-lost/{shard}/{op.Id}", claims.InstanceId,
            alive.IsSuccess ? "claim key is gone or foreign" : alive.Error!.Message, ct);
        return false;
    }

    private async Task<Result<ProcessOutcome>> TransientAsync(
        string cluster, string phase, string? error, CancellationToken ct)
    {
        await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, error, ct);
        return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
    }
}
