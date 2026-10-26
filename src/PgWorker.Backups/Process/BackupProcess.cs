using Microsoft.Extensions.Logging;
using PgWorker.Backups.Job;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Sql;

namespace PgWorker.Backups;

/// <summary>
/// Планировщик полных бэкапов (t02/t26, arch/19 §2): тик под клэймом &lt;C&gt; для
/// каждого шарда Active-кластера с dsn. G0 выключен → no-op; G1 ensure
/// backup_password; на каждый шард: S-супервизия активного → G2 ensure роли
/// backup_exec на мастере (КАЖДЫЙ тик — spec §3.1, до due-гвардов: роль обязана
/// существовать до любого запуска; transient-skip шарда при недоступном
/// мастере) → G3 при due: PLANNED (journal-before-manipulations) → команда
/// POST /api/backup/full приёмнику ВЫБРАННОЙ ноды (канал alias:8080, t26) →
/// RUNNING. Инвариант: максимум один активный (PLANNED/RUNNING/UPLOADING) на
/// шард. Тик не блокируется на длинные операции: полный живёт в процессе
/// приёмника ноды.
/// </summary>
public sealed class BackupProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ShardEndpoints shardEndpoints,
    ISqlExecutor db,
    IClusterSecretEnsurer secrets,
    ClaimStore claims,
    WorkJournal journal,
    InstallSecrets installSecrets,
    BackupsRuntimeOptions options,
    BackupS3Factory s3Factory,
    IWalReceiverApiClient receiver,
    TimeProvider time,
    ILogger<BackupProcess> logger,
    Func<CancellationToken, Task<Result>>? snapshot = null,
    Action<string, IReadOnlyDictionary<string, (long? LastValidUnix, long MaxAgeSec)>>? fullAgeObserver = null)
{
    private const string Op = "backups";

    public async Task<Result<ProcessOutcome>> TickAsync(
        ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации префикса /pgworker/backups/* — только держатель клэйма (arch/19 §4).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"{Op} {cluster}: claim is not ours (or lost) — mutations are forbidden"));

        // G0: подсистема выключена — поведение воркера не меняется (no-op).
        if (!options.Enabled)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        var mine = backups.FirstOrDefault(b => b.Cluster == cluster)
                   ?? new ClusterBackups(cluster, null, new Dictionary<string, ShardBackups>());
        var fullMaxAgeSec = mine.Policy?.FullMaxAgeSec ?? options.FullMaxAgeSec;
        var verifyOnCreate = mine.Policy?.VerifyOnCreate ?? options.VerifyOnCreate;
        var nowUnix = time.GetUtcNow().ToUnixTimeSeconds();

        // G1: ensure per-cluster пароля backup_exec (P1.5-образец, t02).
        var creds = await secrets.EnsureAsync(cluster, snap.Config, ct);
        if (!creds.IsSuccess)
            return Result<ProcessOutcome>.Failed(creds.Error!);

        // t36 (arch/19 §2/§4): резолв пула бэкапов — в начале тика кластера.
        // null (policy без s3) → полный не планируется, journal не пишем
        // (молчание = «пул не выбран», алерт backup-pool-missing — проекция
        // панели по policy); Failed → journal waiting-backup-pool, continue
        // (transient: пул удалён/нода не заявлена/профиль без кредов — ретраи тиком).
        var poolResolved = await s3Factory.ResolveAsync(cluster, ct);
        if (!poolResolved.IsSuccess)
        {
            await journal.WritePhaseAsync(cluster, Op, "waiting-backup-pool", claims.InstanceId,
                poolResolved.Error!.Message, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }
        // Пул не выбран (policy без s3) — подсистема кластера молчит: полный не
        // планируется, journal не пишем (алерт backup-pool-missing — панель).
        if (poolResolved.Value is null)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);

        // Адреса нод один раз на тик (portalloc).
        var addresses = await shardEndpoints.ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return Result<ProcessOutcome>.Failed(addresses.Error!);

        foreach (var shard in snap.Shards.Where(s => s.Dsn is not null && !s.ToRemove))
        {
            // Живая сверка клэйма перед мутациями шарда (arch/17 S5): супервизия
            // (вердикты/старты джобов), hba-патчи, планирование нового полного.
            if (!await ClaimAliveAsync(cluster, shard.Name, ct))
                return Result<ProcessOutcome>.Failed(new ApplicationException(
                    $"{Op} {cluster}: claim lost before shard {shard.Name} supervision"));

            mine.Shards.TryGetValue(shard.Name, out var shardBackups);
            var fulls = shardBackups?.Full ?? (IReadOnlyList<FullBackupState>)[];

            // restore-гвард (t05 §3.4): шард в restore (PLANNED/RUNNING/REJOINING)
            // демонтируется restore-процессом — контуры бэкапов его не трогают.
            if (shardBackups?.Restores.Any(r => r.State
                    is RestoreStatus.Planned or RestoreStatus.Running or RestoreStatus.Rejoining) == true)
                continue;

            // S: супервизия активного (PLANNED — запуск/достарт; Running/Uploading —
            // поллинг UPLOADING/итог/vanished).
            var supervised = await SuperviseActiveAsync(
                cluster, shard, fulls, addresses.Value, verifyOnCreate, ct);
            if (!supervised.IsSuccess)
                return Result<ProcessOutcome>.Failed(supervised.Error!);

            // G2: ensure роли backup_exec на мастере шарда — КАЖДЫЙ тик, до
            // due-гвардов (spec §3.1); идемпотентный gexec-гвард. Мастер
            // недоступен → transient: шард в этом тике skip (супервизия выше
            // уже прошла), следующий тик дообеспечит.
            var master = await shardEndpoints.ResolveMasterAsync(cluster, shard, addresses.Value, ct);
            if (!master.IsSuccess || master.Value is null)
                continue;
            var adminDsn = ShardEndpoints.AdminDsn(master.Value, snap.Config.DbName, installSecrets);
            var guard = await db.ExecuteScalarAsync(
                adminDsn, DatabaseProvisioner.BuildBackupExecRoleGuardSql(creds.Value.BackupPassword), ct);
            if (!guard.IsSuccess)
                continue; // transient (сеть/мастер ушёл) — следующий тик дообеспечит
            if (guard.Value is string createRole)
            {
                var createdRole = await db.ExecuteAsync(adminDsn, createRole, ct);
                if (!createdRole.IsSuccess)
                    continue;
            }

            // pg_hba (arch/19 §7): Spilo разрешает replication-соединения только
            // роли standby — физический WAL-стриминг pg_basebackup для backup_exec
            // отсекается («no pg_hba.conf entry for replication»).
            // Инцидент 2026-09-13: прежний гвард дописывал строку прямо в
            // $PGDATA/pg_hba.conf (docker exec под root) — это ДВЕ дефекта:
            // (1) exec под root в ноду, чей PGDATA pg_basebackup ещё копирует
            //     (Patroni REST отвечает уже в фазе «creating replica», нода
            //     получает state=RUNNING до конца bootstrap), создавал root-овый
            //     pg_hba.conf → «could not create file pg_hba.conf: Permission
            //     denied» и цикл переснятий bootstrap;
            // (2) pg_hba.conf управляется Patroni (секция postgresql.pg_hba
            //     локального конфига): при КАЖДОМ старте postgres Patroni
            //     перегенерирует файл и дописанная строка молча исчезала
            //     («no pg_hba.conf entry» у wal-агентов после любого рестарта).
            // Гвард теперь добавляет строку в ИСТОЧНИК — /run/postgres.yml
            // (postgresql.pg_hba, симлинк /home/postgres/postgres.yml) — и будит
            // Patroni SIGHUP'ом: тот сам перегенерирует pg_hba.conf при каждом
            // старте postgres УЖЕ С нашей строкой. Ожидание применения — опрос
            // $PGDATA/pg_hba.conf (нода без PG_VERSION ещё бутстрапится — ждать
            // нечего, применится при первом старте). Отказ — transient: шард
            // skip, следующий тик дообеспечит (запуск на непатченной ноде карался
            // бы FAILED-циклом с бэкоффом). Усвоенные ноды (object) — exec в их
            // контейнер (pgw-имени у них нет).
            var hbaPatched = true;
            foreach (var nodeKey in addresses.Value.Keys
                         .Where(k => k.StartsWith($"{shard.Name}/", StringComparison.Ordinal)))
            {
                var addr = addresses.Value[nodeKey];
                var nodeName = nodeKey.Split('/')[1];
                string[] hbaCmd =
                [
                    "sh", "-c",
                    """
                    F=/run/postgres.yml; S='replication backup_exec'
                    grep -q "$S" "$PGDATA/pg_hba.conf" 2>/dev/null && grep -q "$S" "$F" 2>/dev/null && exit 0
                    grep -q "$S" "$F" 2>/dev/null || { sed -i 's/^  pg_hba:$/  pg_hba:\n  - hostssl replication backup_exec all scram-sha-256/' "$F" && chown postgres:root "$F" && chmod 644 "$F"; }
                    pkill -HUP -f 'local/bin/patroni' 2>/dev/null
                    [ -f "$PGDATA/PG_VERSION" ] || exit 0
                    grep -q "$S" "$PGDATA/pg_hba.conf" 2>/dev/null && exit 0
                    i=0; while [ $i -lt 10 ]; do grep -q "$S" "$PGDATA/pg_hba.conf" 2>/dev/null && exit 0; i=$((i+1)); sleep 1; done
                    exit 1
                    """,
                ];
                var patched = addr.Object is { Length: > 0 } objectContainer
                    ? await driver.ExecContainerAsync(objectContainer, hbaCmd, ct)
                    : await driver.ExecNodeAsync(cluster, shard.Name, nodeName, hbaCmd, ct);
                if (!patched.IsSuccess)
                {
                    hbaPatched = false;
                    break;
                }
            }

            if (!hbaPatched)
                continue;

            // G3: новый полный — только без активного, при due (возраст, ИЛИ
            // валидного COMPLETED нет вовсе — инвариант «свежий полный заведёт
            // цепочку от своего wal_start», t27: пересъём в окне до первого
            // закрытого сегмента бессмыслен; ИЛИ полный старее последней
            // restore — WalStream восстанавливает ключ быстрее тика, инцидент
            // E2E-гейта t05; t07: BROKEN wal-ключа — пересъём безусловно)
            // и после бэкоффа.
            // t07 (прогон 2026-09-13, arch/19 §2): BROKEN-пересъём — только пока
            // разрыв НЕ покрыт: COMPLETED-полный с wal_start ≥ границы разрыва
            // (chain_start BROKEN-записи) уже ждёт заживления контролем (§3),
            // повторный пересъём поверх — шторм (12 полных за 2 мин) без пользы.
            // Не покрывший (FAILED/ниже границы) — due, общий бэкофф как раньше.
            if (BackupPlanner.HasActive(fulls))
                continue; // инвариант одного активного — новый не создаём
            var lastRestoreFinished = shardBackups?.Restores
                .Where(r => r.State == RestoreStatus.Completed && r.FinishedUnix is not null)
                .Select(r => r.FinishedUnix!.Value)
                .OrderByDescending(f => f)
                .Cast<long?>()
                .FirstOrDefault();
            var brokenBoundary = shardBackups?.Wal is { State: WalStreamStatus.Broken } brokenWal
                ? brokenWal.ChainStartSegment
                : null;
            var breakCovered = brokenBoundary is { Length: > 0 } boundary
                && fulls.Any(f => f.State == FullBackupStatus.Completed
                    && f.WalStartSegment is { Length: > 0 } walStart
                    && string.CompareOrdinal(walStart, boundary) >= 0);

            // t24 (arch/19 §2): живой непокрытый tli-gap — форс-пересъём (симметрично
            // walChainBroken; покрывший COMPLETED-полный ждёт снятия метки контролем).
            var gapBoundary = shardBackups?.Wal is { Gap: { } liveGap } ? liveGap.To : null;
            var gapCovered = gapBoundary is { Length: > 0 } gapTo
                && fulls.Any(f => f.State == FullBackupStatus.Completed
                    && f.WalStartSegment is { Length: > 0 } walStart
                    && string.CompareOrdinal(walStart, gapTo) >= 0);

            if (!BackupPlanner.IsDue(fulls, fullMaxAgeSec, nowUnix, lastRestoreFinished,
                    walChainBroken: brokenBoundary is not null && !breakCovered,
                    walGapUncovered: gapBoundary is not null && !gapCovered))
                continue;
            if (!BackupPlanner.BackoffPassed(fulls, options.RetryBaseSec, options.RetryMaxSec, nowUnix))
                continue; // бэкофф переснятия FAILED — следующий тик

            // источник — sync-standby, fallback мастер; резолв не удался →
            // transient: journal НЕ пишем, следующий тик повторит.
            var source = await shardEndpoints.ResolveBackupSourceAsync(cluster, shard, addresses.Value, ct);
            if (!source.IsSuccess || source.Value is null)
                continue;
            var role = source.Value.Ports == master.Value.Ports && source.Value.Host == master.Value.Host
                ? BackupSourceRole.Master
                : BackupSourceRole.Replica;

            var id = BackupPlanner.NextId(fulls.Select(f => f.Id), time.GetUtcNow().UtcDateTime);
            // node-факт — имя ноды-источника из portalloc; не нашли (рассинхрон
            // portalloc) — журнал-факт host, тик не валим (spec §3.1).
            var nodeKv = addresses.Value.FirstOrDefault(p =>
                    p.Key.StartsWith($"{shard.Name}/", StringComparison.Ordinal)
                    && p.Value.Host == source.Value.Host && p.Value.Ports == source.Value.Ports);
            var node = nodeKv.Key?.Split('/')[1] ?? source.Value.Host;

            // journal-before-manipulations: PLANNED до команды запуска.
            var planned = new FullBackupState(
                id, FullBackupStatus.Planned, node, role, nowUnix, null, null, null, null, null);
            var put = await PutAsync(BackupNames.FullKey(cluster, shard.Name, id), BackupStatusJson.Serialize(planned), ct);
            if (!put.IsSuccess)
                return Result<ProcessOutcome>.Failed(put.Error!);

            // Команда запуска приёмнику ВЫБРАННОЙ ноды (t26, spec §3.5): канал
            // alias:8080 в общей сети воркера и нод (Object — усыновлённая нода).
            // 202 ⇒ RUNNING; 409 = пере-старт прошлого тика, операция уже жива ⇒
            // RUNNING; 503/транспортный отказ ⇒ PLANNED остаётся, следующий тик
            // повторит (transient с journal-фактом адреса — spec §3.1). Контейнера
            // полного больше нет — docker-движок здесь не нужен.
            var launched = await receiver.StartFullAsync(
                WalReceiverAddress.ForNode(cluster, shard.Name, node,
                    nodeKv.Key is not null ? nodeKv.Value.Object : null),
                id, ct);
            if (launched.IsAccepted || launched.IsConflict)
            {
                var running = planned with { State = FullBackupStatus.Running };
                var putRunning = await PutAsync(BackupNames.FullKey(cluster, shard.Name, id), BackupStatusJson.Serialize(running), ct);
                if (!putRunning.IsSuccess)
                    return Result<ProcessOutcome>.Failed(putRunning.Error!);
                await journal.WritePhaseAsync(cluster, Op, $"started/{shard.Name}/{id}", claims.InstanceId, null, ct);
                logger.LogInformation("backups {Cluster}/{Shard}: full {Id} started on {Node} (role={Role})",
                    cluster, shard.Name, id, node, role);
            }
            else if (launched.IsTransient)
            {
                // Недостижимость приёмника (DNS/отказ/таймаут/5xx) — journal-факт
                // с адресом (spec §3.1 диагностика); PLANNED остаётся.
                await journal.WritePhaseAsync(cluster, Op, $"receiver-unavailable/{shard.Name}/{id}",
                    claims.InstanceId, $"{WalReceiverAddress.ForNode(cluster, shard.Name, node,
                        nodeKv.Key is not null ? nodeKv.Value.Object : null)}: {launched.Error}", ct);
            }
            // 503 стоп-гейтов (IsGated) — операция не начинается: PLANNED остаётся,
            // следующий тик повторит (стоп-семантика подсистемы, arch/19 §2).
        }

        // t14 (arch/18 §2.7): возрастные серии полных — наблюдение тика
        // планировщика: набор бэкапимых шардов (dsn, не ToRemove) замещает стейт
        // кластера в instrumentation целиком; возрастной факт — семантика
        // планировщика (BackupPlanner.LastValidUnix), порог — эффективная
        // политика тика (policy-ключ ?? дефолт конфига).
        fullAgeObserver?.Invoke(cluster, snap.Shards
            .Where(s => s.Dsn is not null && !s.ToRemove)
            .ToDictionary(
                s => s.Name,
                s => (BackupPlanner.LastValidUnix(mine.Shards.GetValueOrDefault(s.Name)?.Full ?? []),
                    fullMaxAgeSec)));

        // Делегат снапшота etcd (SnapshotJob) в тике планировщика не используется —
        // параметр держит контракт DI (wiring); прецедент — PasswordRotator.afterCommit.
        _ = snapshot;

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // Живая сверка клэйма перед мутациями шарда (arch/17 S5): IsMine на входе
    // тика не защищает длинный тик от takeover. false — ключа нет/чужой/etcd
    // недоступен: мутации не начинаются (P9), журнал несёт claim-lost/<фазу>.
    private async Task<bool> ClaimAliveAsync(string cluster, string shard, CancellationToken ct)
    {
        var alive = await claims.EnsureClaimAliveAsync(cluster, ct);
        if (alive is { IsSuccess: true, Value: true })
            return true;

        await journal.WritePhaseAsync(cluster, Op, $"claim-lost/{shard}", claims.InstanceId,
            alive.IsSuccess ? "claim key is gone or foreign" : alive.Error!.Message, ct);
        return false;
    }

    // S: супервизия активного через HTTP API приёмника выбранной ноды
    // (t26, arch/19 §2, spec §3.5). Ветвление по active.State:
    //   возрастной бюджет — ПЕРВЫЙ гвард: FAILED job-timeout + cancel API
    //     best-effort (приёмник гасит child-процессы и чистит staging);
    //   PLANNED — идемпотентный повтор StartFullAsync (202/409 ⇒ RUNNING;
    //     503/transient — PLANNED остаётся, следующий тик повторит);
    //   RUNNING/UPLOADING — GetFullAsync: transient ⇒ статус не меняем;
    //     404 ⇒ FAILED operation-vanished (рестарт приёмника/ноды, переснятие
    //     новым id); uploading ⇒ UPLOADING+WalStartSegment; succeeded ⇒
    //     COMPLETED+SizeBytes (verify=PENDING при on_create); failed ⇒ FAILED.
    private async Task<Result> SuperviseActiveAsync(
        string cluster, ShardSpec shard, IReadOnlyList<FullBackupState> fulls,
        IReadOnlyDictionary<string, NodeAddress> addresses,
        bool verifyOnCreate, CancellationToken ct)
    {
        foreach (var active in fulls.Where(f => f.State
                     is FullBackupStatus.Planned or FullBackupStatus.Running or FullBackupStatus.Uploading))
        {
            // t13 (arch/19 §6): возрастной бюджет — ПЕРВЫЙ гвард: вердикт FAILED
            // по возрасту — самостоятельный факт etcd (started_unix + часы
            // воркера), API/docker-доступ не нужен; transient-пропуск ноды
            // (portalloc) бюджет НЕ откладывает (операции с возрастом > 6 ч
            // нечем оправдаться). journal-before-manipulations: FAILED пишется
            // ДО cancel.
            var nowUnix = time.GetUtcNow().ToUnixTimeSeconds();
            var source = addresses.GetValueOrDefault($"{shard.Name}/{active.Node}");

            if (SupervisionTimeouts.IsTimedOut(active.StartedUnix, nowUnix, options.JobFullTimeoutSec))
            {
                var timedOut = active with
                {
                    State = FullBackupStatus.Failed,
                    FinishedUnix = nowUnix,
                    Error = $"job-timeout: {nowUnix - active.StartedUnix} s > {options.JobFullTimeoutSec}",
                };
                var putTimeout = await PutAsync(
                    BackupNames.FullKey(cluster, shard.Name, active.Id), BackupStatusJson.Serialize(timedOut), ct);
                if (!putTimeout.IsSuccess)
                    return putTimeout;

                // Cancel best-effort (Р8): приёмник гасит дерево child-процессов
                // и чистит staging (контейнера полного больше нет — kill+rm не
                // нужен); недостижим → journal-пометка «cancel пропущен» в той
                // же записи. PLANNED без запуска — FAILED без cancel (как раньше).
                var cancelNote = "";
                if (source is null)
                {
                    cancelNote = "; cancel skipped: node unavailable";
                }
                else
                {
                    var cancel = await receiver.CancelFullAsync(
                        WalReceiverAddress.ForNode(cluster, shard.Name, active.Node, source.Object), ct);
                    if (cancel.IsTransient)
                        cancelNote = "; cancel skipped: receiver unavailable";
                }
                await journal.WritePhaseAsync(cluster, Op, $"job-timeout/{shard.Name}/{active.Id}",
                    claims.InstanceId, timedOut.Error + cancelNote, ct);
                continue;
            }

            // node-факт статуса: нода исчезла из portalloc → transient
            // (адрес приёмника не собрать достоверно) — следующий тик.
            if (source is null)
                continue;
            var receiverUrl = WalReceiverAddress.ForNode(cluster, shard.Name, active.Node, source.Object);

            // PLANNED: команда запуска ещё не принята — идемпотентный повтор
            // (202 = принят; 409 = пере-старт прошлого тика, операция уже жива).
            if (active.State == FullBackupStatus.Planned)
            {
                var launch = await receiver.StartFullAsync(receiverUrl, active.Id, ct);
                if (launch.IsAccepted || launch.IsConflict)
                {
                    var running = active with { State = FullBackupStatus.Running };
                    var putRunning = await PutAsync(
                        BackupNames.FullKey(cluster, shard.Name, active.Id), BackupStatusJson.Serialize(running), ct);
                    if (!putRunning.IsSuccess)
                        return putRunning;
                    await journal.WritePhaseAsync(cluster, Op, $"started/{shard.Name}/{active.Id}",
                        claims.InstanceId, null, ct);
                }
                else if (launch.IsTransient)
                {
                    // Недостижимость приёмника — journal-факт с адресом
                    // (spec §3.1); PLANNED остаётся, следующий тик повторит.
                    await journal.WritePhaseAsync(cluster, Op,
                        $"receiver-unavailable/{shard.Name}/{active.Id}",
                        claims.InstanceId, $"{receiverUrl}: {launch.Error}", ct);
                }
                // 503 стоп-гейтов (IsGated) — операция не начинается: PLANNED
                // остаётся молча (стоп-семантика подсистемы, arch/19 §2).
                continue;
            }

            // RUNNING/UPLOADING: GET статуса операции у приёмника ноды.
            var reply = await receiver.GetFullAsync(receiverUrl, ct);
            if (reply.IsTransient)
                continue; // DNS/отказ/5xx/таймаут — transient тика, статус не портим

            if (reply.IsNotFound)
            {
                // Рестарт приёмника/ноды посреди операции (Р9): резюмов нет —
                // FAILED operation-vanished, переснятие по G3 с бэкоффом.
                var vanished = active with
                {
                    State = FullBackupStatus.Failed,
                    FinishedUnix = time.GetUtcNow().ToUnixTimeSeconds(),
                    Error = "operation-vanished",
                };
                var putVanished = await PutAsync(
                    BackupNames.FullKey(cluster, shard.Name, active.Id), BackupStatusJson.Serialize(vanished), ct);
                if (!putVanished.IsSuccess)
                    return putVanished;
                await journal.WritePhaseAsync(cluster, Op, $"vanished/{shard.Name}/{active.Id}",
                    claims.InstanceId, "operation-vanished", ct);
                continue;
            }

            var snapshot = reply.Status;
            if (snapshot is null)
                continue; // 200 без тела — аномалия, ждём следующий тик

            switch (snapshot.State)
            {
                case "running":
                    continue; // жив — ждём следующие тики

                case "uploading":
                    if (active.State != FullBackupStatus.Uploading)
                    {
                        var uploading = active with
                        {
                            State = FullBackupStatus.Uploading,
                            WalStartSegment = snapshot.WalStartSegment,
                        };
                        var put = await PutAsync(
                            BackupNames.FullKey(cluster, shard.Name, active.Id), BackupStatusJson.Serialize(uploading), ct);
                        if (!put.IsSuccess)
                            return put;
                    }
                    continue;

                case "succeeded":
                {
                    var outcome = active with
                    {
                        State = FullBackupStatus.Completed,
                        FinishedUnix = time.GetUtcNow().ToUnixTimeSeconds(),
                        WalStartSegment = snapshot.WalStartSegment ?? active.WalStartSegment,
                        SizeBytes = snapshot.SizeBytes,
                        Verify = verifyOnCreate ? new BackupVerify(BackupVerifyStatus.Pending, null) : null,
                    };
                    var putOutcome = await PutAsync(
                        BackupNames.FullKey(cluster, shard.Name, active.Id), BackupStatusJson.Serialize(outcome), ct);
                    if (!putOutcome.IsSuccess)
                        return putOutcome;
                    await journal.WritePhaseAsync(cluster, Op, $"completed/{shard.Name}/{active.Id}",
                        claims.InstanceId, null, ct);
                    continue;
                }

                case "failed":
                {
                    var outcome = active with
                    {
                        State = FullBackupStatus.Failed,
                        FinishedUnix = time.GetUtcNow().ToUnixTimeSeconds(),
                        Error = snapshot.Error ?? "receiver reported failed",
                    };
                    var putOutcome = await PutAsync(
                        BackupNames.FullKey(cluster, shard.Name, active.Id), BackupStatusJson.Serialize(outcome), ct);
                    if (!putOutcome.IsSuccess)
                        return putOutcome;
                    await journal.WritePhaseAsync(cluster, Op, $"failed/{shard.Name}/{active.Id}",
                        claims.InstanceId, outcome.Error, ct);
                    continue;
                }

                default:
                    continue; // незнакомое состояние — толерантно ждём
            }
        }

        return Result.Success();
    }

    // Failover-обёртка put: первый успешный endpoint выигрывает (образец DeprovisioningProcess).
    private async Task<Result> PutAsync(string key, string value, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.PutAsync(endpoint, key, value, null, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints are not set");
    }
}
