using Microsoft.Extensions.Logging;
using PgWorker.Backups.Job;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Docker.Engine;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Sql;

namespace PgWorker.Backups;

/// <summary>
/// Планировщик полных бэкапов (t02, arch/19 §2): тик под клэймом &lt;C&gt; для
/// каждого шарда Active-кластера с dsn. G0 выключен → no-op; G1 ensure
/// backup_password; на каждый шард: S-супервизия активного → G2 ensure роли
/// backup_exec на мастере (КАЖДЫЙ тик — spec §3.1, до due-гвардов: роль обязана
/// существовать до любого запуска джоба; transient-skip шарда при недоступном
/// мастере) → G3 при due: PLANNED (journal-before-manipulations) → джоб-контейнер
/// на docker-хосте источника → RUNNING. Инвариант: максимум один активный
/// (PLANNED/RUNNING/UPLOADING) на шард. Тик не блокируется на длинные операции:
/// бэкап живёт в контейнере.
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
    TimeProvider time,
    ILogger<BackupProcess> logger,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Op = "backups";

    public async Task<Result<ProcessOutcome>> TickAsync(
        ClusterSnapshot snap, IReadOnlyList<ClusterBackups> backups, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации префикса /pgworker/backups/* — только держатель клэйма (arch/19 §4).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"{Op} {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

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

        // Адреса нод один раз на тик (portalloc).
        var addresses = await shardEndpoints.ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return Result<ProcessOutcome>.Failed(addresses.Error!);

        foreach (var shard in snap.Shards.Where(s => s.Dsn is not null && !s.ToRemove))
        {
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
                cluster, shard, fulls, addresses.Value, creds.Value.BackupPassword, verifyOnCreate, ct);
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

            // G3: новый полный — только без активного, при due (возраст ИЛИ
            // отсутствие wal-ключа после restore, t05 §3.5, ИЛИ полный старее
            // последней restore — WalStream восстанавливает ключ быстрее тика,
            // инцидент E2E-гейта t05; t07: BROKEN wal-ключа — пересъём безусловно)
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
            if (!BackupPlanner.IsDue(fulls, walKeyExists: shardBackups?.Wal is not null,
                    fullMaxAgeSec, nowUnix, lastRestoreFinished,
                    walChainBroken: brokenBoundary is not null && !breakCovered))
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

            var engine = driver.EngineFor(source.Value.Host);
            if (engine is null)
                continue; // хост источника не в таблице Docker:Hosts — transient

            var id = BackupPlanner.NextId(fulls.Select(f => f.Id), time.GetUtcNow().UtcDateTime);
            // node-факт — имя ноды-источника из portalloc; не нашли (рассинхрон
            // portalloc) — журнал-факт host, тик не валим (spec §3.1).
            var node = addresses.Value.FirstOrDefault(p =>
                    p.Key.StartsWith($"{shard.Name}/", StringComparison.Ordinal)
                    && p.Value.Host == source.Value.Host && p.Value.Ports == source.Value.Ports)
                .Key?.Split('/')[1] ?? source.Value.Host;

            // journal-before-manipulations: PLANNED до создания контейнера.
            var planned = new FullBackupState(
                id, FullBackupStatus.Planned, node, role, nowUnix, null, null, null, null, null);
            var put = await PutAsync(BackupNames.FullKey(cluster, shard.Name, id), BackupStatusJson.Serialize(planned), ct);
            if (!put.IsSuccess)
                return Result<ProcessOutcome>.Failed(put.Error!);

            // Джоб-контейнер на docker-хосте источника (extra_hosts, без портов);
            // сбой create/start — PLANNED остаётся, S-супервизия идемпотентно
            // запустит следующим тиком (spec §2.4).
            var spec = BackupJobSpec.Build(options, cluster, shard.Name, id, source.Value, creds.Value.BackupPassword);
            var name = BackupNames.ContainerName(cluster, shard.Name, id);
            var createdContainer = await engine.CreateContainerAsync(spec, name, ct);
            if (!createdContainer.IsSuccess)
                continue;
            var started = await engine.StartContainerAsync(name, ct);
            if (!started.IsSuccess)
                continue;

            var running = planned with { State = FullBackupStatus.Running };
            var putRunning = await PutAsync(BackupNames.FullKey(cluster, shard.Name, id), BackupStatusJson.Serialize(running), ct);
            if (!putRunning.IsSuccess)
                return Result<ProcessOutcome>.Failed(putRunning.Error!);
            await journal.WritePhaseAsync(cluster, Op, $"started/{shard.Name}/{id}", claims.InstanceId, null, ct);
            logger.LogInformation("backups {Cluster}/{Shard}: полный {Id} запущен на {Node} (role={Role})",
                cluster, shard.Name, id, node, role);
        }

        // Делегат снапшота etcd (SnapshotJob) в тике планировщика не используется —
        // параметр держит контракт DI (wiring); прецедент — PasswordRotator.afterCommit.
        _ = snapshot;

        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // S: супервизия активного по детерминированному имени контейнера на
    // docker-хосте его источника (arch/19 §2). Ветвление по active.State
    // (spec §2.4, ревью Ф4 finding 2):
    //   PLANNED + нет контейнера → идемпотентный запуск (сбой create/start
    //     прошлого тика НЕ превращается в FAILED/vanished и НЕ карается бэкоффом);
    //   PLANNED/другой + created → довыгоняем StartContainerAsync (304=успех);
    //   running → поллинг логов → UPLOADING;
    //   exited + result → COMPLETED/FAILED; RUNNING/UPLOADING без контейнера →
    //     FAILED container-vanished; transport-отказ docker (list/logs/inspect)
    //     — transient: статус не меняем, следующий тик повторит (spec §3.1 S).
    private async Task<Result> SuperviseActiveAsync(
        string cluster, ShardSpec shard, IReadOnlyList<FullBackupState> fulls,
        IReadOnlyDictionary<string, NodeAddress> addresses, string backupPassword,
        bool verifyOnCreate, CancellationToken ct)
    {
        foreach (var active in fulls.Where(f => f.State
                     is FullBackupStatus.Planned or FullBackupStatus.Running or FullBackupStatus.Uploading))
        {
            // t13 (arch/19 §6): возрастной бюджет — ПЕРВЫЙ гвард: вердикт FAILED
            // по возрасту — самостоятельный факт etcd (started_unix + часы
            // воркера), docker-доступ не нужен; transient-пропуск источника
            // (portalloc/engine/list) бюджет НЕ откладывает (джобу с возрастом >
            // 6 ч нечем оправдаться). journal-before-manipulations: FAILED
            // пишется ДО cleanup.
            var nowUnix = time.GetUtcNow().ToUnixTimeSeconds();
            var source = addresses.GetValueOrDefault($"{shard.Name}/{active.Node}");
            var engine = source is null ? null : driver.EngineFor(source.Host);
            var name = BackupNames.ContainerName(cluster, shard.Name, active.Id);

            if (SupervisionTimeouts.IsTimedOut(active.StartedUnix, nowUnix, options.JobFullTimeoutSec))
            {
                var timedOut = active with
                {
                    State = FullBackupStatus.Failed,
                    FinishedUnix = nowUnix,
                    Error = $"job-timeout: {nowUnix - active.StartedUnix} с > {options.JobFullTimeoutSec}",
                };
                var putTimeout = await PutAsync(
                    BackupNames.FullKey(cluster, shard.Name, active.Id), BackupStatusJson.Serialize(timedOut), ct);
                if (!putTimeout.IsSuccess)
                    return putTimeout;

                // Cleanup best-effort (t13): kill+rm — только при доступном
                // источнике (source/engine/list); недоступен → cleanup
                // пропускается (осиротевший контейнер с детерминированным именем
                // ничего не держит: id уникален, FAILED уже в etcd), пометка — в
                // lastError той же journal-записи. PLANNED без контейнера —
                // FAILED без kill (как до t13), БЕЗ пометки (cleanup не нужен,
                // а не пропущен).
                string cleanupNote = "";
                if (engine is null)
                {
                    cleanupNote = "; cleanup пропущен: источник недоступен";
                }
                else
                {
                    var cleanupList = await engine.ListContainersAsync(name, all: true, ct);
                    if (!cleanupList.IsSuccess)
                        cleanupNote = "; cleanup пропущен: источник недоступен";
                    else if (cleanupList.Value.Any(c => c.Names.Contains(name)))
                        await CleanupJobAsync(engine, cluster, shard.Name, active.Id, ct); // kill+rm контейнера и volume
                }
                await journal.WritePhaseAsync(cluster, Op, $"job-timeout/{shard.Name}/{active.Id}",
                    claims.InstanceId, timedOut.Error + cleanupNote, ct);
                continue;
            }

            // хост джоба — хост ноды-источника из portalloc (node-факт статуса);
            // нода исчезла из portalloc → transient: следующий тик.
            if (source is null)
                continue;
            if (engine is null)
                continue;

            var list = await engine.ListContainersAsync(name, all: true, ct);
            if (!list.IsSuccess)
                continue; // transient transport-отказ: статус не меняем (arch/19 §2)

            var found = list.Value.FirstOrDefault(c => c.Names.Contains(name));

            // PLANNED: джоб ещё не стартовал — идемпотентный запуск (spec §2.4):
            // нет контейнера → create; старт — в обоих случаях (created прошлом
            // тике / только что созданный; 304 already-started = успех движка).
            if (active.State == FullBackupStatus.Planned && found is not { State: "running" or "exited" })
            {
                if (found is null)
                {
                    var spec = BackupJobSpec.Build(options, cluster, shard.Name, active.Id, source, backupPassword);
                    var created = await engine.CreateContainerAsync(spec, name, ct);
                    if (!created.IsSuccess)
                        continue; // transient — следующий тик повторит запуск
                }

                var started = await engine.StartContainerAsync(name, ct);
                if (!started.IsSuccess)
                    continue;

                var running = active with { State = FullBackupStatus.Running };
                var putRunning = await PutAsync(
                    BackupNames.FullKey(cluster, shard.Name, active.Id), BackupStatusJson.Serialize(running), ct);
                if (!putRunning.IsSuccess)
                    return putRunning;
                await journal.WritePhaseAsync(cluster, Op, $"started/{shard.Name}/{active.Id}", claims.InstanceId, null, ct);
                continue;
            }

            // RUNNING/UPLOADING + created — аномалия (start потерялся между тиками):
            // довыгоняем (304 = успех), статус не трогаем — следующий тик увидит running.
            if (found is { State: "created" })
            {
                await engine.StartContainerAsync(name, ct);
                continue;
            }

            if (found is null)
            {
                // контейнера нет вовсе (включая exited) — сюда попадают только
                // RUNNING/UPLOADING (PLANNED разобран выше): рестарт docker-хоста
                // и пр.; осиротевший staging volume удаляем (404 = ок), переснятие по G3.
                var vanished = active with
                {
                    State = FullBackupStatus.Failed,
                    FinishedUnix = time.GetUtcNow().ToUnixTimeSeconds(),
                    Error = "container-vanished",
                };
                var putVanished = await PutAsync(
                    BackupNames.FullKey(cluster, shard.Name, active.Id), BackupStatusJson.Serialize(vanished), ct);
                if (!putVanished.IsSuccess)
                    return putVanished;
                await CleanupJobAsync(engine, cluster, shard.Name, active.Id, ct);
                await journal.WritePhaseAsync(cluster, Op, $"vanished/{shard.Name}/{active.Id}",
                    claims.InstanceId, "container-vanished", ct);
                continue;
            }

            // Логи — транспорт guarded (spec §3.1 S: logs недоступны → transient,
            // статус не меняем, следующий тик повторит супервизию; ревью Ф4-3).
            var logs = await engine.GetContainerLogsAsync(name, tail: 200, ct);
            if (!logs.IsSuccess)
                continue;
            var markers = BackupJobLog.Parse(logs.Value);

            if (found is { State: "running" or "restarting" })
            {
                if (markers is { Phase: "uploading", WalStartSegment: { } wal }
                    && active.State != FullBackupStatus.Uploading)
                {
                    var uploading = active with { State = FullBackupStatus.Uploading, WalStartSegment = wal };
                    var put = await PutAsync(
                        BackupNames.FullKey(cluster, shard.Name, active.Id), BackupStatusJson.Serialize(uploading), ct);
                    if (!put.IsSuccess)
                        return put;
                }

                continue; // жив — ждём следующие тики
            }

            if (found is { State: "exited" })
            {
                // exit-код — только при успешном инспекте (spec §3.1 S: inspect
                // недоступен → transient, статус не меняем). Без гварда успешный
                // бэкап (exit 0 + ok:true, но логи/инспект не прочитаны из-за
                // transport-отказа) ушёл бы в ЛОЖНЫЙ FAILED («exit 0»/«exit -1»),
                // CleanupJobAsync удалил бы контейнер — попытка потеряна, воркер
                // переснимает полный лишний раз (ложный критичный алерт панели).
                var inspect = await engine.InspectContainerAsync(found.Id, ct);
                if (!inspect.IsSuccess)
                    continue;
                var exitCode = inspect.Value.ExitCode ?? -1;
                FullBackupState outcome;
                if (exitCode == 0 && markers.Result is { Ok: true } result)
                {
                    outcome = active with
                    {
                        State = FullBackupStatus.Completed,
                        FinishedUnix = time.GetUtcNow().ToUnixTimeSeconds(),
                        WalStartSegment = result.WalStartSegment ?? active.WalStartSegment,
                        SizeBytes = result.SizeBytes,
                        Verify = verifyOnCreate ? new BackupVerify(BackupVerifyStatus.Pending, null) : null,
                    };
                }
                else
                {
                    // exit-код — истина итога; result-JSON — метаданные (arch/19 §10).
                    var error = markers.Result is { Ok: false, Error: { } reason }
                        ? reason
                        : $"exit {exitCode}";
                    outcome = active with
                    {
                        State = FullBackupStatus.Failed,
                        FinishedUnix = time.GetUtcNow().ToUnixTimeSeconds(),
                        Error = error,
                    };
                }

                var putOutcome = await PutAsync(
                    BackupNames.FullKey(cluster, shard.Name, active.Id), BackupStatusJson.Serialize(outcome), ct);
                if (!putOutcome.IsSuccess)
                    return putOutcome;
                await CleanupJobAsync(engine, cluster, shard.Name, active.Id, ct);
                await journal.WritePhaseAsync(cluster, Op,
                    $"{(outcome.State == FullBackupStatus.Completed ? "completed" : "failed")}/{shard.Name}/{active.Id}",
                    claims.InstanceId, outcome.Error, ct);
            }
        }

        return Result.Success();
    }

    // Итог зафиксирован — контейнер и staging volume джоба не нужны (arch/19 §2);
    // квота-tmpfs-джоб volume не имеет — RemoveVolumeAsync 404 = успех.
    private async Task CleanupJobAsync(IDockerEngine engine, string cluster, string shard, string id, CancellationToken ct)
    {
        await engine.RemoveContainerAsync(BackupNames.ContainerName(cluster, shard, id), force: true, ct);
        await engine.RemoveVolumeAsync(BackupNames.VolumeName(cluster, shard, id), ct);
    }    // Failover-обёртка put: первый успешный endpoint выигрывает (образец DeprovisioningProcess).
    private async Task<Result> PutAsync(string key, string value, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.PutAsync(endpoint, key, value, null, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
