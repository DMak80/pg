using Microsoft.Extensions.Logging;
using PgWorker.Backups.Job;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Docker.Engine;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
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
            var fulls = mine.Shards.TryGetValue(shard.Name, out var shardBackups)
                ? shardBackups.Full
                : (IReadOnlyList<FullBackupState>)[];

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

            // G3: новый полный — только без активного, при due и после бэкоффа.
            if (BackupPlanner.HasActive(fulls))
                continue; // инвариант одного активного — новый не создаём
            if (!BackupPlanner.IsDue(fulls, fullMaxAgeSec, nowUnix))
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

    // S-ветка супервизии — реализуется Task 12 этого плана.
    private Task<Result> SuperviseActiveAsync(
        string cluster, ShardSpec shard, IReadOnlyList<FullBackupState> fulls,
        IReadOnlyDictionary<string, NodeAddress> addresses, string backupPassword,
        bool verifyOnCreate, CancellationToken ct)
        => Task.FromResult(Result.Success());    // Failover-обёртка put: первый успешный endpoint выигрывает (образец DeprovisioningProcess).
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
