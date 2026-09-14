using System.Text.Json;
using System.Text.RegularExpressions;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using Shared.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Sql;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// Ротация per-cluster секретов по заявке /pgworker/rotations/&lt;C&gt; (t02,
/// arch/14 §5 I / arch/19 §7): app + bucket_admin + bucket_mover + backup_exec.
/// R1 ensure кредов → R2 ALTER ROLE ролей на мастере каждого шарда с dsn
/// (backup_exec — через gexec-гвард: роли может не быть при выключенных бэкапах;
/// реплики получают pg_authid физической репликацией) → R3 атомарный txn
/// [compare value==OLD по кредам и ВСЕМ dsn-ключам][put новые креды; перезапись
/// dsn; del заявки] → R4 снапшот P12. transient-сбой → заявка жива, креды в
/// etcd НЕ меняются, следующий тик повторяет с начала со свежими NEW (ALTER
/// идемпотентен перезаписью). Имя journal-op сохранено (rotate-app-password) —
/// совместимость журналов (образец RotationRole.Phase, arch/16 §5 H). Вызывается
/// только держателем клэйма &lt;C&gt;. Ротация при активном переезде: mover-DSN
/// строится из свежего снапшота на тик — оборванная ротацией фаза переезда
/// возобновляется с journal-фазы уже с новыми кредами.
/// </summary>
public sealed partial class ClusterSecretRotator(
    IEtcdGateway etcd,
    string[] endpoints,
    ISqlExecutor db,
    ShardProbe probe,
    ClaimStore claims,
    WorkJournal journal,
    InstallSecrets secrets,
    IClusterSecretEnsurer appSecret,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Op = "rotate-app-password";

    // Имя mover-роли фиксировано (arch/14 §4); user-ключа mover нет.
    private const string MoverRole = "bucket_mover";

    public async Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации — только держателем живого клэйма (инвариант arch/14 §3.3).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"{Op} {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // R0: заявка (цикл префикс /pgworker/ не читает — читаем ключ сами).
        var ticket = await GetAsync(TicketKey(cluster), ct);
        if (!ticket.IsSuccess)
            return Result<ProcessOutcome>.Failed(ticket.Error!);
        if (ticket.Value is null)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // нет заявки — no-op

        // Битая заявка (не-JSON/без requested_unix) — мусор: удалить с journal-записью.
        if (!IsWellFormed(ticket.Value.Value))
        {
            var cleaned = await DeleteAsync(TicketKey(cluster), ct);
            if (!cleaned.IsSuccess)
                return Result<ProcessOutcome>.Failed(cleaned.Error!);
            await journal.WritePhaseAsync(
                cluster, Op, "malformed-ticket-removed", claims.InstanceId, ticket.Value.Value, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
        }

        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return Result<ProcessOutcome>.Failed(started.Error!);

        // R1: ensure тройки кредов (t02) — OLD-значения после этого существуют.
        var credsResult = await appSecret.EnsureAsync(cluster, snap.Config, ct);
        if (!credsResult.IsSuccess)
            return await FailAsync(cluster, credsResult.Error!, "ensure-app-secret", ct);
        var creds = credsResult.Value;

        // R2: ALTER ROLE трёх ролей + гвард backup_exec на мастере каждого
        // ПОДНЯТОГО шарда (dsn есть; шард без dsn — домен AddShardProcess: роли
        // создадутся/выровняются по свежим кредам, §5 I R2). NEW-пароли
        // генерируются на попытку — ALTER идемпотентен перезаписью, регенерация
        // между тиками безопасна.
        var newAppPassword = AppSecretGenerator.Generate();
        var newMoverPassword = AppSecretGenerator.Generate();
        var newBucketAdminPassword = AppSecretGenerator.Generate();
        var newBackupPassword = AppSecretGenerator.Generate();
        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return await FailAsync(cluster, addresses.Error!, "portalloc", ct);

        foreach (var shard in snap.Shards.Where(s => s.Dsn is not null))
        {
            var master = await ResolveMasterAsync(cluster, shard, addresses.Value, ct);
            if (master is null)
                return await FailAsync(cluster,
                    new ApplicationException(
                        $"шард {shard.Name}: мастер недоступен (master-ключ/Patroni REST) — ретрай тиком"),
                    $"waiting-master/{shard.Name}", ct);

            var dsn = DatabaseProvisioner.BuildAdminDsn(master.Host, master.Ports.Pg, snap.Config.DbName, secrets);
            foreach (var (role, password) in new[]
                     {
                         (creds.App.User, newAppPassword),
                         (creds.BucketAdmin.User, newBucketAdminPassword),
                         (MoverRole, newMoverPassword),
                     })
            {
                var altered = await db.ExecuteAsync(
                    dsn,
                    DatabaseProvisioner.BuildAlterRolePasswordSql(role, password),
                    ct);
                if (!altered.IsSuccess)
                    return await FailAsync(cluster, altered.Error!, $"alter/{shard.Name}", ct);
            }

            // backup_exec (t02, arch/19 §7): роли может не быть (подсистема
            // бэкапов выключена — G2 не выполнялся) → gexec-гвард вместо голого
            // ALTER: нет роли — CREATE с NEW-паролем; есть — ALTER (идемпотентная
            // перезапись, семантика «четвёртого ALTER» сохранена, spec §3.4).
            var backupGuard = await db.ExecuteScalarAsync(
                dsn, DatabaseProvisioner.BuildBackupExecRoleGuardSql(newBackupPassword), ct);
            if (!backupGuard.IsSuccess)
                return await FailAsync(cluster, backupGuard.Error!, $"alter/{shard.Name}", ct);
            if (backupGuard.Value is string createBackupRole)
            {
                var created = await db.ExecuteAsync(dsn, createBackupRole, ct);
                if (!created.IsSuccess)
                    return await FailAsync(cluster, created.Error!, $"alter/{shard.Name}", ct);
            }
            else
            {
                var alteredBackup = await db.ExecuteAsync(
                    dsn,
                    DatabaseProvisioner.BuildAlterRolePasswordSql(DatabaseProvisioner.BackupExecRole, newBackupPassword),
                    ct);
                if (!alteredBackup.IsSuccess)
                    return await FailAsync(cluster, alteredBackup.Error!, $"alter/{shard.Name}", ct);
            }
        }

        // R3: атомарный коммит — новые креды + перезапись dsn + снятие заявки
        // ОДНОЙ txn (нет двойной ротации из-за сбоя между put и del). Compare по
        // OLD-кредам и по прочитанным dsn: внешняя запись etcdctl между R1 и R3
        // (или гонка репарации dsn) → ретрай тиком со свежими OLD.
        var compares = new List<TxnCompare>
        {
            TxnCompare.ValueEqual(PasswordKey(cluster), creds.App.Password),
            TxnCompare.ValueEqual(MoverKey(cluster), creds.MoverPassword),
            TxnCompare.ValueEqual(BucketAdminPasswordKey(cluster), creds.BucketAdmin.Password),
            TxnCompare.ValueEqual(BackupKey(cluster), creds.BackupPassword),
        };
        var ops = new List<TxnOp>
        {
            new TxnOp.Put(PasswordKey(cluster), newAppPassword, null),
            new TxnOp.Put(MoverKey(cluster), newMoverPassword, null),
            new TxnOp.Put(BucketAdminPasswordKey(cluster), newBucketAdminPassword, null),
            new TxnOp.Put(BackupKey(cluster), newBackupPassword, null),
            new TxnOp.Delete(TicketKey(cluster), Prefix: false),
        };
        foreach (var shard in snap.Shards.Where(s => s.Dsn is not null))
        {
            compares.Add(TxnCompare.ValueEqual(DsnKey(cluster, shard.Name), shard.Dsn!));
            ops.Add(new TxnOp.Put(DsnKey(cluster, shard.Name),
                PasswordRegex().Replace(shard.Dsn!,
                    m => (m.Value.StartsWith(' ') ? " " : "") + "password=" + newBucketAdminPassword),
                null));
        }

        var commit = await TxnAsync(TxnRequest.Of([.. compares], [.. ops]), ct);
        if (!commit.IsSuccess)
            return await FailAsync(cluster, commit.Error!, "committing", ct);
        if (!commit.Value.Succeeded)
            return await FailAsync(cluster,
                new ApplicationException(
                    "креды/dsn изменились с момента чтения (внешняя запись?) — ретрай тиком"),
                "commit-conflict", ct);

        // R4: снапшот P12 (точка изменения, best-effort делегат) + journal done.
        if (snapshot is not null)
        {
            var shot = await snapshot(ct);
            if (!shot.IsSuccess)
                return await FailAsync(cluster, shot.Error!, "snapshot", ct);
        }

        return await Finish(cluster, "done", ProcessOutcome.Done, ct);
    }

    // Замена password= в conninfo dsn-ключа (пароль bucket_admin внутри dsn).
    // Пароли AppSecretGenerator — [A-Za-z0-9], экранирования в conninfo не требуют.
    [GeneratedRegex(@"(^| )password=[^ ]*", RegexOptions.CultureInvariant)]
    private static partial Regex PasswordRegex();

    private static string TicketKey(string cluster) => $"/pgworker/rotations/{cluster}";

    private static string PasswordKey(string cluster) => $"/clusters/{cluster}/app_password";

    private static string MoverKey(string cluster) => $"/clusters/{cluster}/mover_password";

    private static string BucketAdminPasswordKey(string cluster) => $"/clusters/{cluster}/bucket_admin_password";

    private static string BackupKey(string cluster) => $"/clusters/{cluster}/backup_password";

    private static string DsnKey(string cluster, string shard) => $"/clusters/{cluster}/shards/{shard}/dsn";

    // Валидная заявка: JSON с числовым requested_unix (панель §9.8 п.3).
    private static bool IsWellFormed(string raw)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            return doc.RootElement.TryGetProperty("requested_unix", out var unix)
                   && unix.ValueKind == JsonValueKind.Number;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    // Мастер шарда: точное имя ноды из master-ключа → doorman-порт (уникален
    // per-node) → HA-лидер контура /service/<C>-<X>/leader → Patroni REST
    // (паттерн ShardEndpoints.ResolveMasterAsync). Совпадение master-ключа ПО
    // ХОСТУ недопустимо: при EnableDoorman=false ключ вырождается в host:0,
    // и все ноды одного хоста равнозначны — резолв вернул бы произвольную
    // (первую/случайный порядок portalloc) ноду, ALTER ROLE на реплике падает
    // 25006 (read-only) — ротация зацикливалась в фазе started (e2e-факт t04).
    private async Task<NodeAddress?> ResolveMasterAsync(
        string cluster, ShardSpec shard, IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var shardNodes = addresses
            .Where(p => p.Key.StartsWith($"{shard.Name}/", StringComparison.Ordinal))
            .ToDictionary(p => p.Key.Split('/')[1], p => p.Value);

        if (!string.IsNullOrWhiteSpace(shard.Master))
        {
            var parts = shard.Master.Split(':');
            if (shardNodes.TryGetValue(parts[0], out var byName))
                return byName;
            if (parts.Length == 2 && int.TryParse(parts[1], out var doorman) && doorman > 0)
            {
                var byDoormanPort = shardNodes.FirstOrDefault(p => p.Value.Ports.Doorman == doorman);
                if (byDoormanPort.Value is not null)
                    return byDoormanPort.Value;
            }
        }

        // HA-лидер контура (окно failover с протухшим master-ключом).
        var leader = await GetAsync($"/service/{cluster}-{shard.Name}/leader", ct);
        if (leader.IsSuccess && leader.Value is { } leaderKv)
        {
            try
            {
                using var doc = JsonDocument.Parse(leaderKv.Value);
                if (doc.RootElement.TryGetProperty("name", out var name)
                    && name.GetString() is { Length: > 0 } leaderName
                    && shardNodes.TryGetValue(leaderName, out var leaderAddr))
                    return leaderAddr;
            }
            catch (JsonException)
            {
                // битый leader-ключ — просто идём дальше по цепочке
            }
        }

        foreach (var node in shardNodes)
        {
            var members = await probe.GetClusterAsync(node.Value, ct);
            if (!members.IsSuccess)
                continue;
            // Patroni 3.x в /cluster называет мастера "leader" (legacy: "master").
            var master = members.Value.FirstOrDefault(m =>
                m.Role is "master" or "leader" or "primary" && m.State == "running");
            if (master is not null && shardNodes.TryGetValue(master.Name, out var addr))
                return addr;
        }

        return null;
    }

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await GetAsync($"/pgworker/portalloc/{cluster}", ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        if (result.Value is not { } kv)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(
                (IReadOnlyDictionary<string, NodeAddress>)new Dictionary<string, NodeAddress>());

        return Portalloc.Parse(cluster, kv.Value);
    }

    private async Task<Result<ProcessOutcome>> Finish(
        string cluster, string phase, ProcessOutcome outcome, CancellationToken ct)
    {
        var written = await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, null, ct);
        return written.IsSuccess
            ? Result<ProcessOutcome>.Success(outcome)
            : Result<ProcessOutcome>.Failed(written.Error!);
    }

    private async Task<Result<ProcessOutcome>> FailAsync(
        string cluster, Exception error, string phase, CancellationToken ct)
    {
        await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, error.Message, ct);
        return Result<ProcessOutcome>.Failed(error);
    }

    // Failover-обёртки: первый успешный endpoint выигрывает (образец AddShardProcess).
    private async Task<Result<Kv?>> GetAsync(string key, CancellationToken ct)
        => await WithFailoverAsync(endpoint => etcd.GetAsync(endpoint, key, ct));

    private async Task<Result> DeleteAsync(string key, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.DeleteAsync(endpoint, key, prefix: false, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private async Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
        => await WithFailoverAsync(endpoint => etcd.TxnAsync(endpoint, req, ct));

    private async Task<Result<T>> WithFailoverAsync<T>(Func<string, Task<Result<T>>> call)
    {
        Result<T>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
