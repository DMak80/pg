using System.Text.Json;
using System.Text.RegularExpressions;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Sql;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// Ротация per-cluster секретов по заявке /pgworker/rotations/&lt;C&gt; (t02,
/// arch/14 §5 I): app + bucket_admin + bucket_mover. R1 ensure тройки кредов →
/// R2 ALTER ROLE трёх ролей на мастере каждого шарда с dsn (реплики получают
/// pg_authid физической репликацией) → R3 атомарный txn [compare value==OLD по
/// кредам и ВСЕМ dsn-ключам][put новые креды; перезапись dsn; del заявки] →
/// R4 снапшот P12. transient-сбой → заявка жива, креды в etcd НЕ меняются,
/// следующий тик повторяет с начала со свежими NEW (ALTER идемпотентен
/// перезаписью). Имя journal-op сохранено (rotate-app-password) — совместимость
/// журналов (образец RotationRole.Phase, arch/16 §5 H). Вызывается только
/// держателем клэйма &lt;C&gt;. Ротация при активном переезде: mover-DSN
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

        // R2: ALTER ROLE трёх ролей на мастере каждого ПОДНЯТОГО шарда (dsn есть;
        // шард без dsn — домен AddShardProcess: роли создадутся/выровняются по
        // свежим кредам, §5 I R2). NEW-пароли генерируются на попытку — ALTER
        // идемпотентен перезаписью, регенерация между тиками безопасна.
        var newAppPassword = AppSecretGenerator.Generate();
        var newMoverPassword = AppSecretGenerator.Generate();
        var newBucketAdminPassword = AppSecretGenerator.Generate();
        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return await FailAsync(cluster, addresses.Error!, "portalloc", ct);

        foreach (var shard in snap.Shards.Where(s => s.Dsn is not null))
        {
            var master = await ResolveMasterAsync(shard, addresses.Value, ct);
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
        };
        var ops = new List<TxnOp>
        {
            new TxnOp.Put(PasswordKey(cluster), newAppPassword, null),
            new TxnOp.Put(MoverKey(cluster), newMoverPassword, null),
            new TxnOp.Put(BucketAdminPasswordKey(cluster), newBucketAdminPassword, null),
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

    // Мастер шарда: host из master-ключа (по portalloc) → fallback Patroni REST
    // (паттерн ProvisioningProcess.ResolveMasterAsync, упрощённо для чтения).
    private async Task<NodeAddress?> ResolveMasterAsync(
        ShardSpec shard, IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var byKey = shard.Master?.Split(':')[0];
        foreach (var (key, addr) in addresses.Where(p =>
                     p.Key.StartsWith($"{shard.Name}/", StringComparison.Ordinal)))
        {
            var node = key.Split('/')[1];
            if (byKey is { Length: > 0 } && (byKey == addr.Host || byKey == node))
                return addr;
        }

        foreach (var pair in addresses.Where(p =>
                     p.Key.StartsWith($"{shard.Name}/", StringComparison.Ordinal)))
        {
            var members = await probe.GetClusterAsync(pair.Value, ct);
            if (!members.IsSuccess)
                continue;
            var master = members.Value.FirstOrDefault(m =>
                m.Role is "master" or "leader" or "primary" && m.State == "running");
            if (master is not null && addresses.TryGetValue($"{shard.Name}/{master.Name}", out var addr))
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
