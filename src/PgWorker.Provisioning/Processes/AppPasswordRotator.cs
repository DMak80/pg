using System.Text.Json;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Sql;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// Ротация per-cluster app-пароля по заявке /pgworker/rotations/&lt;C&gt;
/// (arch/14 §5 I, spec §4.3): R1 ensure app-секрета → R2 ALTER ROLE на мастере
/// каждого шарда с dsn (реплики получают pg_authid физической репликацией) →
/// R3 атомарный txn [compare value==OLD][put app_password=NEW; del заявки] →
/// R4 снапшот P12. transient-сбой → заявка жива, пароль в etcd НЕ меняется,
/// следующий тик повторяет с начала со свежим NEW (ALTER идемпотентен
/// перезаписью). Вызывается только держателем клэйма &lt;C&gt;.
/// </summary>
public sealed class AppPasswordRotator(
    IEtcdGateway etcd,
    string[] endpoints,
    ISqlExecutor db,
    ShardProbe probe,
    ClaimStore claims,
    WorkJournal journal,
    InstallSecrets secrets,
    IAppSecretEnsurer appSecret,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Op = "rotate-app-password";

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

        // R1: ensure app-секрета (P1.5) — OLD после этого существует.
        var creds = await appSecret.EnsureAsync(cluster, ct);
        if (!creds.IsSuccess)
            return await FailAsync(cluster, creds.Error!, "ensure-app-secret", ct);

        // R2: ALTER ROLE на мастере каждого ПОДНЯТОГО шарда (dsn есть; шард без
        // dsn — домен AddShardProcess: роль создастся/выравнивается по свежему
        // app_password, spec §4.3 R2).
        var newSecret = AppSecretGenerator.Generate();
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
            var altered = await db.ExecuteAsync(
                dsn,
                DatabaseProvisioner.BuildAlterAppPasswordSql(new AppCredentials(creds.Value.User, newSecret)),
                ct);
            if (!altered.IsSuccess)
                return await FailAsync(cluster, altered.Error!, $"alter/{shard.Name}", ct);
        }

        // R3: атомарный коммит — put нового пароля + снятие заявки ОДНОЙ txn
        // (нет двойной ротации из-за сбоя между put и del); compare по OLD —
        // внешняя запись etcdctl между R1 и R3 → ретрай тиком со свежим OLD.
        var commit = await TxnAsync(
            TxnRequest.Of(
                [TxnCompare.ValueEqual(PasswordKey(cluster), creds.Value.Password)],
                [
                    new TxnOp.Put(PasswordKey(cluster), newSecret, null),
                    new TxnOp.Delete(TicketKey(cluster), Prefix: false),
                ]),
            ct);
        if (!commit.IsSuccess)
            return await FailAsync(cluster, commit.Error!, "committing", ct);
        if (!commit.Value.Succeeded)
            return await FailAsync(cluster,
                new ApplicationException(
                    "app_password изменился с момента чтения (внешняя запись?) — ретрай тиком"),
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

    private static string TicketKey(string cluster) => $"/pgworker/rotations/{cluster}";

    private static string PasswordKey(string cluster) => $"/clusters/{cluster}/app_password";

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
