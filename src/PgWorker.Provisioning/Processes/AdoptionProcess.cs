using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Sql;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// Усыновление кластера (spec §3.2, arch/14 §5 J AD0–AD4): Active-кластер с
/// dsn-шардами без записей portalloc получает адреса из HA-контура + docker
/// (InspectNodesAsync) и переходит в обычный домен воркера. «Не наших»
/// объектов не существует; 0 docker-находок — тихий skip (кластер вне
/// docker-хостов воркера). Идемпотентно: только отсутствующие записи.
/// </summary>
public sealed class AdoptionProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ShardEndpoints shards,
    ISqlExecutor sql,
    IAppSecretEnsurer appSecret,
    IAppParamsEnsurer appParams,
    InstallSecrets secrets,
    ClaimStore claims,
    WorkJournal journal,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Op = "adopt";

    public async Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;

        // Мутации — только держателем живого клэйма (инвариант arch/14 §3.3).
        if (!claims.IsMine(cluster))
            return Result<ProcessOutcome>.Failed(new ApplicationException(
                $"{Op} {cluster}: клэйм не наш (или потерян) — мутации запрещены"));
        if (snap.Config.State != ClusterState.Active)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // провижининг/демонтаж — свои процессы

        // AD1: кандидаты — шарды с dsn; недостающие ноды = HA-members − portalloc.
        var existing = await shards.ReadPortAllocAsync(cluster, ct);
        if (!existing.IsSuccess)
            return Result<ProcessOutcome>.Failed(existing.Error!);

        var missingByShard = new Dictionary<string, List<string>>();
        foreach (var shard in snap.Shards.Where(s => s.Dsn is not null && !s.ToRemove))
        {
            var members = await ReadMemberNamesAsync(cluster, shard.Name, ct);
            if (!members.IsSuccess)
                return Result<ProcessOutcome>.Failed(members.Error!);
            var missing = members.Value
                .Where(n => !existing.Value.ContainsKey($"{shard.Name}/{n}"))
                .ToList();
            if (missing.Count > 0)
                missingByShard[shard.Name] = missing;
        }

        // Инвариант «воркер — хозяин» (arch/14 §3): ensure БД и ролей бакетного
        // слоя выполняется КАЖДЫЙ тик для всех dsn-шардов, а не только при
        // усыновлении нод. Иначе падение ensure ПОСЛЕ записи portalloc (AD2)
        // терялось навсегда: missingByShard пуст → ранний выход → шарды
        // оставались без app/bucket_mover (42704/28000 в move/repair), хотя
        // adopt «Done». Гварды идемпотентны — на здоровом кластере это
        // несколько дешёвых SELECT на тик.
        var creds = await appSecret.EnsureAsync(cluster, ct);
        if (!creds.IsSuccess)
            return await FailAsync(cluster, creds.Error!, ct);

        foreach (var shard in snap.Shards.Where(s => s.Dsn is not null && !s.ToRemove))
        {
            var master = await shards.ResolveMasterAsync(cluster, shard, existing.Value, ct);
            if (!master.IsSuccess)
                return await FailAsync(cluster, master.Error!, ct);
            if (master.Value is not { } invariantMaster)
                continue; // мастер ещё не определён (portalloc пуст/выборы) — обеспечит путь усыновления ниже

            var invariantDsn = ShardEndpoints.AdminDsn(invariantMaster, snap.Config.DbName, secrets);
            var ensured = await EnsureShardDatabaseAsync(invariantDsn, snap, creds.Value, ct);
            if (!ensured.IsSuccess)
                return await FailAsync(cluster, ensured.Error!, ct);
        }

        if (missingByShard.Count == 0)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // всё на месте (роли обеспечены) — no-op

        var wanted = missingByShard.Values.SelectMany(v => v).Distinct().ToList();
        var discovered = await driver.InspectNodesAsync(wanted, ct);
        if (!discovered.IsSuccess)
            return Result<ProcessOutcome>.Failed(discovered.Error!);
        if (discovered.Value.Count == 0)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // тихий skip (spec §2.5)

        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return Result<ProcessOutcome>.Failed(started.Error!);

        // Spec §3.1: неопознанные ноды (неоднозначный матчинг/нет контейнера) —
        // безопасный пропуск С журнальной записью: оператор видит, кто не
        // усыновлен; усыновление частично (остальные ноды — следующим тиком
        // после разбора, идемпотентность merge это допускает).
        var skipped = wanted.Where(n => !discovered.Value.ContainsKey(n)).ToList();
        if (skipped.Count > 0)
            await journal.WritePhaseAsync(cluster, Op, "skipped", claims.InstanceId,
                $"контейнеры не опознаны (неоднозначность/отсутствие): {string.Join(", ", skipped)}", ct);

        // AD2: merge portalloc — только отсутствующие записи, под клэймом.
        var merged = new Dictionary<string, NodeAddress>(existing.Value);
        foreach (var (name, node) in discovered.Value)
        {
            var shard = missingByShard.First(kv => kv.Value.Contains(name)).Key;
            merged[$"{shard}/{name}"] = node.ToAddress();
        }

        var put = await PutAsync($"/pgworker/portalloc/{cluster}", Portalloc.Serialize(merged), ct);
        if (!put.IsSuccess)
            return await FailAsync(cluster, put.Error!, ct);

        // AD3: nodes-ключи put-if-absent (декларация следует за фактом).
        foreach (var (shard, nodes) in missingByShard)
        {
            var ensuredNodes = nodes.Where(n => discovered.Value.ContainsKey(n)).ToList();
            foreach (var node in ensuredNodes)
            {
                var key = $"/clusters/{cluster}/shards/{shard}/nodes/{node}/state";
                var txn = await TxnPutIfAbsentAsync(key, "RUNNING", ct);
                if (!txn.IsSuccess)
                    return await FailAsync(cluster, txn.Error!, ct);
            }

            var appParamsDone = await appParams.EnsureShardAsync(cluster, shard, ensuredNodes, ct);
            if (!appParamsDone.IsSuccess)
                return await FailAsync(cluster, appParamsDone.Error!, ct);
        }

        // AD3 (продолжение): app-секрет обеспечен инвариантом выше; здесь —
        // до-ensure шардов, чьи мастера резолвились только после merge portalloc.
        foreach (var shard in snap.Shards.Where(s => missingByShard.ContainsKey(s.Name)))
        {
            var master = await shards.ResolveMasterAsync(cluster, shard, merged, ct);
            if (!master.IsSuccess)
                return await FailAsync(cluster, master.Error!, ct);
            if (master.Value is null)
                return await FailAsync(cluster, new ApplicationException(
                    $"{Op} {cluster}: мастер шарда '{shard.Name}' не определён — повтор следующим тиком"), ct);

            var dsn = ShardEndpoints.AdminDsn(master.Value, snap.Config.DbName, secrets);
            var provisioned = await EnsureShardDatabaseAsync(dsn, snap, creds.Value, ct);
            if (!provisioned.IsSuccess)
                return await FailAsync(cluster, provisioned.Error!, ct);
        }

        // AD4: снапшот P12 (точка изменения, best-effort) + journal done.
        if (snapshot is not null)
            await snapshot(ct); // неудача — не повод откатывать усыновление (журналируется SnapshotJob)

        await journal.WritePhaseAsync(cluster, Op, "done", claims.InstanceId,
            $"усыновлено нод: {discovered.Value.Count} ({string.Join(", ", discovered.Value.Keys.OrderBy(n => n, StringComparer.Ordinal))})"
            + (skipped.Count > 0 ? $"; пропущено: {skipped.Count}" : ""), ct);
        return Result<ProcessOutcome>.Success(ProcessOutcome.Done);
    }

    // Имена членов HA-контура = последние сегменты ключей /service/<scope>/members/*.
    private async Task<Result<IReadOnlyList<string>>> ReadMemberNamesAsync(
        string cluster, string shard, CancellationToken ct)
    {
        var range = await RangeAsync($"/service/{cluster}-{shard}/members/", ct);
        if (!range.IsSuccess)
            return Result<IReadOnlyList<string>>.Failed(range.Error!);
        return Result<IReadOnlyList<string>>.Success(
            (IReadOnlyList<string>)range.Value
                .Select(kv => kv.Key.Split('/')[^1])
                .Where(n => n.Length > 0)
                .Distinct().OrderBy(n => n, StringComparer.Ordinal).ToList());
    }

    // Ensure БД + ролей бакетного слоя на мастере усыновляемого шарда —
    // идемпотентные тексты P2.3 (gexec-гварды → exec, ALTER app-пароля).
    private async Task<Result> EnsureShardDatabaseAsync(
        string dsn, ClusterSnapshot snap, AppCredentials app, CancellationToken ct)
    {
        var db = await sql.EnsureDatabaseAsync(dsn, snap.Config.DbName, ct);
        if (!db.IsSuccess)
            return db;

        foreach (var guard in DatabaseProvisioner.BuildRoleGuardsSql(
                     secrets, app, snap.Config.BucketAdminUser, snap.Config.BucketAdminPassword))
        {
            var role = await sql.ExecuteScalarAsync(dsn, guard, ct);
            if (!role.IsSuccess)
                return role;
            if (role.Value is string { Length: > 0 } create)
            {
                var exec = await sql.ExecuteAsync(dsn, create, ct);
                if (!exec.IsSuccess)
                    return exec;
            }
        }

        foreach (var execSql in DatabaseProvisioner.BuildRoleExecSql(snap.Config.BucketAdminUser))
        {
            var exec = await sql.ExecuteAsync(dsn, execSql, ct);
            if (!exec.IsSuccess)
                return exec;
        }

        var alter = await sql.ExecuteAsync(dsn, DatabaseProvisioner.BuildAlterAppPasswordSql(app), ct);
        return alter;
    }

    private async Task<Result<ProcessOutcome>> FailAsync(string cluster, Exception error, CancellationToken ct)
    {
        await journal.WritePhaseAsync(cluster, Op, "failed", claims.InstanceId, error.Message, ct);
        return Result<ProcessOutcome>.Failed(error);
    }

    // Failover-обёртки: первый успешный endpoint выигрывает (паттерн ShardEndpoints).
    private async Task<Result> PutAsync(string key, string value, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.PutAsync(endpoint, key, value, lease: null, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    // Put-if-absent (txn NotExists = version==0): операторские nodes-ключи не
    // перезаписываются; эталон txn — ClaimStore.TryPutLeasedKeyAsync.
    private async Task<Result> TxnPutIfAbsentAsync(string key, string value, CancellationToken ct)
    {
        Result<TxnResult>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.TxnAsync(endpoint, TxnRequest.Of(
                [TxnCompare.NotExists(key)],
                [new TxnOp.Put(key, value, null)]), ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private async Task<Result<IReadOnlyList<Kv>>> RangeAsync(string prefix, CancellationToken ct)
    {
        Result<IReadOnlyList<Kv>>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.RangeAsync(endpoint, prefix, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
