using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Planning;
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
    PortAllocIndex portAlloc,
    PortAllocLock portLock,
    PlacementOptions placementOpts,
    EtcdEndpoints etcdEndpoints,
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

        // AD2' (Д2, arch/14 §5 J): инвариант адресов Active — portalloc/dsn = факт
        // живых канонических контейнеров; расхождение репарируется под клэймом с
        // журналом. Transport-провал инспекции — transient: тик продолжается
        // без репарации (следующий тик повторит).
        var reconciled = await ReconcileAddressesAsync(snap, existing.Value, ct);
        if (reconciled.Error is PortLockBusyException)
        {
            await journal.WritePhaseAsync(cluster, Op, "waiting-portalloc-lock", claims.InstanceId, null, ct);
            return Result<ProcessOutcome>.Success(ProcessOutcome.InProgress);
        }
        if (!reconciled.IsSuccess)
            return await FailAsync(cluster, reconciled.Error!, ct);
        existing = Result<IReadOnlyDictionary<string, NodeAddress>>.Success(reconciled.Value);

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

            var ensured = await EnsureShardDatabaseAsync(invariantMaster, shard, snap, creds.Value, ct);
            if (!ensured.IsSuccess)
                return await FailAsync(cluster, ensured.Error!, ct);
        }

        if (missingByShard.Count == 0)
            return Result<ProcessOutcome>.Success(ProcessOutcome.Done); // всё на месте (роли обеспечены) — no-op

        var wanted = missingByShard.Values.SelectMany(v => v).Distinct().ToList();
        // Фильтр кластера (live-Ф7): чужие pgw-<C'>-* с теми же именами нод больше
        // не создают ложную неоднозначность; внешние контейнеры кластера — видны.
        var discovered = await driver.InspectNodesAsync(cluster, wanted, ct);
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

            var provisioned = await EnsureShardDatabaseAsync(master.Value, shard, snap, creds.Value, ct);
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

    // AD2' (Д2, spec §3.7): кандидаты — nodes-ключи снапшота ∪ HA-members (как AD1:
    // сценарий «Active + dsn, nodes-ключей нет» тоже репарируется); merge факта
    // канонических контейнеров (тот же фильтр, что P1) + перепланирование занятых
    // чужим (PortPlanConvergence) + пересборка dsn из фактического portalloc.
    // 0 находок — тихий skip (кластер вне docker-хостов); transport-провал
    // инспекции — transient (не роняем тик).
    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReconcileAddressesAsync(
        ClusterSnapshot snap, IReadOnlyDictionary<string, NodeAddress> existing, CancellationToken ct)
    {
        var cluster = snap.Config.Cluster;
        var dsnShards = snap.Shards.Where(s => s.Dsn is not null && !s.ToRemove).ToList();

        // Кандидаты адресов по каждому dsn-шарду: nodes-ключи ∪ HA-members
        // (members читаются и ниже в AD1 — дешёвый range, дублирование осознанное).
        var candidatesByShard = new Dictionary<string, List<string>>();
        foreach (var shard in dsnShards)
        {
            var members = await ReadMemberNamesAsync(cluster, shard.Name, ct);
            if (!members.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(members.Error!); // etcd-транспорт — как AD1
            var names = shard.Nodes.Select(n => n.Name).Concat(members.Value).Distinct().ToList();
            if (names.Count > 0)
                candidatesByShard[shard.Name] = names;
        }

        if (candidatesByShard.Count == 0)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(existing); // кандидатов нет — репарировать нечего

        var discovered = await driver.InspectNodesAsync(
            cluster, candidatesByShard.Values.SelectMany(v => v).Distinct().ToList(), ct);
        if (!discovered.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(existing); // transient
        if (discovered.Value.Count == 0)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(existing); // тихий skip (вне docker-хостов)

        var merged = new Dictionary<string, NodeAddress>(existing);
        var selfFactByNode = new Dictionary<string, IReadOnlySet<(string, int)>>();
        var changed = false;
        foreach (var (shardName, names) in candidatesByShard)
            foreach (var nodeName in names)
            {
                var key = $"{shardName}/{nodeName}";
                if (!discovered.Value.TryGetValue(nodeName, out var node))
                    continue;
                var canonicalObject = $"pgw-{cluster}-{key.Replace('/', '-')}";
                if (node.Object != canonicalObject || node.Pg <= 0 || node.Patroni <= 0)
                    continue; // не наша находка — фильтр канонического имени (как P1)
                var fact = node.ToAddress() with { Object = null };
                var factPorts = new HashSet<(string, int)>();
                foreach (var p in new[] { fact.Ports.Pg, fact.Ports.Patroni, fact.Ports.Doorman })
                    if (p > 0)
                        factPorts.Add((fact.Host, p));
                selfFactByNode[key] = factPorts; // подтверждение per-node (живой-Ф7)
                if (merged.TryGetValue(key, out var current) && current.Object is not null)
                    continue; // object-записи не перезаписываем (R9)
                if (!merged.TryGetValue(key, out var same) || !same.Equals(fact))
                {
                    merged[key] = fact;
                    changed = true;
                }
            }

        // t90: пред-выход ДО глобального portalloc-клэйма (spec §3.2 п.3 «без
        // изменений (changed=false) — до лока»): merge ничего не изменил И каждый
        // ключ-кандидат "{shard}/{name}" (candidatesByShard: nodes ∪ members) имеет
        // запись, подтверждённую фактом своей ноды либо object (AllConfirmed).
        // Недобор кандидата без записи даёт false — лок обязателен; расхождение
        // записи с фактом (changed=true) — тоже. Все подтверждено → detach пуст,
        // allocate не сработает, changed останется false — секция была бы no-op,
        // лок не берём (симметрия пред-выхода P1: тики здорового Active-кластера
        // не соперничают за глобальный клэйм, arch/14 §2.4).
        var allWanted = candidatesByShard
            .SelectMany(kv => kv.Value.Select(name => $"{kv.Key}/{name}"))
            .ToList();
        if (changed || !PortPlanConvergence.AllConfirmed(merged, selfFactByNode, allWanted))
        {
            // t90: глобальный portalloc-клэйм — чтение кросс-картины занятости,
            // пере-allocate и ЗАПИСЬ portalloc взаимоисключающи между кластерами/
            // инстансами (инвариант arch/14 §2.4: порты выбираются под локом —
            // публикация до release, иначе конкурент успевает выбрать те же порты).
            // Сбой захвата — обычный фейл (бэкофф); занят — PortLockBusyException
            // → waiting-portalloc-lock, следующий тик повторяет.
            var acquired = await portLock.TryAcquireAsync(ct);
            if (!acquired.IsSuccess)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(acquired.Error!);
            if (!acquired.Value)
                return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(new PortLockBusyException());
            try
            {
                // Перепланирование занятых (Д1-механика для Active, живой-Ф7): занятость =
                // docker-публикации (чужие И своих соседей — дубликат внутри кластера тоже
                // конфликт) ∪ portalloc соседей; подтверждение per-node. Placement строится
                // по dsn-шардам снапшота: у шарда без nodes-ключей detach-нутая нода
                // переаллоцируется следующим тиком (после того как AD3 доведёт nodes).
                var dockerBusy = await driver.GetBusyPortsAsync(ct);
                if (!dockerBusy.IsSuccess)
                    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(dockerBusy.Error!);
                var foreignAlloc = await portAlloc.ReadBusyAsync(cluster, ct);
                if (!foreignAlloc.IsSuccess)
                    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(foreignAlloc.Error!);
                var busy = new HashSet<(string, int)>(foreignAlloc.Value);
                foreach (var p in dockerBusy.Value)
                    busy.Add(p);
                if (PortPlanConvergence.DetachColliding(merged, selfFactByNode, busy))
                {
                    // Недобор адресов снятых нод: переаллокация (паттерн P1-недобора);
                    // taken = busy − факты подтверждённых записей (переиспользование валидных).
                    var hosts = await driver.GetHostsAsync(ct);
                    if (!hosts.IsSuccess)
                        return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(hosts.Error!);
                    var taken = new HashSet<(string, int)>(busy);
                    foreach (var p in PortPlanConvergence.ConfirmedFact(merged, selfFactByNode))
                        taken.Remove(p);
                    var plan = PlacementPlanner.Plan(dsnShards, hosts.Value);
                    var allocated = PortAllocator.Allocate(plan, merged, taken, placementOpts.PortFrom, placementOpts.PortTo);
                    if (!allocated.IsSuccess)
                        return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(allocated.Error!);
                    foreach (var (k, addr) in allocated.Value)
                        merged[k] = addr;
                    changed = true;
                }

                if (changed)
                {
                    var put = await PutAsync($"/pgworker/portalloc/{cluster}", Portalloc.Serialize(merged), ct);
                    if (!put.IsSuccess)
                        return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(put.Error!);
                    await journal.WritePhaseAsync(cluster, Op, "repaired-portalloc", claims.InstanceId, null, ct);
                }
            }
            finally
            {
                await portLock.ReleaseAsync();
            }
        }

        // dsn-инвариант: пересборка multi-host dsn по кандидатам (nodes ∪ members) из
        // фактического portalloc (креды как P2.5: per-cluster override → глобальные).
        // Граница (живой-Ф7, R9-симметрия «object-записи не перезаписываются»):
        // шард с ХОТЬ ОДНОЙ object-нодой (внешний контур as-*/hc*) — его dsn
        // ОПЕРАТОРСКИЙ факт (postgres-подписки сидом по именам as-нод; host записей
        // «local» не резолвится внутри postgres приёмника) — НЕ пересобираем.
        foreach (var shard in dsnShards)
        {
            if (!candidatesByShard.TryGetValue(shard.Name, out var names) || names.Count == 0)
                continue;
            var ordered = names.OrderBy(n => n, StringComparer.Ordinal).ToList();
            if (ordered.Any(n => !merged.ContainsKey($"{shard.Name}/{n}")))
                continue; // адресов не хватает — усыновление/следующий тик доведут
            if (ordered.Any(n => merged[$"{shard.Name}/{n}"].Object is not null))
                continue; // внешний (object) шард: dsn — операторский факт (R9)
            var hosts = string.Join(",", ordered.Select(n => merged[$"{shard.Name}/{n}"].Host));
            var ports = string.Join(",", ordered.Select(n => merged[$"{shard.Name}/{n}"].Ports.Pg));
            var user = snap.Config.BucketAdminUser ?? "bucket_admin";
            var password = snap.Config.BucketAdminPassword ?? secrets.BucketAdminPassword;
            var dsn = $"host={hosts} port={ports} dbname={snap.Config.DbName} user={user} password={password}";
            if (shard.Dsn != dsn)
            {
                var put = await PutAsync($"/clusters/{cluster}/shards/{shard.Name}/dsn", dsn, ct);
                if (!put.IsSuccess)
                    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(put.Error!);
                await journal.WritePhaseAsync(cluster, Op, "repaired-dsn", claims.InstanceId, null, ct);
            }
        }

        // Дефект-B фикс (живой-Ф7, дух AD2'): запись portalloc БЕЗ живого (running)
        // контейнера — Created/exited-черепок или снесённый контейнер при
        // node state=RUNNING (процессные пути скипают RUNNING, инспекция видит
        // только running) — чинится EnsureNode НАПРЯМУЮ: сверка портов увидит
        // расхождение Created-контейнера (или контейнер отсутствует) → stop+rm+
        // create по плану (план уже без дубликатов — detach выше). «Воркер —
        // хозяин»: сломали контейнер/план → тик сам чинит, без оператора.
        // Граница с эвакуацией (arch/14 §5 D, t09): шард, в котором НЕ жив ни
        // один контейнер, — сценарий всего-шарда-мёртв: его судьба — BucketEvacuator
        // (E0–E4 после NodeDeadSec+ShardDeadSec), а не recreate-гонка: мгновенное
        // пересоздание стопнутых нод не давало supervise досчитать до порога и
        // эвакуация не стартовала вовсе.
        var recreated = false;
        foreach (var (shardName, names) in candidatesByShard)
        {
            if (names.All(n => !discovered.Value.ContainsKey(n)))
                continue; // весь шард без живых контейнеров — домен эвакуатора
            var topology = new ShardTopology(
                cluster, shardName, $"{cluster}-{shardName}",
                merged
                    .Where(p => p.Key.StartsWith($"{shardName}/", StringComparison.Ordinal))
                    .ToDictionary(p => p.Key.Split('/')[1], p => p.Value));
            foreach (var nodeName in names)
            {
                var key = $"{shardName}/{nodeName}";
                if (!merged.TryGetValue(key, out var addr) || addr.Object is not null)
                    continue; // записи нет / усыновлённая (object) — чужой контейнер, R9
                if (discovered.Value.ContainsKey(nodeName))
                    continue; // живой контейнер на месте — сверка EnsureNode-путей процессов

                var ensured = await driver.EnsureNodeAsync(
                    topology, nodeName, addr, secrets, etcdEndpoints, resources: null, ct);
                if (!ensured.IsSuccess)
                    return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(ensured.Error!);
                recreated = true;
            }
        }

        if (recreated)
            await journal.WritePhaseAsync(cluster, Op, "recreated-node", claims.InstanceId, null, ct);

        return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(merged);
    }

    // Ensure БД + ролей бакетного слоя + схем владельца по routing на мастере
    // шарда — идемпотентные тексты P2.3/P2.6 (gexec-гварды → exec, ALTER
    // app-пароля, CREATE SCHEMA IF NOT EXISTS). БД кластера создаётся
    // подключением к postgres (паттерн P2.x): после утраты данных и
    // re-bootstrap Patroni артефакты etcd есть, а базы/схем нет (initdb создал
    // только postgres; схемы поднимал лишь путь provisioning) — целевой dsn дал
    // бы вечный 3D000, а панельный inventory (routing ↔ схемы) не сошёлся бы
    // никогда («воркер — хозяин»: поднимает сам, живой-Ф7').
    private async Task<Result> EnsureShardDatabaseAsync(
        NodeAddress master, ShardSpec shard, ClusterSnapshot snap, AppCredentials app, CancellationToken ct)
    {
        var adminDsn = ShardEndpoints.AdminDsn(master, "postgres", secrets);
        var db = await sql.EnsureDatabaseAsync(adminDsn, snap.Config.DbName, ct);
        if (!db.IsSuccess)
            return db;

        var dsn = ShardEndpoints.AdminDsn(master, snap.Config.DbName, secrets);

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
        if (!alter.IsSuccess)
            return alter;

        // Схемы бакетов-владельцев по routing (P2.6): идемпотентный IF NOT EXISTS —
        // здоровый шард no-op, опустошённый (re-bootstrap) — поднимается инвариантом.
        var bucketIds = snap.Routing
            .Where(r => r.Owner == shard.Name)
            .Select(r => r.Id)
            .OrderBy(i => i)
            .ToList();
        if (bucketIds.Count > 0)
        {
            var schemas = await sql.ExecuteAsync(
                dsn, DatabaseProvisioner.BuildSchemasSql(
                    snap.Config.DbName, bucketIds,
                    snap.Config.BucketAdminUser ?? "bucket_admin", app.User), ct);
            if (!schemas.IsSuccess)
                return schemas;
        }

        return Result.Success();
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
