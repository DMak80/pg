using System.Text.Json;
using System.Text.RegularExpressions;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Etcd.Parsing;
using Shared.Etcd.Client;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Sql;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// Ротация per-cluster секретов по заявке /pgworker/rotations/&lt;C&gt; (
/// arch/14 §5 I / arch/19 §7): app + bucket_admin + bucket_mover + backup_exec
/// + rest_password (: basic-auth Patroni REST — rolling-пересоздание нод
/// общим шагом надзора, пара NEW «в полёте» — rest_pending журнала; txn-коммит
/// R3 после применения на всех нодах; гварды окна — в NodeSupervisor).
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
    INodeBootstrapDelivery bootstrap,
    PgtuneInputsFactory pgtune,
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
                $"{Op} {cluster}: claim is not ours (or lost) — mutations forbidden"));

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

        // Гвард «тихого окна» (arch/14 §5 I): ротация мутит pg_authid (R2
        // ALTER app на мастере каждого шарда) — клиенты с etcd-кредами (OLD
        // до R3) наInitializing-нодах теряют вход. Пока ВСЕ канонические ноды
        // кластера не ready (статусные машины агентов), мутаций нет: тик —
        // InProgress (заявка жива, R1/R2 повторяются следующим тиком с начала).
        var addressesGuard = await ReadPortAllocAsync(cluster, ct);
        if (!addressesGuard.IsSuccess)
            return await FailAsync(cluster, addressesGuard.Error!, "portalloc", ct);
        var allReady = await AllNodesReadyAsync(cluster, snap, addressesGuard.Value, ct);
        if (!allReady)
            return await Finish(cluster, "waiting-nodes-ready", ProcessOutcome.InProgress, ct);

        var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
        if (!started.IsSuccess)
            return Result<ProcessOutcome>.Failed(started.Error!);

        // R1: ensure тройки кредов — OLD-значения после этого существуют.
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

        // R2-rest (arch/14 §5 I): пара NEW «в полёте» — rest_pending
        // журнала: повторные тики ПРОДОЛЖАЮТ проход с той же парой (регенерация
        // обнуляла бы прогресс rolling); потери поля (битый журнал) — свежая
        // пара, повторный rolling идемпотентен.
        var work = await journal.ReadAsync(cluster, ct);
        if (!work.IsSuccess)
            return await FailAsync(cluster, work.Error!, "journal-read", ct);
        string newRestPassword;
        if (work.Value?.RestPending is { } pending)
        {
            newRestPassword = pending;
        }
        else
        {
            newRestPassword = AppSecretGenerator.Generate();
            var fixedPending = await journal.WritePhaseAsync(cluster, Op, "rotate-rest-start",
                claims.InstanceId, null, ct, restPending: newRestPassword);
            if (!fixedPending.IsSuccess)
                return Result<ProcessOutcome>.Failed(fixedPending.Error!);
            // work прочитан ДО постановки pending (первый тик окна: журнала могло
            // не быть) — перечитать: R2-блок ниже смотрит rest_pending.
            work = await journal.ReadAsync(cluster, ct);
            if (!work.IsSuccess)
                return await FailAsync(cluster, work.Error!, "journal-read", ct);
        }
        if (!addresses.IsSuccess)
            return await FailAsync(cluster, addresses.Error!, "portalloc", ct);

        foreach (var shard in snap.Shards.Where(s => s.Dsn is not null))
        {
            var master = await ResolveMasterAsync(cluster, shard, addresses.Value, ct);
            if (master is null)
                return await FailAsync(cluster,
                    new ApplicationException(
                        $"shard {shard.Name}: master is unreachable (master key/Patroni REST) — retry on the next tick"),
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

            // backup_exec (arch/19 §7): роли может не быть (подсистема
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

        // R2-rest (arch/14 §5 I): NEW-пара применяется доставкой каналом БЕЗ
        // пересоздания: перерендер bootstrap-ключа ОДНОЙ ноды с pending-парой
        // + команда применения (≤1 нода/тик; реплики вперёд, лидер последним;
        // restore/TO_REMOVE-шарды и усыновлённые ноды гейт пропускает — их
        // ноды создадутся с NEW после R3). Прогресс — немутирующая проба
        // IsOnRevisionAsync(pending): content хранимого ключа == рендер под
        // NEW + ready + applied_revision == mod_revision (сверка по NEW, а не
        // по текущему/OLD ключу — нода со стационарным applied==mod(OLD)
        // пробу НЕ проходит и получает доставку NEW).
        var restoring = await ReadRestoringShardsAsync(cluster, ct);
        if (!restoring.IsSuccess)
            return await FailAsync(cluster, restoring.Error!, "backups-read", ct);

        if (work.Value?.RestPending is not null)
        {
            foreach (var shard in snap.Shards.Where(s => s.Dsn is not null && !s.ToRemove))
            {
                if (restoring.Value.Contains(shard.Name))
                    continue; // демонтажем владеет restore-процесс
                var scopeKvs = await RangeAsync($"/service/{cluster}-{shard.Name}/", ct);
                if (!scopeKvs.IsSuccess)
                    return await FailAsync(cluster, scopeKvs.Error!, $"scope/{shard.Name}", ct);
                var leader = ClusterSnapshotParser.ParseService(scopeKvs.Value)
                    .FirstOrDefault()?.LeaderName;
                var resources = await ReadShardResourcesAsync(cluster, shard.Name, ct);
                var tuning = pgtune.Create(resources);
                var ordered = shard.Nodes
                    .Where(n => n.State is not (NodeState.Quarantined or NodeState.Removing))
                    .OrderBy(n => n.Name == leader ? 1 : 0); // реплики вперёд, лидер последним
                foreach (var node in ordered)
                {
                    if (!addresses.Value.TryGetValue($"{shard.Name}/{node.Name}", out var addr)
                        || addr.Object is not null)
                        continue; // записи нет / усыновлённая (R9) — гейт пропускает
                    var topology = new ShardTopology(cluster, shard.Name, $"{cluster}-{shard.Name}",
                        addresses.Value.Where(p => p.Key.StartsWith($"{shard.Name}/", StringComparison.Ordinal))
                            .ToDictionary(p => p.Key.Split('/')[1], p => p.Value));
                    if (await bootstrap.IsOnRevisionAsync(
                            topology, node.Name, addr, tuning, pgtune.ExcludeParams,
                            snap.Config.SyncStrict, newRestPassword, ct))
                        continue; // применила NEW — следующая нода

                    // ОДНА доставка на тик (spec §3.7.4: ≤1 нода/тик, реплики
                    // вперёд, лидер последним): доставка завершает тик В
                    // ЛЮБОМ случае — синхронный apply не даёт этому же тику
                    // уйти дальше по нодам; следующая нода — следующим тиком.
                    // Finish фиксирует фазу rotate-rest-rolling/<X>/<n> в обоих
                    // исходах; исход диагностируется следующим тиком (проба
                    // IsOnRevisionAsync: применила — идём дальше, нет — повтор).
                    await bootstrap.EnsureAsync(
                        topology, node.Name, addr, secrets, tuning, pgtune.ExcludeParams,
                        snap.Config.SyncStrict, newRestPassword, ct);
                    return await Finish(cluster,
                        $"rotate-rest-rolling/{shard.Name}/{node.Name}", ProcessOutcome.Done, ct);
                }
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
            TxnCompare.ValueEqual(RestKey(cluster), creds.RestPassword),
        };
        var ops = new List<TxnOp>
        {
            new TxnOp.Put(PasswordKey(cluster), newAppPassword, null),
            new TxnOp.Put(MoverKey(cluster), newMoverPassword, null),
            new TxnOp.Put(BucketAdminPasswordKey(cluster), newBucketAdminPassword, null),
            new TxnOp.Put(BackupKey(cluster), newBackupPassword, null),
            new TxnOp.Put(RestKey(cluster), newRestPassword, null),
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
                    "credentials/dsn changed since they were read (external write?) — retry on the next tick"),
                "commit-conflict", ct, dropRestPending: true);

        // R4: снапшот P12 (точка изменения, best-effort делегат) + journal done.
        if (snapshot is not null)
        {
            var shot = await snapshot(ct);
            if (!shot.IsSuccess)
                return await FailAsync(cluster, shot.Error!, "snapshot", ct);
        }

        return await Finish(cluster, "done", ProcessOutcome.Done, ct, dropRestPending: true);
    }

    // Замена password= в conninfo dsn-ключа (пароль bucket_admin внутри dsn).
    // Пароли AppSecretGenerator — [A-Za-z0-9], экранирования в conninfo не требуют.
    [GeneratedRegex(@"(^| )password=[^ ]*", RegexOptions.CultureInvariant)]
    private static partial Regex PasswordRegex();

    private static string TicketKey(string cluster) => $"/pgworker/rotations/{cluster}";

    // Гвард «тихого окна» (arch/14 §5 I): все канонические ноды кластера
    // должны иметь статусную машину ready ДО мутаций ротации (R2 ALTER app
    // меняет pg_authid — Initializing-нода с etcd-кредами теряет вход через
    // doorman навсегда, пока R3 не закроет окно). Один range статусных ключей
    // кластера; гварды надзора повторены: QUARANTINED/REMOVING, усыновлённые
    // (object), ноды без записи portalloc — вне проверки.
    private async Task<bool> AllNodesReadyAsync(
        string cluster, ClusterSnapshot snap, IReadOnlyDictionary<string, NodeAddress> addresses, CancellationToken ct)
    {
        var range = await RangeAsync($"/pgworker/nodes/{cluster}/", ct);
        if (!range.IsSuccess)
            return false; // etcd-сбой — не мутим
        var statuses = new Dictionary<string, PgNodeStatusFact>();
        foreach (var kv in range.Value)
        {
            var segments = kv.Key.Split('/');
            if (segments.Length != 7 || segments[6] != "status")
                continue;
            var fact = PgNodeStatusFact.Parse(kv.Value);
            if (fact is not null)
                statuses[$"{segments[4]}/{segments[5]}"] = fact;
        }

        foreach (var shard in snap.Shards.Where(s => s.Dsn is not null && !s.ToRemove))
        {
            foreach (var node in shard.Nodes)
            {
                if (node.State is NodeState.Quarantined or NodeState.Removing)
                    continue;
                if (!addresses.TryGetValue($"{shard.Name}/{node.Name}", out var addr)
                    || addr.Object is not null)
                    continue;
                if (!statuses.TryGetValue($"{shard.Name}/{node.Name}", out var status)
                    || status.State != "ready")
                    return false; // нода ещё стартует/не готова — окно не открываем
            }
        }

        return true;
    }

    private static string PasswordKey(string cluster) => $"/clusters/{cluster}/app_password";

    private static string MoverKey(string cluster) => $"/clusters/{cluster}/mover_password";

    private static string BucketAdminPasswordKey(string cluster) => $"/clusters/{cluster}/bucket_admin_password";

    private static string BackupKey(string cluster) => $"/clusters/{cluster}/backup_password";

    private static string RestKey(string cluster) => $"/clusters/{cluster}/rest_password";

    private static string DsnKey(string cluster, string shard) => $"/clusters/{cluster}/shards/{shard}/dsn";

    // restore-шарды кластера (PLANNED/RUNNING/REJOINING) — те же факты, что
    // гварды надзора (§3.4): демонтажем владеет restore-процесс, окно
    // REST-ротации такие шарды не удержывают (ноды создадутся с новой парой).
    private async Task<Result<HashSet<string>>> ReadRestoringShardsAsync(string cluster, CancellationToken ct)
    {
        var range = await RangeAsync("/pgworker/backups/", ct);
        if (!range.IsSuccess)
            return Result<HashSet<string>>.Failed(range.Error!);
        var parsed = BackupsParser.Parse(range.Value, out _);
        if (!parsed.IsSuccess)
            return Result<HashSet<string>>.Failed(parsed.Error!);
        var restoring = new HashSet<string>();
        if (parsed.Value.FirstOrDefault(b => b.Cluster == cluster) is { } mine)
            foreach (var (shard, status) in mine.Shards)
                if (status.Restores.Any(r => r.State
                        is RestoreStatus.Planned or RestoreStatus.Running or RestoreStatus.Rejoining))
                    restoring.Add(shard);
        return Result<HashSet<string>>.Success(restoring);
    }

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
    // ХОСТУ недопустимо: вырожденное значение host:0 недискриминантно (S4;
    // в штатном режиме невозможно — пулер обязателен, guard ниже — защита),
    // и все ноды одного хоста равнозначны — резолв вернул бы произвольную
    // (первую/случайный порядок portalloc) ноду, ALTER ROLE на реплике падает
    // 25006 (read-only) — ротация зацикливалась в фазе started (факт стенда).
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
        string cluster, string phase, ProcessOutcome outcome, CancellationToken ct,
        bool dropRestPending = false)
    {
        var written = await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, null, ct,
            dropRestPending: dropRestPending);
        return written.IsSuccess
            ? Result<ProcessOutcome>.Success(outcome)
            : Result<ProcessOutcome>.Failed(written.Error!);
    }

    private async Task<Result<ProcessOutcome>> FailAsync(
        string cluster, Exception error, string phase, CancellationToken ct, bool dropRestPending = false)
    {
        await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, error.Message, ct,
            dropRestPending: dropRestPending);
        return Result<ProcessOutcome>.Failed(error);
    }

    // Failover-обёртки: первый успешный endpoint выигрывает (образец AddShardProcess).
    private async Task<Result<Kv?>> GetAsync(string key, CancellationToken ct)
        => await WithFailoverAsync(endpoint => etcd.GetAsync(endpoint, key, ct));

    private async Task<Result<IReadOnlyList<Kv>>> RangeAsync(string prefix, CancellationToken ct)
        => await WithFailoverAsync(endpoint => etcd.RangeAsync(endpoint, prefix, ct));

    private async Task<Result> DeleteAsync(string key, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.DeleteAsync(endpoint, key, prefix: false, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints are not set");
    }

    private async Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
        => await WithFailoverAsync(endpoint => etcd.TxnAsync(endpoint, req, ct));

    private async Task<Result<T>> WithFailoverAsync<T>(Func<string, Task<Result<T>>> call)
    {
        Result<T> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints are not set");
    }

    // Лимиты шарда от заявки request_* (копия NodeSupervisor.ReadShardResourcesAsync
    // — у ротатора своей не было; прецедент упрощённой копии RestoreProcess).
    private async Task<NodeResources?> ReadShardResourcesAsync(
        string cluster, string shard, CancellationToken ct)
    {
        var scope = $"{cluster}-{shard}";
        var cpu = await GetAsync($"/service/{scope}/request_cpu", ct);
        if (!cpu.IsSuccess)
            return null;
        var mem = await GetAsync($"/service/{scope}/request_mem", ct);
        return mem.IsSuccess ? NodeResourcesParser.Parse(cpu.Value?.Value, mem.Value?.Value) : null;
    }
}
