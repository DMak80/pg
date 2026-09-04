using System.Collections.Concurrent;
using System.Text.Json;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Planning;
using KafkaWorker.Core.Templates;
using KafkaWorker.Docker.Drivers;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Provisioning.Kafka;

namespace KafkaWorker.Provisioning.Processes;

/// <summary>
/// PasswordRotator (arch/16 §5 H, t03): исполнение заявок ротации паролей
/// ролей app (/kafkaworker/rotations/&lt;C&gt;) и admin (/kafkaworker/admin_rotations/&lt;C&gt;).
/// Обрабатывает не более ОДНОЙ заявки за тик: app — раньше admin (детерминированный
/// порядок). Фазы без окна недоступности: A) rolling-пересоздание брокеров с JAAS
/// из ДВУХ кредов (OLD+NEW — все клиенты работают со OLD); B) ОДНА txn
/// [compare value(пароль)==OLD][put NEW; del заявку] — клиенты перечитывают etcd;
/// C) rolling с JAAS только NEW (снятие OLD). Окно ротации — ТОЛЬКО ротируемой
/// роли, вторая роль несёт текущий пароль. Отказ между фазами безопасен (оба
/// креда валидны; повтор продолжает: заявку — с начала A идемпотентно, после
/// B — по journal-фазе). Снапшоты P12 «до/после». Вызывается только держателем
/// клэйма &lt;C&gt;.
/// </summary>
public sealed class PasswordRotator(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    IKafkaAdminClientFactory adminFactory,
    ProvisioningOptions options,
    BrokerCertificateCache certificates,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Op = "rotate";
    private const string PhaseCommitted = "rotated-commit"; // B прошла, C не завершена
    private const string PhaseDone = "done";

    // Роли ротации (spec §5.2): заявка/пароль/JAAS-пользователь на роль; app —
    // раньше admin (детерминированный порядок, одна заявка за тик).
    private sealed record RotationRole(string Name, string TicketKeyPrefix, string PasswordKey, string User)
    {
        public static readonly RotationRole App = new("app", "rotations", "app_password", "app");
        public static readonly RotationRole Admin = new("admin", "admin_rotations", "admin_password", "admin");
        public static readonly IReadOnlyList<RotationRole> Order = [App, Admin];

        // Фаза journal: app — legacy-имена (совместимость журналов), admin —
        // с префиксом (фазы ролей не смешиваются в одном журнале кластера).
        public string Phase(string phase) => Name == App.Name ? phase : $"{Name}:{phase}";
    }

    // Rolling-трек фазы (cluster, role, phase) → пересозданные брокеры: тик не
    // повторяет уже пересозданное; рестарт процесса теряет трек — rolling
    // безопасно начинается заново (идемпотентен по построению).
    private readonly ConcurrentDictionary<(string Cluster, string Role, string Phase), HashSet<string>> _rolled = new();

    // Снапшот «до» уже сделан для этой ротации (старт A; повторные тики A — нет).
    private readonly ConcurrentDictionary<(string Cluster, string Role), string> _snapshotBeforeDone = new();

    public async Task<Result> RunAsync(KafkaClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        if (!claims.IsMine(cluster))
            return Result.Failed(new ApplicationException(
                $"rotate {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // По одной заявке за тик: заявка роли обрабатывалась (или роль доводила
        // свой журнал) — вторая роль ждёт следующего тика.
        foreach (var role in RotationRole.Order)
        {
            var outcome = await RunRoleAsync(snap, role, ct);
            if (!outcome.IsSuccess)
                return outcome;
            if (outcome.Value)
                break;
        }

        return Result.Success();
    }

    // Алгоритм A/B/C одной роли; true — роль обрабатывалась в этом тике.
    private async Task<Result<bool>> RunRoleAsync(
        KafkaClusterSnapshot snap, RotationRole role, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var ticket = await GetAsync(TicketKey(role, cluster), ct);
        if (!ticket.IsSuccess)
            return Result<bool>.Failed(ticket.Error!);

        var journalState = await journal.ReadAsync(cluster, ct);
        if (!journalState.IsSuccess)
            return Result<bool>.Failed(journalState.Error!);
        // Признак «B прошла, C не завершена»: journal-фаза коммита либо оборванная
        // phase-c (Fail между B и C перезаписывает фазу с last_error).
        var afterCommit = journalState.Value is { Op: Op }
            and { Phase: var phase }
            && (phase == role.Phase(PhaseCommitted) || phase == role.Phase("phase-c"));

        if (ticket.Value is null && !afterCommit)
            return Result<bool>.Success(false); // заявки нет, фазы C не висит — no-op

        if (snap.Endpoints is null || snap.AppPassword is null || snap.AdminPassword is null)
        {
            // Кластер не поднят: ждём (заявка жива — ротация не теряется).
            if (ticket.Value is null)
                return Result<bool>.Success(false);
            var waiting = await journal.WriteAsync(
                cluster, Op, role.Phase("waiting-cluster"), claims.InstanceId, null, ct);
            return waiting.IsSuccess ? Result<bool>.Success(true) : Result<bool>.Failed(waiting.Error!);
        }

        // Живые брокеры ротации (TO_REMOVE/REMOVING исключены — их разбирает G).
        var brokers = snap.Brokers
            .Where(b => b.State is not "TO_REMOVE" and not "REMOVING")
            .OrderBy(b => b.Name, StringComparer.Ordinal)
            .ToList();

        // Преф-чек: кластер должен отвечать DescribeCluster ДО rolling (ротация
        // недоступного кластера бессмысленна — ждём, брокеры не трогаем).
        var alive = await WaitForBrokersAsync(snap, 1, ct);
        if (!alive.Value)
        {
            var waitingCluster = await journal.WriteAsync(
                cluster, Op, role.Phase("waiting-cluster"), claims.InstanceId, null, ct);
            return waitingCluster.IsSuccess ? Result<bool>.Success(true) : Result<bool>.Failed(waitingCluster.Error!);
        }

        if (ticket.Value is not null)
        {
            // Снапшот P12 «до» (старт ротации — точка изменения).
            if (_snapshotBeforeDone.TryAdd((cluster, role.Name), "started") && snapshot is not null)
            {
                var before = await snapshot(ct);
                if (!before.IsSuccess)
                    return Result<bool>.Failed(Fail(cluster, before.Error!, role.Phase("phase-a")));
            }

            var started = await journal.WriteAsync(cluster, Op, role.Phase("phase-a"), claims.InstanceId, null, ct);
            if (!started.IsSuccess)
                return Result<bool>.Failed(started.Error!);

            // Фаза A: rolling с JAAS OLD+NEW (том сохраняется — данные и метаданные).
            var oldPassword = role == RotationRole.Admin ? snap.AdminPassword! : snap.AppPassword!;
            var newPassword = KafkaPasswordGenerator.Generate();

            // Новая генерация = новая попытка фазы A: трек прошлой попытки
            // недействителен (брокеры с [OLD, NEW_прошлый] обязаны пересоздаться
            // с [OLD, NEW_текущий]) — иначе B закоммитит пароль, которого нет
            // на части брокеров: SASL-отказ NEW-клиентам до конца C (окно
            // недоступности, невозможное по построению — spec §4.2 H).
            _rolled.TryRemove((cluster, role.Name, "phase-a"), out _);

            var rolledA = await RollingRecreateAsync(snap, brokers, role, [oldPassword, newPassword], "phase-a", ct);
            if (!rolledA.IsSuccess)
                return Result<bool>.Failed(Fail(cluster, rolledA.Error!, role.Phase("phase-a")));

            var readyA = await WaitForBrokersAsync(snap, brokers.Count, ct);
            if (!readyA.IsSuccess)
                return Result<bool>.Failed(Fail(cluster, readyA.Error!, role.Phase("phase-a")));
            if (!readyA.Value)
                return Result<bool>.Success(true); // кластер ещё сходится — следующий тик продолжит A

            // Фаза B: ОДНА txn — атомарная замена пароля + снятие заявки.
            var committed = await CommitPasswordAsync(role, cluster, oldPassword, newPassword, ct);
            if (!committed.IsSuccess)
                return Result<bool>.Failed(Fail(cluster, committed.Error!, role.Phase("phase-b")));

            _ = afterCommit; // фаза C — ниже (тот же вызов или следующий тик по journal)
        }

        // Фаза C: rolling с JAAS только текущего пароля из etcd (после B = NEW).
        var current = await GetAsync(PasswordKey(role, cluster), ct);
        if (!current.IsSuccess || current.Value is null)
            return Result<bool>.Failed(Fail(cluster,
                current.Error ?? new ApplicationException($"нет ключа {PasswordKey(role, cluster)}"),
                role.Phase("phase-c")));

        var markedC = await journal.WriteAsync(cluster, Op, role.Phase(PhaseCommitted), claims.InstanceId, null, ct);
        if (!markedC.IsSuccess)
            return Result<bool>.Failed(markedC.Error!);

        var rolledC = await RollingRecreateAsync(snap, brokers, role, [current.Value.Value], "phase-c", ct);
        if (!rolledC.IsSuccess)
            return Result<bool>.Failed(Fail(cluster, rolledC.Error!, role.Phase("phase-c")));

        var readyC = await WaitForBrokersAsync(snap, brokers.Count, ct);
        if (!readyC.IsSuccess)
            return Result<bool>.Failed(Fail(cluster, readyC.Error!, role.Phase("phase-c")));
        if (!readyC.Value)
            return Result<bool>.Success(true); // следующий тик продолжит C по journal-фазе

        // Финал: снапшот P12 «после» + journal done + очистка треков.
        if (snapshot is not null)
        {
            var after = await snapshot(ct);
            if (!after.IsSuccess)
                return Result<bool>.Failed(Fail(cluster, after.Error!, role.Phase("snapshot-after")));
        }

        _rolled.TryRemove((cluster, role.Name, "phase-a"), out _);
        _rolled.TryRemove((cluster, role.Name, "phase-c"), out _);
        _snapshotBeforeDone.TryRemove((cluster, role.Name), out _);
        var done = await journal.WriteAsync(cluster, Op, role.Phase(PhaseDone), claims.InstanceId, null, ct);
        return Result<bool>.Success(done.IsSuccess);
    }

    // Rolling-пересоздание: RemoveNode(том жив) → EnsureNode; окно ротации —
    // ТОЛЬКО у ротируемой роли (вторая несёт текущий пароль из снапшота);
    // уже пересозданные в этой фазе — пропуск (трек).
    private async Task<Result> RollingRecreateAsync(
        KafkaClusterSnapshot snap,
        IReadOnlyList<KafkaBrokerDecl> brokers,
        RotationRole role,
        IReadOnlyList<string> passwords,
        string phase,
        CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var rolled = _rolled.GetOrAdd((cluster, role.Name, phase), _ => []);

        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return addresses.Error!;

        foreach (var broker in brokers)
        {
            if (rolled.Contains(broker.Name))
                continue; // пересоздан в этой фазе ранее (текущий тик/предыдущий тик)

            if (!addresses.Value.TryGetValue(broker.Name, out var addr))
                return Result.Failed(new ApplicationException(
                    $"rotate {cluster}: broker {broker.Name} не закреплён в portalloc"));

            var removed = await driver.RemoveNodeAsync(cluster, broker.Name, removeVolume: false, ct);
            if (!removed.IsSuccess)
                return removed;

            var env = BrokerEnvBuilder.Build(
                snap, broker.Name, addr,
                role == RotationRole.App ? passwords : [snap.AppPassword!],
                role == RotationRole.Admin ? passwords : [snap.AdminPassword!],
                options, certificates);
            var spec = new KafkaNodeSpec(
                cluster, broker.Name, addr.Host, addr.ClientPort, options.NodeImage, env,
                broker.Resources?.Cpu,
                broker.Resources is null ? null : broker.Resources.MemGi * 1024L * 1024 * 1024);
            var ensured = await driver.EnsureNodeAsync(spec, ct);
            if (!ensured.IsSuccess)
                return ensured;

            rolled.Add(broker.Name);
        }

        return Result.Success();
    }

    // DescribeCluster: состав кластера = числу живых брокеров ротации.
    private async Task<Result<bool>> WaitForBrokersAsync(
        KafkaClusterSnapshot snap, int expected, CancellationToken ct)
    {
        await using var admin = adminFactory.Create(snap.Endpoints!, snap.AdminUser ?? "admin", snap.AdminPassword!, snap.CaPem);
        var view = await admin.DescribeClusterAsync(ct);
        return Result<bool>.Success(view.IsSuccess && view.Value.Brokers.Count >= expected);
    }

    // Фаза B: txn [compare value(пароль)==OLD][put NEW][del заявку]. Compare не
    // сошёлся → пароль уже другой (гонка/повтор) — заявка снимается etcd-фактом,
    // повтор тика увидит актуальный пароль в фазе C.
    private async Task<Result> CommitPasswordAsync(
        RotationRole role, string cluster, string oldPassword, string newPassword, CancellationToken ct)
    {
        var txn = await TxnAsync(
            TxnRequest.Of(
                [TxnCompare.ValueEqual(PasswordKey(role, cluster), oldPassword)],
                [
                    new TxnOp.Put(PasswordKey(role, cluster), newPassword, null),
                    new TxnOp.Delete(TicketKey(role, cluster), false),
                ]),
            ct);
        if (!txn.IsSuccess)
            return txn;
        if (!txn.Value.Succeeded)
            return Result.Failed(new ApplicationException(
                $"{role.PasswordKey} {cluster} изменился с момента чтения (compare value не сошёлся)"));

        return Result.Success();
    }

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await GetAsync(PortAllocKey(cluster), ct);
        if (!result.IsSuccess)
            return Result<IReadOnlyDictionary<string, NodeAddress>>.Failed(result.Error!);
        var addresses = new Dictionary<string, NodeAddress>();
        if (result.Value is { } kv)
        {
            using var doc = JsonDocument.Parse(kv.Value);
            foreach (var node in doc.RootElement.EnumerateObject())
                addresses[node.Name] = new NodeAddress(
                    node.Value.GetProperty("host").GetString()!,
                    node.Value.GetProperty("client").GetInt32());
        }

        return Result<IReadOnlyDictionary<string, NodeAddress>>.Success(addresses);
    }

    private Result Fail(string cluster, Exception error, string phase)
    {
        journal.WriteAsync(cluster, Op, phase, claims.InstanceId, error.Message, CancellationToken.None)
            .GetAwaiter().GetResult();
        return Result.Failed(error);
    }

    private static string TicketKey(RotationRole role, string cluster)
        => $"/kafkaworker/{role.TicketKeyPrefix}/{cluster}";

    private static string PasswordKey(RotationRole role, string cluster)
        => $"/kafka/clusters/{cluster}/{role.PasswordKey}";

    private static string PortAllocKey(string cluster) => $"/kafkaworker/portalloc/{cluster}";

    private async Task<Result<Kv?>> GetAsync(string key, CancellationToken ct)
    {
        Result<Kv?>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, key, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private async Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
    {
        Result<TxnResult>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.TxnAsync(endpoint, req, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
