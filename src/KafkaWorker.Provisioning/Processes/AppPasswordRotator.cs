using System.Collections.Concurrent;
using System.Text.Json;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Planning;
using KafkaWorker.Docker.Drivers;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Provisioning.Kafka;

namespace KafkaWorker.Provisioning.Processes;

/// <summary>
/// AppPasswordRotator (arch/16 §5 H): исполнение заявки /kafkaworker/rotations/&lt;C&gt;.
/// Фазы без окна недоступности: A) rolling-пересоздание брокеров с JAAS из
/// ДВУХ кредов (OLD+NEW — все клиенты работают со OLD); B) ОДНА txn
/// [compare value(app_password)==OLD][put NEW; del заявку] — клиенты
/// перечитывают etcd; C) rolling с JAAS только NEW (снятие OLD). Отказ между
/// фазами безопасен (оба креда валидны; повтор продолжает: заявку — с начала
/// A идемпотентно, после B — по journal-фазе). Снапшоты P12 «до/после».
/// Окно «часть брокеров знает только NEW» невозможно по построению: до B все
/// пересозданы с OLD+NEW, C стартует после B. Вызывается только держателем
/// клэйма &lt;C&gt;.
/// </summary>
public sealed class AppPasswordRotator(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    IKafkaAdminClientFactory adminFactory,
    ProvisioningOptions options,
    Func<CancellationToken, Task<Result>>? snapshot = null)
{
    private const string Op = "rotate";
    private const string PhaseCommitted = "rotated-commit"; // B прошла, C не завершена
    private const string PhaseDone = "done";

    // Rolling-трек фазы (cluster, phase) → пересозданные брокеры: тик не
    // повторяет уже пересозданное; рестарт процесса теряет трек — rolling
    // безопасно начинается заново (идемпотентен по построению).
    private readonly ConcurrentDictionary<(string Cluster, string Phase), HashSet<string>> _rolled = new();

    // Снапшот «до» уже сделан для этой ротации (старт A; повторные тики A — нет).
    private readonly ConcurrentDictionary<string, string> _snapshotBeforeDone = new();

    public async Task<Result> RunAsync(KafkaClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        if (!claims.IsMine(cluster))
            return Result.Failed(new ApplicationException(
                $"rotate {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        var ticket = await GetAsync(RotationKey(cluster), ct);
        if (!ticket.IsSuccess)
            return Fail(cluster, ticket.Error!, "reading-ticket");

        var journalState = await journal.ReadAsync(cluster, ct);
        if (!journalState.IsSuccess)
            return Fail(cluster, journalState.Error!, "reading-journal");
        // Признак «B прошла, C не завершена»: journal-фаза коммита либо оборванная
        // phase-c (Fail между B и C перезаписывает фазу с last_error).
        var afterCommit = journalState.Value is { Op: Op }
            and { Phase: PhaseCommitted or "phase-c" };

        if (ticket.Value is null && !afterCommit)
            return Result.Success(); // заявки нет, фазы C не висит — no-op

        if (snap.Endpoints is null || snap.AppUser is null || snap.AppPassword is null)
        {
            // Кластер не поднят: ждём (заявка жива — ротация не теряется).
            if (ticket.Value is null)
                return Result.Success();
            return await journal.WriteAsync(cluster, Op, "waiting-cluster", claims.InstanceId, null, ct);
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
            return await journal.WriteAsync(cluster, Op, "waiting-cluster", claims.InstanceId, null, ct);

        if (ticket.Value is not null)
        {
            // Снапшот P12 «до» (старт ротации — точка изменения).
            if (_snapshotBeforeDone.TryAdd(cluster, "started") && snapshot is not null)
            {
                var before = await snapshot(ct);
                if (!before.IsSuccess)
                    return Fail(cluster, before.Error!, "snapshot-before");
            }

            var started = await journal.WriteAsync(cluster, Op, "phase-a", claims.InstanceId, null, ct);
            if (!started.IsSuccess)
                return started;

            // Фаза A: rolling с JAAS OLD+NEW (том сохраняется — данные и метаданные).
            var oldPassword = snap.AppPassword;
            var newPassword = KafkaPasswordGenerator.Generate();

            // Новая генерация = новая попытка фазы A: трек прошлой попытки
            // недействителен (брокеры с [OLD, NEW_прошлый] обязаны пересоздаться
            // с [OLD, NEW_текущий]) — иначе B закоммитит пароль, которого нет
            // на части брокеров: SASL-отказ NEW-клиентам до конца C (окно
            // недоступности, невозможное по построению — spec §4.2 H).
            _rolled.TryRemove((cluster, "phase-a"), out _);

            var rolledA = await RollingRecreateAsync(snap, brokers, [oldPassword, newPassword], "phase-a", ct);
            if (!rolledA.IsSuccess)
                return Fail(cluster, rolledA.Error!, "phase-a");

            var readyA = await WaitForBrokersAsync(snap, brokers.Count, ct);
            if (!readyA.IsSuccess)
                return Fail(cluster, readyA.Error!, "phase-a");
            if (!readyA.Value)
                return Result.Success(); // кластер ещё сходится — следующий тик продолжит A

            // Фаза B: ОДНА txn — атомарная замена пароля + снятие заявки.
            var committed = await CommitPasswordAsync(cluster, oldPassword, newPassword, ct);
            if (!committed.IsSuccess)
                return Fail(cluster, committed.Error!, "phase-b");

            _ = afterCommit; // фаза C — ниже (тот же вызов или следующий тик по journal)
        }

        // Фаза C: rolling с JAAS только текущего пароля из etcd (после B = NEW).
        var current = await GetAsync(PasswordKey(cluster), ct);
        if (!current.IsSuccess || current.Value is null)
            return Fail(cluster,
                current.Error ?? new ApplicationException($"нет ключа {PasswordKey(cluster)}"),
                "phase-c");

        var markedC = await journal.WriteAsync(cluster, Op, PhaseCommitted, claims.InstanceId, null, ct);
        if (!markedC.IsSuccess)
            return markedC;

        var rolledC = await RollingRecreateAsync(snap, brokers, [current.Value.Value], "phase-c", ct);
        if (!rolledC.IsSuccess)
            return Fail(cluster, rolledC.Error!, "phase-c");

        var readyC = await WaitForBrokersAsync(snap, brokers.Count, ct);
        if (!readyC.IsSuccess)
            return Fail(cluster, readyC.Error!, "phase-c");
        if (!readyC.Value)
            return Result.Success(); // следующий тик продолжит C по journal-фазе

        // Финал: снапшот P12 «после» + journal done + очистка треков.
        if (snapshot is not null)
        {
            var after = await snapshot(ct);
            if (!after.IsSuccess)
                return Fail(cluster, after.Error!, "snapshot-after");
        }

        _rolled.TryRemove((cluster, "phase-a"), out _);
        _rolled.TryRemove((cluster, "phase-c"), out _);
        _snapshotBeforeDone.TryRemove(cluster, out _);
        return await journal.WriteAsync(cluster, Op, PhaseDone, claims.InstanceId, null, ct);
    }

    // Rolling-пересоздание: RemoveNode(том жив) → EnsureNode с данным набором
    // паролей; уже пересозданные в этой фазе — пропуск (трек).
    private async Task<Result> RollingRecreateAsync(
        KafkaClusterSnapshot snap,
        IReadOnlyList<KafkaBrokerDecl> brokers,
        IReadOnlyList<string> passwords,
        string phase,
        CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var rolled = _rolled.GetOrAdd((cluster, phase), _ => []);

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

            var env = BrokerEnvBuilder.Build(snap, broker.Name, addr, passwords, options);
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
        await using var admin = adminFactory.Create(snap.Endpoints!, snap.AppUser!, snap.AppPassword!, null); // переходно: admin+caPem — Task 8
        var view = await admin.DescribeClusterAsync(ct);
        return Result<bool>.Success(view.IsSuccess && view.Value.Brokers.Count >= expected);
    }

    // Фаза B: txn [compare value(app_password)==OLD][put NEW][del заявку].
    // Compare не сошёлся → пароль уже другой (гонка/повтор) — заявка снимается
    // etcd-фактом, повтор тика увидит актуальный пароль в фазе C.
    private async Task<Result> CommitPasswordAsync(
        string cluster, string oldPassword, string newPassword, CancellationToken ct)
    {
        var txn = await TxnAsync(
            TxnRequest.Of(
                [TxnCompare.ValueEqual(PasswordKey(cluster), oldPassword)],
                [
                    new TxnOp.Put(PasswordKey(cluster), newPassword, null),
                    new TxnOp.Delete(RotationKey(cluster), false),
                ]),
            ct);
        if (!txn.IsSuccess)
            return txn;
        if (!txn.Value.Succeeded)
            return Result.Failed(new ApplicationException(
                $"app_password {cluster} изменился с момента чтения (compare value не сошёлся)"));

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

    private static string RotationKey(string cluster) => $"/kafkaworker/rotations/{cluster}";

    private static string PasswordKey(string cluster) => $"/kafka/clusters/{cluster}/app_password";

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
