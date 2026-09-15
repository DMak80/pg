using System.Collections.Concurrent;
using System.Text.Json;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Templates;
using KafkaWorker.Docker.Drivers;
using Shared.Etcd.Client;
using KafkaWorker.Provisioning.Kafka;

namespace KafkaWorker.Provisioning.Processes;

/// <summary>
/// CaRotator (arch/16 §5 K, t07): ротация per-cluster CA и серверных сертов по
/// заявке /kafkaworker/ca_rotations/&lt;C&gt; — окно двойного доверия без остановки
/// записи. Фазы: P (staging ca_next_* put-if-absent — в etcd, переживает рестарт
/// воркера) → D (ca_pem = bundle OLD+NEW — клиенты начинают доверять NEW до
/// замены сертов) → R (rolling-пересоздание брокеров по одному: серт подписан
/// NEW CA, truststore = bundle, том жив) → C (атомарный txn: ca_pem/ca_key ←
/// NEW, del staging, del заявки — OLD-ключ уничтожается перезаписью) → снапшот
/// P12 + done. Guard'ы — образец H (§5): клэйм, waiting-cluster, waiting при
/// живой пароль-ротации/reassignment (rolling-ы не смешиваются). Вызывается
/// только держателем клэйма &lt;C&gt;.
/// </summary>
public sealed class CaRotator(
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
    private const string Op = "rotate-ca";
    private const string PhaseCommitted = "committed"; // C прошла, финал не завершён

    // Rolling-трек фазы (cluster, phase) → пересозданные брокеры: тик не повторяет
    // уже пересозданное; рестарт процесса теряет трек — rolling безопасно
    // начинается заново (env от NEW CA идемпотентен).
    private readonly ConcurrentDictionary<(string Cluster, string Phase), HashSet<string>> _rolled = new();

    public async Task<Result> RunAsync(KafkaClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        if (!claims.IsMine(cluster))
            return Result.Failed(new ApplicationException(
                $"rotate-ca {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        var ticket = await GetAsync(TicketKey(cluster), ct);
        if (!ticket.IsSuccess)
            return Result.Failed(ticket.Error!);

        var journalState = await journal.ReadAsync(cluster, ct);
        if (!journalState.IsSuccess)
            return Result.Failed(journalState.Error!);

        // Хвост после C (заявка снята, done не записан — сбой между C и финалом):
        // идемпотентное завершение — снапшот + done, staging/rolling не трогаем.
        var afterCommit = journalState.Value is { Op: Op } j && j.Phase == PhaseCommitted;
        if (afterCommit && ticket.Value is null)
            return await FinishAsync(cluster, ct);

        if (ticket.Value is null)
            return Result.Success(); // заявки нет, хвостов нет — no-op

        // Кластер не поднят / не канонический: ждём (заявка жива — ротация не теряется).
        if (snap.Endpoints is null || snap.AppPassword is null || snap.AdminPassword is null
            || snap.CaPem is null || snap.CaKey is null)
        {
            return await WaitAsync(cluster, "waiting-cluster", ct);
        }

        // Guard: живая пароль-ротация (H) — rolling-ы не смешиваются (журнал один).
        var passwordTicket = await GetAsync($"/kafkaworker/rotations/{cluster}", ct);
        if (!passwordTicket.IsSuccess)
            return Result.Failed(passwordTicket.Error!);
        if (passwordTicket.Value is not null
            || journalState.Value is { Op: "rotate" } r && r.Phase != "done")
        {
            return await WaitAsync(cluster, "waiting-password-rotation", ct);
        }

        // Guard: живой reassignment (I) — rolling не смешивается с переносом реплик.
        var reassignment = await GetAsync($"/kafkaworker/reassignments/{cluster}", ct);
        if (!reassignment.IsSuccess)
            return Result.Failed(reassignment.Error!);
        if (reassignment.Value is not null)
            return await WaitAsync(cluster, "waiting-reassignment", ct);

        // Живые брокеры ротации (TO_REMOVE/REMOVING исключены — их разбирает G).
        var brokers = snap.Brokers
            .Where(b => b.State is not "TO_REMOVE" and not "REMOVING")
            .OrderBy(b => b.Name, StringComparer.Ordinal)
            .ToList();

        // Преф-чек: кластер отвечает DescribeCluster до rolling (ротация
        // недоступного кластера бессмысленна — ждём, брокеры не трогаем).
        var alive = await WaitForBrokersAsync(snap, 1, ct);
        if (!alive.IsSuccess)
            return Result.Failed(alive.Error!);
        if (!alive.Value)
            return await WaitAsync(cluster, "waiting-cluster", ct);

        // Фаза P: staging НОВОЙ CA — одна на жизнь ротации (в etcd, а не в памяти —
        // переживает рестарт воркера, в отличие от NEW-паролей H).
        var staging = await EnsureStagingAsync(cluster, ct);
        if (!staging.IsSuccess)
            return Result.Failed(staging.Error!);
        var (nextKey, nextPem) = staging.Value;

        // Фаза D: bundle OLD+NEW в точке дискавери ДО замены сертов — клиенты,
        // перечитавшие ca_pem, доверяют сертам обоих поколений. Идемпотентно:
        // bundle уже содержит nextPem — put пропускается.
        var caPem = snap.CaPem;
        if (!caPem.Contains(nextPem))
        {
            var markedD = await journal.WritePhaseAsync(cluster, Op, "phase-d", claims.InstanceId, null, ct);
            if (!markedD.IsSuccess)
                return Result.Failed(markedD.Error!);
            var bundlePut = await TxnAsync(TxnRequest.Of(
                [TxnCompare.ValueEqual($"/kafka/clusters/{cluster}/ca_pem", caPem)],
                [new TxnOp.Put($"/kafka/clusters/{cluster}/ca_pem", caPem + "\n" + nextPem, null)]), ct);
            if (!bundlePut.IsSuccess)
                return Result.Failed(bundlePut.Error!);
            if (!bundlePut.Value.Succeeded)
                return Result.Failed(new ApplicationException(
                    "ca_pem изменился с момента чтения (внешняя запись?) — ретрай тиком"));
        }

        // Фаза R: rolling по одному брокеру за тик; env — серт от NEW CA (кеш R3
        // по хешу CA-ключа), truststore — bundle. Ожидание сходимости после
        // КАЖДОГО брокера (arch/16 §2.3); трек не повторяет пересозданное.
        var bundle = caPem.Contains(nextPem) ? caPem : caPem + "\n" + nextPem;
        var markedR = await journal.WritePhaseAsync(cluster, Op, "phase-r", claims.InstanceId, null, ct);
        if (!markedR.IsSuccess)
            return Result.Failed(markedR.Error!);
        var rolled = await RollingRecreateAsync(snap, brokers, nextPem, nextKey, bundle, ct);
        if (!rolled.IsSuccess)
            return Result.Failed(rolled.Error!);
        if (!rolled.Value)
            return Result.Success(); // кластер ещё сходится — следующий тик продолжит R

        // Фаза C: атомарный коммит — NEW в канон, staging долой, заявка снята;
        // compare по staging-ключу закрывает гонку параллельной ротации.
        var markedC = await journal.WritePhaseAsync(cluster, Op, PhaseCommitted, claims.InstanceId, null, ct);
        if (!markedC.IsSuccess)
            return Result.Failed(markedC.Error!);
        var commit = await TxnAsync(TxnRequest.Of(
            [TxnCompare.ValueEqual(NextKeyKey(cluster), nextKey)],
            [
                new TxnOp.Put($"/kafka/clusters/{cluster}/ca_pem", nextPem, null),
                new TxnOp.Put($"/kafka/clusters/{cluster}/ca_key", nextKey, null),
                new TxnOp.Delete(NextPemKey(cluster), Prefix: false),
                new TxnOp.Delete(NextKeyKey(cluster), Prefix: false),
                new TxnOp.Delete(TicketKey(cluster), Prefix: false),
            ]), ct);
        if (!commit.IsSuccess)
            return Result.Failed(commit.Error!);
        if (!commit.Value.Succeeded)
            return Result.Failed(new ApplicationException(
                "ca_next_key изменился с момента чтения (параллельная ротация?) — ретрай тиком"));

        return await FinishAsync(cluster, ct);
    }

    // Финал: снапшот P12 «после» + journal done + очистка треков (идемпотентно).
    private async Task<Result> FinishAsync(string cluster, CancellationToken ct)
    {
        if (snapshot is not null)
        {
            var after = await snapshot(ct);
            if (!after.IsSuccess)
                return Result.Failed(after.Error!);
        }

        _rolled.TryRemove((cluster, "phase-r"), out _);
        var done = await journal.WritePhaseAsync(cluster, Op, "done", claims.InstanceId, null, ct);
        return done.IsSuccess ? Result.Success() : Result.Failed(done.Error!);
    }

    // Ждущий исход: journal-запись + успех тика (заявка жива — продолжим позже).
    private async Task<Result> WaitAsync(string cluster, string phase, CancellationToken ct)
    {
        var waiting = await journal.WritePhaseAsync(cluster, Op, phase, claims.InstanceId, null, ct);
        return waiting.IsSuccess ? Result.Success() : Result.Failed(waiting.Error!);
    }

    // Фаза P: чтение staging; отсутствующий — генерация + txn put-if-absent,
    // проигрыш compare (гонка) разрешается re-read — чужая staging валидна.
    private async Task<Result<(string Key, string Pem)>> EnsureStagingAsync(
        string cluster, CancellationToken ct)
    {
        var key = await GetAsync(NextKeyKey(cluster), ct);
        if (!key.IsSuccess)
            return Result<(string, string)>.Failed(key.Error!);
        var pem = await GetAsync(NextPemKey(cluster), ct);
        if (!pem.IsSuccess)
            return Result<(string, string)>.Failed(pem.Error!);
        if (key.Value is { } existingKey && pem.Value is { } existingPem)
            return Result<(string, string)>.Success((existingKey.Value, existingPem.Value));

        var generated = ClusterPki.GenerateCa(cluster);
        var txn = await TxnAsync(TxnRequest.Of(
            [TxnCompare.NotExists(NextKeyKey(cluster)), TxnCompare.NotExists(NextPemKey(cluster))],
            [
                new TxnOp.Put(NextKeyKey(cluster), generated.CaKeyPem, null),
                new TxnOp.Put(NextPemKey(cluster), generated.CaPem, null),
            ]), ct);
        if (!txn.IsSuccess)
            return Result<(string, string)>.Failed(txn.Error!);

        var finalKey = await GetAsync(NextKeyKey(cluster), ct);
        if (!finalKey.IsSuccess || finalKey.Value is null)
            return Result<(string, string)>.Failed(finalKey.Error
                ?? new ApplicationException($"staging {cluster}: ca_next_key не читается после txn"));
        var finalPem = await GetAsync(NextPemKey(cluster), ct);
        if (!finalPem.IsSuccess || finalPem.Value is null)
            return Result<(string, string)>.Failed(finalPem.Error
                ?? new ApplicationException($"staging {cluster}: ca_next_pem не читается после txn"));
        return Result<(string, string)>.Success((finalKey.Value!.Value, finalPem.Value!.Value));
    }

    // Rolling-пересоздание: RemoveNode(том жив) → EnsureNode с env от NEW CA;
    // ожидание сходимости после каждого брокера; true — все брокеры на NEW.
    private async Task<Result<bool>> RollingRecreateAsync(
        KafkaClusterSnapshot snap,
        IReadOnlyList<KafkaBrokerDecl> brokers,
        string nextPem,
        string nextKey,
        string bundle,
        CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var rolled = _rolled.GetOrAdd((cluster, "phase-r"), _ => []);

        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return Result<bool>.Failed(addresses.Error!);

        foreach (var broker in brokers)
        {
            if (rolled.Contains(broker.Name))
                continue; // пересоздан ранее (текущий тик/предыдущий тик)

            if (!addresses.Value.TryGetValue(broker.Name, out var addr))
                return Result<bool>.Failed(new ApplicationException(
                    $"rotate-ca {cluster}: broker {broker.Name} не закреплён в portalloc"));

            var removed = await driver.RemoveNodeAsync(cluster, broker.Name, removeVolume: false, ct);
            if (!removed.IsSuccess)
                return Result<bool>.Failed(removed.Error!);

            var env = BrokerEnvBuilder.Build(
                snap, broker.Name, addr,
                [snap.AppPassword!], [snap.AdminPassword!],
                options, certificates,
                signingCaPem: nextPem, signingCaKey: nextKey, trustCaPem: bundle);
            var spec = new KafkaNodeSpec(
                cluster, broker.Name, addr.Host, addr.ClientPort, options.NodeImage, env,
                broker.Resources?.Cpu,
                broker.Resources is null ? null : broker.Resources.MemGi * 1024L * 1024 * 1024);
            var ensured = await driver.EnsureNodeAsync(spec, ct);
            if (!ensured.IsSuccess)
                return Result<bool>.Failed(ensured.Error!);

            var mark = await journal.WritePhaseAsync(
                cluster, Op, $"phase-r/{broker.Name}", claims.InstanceId, null, ct);
            if (!mark.IsSuccess)
                return Result<bool>.Failed(mark.Error!);
            rolled.Add(broker.Name);

            // Ожидание сходимости после каждого брокера (arch/16 §2.3):
            // следующий брокер пересоздаётся только на сошедшемся кластере.
            var ready = await WaitForBrokersAsync(snap, brokers.Count, ct);
            if (!ready.IsSuccess)
                return Result<bool>.Failed(ready.Error!);
            if (!ready.Value)
                return Result<bool>.Success(false);
        }

        return Result<bool>.Success(true);
    }

    // DescribeCluster: состав кластера = числу живых брокеров ротации
    // (доверие — snap.CaPem: в фазе R это уже bundle OLD+NEW).
    private async Task<Result<bool>> WaitForBrokersAsync(
        KafkaClusterSnapshot snap, int expected, CancellationToken ct)
    {
        await using var admin = adminFactory.Create(
            snap.Endpoints!, snap.AdminUser ?? "admin", snap.AdminPassword!, snap.CaPem);
        var view = await admin.DescribeClusterAsync(ct);
        return Result<bool>.Success(view.IsSuccess && view.Value.Brokers.Count >= expected);
    }

    private async Task<Result<IReadOnlyDictionary<string, NodeAddress>>> ReadPortAllocAsync(
        string cluster, CancellationToken ct)
    {
        var result = await GetAsync($"/kafkaworker/portalloc/{cluster}", ct);
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

    private static string TicketKey(string cluster) => $"/kafkaworker/ca_rotations/{cluster}";

    private static string NextKeyKey(string cluster) => $"/kafka/clusters/{cluster}/ca_next_key";

    private static string NextPemKey(string cluster) => $"/kafka/clusters/{cluster}/ca_next_pem";

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
