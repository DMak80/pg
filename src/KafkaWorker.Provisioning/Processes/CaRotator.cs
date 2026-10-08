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
/// P12 + done. Вызывается только держателем клэйма &lt;C&gt;.
/// <para>Структура тика (t10, зеркалит valkey K0.2→K0.5): K0.2 хвост committed →
/// доигрывание финала; K0.3 детект ОТКРЫТОГО окна (staging жив ИЛИ журнал
/// rotate-ca вне {done, waiting-*}) → доигрывание P→D→R→C БЕЗ ждущих guard'ов
/// и БЕЗ экспирации (окно не сиротеет: слепой кластер посреди R — передержка);
/// K0.4 заявки нет → no-op; K0.5 дооконные ждущие guard'ы (waiting-cluster /
/// waiting-password-rotation / waiting-reassignment) — ЭКСПИРАЦИОННЫЕ под
/// тройным гвардом §3.1 (staging-предикат истинен по построению ветки — K0.3
/// уводит открытое окно мимо, журнал — явной проверкой). Финал K4 пишет исход
/// done в ticket_outcomes/&lt;C&gt;.</para>
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

    // t10: экспирация не-начатых заявок + исходы финалов.
    private readonly TicketExpirator _tickets = new(etcd, endpoints);

    // Аудит заявки текущей ротации (для исхода done; рестарт в окне C→финал
    // теряет аудит — исход пишется с фактическим временем финала).
    private readonly ConcurrentDictionary<string, TicketRequestAudit?> _ticketAudit = new();

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

        // Аудит живой заявки — для исхода done финала K4.
        if (ticket.Value is not null)
            _ticketAudit[cluster] = TicketOutcomes.ParseAudit(ticket.Value.Value);

        // K0.2 (t10): хвост после C (заявка снята, done не записан — сбой между
        // C и финалом): идемпотентное завершение — снапшот + done, staging/rolling
        // не трогаем.
        var afterCommit = journalState.Value is { Op: Op } j && j.Phase == PhaseCommitted;
        if (afterCommit && ticket.Value is null)
            return await FinishAsync(cluster, ct);

        // K0.3 (t10, зеркалит valkey): окно открыто — staging жив ИЛИ журнал
        // rotate-ca в мутационной фазе (вне {done, waiting-*}) — доигрывание
        // P→D→R→C БЕЗ ждущих guard'ов и БЕЗ экспирации: слепой кластер посреди R —
        // передержка (rolling стоит на ожидании сходимости); потеря endpoints или
        // появление пароль-заявки при живом staging в waiting-точки НЕ уводят —
        // окно не сиротеет, заявка жива.
        var stagingKey = await GetAsync(NextKeyKey(cluster), ct);
        if (!stagingKey.IsSuccess)
            return Result.Failed(stagingKey.Error!);
        var stagingPem = await GetAsync(NextPemKey(cluster), ct);
        if (!stagingPem.IsSuccess)
            return Result.Failed(stagingPem.Error!);
        var stagingLive = stagingKey.Value is not null || stagingPem.Value is not null;
        if (stagingLive || CaMutationLive(journalState.Value))
        {
            if (snap.CaPem is null || snap.CaKey is null)
                return Result.Failed(new ApplicationException(
                    $"rotate-ca {cluster}: окно открыто, но ca_pem/ca_key отсутствуют — внешняя порча, ретрай тиком"));
            return await PhasesAsync(snap, ct);
        }

        // K0.4 (t10): заявки нет, хвостов нет — no-op.
        if (ticket.Value is null)
            return Result.Success();
        var ticketPayload = ticket.Value.Value; // живая заявка (не null — K0.4)

        // K0.5 (t10) — дооконные ждущие guard'ы (экспирационные под тройным
        // гвардом §3.1; staging-предикат истинен по построению ветки — K0.3
        // уводит открытое окно мимо; журнал — явной проверкой ниже).

        // Кластер не поднят / не канонический: ждём (заявка жива — ротация не теряется).
        if (snap.Endpoints is null || snap.AppPassword is null || snap.AdminPassword is null
            || snap.CaPem is null || snap.CaKey is null)
        {
            return await WaitAsync(cluster, "waiting-cluster", ticketPayload,
                CaMutationLive(journalState.Value), ct);
        }

        // Guard: живая пароль-ротация (H, app/admin) — rolling-ы не смешиваются
        // (журнал один). Живая admin-заявка H — та же семья (приоритет H, t10).
        var passwordTicket = await GetAsync($"/kafkaworker/rotations/{cluster}", ct);
        if (!passwordTicket.IsSuccess)
            return Result.Failed(passwordTicket.Error!);
        var adminTicket = await GetAsync($"/kafkaworker/admin_rotations/{cluster}", ct);
        if (!adminTicket.IsSuccess)
            return Result.Failed(adminTicket.Error!);
        if (passwordTicket.Value is not null || adminTicket.Value is not null
            || journalState.Value is { Op: "rotate" } r && r.Phase != "done")
        {
            return await WaitAsync(cluster, "waiting-password-rotation", ticketPayload,
                CaMutationLive(journalState.Value), ct);
        }

        // Guard: живой reassignment (I) или regen — rolling не смешивается с
        // чужими мутациями (spec §3.2 K: regen гейтит так же — фаза едина,
        // прецедент SecurityMigrator M0).
        var reassignment = await GetAsync($"/kafkaworker/reassignments/{cluster}", ct);
        if (!reassignment.IsSuccess)
            return Result.Failed(reassignment.Error!);
        if (reassignment.Value is not null)
            return await WaitAsync(cluster, "waiting-reassignment", ticketPayload,
                CaMutationLive(journalState.Value), ct);
        var regen = await GetAsync($"/kafkaworker/regens/{cluster}", ct);
        if (!regen.IsSuccess)
            return Result.Failed(regen.Error!);
        if (regen.Value is not null)
            return await WaitAsync(cluster, "waiting-reassignment", ticketPayload,
                CaMutationLive(journalState.Value), ct);

        // Преф-чек: кластер отвечает DescribeCluster до rolling (ротация
        // недоступного кластера бессмысленна — ждём, брокеры не трогаем).
        var alive = await WaitForBrokersAsync(snap, 1, ct);
        if (!alive.IsSuccess)
            return Result.Failed(alive.Error!);
        if (!alive.Value)
            return await WaitAsync(cluster, "waiting-cluster", ticket.Value.Value,
                CaMutationLive(journalState.Value), ct);

        return await PhasesAsync(snap, ct);
    }

    // Хвост фаз (P→D→R→C→финал): вызывается ТОЛЬКО из доигрывания окна (K0.3)
    // или после пройденных дооконных guard'ов (K0.5) — логика фаз без изменений.
    private async Task<Result> PhasesAsync(KafkaClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Живые брокеры ротации (TO_REMOVE/REMOVING исключены — их разбирает G).
        var brokers = snap.Brokers
            .Where(b => b.State is not "TO_REMOVE" and not "REMOVING")
            .OrderBy(b => b.Name, StringComparer.Ordinal)
            .ToList();

        // Фаза P: staging НОВОЙ CA — одна на жизнь ротации (в etcd, а не в памяти —
        // переживает рестарт воркера, в отличие от NEW-паролей H).
        var staging = await EnsureStagingAsync(cluster, ct);
        if (!staging.IsSuccess)
            return Result.Failed(staging.Error!);
        var (nextKey, nextPem) = staging.Value;

        // Фаза D: bundle OLD+NEW в точке дискавери ДО замены сертов — клиенты,
        // перечитавшие ca_pem, доверяют сертам обоих поколений. Идемпотентно:
        // bundle уже содержит nextPem — put пропускается.
        // ca_pem гарантирован вызывающей веткой: K0.3 (аномалия-защита окна)
        // или K0.5 (проверка «кластер не поднят»).
        var caPem = snap.CaPem!;
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

    // Финал: снапшот P12 «после» + исход done + journal done + очистка треков
    // (идемпотентно; хвост afterCommit доиграет финал повторно — put исхода
    // идемпотентен).
    private async Task<Result> FinishAsync(string cluster, CancellationToken ct)
    {
        if (snapshot is not null)
        {
            var after = await snapshot(ct);
            if (!after.IsSuccess)
                return Result.Failed(after.Error!);
        }

        _rolled.TryRemove((cluster, "phase-r"), out _);

        // Исход done ДО journal done (t10): провал put → тик Failed → финал
        // повторится хвостом afterCommit — исход не теряется.
        var outcomeDone = await _tickets.WriteDoneAsync(
            TicketOutcomes.Key("/kafkaworker", cluster), TicketOutcomes.KindCa,
            _ticketAudit.GetValueOrDefault(cluster), DateTimeOffset.UtcNow.ToUnixTimeSeconds(), ct);
        if (!outcomeDone.IsSuccess)
            return Result.Failed(outcomeDone.Error!);
        _ticketAudit.TryRemove(cluster, out _);

        var done = await journal.WritePhaseAsync(cluster, Op, "done", claims.InstanceId, null, ct);
        return done.IsSuccess ? Result.Success() : Result.Failed(done.Error!);
    }

    // Ждущий исход с экспирацией под гвардом (t10): ca-заявка старее порога и
    // гвард пройден (дооконная ветка: staging отсутствует по построению K0.3;
    // mutationLive — явная проверка журнала) → снятие (kind=ca), иначе
    // journal-waiting. Фаза expired — непрефиксованная (терминальная, FinalPhases).
    private async Task<Result> WaitAsync(
        string cluster, string phase, string ticketPayload, bool mutationLive, CancellationToken ct)
    {
        var expired = await _tickets.TryExpireAsync(
            journal, cluster, Op, claims.InstanceId,
            TicketOutcomes.Key("/kafkaworker", cluster), TicketOutcomes.KindCa,
            TicketKey(cluster), ticketPayload, phase,
            options.RotationTicketTimeoutSec, DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            mutationLive, ct);
        if (!expired.IsSuccess)
            return Result.Failed(expired.Error!);
        if (expired.Value)
            return Result.Success(); // заявка снята штатно

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
        // Передержка окна (t10): DescribeCluster невозможен без endpoints —
        // считаем «не сошлось» (rolling стоит, следующий тик повторит).
        if (snap.Endpoints is null)
            return Result<bool>.Success(false);
        await using var admin = adminFactory.Create(
            snap.Endpoints!, snap.AdminUser ?? "admin", snap.AdminPassword!, snap.CaPem);
        var view = await admin.DescribeClusterAsync(ct);
        return Result<bool>.Success(view.IsSuccess && view.Value.Brokers.Count >= expected);
    }

    // Третий предикат гварда экспирации (§3.1) и он же — детектор открытого
    // окна (K0.3): журнал rotate-ca в мутационной фазе — вне {done, waiting-*}
    // (staging ставится в P и живёт до C; waiting-фазы — дооконные передержки).
    private static bool CaMutationLive(WorkState? journalState)
        => journalState is { Op: Op } j
           && j.Phase != "done"
           && !j.Phase.StartsWith("waiting-", StringComparison.Ordinal);

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
