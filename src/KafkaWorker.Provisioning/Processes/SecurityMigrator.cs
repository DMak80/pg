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
/// SecurityMigrator (t03, arch/16 §5 M): converge-миграция премиграционного
/// кластера (SASL_PLAINTEXT) в канон t03 (SASL_SSL + authorizer). Детект —
/// чистая функция NeedsMigration (etcd-поля CA/admin отсутствуют ИЛИ env
/// живого контейнера без KAFKA_SSL_TRUSTSTORE_TYPE). Фазы: M0 guard'ы живых
/// операций (ротации/rebalance/reassignment/regen → journal-waiting);
/// M1 ensure секретов (CA+admin+app одной txn); M2 rolling-пересоздание ВСЕХ
/// живых брокеров разом (том сохраняется; порты/сеть/roles из portalloc — без
/// изменений); M3 ожидание готовности (DescribeCluster с admin-кредом по CLIENT
/// endpoints, бюджет BrokerBootSec); M4 стартовый ACL-converge + journal done.
/// Идемпотентно: канонический кластер → NotNeeded. Вызывается только держателем
/// клэйма &lt;C&gt;, первым шагом Active-ветки.
/// </summary>
public sealed class SecurityMigrator(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    IClusterSecretEnsurer secrets,
    IKafkaAdminClientFactory adminFactory,
    IClusterConfigConverger converger,
    ProvisioningOptions options,
    BrokerCertificateCache certificates)
{
    public const string Op = "migrate-security";

    // Бюджет ожидания M3 (BrokerBootSec): отсчёт с первого наблюдения
    // неготовности — диагностика, не клэйм (образец ProvisioningProcess).
    private readonly ConcurrentDictionary<string, long> _bootWaitSince = new();

    private const string PhaseWaitingRotation = "waiting-rotation";
    private const string PhaseWaitingReassignment = "waiting-reassignment";
    private const string PhaseWaitingBrokers = "waiting-brokers";
    private const string PhaseDone = "done";

    /// <summary>Итог тика миграции: NotNeeded — канонический кластер.</summary>
    public enum MigrationOutcome
    {
        NotNeeded,
        InProgress,
    }

    /// <summary>
    /// Чистый детект премиграционного кластера (arch/16 §5 M): CA/admin-ключи
    /// в etcd отсутствуют ИЛИ любой живой контейнер без KAFKA_SSL_TRUSTSTORE_TYPE.
    /// </summary>
    public static bool NeedsMigration(
        KafkaClusterSnapshot snap, IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> containerEnvs)
    {
        // etcd-поля безопасности отсутствуют — премиграционный кластер.
        if (snap.CaPem is null || snap.CaKey is null || snap.AdminPassword is null)
            return true;

        // Любой живой контейнер без TLS-truststore — env старого канона.
        foreach (var env in containerEnvs.Values)
            if (!env.ContainsKey("KAFKA_SSL_TRUSTSTORE_TYPE"))
                return true;

        return false;
    }

    public async Task<Result<MigrationOutcome>> RunAsync(KafkaClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Мутации — только держателем живого клэйма (arch/16 §5).
        if (!claims.IsMine(cluster))
            return Result<MigrationOutcome>.Failed(new ApplicationException(
                $"migrate-security {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // Детект: etcd-поля + env живых контейнеров (best-effort инспект:
        // контейнер не найден → env нет — не блокирует детект по etcd-полям).
        var liveBrokers = snap.Brokers
            .Where(b => b.State is not "TO_REMOVE" and not "REMOVING")
            .OrderBy(b => b.Name, StringComparer.Ordinal)
            .ToList();
        var containerEnvs = new Dictionary<string, IReadOnlyDictionary<string, string>>();
        foreach (var broker in liveBrokers)
        {
            var env = await driver.NodeEnvAsync(cluster, broker.Name, ct);
            if (env.IsSuccess && env.Value is not null)
                containerEnvs[broker.Name] = env.Value;
        }

        // NeedsMigration=false возможно уже после M2 (env контейнера нового
        // канона), но M3 (готовность) и M4 (ACL-converge) могли не завершиться
        // (типовой случай: ожидание готовности между тиками). Канонический итог
        // фиксирует journal done — только он даёт NotNeeded.
        var journalState = await journal.ReadAsync(cluster, ct);
        if (!journalState.IsSuccess)
            return Result<MigrationOutcome>.Failed(journalState.Error!);
        // Нет нашей journal-записи вовсе (кластер сразу поднят новым кодом) ИЛИ
        // миграция доведена до done — канонический итог.
        var migrationDone = !NeedsMigration(snap, containerEnvs)
            && (journalState.Value is null
                || journalState.Value.Op != Op
                || journalState.Value is { Phase: PhaseDone });
        if (migrationDone)
            return Result<MigrationOutcome>.Success(MigrationOutcome.NotNeeded);

        // M0: guard'ы живых операций — миграция не должна пересекаться с
        // ротациями/rebalance/reassignment/regen (передержка тиком).
        var guard = await GuardAliveOperationsAsync(cluster, ct);
        if (guard is not null)
            return await FinishTickAsync(cluster, guard.Value.Phase, ct);

        // M1: ensure секретов кластера — CA + admin (+ app добором той же txn).
        var ensured = await secrets.EnsureAsync(cluster, ct);
        if (!ensured.IsSuccess)
            return await FailAsync(cluster, ensured.Error!, "ensure-secrets", ct);

        // Снапшот прибыл премиграционным (поля безопасности null) — после M1
        // обновляем его ensured-значениями: env нового канона собирается
        // BrokerEnvBuilder'ом именно из снапшота (guard валиден для чужих путей).
        snap = snap with
        {
            AppUser = ensured.Value.AppUser,
            AppPassword = ensured.Value.AppPassword,
            AdminUser = ensured.Value.AdminUser,
            AdminPassword = ensured.Value.AdminPassword,
            CaPem = ensured.Value.CaPem,
            CaKey = ensured.Value.CaKey,
        };

        var brokers = liveBrokers;
        if (brokers.Count == 0)
            return Result<MigrationOutcome>.Success(MigrationOutcome.InProgress);

        // M2: пересоздание ВСЕХ живых брокеров разом (не rolling: смешанные
        // inter-broker протоколы роняют ISR ниже minISR, arch/16 §5 M). Env —
        // нового канона (BrokerEnvBuilder guard защищает от премиграционного
        // снапшота); тома сохраняются, адреса — portalloc.
        foreach (var broker in brokers)
        {
            if (containerEnvs.TryGetValue(broker.Name, out var env)
                && env.ContainsKey("KAFKA_SSL_TRUSTSTORE_TYPE"))
                continue; // уже в новом каноне (повторный тик) — не трогаем

            var recreated = await RecreateBrokerAsync(snap, broker, ensured.Value, ct);
            if (!recreated.IsSuccess)
                return await FailAsync(cluster, recreated.Error!, "recreate-brokers", ct);
        }

        // M3: ожидание готовности — DescribeCluster с admin-кредом по CLIENT
        // endpoints; бюджет BrokerBootSec с первого наблюдения неготовности.
        var endpoints = snap.Endpoints;
        if (endpoints is null)
        {
            var started = await journal.WriteAsync(cluster, Op, "waiting-endpoints", claims.InstanceId, null, ct);
            return started.IsSuccess
                ? Result<MigrationOutcome>.Success(MigrationOutcome.InProgress)
                : Result<MigrationOutcome>.Failed(started.Error!);
        }

        await using (var admin = adminFactory.Create(
            endpoints, ensured.Value.AdminUser, ensured.Value.AdminPassword, ensured.Value.CaPem))
        {
            var view = await admin.DescribeClusterAsync(ct);
            var ready = view.IsSuccess
                && view.Value.ControllerId is not null
                && view.Value.Brokers.Count >= brokers.Count;
            if (ready)
            {
                _bootWaitSince.TryRemove(cluster, out _);
                // Перезакатанные брокеры — в строй (образец K4): state=RUNNING
                // тем, кто ещё не в RUNNING.
                foreach (var broker in brokers.Where(b => b.State != "RUNNING"))
                {
                    var running = await PutAsync(BrokerStateKey(cluster, broker.Name), "RUNNING", ct);
                    if (!running.IsSuccess)
                        return Result<MigrationOutcome>.Failed(running.Error!);
                }
            }
            else
            {
                // Не готово: budget с первого наблюдения; превышение — Failed
                // тик с диагнозом (брокер завис — не ждём вечно).
                var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                var since = _bootWaitSince.GetOrAdd(cluster, now);
                var reason = view.IsSuccess
                    ? $"брокеров в кластере {view.Value.Brokers.Count} из {brokers.Count}"
                    : view.Error!.Message;
                if (options.BrokerBootSec <= 0 || now - since > options.BrokerBootSec)
                    return await FailAsync(cluster, new ApplicationException(
                        $"migrate-security {cluster}: кластер не собрался за бюджет {options.BrokerBootSec} с: {reason}"),
                        PhaseWaitingBrokers, ct);

                return await FinishTickAsync(cluster, PhaseWaitingBrokers, ct);
            }
        }

        // M4: стартовый ACL-converge (E); endpoints НЕ пишем (хосты/порты
        // не менялись); journal done — следующий тик: NeedsMigration=false → NotNeeded.
        var converged = await converger.ApplyAsync(
            cluster, endpoints, ensured.Value.AdminUser, ensured.Value.AdminPassword,
            ensured.Value.CaPem, snap.Config, ct);
        if (!converged.IsSuccess)
            return await FailAsync(cluster, converged.Error!, "acl-converge", ct);

        var done = await journal.WriteAsync(cluster, Op, PhaseDone, claims.InstanceId, null, ct);
        return done.IsSuccess
            ? Result<MigrationOutcome>.Success(MigrationOutcome.InProgress)
            : Result<MigrationOutcome>.Failed(done.Error!);
    }

    // M0: живые операции блокируют миграцию (journal-waiting, InProgress).
    private async Task<(string Phase, string Key)?> GuardAliveOperationsAsync(
        string cluster, CancellationToken ct)
    {
        foreach (var (phase, prefix) in new[]
                 {
                     (PhaseWaitingRotation, "/kafkaworker/rotations/"),
                     (PhaseWaitingRotation, "/kafkaworker/admin_rotations/"),
                     (PhaseWaitingReassignment, "/kafkaworker/rebalances/"),
                 })
        {
            var ticket = await GetAsync($"{prefix}{cluster}", ct);
            if (ticket.IsSuccess && ticket.Value is not null)
                return (phase, $"{prefix}{cluster}");
        }

        foreach (var prefix in new[] { "/kafkaworker/reassignments/", "/kafkaworker/regens/" })
        {
            var progress = await GetAsync($"{prefix}{cluster}", ct);
            if (progress.IsSuccess && progress.Value is not null)
                return (PhaseWaitingReassignment, $"{prefix}{cluster}");
        }

        return null;
    }

    private async Task<Result<MigrationOutcome>> FinishTickAsync(
        string cluster, string phase, CancellationToken ct)
    {
        var written = await journal.WriteAsync(cluster, Op, phase, claims.InstanceId, null, ct);
        return written.IsSuccess
            ? Result<MigrationOutcome>.Success(MigrationOutcome.InProgress)
            : Result<MigrationOutcome>.Failed(written.Error!);
    }

    private async Task<Result<MigrationOutcome>> FailAsync(
        string cluster, Exception error, string phase, CancellationToken ct)
    {
        var written = await journal.WriteAsync(cluster, Op, phase, claims.InstanceId, error.Message, ct);
        return written.IsSuccess
            ? Result<MigrationOutcome>.Failed(error)
            : Result<MigrationOutcome>.Failed(written.Error!);
    }

    // Пересоздание одного брокера в новом каноне: RemoveNode (том жив) →
    // EnsureNode с env нового канона; адреса — portalloc (без изменений).
    private async Task<Result> RecreateBrokerAsync(
        KafkaClusterSnapshot snap, KafkaBrokerDecl broker, ClusterSecrets secret, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        var addresses = await ReadPortAllocAsync(cluster, ct);
        if (!addresses.IsSuccess)
            return addresses.Error!;
        if (!addresses.Value.TryGetValue(broker.Name, out var addr))
            return Result.Failed(new ApplicationException(
                $"migrate-security {cluster}: broker {broker.Name} не закреплён в portalloc"));

        var removed = await driver.RemoveNodeAsync(cluster, broker.Name, removeVolume: false, ct);
        if (!removed.IsSuccess)
            return removed;

        var env = BrokerEnvBuilder.Build(
            snap, broker.Name, addr,
            [secret.AppPassword], [secret.AdminPassword], options, certificates);
        var spec = new KafkaNodeSpec(
            cluster, broker.Name, addr.Host, addr.ClientPort, options.NodeImage, env,
            broker.Resources?.Cpu,
            broker.Resources is null ? null : broker.Resources.MemGi * 1024L * 1024 * 1024);
        return await driver.EnsureNodeAsync(spec, ct);
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

    private static string BrokerStateKey(string cluster, string broker)
        => $"/kafka/clusters/{cluster}/brokers/{broker}/state";

    private async Task<Result> PutAsync(string key, string value, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.PutAsync(endpoint, key, value, null, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }
}
