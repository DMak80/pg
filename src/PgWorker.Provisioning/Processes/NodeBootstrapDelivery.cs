using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Core.Tuning;
using PgWorker.Provisioning.Probes;
using Shared.Core;
using Shared.Etcd.Client;

namespace PgWorker.Provisioning.Processes;

/// <summary>Итог доставки: Delivered — команда принята/агент на ревизии;
/// Wait — машина в ходу (нет статуса / initializing / start_failed — реакция
/// надзора); Transient — канал/etcd недоступны (journal-факт с Detail, НЕ
/// провал EnsureNode-пути — spec §3.7.2).</summary>
public sealed record DeliveryOutcome(bool Delivered, bool Wait, bool Transient, string? Detail);

/// <summary>Доставка bootstrap-данных ноде (arch/14 §4, spec §3.7.2): put
/// ключа /clusters/&lt;C&gt;/shards/&lt;X&gt;/nodes/&lt;n&gt;/bootstrap только
/// при изменении (симметричная сериализация — без дребезга mod_revision),
/// затем хендшейк по статусному ключу: not_initialized → команда доставки с
/// per-install паролями PG; ready c расхождением applied_revision → команда
/// применения. Инициатор всегда воркер; идемпотентен на каждом шаге.</summary>
public interface INodeBootstrapDelivery
{
    // isToolStart (spec §3.6-3а/§8-22): true — агент ТОЛЬКО готовит параметры
    // (tmpfs-рендер, компоненты не стартуют, → ready prepared); false —
    // штатный bootstrap (старт компонентов, → ready). Restore-контур — true.
    Task<DeliveryOutcome> EnsureAsync(
        ShardTopology topology, string nodeName, NodeAddress addr, InstallSecrets secrets,
        PgTuneResult? tuning, IReadOnlySet<string>? excludeParams, bool syncStrict,
        string restPassword, bool isToolStart, CancellationToken ct);

    /// <summary>Немутирующая проба «нода применила bootstrap с ЭТОЙ парой
    /// restPassword» (ротация, шаг 11): без put и без команд канала.
    /// true ⇔ content хранимого ключа == NodeBootstrapBuilder.Build(…,
    /// restPassword, …) И статус ready И applied_revision == mod_revision
    /// ключа. Отказ etcd → false (EnsureAsync разрулит transient).</summary>
    Task<bool> IsOnRevisionAsync(
        ShardTopology topology, string nodeName, NodeAddress addr,
        PgTuneResult? tuning, IReadOnlySet<string>? excludeParams, bool syncStrict,
        string restPassword, CancellationToken ct);
}

public sealed class NodeBootstrapDelivery(
    IEtcdGateway etcd,
    string[] endpoints,
    INodeChannelClient channel,
    RestCertificateCache restCertificates) : INodeBootstrapDelivery
{
    public static string BootstrapKey(string cluster, string shard, string node)
        => $"/clusters/{cluster}/shards/{shard}/nodes/{node}/bootstrap";

    public async Task<DeliveryOutcome> EnsureAsync(
        ShardTopology topology, string nodeName, NodeAddress addr, InstallSecrets secrets,
        PgTuneResult? tuning, IReadOnlySet<string>? excludeParams, bool syncStrict,
        string restPassword, bool isToolStart, CancellationToken ct)
    {
        // 1) Построить ожидаемый JSON (детерминированный рендер) и сравнить с
        // хранимым: равен — put НЕ делаем (без дребезга mod_revision, spec
        // §3.7.2); иначе put → повторный GET (нужен актуальный ModRevision).
        var (certPem, keyPem) = restCertificates.GetOrCreate(topology.Cluster, topology.Shard, nodeName);
        var expectedJson = NodeBootstrapBuilder.Build(
            topology, new EtcdEndpoints(endpoints), syncStrict, nodeName,
            certPem, keyPem, restPassword, tuning, excludeParams);
        var bootstrapKey = BootstrapKey(topology.Cluster, topology.Shard, nodeName);

        var read = await GetAsync(bootstrapKey, ct);
        if (!read.IsSuccess)
            return Transient($"etcd {bootstrapKey}: {read.Error!.Message.Split('\n')[0]}");
        ulong modRevision;
        if (read.Value is null || read.Value.Value != expectedJson)
        {
            var put = await EtcdEndpointFailover.CallAsync(
                endpoints, (endpoint, token) => etcd.PutAsync(endpoint, bootstrapKey, expectedJson, null, token), ct);
            if (!put.IsSuccess)
                return Transient($"etcd put {bootstrapKey}: {put.Error!.Message.Split('\n')[0]}");
            var reRead = await GetAsync(bootstrapKey, ct);
            if (!reRead.IsSuccess || reRead.Value is null)
                return Transient($"etcd re-read {bootstrapKey} failed");
            modRevision = reRead.Value.ModRevision;
        }
        else
        {
            modRevision = read.Value.ModRevision;
        }

        // 2) Статусный ключ → реакции: нет — Wait (агент стартует, следующий
        // тик увидит not_initialized — arch/14 §5 C); not_initialized —
        // команда доставки; initializing/start_failed — Wait; ready с
        // расхождением applied_revision — команда применения.
        var statusKey = PgNodeStatusFact.StatusKey(topology.Cluster, topology.Shard, nodeName);
        var statusRead = await GetAsync(statusKey, ct);
        if (!statusRead.IsSuccess)
            return Transient($"etcd {statusKey}: {statusRead.Error!.Message.Split('\n')[0]}");
        var status = statusRead.Value is { } kv ? PgNodeStatusFact.Parse(kv.Value) : null;
        if (status is null)
            return new DeliveryOutcome(Delivered: false, Wait: true, Transient: false, null);

        var baseUrl = NodeChannelAddress.ForNode(topology.Cluster, topology.Shard, nodeName);
        switch (status.State)
        {
            case "not_initialized":
                var handshake = await channel.HandshakeAsync(
                    baseUrl, secrets.SuPassword, secrets.StandbyPassword,
                    isToolStart, ct);
                return handshake.IsSuccess
                    ? new DeliveryOutcome(Delivered: true, Wait: false, Transient: false, null)
                    : Transient($"{baseUrl}/api/bootstrap → {handshake.StatusCode}");
            case "initializing":
            case "start_failed":
                return new DeliveryOutcome(Delivered: false, Wait: true, Transient: false, null);
            case "ready":
                if (status.AppliedRevision == (long)modRevision)
                    return new DeliveryOutcome(Delivered: true, Wait: false, Transient: false, null);
                var apply = await channel.ApplyAsync(baseUrl, ct);
                return apply.IsSuccess
                    ? new DeliveryOutcome(Delivered: true, Wait: false, Transient: false, null)
                    : Transient($"{baseUrl}/api/bootstrap/apply → {apply.StatusCode}");
            default:
                return new DeliveryOutcome(Delivered: false, Wait: true, Transient: false, null);
        }
    }

    public async Task<bool> IsOnRevisionAsync(
        ShardTopology topology, string nodeName, NodeAddress addr,
        PgTuneResult? tuning, IReadOnlySet<string>? excludeParams, bool syncStrict,
        string restPassword, CancellationToken ct)
    {
        // БЕЗ put и БЕЗ команд канала (не нарушает ≤1 нода/тик): content-сверка
        // хранимого ключа с рендером под ЭТУ пару + ready + applied==mod.
        var (certPem, keyPem) = restCertificates.GetOrCreate(topology.Cluster, topology.Shard, nodeName);
        var expectedJson = NodeBootstrapBuilder.Build(
            topology, new EtcdEndpoints(endpoints), syncStrict, nodeName,
            certPem, keyPem, restPassword, tuning, excludeParams);

        var bootstrapKey = BootstrapKey(topology.Cluster, topology.Shard, nodeName);
        var read = await GetAsync(bootstrapKey, ct);
        if (!read.IsSuccess || read.Value is null || read.Value.Value != expectedJson)
            return false; // ключ ещё не перерендерен под эту пару / etcd-отказ

        var statusKey = PgNodeStatusFact.StatusKey(topology.Cluster, topology.Shard, nodeName);
        var statusRead = await GetAsync(statusKey, ct);
        if (!statusRead.IsSuccess || statusRead.Value is null)
            return false;
        var status = PgNodeStatusFact.Parse(statusRead.Value.Value);
        return status is { State: "ready" }
            && status.AppliedRevision == (long)read.Value.ModRevision;
    }

    private async Task<Result<Kv?>> GetAsync(string key, CancellationToken ct)
        => await EtcdEndpointFailover.CallAsync(
            endpoints, (endpoint, token) => etcd.GetAsync(endpoint, key, token), ct);

    private static DeliveryOutcome Transient(string detail)
        => new(Delivered: false, Wait: false, Transient: true, detail);
}
