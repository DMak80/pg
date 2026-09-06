using System.Globalization;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Planning;
using KafkaWorker.Core.Templates;

namespace KafkaWorker.Provisioning.Processes;

/// <summary>
/// Сборка env брокера для процессов B-волны (add-broker, ротация, надзор):
/// кворум — ТОЛЬКО из существующих controller-нод (роль фиксируется при
/// создании, arch/15 §2), advertised — по правилу arch/16 §2.1; креды
/// admin/app и per-cluster PKI — из снапшота + кеша сертов (t03, arch/16
/// §2.2/§2.3). Дублирование с ProvisioningProcess осознанное (прецедент
/// надзора).
/// </summary>
internal static class BrokerEnvBuilder
{
    // NodeId из имени broker<k> (0 — неканоническое имя, тесты это не используют).
    internal static int NodeId(string nodeName)
        => int.TryParse(nodeName["broker".Length..], NumberStyles.Integer, CultureInfo.InvariantCulture, out var id)
            ? id
            : 0;

    internal static string AdvertisedClient(
        KafkaClusterSnapshot snap, string broker, NodeAddress addr, ProvisioningOptions options)
        => $"{options.AdvertisedClientHost ?? addr.Host}:{addr.ClientPort}";

    // Кворум "id@broker<k>:9093" по controller-ролям декларации (не меняется
    // при добавлении broker-only нод — arch/16 §5 F).
    internal static IReadOnlyList<string> QuorumVoters(KafkaClusterSnapshot snap)
        => snap.Brokers
            .Where(b => b.Role == "controller")
            .OrderBy(b => b.Name, StringComparer.Ordinal)
            .Select(b => $"{NodeId(b.Name)}@{b.Name}:9093")
            .ToList();

    // Env одного брокера: guard премиграционного кластера (нет CA/admin-ключей —
    // сначала M), креды (1 пароль штатно, 2 — окно ротации), серт ноды — один
    // раз на (кластер, нода, CA) через кеш R3. Окно ротации CA (t07, arch/16
    // §2.3): подпись серта — NEW CA (signingCa*), truststore — bundle OLD+NEW
    // (trustCaPem); вне ротации все три параметра null → snap-значения.
    internal static IReadOnlyDictionary<string, string> Build(
        KafkaClusterSnapshot snap,
        string broker,
        NodeAddress addr,
        IReadOnlyList<string> appPasswords,
        IReadOnlyList<string> adminPasswords,
        ProvisioningOptions options,
        BrokerCertificateCache certificates,
        string? signingCaPem = null,
        string? signingCaKey = null,
        string? trustCaPem = null)
    {
        var signingPem = signingCaPem ?? snap.CaPem;
        var signingKey = signingCaKey ?? snap.CaKey;
        var trust = trustCaPem ?? snap.CaPem;
        if (signingPem is null || signingKey is null || snap.AdminPassword is null)
            throw new ApplicationException(
                $"env {snap.Cluster}/{broker}: премиграционный кластер (нет CA/admin-ключей) — сначала SecurityMigrator M");

        var decl = snap.Brokers.Single(b => b.Name == broker);
        var advertisedClient = AdvertisedClient(snap, broker, addr, options);
        var (certPem, keyPem) = certificates.GetOrCreate(
            snap.Cluster, broker, signingPem, signingKey, advertisedClient);
        return NodeEnvBuilder.Build(new NodeEnvSpec(
            snap.Cluster,
            NodeId(broker),
            broker,
            advertisedClient,
            decl.Role == "controller",
            QuorumVoters(snap),
            snap.AppUser ?? "app",
            appPasswords,
            snap.AdminUser ?? "admin",
            adminPasswords,
            trust!,
            certPem,
            keyPem,
            snap.Config,
            snap.Config.Brokers,
            "/var/lib/kafka/data"));
    }
}
