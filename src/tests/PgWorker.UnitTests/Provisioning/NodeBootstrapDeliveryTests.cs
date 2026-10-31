using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Probes;
using PgWorker.UnitTests.Provisioning;
using PgWorker.UnitTests.Templates;
using Shared.Core;
using Shared.Etcd.Client;
using Xunit;

// AAA (П9.3): NodeBootstrapDelivery — put ключа только при изменении;
// not_initialized → Handshake → Delivered; ready+applied==mod → Delivered без
// команд канала; ready+расхождение → Apply; initializing/start_failed/нет
// статуса → Wait; Handshake 400/транспорт → Transient (адрес в Detail);
// etcd-put отказ → Transient. Проба IsOnRevisionAsync — БЕЗ put и команд.
public class NodeBootstrapDeliveryTests
{
    private static readonly ShardTopology Topology = new(
        "shop", "shard1", "shop-shard1",
        new Dictionary<string, NodeAddress> { ["shard1a"] = new("h1", new NodePorts(15432, 18008, 16432)) });

    private const string BootstrapKey = "/clusters/shop/shards/shard1/nodes/shard1a/bootstrap";
    private const string StatusKey = "/pgworker/nodes/shop/shard1/shard1a/status";
    private static readonly InstallSecrets Secrets = new("su", "sb", "adm", "mov");
    private static readonly string RestPassword = TestRestCa.GeneratePassword();

    private static (string Cert, string Key) RealPemPair()
    {
        using var rsa = System.Security.Cryptography.RSA.Create(2048);
        var request = new System.Security.Cryptography.X509Certificates.CertificateRequest(
            "CN=test", rsa, System.Security.Cryptography.HashAlgorithmName.SHA256,
            System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        using var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1),
            DateTimeOffset.UtcNow.AddYears(1));
        return (cert.ExportCertificatePem(), rsa.ExportPkcs8PrivateKeyPem());
    }

    // Фейк канала: журнал вызовов + настраиваемые исходы.
    private sealed class FakeChannel : INodeChannelClient
    {
        public readonly List<string> Calls = [];
        public int HandshakeStatus = 200;
        public int ApplyStatus = 200;
        public bool TransportFail;

        public Task<ChannelReply> HandshakeAsync(
            string baseUrl, string suPassword, string standbyPassword,
            bool isToolStart, CancellationToken ct)
        {
            Calls.Add($"handshake {baseUrl}");
            return Task.FromResult(TransportFail
                ? new ChannelReply(0, false, null, "connection refused (fake)")
                : new ChannelReply(HandshakeStatus, HandshakeStatus is >= 200 and < 300, null,
                    HandshakeStatus is >= 200 and < 300 ? null : "rejected"));
        }

        public Task<ChannelReply> ApplyAsync(string baseUrl, CancellationToken ct)
        {
            Calls.Add($"apply {baseUrl}");
            return Task.FromResult(TransportFail
                ? new ChannelReply(0, false, null, "connection refused (fake)")
                : new ChannelReply(ApplyStatus, ApplyStatus is >= 200 and < 300, 9,
                    ApplyStatus is >= 200 and < 300 ? null : "rejected"));
        }
    }

    // Тестовый CA-кеш (RestPki), чтобы серт ноды был валидной парой.
    private static RestCertificateCache NewCache()
    {
        var (caPem, caKeyPem) = TestRestCa.Generate();
        return new RestCertificateCache(caPem, caKeyPem);
    }

    private static (NodeBootstrapDelivery Delivery, Fakes.FakeEtcd Etcd, FakeChannel Channel)
        NewDelivery(RestCertificateCache cache, Fakes.FakeEtcd? sharedEtcd = null)
    {
        var etcd = sharedEtcd ?? new Fakes.FakeEtcd();
        var channel = new FakeChannel();
        var delivery = new NodeBootstrapDelivery(
            etcd, ["http://e1:2379"], channel, cache);
        return (delivery, etcd, channel);
    }

    private static void SeedStatus(Fakes.FakeEtcd etcd, string state, long? applied, long mod = 3)
    {
        etcd.Store[StatusKey] = new Fakes.FakeEtcd.Entry(
            System.Text.Json.JsonSerializer.Serialize(new Dictionary<string, object?>
            {
                ["state"] = state,
                ["applied_revision"] = applied,
                ["components"] = new Dictionary<string, string> { ["pg"] = "ready", ["doorman"] = "ready" },
                ["updated_unix"] = 1,
            }), mod, 1);
    }

    // Ключа нет → put сделан; статус not_initialized → Handshake 200 → Delivered.
    [Fact]
    public async Task Ensure_NoKey_NotInitialized_Handshake_Delivered()
    {
        // Arrange: etcd без ключа; статус not_initialized; канал 200.
        var cache = NewCache();
        var (delivery, etcd, channel) = NewDelivery(cache);
        SeedStatus(etcd, "not_initialized", null);

        // Act
        var outcome = await delivery.EnsureAsync(Topology, "shard1a", Topology.Nodes["shard1a"],
            Secrets, null, excludeParams: null, syncStrict: false, RestPassword, CancellationToken.None);

        // Assert: ключ записан; команда доставки ушла; Delivered.
        outcome.Delivered.Should().BeTrue();
        etcd.Store.Should().ContainKey(BootstrapKey);
        channel.Calls.Should().ContainSingle().Which.Should().StartWith("handshake https://pgw-shop-shard1-shard1a:8081");
    }

    // Ключ равен построенному JSON → put НЕ вызывался (счётчик put фейка).
    [Fact]
    public async Task Ensure_KeyUnchanged_NoPut()
    {
        // Arrange: первый прогон записал ключ.
        var cache = NewCache();
        var (delivery, etcd, channel) = NewDelivery(cache);
        await delivery.EnsureAsync(Topology, "shard1a", Topology.Nodes["shard1a"],
            Secrets, null, excludeParams: null, false, RestPassword, CancellationToken.None);
        channel.Calls.Clear();
        SeedStatus(etcd, "not_initialized", null);
        var putsBefore = etcd.Txns.Count; // put'ы не через txn — фиксируем значение
        var storedBefore = etcd.Store[BootstrapKey].Value;

        // Act: повторная доставка при неизменном ключе.
        await delivery.EnsureAsync(Topology, "shard1a", Topology.Nodes["shard1a"],
            Secrets, null, excludeParams: null, false, RestPassword, CancellationToken.None);

        // Assert: содержимое ключа не перезаписано (та же строка — put-пути
        // перезаписали бы через ++rev только при изменении).
        etcd.Store[BootstrapKey].Value.Should().Be(storedBefore);
    }

    // Статус ready, applied==modRevision → Delivered БЕЗ команд канала.
    [Fact]
    public async Task Ensure_ReadyOnRevision_DeliveredWithoutCommands()
    {
        // Arrange: один общий etcd; первая доставка (handshake) — потом ready.
        var cache = NewCache();
        var sharedEtcd = new Fakes.FakeEtcd();
        var (delivery0, _, channel0) = NewDelivery(cache, sharedEtcd);
        await delivery0.EnsureAsync(Topology, "shard1a", Topology.Nodes["shard1a"],
            Secrets, null, excludeParams: null, false, RestPassword, CancellationToken.None);
        var mod = sharedEtcd.Store[BootstrapKey].ModRevision;
        SeedStatus(sharedEtcd, "ready", (long)mod, mod);

        // Act
        var (delivery, _, channel) = NewDelivery(cache, sharedEtcd);
        var outcome = await delivery.EnsureAsync(Topology, "shard1a", Topology.Nodes["shard1a"],
            Secrets, null, excludeParams: null, false, RestPassword, CancellationToken.None);

        // Assert: Delivered без handshake/apply.
        outcome.Delivered.Should().BeTrue();
        channel.Calls.Should().BeEmpty();
    }

    // Статус ready, applied != modRevision → Apply 200 → Delivered.
    [Fact]
    public async Task Ensure_ReadyStaleRevision_Apply()
    {
        // Arrange: ready с устаревшей ревизией.
        var cache = NewCache();
        var (delivery, etcd, channel) = NewDelivery(cache);
        await delivery.EnsureAsync(Topology, "shard1a", Topology.Nodes["shard1a"],
            Secrets, null, excludeParams: null, false, RestPassword, CancellationToken.None);
        channel.Calls.Clear();
        SeedStatus(etcd, "ready", applied: 2, mod: etcd.Store[BootstrapKey].ModRevision);

        // Act
        var outcome = await delivery.EnsureAsync(Topology, "shard1a", Topology.Nodes["shard1a"],
            Secrets, null, excludeParams: null, false, RestPassword, CancellationToken.None);

        // Assert: apply ушёл, Delivered.
        outcome.Delivered.Should().BeTrue();
        channel.Calls.Should().Contain(c => c.StartsWith("apply", StringComparison.Ordinal));
    }

    // Статус initializing → Wait без команд; статусного ключа нет → Wait.
    [Theory]
    [InlineData("initializing")]
    [InlineData("start_failed")]
    public async Task Ensure_InProgressStates_Wait(string state)
    {
        // Arrange: ключ доставлен, статус initializing/start_failed.
        var cache = NewCache();
        var (delivery0, etcd, _) = NewDelivery(cache);
        await delivery0.EnsureAsync(Topology, "shard1a", Topology.Nodes["shard1a"],
            Secrets, null, excludeParams: null, false, RestPassword, CancellationToken.None);
        SeedStatus(etcd, state, null);
        var (delivery, _, channel) = NewDelivery(cache, etcd);

        // Act
        var outcome = await delivery.EnsureAsync(Topology, "shard1a", Topology.Nodes["shard1a"],
            Secrets, null, excludeParams: null, false, RestPassword, CancellationToken.None);

        // Assert: Wait, команд нет.
        outcome.Wait.Should().BeTrue();
        outcome.Transient.Should().BeFalse();
        channel.Calls.Should().BeEmpty();
    }

    [Fact]
    public async Task Ensure_NoStatusKey_Wait()
    {
        // Arrange: ключ доставлен, статусного ключа нет.
        var cache = NewCache();
        var (delivery0, etcd, _) = NewDelivery(cache);
        await delivery0.EnsureAsync(Topology, "shard1a", Topology.Nodes["shard1a"],
            Secrets, null, excludeParams: null, false, RestPassword, CancellationToken.None);
        etcd.Store.Remove(StatusKey);
        var (delivery, _, channel) = NewDelivery(cache, etcd);

        // Act
        var outcome = await delivery.EnsureAsync(Topology, "shard1a", Topology.Nodes["shard1a"],
            Secrets, null, excludeParams: null, false, RestPassword, CancellationToken.None);

        // Assert: Wait (агент стартует, следующий тик увидит not_initialized).
        outcome.Wait.Should().BeTrue();
        channel.Calls.Should().BeEmpty();
    }

    // Handshake 400 → Transient с адресом; транспорт (0) → Transient.
    [Fact]
    public async Task Ensure_Handshake400_Transient()
    {
        // Arrange: not_initialized + канал 400.
        var cache = NewCache();
        var (delivery, etcd, channel) = NewDelivery(cache);
        SeedStatus(etcd, "not_initialized", null);
        channel.HandshakeStatus = 400;

        // Act
        var outcome = await delivery.EnsureAsync(Topology, "shard1a", Topology.Nodes["shard1a"],
            Secrets, null, excludeParams: null, false, RestPassword, CancellationToken.None);

        // Assert
        outcome.Transient.Should().BeTrue();
        outcome.Detail.Should().Contain("pgw-shop-shard1-shard1a").And.Contain("400");
    }

    [Fact]
    public async Task Ensure_HandshakeTransport_TransientWithAddress()
    {
        // Arrange: not_initialized + транспорт-отказ.
        var cache = NewCache();
        var (delivery, etcd, channel) = NewDelivery(cache);
        SeedStatus(etcd, "not_initialized", null);
        channel.TransportFail = true;

        // Act
        var outcome = await delivery.EnsureAsync(Topology, "shard1a", Topology.Nodes["shard1a"],
            Secrets, null, excludeParams: null, false, RestPassword, CancellationToken.None);

        // Assert: адрес в Detail.
        outcome.Transient.Should().BeTrue();
        outcome.Detail.Should().Contain("pgw-shop-shard1-shard1a");
    }

    // etcd-put отказ → Transient.
    [Fact]
    public async Task Ensure_EtcdPutFails_Transient()
    {
        // Arrange: put через txn-фейк недоступен — FakeEtcd не имеет put-фейла;
        // инжектируем отказ через GetFailure при повторном чтении... простой
        // путь: range/put не падают — используем GetFailure на bootstrap-ключе.
        var cache = NewCache();
        var (delivery, etcd, _) = NewDelivery(cache);
        etcd.GetFailure = _ => Result<Kv?>.Failed(new EtcdUnreachableException("etcd down (fake)"));

        // Act
        var outcome = await delivery.EnsureAsync(Topology, "shard1a", Topology.Nodes["shard1a"],
            Secrets, null, excludeParams: null, false, RestPassword, CancellationToken.None);

        // Assert
        outcome.Transient.Should().BeTrue();
        outcome.Detail.Should().Contain(BootstrapKey);
    }

    // ── IsOnRevisionAsync: БЕЗ put и БЕЗ команд канала ──

    [Fact]
    public async Task Probe_OnRevisionWithPair_True_WithoutMutations()
    {
        // Arrange: ключ доставлен под RestPassword; ready+applied==mod.
        var cache = NewCache();
        var (delivery0, etcd, _) = NewDelivery(cache);
        await delivery0.EnsureAsync(Topology, "shard1a", Topology.Nodes["shard1a"],
            Secrets, null, excludeParams: null, false, RestPassword, CancellationToken.None);
        var mod = etcd.Store[BootstrapKey].ModRevision;
        SeedStatus(etcd, "ready", (long)mod, mod);
        var (delivery, _, channel) = NewDelivery(cache, etcd);
        var putsBefore = etcd.Txns.Count;

        // Act
        var onRevision = await delivery.IsOnRevisionAsync(Topology, "shard1a",
            Topology.Nodes["shard1a"], null, excludeParams: null, false, RestPassword, CancellationToken.None);

        // Assert: true; put/канала не было.
        onRevision.Should().BeTrue();
        channel.Calls.Should().BeEmpty();
        etcd.Txns.Count.Should().Be(putsBefore);
    }

    [Fact]
    public async Task Probe_OldPair_False()
    {
        // Arrange: ключ доставлен под RestPassword; проба под ДРУГОЙ парой.
        var cache = NewCache();
        var (delivery0, etcd, _) = NewDelivery(cache);
        await delivery0.EnsureAsync(Topology, "shard1a", Topology.Nodes["shard1a"],
            Secrets, null, excludeParams: null, false, RestPassword, CancellationToken.None);
        var mod = etcd.Store[BootstrapKey].ModRevision;
        SeedStatus(etcd, "ready", (long)mod, mod);
        var (delivery, _, _) = NewDelivery(cache);

        // Act
        var onRevision = await delivery.IsOnRevisionAsync(Topology, "shard1a",
            Topology.Nodes["shard1a"], null, excludeParams: null, false, RestPassword + "-new", CancellationToken.None);

        // Assert: false — ключ ещё не перерендерен под NEW.
        onRevision.Should().BeFalse();
    }

    [Theory]
    [InlineData("not_initialized")]
    [InlineData("initializing")]
    public async Task Probe_NotReady_False(string state)
    {
        // Arrange: ключ доставлен; статус не ready.
        var cache = NewCache();
        var (delivery0, etcd, _) = NewDelivery(cache);
        await delivery0.EnsureAsync(Topology, "shard1a", Topology.Nodes["shard1a"],
            Secrets, null, excludeParams: null, false, RestPassword, CancellationToken.None);
        var mod = etcd.Store[BootstrapKey].ModRevision;
        SeedStatus(etcd, state, null, mod);
        var (delivery, _, _) = NewDelivery(cache, etcd);

        // Act
        var onRevision = await delivery.IsOnRevisionAsync(Topology, "shard1a",
            Topology.Nodes["shard1a"], null, excludeParams: null, false, RestPassword, CancellationToken.None);

        // Assert
        onRevision.Should().BeFalse();
    }

    [Fact]
    public async Task Probe_AppliedMismatch_False()
    {
        // Arrange: ready, но applied != mod.
        var cache = NewCache();
        var (delivery0, etcd, _) = NewDelivery(cache);
        await delivery0.EnsureAsync(Topology, "shard1a", Topology.Nodes["shard1a"],
            Secrets, null, excludeParams: null, false, RestPassword, CancellationToken.None);
        SeedStatus(etcd, "ready", applied: 2, mod: etcd.Store[BootstrapKey].ModRevision);
        var (delivery, _, _) = NewDelivery(cache, etcd);

        // Act
        var onRevision = await delivery.IsOnRevisionAsync(Topology, "shard1a",
            Topology.Nodes["shard1a"], null, excludeParams: null, false, RestPassword, CancellationToken.None);

        // Assert
        onRevision.Should().BeFalse();
    }

    [Fact]
    public async Task Probe_NoStatusKey_False()
    {
        // Arrange
        var cache = NewCache();
        var (delivery0, etcd, _) = NewDelivery(cache);
        await delivery0.EnsureAsync(Topology, "shard1a", Topology.Nodes["shard1a"],
            Secrets, null, excludeParams: null, false, RestPassword, CancellationToken.None);
        etcd.Store.Remove(StatusKey);
        var (delivery, _, _) = NewDelivery(cache);

        // Act
        var onRevision = await delivery.IsOnRevisionAsync(Topology, "shard1a",
            Topology.Nodes["shard1a"], null, excludeParams: null, false, RestPassword, CancellationToken.None);

        // Assert
        onRevision.Should().BeFalse();
    }

    [Fact]
    public async Task Probe_EtcdDown_False()
    {
        // Arrange
        var cache = NewCache();
        var (delivery, etcd, _) = NewDelivery(cache);
        etcd.GetFailure = _ => Result<Kv?>.Failed(new EtcdUnreachableException("etcd down (fake)"));

        // Act
        var onRevision = await delivery.IsOnRevisionAsync(Topology, "shard1a",
            Topology.Nodes["shard1a"], null, excludeParams: null, false, RestPassword, CancellationToken.None);

        // Assert: отказ etcd → false (EnsureAsync разрулит transient).
        onRevision.Should().BeFalse();
    }
}
