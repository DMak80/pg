using AdminPanel.Core;
using AdminPanel.Core.Valkey;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Shared.Etcd.Client;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер valkey-домена: канон arch/20 §2.1 (приёмочные) + толерантность §5.
public sealed class ValkeyParserTests
{
    // Arrange: канонические фикстуры §2.1. Act: ParseClusters. Assert: поля дословно.
    [Fact]
    public void ParseClusters_Canonical_BuildsModel()
    {
        var kvs = EtcdFixtures.LoadKv("Valkey/clusters-canonical.json");

        var result = ValkeyParser.ParseClusters(kvs);

        // cache: заявка NOT_INITIALIZED (config со state), нода без live.
        var cache = Assert.Single(result.Clusters, c => c.Name == "cache");
        Assert.Equal(ValkeyClusterState.NotInitialized, cache.State);
        Assert.Equal(1, cache.Nodes);
        Assert.Equal(536870912L, cache.MaxmemoryBytes);
        Assert.Equal("allkeys-lru", cache.MaxmemoryPolicy);
        Assert.Equal(1756500000L, cache.CreatedUnix);
        Assert.Null(cache.Endpoints);
        var node = Assert.Single(cache.NodesList);
        Assert.Equal("node1", node.Name);
        Assert.Equal("NOT_INITIALIZED", node.State);
        Assert.Equal(2m, node.Cpu);
        Assert.Equal(4, node.MemGi);
        Assert.Equal(40, node.DiskGi);

        // live: Active-config БЕЗ поля state; endpoints фактом.
        var live = Assert.Single(result.Clusters, c => c.Name == "live");
        Assert.Equal(ValkeyClusterState.Active, live.State);
        Assert.Equal("host.docker.internal:17001", live.Endpoints);
        Assert.Equal("RUNNING", Assert.Single(live.NodesList).State);

        // dying: TO_REMOVE сохранён.
        Assert.Equal(ValkeyClusterState.ToRemove,
            Assert.Single(result.Clusters, c => c.Name == "dying").State);

        // app_*/admin_* пропущены МОЛЧА: в модель не попадают и unknownKeys не растят.
        Assert.Equal(1, result.UnknownKeyCount); // только future_feature
        Assert.Empty(result.Errors);
    }

    // Arrange: битый config/weird state/пустой endpoints/битые ресурсы.
    // Act: ParseClusters. Assert: parseError-записи без исключений; странное state → Active.
    [Fact]
    public void ParseClusters_Tolerance_TableRows()
    {
        var result = ValkeyParser.ParseClusters(EtcdFixtures.LoadKv("Valkey/clusters-tolerance.json"));

        // битый JSON config → кластер-скелет + parseError.
        Assert.Contains(result.Errors, e => e.Key == "/valkey/clusters/bad/config");
        // незнакомое state → Active.
        Assert.Equal(ValkeyClusterState.Active,
            Assert.Single(result.Clusters, c => c.Name == "weird").State);
        // пустой/пробельный endpoints → null (нет — как отсутствие).
        Assert.Null(Assert.Single(result.Clusters, c => c.Name == "noep").Endpoints);
        // resources: cpu не число → parseError; неканонический суффикс mem/disk → null-поля (не ошибка формата Gi).
        var badRes = Assert.Single(result.Clusters, c => c.Name == "badres");
        var res = Assert.Single(badRes.NodesList);
        Assert.Null(res.Cpu);
        Assert.Null(res.MemGi);
        Assert.Null(res.DiskGi);
        // частичные креды (один admin_user) — не ошибка парсера: ignored-ключ.
        Assert.DoesNotContain(result.Errors, e => e.Key.Contains("half", StringComparison.Ordinal));
    }

    // Arrange: rotations.json. Act: ParseRotations. Assert: role raw-строка; битый JSON → parseError.
    [Fact]
    public void ParseRotations_RawRole_And_BrokenJson()
    {
        var result = ValkeyParser.ParseRotations(EtcdFixtures.LoadKv("Valkey/rotations.json"));

        Assert.Equal(2, result.Tickets.Count);
        var app = Assert.Single(result.Tickets, t => t.Cluster == "live");
        Assert.Equal("app", app.Role);
        Assert.Equal("seed", app.RequestedBy);
        var admin = Assert.Single(result.Tickets, t => t.Cluster == "other");
        Assert.Equal("admin", admin.Role); // raw-строка, толерантно к новым значениям
        Assert.Null(admin.RequestedBy);
        Assert.Contains(result.Errors, e => e.Key == "/valkeyworker/rotations/broken");
        Assert.Contains(result.Errors, e => e.Key == "/valkeyworker/rotations/nofield");
    }

    // t06: ca_pem — факт TLS-канона: HasCaPem=true; отсутствие ключа — false;
    // в unknownKeys ca_pem НЕ попадает (известный ключ, arch/20 §2).
    [Fact]
    public void ParseClusters_CaPem_HasCaPemFlag()
    {
        // Arrange: два кластера — с ca_pem и без.
        var kvs = new List<Kv>
        {
            new("/valkey/clusters/withca/config",
                """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"allkeys-lru","created_unix":1}""", 1),
            new("/valkey/clusters/withca/ca_pem", "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----", 1),
            new("/valkey/clusters/noca/config",
                """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"allkeys-lru","created_unix":1}""", 1),
        };

        // Act
        var result = ValkeyParser.ParseClusters(kvs);

        // Assert
        Assert.True(Assert.Single(result.Clusters, c => c.Name == "withca").HasCaPem);
        Assert.False(Assert.Single(result.Clusters, c => c.Name == "noca").HasCaPem);
        Assert.Equal(0, result.UnknownKeyCount);
    }

    // Arrange: ca-rotations.json. Act: ParseCaRotations. Assert: тикет без role;
    // битый JSON/пустой/вложенный ключ → parseError-толерантность (порт rotations).
    [Fact]
    public void ParseCaRotations_TicketWithoutRole_And_BrokenJson()
    {
        var result = ValkeyParser.ParseCaRotations(EtcdFixtures.LoadKv("Valkey/ca-rotations.json"));

        result.Tickets.Should().ContainSingle(t => t.Cluster == "live"
            && t.RequestedUnix == 1756500123 && t.RequestedBy == "seed");
        Assert.Contains(result.Errors, e => e.Key == "/valkeyworker/ca_rotations/broken");
        Assert.Contains(result.Errors, e => e.Key == "/valkeyworker/ca_rotations/nofield");
        Assert.Contains(result.Errors, e => e.Key == "/valkeyworker/ca_rotations/nested/bad");
    }
}
