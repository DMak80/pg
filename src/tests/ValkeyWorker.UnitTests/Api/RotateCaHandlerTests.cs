using FluentAssertions;
using ValkeyWorker.App.Api.Operations;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.Api;

// RotateCaHandler (t07): валидация имени → 404; config-гейт → 404;
// state-гейт (НЕ-Active) → 409; живая заявка → 409; happy-path → 202-DTO +
// payload заявки {"requested_unix","requested_by"} без role. AAA.
public class RotateCaHandlerTests
{
    private static readonly Fakes.FakeEtcd Etcd = new();

    public RotateCaHandlerTests()
    {
        Etcd.Seed("/valkey/clusters/live/config",
            """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"allkeys-lru","created_unix":1}""");
        Etcd.Seed("/valkey/clusters/removing/config",
            """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"allkeys-lru","created_unix":1,"state":"TO_REMOVE"}""");
    }

    private static RotateCaHandler Handler()
        => new(Etcd, ["http://etcd:2379"], TimeProvider.System);

    [Fact]
    public async Task NonCanonicalName_ClusterNotFound()
    {
        // Arrange / Act — имя вне канона
        var result = await Handler().HandleAsync("Bad_Name", "it", TestContext.Current.CancellationToken);

        // Assert
        result.Error.Should().BeOfType<ValkeyClusterNotFoundException>();
    }

    [Fact]
    public async Task NoConfig_ClusterNotFound()
    {
        // Arrange / Act — имя каноническое, config-ключа нет
        var result = await Handler().HandleAsync("ghost", "it", TestContext.Current.CancellationToken);

        // Assert
        result.Error.Should().BeOfType<ValkeyClusterNotFoundException>();
    }

    [Fact]
    public async Task NotActive_Conflict()
    {
        // Arrange / Act — state=TO_REMOVE: ротация не поднятого кластера бессмысленна
        var result = await Handler().HandleAsync("removing", "it", TestContext.Current.CancellationToken);

        // Assert
        result.Error.Should().BeOfType<ValkeyClusterNotActiveException>();
    }

    [Fact]
    public async Task LiveTicket_Conflict()
    {
        // Arrange — заявка уже жива
        Etcd.Seed("/valkeyworker/ca_rotations/live",
            """{"requested_unix":1756500000,"requested_by":"x"}""");

        // Act
        var result = await Handler().HandleAsync("live", "it", TestContext.Current.CancellationToken);

        // Assert — 409 (после исполнения ключ исчезает — POST снова валиден)
        result.Error.Should().BeOfType<ValkeyRotationAlreadyRequestedException>();
    }

    [Fact]
    public async Task HappyPath_ClaimTxnAndDto()
    {
        // Arrange — СВОЙ кластер (Etcd статический: ключ заявки от
        // LiveTicket_Conflict на "live" не должен влиять на этот кейс)
        Etcd.Seed("/valkey/clusters/fresh/config",
            """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"allkeys-lru","created_unix":1}""");

        // Act
        var result = await Handler().HandleAsync("fresh", "opsuser", TestContext.Current.CancellationToken);

        // Assert — 202-DTO + payload заявки в etcd (без role)
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        result.Value.Cluster.Should().Be("fresh");
        result.Value.RequestedBy.Should().Be("opsuser");
        result.Value.RequestedUnix.Should().BeGreaterThan(0);
        var raw = (await Etcd.GetAsync("http://etcd:2379", "/valkeyworker/ca_rotations/fresh",
            TestContext.Current.CancellationToken)).Value!.Value;
        raw.Should().Contain("\"requested_by\":\"opsuser\"").And.NotContain("role");
    }
}
