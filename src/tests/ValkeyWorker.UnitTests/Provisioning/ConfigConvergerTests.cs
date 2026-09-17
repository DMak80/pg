using FluentAssertions;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;
using ValkeyWorker.Etcd.Parsing;
using ValkeyWorker.UnitTests.Provisioning;
using Xunit;

namespace ValkeyWorker.UnitTests.Provisioning;

// ConfigConverger (arch/21 §5 D): CONFIG SET при расхождении, ACL-восстановление,
// R3-warning, слепая нода → Failed.
public class ConfigConvergerTests
{
    private sealed class Rig
    {
        public Fakes.FakeEtcd Etcd = new();
        public Fakes.FakeValkeyConnection Valkey = new() { TrustAnyPassword = true };
        public ValkeyWorker.Provisioning.Processes.ConfigConverger Converger = null!;

        public Rig()
        {
            Converger = new ValkeyWorker.Provisioning.Processes.ConfigConverger(
                Valkey, Etcd, ["http://etcd:2379"],
                new WorkJournal("/valkeyworker", Etcd, ["http://etcd:2379"]));
        }

        public void SeedActive(string cluster, string resources = """{"cpu":"2","mem":"1Gi","disk":"10Gi"}""")
        {
            Etcd.Seed($"/valkey/clusters/{cluster}/config",
                """{"nodes":1,"maxmemory_bytes":536870912,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""");
            Etcd.Seed($"/valkey/clusters/{cluster}/nodes/node1/resources", resources);
            Etcd.Seed($"/valkey/clusters/{cluster}/endpoints", "h1:17001");
            Etcd.Seed($"/valkey/clusters/{cluster}/app_user", "app");
            Etcd.Seed($"/valkey/clusters/{cluster}/app_password", "AppPassword0123456789abcdef12345");
            Etcd.Seed($"/valkey/clusters/{cluster}/admin_user", "admin");
            Etcd.Seed($"/valkey/clusters/{cluster}/admin_password", "AdminPassword0123456789abcdef12345");
        }

        public ValkeyClusterSnapshot Snapshot(string cluster)
        {
            var range = Etcd.RangeAsync("", "/valkey/clusters/", TestContext.Current.CancellationToken)
                .GetAwaiter().GetResult();
            return ValkeySnapshotParser.Parse(range.Value).Value.Clusters.First(c => c.Cluster == cluster);
        }
    }

    [Fact]
    public async Task РасхождениеMaxmemory_ConfigSetВызван()
    {
        // Arrange: живая нода держит старое значение maxmemory.
        const string cluster = "cfg";
        var rig = new Rig();
        rig.SeedActive(cluster);
        rig.Valkey.Config["maxmemory"] = "1";
        rig.Valkey.Config["maxmemory-policy"] = "allkeys-lru";

        // Act
        var result = await rig.Converger.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: CONFIG SET maxmemory=536870912.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Valkey.Config["maxmemory"].Should().Be("536870912");
    }

    [Fact]
    public async Task СовпадениеКонфига_Пусто()
    {
        // Arrange: конфиг ноды уже канон.
        const string cluster = "same";
        var rig = new Rig();
        rig.SeedActive(cluster);
        rig.Valkey.Config["maxmemory"] = "536870912";
        rig.Valkey.Config["maxmemory-policy"] = "allkeys-lru";

        // Act
        var result = await rig.Converger.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: без CONFIG SET.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
    }

    [Fact]
    public async Task ПраваАppБезRead_SETUSERВосстанавливает()
    {
        // Arrange: app без +@read (ACL-дрейф), admin канон.
        const string cluster = "acl";
        var rig = new Rig();
        rig.SeedActive(cluster);
        rig.Valkey.AddUser("app", "AppPassword0123456789abcdef12345", "~*", "+@write");

        // Act
        var result = await rig.Converger.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: ACL SETUSER app c +@read вызван.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var call = rig.Valkey.SetUserCalls.Should().Contain(c => c.User == "app").Subject;
        call.Args.Should().Contain("+@read");
    }

    [Fact]
    public async Task ВключённыйDefault_КонвергеОтключает()
    {
        // Arrange: default включён (дрейф от канона «default off», arch/21 §5 D).
        const string cluster = "defon";
        var rig = new Rig();
        rig.SeedActive(cluster);
        rig.Valkey.Config["maxmemory"] = "536870912";
        rig.Valkey.Config["maxmemory-policy"] = "allkeys-lru";
        rig.Valkey.AddUser("default", "", "~*", "+@all");

        // Act
        var result = await rig.Converger.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: ACL SETUSER default off вызван, пользователь выключен.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var call = rig.Valkey.SetUserCalls.Should().Contain(c => c.User == "default").Subject;
        call.Args.Should().Contain("off");
        rig.Valkey.Users["default"].On.Should().BeFalse();
    }

    [Fact]
    public async Task ВыключенныйApp_КонвергеВключаетСПравами()
    {
        // Arrange: app выключен (права на месте — пользователи всё равно не работают).
        const string cluster = "appoff";
        var rig = new Rig();
        rig.SeedActive(cluster);
        rig.Valkey.Config["maxmemory"] = "536870912";
        rig.Valkey.Config["maxmemory-policy"] = "allkeys-lru";
        var app = rig.Valkey.AddUser("app", "AppPassword0123456789abcdef12345", "~*", "+@read", "+@write");
        app.On = false;

        // Act
        var result = await rig.Converger.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: SETUSER app несёт on (права доливаются планом).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var call = rig.Valkey.SetUserCalls.Should().Contain(c => c.User == "app").Subject;
        call.Args.Should().Contain("on");
        rig.Valkey.Users["app"].On.Should().BeTrue();
    }

    [Fact]
    public async Task MaxmemoryГеMem_JournalWarningR3()
    {
        // Arrange: maxmemory_bytes == mem-лимита (1Gi) — инвариант нарушен.
        const string cluster = "warn";
        var rig = new Rig();
        rig.SeedActive(cluster);
        rig.Etcd.Store["/valkey/clusters/warn/config"] =
            new Fakes.FakeEtcd.Entry(
                """{"nodes":1,"maxmemory_bytes":1073741824,"maxmemory_policy":"allkeys-lru","created_unix":1756500000}""",
                10, 1);
        rig.Valkey.Config["maxmemory"] = "1073741824";
        rig.Valkey.Config["maxmemory-policy"] = "allkeys-lru";

        // Act
        var result = await rig.Converger.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: тик успех + journal warning-maxmemory-mem.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        var work = rig.Etcd.Store["/valkeyworker/work/warn"].Value;
        work.Should().Contain("warning-maxmemory-mem");
    }

    [Fact]
    public async Task СлепаяНода_Failed()
    {
        // Arrange: нода не отвечает на соединение.
        const string cluster = "blind";
        var rig = new Rig();
        rig.SeedActive(cluster);
        rig.Valkey.ConnectionFault = true;

        // Act
        var result = await rig.Converger.TickAsync(rig.Snapshot(cluster), TestContext.Current.CancellationToken);

        // Assert: Failed (тик повторится).
        result.IsSuccess.Should().BeFalse();
    }
}
