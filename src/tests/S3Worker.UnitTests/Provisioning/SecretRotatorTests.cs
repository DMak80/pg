using FluentAssertions;
using S3Worker.Core.Model;
using S3Worker.Docker.Drivers;
using S3Worker.Provisioning;
using S3Worker.Provisioning.Processes;
using Xunit;

namespace S3Worker.UnitTests.Provisioning;

// Rolling-ротация (arch/23 §6, arch/24 §4): заявка нет → no-op; невалидный
// профиль → journal-отказ, заявка жива; по одной ноде за тик (wait: неоживлённая
// держит следующую); все приведены → del заявки + done; секреты не в журнале.
public class SecretRotatorTests
{
    private const string Prefix = "/s3worker";
    private const string OldUser = "old-root";
    private const string NewUser = "new-root";

    private sealed class Rig
    {
        public Fakes.FakeEtcd Etcd = new();
        public Fakes.FakeS3Driver Driver = new();
        public SecretProfileStore Secrets;
        public ClaimStore Claims = null!;
        public SecretRotator Rotator = null!;

        public Rig(string rootUser)
        {
            Secrets = new SecretProfileStore(
                name => name == "S3W_NODE_SECRETS__backups-main__ROOT_USER" ? rootUser
                    : name == "S3W_NODE_SECRETS__backups-main__ROOT_PASSWORD" ? "pw" : null,
                ["S3W_NODE_SECRETS__backups-main__ROOT_USER", "S3W_NODE_SECRETS__backups-main__ROOT_PASSWORD"]);
        }

        public static Rig New(string rootUser = NewUser)
        {
            var rig = new Rig(rootUser);
            rig.Claims = new ClaimStore(Prefix, ["http://etcd:2379"], rig.Etcd, TimeProvider.System);
            rig.Rotator = new SecretRotator(
                rig.Etcd, ["http://etcd:2379"], rig.Driver, rig.Claims,
                new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"]),
                rig.Secrets,
                new S3ProvisioningOptions(18000, 18999, 100, 90, 15, null, "owns3:dev", "pgworker-backups"));
            return rig;
        }

        // Active-кластер N=2 + живая заявка ротации.
        public async Task SeedAsync(
            string node1State = S3NodeStates.Running, string node2State = S3NodeStates.Running,
            bool withTicket = true, string envUser = OldUser)
        {
            Etcd.Seed("/s3/clusters/demo/config",
                """{"platform":"owns3","nodes":2,"secret_profile":"backups-main","created_unix":1760156800}""");
            Etcd.Seed("/s3/clusters/demo/nodes/node1/state", node1State);
            Etcd.Seed("/s3/clusters/demo/nodes/node2/state", node2State);
            Etcd.Seed($"{Prefix}/portalloc/demo", """{"node1":{"host":"h1","s3":18000},"node2":{"host":"h1","s3":18001}}""");
            Driver.Envs["demo/node1"] = new Dictionary<string, string>
            {
                ["OWNS3_ROOT_USER"] = envUser,
                ["OWNS3_DATA_DIR"] = "/data",
            };
            Driver.Envs["demo/node2"] = new Dictionary<string, string>
            {
                ["OWNS3_ROOT_USER"] = envUser,
                ["OWNS3_DATA_DIR"] = "/data",
            };
            Driver.Nodes["demo/node1"] = new Fakes.FakeS3Driver.NodeFact("h1", 18000);
            Driver.Nodes["demo/node2"] = new Fakes.FakeS3Driver.NodeFact("h1", 18001);
            if (withTicket)
                Etcd.Seed($"{Prefix}/rotations/demo", """{"requested_unix":1760156800,"requested_by":"api"}""");
            await Claims.TryClaimClusterAsync("demo", TestContext.Current.CancellationToken);
        }

        public S3ClusterSnapshot Snap(string node1State = S3NodeStates.Running, string node2State = S3NodeStates.Running) => new(
            "demo",
            new S3ClusterConfig("owns3", 2, "backups-main", 1760156800, null),
            [
                new S3NodeSnapshot("node1", node1State, "h1", null,
                    new S3NodeResources("1", "1Gi", "5Gi"), null, null),
                new S3NodeSnapshot("node2", node2State, "h1", null,
                    new S3NodeResources("1", "1Gi", "5Gi"), null, null),
            ],
            null, null);
    }

    // AAA: заявки нет → no-op (ветка не активна).
    [Fact]
    public async Task ЗаявкиНет_NoOp()
    {
        // Arrange
        var rig = Rig.New();
        await rig.SeedAsync(withTicket: false);

        // Act
        var result = await rig.Rotator.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);

        // Assert: Success; docker не тронут.
        result.IsSuccess.Should().BeTrue();
        rig.Driver.Ensured.Should().BeEmpty();
    }

    // AAA: невалидный профиль → journal-отказ (постоянный), заявка жива,
    // пересозданий нет.
    [Fact]
    public async Task ПрофильНевалиден_JournalОтказ_ЗаявкаЖива()
    {
        // Arrange: профиль не задан в env вовсе.
        var rig = Rig.New();
        rig.Secrets = new SecretProfileStore(_ => null, []);
        rig.Rotator = new SecretRotator(
            rig.Etcd, ["http://etcd:2379"], rig.Driver, rig.Claims,
            new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"]),
            rig.Secrets,
            new S3ProvisioningOptions(18000, 18999, 100, 90, 15, null, "owns3:dev", "pgworker-backups"));
        await rig.SeedAsync();

        // Act
        var result = await rig.Rotator.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);

        // Assert: Success-грань тика (отказ постоянный в журнале), заявка жива,
        // docker не тронут.
        result.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Should().ContainKey($"{Prefix}/rotations/demo");
        rig.Driver.Ensured.Should().BeEmpty();
        var journal = await new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"])
            .ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("invalid-secret-profile");
    }

    // AAA: тик 1 — node1 (Ordinal-первая с чужим env) пересоздана; одна за тик.
    [Fact]
    public async Task Тик1_ОднаНода()
    {
        // Arrange: обе ноды с чужим env.
        var rig = Rig.New();
        await rig.SeedAsync();

        // Act
        var result = await rig.Rotator.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);

        // Assert: node1 пересоздана (без volume-сноса), state=PROVISIONING,
        // env профиля; node2 НЕ тронута (одна за тик).
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Driver.Removed.Should().BeEquivalentTo(["demo/node1"]);
        rig.Driver.Ensured.Should().ContainSingle()
            .Which.NodeName.Should().Be("node1");
        rig.Driver.Ensured[0].Env!["OWNS3_ROOT_USER"].Should().Be(NewUser);
        rig.Etcd.Store["/s3/clusters/demo/nodes/node1/state"].Value.Should().Be(S3NodeStates.Provisioning);
        var journal = await new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"])
            .ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("rotated-node1");
        journal.Value.LastError.Should().BeNull();
    }

    // AAA: wait-семантика — node1 пересоздана, но не RUNNING: тик 2 НЕ трогает
    // node2 (rolling: ожидание, не параллельный снос).
    [Fact]
    public async Task Тик2_НеоживлённаяДержитСледующую()
    {
        // Arrange: после тика 1 — node1 PROVISIONING (не RUNNING), env уже новый;
        // node2 всё ещё с чужим env.
        var rig = Rig.New();
        await rig.SeedAsync();
        await rig.Rotator.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);
        rig.Driver.Envs["demo/node1"] = new Dictionary<string, string>
        {
            ["OWNS3_ROOT_USER"] = NewUser,
            ["OWNS3_DATA_DIR"] = "/data",
        };

        // Act
        var result = await rig.Rotator.TickAsync(
            rig.Snap(node1State: S3NodeStates.Provisioning), TestContext.Current.CancellationToken);

        // Assert: waiting-node; node2 не тронута.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Driver.Removed.Should().BeEquivalentTo(["demo/node1"]);
        rig.Driver.Ensured.Should().ContainSingle();
        var journal = await new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"])
            .ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("waiting-node");
    }

    // AAA: все ноды приведены → del заявки + journal done.
    [Fact]
    public async Task ВсеПриведены_ЗаявкаГасится()
    {
        // Arrange: env обеих нод — новый профиль, обе RUNNING.
        var rig = Rig.New();
        await rig.SeedAsync(envUser: NewUser);

        // Act
        var result = await rig.Rotator.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);

        // Assert: заявка удалена; journal done; docker не тронут.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store.Should().NotContainKey($"{Prefix}/rotations/demo");
        rig.Driver.Ensured.Should().BeEmpty();
        var journal = await new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"])
            .ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("done");
    }

    // AAA: значения секретов не появляются в journal-фазах.
    [Fact]
    public async Task СекретыНеВЖурнале()
    {
        // Arrange: пароль «суперсекрет» в env воркера; ноды с чужим env.
        const string password = "суперсекрет-значение";
        var rig = Rig.New();
        rig.Secrets = new SecretProfileStore(
            name => name == "S3W_NODE_SECRETS__backups-main__ROOT_USER" ? NewUser
                : name == "S3W_NODE_SECRETS__backups-main__ROOT_PASSWORD" ? password : null,
            ["S3W_NODE_SECRETS__backups-main__ROOT_USER", "S3W_NODE_SECRETS__backups-main__ROOT_PASSWORD"]);
        rig.Rotator = new SecretRotator(
            rig.Etcd, ["http://etcd:2379"], rig.Driver, rig.Claims,
            new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"]),
            rig.Secrets,
            new S3ProvisioningOptions(18000, 18999, 100, 90, 15, null, "owns3:dev", "pgworker-backups"));
        await rig.SeedAsync();

        // Act
        await rig.Rotator.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);

        // Assert: во всей etcd-записи журнала пароля нет.
        rig.Etcd.Store[$"{Prefix}/work/demo"].Value.Should().NotContain(password);
    }
}
