using FluentAssertions;
using S3Worker.Core.Model;
using S3Worker.Core.OwnS3;
using S3Worker.Docker.Drivers;
using S3Worker.Provisioning;
using S3Worker.Provisioning.Processes;
using Xunit;

namespace S3Worker.UnitTests.Provisioning;

// Rolling-ротация (arch/23 §6, arch/24 §4): заявка нет → no-op; secrets
// отсутствуют/битые → journal-отказ (invalid-secrets), заявка жива; проигрыш
// RMW-замены secrets → waiting-secrets (заявка жива, повтор следующим тиком);
// по одной ноде за тик (wait: неоживлённая держит следующую); все приведены →
// del заявки + done; секреты не в журнале.
public class SecretRotatorTests
{
    private const string Prefix = "/s3worker";
    private const string OldUser = "old-root";
    private const string NewUser = "new-root";

    // getenv-профиль store'а: спец-ключ etcd-backup (тестами не используется).
    private static readonly Dictionary<string, string?> Env = [];

    private sealed class Rig
    {
        public Fakes.FakeEtcd Etcd = new();
        public Fakes.FakeS3Driver Driver = new();
        public ClusterSecretsStore Secrets = null!;
        public ClaimStore Claims = null!;
        public SecretRotator Rotator = null!;

        // root_user secrets кластера: ротация его СОХРАНЯЕТ — ноды после
        // ротации сходятся на этом пользователе.
        private readonly string _rootUser;

        public Rig(string rootUser)
        {
            _rootUser = rootUser;
            Secrets = new ClusterSecretsStore(
                Etcd, ["http://etcd:2379"], env => env is null ? null : Env.GetValueOrDefault(env));
        }

        public static Rig New(string rootUser = NewUser)
        {
            var rig = new Rig(rootUser);
            rig.Claims = new ClaimStore(Prefix, ["http://etcd:2379"], rig.Etcd, TimeProvider.System);
            rig.Rotator = new SecretRotator(
                rig.Etcd, ["http://etcd:2379"], rig.Driver, rig.Claims,
                new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"]),
                rig.Secrets,
                new PkiStore(rig.Etcd, ["http://etcd:2379"], TimeProvider.System),
                new InstanceEnvComposer(),
                new Fakes.FakeNodeClient(),
                new S3ProvisioningOptions(18000, 18999, 100, 90, 15, null, "owns3:dev", "pgworker-backups"));
            return rig;
        }

        // Active-кластер N=2 + живая заявка ротации.
        public async Task SeedAsync(
            string node1State = S3NodeStates.Running, string node2State = S3NodeStates.Running,
            bool withTicket = true, string envUser = OldUser,
            bool withSecrets = true, string rootPassword = "root-password-fixed")
        {
            Etcd.Seed("/s3/clusters/demo/config",
                """{"platform":"owns3","nodes":2,"created_unix":1760156800}""");
            if (withSecrets)
                Etcd.Seed("/s3/clusters/demo/secrets", ClusterSecretsStore.Serialize(new ClusterSecrets(
                    _rootUser, rootPassword,
                    [
                        new ClusterAccessKey("backups", "backups-ak-fixed", "backups-sk-fixed", "read-write"),
                        new ClusterAccessKey("panel", "panel-ak-fixed", "panel-sk-fixed", "read-only"),
                    ])));
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
            new S3ClusterConfig("owns3", 2, null, 1760156800, null),
            [
                new S3NodeSnapshot("node1", node1State, "h1", null,
                    new S3NodeResources("1", "1Gi", "5Gi"), null, null),
                new S3NodeSnapshot("node2", node2State, "h1", null,
                    new S3NodeResources("1", "1Gi", "5Gi"), null, null),
            ],
            null, null);

        // env ноды «на secrets кластера»: root-пользователь + ОБА key-индекса
        // (сверка ротатора — по root user и составу индексов, значения не
        // сверяются).
        public void MarkNodeEnvConverged(string node)
            => Driver.Envs[$"demo/{node}"] = new Dictionary<string, string>
            {
                ["OWNS3_ROOT_USER"] = _rootUser,
                ["OWNS3_DATA_DIR"] = "/data",
                ["OWNS3_ACCESS_KEYS__0__ACCESSKEY"] = "key0",
                ["OWNS3_ACCESS_KEYS__0__SECRETKEY"] = "secret0",
                ["OWNS3_ACCESS_KEYS__0__POLICY"] = "read-write",
                ["OWNS3_ACCESS_KEYS__1__ACCESSKEY"] = "key1",
                ["OWNS3_ACCESS_KEYS__1__SECRETKEY"] = "secret1",
                ["OWNS3_ACCESS_KEYS__1__POLICY"] = "read-only",
            };
    }

    // AAA: заявки нет → no-op (ветка не активна).
    [Fact]
    public async Task NoTicket_NoOp()
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

    // AAA: secrets-ключа нет → journal-отказ (постоянный), заявка жива,
    // пересозданий нет.
    [Fact]
    public async Task MissingSecrets_JournalFailure_TicketStays()
    {
        // Arrange: ключ /s3/clusters/demo/secrets не записан.
        var rig = Rig.New();
        await rig.SeedAsync(withSecrets: false);

        // Act
        var result = await rig.Rotator.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);

        // Assert: Success-грань тика (отказ постоянный в журнале), заявка жива,
        // docker не тронут.
        result.IsSuccess.Should().BeTrue();
        rig.Etcd.Store.Should().ContainKey($"{Prefix}/rotations/demo");
        rig.Driver.Ensured.Should().BeEmpty();
        var journal = await new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"])
            .ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("invalid-secrets");
        journal.Value.LastError.Should().Contain("secrets /s3/clusters/demo/secrets not found");
    }

    // AAA: RMW-проигрыш — secrets изменены под нами (хук txn перезаписывает
    // ключ до compare) → journal waiting-secrets, заявка жива: следующий тик
    // повторит ротацию.
    [Fact]
    public async Task KeyChangedUnderneath_WaitingSecrets_TicketStays()
    {
        // Arrange: secrets выпущены; хук перезаписывает secrets-ключ ДО
        // compare — ModRevisionEqual ломается (гонка RMW).
        var rig = Rig.New();
        await rig.SeedAsync();
        rig.Etcd.OnTxnBeforeCompare = _ => rig.Etcd.Seed("/s3/clusters/demo/secrets", "clobbered");

        // Act
        var result = await rig.Rotator.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);

        // Assert: waiting-secrets; заявка НЕ погашена; docker не тронут.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store.Should().ContainKey($"{Prefix}/rotations/demo");
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Driver.Removed.Should().BeEmpty();
        var journal = await new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"])
            .ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("waiting-secrets");
    }

    // AAA: тик 1 — node1 (Ordinal-первая с чужим env) пересоздана; одна за тик.
    [Fact]
    public async Task Tick1_OneNode()
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

    // AAA: ротатор САМ живит пересозданную ноду (health OK → RUNNING) и
    // переходит к следующей: node1 доведена, node2 пересоздана (одно
    // пересоздание за тик; живление — etcd-запись, не docker-мутация).
    [Fact]
    public async Task Tick2_Revived_AdvancesToNext()
    {
        // Arrange: после тика 1 — node1 PROVISIONING, env уже новый;
        // node2 всё ещё с чужим env; health-грань отвечает OK.
        var rig = Rig.New();
        await rig.SeedAsync();
        await rig.Rotator.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);
        rig.MarkNodeEnvConverged("node1");

        // Act
        var result = await rig.Rotator.TickAsync(
            rig.Snap(node1State: S3NodeStates.Provisioning), TestContext.Current.CancellationToken);

        // Assert: node1 → RUNNING (оживлена ротатором), node2 пересоздана
        // (одно пересоздание за тик), заявка ещё жива.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Etcd.Store["/s3/clusters/demo/nodes/node1/state"].Value.Should().Be(S3NodeStates.Running);
        rig.Driver.Removed.Should().BeEquivalentTo(["demo/node1", "demo/node2"]);
        rig.Etcd.Store.Should().ContainKey($"{Prefix}/rotations/demo");
        var journal = await new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"])
            .ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("rotated-node2");
    }

    // AAA: wait-семантика — node1 пересоздана, health молчит (не ожила):
    // тик 2 НЕ трогает node2 (rolling: ожидание, не параллельный снос).
    [Fact]
    public async Task Tick2_NotRevived_HoldsNext()
    {
        // Arrange: после тика 1 — node1 PROVISIONING (не RUNNING), env уже новый;
        // node2 всё ещё с чужим env; health-грань молчит.
        var rig = Rig.New();
        var nodes = new Fakes.FakeNodeClient { Health = false };
        rig.Rotator = new SecretRotator(
            rig.Etcd, ["http://etcd:2379"], rig.Driver, rig.Claims,
            new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"]),
            rig.Secrets,
            new PkiStore(rig.Etcd, ["http://etcd:2379"], TimeProvider.System),
            new InstanceEnvComposer(),
            nodes,
            new S3ProvisioningOptions(18000, 18999, 100, 90, 15, null, "owns3:dev", "pgworker-backups"));
        await rig.SeedAsync();
        await rig.Rotator.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);
        rig.MarkNodeEnvConverged("node1");

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
    public async Task AllConverged_TicketCleared()
    {
        // Arrange: env обеих нод — на secrets кластера (root user + оба
        // key-индекса), обе RUNNING.
        var rig = Rig.New();
        await rig.SeedAsync();
        rig.MarkNodeEnvConverged("node1");
        rig.MarkNodeEnvConverged("node2");

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

    // AAA: env-сверка ротатора по индексам OWNS3_ACCESS_KEYS__<i>__* (второй
    // сегмент): нода на произвольных ЗНАЧЕНИЯХ обоих индексов — matches, без
    // пересозданий (регрессия: Split("__")[2] давал имя поля вместо индекса —
    // сверка всегда false, пересоздание каждый тик).
    [Fact]
    public async Task AccessKeyIndexes_IndexBasedMatch_NoRecreations()
    {
        // Arrange: env обеих нод — root user кластера + оба key-индекса;
        // значения не совпадают с secrets — сверка по составу индексов.
        var rig = Rig.New();
        await rig.SeedAsync();
        foreach (var node in new[] { "node1", "node2" })
        {
            rig.Driver.Envs[$"demo/{node}"] = new Dictionary<string, string>
            {
                ["OWNS3_ROOT_USER"] = NewUser,
                ["OWNS3_DATA_DIR"] = "/data",
                ["OWNS3_ACCESS_KEYS__0__ACCESSKEY"] = "stale-ak-0",
                ["OWNS3_ACCESS_KEYS__0__SECRETKEY"] = "stale-sk-0",
                ["OWNS3_ACCESS_KEYS__0__POLICY"] = "read-write",
                ["OWNS3_ACCESS_KEYS__1__ACCESSKEY"] = "stale-ak-1",
                ["OWNS3_ACCESS_KEYS__1__SECRETKEY"] = "stale-sk-1",
                ["OWNS3_ACCESS_KEYS__1__POLICY"] = "read-only",
            };
        }

        // Act
        var result = await rig.Rotator.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);

        // Assert: env совпадает — пересозданий нет, заявка погашена, done.
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        rig.Driver.Ensured.Should().BeEmpty();
        rig.Etcd.Store.Should().NotContainKey($"{Prefix}/rotations/demo");
        var journal = await new WorkJournal(Prefix, rig.Etcd, ["http://etcd:2379"])
            .ReadAsync("demo", TestContext.Current.CancellationToken);
        journal.Value!.Phase.Should().Be("done");
    }

    // AAA: значения секретов не появляются в journal-фазах.
    [Fact]
    public async Task SecretsNotInJournal()
    {
        // Arrange: пароль «суперсекрет» в secrets кластера; ноды с чужим env.
        const string password = "supersecret-value";
        var rig = Rig.New();
        await rig.SeedAsync(rootPassword: password);

        // Act
        await rig.Rotator.TickAsync(rig.Snap(), TestContext.Current.CancellationToken);

        // Assert: во всей etcd-записи журнала пароля нет.
        rig.Etcd.Store[$"{Prefix}/work/demo"].Value.Should().NotContain(password);
    }
}
