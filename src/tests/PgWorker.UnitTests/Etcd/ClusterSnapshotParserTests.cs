using PgWorker.Core.Model;
using PgWorker.Etcd.Client;
using PgWorker.Etcd.Parsing;
using Xunit;

namespace PgWorker.UnitTests.Etcd;

// Парсер /clusters/ и /service/ в доменную модель PgWorker (задача 11, spec §4.1).
public class ClusterSnapshotParserTests
{
    [Fact]
    public void ParseClusters_PanelSeed_NotInitializedCluster()
    {
        // Arrange — сид панели (02 §9.1): config NOT_INITIALIZED, 2 шарда, nodes, routing+status
        var kvs = EtcdFixtures.LoadKv("clusters-provisioning.json");

        // Act
        var result = ClusterSnapshotParser.ParseClusters(kvs, out var errors);

        // Assert
        result.IsSuccess.Should().BeTrue();
        errors.Should().BeEmpty();
        var snap = result.Value.Should().ContainSingle().Subject;
        snap.Config.Cluster.Should().Be("shop");
        snap.Config.DbName.Should().Be("shop");
        snap.Config.Buckets.Should().Be(6);
        snap.Config.State.Should().Be(ClusterState.NotInitialized);
        snap.Shards.Should().HaveCount(2);
        var shard1 = snap.Shards.Should().Contain(s => s.Name == "shard1").Subject;
        shard1.Replicas.Should().Be(2);
        shard1.Nodes.Should().HaveCount(2);
        shard1.Nodes.Should().Contain(n => n.Name == "shard1a" && n.State == NodeState.NotInitialized);
        shard1.Nodes.Should().Contain(n => n.Name == "shard1b" && n.State == NodeState.NotInitialized);
        snap.Routing.Should().HaveCount(6); // все N бакетов из config.buckets
        snap.Routing.Should().Contain(r => r.Id == 0 && r.Owner == "shard1" && r.Status == BucketMoveState.NotInitialized);
        snap.Routing.Should().Contain(r => r.Id == 5 && r.Owner == "shard2");
    }

    [Fact]
    public void ParseClusters_ConfigWithoutState_IsActive()
    {
        // Arrange — clusters-full: config демо-кластера без поля state
        var kvs = EtcdFixtures.LoadKv("clusters-full.json");

        // Act
        var result = ClusterSnapshotParser.ParseClusters(kvs, out _);

        // Assert: отсутствие state = Active (контракт панели 02 §2.1)
        result.IsSuccess.Should().BeTrue();
        var demo = result.Value.Should().Contain(c => c.Config.Cluster == "demo").Subject;
        demo.Config.State.Should().Be(ClusterState.Active);
        demo.Config.Buckets.Should().Be(16);
        demo.Config.CreatedUnix.Should().Be(1755800000);
        var s1 = demo.Shards.Should().Contain(s => s.Name == "s1").Subject;
        s1.Dsn.Should().Be("host=s1a,s1b port=5432 dbname=demo user=postgres");
        s1.Master.Should().Be("s1a:5432");
        s1.Replicas.Should().Be(1);
    }

    [Fact]
    public void ParseClusters_BrokenJson_GoesToErrors_OthersAlive()
    {
        // Arrange — degenerate-фикстура + кластер с реально битым JSON config
        var kvs = EtcdFixtures.LoadKv("clusters-degenerate.json")
            .Concat([new Kv("/clusters/badjson/config", "{\"buckets\":", 99)])
            .ToList();

        // Act
        var result = ClusterSnapshotParser.ParseClusters(kvs, out var errors);

        // Assert: битые ключи — в parseErrors, живые кластеры (demo2) парсятся
        result.IsSuccess.Should().BeTrue();
        errors.Should().NotBeEmpty();
        errors.Should().Contain(e => e.Contains("/clusters/badjson/config"));
        errors.Should().Contain(e => e.Contains("/clusters/broken/shards/x1/replicas"));
        var demo2 = result.Value.Should().Contain(c => c.Config.Cluster == "demo2").Subject;
        demo2.Routing.Should().Contain(r => r.Id == 0 && r.Owner == "s1");
    }

    [Fact]
    public void ParseClusters_RoutingWithoutStatus_StatusNull()
    {
        // Arrange — у большинства бакетов demo нет status-ключа (= ACTIVE)
        var kvs = EtcdFixtures.LoadKv("clusters-full.json");

        // Act
        var result = ClusterSnapshotParser.ParseClusters(kvs, out _);

        // Assert
        var demo = result.Value.Should().Contain(c => c.Config.Cluster == "demo").Subject;
        demo.Routing.Should().Contain(r => r.Id == 0 && r.Owner == "s1" && r.Status == null);
        demo.Routing.Should().Contain(r => r.Id == 3 && r.Status == BucketMoveState.Syncing);
        demo.Routing.Should().Contain(r => r.Id == 7 && r.Status == BucketMoveState.Aborting);
        demo.Routing.Should().Contain(r => r.Id == 11 && r.Status == BucketMoveState.Frozen);
    }

    [Fact]
    public void ParseClusters_PgWorkerKeys_DoNotBreakParsing()
    {
        // Arrange — координационный префикс /pgworker/ попадает в общий range-снапшот
        var kvs = EtcdFixtures.LoadKv("clusters-provisioning.json").Concat(
        [
            new Kv("/pgworker/leader", """{"instance":"abc","since_unix":1}""", 200),
            new Kv("/pgworker/work/shop", """{"op":"provision","phase":"planned"}""", 201),
            new Kv("/pgworker/portalloc/shop", """{"shard1/shard1a":{"host":"h1","pg":15432}}""", 202),
        ]).ToList();

        // Act
        var result = ClusterSnapshotParser.ParseClusters(kvs, out var errors);

        // Assert: неизвестный префикс игнорируется, кластер цел
        result.IsSuccess.Should().BeTrue();
        errors.Should().BeEmpty();
        result.Value.Should().ContainSingle(c => c.Config.Cluster == "shop");
    }

    [Fact]
    public void ParseClusters_ShardStateToRemove_SetsToRemoveTrue()
    {
        // Arrange — Active-кластер с маркером демонтажа шарда (t06 §4.2)
        var kvs = new List<Kv>
        {
            new("/clusters/shop/config", """{"buckets":2,"dbname":"shop"}""", 1),
            new("/clusters/shop/shards/shard1/replicas", "2", 2),
            new("/clusters/shop/shards/shard1/state", "TO_REMOVE", 3),
            new("/clusters/shop/buckets/routing/bucket_0", "shard1", 4),
            new("/clusters/shop/buckets/routing/bucket_1", "shard1", 5),
        };

        // Act
        var result = ClusterSnapshotParser.ParseClusters(kvs, out var errors);

        // Assert — маркер прочитан; parseError нет (значение одно — толерантность)
        errors.Should().BeEmpty();
        result.Value.Single().Shards.Single().ToRemove.Should().BeTrue();
    }

    [Theory]
    [InlineData(null)] [InlineData("ACTIVE")] [InlineData("")]
    public void ParseClusters_ShardStateAbsentOrOther_ToRemoveFalse(string? raw)
    {
        // Arrange — ключа нет / иное значение = обычный шард (толерантность как у config.state)
        var kvs = new List<Kv>
        {
            new("/clusters/shop/config", """{"buckets":1,"dbname":"shop"}""", 1),
            new("/clusters/shop/shards/shard1/replicas", "1", 2),
        };
        if (raw is not null) kvs.Add(new("/clusters/shop/shards/shard1/state", raw, 3));

        // Act
        var result = ClusterSnapshotParser.ParseClusters(kvs, out _);

        // Assert
        result.Value.Single().Shards.Single().ToRemove.Should().BeFalse();
    }

    [Fact]
    public void ParseClusters_StatusOwnerAndTarget_ProduceMoveSourceAndMoveTarget()
    {
        // Arrange — «flip прошёл, статус завис» (P7/G4): routing уже на shard2,
        // статус-ключ ещё жив с owner=shard1 (статус-owner ≠ routing-owner)
        var kvs = new List<Kv>
        {
            new("/clusters/shop/config", """{"buckets":1,"dbname":"shop"}""", 1),
            new("/clusters/shop/shards/shard1/replicas", "1", 2),
            new("/clusters/shop/shards/shard2/replicas", "1", 3),
            new("/clusters/shop/buckets/routing/bucket_0", "shard2", 4),
            new("/clusters/shop/buckets/status/bucket_0",
                """{"state":"FROZEN","owner":"shard1","target":"shard2","phase":"flip"}""", 5),
        };

        // Act
        var result = ClusterSnapshotParser.ParseClusters(kvs, out _);

        // Assert — owner И target статус-ключа попадают в маршрут; routing-owner —
        // отдельно (guard G4 сравнивает X со статус-owner/target, не с routing)
        var route = result.Value.Single().Routing.Single();
        route.Owner.Should().Be("shard2");
        route.Status.Should().Be(BucketMoveState.Frozen);
        route.MoveSource.Should().Be("shard1");
        route.MoveTarget.Should().Be("shard2");
    }

    [Fact]
    public void ParseClusters_StatusNotInitialized_OwnerOnlyNoTarget()
    {
        // Arrange — начальный статус создаваемого кластера: owner есть, target нет (02 §9)
        var kvs = new List<Kv>
        {
            new("/clusters/shop/config", """{"buckets":1,"dbname":"shop"}""", 1),
            new("/clusters/shop/buckets/routing/bucket_0", "shard1", 2),
            new("/clusters/shop/buckets/status/bucket_0",
                """{"state":"NOT_INITIALIZED","owner":"shard1","updated_unix":1}""", 3),
        };

        // Act
        var result = ClusterSnapshotParser.ParseClusters(kvs, out _);

        // Assert — NOT_INITIALIZED: MoveSource = owner статуса, MoveTarget = null
        var route = result.Value.Single().Routing.Single();
        route.Status.Should().Be(BucketMoveState.NotInitialized);
        route.MoveSource.Should().Be("shard1");
        route.MoveTarget.Should().BeNull();
    }

    [Fact]
    public void ParseClusters_AppSecretKeys_FilledIntoSnapshot()
    {
        // Arrange — оба ключа app_user/app_password (spec §3.1)
        var kvs = EtcdFixtures.LoadKv("clusters-app-secret.json");

        // Act
        var result = ClusterSnapshotParser.ParseClusters(kvs, out var errors);

        // Assert
        result.IsSuccess.Should().BeTrue();
        errors.Should().BeEmpty();
        var snap = result.Value.Should().ContainSingle().Subject;
        snap.App.Should().NotBeNull();
        snap.App!.User.Should().Be("app");
        snap.App.Password.Should().Be("Kj9mP2qR7sT3vW5xYz1aBc4dEf6Gh8Jk");
        // bucket_admin-поля config не задеты (механизм сохраняется)
        snap.Config.BucketAdminUser.Should().BeNull();
        snap.Config.BucketAdminPassword.Should().BeNull();
    }

    [Fact]
    public void ParseClusters_NoAppKeys_AppIsNull()
    {
        // Arrange — кластер без app-ключей (до первого ensure)
        var kvs = EtcdFixtures.LoadKv("clusters-provisioning.json");

        // Act
        var result = ClusterSnapshotParser.ParseClusters(kvs, out _);

        // Assert
        result.Value.Single().App.Should().BeNull();
    }

    [Fact]
    public void ParseClusters_PartialAppKeys_AppIsNull()
    {
        // Arrange — только app_user без пароля (битое состояние): толерантно null
        var kvs = new List<Kv>
        {
            new("/clusters/shop/config", "{\"buckets\":1,\"dbname\":\"shop\"}", 1),
            new("/clusters/shop/app_user", "app", 2),
        };

        // Act
        var result = ClusterSnapshotParser.ParseClusters(kvs, out var errors);

        // Assert — не ошибка парсинга: ensure допишет недостающий ключ
        errors.Should().BeEmpty();
        result.Value.Single().App.Should().BeNull();
    }

    [Fact]
    public void ParseService_ScopesWithLeaderAndInitialize()
    {
        // Arrange
        var kvs = EtcdFixtures.LoadKv("service-full.json");

        // Act
        var scopes = ClusterSnapshotParser.ParseService(kvs);

        // Assert
        scopes.Should().HaveCount(2);
        var s1 = scopes.Should().Contain(s => s.Scope == "demo-s1").Subject;
        s1.Initialized.Should().BeTrue();
        s1.LeaderName.Should().Be("s1a");
        var s2 = scopes.Should().Contain(s => s.Scope == "demo-s2").Subject;
        s2.Initialized.Should().BeTrue();
        s2.LeaderName.Should().Be("s2a");
    }

    [Fact]
    public void ParseService_NoKeys_NotInitializedNoLeader()
    {
        // Arrange — scope без initialize/leader (Patroni ещё не поднялся, P2.2)
        var kvs = new List<Kv>
        {
            new("/service/shop-shard1/config", """{"ttl":5}""", 1),
            new("/service/shop-shard1/members/shard1a", """{"role":"replica","state":"starting"}""", 2),
        };

        // Act
        var scopes = ClusterSnapshotParser.ParseService(kvs);

        // Assert
        var scope = scopes.Should().ContainSingle().Subject;
        scope.Scope.Should().Be("shop-shard1");
        scope.Initialized.Should().BeFalse();
        scope.LeaderName.Should().BeNull();
    }

    // AAA: per-node app_params (spec §3.1): значение на ноду; пустое = "" (ключ есть),
    // отсутствие ключа = null (не обеспечен — фильтр миграции надзора)
    [Fact]
    public void Parse_NodeAppParams_PerNodeValueEmptyStringAndMissing()
    {
        // Arrange — ноды шарда с app_params: значение / пустое / отсутствие
        var kvs = new List<Kv>
        {
            new("/clusters/shop/config", """{"buckets":1,"dbname":"shop"}""", 1),
            new("/clusters/shop/shards/shard1/replicas", "2", 2),
            new("/clusters/shop/shards/shard1/nodes/shard1a/state", "RUNNING", 3),
            new("/clusters/shop/shards/shard1/nodes/shard1a/app_params", "sslmode=require", 4),
            new("/clusters/shop/shards/shard1/nodes/shard1b/state", "RUNNING", 5),
            new("/clusters/shop/shards/shard1/nodes/shard1b/app_params", "  ", 6),
            new("/clusters/shop/shards/shard1/nodes/shard1c/state", "RUNNING", 7),
        };

        // Act
        var result = ClusterSnapshotParser.ParseClusters(kvs, out _);

        // Assert — значение на своей ноде; whitespace → ""; нет ключа → null
        var nodes = result.Value.Single().Shards.Single().Nodes;
        nodes.Single(n => n.Name == "shard1a").AppParams.Should().Be("sslmode=require");
        nodes.Single(n => n.Name == "shard1b").AppParams.Should().Be("");
        nodes.Single(n => n.Name == "shard1c").AppParams.Should().BeNull();
    }
}
