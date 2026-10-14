using S3Worker.Core.Model;
using S3Worker.Etcd.Parsing;
using Shared.Etcd.Client;

namespace S3Worker.UnitTests.Etcd;

// S3SnapshotParser — канонические примеры значений arch/23 §2.1 (критерий
// приёмки парсеров), unknownKeys-толерантность, битый JSON → parseError без
// падения тика, state-семантика «отсутствие = Active» (arch/23 §2/§9).
public class S3SnapshotParserTests
{
    private static Kv Kv(string key, string value) => new(key, value, 1);

    [Fact]
    public void Parse_ConfigRequest_CanonicalValues()
    {
        // Arrange — канонический пример заявки arch/23 §2.1 (один в один)
        var kvs = new[] { Kv("/s3/clusters/backups/config",
            """{"platform":"owns3","nodes":3,"secret_profile":"backups-main","created_unix":1760156800,"state":"NOT_INITIALIZED"}""") };

        // Act
        var result = S3SnapshotParser.Parse(kvs);

        // Assert — значения совпали с каноном
        result.IsSuccess.Should().BeTrue();
        var cluster = result.Value.Clusters.Single();
        cluster.Config.Should().BeEquivalentTo(new S3ClusterConfig(
            "owns3", 3, "backups-main", 1760156800, "NOT_INITIALIZED"));
    }

    [Fact]
    public void Parse_ConfigActive_NoStateMeansActive()
    {
        // Arrange — Active-пример arch/23 §2.1: то же без state
        var kvs = new[] { Kv("/s3/clusters/backups/config",
            """{"platform":"owns3","nodes":3,"secret_profile":"backups-main","created_unix":1760156800}""") };

        // Act
        var snap = S3SnapshotParser.Parse(kvs).Value;

        // Assert — State null = Active
        snap.Clusters.Single().Config!.State.Should().BeNull();
    }

    [Fact]
    public void Parse_ConfigToRemove()
    {
        // Arrange — заявка удаления: Active + "state":"TO_REMOVE"
        var kvs = new[] { Kv("/s3/clusters/backups/config",
            """{"platform":"owns3","nodes":3,"secret_profile":"backups-main","created_unix":1760156800,"state":"TO_REMOVE"}""") };

        // Act
        var snap = S3SnapshotParser.Parse(kvs).Value;

        // Assert
        snap.Clusters.Single().Config!.State.Should().Be("TO_REMOVE");
    }

    [Fact]
    public void Parse_PlacementVariants_Canonical()
    {
        // Arrange — канонические селекторы arch/23 §2.1: plain / swarm_node / swarm_labels
        var kvs = new[]
        {
            Kv("/s3/clusters/c/nodes/node1/placement", """{"docker_host":"h1"}"""),
            Kv("/s3/clusters/c/nodes/node2/placement", """{"swarm_node":"n1"}"""),
            Kv("/s3/clusters/c/nodes/node3/placement", """{"swarm_labels":{"geo":"dc1","tier":"s3"}}"""),
        };

        // Act
        var snap = S3SnapshotParser.Parse(kvs).Value;

        // Assert — конъюнкция атрибутов отражена
        var nodes = snap.Clusters.Single().Nodes;
        nodes[0].Placement!.DockerHost.Should().Be("h1");
        nodes[0].Placement!.SwarmNode.Should().BeNull();
        nodes[1].Placement!.SwarmNode.Should().Be("n1");
        nodes[2].Placement!.SwarmLabels.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            ["geo"] = "dc1",
            ["tier"] = "s3",
        });
    }

    [Fact]
    public void Parse_NodeKeys_Canonical()
    {
        // Arrange — resources/state/placed_on/usage/endpoints по arch/23 §2.1
        var kvs = new[]
        {
            Kv("/s3/clusters/c/nodes/node1/resources", """{"cpu":"2","mem":"4Gi","disk":"200Gi"}"""),
            Kv("/s3/clusters/c/nodes/node1/state", "RUNNING"),
            Kv("/s3/clusters/c/nodes/node1/placed_on", "h1"),
            Kv("/s3/clusters/c/nodes/node1/usage",
                """{"used_bytes":107374182400,"total_bytes":214748364800,"updated_unix":1760156860}"""),
            Kv("/s3/clusters/c/endpoints", "h1:18001,h2:18002,h3:18003"),
        };

        // Act
        var snap = S3SnapshotParser.Parse(kvs).Value;
        var cluster = snap.Clusters.Single();

        // Assert
        var node = cluster.Nodes.Single();
        node.State.Should().Be("RUNNING");
        node.PlacedOn.Should().Be("h1");
        node.Resources.Should().BeEquivalentTo(new S3NodeResources("2", "4Gi", "200Gi"));
        node.Usage.Should().BeEquivalentTo(new S3NodeUsage(107374182400, 214748364800, 1760156860));
        cluster.Endpoints.Should().Be("h1:18001,h2:18002,h3:18003");
    }

    [Fact]
    public void Parse_ConnectivitySync_НеUnknown_КластерЦел()
    {
        // Arrange — ключи наблюдения зонда t33 (arch/23 §3) и вовсе неизвестный
        var kvs = new[]
        {
            Kv("/s3/clusters/c/config", """{"platform":"owns3","nodes":1,"secret_profile":"p","created_unix":1}"""),
            Kv("/s3/clusters/c/connectivity", """{"state":"OK","checked_unix":1}"""),
            Kv("/s3/clusters/c/sync", """{"state":"SYNCED","checked_unix":1}"""),
            Kv("/s3/whatever", "x"),
        };

        // Act
        var snap = S3SnapshotParser.Parse(kvs).Value;
        var cluster = snap.Clusters.Single();

        // Assert — connectivity/sync — известные листы (НЕ unknownKeys, счётчик 0);
        // снапшот кластера строится; прочий неизвестный — по-прежнему unknownKeys
        snap.UnknownKeys.Should().BeEquivalentTo("/s3/whatever");
        cluster.Config!.Platform.Should().Be("owns3");
        cluster.ParseError.Should().BeNull();
    }

    [Fact]
    public void Parse_BrokenConfig_ParseErrorWithoutThrow()
    {
        // Arrange — битый JSON config
        var kvs = new[] { Kv("/s3/clusters/c/config", "{not json") };

        // Act
        var result = S3SnapshotParser.Parse(kvs);

        // Assert — Success с ParseError; тик не падает
        result.IsSuccess.Should().BeTrue();
        var cluster = result.Value.Clusters.Single();
        cluster.ParseError.Should().NotBeNull();
        cluster.Config.Should().BeNull();
    }

    [Fact]
    public void Parse_BrokenNodeJson_NodeParseError_ClusterAlive()
    {
        // Arrange — битый usage и placement у одной ноды, config цел
        var kvs = new[]
        {
            Kv("/s3/clusters/c/config", """{"platform":"owns3","nodes":2,"secret_profile":"p","created_unix":1}"""),
            Kv("/s3/clusters/c/nodes/node1/usage", "{bad"),
            Kv("/s3/clusters/c/nodes/node2/placement", "{bad"),
        };

        // Act
        var snap = S3SnapshotParser.Parse(kvs).Value;
        var cluster = snap.Clusters.Single();

        // Assert — ноды с parseError, кластер жив
        cluster.ParseError.Should().BeNull();
        cluster.Nodes.First(n => n.Node == "node1").ParseError.Should().NotBeNull();
        cluster.Nodes.First(n => n.Node == "node1").Usage.Should().BeNull();
        cluster.Nodes.First(n => n.Node == "node2").ParseError.Should().NotBeNull();
        cluster.Nodes.First(n => n.Node == "node2").Placement.Should().BeNull();
    }

    [Fact]
    public void Parse_NodesSortedByNumberOrdinal()
    {
        // Arrange — ноды в «перепутанном» порядке ключей
        var kvs = new[]
        {
            Kv("/s3/clusters/c/nodes/node3/state", "RUNNING"),
            Kv("/s3/clusters/c/nodes/node1/state", "RUNNING"),
            Kv("/s3/clusters/c/nodes/node2/state", "RUNNING"),
        };

        // Act
        var snap = S3SnapshotParser.Parse(kvs).Value;

        // Assert — сортировка node1, node2, node3
        snap.Clusters.Single().Nodes.Select(n => n.Node)
            .Should().ContainInOrder("node1", "node2", "node3");
    }

    [Fact]
    public void Parse_ForeignPrefix_Ignored()
    {
        // Arrange — чужие префиксы (s3worker, pg)
        var kvs = new[]
        {
            Kv("/s3worker/leader", "x"),
            Kv("/pgworker/something", "x"),
        };

        // Act
        var snap = S3SnapshotParser.Parse(kvs).Value;

        // Assert — пусто
        snap.Clusters.Should().BeEmpty();
        snap.UnknownKeys.Should().BeEmpty();
    }

    [Fact]
    public void Parse_MissingConfig_ConfigNull_ClassifierSkips()
    {
        // Arrange — только placed_on ноды, config-ключа нет
        var kvs = new[] { Kv("/s3/clusters/c/nodes/node1/placed_on", "h1") };

        // Act
        var snap = S3SnapshotParser.Parse(kvs).Value;

        // Assert — Config null (нет ключа — не ошибка; классификатор: Skip)
        var cluster = snap.Clusters.Single();
        cluster.Config.Should().BeNull();
        cluster.ParseError.Should().BeNull();
    }

    [Fact]
    public void Parse_LinksDenyListPki_CanonicalValues()
    {
        // Arrange — канонические примеры arch/23 §2.1: частичный граф links,
        // denylist ["west1"], pki-ключи (сертификат — экранированная JSON-строка).
        var nodePki = """{"certificate":"{\"name\":\"east1\",\"publicKeyPem\":\"-----BEGIN PUBLIC KEY-----…\",\"issuedAtUnixMs\":1760156800000,\"expiresAtUnixMs\":0,\"caName\":\"backups\",\"signature\":\"…\"}","private_key_pem":"-----BEGIN PRIVATE KEY-----…"}""";
        var kvs = new[]
        {
            Kv("/s3/clusters/backups/config",
                """{"platform":"owns3","nodes":0,"secret_profile":"backups-main","created_unix":1760156800}"""),
            Kv("/s3/clusters/backups/links",
                """[{"a":"east1","b":"east2"},{"a":"east1","b":"west1"}]"""),
            Kv("/s3/clusters/backups/denylist", """["west1"]"""),
            Kv("/s3/clusters/backups/nodes/east1/pki", nodePki),
            Kv("/s3/clusters/backups/pki/ca",
                """{"name":"backups","publicKeyPem":"-----BEGIN PUBLIC KEY-----…","private_key_pem":"-----BEGIN PRIVATE KEY-----…"}"""),
            Kv("/s3/clusters/backups/pki/orchestrator",
                """{"certificate":"{\"name\":\"orchestrator\",\"publicKeyPem\":\"-----BEGIN PUBLIC KEY-----…\",\"issuedAtUnixMs\":1760156800000,\"expiresAtUnixMs\":0,\"caName\":\"backups\",\"signature\":\"…\"}","private_key_pem":"-----BEGIN PRIVATE KEY-----…"}"""),
        };

        // Act
        var snap = S3SnapshotParser.Parse(kvs).Value;
        var cluster = snap.Clusters.Single();

        // Assert — модель по канону; config nodes=0 читается; pki в unknownKeys НЕ попал
        cluster.Config!.Nodes.Should().Be(0);
        cluster.Links.Should().BeEquivalentTo(new[]
        {
            new S3ReplicationLink("east1", "east2"),
            new S3ReplicationLink("east1", "west1"),
        });
        cluster.DenyList.Should().BeEquivalentTo("west1");
        cluster.ParseError.Should().BeNull();
        snap.UnknownKeys.Should().BeEmpty();
    }

    [Fact]
    public void Parse_EmptyLinksAndDenyList_EmptyDeclarations()
    {
        // Arrange — пустой граф/отзыв: валидная декларация (полный разрыв/нет отзывов)
        var kvs = new[]
        {
            Kv("/s3/clusters/c/links", "[]"),
            Kv("/s3/clusters/c/denylist", "[]"),
        };

        // Act
        var cluster = S3SnapshotParser.Parse(kvs).Value.Clusters.Single();

        // Assert — пустые (не null): ключ есть, декларация «нет связей/отзывов»
        cluster.Links.Should().NotBeNull().And.BeEmpty();
        cluster.DenyList.Should().NotBeNull().And.BeEmpty();
        cluster.ParseError.Should().BeNull();
    }

    [Fact]
    public void Parse_BrokenLinksAndDenyList_ParseError_NullValues_NoThrow()
    {
        // Arrange — битые значения (ручная правка etcd, arch/23 §9)
        var kvs = new[]
        {
            Kv("/s3/clusters/c/config", """{"platform":"owns3","nodes":1,"secret_profile":"p","created_unix":1}"""),
            Kv("/s3/clusters/c/links", "{not json"),
            Kv("/s3/clusters/c/denylist", """["ok", 42]"""),
        };

        // Act
        var result = S3SnapshotParser.Parse(kvs);

        // Assert — Success c ParseError; Links/DenyList null; тик (парсер) не падает
        result.IsSuccess.Should().BeTrue();
        var cluster = result.Value.Clusters.Single();
        cluster.Links.Should().BeNull();
        cluster.DenyList.Should().BeNull();
        cluster.ParseError.Should().Contain("/links: битый JSON");
        cluster.ParseError.Should().Contain("/denylist: битый JSON");
        cluster.Config.Should().NotBeNull();
    }

    [Fact]
    public void Parse_MalformedLinksItems_ParseError()
    {
        // Arrange — массив не из пар / не-строковые поля (битое значение)
        var kvs = new[]
        {
            Kv("/s3/clusters/c/links", """[{"a":"x"},{"a":"x","b":5}]"""),
        };

        // Act
        var cluster = S3SnapshotParser.Parse(kvs).Value.Clusters.Single();

        // Assert
        cluster.Links.Should().BeNull();
        cluster.ParseError.Should().Contain("/links: битый JSON");
    }

    [Fact]
    public void Parse_PkiKeys_NotUnknown_RealUnknownKey_IsUnknown()
    {
        // Arrange — pki-ключи (кластерные и лист ноды) известны; реально
        // неизвестный ключ — по-прежнему unknownKeys
        var kvs = new[]
        {
            Kv("/s3/clusters/c/pki/ca", """{"name":"c","publicKeyPem":"p","private_key_pem":"k"}"""),
            Kv("/s3/clusters/c/pki/orchestrator", """{"certificate":"{}","private_key_pem":"k"}"""),
            Kv("/s3/clusters/c/nodes/n1/pki", """{"certificate":"{}","private_key_pem":"k"}"""),
            Kv("/s3/clusters/c/whatever-t33", "x"),
        };

        // Act
        var snap = S3SnapshotParser.Parse(kvs).Value;
        var cluster = snap.Clusters.Single();

        // Assert — pki не в unknownKeys; неизвестный — в unknownKeys; тик цел
        snap.UnknownKeys.Should().BeEquivalentTo("/s3/clusters/c/whatever-t33");
        cluster.ParseError.Should().BeNull();
        cluster.Nodes.Should().ContainSingle(n => n.Node == "n1");
    }
}
