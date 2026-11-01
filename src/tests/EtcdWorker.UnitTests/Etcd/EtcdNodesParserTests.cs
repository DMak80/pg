using EtcdWorker.Etcd;
using Shared.Etcd.Client;

namespace EtcdWorker.UnitTests.Etcd;

// Разбор листов /etcd/nodes/<k>/{state,placed_on}: сборка узлов, толерантность к мусору.
public class EtcdNodesParserTests
{
    private static Kv KvOf(string key, string value) => new(key, value, ModRevision: 1);

    [Fact]
    public void Parse_StateAndPlacedOn_BuildsOneNode()
    {
        // Arrange — оба листа одного узла k=2
        var kvs = new List<Kv>
        {
            KvOf("/etcd/nodes/2/state", "RUNNING"),
            KvOf("/etcd/nodes/2/placed_on", "host-a"),
        };

        // Act
        var nodes = EtcdNodesParser.Parse(kvs);

        // Assert — ровно один узел с обоими значениями
        nodes.Should().HaveCount(1);
        nodes[0].Should().Be(new EtcdNodesParser.ManagedNodeState(2, "RUNNING", "host-a"));
    }

    [Fact]
    public void Parse_TwoNodes_SortsByK()
    {
        // Arrange — узлы подаются вперемешку (10 раньше 1)
        var kvs = new List<Kv>
        {
            KvOf("/etcd/nodes/10/state", "RUNNING"),
            KvOf("/etcd/nodes/1/state", "EMPTY"),
        };

        // Act
        var nodes = EtcdNodesParser.Parse(kvs);

        // Assert — сортировка по K независимо от порядка входа
        nodes.Select(n => n.K).Should().Equal(1, 10);
        nodes[0].State.Should().Be("EMPTY");
        nodes[1].State.Should().Be("RUNNING");
    }

    [Fact]
    public void Parse_JsonQuotedValue_StripsQuotes()
    {
        // Arrange — значение как JSON-строка, с кавычками и пробелами
        var kvs = new List<Kv> { KvOf("/etcd/nodes/3/state", "\"RUNNING\"") };

        // Act
        var nodes = EtcdNodesParser.Parse(kvs);

        // Assert — кавычки сняты
        nodes.Single().State.Should().Be("RUNNING");
    }

    [Fact]
    public void Parse_BrokenK_Ignored()
    {
        // Arrange — нечисловой номер узла
        var kvs = new List<Kv>
        {
            KvOf("/etcd/nodes/abc/state", "RUNNING"),
            KvOf("/etcd/nodes/1/state", "RUNNING"),
        };

        // Act
        var nodes = EtcdNodesParser.Parse(kvs);

        // Assert — битый ключ выброшен, валидный остался
        nodes.Should().ContainSingle();
        nodes[0].K.Should().Be(1);
    }

    [Fact]
    public void Parse_ForeignKeyScheme_Ignored()
    {
        // Arrange — чужие ключи вне схемы /etcd/nodes/<k>/<leaf>
        var kvs = new List<Kv>
        {
            KvOf("/etcd/config", "{}"),
            KvOf("/etcd/nodes", ""),
            KvOf("/etcd/nodes/1", "x"),
            KvOf("/etcd/nodes/1/state/extra", "x"),
            KvOf("/other/nodes/1/state", "RUNNING"),
        };

        // Act
        var nodes = EtcdNodesParser.Parse(kvs);

        // Assert — ни один чужой ключ не породил узел
        nodes.Should().BeEmpty();
    }

    [Fact]
    public void Parse_UnknownLeaf_NoErrorAndNoData()
    {
        // Arrange — unknown-лист узла k=1
        var kvs = new List<Kv> { KvOf("/etcd/nodes/1/usage", "whatever") };

        // Act
        var nodes = EtcdNodesParser.Parse(kvs);

        // Assert — не ошибка; unknown-лист узла НЕ создаёт (узел рождается
        // только листами state/placed_on — правка оркестратора по конфликту
        // план-кода/план-теста, семантика план-теста корректна).
        nodes.Should().BeEmpty();
    }
}
