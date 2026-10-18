using FluentAssertions;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

/// <summary>Юниты разбора кворума HA-etcd (endpoint status -w json):
/// формат фиксирован реальным выводом etcdctl 3.5.21 (member_id/leader —
/// uint64 больше long.MaxValue — регресс фикса пробы без /bin/sh).</summary>
public sealed class E2eQuorumElectedTests
{
    private static string ClusterJson(string leader1, string leader2, string leader3)
        => $$$"""
             [{"Endpoint":"http://etcd1:2379","Status":{"header":{"cluster_id":9003843037621960302,"member_id":4088032412695972013,"revision":1,"raft_term":2},"version":"3.5.21","dbSize":20480,"leader":{{{leader1}}},"raftIndex":11,"raftTerm":2,"raftAppliedIndex":11,"dbSizeInUse":16384}},
             {"Endpoint":"http://etcd2:2379","Status":{"header":{"cluster_id":9003843037621960302,"member_id":7623479705957852552,"revision":1,"raft_term":2},"version":"3.5.21","dbSize":20480,"leader":{{{leader2}}},"raftIndex":11,"raftTerm":2,"raftAppliedIndex":11,"dbSizeInUse":16384}},
             {"Endpoint":"http://etcd3:2379","Status":{"header":{"cluster_id":9003843037621960302,"member_id":11870244905213087059,"revision":1,"raft_term":2},"version":"3.5.21","dbSize":20480,"leader":{{{leader3}}},"raftIndex":11,"raftTerm":2,"raftAppliedIndex":11,"dbSizeInUse":16384}}]
             """;

    [Fact]
    public void AllNodesSameNonZeroLeader_True()
    {
        // Arrange: 3 узла ответили, лидер один; его member_id ВЫШЕ long.MaxValue
        // (11870244905213087059 > 9223372036854775807) — числовой парсер
        // int64 здесь не годится, разбор — по сырым JSON-токенам.
        var json = ClusterJson(
            "11870244905213087059", "11870244905213087059", "11870244905213087059");

        // Act
        var elected = E2eEnvironment.EtcdQuorumElected(json, 3);

        // Assert
        elected.Should().BeTrue("all nodes name the same non-zero leader");
    }

    [Fact]
    public void LeaderZero_False()
    {
        // Arrange: лидер ещё не избран (leader:0 у всех ответивших).
        var json = ClusterJson("0", "0", "0");

        // Act
        var elected = E2eEnvironment.EtcdQuorumElected(json, 3);

        // Assert
        elected.Should().BeFalse("a zero leader record — the election has not finished");
    }

    [Fact]
    public void LeadersDisagree_False()
    {
        // Arrange: узлы расходятся в лидере (транзиент выборов / раскол).
        var json = ClusterJson("11870244905213087059", "7623479705957852552", "11870244905213087059");

        // Act
        var elected = E2eEnvironment.EtcdQuorumElected(json, 3);

        // Assert
        elected.Should().BeFalse("different leaders in the node answers — no quorum");
    }

    [Fact]
    public void NotAllEndpointsAnswered_False()
    {
        // Arrange: недоступный endpoint не попадает в выдачу etcdctl —
        // массив короче ожидаемого числа узлов (ответили только двое).
        var json = """
             [{"Endpoint":"http://etcd1:2379","Status":{"header":{"cluster_id":9003843037621960302,"member_id":4088032412695972013,"revision":1,"raft_term":2},"version":"3.5.21","dbSize":20480,"leader":11870244905213087059}},
             {"Endpoint":"http://etcd2:2379","Status":{"header":{"cluster_id":9003843037621960302,"member_id":7623479705957852552,"revision":1,"raft_term":2},"version":"3.5.21","dbSize":20480,"leader":11870244905213087059}}]
             """;

        // Act
        var elected = E2eEnvironment.EtcdQuorumElected(json, 3);

        // Assert
        elected.Should().BeFalse("not all cluster nodes answered");
    }
}
