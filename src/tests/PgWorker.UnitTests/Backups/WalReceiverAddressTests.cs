using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Адресация HTTP API приёмника (arch/19 §2/§3.5, spec §3.1): нода —
// alias:8080 по docker-имени (канонические pgw-<C>-<X>-<n>; object — имя её
// контейнера). Все операции бэкапов — команды на приёмники нод (§5/§3.5/§3.6).
public class WalReceiverAddressTests
{
    [Theory]
    [InlineData("shop", "shard1", "shard1a", "http://pgw-shop-shard1-shard1a:8080")]
    [InlineData("c", "s", "n1", "http://pgw-c-s-n1:8080")]
    public void ForNode_CanonicalName_CanonicalAlias(
        string cluster, string shard, string node, string expected)
    {
        // Arrange / Act / Assert — objectContainer null/пуст → каноническое имя.
        WalReceiverAddress.ForNode(cluster, shard, node, null).Should().Be(expected);
        WalReceiverAddress.ForNode(cluster, shard, node, "").Should().Be(expected);
    }

    [Fact]
    public void ForNode_AdoptedNode_ObjectContainerName()
    {
        // Arrange / Act / Assert — object-нода: адрес по имени её контейнера.
        WalReceiverAddress.ForNode("shop", "shard1", "shard1a", "as-shard1a")
            .Should().Be("http://as-shard1a:8080");
    }
}
