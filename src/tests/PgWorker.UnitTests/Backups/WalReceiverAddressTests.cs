using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Адресация HTTP API приёмника (t26, arch/19 §2/§3.5, spec §3.1): нода —
// alias:8080 по docker-имени (канонические pgw-<C>-<X>-<n>; object — имя её
// контейнера); tool-контейнер — контур: alias имени в сети контура (без
// публикации), прод: host:<динамический порт> из инспекта публикации :8080.
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

    [Fact]
    public void ForTool_ContourMode_ContainerAliasNoPublish()
    {
        // Arrange / Act / Assert — контур: alias имени :8080 в сети контура
        // (host/порт не участвуют).
        WalReceiverAddress.ForTool("pgw-backup-drill-c1-s1-id", "h1", null, contourMode: true)
            .Should().Be("http://pgw-backup-drill-c1-s1-id:8080");
    }

    [Fact]
    public void ForTool_ProdMode_HostWithPublishedPort()
    {
        // Arrange / Act / Assert — прод: host + динамический порт публикации.
        WalReceiverAddress.ForTool("pgw-backup-verify-c1-s1-id", "h1", 31999, contourMode: false)
            .Should().Be("http://h1:31999");
    }
}
