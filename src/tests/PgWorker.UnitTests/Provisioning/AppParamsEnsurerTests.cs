using PgWorker.Core;
using PgWorker.Etcd.Client;
using PgWorker.Provisioning.Processes;
using Xunit;

namespace PgWorker.UnitTests.Provisioning;

// Ensure per-node app_params (spec §3.1/§4.2): put-if-absent дефолта,
// существующее не перезаписывается, пустое значение живёт.
public class AppParamsEnsurerTests
{
    private const string Ep = "http://etcd:2379";

    private static AppParamsEnsurer Sut(Fakes.FakeEtcd etcd)
        => new(etcd, [Ep], "sslmode=require");

    [Fact]
    public async Task Ensure_MissingKeys_PutsDefaultPerNode()
    {
        // Arrange — ключей нет (provisioning P2.5' после dsn)
        var etcd = new Fakes.FakeEtcd();

        // Act
        var result = await Sut(etcd).EnsureShardAsync(
            "shop", "shard1", ["shard1a", "shard1b"], CancellationToken.None);

        // Assert — обе ноды получили дефолт, разными txn (put-if-absent)
        result.IsSuccess.Should().BeTrue();
        etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/app_params"].Value
            .Should().Be("sslmode=require");
        etcd.Store["/clusters/shop/shards/shard1/nodes/shard1b/app_params"].Value
            .Should().Be("sslmode=require");
    }

    [Fact]
    public async Task Ensure_ExistingKeys_NotOverwritten()
    {
        // Arrange — оператор etcdctl'ом записал своё значение (в т.ч. пустое)
        var etcd = new Fakes.FakeEtcd();
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1a/app_params", "sslmode=verify-full");
        etcd.Seed("/clusters/shop/shards/shard1/nodes/shard1b/app_params", "");

        // Act
        var result = await Sut(etcd).EnsureShardAsync(
            "shop", "shard1", ["shard1a", "shard1b"], CancellationToken.None);

        // Assert — txn проигран compare NotExists, значения нетронуты (spec §3.1)
        result.IsSuccess.Should().BeTrue();
        etcd.Store["/clusters/shop/shards/shard1/nodes/shard1a/app_params"].Value
            .Should().Be("sslmode=verify-full");
        etcd.Store["/clusters/shop/shards/shard1/nodes/shard1b/app_params"].Value
            .Should().Be("");
    }
}
