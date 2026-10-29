using FluentAssertions;
using PgWorker.WalReceiver;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Контракт идентичности приёмника (arch/19 §3): полный env — объект;
// отсутствие обязательного ключа — null + имена отсутствующих; PGW_ETCD без
// элементов — null. Значения env в тексты ошибок не включаются.
public class WalReceiverIdentityTests
{
    private static Dictionary<string, string?> FullEnv() => new()
    {
        ["PGW_BACKUP_CLUSTER"] = "c1",
        ["PGW_BACKUP_SHARD"] = "shard1",
        ["PGW_BACKUP_NODE"] = "s1a",
        ["PGW_ETCD"] = "http://etcd1:2379,http://etcd2:2379",
        ["PGW_BACKUP_S3_BUCKET"] = "backups",
        ["PGW_BACKUP_S3_CA_PEM"] = "-----BEGIN CERTIFICATE-----",
    };

    [Fact]
    public void FullEnv_ParsesAllFields()
    {
        // Arrange — полный env (вкл. опциональные bucket/CA)
        var env = FullEnv();

        // Act
        var identity = WalReceiverIdentity.Parse(env.GetValueOrDefault, out var errors);

        // Assert — все поля на месте, ошибок нет
        errors.Should().BeEmpty();
        identity.Should().NotBeNull();
        identity!.Cluster.Should().Be("c1");
        identity.Shard.Should().Be("shard1");
        identity.Node.Should().Be("s1a");
        identity.EtcdEndpoints.Should().Equal("http://etcd1:2379", "http://etcd2:2379");
        identity.S3Bucket.Should().Be("backups");
        identity.S3CaPem.Should().Be("-----BEGIN CERTIFICATE-----");
    }

    [Fact]
    public void MissingRequiredEnv_ReturnsNullWithKeyNames()
    {
        // Arrange — нет CLUSTER/SHARD/NODE и PGW_ETCD; секретных значений в env нет
        var env = new Dictionary<string, string?>
        {
            ["PGW_BACKUP_S3_BUCKET"] = "backups",
        };

        // Act
        var identity = WalReceiverIdentity.Parse(env.GetValueOrDefault, out var errors);

        // Assert — null + имена отсутствующих ключей (без значений)
        identity.Should().BeNull();
        errors.Should().BeEquivalentTo(
            "required env PGW_BACKUP_CLUSTER is missing or empty",
            "required env PGW_BACKUP_SHARD is missing or empty",
            "required env PGW_BACKUP_NODE is missing or empty",
            "required env PGW_ETCD is missing or empty");
    }

    [Fact]
    public void EmptyEtcdList_ReturnsNull()
    {
        // Arrange — обязательные ключи есть, PGW_ETCD — только запятые/пробелы
        var env = FullEnv();
        env["PGW_ETCD"] = " , ,";

        // Act
        var identity = WalReceiverIdentity.Parse(env.GetValueOrDefault, out var errors);

        // Assert
        identity.Should().BeNull();
        errors.Should().ContainSingle()
            .Which.Should().Be("env PGW_ETCD must be a non-empty comma-separated list of urls");
    }

    [Fact]
    public void OptionalBucketAndCa_AreNullable()
    {
        // Arrange — без опциональных bucket/CA (подсистема гейтится etcd-ключами)
        var env = FullEnv();
        env["PGW_BACKUP_S3_BUCKET"] = null;
        env["PGW_BACKUP_S3_CA_PEM"] = null;

        // Act
        var identity = WalReceiverIdentity.Parse(env.GetValueOrDefault, out var errors);

        // Assert — null-значения, ошибок нет
        errors.Should().BeEmpty();
        identity!.S3Bucket.Should().BeNull();
        identity.S3CaPem.Should().BeNull();
    }
}
