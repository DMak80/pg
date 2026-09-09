using PgWorker.Etcd.Client;
using PgWorker.Etcd.Parsing;

namespace PgWorker.UnitTests.Etcd;

// Парсер /pgworker/backups/* (arch/19 §4, t01): полный набор ключей,
// битые значения — parseErrors + пропуск записи, неизвестные ключи и чужие
// префиксы — игнор (толерантность снапшот-парсеров).
public class BackupsParserTests
{
    [Fact]
    public void Parse_FullFixture_ClustersPolicyFullsWal()
    {
        // Arrange — два кластера: demo (policy + 2 полных + WAL), shop (битый
        // policy, DELETING-full с суффиксом коллизии, DEGRADED WAL).
        var kvs = EtcdFixtures.LoadKv("backups-full.json");

        // Act
        var result = BackupsParser.Parse(kvs, out var errors);

        // Assert — кластеры отсортированы по имени; demo разобран полностью.
        result.IsSuccess.Should().BeTrue();
        var demo = result.Value.Should().Contain(c => c.Cluster == "demo").Subject;
        demo.Policy.Should().NotBeNull();
        demo.Policy!.RetentionDays.Should().Be(14);
        demo.Policy.RetentionWeeks.Should().Be(8);
        demo.Policy.RetentionMonths.Should().Be(12);
        demo.Policy.FullMaxAgeSec.Should().Be(43200);
        demo.Policy.VerifyOnCreate.Should().BeFalse();
        var s1 = demo.Shards.Should().ContainKey("s1").WhoseValue;
        s1.Full.Should().HaveCount(2);
        s1.Full[0].Id.Should().Be("20260908030000Z"); // сортировка по Id (Ordinal)
        s1.Full[0].State.Should().Be(FullBackupStatus.Completed);
        s1.Full[0].Role.Should().Be(BackupSourceRole.Replica);
        s1.Full[0].Node.Should().Be("pgw-demo-s1-2");
        s1.Full[0].StartedUnix.Should().Be(1757290800);
        s1.Full[0].FinishedUnix.Should().Be(1757294400);
        s1.Full[0].WalStartSegment.Should().Be("000000010000000000000042");
        s1.Full[0].SizeBytes.Should().Be(104857600);
        s1.Full[0].Verify.Should().NotBeNull();
        s1.Full[0].Verify!.State.Should().Be(BackupVerifyStatus.Ok);
        s1.Full[0].Verify!.CheckedUnix.Should().Be(1757294500);
        s1.Full[1].State.Should().Be(FullBackupStatus.Failed);
        s1.Full[1].Error.Should().Contain("no space");
        s1.Full[1].Verify.Should().BeNull(); // verify отсутствует → null
        s1.Wal.Should().NotBeNull();
        s1.Wal!.State.Should().Be(WalStreamStatus.Active);
        s1.Wal.Slot.Should().Be("pgw_bkp_demo_s1");
        s1.Wal.MasterNode.Should().Be("pgw-demo-s1-1");
        s1.Wal.ChainStartSegment.Should().Be("000000010000000000000042");
        s1.Wal.LastReceivedSegment.Should().Be("0000000100000000000000C3");
        s1.Wal.LastUploadedSegment.Should().Be("0000000100000000000000C2");
        s1.Wal.LastUploadedUnix.Should().Be(1757377213);
        s1.Wal.LagSegments.Should().Be(1);
        demo.Shards.Should().ContainKey("s2")
            .WhoseValue.Full[0].State.Should().Be(FullBackupStatus.Uploading);
        // demo разобран без ошибок (битые значения есть только у shop — тест 2).
        errors.Should().NotContain(e => e.Contains("/pgworker/backups/demo"));
    }

    [Fact]
    public void Parse_MalformedValues_SkippedWithErrors()
    {
        // Arrange — битые policy и full у shop; demo жив.
        var kvs = EtcdFixtures.LoadKv("backups-full.json");

        // Act
        var result = BackupsParser.Parse(kvs, out var errors);

        // Assert — битые записи в parseErrors и пропущены; живые на месте.
        result.IsSuccess.Should().BeTrue();
        errors.Should().Contain(e => e.Contains("/pgworker/backups/shop/policy"));
        errors.Should().Contain(e => e.Contains("/pgworker/backups/shop/s1/full/broken-json"));
        var shop = result.Value.Should().Contain(c => c.Cluster == "shop").Subject;
        shop.Policy.Should().BeNull(); // битый policy → null (дефолт — у потребителя)
        shop.Shards["s1"].Full.Should().ContainSingle(); // остался только DELETING
        shop.Shards["s1"].Full[0].Id.Should().Be("20260907030000Z-2"); // суффикс коллизии
        shop.Shards["s1"].Full[0].State.Should().Be(FullBackupStatus.Deleting);
        shop.Shards["s1"].Full[0].Role.Should().Be(BackupSourceRole.Master); // fallback-факт
    }

    [Fact]
    public void Parse_UnknownKeysAndForeignPrefix_Ignored()
    {
        // Arrange — unknown-leaf внутри префикса + чужой /clusters/-ключ.
        var kvs = EtcdFixtures.LoadKv("backups-full.json");

        // Act
        var result = BackupsParser.Parse(kvs, out var errors);

        // Assert — ни ошибки, ни записи: неизвестное — игнор (обратная
        // совместимость), /clusters/ — не наш префикс.
        result.IsSuccess.Should().BeTrue();
        errors.Should().NotContain(e => e.Contains("unknown-leaf"));
        errors.Should().NotContain(e => e.Contains("/clusters/"));
        result.Value.Should().HaveCount(2); // demo + shop, /clusters/demo не стал кластером
    }

    [Fact]
    public void Parse_NoPolicyNoWal_NullsWithoutErrors()
    {
        // Arrange — кластер без policy и wal: только один full.
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/solo/x1/full/20260908030000Z",
                "{\"state\":\"PLANNED\",\"node\":\"pgw-solo-x1-2\",\"role\":\"replica\",\"started_unix\":1757290800,\"wal_start_segment\":\"000000010000000000000001\"}", 1),
        };

        // Act
        var result = BackupsParser.Parse(kvs, out var errors);

        // Assert — отсутствующие опциональные ключи — не ошибка.
        result.IsSuccess.Should().BeTrue();
        errors.Should().BeEmpty();
        var solo = result.Value.Should().ContainSingle().Subject;
        solo.Cluster.Should().Be("solo");
        solo.Policy.Should().BeNull();
        solo.Shards["x1"].Wal.Should().BeNull();
        solo.Shards["x1"].Full.Should().ContainSingle()
            .Which.State.Should().Be(FullBackupStatus.Planned);
    }

    [Fact]
    public void Parse_UnknownState_RecordSkippedWithError()
    {
        // Arrange — неизвестное state (будущая версия воркера писала новое).
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/c1/x1/full/20260908030000Z",
                "{\"state\":\"ARCHIVED\",\"node\":\"n1\",\"role\":\"replica\",\"started_unix\":1,\"wal_start_segment\":\"000000010000000000000001\"}", 1),
        };

        // Act
        var result = BackupsParser.Parse(kvs, out var errors);

        // Assert — запись пропущена с диагностикой, не исключение.
        result.IsSuccess.Should().BeTrue();
        errors.Should().Contain(e => e.Contains("state"));
        result.Value.Should().ContainSingle().Which.Shards["x1"].Full.Should().BeEmpty();
    }
}
