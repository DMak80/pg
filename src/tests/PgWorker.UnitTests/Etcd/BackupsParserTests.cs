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

    [Fact]
    public void Parse_FullWithoutWalStart_ParsedWithNull()
    {
        // Arrange — записи до фазы UPLOADING не знают wal_start_segment
        // (arch/19 §4: заполняется с UPLOADING, у рано упавших может отсутствовать).
        var kvs = EtcdFixtures.LoadKv("backups-full.json");

        // Act
        var result = BackupsParser.Parse(kvs, out var errors);

        // Assert — null без ошибки; обязательны только state/node/role/started_unix.
        result.IsSuccess.Should().BeTrue();
        errors.Should().NotContain(e => e.Contains("20260910030000Z"));
        var planned = result.Value.First(c => c.Cluster == "demo").Shards["s2"].Full
            .Single(f => f.Id == "20260910030000Z");
        planned.State.Should().Be(FullBackupStatus.Planned);
        planned.WalStartSegment.Should().BeNull();
        var running = result.Value.First(c => c.Cluster == "shop").Shards["s2"].Full
            .Single(f => f.Id == "20260910030000Z");
        running.State.Should().Be(FullBackupStatus.Running);
        running.WalStartSegment.Should().BeNull();
    }

    // AAA: verify.error читается из full-ключа; interval_sec — из policy-ключа
    [Fact]
    public void Parse_VerifyErrorИIntervalSec_Читаются()
    {
        // Arrange
        var kvs = new[]
        {
            new Kv("/pgworker/backups/c1/policy",
                """{"full_max_age_sec":86400,"verify":{"on_create":true,"interval_sec":3600}}""", 1),
            new Kv("/pgworker/backups/c1/shard1/full/20260911120000Z",
                """{"state":"COMPLETED","node":"n1","role":"replica","started_unix":1757500000,"finished_unix":1757500300,"verify":{"state":"FAILED","checked_unix":1757500600,"error":"pg_verifybackup failed"}}""", 2),
        };

        // Act
        var parsed = BackupsParser.Parse(kvs, out var errors);

        // Assert
        errors.Should().BeEmpty();
        var full = parsed.Value.Single(c => c.Cluster == "c1").Shards["shard1"].Full.Single();
        full.Verify!.Error.Should().Be("pg_verifybackup failed");
        full.Verify.State.Should().Be(BackupVerifyStatus.Failed);
        parsed.Value.Single(c => c.Cluster == "c1").Policy!.VerifyIntervalSec.Should().Be(3600);
    }

    // AAA: старые записи (verify без error, policy без interval_sec) парсятся — обратная совместимость
    [Fact]
    public void Parse_СтарыеЗаписи_БезНовыхПолей_Ок()
    {
        // Arrange
        var kvs = new[]
        {
            new Kv("/pgworker/backups/c2/policy",
                """{"full_max_age_sec":86400,"verify":{"on_create":false}}""", 1),
            new Kv("/pgworker/backups/c2/shard1/full/20260911120000Z",
                """{"state":"COMPLETED","node":"n1","role":"replica","started_unix":1757500000,"verify":{"state":"OK","checked_unix":1757500600}}""", 2),
        };

        // Act
        var parsed = BackupsParser.Parse(kvs, out var errors);

        // Assert — null, а не null-политика: дефолт подставляет потребитель
        errors.Should().BeEmpty();
        var cluster = parsed.Value.Single(c => c.Cluster == "c2");
        cluster.Policy.Should().NotBeNull();
        cluster.Policy!.VerifyIntervalSec.Should().BeNull();
        cluster.Shards["shard1"].Full.Single().Verify!.Error.Should().BeNull();
    }

    [Fact]
    public void Parse_PolicyValidJsonNotObject_SkippedWithErrorNoThrow()
    {
        // Arrange — валидный JSON, но не объект (число, строка): без guard
        // TryGetProperty бросил бы InvalidOperationException вне JsonException.
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/n1/policy", "123", 1),
            new("/pgworker/backups/s1/policy", "\"x\"", 2),
        };

        // Act
        var result = BackupsParser.Parse(kvs, out var errors);

        // Assert — обе записи в parseErrors и пропущены (Policy = null),
        // никакого исключения наружу (толерантность парсера).
        result.IsSuccess.Should().BeTrue();
        errors.Should().Contain(e => e.Contains("/pgworker/backups/n1/policy"));
        errors.Should().Contain(e => e.Contains("/pgworker/backups/s1/policy"));
        result.Value.Should().Contain(c => c.Cluster == "n1").Which.Policy.Should().BeNull();
        result.Value.Should().Contain(c => c.Cluster == "s1").Which.Policy.Should().BeNull();
    }
}
