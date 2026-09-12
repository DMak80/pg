using AdminPanel.Core;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Панельный парсер /pgworker/backups/ (t02): never-семантика — шард с ключами
// полных, но без COMPLETED, попадает в словарь со значением null (не молчит:
// молчание правила — только для ПУСТОГО префикса кластера).
public class BackupsParserTests
{
    // AAA: ключи есть, COMPLETED нет → ShardLastCompletedUnix[s1] = null
    [Fact]
    public void Parse_ShardWithKeysButNoCompleted_NullLastCompleted()
    {
        // Arrange — RUNNING + FAILED (первое включение подсистемы), COMPLETED нет
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/demo/s1/full/20260910030000Z",
                "{\"state\":\"RUNNING\",\"node\":\"n\",\"role\":\"replica\",\"started_unix\":1757463600}", 1),
            new("/pgworker/backups/demo/s1/full/20260910030100Z",
                "{\"state\":\"FAILED\",\"node\":\"n\",\"role\":\"replica\",\"started_unix\":1757463660}", 2),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert — шард в словаре (never-алерт реализуем), значение null
        result.Errors.Should().BeEmpty();
        var cluster = result.Clusters.Should().ContainSingle().Subject;
        cluster.Cluster.Should().Be("demo");
        cluster.FullMaxAgeSec.Should().BeNull("policy-ключа нет — панельный дефолт в правиле");
        cluster.ShardLastCompletedUnix.Should().ContainKey("s1")
            .WhoseValue.Should().BeNull();
    }

    // AAA: несколько COMPLETED — в словаре максимум finished_unix
    // AAA: FAILED-verify полный не считается «последним COMPLETED» (свежесть — валидные);
    // фиксируется в ShardVerifyFailures с error/checked_unix
    [Fact]
    public void Parse_FailedVerify_НеДаетСвежести_иПопадаетВFailures()
    {
        // Arrange — битый полный СВЕЖЕЕ валидного
        var kvs = new[]
        {
            new Kv("/pgworker/backups/p1/shard1/full/20260910120000Z",
                """{"state":"COMPLETED","node":"n1","role":"replica","started_unix":100,"finished_unix":200,"verify":{"state":"OK","checked_unix":300}}""", 1),
            new Kv("/pgworker/backups/p1/shard1/full/20260911120000Z",
                """{"state":"COMPLETED","node":"n1","role":"replica","started_unix":1000,"finished_unix":1100,"verify":{"state":"FAILED","checked_unix":1200,"error":"дыра WAL-цепочки: ожидался A, найден B"}}""", 2),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert — свежесть = валидный (200), failure = битый с его текстом
        var cluster = result.Clusters.Single(c => c.Cluster == "p1");
        cluster.ShardLastCompletedUnix["shard1"].Should().Be(200);
        cluster.ShardVerifyFailures!["shard1"].Should().Be(
            new ShardVerifyFailure("shard1", "20260911120000Z", "дыра WAL-цепочки: ожидался A, найден B", 1200));
    }

    // AAA: несколько FAILED — последний по checked_unix; битый verify.state →
    // KeyParseError + verify игнор (полный валиден, непроверен)
    [Fact]
    public void Parse_НесколькоБитых_иТолерантность()
    {
        // Arrange — два FAILED-полных (checked_unix 100 и 200) + битый verify.state
        var kvs = new[]
        {
            new Kv("/pgworker/backups/p2/shard1/full/20260910120000Z",
                """{"state":"COMPLETED","node":"n1","role":"replica","started_unix":1,"finished_unix":2,"verify":{"state":"FAILED","checked_unix":100,"error":"первый фейл"}}""", 1),
            new Kv("/pgworker/backups/p2/shard1/full/20260910130000Z",
                """{"state":"COMPLETED","node":"n1","role":"replica","started_unix":3,"finished_unix":4,"verify":{"state":"FAILED","checked_unix":200,"error":"второй фейл"}}""", 2),
            new Kv("/pgworker/backups/p2/shard1/full/20260910140000Z",
                """{"state":"COMPLETED","node":"n1","role":"replica","started_unix":5,"finished_unix":6,"verify":{"state":"BROKEN"}}""", 3),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert — failure = последний по checked_unix; битый verify — диагностика,
        // запись жива и непроверена (валидна); парсер не падает
        var cluster = result.Clusters.Single(c => c.Cluster == "p2");
        cluster.ShardVerifyFailures!["shard1"].Error.Should().Be("второй фейл");
        cluster.ShardVerifyFailures["shard1"].CheckedUnix.Should().Be(200);
        result.Errors.Should().ContainSingle(e => e.Key.EndsWith("20260910140000Z"));
    }

    [Fact]
    public void Parse_MultipleCompleted_LatestFinishedUnix()
    {
        // Arrange — два COMPLETED с разными finished_unix
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/demo/s1/full/20260908030000Z",
                "{\"state\":\"COMPLETED\",\"node\":\"n\",\"role\":\"replica\",\"started_unix\":1757290800,\"finished_unix\":1757294400}", 1),
            new("/pgworker/backups/demo/s1/full/20260909030000Z",
                "{\"state\":\"COMPLETED\",\"node\":\"n\",\"role\":\"replica\",\"started_unix\":1757377200,\"finished_unix\":1757380800}", 2),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert
        result.Clusters.Single().ShardLastCompletedUnix["s1"].Should().Be(1757380800);
    }

    // AAA: COMPLETED без finished_unix (битый) — шард регистрируется, значение null
    [Fact]
    public void Parse_CompletedWithoutFinishedUnix_ShardKeptValueNull()
    {
        // Arrange
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/demo/s1/full/20260908030000Z",
                "{\"state\":\"COMPLETED\",\"node\":\"n\",\"role\":\"replica\",\"started_unix\":1757290800}", 1),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert — шард в словаре со значением null (never-ветка правила)
        result.Clusters.Single().ShardLastCompletedUnix.Should().ContainKey("s1")
            .WhoseValue.Should().BeNull();
    }

    // AAA: policy кластера парсится в FullMaxAgeSec; без него — null
    [Fact]
    public void Parse_PolicyKey_FullMaxAgeSec()
    {
        // Arrange
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/demo/policy",
                "{\"retention\":{\"days\":7,\"weeks\":4,\"months\":6},\"full_max_age_sec\":43200,\"verify\":{\"on_create\":true}}", 1),
            new("/pgworker/backups/demo/s1/full/20260908030000Z",
                "{\"state\":\"COMPLETED\",\"node\":\"n\",\"role\":\"replica\",\"started_unix\":1757290800,\"finished_unix\":1757294400}", 2),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert
        result.Errors.Should().BeEmpty();
        result.Clusters.Single().FullMaxAgeSec.Should().Be(43200);
    }

    // AAA: битый JSON — KeyParseError, запись пропущена (тик не роняет)
    [Fact]
    public void Parse_MalformedJson_KeyParseError()
    {
        // Arrange
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/demo/s1/full/20260908030000Z", "not-json", 1),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert
        result.Errors.Should().ContainSingle().Which.Key.Should().Be("/pgworker/backups/demo/s1/full/20260908030000Z");
        result.Clusters.Should().BeEmpty();
    }

    // AAA: пустой префикс — Clusters пуст (правило молчит: подсистема не включена)
    [Fact]
    public void Parse_EmptyKvs_NoClusters()
    {
        // Arrange / Act
        var result = BackupsParser.Parse([]);

        // Assert
        result.Clusters.Should().BeEmpty();
        result.Errors.Should().BeEmpty();
    }

    // ---- t06: ключ storage + DELETING-полные ----

    // AAA: валидный storage-ключ → Storage со всеми полями, state=WARN
    [Fact]
    public void Parse_StorageKey_StorageInfo()
    {
        // Arrange — глобальный ключ 4 сегментов с квотой
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/storage",
                """{"used_bytes":300,"quota_bytes":1000,"used_percent":30,"state":"WARN","updated_unix":1760000000}""", 1),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert
        result.Errors.Should().BeEmpty();
        result.Storage.Should().NotBeNull();
        result.Storage!.UsedBytes.Should().Be(300);
        result.Storage.QuotaBytes.Should().Be(1000);
        result.Storage.UsedPercent.Should().Be(30);
        result.Storage.State.Should().Be(BackupStorageState.Warn);
        result.Storage.UpdatedUnix.Should().Be(1760000000);
    }

    // AAA: storage без квоты — QuotaBytes/UsedPercent null
    [Fact]
    public void Parse_StorageWithoutQuota_Nulls()
    {
        // Arrange
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/storage",
                """{"used_bytes":123,"state":"OK","updated_unix":1760000000}""", 1),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert
        result.Storage.Should().NotBeNull();
        result.Storage!.QuotaBytes.Should().BeNull();
        result.Storage.UsedPercent.Should().BeNull();
        result.Storage.State.Should().Be(BackupStorageState.Ok);
    }

    // AAA: битый JSON storage → KeyParseError, Storage null
    [Fact]
    public void Parse_StorageMalformedJson_KeyParseError()
    {
        // Arrange
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/storage", "not-json", 1),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert
        result.Storage.Should().BeNull();
        result.Errors.Should().ContainSingle().Which.Key.Should().Be("/pgworker/backups/storage");
    }

    // AAA: неизвестное state → KeyParseError
    [Fact]
    public void Parse_StorageBrokenState_KeyParseError()
    {
        // Arrange
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/storage",
                """{"used_bytes":1,"state":"BROKEN","updated_unix":1760000000}""", 1),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert
        result.Storage.Should().BeNull();
        result.Errors.Should().ContainSingle();
    }

    // AAA: DELETING-полный попадает в DeletingFulls[shard]; finished опционален
    [Fact]
    public void Parse_DeletingFull_GoesToDeletingFulls()
    {
        // Arrange — DELETING с finished_unix и без
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/demo/s1/full/20260901030000Z",
                """{"state":"DELETING","node":"n","role":"replica","started_unix":1757290800,"finished_unix":1757294400}""", 1),
            new("/pgworker/backups/demo/s1/full/20260902030000Z",
                """{"state":"DELETING","node":"n","role":"replica","started_unix":1757377200}""", 2),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert
        result.Errors.Should().BeEmpty();
        var cluster = result.Clusters.Single();
        cluster.DeletingFulls.Should().ContainKey("s1");
        cluster.DeletingFulls!["s1"].Should().HaveCount(2);
        cluster.DeletingFulls["s1"][0].Id.Should().Be("20260901030000Z");
        cluster.DeletingFulls["s1"][0].FinishedUnix.Should().Be(1757294400);
        cluster.DeletingFulls["s1"][1].FinishedUnix.Should().BeNull();
    }

    // AAA: DELETING без started_unix — KeyParseError + пропуск записи (новое
    // правило ретенции: возраст не посчитать)
    [Fact]
    public void Parse_DeletingWithoutStarted_KeyParseError()
    {
        // Arrange
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/demo/s1/full/20260901030000Z",
                """{"state":"DELETING","node":"n","role":"replica"}""", 1),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert
        result.Errors.Should().ContainSingle().Which.Reason.Should().Contain("started_unix");
        result.Clusters.Single().DeletingFulls.Should().BeNullOrEmpty();
    }

    // AAA: storage-ключ не создаёт псевдо-кластер (ветка ДО гварда длины —
    // 4-сегментный ключ обрабатывается и пропускается, не попадая в Clusters)
    [Fact]
    public void Parse_StorageKey_NoPseudoCluster()
    {
        // Arrange — только storage-ключ
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/storage",
                """{"used_bytes":1,"state":"OK","updated_unix":1760000000}""", 1),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert
        result.Clusters.Should().BeEmpty();
        result.Storage.Should().NotBeNull();
    }
}
