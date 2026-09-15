using AdminPanel.Core;
using Shared.Etcd.Client;
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

    // AAA (t05): restore-ключ → RestoreOperationInfo; сортировка по Id
    [Fact]
    public void Parse_RestoreKey_GoesToShardsRestores()
    {
        // Arrange — три ключа (id не по порядку) с полным набором полей
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/demo/s1/restore/20260911120000Z",
                "{\"state\":\"PLANNED\",\"backup_id\":\"b\",\"source\":\"demo/s1\",\"target\":\"latest\",\"node\":\"s1a\",\"requested_unix\":1760000000,\"requested_by\":\"api\"}", 1),
            new("/pgworker/backups/demo/s1/restore/20260911090000Z",
                "{\"state\":\"COMPLETED\",\"backup_id\":\"b\",\"source\":\"demo/s1\",\"target\":\"latest\",\"node\":\"s1a\",\"requested_unix\":1759998000,\"requested_by\":\"api\",\"started_unix\":1759998005,\"finished_unix\":1759999000,\"phase\":\"recovering\",\"restored_to_lsn\":\"0/42\"}", 2),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert — сортировка Id (Ordinal), поля по факту
        result.Errors.Should().BeEmpty();
        var cluster = result.Clusters.Single();
        cluster.ShardsRestores.Should().ContainKey("s1");
        var restores = cluster.ShardsRestores!["s1"];
        restores.Select(r => r.Id).Should().Equal("20260911090000Z", "20260911120000Z");
        restores[0].State.Should().Be("COMPLETED");
        restores[0].StartedUnix.Should().Be(1759998005);
        restores[0].FinishedUnix.Should().Be(1759999000);
        restores[1].State.Should().Be("PLANNED");
        restores[1].StartedUnix.Should().BeNull();
    }

    // AAA (t05): битый restore (незнакомое state) — KeyParseError, записи нет
    [Fact]
    public void Parse_RestoreBrokenState_KeyParseError()
    {
        // Arrange — state вне словаря канона
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/demo/s1/restore/x1",
                "{\"state\":\"WAT\",\"requested_unix\":1}", 1),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert
        result.Errors.Should().ContainSingle(e => e.Key.Contains("restore/x1"));
        result.Clusters.Where(c => c.ShardsRestores is { Count: > 0 }).Should().BeEmpty();
    }

    // AAA (t05): шард без restore-ключей — ShardsRestores не содержит его
    [Fact]
    public void Parse_ShardWithoutRestore_NoRestoresEntry()
    {
        // Arrange — только full-ключ шарда
        var kvs = new List<Kv>
        {
            new("/pgworker/backups/demo/s1/full/20260910030000Z",
                "{\"state\":\"COMPLETED\",\"node\":\"n\",\"role\":\"replica\",\"started_unix\":1757290800,\"finished_unix\":1757294400}", 1),
        };

        // Act
        var result = BackupsParser.Parse(kvs);

        // Assert
        result.Errors.Should().BeEmpty();
        var cluster = result.Clusters.Single();
        cluster.ShardLastCompletedUnix.Should().ContainKey("s1");
        cluster.ShardsRestores.Should().BeEmpty("restore-ключей нет — правила молчат");
    }

// ---- t07: BROKEN wal + ключ реестра сирот ----

// AAA (AC8): state=BROKEN читается; незнакомое state — KeyParseError (как прежде)
[Fact]
public void Parse_WalBroken_Читается()
{
    // Arrange — wal-ключ с state=BROKEN (граница разрыва в chain_start_segment)
    var kvs = new List<Kv>
    {
        new("/pgworker/backups/c1/s1/wal",
            """{"state":"BROKEN","slot":"pgw_bkp_c1_s1","master_node":"s1a","chain_start_segment":"000000010000000000000005","last_received_segment":"000000010000000000000005","last_uploaded_segment":"000000010000000000000005","last_uploaded_unix":1760000000,"error":"дыра WAL-цепочки: ожидался 000000010000000000000006"}""", 1),
    };

    // Act
    var result = BackupsParser.Parse(kvs);

    // Assert — Broken прочитан; error доступен правилу
    result.Errors.Should().BeEmpty();
    var wal = result.Clusters.Single().Shards!["s1"];
    wal.Should().NotBeNull();
    wal!.State.Should().Be(WalStreamInfoState.Broken);
    wal.Error.Should().Contain("дыра");
}

// AAA (AC8): незнакомое state wal-ключа → KeyParseError (поведение сохранено)
[Fact]
public void Parse_WalUnknownState_KeyParseError()
{
    // Arrange
    var kvs = new List<Kv>
    {
        new("/pgworker/backups/c1/s1/wal",
            """{"state":"???","slot":"x","master_node":"s1a","last_uploaded_unix":1}""", 1),
    };

    // Act
    var result = BackupsParser.Parse(kvs);

    // Assert
    result.Errors.Should().ContainSingle().Which.Key.Should().Be("/pgworker/backups/c1/s1/wal");
}

// AAA (AC7/AC8): глобальный ключ orphans парсится; битая запись — ошибка ключа
[Fact]
public void Parse_Orphans_Ключ_Читается()
{
    // Arrange
    var kvs = new List<Kv>
    {
        new("/pgworker/backups/orphans",
            """{"orphans":[{"prefix":"ghost1/s1","kind":"cluster","size_bytes":123,"first_seen_unix":1757100000,"state":"OBSERVED"},{"prefix":"live1/s2","kind":"shard","size_bytes":456,"first_seen_unix":1757000000,"state":"DELETING"}],"updated_unix":1760000000}""", 1),
    };

    // Act
    var result = BackupsParser.Parse(kvs);

    // Assert — обе записи; kind/state сохранены
    result.Errors.Should().BeEmpty();
    result.Orphans.Should().NotBeNull();
    result.Orphans!.Orphans.Should().HaveCount(2);
    result.Orphans.Orphans[0].Prefix.Should().Be("ghost1/s1");
    result.Orphans.Orphans[0].State.Should().Be("OBSERVED");
    result.Orphans.Orphans[1].Kind.Should().Be("shard");
    result.Orphans.UpdatedUnix.Should().Be(1760000000);
}

// AAA: битый JSON orphans → KeyParseError, Orphans null
[Fact]
public void Parse_OrphansMalformedJson_KeyParseError()
{
    // Arrange
    var kvs = new List<Kv>
    {
        new("/pgworker/backups/orphans", "not-json", 1),
    };

    // Act
    var result = BackupsParser.Parse(kvs);

    // Assert
    result.Orphans.Should().BeNull();
    result.Errors.Should().ContainSingle().Which.Key.Should().Be("/pgworker/backups/orphans");
}

// AAA: full-ключ собирает полный факт per-full для сверки (t08): state/verify/size
[Fact]
public void Parse_FullKey_CollectsFullInfo()
{
    // Arrange — COMPLETED-полный с verify OK и size_bytes
    var kvs = new List<Kv>
    {
        new("/pgworker/backups/demo/s1/full/20260913a",
            """{"state":"COMPLETED","node":"s1a","role":"replica","started_unix":100,"finished_unix":200,"wal_start_segment":"000000010000000000000001","size_bytes":123,"verify":{"state":"OK","checked_unix":201}}""", 1),
    };

    // Act
    var result = BackupsParser.Parse(kvs);

    // Assert — ShardsFulls["s1"] несёт полный факт (t08)
    result.Errors.Should().BeEmpty();
    var cluster = result.Clusters.Single(c => c.Cluster == "demo");
    cluster.ShardsFulls.Should().NotBeNull();
    var full = cluster.ShardsFulls!["s1"].Should().ContainSingle().Subject;
    full.Id.Should().Be("20260913a");
    full.State.Should().Be("COMPLETED");
    full.SizeBytes.Should().Be(123);
    full.VerifyState.Should().Be("OK");
    full.VerifyCheckedUnix.Should().Be(201);
}

// AAA: FAILED-ключ без size/verify → факт с null-полями и error из ключа
[Fact]
public void Parse_FullKey_MinimalFailed()
{
    // Arrange — ранний FAILED: ни size_bytes, ни verify
    var kvs = new List<Kv>
    {
        new("/pgworker/backups/demo/s1/full/broke",
            """{"state":"FAILED","node":"s1a","role":"replica","started_unix":100,"error":"pg_basebackup exit 1"}""", 1),
    };

    // Act
    var result = BackupsParser.Parse(kvs);

    // Assert — SizeBytes/VerifyState null, Error из ключа
    var cluster = result.Clusters.Single(c => c.Cluster == "demo");
    var full = cluster.ShardsFulls!["s1"].Should().ContainSingle().Subject;
    full.State.Should().Be("FAILED");
    full.SizeBytes.Should().BeNull();
    full.VerifyState.Should().BeNull();
    full.Error.Should().Be("pg_basebackup exit 1");
}

// AAA: wal-ключ с last_uploaded_segment → в WalStreamInfo; без поля → null (толерантно)
[Fact]
public void Parse_Wal_LastUploadedSegment()
{
    // Arrange — ключ с полем и ключ без него (старый формат)
    var kvs = new List<Kv>
    {
        new("/pgworker/backups/demo/s1/wal",
            """{"state":"ACTIVE","slot":"sub","master_node":"s1a","last_uploaded_unix":500,"last_uploaded_segment":"00000001000000000000000A"}""", 1),
        new("/pgworker/backups/demo/s2/wal",
            """{"state":"ACTIVE","slot":"sub2","master_node":"s2a","last_uploaded_unix":600}""", 2),
    };

    // Act
    var result = BackupsParser.Parse(kvs);

    // Assert — факт прочитан там, где есть; отсутствие поля — не ошибка
    result.Errors.Should().BeEmpty();
    var demo = result.Clusters.Single(c => c.Cluster == "demo");
    demo.Shards!["s1"]!.LastUploadedSegment.Should().Be("00000001000000000000000A");
    demo.Shards!["s2"]!.LastUploadedSegment.Should().BeNull();
}

// AAA: два full-ключа одного шарда → оба в списке, сортировка по Id Ordinal
[Fact]
public void Parse_MultipleFulls_SameShard()
{
    // Arrange — вставка в порядке, обратном алфавитному
    var kvs = new List<Kv>
    {
        new("/pgworker/backups/demo/s1/full/20260913b",
            """{"state":"COMPLETED","node":"n","role":"replica","started_unix":200,"finished_unix":300}""", 1),
        new("/pgworker/backups/demo/s1/full/20260913a",
            """{"state":"COMPLETED","node":"n","role":"replica","started_unix":100,"finished_unix":150}""", 2),
    };

    // Act
    var result = BackupsParser.Parse(kvs);

    // Assert — оба собраны, порядок Id Ordinal
    var fulls = result.Clusters.Single(c => c.Cluster == "demo").ShardsFulls!["s1"];
    fulls.Select(f => f.Id).Should().Equal("20260913a", "20260913b");
}

// AAA: незнакомое state полного читается КАК ЕСТЬ (панель — толерантный читатель,
// писатель — воркер); нового пути отказа не вводим
[Fact]
public void Parse_FullKey_UnknownState_Tolerated()
{
    // Arrange — state вне известного набора (воркер новее панели)
    var kvs = new List<Kv>
    {
        new("/pgworker/backups/demo/s1/full/weird1",
            """{"state":"WEIRD","node":"n","role":"replica","started_unix":100}""", 1),
    };

    // Act
    var result = BackupsParser.Parse(kvs);

    // Assert — факт собран, state без изменений, ошибок нет
    result.Errors.Should().BeEmpty();
    var full = result.Clusters.Single(c => c.Cluster == "demo").ShardsFulls!["s1"].Single();
    full.State.Should().Be("WEIRD");
}
}
