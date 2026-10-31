using PgWorker.App;
using PgWorker.Backups.EtcdExport;

// FluentAssertions и PgWorker.Backups — в GlobalUsings.cs проекта.

namespace PgWorker.UnitTests.App;

// Фабрика цели слепков (arch/14 §6/§8): пул-клиент собирается из ОТДЕЛЬНОЙ
// пары Snapshots:Export:S3 (env PGW_SNAPSHOTS_S3_ENDPOINTS — запятая-список
// URI; перебор целей до первого доступного — arch/19 §5) — не из Backups:S3
// (источник правды о DR-слепках не живёт в восстанавливаемом etcd).
public class SnapshotSinkFactoryTests
{
    // Тестовая пара S3-ключей слепков (литералы — только в константах).
    private const string TestAccessKey = "snap-ak";
    private const string TestSecretKey = "snap-sk";

    // Полная тестовая пара с настраиваемым списком целей.
    private static SnapshotExportS3Options Pair(string endpoints) => new()
    {
        Endpoints = endpoints,
        Bucket = "pgworker-etcd-snapshots",
        AccessKey = TestAccessKey,
        SecretKey = TestSecretKey,
        Region = "us-east-1",
        CaBundlePath = "/tls/ca.pem",
    };

    // AAA: разбор списка «a,b» — две цели в порядке перечисления (порядок = приоритет перебора).
    [Fact]
    public void ParseEndpoints_TwoUris_TwoTargetsInOrder()
    {
        // Act
        var targets = SnapshotExportS3Options.ParseEndpoints("http://h1:9000,http://h2:9000");

        // Assert
        targets.Should().Equal("http://h1:9000", "http://h2:9000");
    }

    // AAA: одиночный URI — частный случай; пробелы/пустые элементы отбрасываются.
    [Theory]
    [InlineData("http://only:9000", "http://only:9000")]
    [InlineData(" http://h1:9000 , , http://h2:9000 ,", "http://h1:9000|http://h2:9000")]
    [InlineData(" , ", "")]
    public void ParseEndpoints_SingleAndJunk_CleanedList(string raw, string expected)
    {
        // Act
        var targets = SnapshotExportS3Options.ParseEndpoints(raw);

        // Assert
        targets.Should().Equal(expected.Split('|', StringSplitOptions.RemoveEmptyEntries));
    }

    // AAA: дубликат URI схлопывается (дедуп, arch/14 §6) с сохранением порядка
    // ПЕРВОГО вхождения — копипаст-ошибка не крашит сборку per-целей и не
    // меняет приоритет перебора.
    [Fact]
    public void ParseEndpoints_Duplicates_DedupedKeepingFirstOrder()
    {
        // Act
        var targets = SnapshotExportS3Options.ParseEndpoints(
            "http://h2:9000,http://h1:9000,http://h2:9000, http://h1:9000 ");

        // Assert — h2 один раз (первое вхождение раньше h1), дубликаты h1 схлопнуты.
        targets.Should().Equal("http://h2:9000", "http://h1:9000");
    }

    // AAA: per-цельные опции — bucket/креды/Region/CA ОДНИ на все цели пары,
    // Endpoint — каждой цели свой.
    [Fact]
    public void TargetOptions_PairFieldsAppliedToEveryTarget()
    {
        // Act
        var perTarget = EtcdSnapshotSinkFactory.TargetOptions(Pair("http://h1:9000,http://h2:9000"));

        // Assert
        perTarget.Should().HaveCount(2);
        perTarget.Select(t => t.Endpoint).Should().Equal("http://h1:9000", "http://h2:9000");
        foreach (var target in perTarget)
        {
            target.Bucket.Should().Be("pgworker-etcd-snapshots");
            target.AccessKey.Should().Be(TestAccessKey);
            target.SecretKey.Should().Be(TestSecretKey);
            target.Region.Should().Be("us-east-1");
            target.CaBundlePath.Should().Be("/tls/ca.pem");
            // Клиентские ретраи AWSSDK выключены на per-целях — перебор несёт
            // пул (arch/14 §6): ретрай клиента на мёртвой цели съедает бюджет
            // экспорта (30 с) до перебора следующих целей.
            target.MaxErrorRetry.Should().Be(0);
        }
    }

    // AAA: Create — пул-клиент PoolBackupS3 (носитель перебора arch/19 §5;
    // write=read=список пары — по построению фабрики). CA не задан — per-цели
    // собираются без чтения файла.
    [Fact]
    public void Create_ReturnsPoolBackupS3()
    {
        // Arrange
        var opts = new PgWorkerOptions();
        var pair = Pair("http://h1:9000,http://h2:9000");
        pair.CaBundlePath = null;
        opts.Snapshots.Export.S3 = pair;
        opts.Backups.S3 = new BackupsS3Options { Bucket = "pgworker-backups" };

        // Act
        var target = EtcdSnapshotSinkFactory.Create(opts);

        // Assert
        target.Should().BeOfType<PoolBackupS3>();
    }

    // AAA (п.1 ревью): per-цели строятся EAGERLY в Create — нечитаемый CA-файл
    // пары роняет СБОРКУ пула на старте хоста с диагностикой BackupCaPem
    // (fail-fast «битый файл это дефект инсталляции»), а не первую попытку
    // экспорта (там исключение глоталось бы молча или роняло хост в crash-loop).
    [Fact]
    public void Create_UnreadableCaBundle_ThrowsAtBuildTime()
    {
        // Arrange — путь CA не существует (дефект инсталляции)
        var opts = new PgWorkerOptions();
        opts.Snapshots.Export.S3 = Pair("http://h1:9000");

        // Act
        var act = () => EtcdSnapshotSinkFactory.Create(opts);

        // Assert — диагностика BackupCaPem именно на сборке фабрики
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*CA file is not readable*");
    }
}
