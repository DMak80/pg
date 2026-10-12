using Microsoft.Extensions.Logging.Abstractions;
using OwnS3.Replication;

namespace OwnS3.UnitTests.Replication;

// Состав mesh (t35, глава 07 §2): источник истины — targets.json тома;
// env — seed первого старта; замена — hot-apply с персистом и событием.
public sealed class ReplicationTargetsFixture : IDisposable
{
    public string Dir { get; } = Path.Combine(Path.GetTempPath(), "owns3-test-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        // Teardown: temp-каталог удаляется при любом исходе
        if (Directory.Exists(Dir))
            Directory.Delete(Dir, recursive: true);
    }
}

public class ReplicationTargetsTests : IClassFixture<ReplicationTargetsFixture>, IDisposable
{
    private readonly ReplicationTargetsFixture _fixture = new();

    private static readonly TargetOptions SeedA = new("peer-a", "http://peer-a:9000", "ak-a", "sk-a");
    private static readonly TargetOptions SeedB = new("peer-b", "http://peer-b:9000", "ak-b", "sk-b");

    public void Dispose()
    {
        // Изоляция кейсов: temp-каталог на кейс
        if (Directory.Exists(_fixture.Dir))
            Directory.Delete(_fixture.Dir, recursive: true);
    }

    private string TargetsPath => Path.Combine(_fixture.Dir, "targets.json");

    [Fact]
    public void Load_FirstStart_SeedsFileFromEnv()
    {
        // Arrange — файла нет, env-seed не пуст
        // Act
        var targets = ReplicationTargets.Load(_fixture.Dir, [SeedA], NullLogger<ReplicationTargets>.Instance);

        // Assert — seed переносится в targets.json, репликация активна
        File.Exists(TargetsPath).Should().BeTrue();
        targets.Active.Should().BeTrue();
        targets.Snapshot.Should().ContainSingle()
            .Which.Should().BeEquivalentTo(new TargetRecord(SeedA.Name, SeedA.Endpoint, SeedA.AccessKey, SeedA.SecretKey));
    }

    [Fact]
    public void Load_FileExists_EnvNotOverwritten()
    {
        // Arrange — первый старт с peer-a; второй Load с ДРУГИМ env-seed
        ReplicationTargets.Load(_fixture.Dir, [SeedA], NullLogger<ReplicationTargets>.Instance);
        var before = File.ReadAllText(TargetsPath);

        // Act — повторный Load с env peer-b
        var targets = ReplicationTargets.Load(_fixture.Dir, [SeedB], NullLogger<ReplicationTargets>.Instance);

        // Assert — источник истины файл: peer-b не перезаписал peer-a
        targets.Snapshot.Should().ContainSingle().Which.Name.Should().Be("peer-a");
        File.ReadAllText(TargetsPath).Should().Be(before);
    }

    [Fact]
    public void Load_EmptyEnvNoFile_InactiveNoFile()
    {
        // Arrange — env пуст, файла нет
        // Act
        var targets = ReplicationTargets.Load(_fixture.Dir, [], NullLogger<ReplicationTargets>.Instance);

        // Assert — репликация неактивна, файл не создан (Existing-регресс, глава 07 §2)
        targets.Active.Should().BeFalse();
        targets.Snapshot.Should().BeEmpty();
        File.Exists(TargetsPath).Should().BeFalse();
    }

    [Fact]
    public void Replace_PersistsAndRaisesChanged()
    {
        // Arrange — активный контур с peer-a; приёмник события
        var targets = ReplicationTargets.Load(_fixture.Dir, [SeedA], NullLogger<ReplicationTargets>.Instance);
        IReadOnlyList<TargetRecord>? changed = null;
        targets.TargetsChanged += snapshot => changed = snapshot;
        var replacement = new List<TargetRecord>
        {
            new("peer-b", "http://peer-b:9000", "ak-b", "sk-b"),
            new("peer-c", "http://peer-c:9000", "ak-c", "sk-c"),
        };

        // Act
        targets.Replace(replacement);

        // Assert — снапшот заменён, событие с новым составом, перезагрузка видит замену
        targets.Snapshot.Should().HaveCount(2);
        changed.Should().NotBeNull().And.HaveCount(2);
        var reloaded = ReplicationTargets.Load(_fixture.Dir, [SeedA], NullLogger<ReplicationTargets>.Instance);
        reloaded.Snapshot.Select(t => t.Name).Should().ContainInOrder("peer-b", "peer-c");
    }

    [Fact]
    public void Replace_DuplicateAccessKey_Throws()
    {
        // Arrange — два соседа с ОДНИМ accessKey (ревью t35, finding 5: accessKey —
        // идентификатор источника на приёмнике, общие креды ломают дедуп)
        var targets = ReplicationTargets.Load(_fixture.Dir, [], NullLogger<ReplicationTargets>.Instance);
        var withDuplicateKey = new List<TargetRecord>
        {
            new("peer-a", "http://a:9000", "shared-key", "s1"),
            new("peer-b", "http://b:9000", "shared-key", "s2"),
        };

        // Act
        var act = () => targets.Replace(withDuplicateKey);

        // Assert
        act.Should().Throw<ArgumentException>().WithMessage("*shared-key*");
    }

    [Theory]
    [InlineData("peer-a:9000")]      // нет схемы
    [InlineData("ftp://peer-a:9000")] // не http(s)
    [InlineData("http://")]          // нет host
    public void Replace_InvalidEndpoint_Throws(string endpoint)
    {
        // Arrange — endpoint вне scheme://host:port (ревью t35, finding 13)
        var targets = ReplicationTargets.Load(_fixture.Dir, [], NullLogger<ReplicationTargets>.Instance);
        var withBadEndpoint = new List<TargetRecord>
        {
            new("peer-x", endpoint, "ak", "sk"),
        };

        // Act
        var act = () => targets.Replace(withBadEndpoint);

        // Assert
        act.Should().Throw<ArgumentException>().WithMessage("*endpoint*");
    }

    [Fact]
    public void Replace_DuplicateName_Throws()
    {
        // Arrange — замена с дублем имени
        var targets = ReplicationTargets.Load(_fixture.Dir, [], NullLogger<ReplicationTargets>.Instance);
        var withDuplicate = new List<TargetRecord>
        {
            new("peer-a", "http://a:9000", "ak-a", "sk-a"),
            new("peer-a", "http://b:9000", "ak-b", "sk-b"),
        };

        // Act
        var act = () => targets.Replace(withDuplicate);

        // Assert — имена соседей уникальны (глава 07 §2)
        act.Should().Throw<ArgumentException>();
    }
}
