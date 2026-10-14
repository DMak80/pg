using System.Security.Cryptography;
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
        targets.Snapshot.Should().ContainSingle().Which.Should().BeEquivalentTo(
            new TargetRecord(SeedA.Name, SeedA.Endpoint, TargetRecord.AuthLegacy, SeedA.AccessKey, SeedA.SecretKey));
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
            new("peer-b", "http://peer-b:9000", TargetRecord.AuthLegacy, "ak-b", "sk-b"),
            new("peer-c", "http://peer-c:9000", TargetRecord.AuthLegacy, "ak-c", "sk-c"),
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
            new("peer-a", "http://a:9000", TargetRecord.AuthLegacy, "shared-key", "s1"),
            new("peer-b", "http://b:9000", TargetRecord.AuthLegacy, "shared-key", "s2"),
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
            new("peer-x", endpoint, TargetRecord.AuthLegacy, "ak", "sk"),
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
            new("peer-a", "http://a:9000", TargetRecord.AuthLegacy, "ak-a", "sk-a"),
            new("peer-a", "http://b:9000", TargetRecord.AuthLegacy, "ak-b", "sk-b"),
        };

        // Act
        var act = () => targets.Replace(withDuplicate);

        // Assert — имена соседей уникальны (глава 07 §2)
        act.Should().Throw<ArgumentException>();
    }

    // ── t39: записи двух видов (auth legacy|key, глава 07 §2) ──

    [Fact]
    public void Load_OldFormatFile_ReadsAsLegacy()
    {
        // Arrange — старый targets.json (4 поля, без auth)
        Directory.CreateDirectory(_fixture.Dir);
        File.WriteAllText(TargetsPath,
            """[{"name":"peer-a","endpoint":"http://peer-a:9000","accessKey":"ak-a","secretKey":"sk-a"}]""");

        // Act
        var targets = ReplicationTargets.Load(_fixture.Dir, [], NullLogger<ReplicationTargets>.Instance);

        // Assert — читается как legacy, креды на месте (обратная совместимость формата)
        var record = targets.Snapshot.Should().ContainSingle().Which;
        record.Auth.Should().Be(TargetRecord.AuthLegacy);
        record.AccessKey.Should().Be("ak-a");
        record.SecretKey.Should().Be("sk-a");
    }

    [Fact]
    public void Replace_KeyRecord_PersistsAuthAndPem()
    {
        // Arrange — key-запись с валидным PEM публичного ключа ECDSA P-256
        var targets = ReplicationTargets.Load(_fixture.Dir, [], NullLogger<ReplicationTargets>.Instance);
        var pem = ECDsa.Create(ECCurve.NamedCurves.nistP256).ExportSubjectPublicKeyInfoPem();
        var keyTarget = new TargetRecord("peer-k", "http://peer-k:9000", TargetRecord.AuthKey, PublicKeyPem: pem);

        // Act
        targets.Replace([keyTarget]);

        // Assert — файл перечитан Load'ом: auth и PEM на месте, кредов нет
        var reloaded = ReplicationTargets.Load(_fixture.Dir, [], NullLogger<ReplicationTargets>.Instance);
        var record = reloaded.Snapshot.Should().ContainSingle().Which;
        record.Auth.Should().Be(TargetRecord.AuthKey);
        record.PublicKeyPem.Should().Be(pem);
        record.AccessKey.Should().BeNull();
        record.SecretKey.Should().BeNull();
    }

    [Fact]
    public void Replace_KeyRecord_InvalidPem_Throws()
    {
        // Arrange — key-запись с мусорным PEM
        var targets = ReplicationTargets.Load(_fixture.Dir, [], NullLogger<ReplicationTargets>.Instance);
        var bad = new TargetRecord("peer-k", "http://k:9000", TargetRecord.AuthKey, PublicKeyPem: "not-a-pem");

        // Act
        var act = () => targets.Replace([bad]);

        // Assert — key-запись требует валидный PEM (глава 07 §2)
        act.Should().Throw<ArgumentException>().WithMessage("*peer-k*");
    }

    [Fact]
    public void Replace_KeyRecord_MissingPublicKey_Throws()
    {
        // Arrange — key-запись без publicKeyPem
        var targets = ReplicationTargets.Load(_fixture.Dir, [], NullLogger<ReplicationTargets>.Instance);
        var bad = new TargetRecord("peer-k", "http://k:9000", TargetRecord.AuthKey);

        // Act
        var act = () => targets.Replace([bad]);

        // Assert
        act.Should().Throw<ArgumentException>().WithMessage("*peer-k*");
    }

    [Fact]
    public void Replace_LegacyRecord_MissingCreds_Throws()
    {
        // Arrange — legacy-запись без пары кредов
        var targets = ReplicationTargets.Load(_fixture.Dir, [], NullLogger<ReplicationTargets>.Instance);
        var bad = new TargetRecord("peer-a", "http://a:9000", TargetRecord.AuthLegacy);

        // Act
        var act = () => targets.Replace([bad]);

        // Assert — legacy требует пару accessKey/secretKey (глава 07 §2)
        act.Should().Throw<ArgumentException>().WithMessage("*peer-a*");
    }

    [Fact]
    public void Replace_MixedList_AccessKeyUniqueAmongLegacyOnly()
    {
        // Arrange — две key-записи с пустым accessKey (дедуп их не касается)
        var targets = ReplicationTargets.Load(_fixture.Dir, [], NullLogger<ReplicationTargets>.Instance);
        var pem1 = ECDsa.Create(ECCurve.NamedCurves.nistP256).ExportSubjectPublicKeyInfoPem();
        var pem2 = ECDsa.Create(ECCurve.NamedCurves.nistP256).ExportSubjectPublicKeyInfoPem();
        var keys = new List<TargetRecord>
        {
            new("k1", "http://k1:9000", TargetRecord.AuthKey, PublicKeyPem: pem1),
            new("k2", "http://k2:9000", TargetRecord.AuthKey, PublicKeyPem: pem2),
        };

        // Act / Assert — key-записи проходят без accessKey
        var actKeys = () => targets.Replace(keys);
        actKeys.Should().NotThrow();

        // Arrange — дубль accessKey среди legacy — отказ (идентификатор источника)
        var legacies = new List<TargetRecord>
        {
            new("l1", "http://l1:9000", TargetRecord.AuthLegacy, "shared", "s1"),
            new("l2", "http://l2:9000", TargetRecord.AuthLegacy, "shared", "s2"),
        };

        // Act / Assert
        var actLegacy = () => targets.Replace(legacies);
        actLegacy.Should().Throw<ArgumentException>().WithMessage("*shared*");
    }

    [Fact]
    public void AddOrReplace_NewTarget_PersistsAndRaisesChanged()
    {
        // Arrange — активный контур с peer-a; приёмник события замены состава
        var targets = ReplicationTargets.Load(_fixture.Dir, [SeedA], NullLogger<ReplicationTargets>.Instance);
        IReadOnlyList<TargetRecord>? changed = null;
        targets.TargetsChanged += snapshot => changed = snapshot;
        var pem = ECDsa.Create(ECCurve.NamedCurves.nistP256).ExportSubjectPublicKeyInfoPem();

        // Act — добавление key-соседа (join, глава 07 §2)
        targets.AddOrReplace(new TargetRecord("peer-k", "http://peer-k:9000", TargetRecord.AuthKey,
            PublicKeyPem: pem));

        // Assert — снапшот дополнен, событие поднято, файл перечитан Load'ом
        targets.Snapshot.Should().HaveCount(2);
        changed.Should().NotBeNull().And.HaveCount(2);
        var reloaded = ReplicationTargets.Load(_fixture.Dir, [SeedA], NullLogger<ReplicationTargets>.Instance);
        reloaded.Snapshot.Should().Contain(t => t.Name == "peer-k" && t.Auth == TargetRecord.AuthKey);
    }

    [Fact]
    public void AddOrReplace_ExistingName_ReplacesEndpointAndKey()
    {
        // Arrange — контур с peer-a (legacy)
        var targets = ReplicationTargets.Load(_fixture.Dir, [SeedA], NullLogger<ReplicationTargets>.Instance);
        var pem = ECDsa.Create(ECCurve.NamedCurves.nistP256).ExportSubjectPublicKeyInfoPem();

        // Act — вторая запись тем же именем
        targets.AddOrReplace(new TargetRecord("peer-a", "http://peer-a-new:9000", TargetRecord.AuthKey,
            PublicKeyPem: pem));

        // Assert — запись заменена (не дубль), endpoint и вид обновлены
        targets.Snapshot.Should().ContainSingle();
        targets.Snapshot[0].Endpoint.Should().Be("http://peer-a-new:9000");
        targets.Snapshot[0].Auth.Should().Be(TargetRecord.AuthKey);
    }

    [Fact]
    public void Remove_Existing_TrueAndGone()
    {
        // Arrange — соседи peer-a/peer-b; у peer-b курсор приёма на диске
        var targets = ReplicationTargets.Load(_fixture.Dir, [SeedA, SeedB],
            NullLogger<ReplicationTargets>.Instance);
        var cursorsDir = Path.Combine(_fixture.Dir, "cursors");
        Directory.CreateDirectory(cursorsDir);
        var cursorFile = Path.Combine(cursorsDir, "peer-b.json");
        File.WriteAllText(cursorFile, """{"acceptedSeqNo":42,"mode":"tail"}""");

        // Act
        var removed = targets.Remove("peer-b");

        // Assert — удалён из снапшота и файла; курсор соседа жив
        // (возврат соседа продолжится с места, глава 07 §2)
        removed.Should().BeTrue();
        targets.Snapshot.Select(t => t.Name).Should().ContainSingle().Which.Should().Be("peer-a");
        var reloaded = ReplicationTargets.Load(_fixture.Dir, [SeedA], NullLogger<ReplicationTargets>.Instance);
        reloaded.Snapshot.Select(t => t.Name).Should().NotContain("peer-b");
        File.Exists(cursorFile).Should().BeTrue();
    }

    [Fact]
    public void Remove_Unknown_False()
    {
        // Arrange — контур с peer-a
        var targets = ReplicationTargets.Load(_fixture.Dir, [SeedA], NullLogger<ReplicationTargets>.Instance);
        var before = File.ReadAllText(TargetsPath);

        // Act
        var removed = targets.Remove("no-such");

        // Assert — false, персист не менялся
        removed.Should().BeFalse();
        File.ReadAllText(TargetsPath).Should().Be(before);
    }
}
