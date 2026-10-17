namespace PgWorker.UnitTests.Backups;

// BackupSecretProfileStore (t36, arch/19 §7): env-профили кредов пулов
// PGW_BACKUP_S3_PROFILES__<P>__{ACCESS_KEY,SECRET_KEY}; диагностики — только
// имена, никогда значения.
public class BackupSecretProfileStoreTests
{
    // AAA: полный профиль → пара access/secret.
    [Fact]
    public void TryGet_FullProfile_ReturnsPair()
    {
        // Arrange — обе переменные профиля backups-main.
        var store = new BackupSecretProfileStore(
            getenv: name => name switch
            {
                "PGW_BACKUP_S3_PROFILES__backups-main__ACCESS_KEY" => "ak",
                "PGW_BACKUP_S3_PROFILES__backups-main__SECRET_KEY" => "sk",
                _ => null,
            },
            envNames:
            [
                "PGW_BACKUP_S3_PROFILES__backups-main__ACCESS_KEY",
                "PGW_BACKUP_S3_PROFILES__backups-main__SECRET_KEY",
            ]);

        // Act
        var ok = store.TryGet("backups-main", out var accessKey, out var secretKey, out var diagnostic);

        // Assert
        ok.Should().BeTrue();
        accessKey.Should().Be("ak");
        secretKey.Should().Be("sk");
        diagnostic.Should().BeNull();
    }

    // AAA: профиль не задан → false; diagnostic содержит имя профиля и перечень
    // ProfileNames, НЕ содержит значений.
    [Fact]
    public void TryGet_MissingProfile_DiagnosticWithoutValues()
    {
        // Arrange — есть чужой профиль, запрашиваем отсутствующий.
        var store = new BackupSecretProfileStore(
            getenv: _ => null,
            envNames: ["PGW_BACKUP_S3_PROFILES__other__ACCESS_KEY", "PGW_BACKUP_S3_PROFILES__other__SECRET_KEY"]);

        // Act
        var ok = store.TryGet("backups-main", out var accessKey, out var secretKey, out var diagnostic);

        // Assert — значения не утекают: accessKey/secretKey null.
        ok.Should().BeFalse();
        accessKey.Should().BeNull();
        secretKey.Should().BeNull();
        diagnostic.Should().Contain("backups-main");
        diagnostic.Should().Contain("other");
        diagnostic.Should().NotContain("ak").And.NotContain("sk");
    }

    // AAA: только ACCESS_KEY (без SECRET_KEY) — имя профиля в списке полупар.
    [Fact]
    public void ValidateEnvPairs_HalfPair()
    {
        // Arrange — полупара профиля broken + полный профиль good.
        var store = new BackupSecretProfileStore(
            envNames:
            [
                "PGW_BACKUP_S3_PROFILES__broken__ACCESS_KEY",
                "PGW_BACKUP_S3_PROFILES__good__ACCESS_KEY",
                "PGW_BACKUP_S3_PROFILES__good__SECRET_KEY",
            ]);

        // Act
        var broken = store.ValidateEnvPairs();

        // Assert
        broken.Should().Equal("broken");
    }

    // AAA: два профиля → отсортированы по Ordinal.
    [Fact]
    public void ProfileNames_Sorted()
    {
        // Arrange — имена в env в «перевёрнутом» порядке.
        var store = new BackupSecretProfileStore(
            envNames:
            [
                "PGW_BACKUP_S3_PROFILES__zeta__ACCESS_KEY",
                "PGW_BACKUP_S3_PROFILES__zeta__SECRET_KEY",
                "PGW_BACKUP_S3_PROFILES__alpha__ACCESS_KEY",
                "PGW_BACKUP_S3_PROFILES__alpha__SECRET_KEY",
                "PGW_SNAPSHOTS_S3_ENDPOINT", // чужой префикс — не профиль
            ]);

        // Act
        var names = store.ProfileNames;

        // Assert
        names.Should().Equal("alpha", "zeta");
    }
}
