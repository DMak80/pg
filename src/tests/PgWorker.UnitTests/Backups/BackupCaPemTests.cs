using PgWorker.Backups;

namespace PgWorker.UnitTests.Backups;

// CA-PEM для контейнеров (arch/19 §7): без пути — null; файл — Trim-PEM;
// нет файла/пустой — исключение с путём в диагностике.
public class BackupCaPemTests
{
    // AAA: путь не задан — null (http-endpoint, прежний env-набор)
    [Fact]
    public void NoPath_ReturnsNull()
    {
        BackupCaPem.ReadIfConfigured(new BackupsRuntimeOptions()).Should().BeNull();
    }

    // AAA: валидный файл — содержимое с Trim
    [Fact]
    public void ValidFile_ReturnsTrimmedPem()
    {
        var path = Path.Combine(Path.GetTempPath(), $"ca-{Guid.NewGuid():N}.pem");
        File.WriteAllText(path, "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----\n");
        BackupCaPem.ReadIfConfigured(new BackupsRuntimeOptions { S3CaBundlePath = path })
            .Should().Be("-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----");
    }

    // AAA: файла нет — исключение с путём
    [Fact]
    public void MissingFile_Throws()
    {
        var path = Path.Combine(Path.GetTempPath(), $"ca-{Guid.NewGuid():N}.pem");
        var act = () => BackupCaPem.ReadIfConfigured(new BackupsRuntimeOptions { S3CaBundlePath = path });
        act.Should().Throw<InvalidOperationException>().WithMessage("*not readable*");
    }

    // AAA: пустой файл — исключение
    [Fact]
    public void EmptyFile_Throws()
    {
        var path = Path.Combine(Path.GetTempPath(), $"ca-{Guid.NewGuid():N}.pem");
        File.WriteAllText(path, "  \n");
        var act = () => BackupCaPem.ReadIfConfigured(new BackupsRuntimeOptions { S3CaBundlePath = path });
        act.Should().Throw<InvalidOperationException>().WithMessage("*empty*");
    }
}
