using System.Text.RegularExpressions;
using PgWorker.Core.Model;
using Xunit;

namespace PgWorker.UnitTests.Model;

// Генератор per-cluster app-пароля (spec §4.1): 32 символа [A-Za-z0-9].
public partial class AppSecretGeneratorTests
{
    [GeneratedRegex("^[A-Za-z0-9]{32}$")]
    private static partial Regex PasswordPattern();

    [Fact]
    public void Generate_LengthAndAlphabet()
    {
        // Act
        var password = AppSecretGenerator.Generate();

        // Assert — 32 символа, только буквы/цифры (без спецсимволов: безопасно
        // для SQL-литералов, DSN, env, JSON — spec §4.1)
        PasswordPattern().IsMatch(password).Should().BeTrue();
    }

    [Fact]
    public void Generate_UniqueAcrossRuns()
    {
        // Arrange / Act — 100 генераций
        var generated = Enumerable.Range(0, 100)
            .Select(_ => AppSecretGenerator.Generate())
            .ToHashSet();

        // Assert — все различны (криптостойкий источник, не константа)
        generated.Should().HaveCount(100);
    }
}
