using PgWorker.PgNode;
using Xunit;

// AAA: EnvironmentFile — KEY=VALUE-строки source-able env-файла tmpfs (spec
// §3.5): safe-алфавит без кавычек; прочее — sh-экранирование одинарными.
public class EnvironmentFileTests
{
    [Fact]
    public void SafeValue_WrittenAsIs()
    {
        // Arrange: значение из алфавита [A-Za-z0-9._-].
        const string value = "pass.Word-123_456";

        // Act: генерация строки env-файла.
        var line = EnvironmentFile.GenerateEnvironmentVariable("PGPASSWORD_SUPERUSER", value);

        // Assert: без кавычек (sh source-able как есть).
        line.Should().Be("PGPASSWORD_SUPERUSER=pass.Word-123_456");
    }

    [Theory]
    [InlineData("pa;ss")]
    [InlineData("pa=ss")]
    [InlineData("pa ss")]
    [InlineData("pa'ss")]
    public void UnsafeValue_QuotedSingle(string value)
    {
        // Arrange: значение с `;`/`=`/пробелом/апострофом (вне safe-алфавита).

        // Act: генерация строки.
        var line = EnvironmentFile.GenerateEnvironmentVariable("PGPASSWORD_SUPERUSER", value);

        // Assert: в одинарных кавычках (sh-экранирование — защитный слой,
        // per-install пароли DSN-safe по InstallSecretsGuard).
        line.Should().StartWith("PGPASSWORD_SUPERUSER='").And.EndWith("'");
    }

    [Fact]
    public void Apostrophe_Escaped()
    {
        // Arrange: значение с апострофом.
        const string value = "pa'ss";

        // Act: генерация строки.
        var line = EnvironmentFile.GenerateEnvironmentVariable("K", value);

        // Assert: апостроф экранирован каноническим sh-паттерном '\''.
        line.Should().Be("K='pa'\\''ss'");
    }
}
