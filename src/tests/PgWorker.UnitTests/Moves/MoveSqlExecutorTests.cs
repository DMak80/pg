using PgWorker.Moves;

namespace PgWorker.UnitTests.Moves;

public class MoveSqlExecutorTests
{
    // Тестовый пароль DSN (литералы — только в константах).
    private const string TestDsnPassword = "secret";

    // AAA: пароль не утекает в текст ошибки (P12/P17)
    [Fact]
    public void WrapError_MasksPassword()
    {
        // Arrange
        var dsn = $"Host=h;Port=1;Database=d;Username=postgres;Password={TestDsnPassword}";

        // Act
        var failed = NpgsqlMoveSqlExecutor.WrapError(dsn, new ApplicationException("boom"));

        // Assert
        failed.Error!.Message.Should().NotContain(TestDsnPassword);
        failed.Error!.Message.Should().Contain("password=***");
    }
}
