using PgWorker.Moves;

namespace PgWorker.UnitTests.Moves;

public class MoveSqlExecutorTests
{
    // AAA: пароль не утекает в текст ошибки (P12/P17)
    [Fact]
    public void WrapError_MasksPassword()
    {
        // Arrange
        var dsn = "Host=h;Port=1;Database=d;Username=postgres;Password=secret";

        // Act
        var failed = NpgsqlMoveSqlExecutor.WrapError(dsn, new ApplicationException("boom"));

        // Assert
        failed.Error!.Message.Should().NotContain("secret");
        failed.Error!.Message.Should().Contain("password=***");
    }
}
