using FluentAssertions;
using KafkaWorker.Core;

namespace KafkaWorker.UnitTests;

// Smoke-тесты Result: сам файл — копия проверенного кода Puzzle, проверяем
// ключевые сценарии From при успехе и ошибке.

public class ResultTests
{
    [Fact]
    public void From_SuccessAction_ReturnsSuccess()
    {
        // Arrange: действие без исключения.
        var sideEffect = 0;

        // Act: оборачиваем успешное действие в Result.
        var result = Result.From(() => sideEffect = 42);

        // Assert: результат успешен, побочный эффект применён.
        result.IsSuccess.Should().BeTrue();
        result.Error.Should().BeNull();
        sideEffect.Should().Be(42);
    }

    [Fact]
    public void From_FailingAction_ReturnsError()
    {
        // Arrange: действие, бросающее исключение.
        var expected = new InvalidOperationException("сбой действия");

        // Act: оборачиваем падающее действие в Result.
        var result = Result.From(() => throw expected);

        // Assert: результат неуспешен, ошибка — исходное исключение.
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeSameAs(expected);
    }
}
