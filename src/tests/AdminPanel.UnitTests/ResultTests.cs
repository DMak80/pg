using FluentAssertions;
using AdminPanel.Infrastructure;
using Xunit;

namespace AdminPanel.UnitTests;

// Тесты монады Result, перенесённой из референса Puzzle.
public class ResultTests
{
    [Fact]
    public void Success_IsSuccessTrue_AndMatchChoosesSuccessBranch()
    {
        // Arrange
        var result = Result<string>.Success("value");

        // Act
        var matched = result.Match(v => $"ok:{v}", e => $"err:{e.Message}");

        // Assert
        result.IsSuccess.Should().BeTrue();
        matched.Should().Be("ok:value");
    }

    [Fact]
    public void Failed_IsSuccessFalse_AndMatchChoosesFailureBranch()
    {
        // Arrange
        var result = Result<string>.Failed(new InvalidOperationException("boom"));

        // Act
        var matched = result.Match(v => $"ok:{v}", e => $"err:{e.Message}");

        // Assert
        result.IsSuccess.Should().BeFalse();
        matched.Should().Be("err:boom");
    }

    [Fact]
    public void Bind_OnSuccess_ChainsNextResult()
    {
        // Arrange
        var result = Result<int>.Success(2);

        // Act
        var bound = result.Bind(v => Result<string>.Success($"v={v * 2}"));

        // Assert
        bound.IsSuccess.Should().BeTrue();
        bound.Value.Should().Be("v=4");
    }

    [Fact]
    public void Map_OnFailure_PropagatesError()
    {
        // Arrange
        var error = new InvalidOperationException("boom");
        var result = Result<int>.Failed(error);

        // Act
        var mapped = result.Map(v => v * 2);

        // Assert
        mapped.IsSuccess.Should().BeFalse();
        mapped.Error.Should().BeSameAs(error);
    }

    [Fact]
    public void From_WithThrowingDelegate_ReturnsFailure()
    {
        // Arrange
        // Act
        var result = Result<int>.From(() => throw new ArgumentException("bad"));

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<ArgumentException>();
    }

    [Fact]
    public void FromValue_WithNull_ReturnsFailure()
    {
        // Arrange
        // Act
        var result = Result.FromValue((string?)null, "no value");

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Be("no value");
    }

    [Fact]
    public void CollBind_StopsOnFirstFailure()
    {
        // Arrange
        var items = new[] { 1, 2, 3 };
        var visited = 0;
        var target = new InvalidOperationException("stop");

        // Act
        var result = items.CollBind<int>(i =>
        {
            visited++;
            return i == 2 ? Result.Failed(target) : Result.Success();
        });

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeSameAs(target);
        visited.Should().Be(2);
    }
}
