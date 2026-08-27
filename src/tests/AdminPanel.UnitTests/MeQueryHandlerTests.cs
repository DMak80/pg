using AdminPanel.Api.Auth;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Тест хендлера запроса текущей сессии (spec t02 §9.3).
public class MeQueryHandlerTests
{
    [Fact]
    public async Task Handle_ReturnsUsernameFromQuery()
    {
        // Arrange
        var handler = new MeQueryHandler();

        // Act
        var result = await handler.Handle(new MeQuery("admin"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Username.Should().Be("admin");
    }
}
