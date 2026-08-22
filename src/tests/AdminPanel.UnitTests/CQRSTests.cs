using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AdminPanel.UnitTests;

// Тесты query-диспетчера: резолв IHandler из AutoRegistration и вызов хендлера из корневого провайдера.
public class CQRSTests
{
    [Fact]
    public async Task HandleQuery_FromRootProvider_ReturnsHandlerValue()
    {
        // Arrange
        var provider = TestHost.BuildProvider();
        var handler = provider.GetRequiredService<IHandler>();

        // Act
        var result = await handler.HandleQuery<TestQuery, string>(new TestQuery(), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be("pong");
    }
}

// Тестовый запрос.
public sealed record TestQuery : IQuery<string>;

// Тестовый хендлер: scoped — диспетчер обязан корректно его резолвить из корневого провайдера.
[InjectAsScoped]
public class TestQueryHandler : IQueryHandler<TestQuery, string>
{
    public ValueTask<Result<string>> Handle(TestQuery query, CancellationToken ct)
        => new(Result<string>.Success("pong"));
}
