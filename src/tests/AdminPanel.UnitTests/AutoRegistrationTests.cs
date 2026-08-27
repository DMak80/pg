using AdminPanel.Infrastructure.DI;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Тесты attribute-DI: авто-регистрация сервисов и биндинг [Config]-POCO.
public class AutoRegistrationTests
{
    [Fact]
    public void InjectAsSingleton_RegistersTypeAndInterface()
    {
        // Arrange
        var provider = TestHost.BuildProvider();

        // Act
        var bySelf = provider.GetRequiredService<SingletonService>();
        var byInterface = provider.GetRequiredService<ISingletonService>();

        // Assert
        bySelf.Should().BeSameAs(byInterface);
    }

    [Fact]
    public void Config_BindsPocoFromConfiguration()
    {
        // Arrange
        var provider = TestHost.BuildProvider();

        // Act
        var options = provider.GetRequiredService<IOptions<TestConfigOptions>>();

        // Assert
        options.Value.Value.Should().Be("test-value");
    }
}

// Тестовый сервис с singleton-регистрацией через атрибут.
[InjectAsSingleton]
public class SingletonService : ISingletonService;

public interface ISingletonService;

// Тестовый [Config]-POCO: секция "TestConfigOptions" из in-memory конфигурации TestHost.
[Config]
public class TestConfigOptions
{
    public string? Value { get; set; }
}
