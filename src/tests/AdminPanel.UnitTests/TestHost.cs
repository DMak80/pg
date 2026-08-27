using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.DI;
using AdminPanel.Infrastructure.Traces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AdminPanel.UnitTests;

// Единая точка DI-регистрации тестовой сборки: скан сборок выполняется ровно один раз
// (ServiceCollectionExtensions кеширует просканированные сборки в статическом состоянии).
public static class TestHost
{
    private static readonly Lazy<ServiceCollection> Services = new(CreateCollection);

    public static IServiceProvider BuildProvider()
        => Services.Value.BuildServiceProvider();

    private static ServiceCollection CreateCollection()
    {
        // Инициализация ActivitySource до первого HandleQuery — иначе NRE в Tracing.
        Tracing.Init("AdminPanel.UnitTests");

        // Arrange-часть всех DI-тестов: in-memory конфигурация с тестовой секцией.
        var configuration = new ConfigurationBuilder()
           .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["TestConfigOptions:Value"] = "test-value",
            })
           .Build();

        var services = new ServiceCollection();
        services.UseDiBehaviours(configuration);
        services.AutoRegistration(typeof(TestHost).Assembly);
        // Скан сборки каркаса: ServiceProviderHelper, IHandler (с Task 4) и будущие сервисы.
        services.AddInfrastructure();
        return services;
    }
}
