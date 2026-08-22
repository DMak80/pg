using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Npgsql;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Testcontainers postgres:18 (spec §9.6): trust-стенд + wal_level=logical ради живых
// логических слотов; готовность — ретрай-подключение Npgsql (паттерн EtcdContainerFixture).
// IClassFixture — контейнер на тестовый класс, изоляция между классами.
public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly IContainer _container = new ContainerBuilder("postgres:18")
        .WithEnvironment("POSTGRES_HOST_AUTH_METHOD", "trust")
        .WithCommand("postgres", "-c", "wal_level=logical")
        .WithPortBinding(5432, assignRandomHostPort: true)
        .Build();

    public int Port { get; private set; }

    public string ConnectionString => $"Host=127.0.0.1;Port={Port};Username=postgres;Timeout=5";

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await _container.StartAsync(ct);
        Port = _container.GetMappedPublicPort(5432);

        for (var i = 0; i < 30; i++)
        {
            try
            {
                await using var connection = new NpgsqlConnection(ConnectionString);
                await connection.OpenAsync(ct);
                return;
            }
            catch (NpgsqlException)
            {
                // postgres ещё поднимается — ждём следующую попытку
                await Task.Delay(1000, ct);
            }
        }

        throw new InvalidOperationException("postgres:18 не поднялся за 30 c");
    }

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();
}
