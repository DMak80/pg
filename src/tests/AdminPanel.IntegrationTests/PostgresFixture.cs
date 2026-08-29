using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Npgsql;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Testcontainers postgres:18 (spec §9.6): trust-стенд + wal_level=logical ради живых
// логических слотов; готовность — ретрай-подключение Npgsql (паттерн EtcdContainerFixture).
// IClassFixture — контейнер на тестовый класс, изоляция между классами.
// TLS: самоподписанный сертификат генерируется на старте контейнера (openssl
// есть в debian-образе postgres); ssl=on — как у Spilo-нод стенда, к которым
// подключается SqlProbe с SslMode.Require. Npgsql Require цепочку не проверяет
// (Require ≠ Verify) — самоподписанный сертификат достаточен.
public sealed class PostgresFixture : IAsyncLifetime
{
    // Команда выполняется от root (это не "postgres"), поэтому сертификат
    // генерируем здесь же и передаём владением postgres (ключ 600 — требование
    // PG); сервер стартуем через штатный docker-entrypoint.sh: он выполнит
    // initdb и exec postgres от пользователя postgres с нашими -c опциями.
    private const string StartWithSsl =
        """
        openssl req -new -x509 -days 1 -nodes -subj /CN=localhost \
          -keyout /tmp/server.key -out /tmp/server.crt \
          && chown postgres:postgres /tmp/server.key /tmp/server.crt \
          && chmod 600 /tmp/server.key \
          && exec docker-entrypoint.sh postgres -c ssl=on \
               -c ssl_cert_file=/tmp/server.crt \
               -c ssl_key_file=/tmp/server.key \
               -c wal_level=logical
        """;

    private readonly IContainer _container = new ContainerBuilder("postgres:18")
        .WithEnvironment("POSTGRES_HOST_AUTH_METHOD", "trust")
        .WithCommand("bash", "-c", StartWithSsl)
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
