using System.Collections.Concurrent;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace OwnS3.IntegrationTests.Api;

// In-memory хост ownS3 (решение пользователя 3): без докера/портов; конфигурация
// — in-memory секция OwnS3 с root-парой и тремя статическими ключами (роли
// read-only/read-write/admin). Тестовые креды фиксированы литералами.
// Дополнительно собирает записи структурного лога (потокобезопасно).
public sealed class OwnS3AppFactory : WebApplicationFactory<Program>
{
    public const string RootAccessKey = "testroot";
    public const string RootSecretKey = "testrootsecret";
    public const string ReaderAccessKey = "testreader";
    public const string ReaderSecretKey = "testreadersecret";
    public const string WriterAccessKey = "testwriter";
    public const string WriterSecretKey = "testwritersecret";
    public const string AdminAccessKey = "testadmin";
    public const string AdminSecretKey = "testadminsecret";

    private readonly ConcurrentQueue<(LogLevel Level, string Message)> _logEntries = new();

    public IReadOnlyList<(LogLevel Level, string Message)> LogEntries => [.. _logEntries];

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["OwnS3:Root:User"] = "root",
            ["OwnS3:Root:Password"] = "rootpassword",
            ["OwnS3:HostId"] = "owns3-test",
            ["OwnS3:AccessKeys:0:AccessKey"] = ReaderAccessKey,
            ["OwnS3:AccessKeys:0:SecretKey"] = ReaderSecretKey,
            ["OwnS3:AccessKeys:0:Policy"] = "read-only",
            ["OwnS3:AccessKeys:1:AccessKey"] = WriterAccessKey,
            ["OwnS3:AccessKeys:1:SecretKey"] = WriterSecretKey,
            ["OwnS3:AccessKeys:1:Policy"] = "read-write",
            ["OwnS3:AccessKeys:2:AccessKey"] = AdminAccessKey,
            ["OwnS3:AccessKeys:2:SecretKey"] = AdminSecretKey,
            ["OwnS3:AccessKeys:2:Policy"] = "admin",
        }));
        builder.ConfigureLogging(logging =>
        {
            logging.Services.AddLogging();
            logging.Services.AddSingleton<ILoggerProvider>(new CollectorLoggerProvider(_logEntries));
        });
    }

    // Провайдер-коллектор логов для проверки структурного лога запросов.
    private sealed class CollectorLoggerProvider(ConcurrentQueue<(LogLevel, string)> entries) : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => new CollectorLogger(entries);

        public void Dispose() { }

        private sealed class CollectorLogger(ConcurrentQueue<(LogLevel, string)> entries) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                var detail = formatter(state, exception)
                    + (exception is null ? string.Empty : $" EX[{exception.GetType().Name}]: {exception.Message}");
                entries.Enqueue((logLevel, detail));
            }
        }
    }
}

// Коллекция-фикстура: один хост на все сценарные классы (паттерн KafkaApiCollection).
[CollectionDefinition(Name)]
public sealed class OwnS3TestCollection : ICollectionFixture<OwnS3AppFactory>
{
    public const string Name = "OwnS3";
}
