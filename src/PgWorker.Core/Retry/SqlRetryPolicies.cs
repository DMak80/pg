using Npgsql;
using Polly;
using Polly.Retry;

namespace PgWorker.Core.Retry;

// SQL-ретрай воркера (Npgsql): HttpRetry/GeneralRetry — общие в Shared.Core.Retry
// (t08), локальный класс переименован из RetryPolicies, чтобы не пересекаться
// именем с общим.
public static class SqlRetryPolicies
{
    // Транзиентные ошибки Npgsql (обрывы соединения, timeout) и отмены задач;
    // политику применяет вызывающий код к коротким операциям.
    public static ResiliencePipeline SqlRetry(int retryCount, TimeSpan medianFirstRetryDelay) =>
        new ResiliencePipelineBuilder()
           .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = retryCount,
                UseJitter = true,
                Delay = medianFirstRetryDelay,
                ShouldHandle = new PredicateBuilder()
                   .Handle<NpgsqlException>()
                   .Handle<TaskCanceledException>(),
            })
           .Build();
}
