using Npgsql;
using Polly;
using Polly.Retry;

namespace PgWorker.Core.Retry;

// SQL-ретрай воркера (Npgsql): единственный в монорепо модуль ретраев.
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
