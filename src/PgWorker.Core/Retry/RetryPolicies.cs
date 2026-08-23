using System.Net.Http;
using Npgsql;
using Polly;
using Polly.Retry;

namespace PgWorker.Core.Retry;

/// <summary>
/// Джиттер-ретраи (Polly 8, ResiliencePipeline). Адаптация RetryPolicies из Puzzle
/// (PuzzleServer.Infrastructure.App) на v8-API: вместо Policy.Handle —
/// PredicateBuilder + RetryStrategyOptions, DecorrelatedJitterBackoffV2-эффект
/// даёт комбинация Exponential + UseJitter.
/// </summary>
public static class RetryPolicies
{
    // HTTP-ретрай: сетевые ошибки (запрос не дошёл/оборван), не HTTP-статусы.
    public static ResiliencePipeline<HttpResponseMessage> HttpRetry(
        int retryCount, TimeSpan medianFirstRetryDelay) =>
        new ResiliencePipelineBuilder<HttpResponseMessage>()
           .AddRetry(new RetryStrategyOptions<HttpResponseMessage>
            {
                MaxRetryAttempts = retryCount,
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                Delay = medianFirstRetryDelay,
                ShouldHandle = new PredicateBuilder<HttpResponseMessage>()
                   .Handle<HttpRequestException>()
                   .Handle<TaskCanceledException>(),
            })
           .Build();

    // SQL-ретрай: транзиентные ошибки Npgsql (обрывы соединения, timeout)
    // и отмены задач; политику применяет вызывающий код к коротким операциям.
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
