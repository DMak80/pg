using System.Net.Http;
using Polly;
using Polly.Retry;

namespace KafkaWorker.Core.Retry;

/// <summary>
/// Джиттер-ретраи (Polly 8, ResiliencePipeline). Порт RetryPolicies PgWorker
/// (адаптация Puzzle на v8-API): вместо Policy.Handle — PredicateBuilder +
/// RetryStrategyOptions, DecorrelatedJitterBackoffV2-эффект даёт комбинация
/// Exponential + UseJitter. SQL-ретрая нет — воркер kafka не ходит в базы
/// (короткие сетевые операции — HTTP к etcd/docker и Kafka AdminClient).
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

    // Базовый джиттер-ретрай произвольных операций (Kafka AdminClient-вызовы
    // поверх оркестрации — повтор безопасен, arch/16 §5 D).
    public static ResiliencePipeline GeneralRetry(int retryCount, TimeSpan medianFirstRetryDelay) =>
        new ResiliencePipelineBuilder()
           .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = retryCount,
                UseJitter = true,
                Delay = medianFirstRetryDelay,
                ShouldHandle = new PredicateBuilder()
                   .Handle<TaskCanceledException>(),
            })
           .Build();
}
