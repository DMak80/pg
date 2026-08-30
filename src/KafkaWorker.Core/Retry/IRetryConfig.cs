namespace KafkaWorker.Core.Retry;

public interface IRetryConfig
{
    int FirstRetryDelayInSec { get; }

    int RetryCount { get; }
}