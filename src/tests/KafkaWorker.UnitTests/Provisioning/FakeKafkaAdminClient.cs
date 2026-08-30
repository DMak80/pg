using KafkaWorker.Core;
using KafkaWorker.Provisioning.Kafka;

namespace KafkaWorker.UnitTests.Provisioning;

/// <summary>
/// Hand-written fake IKafkaAdminClient для юнит-тестов процессов (A9+):
/// сценарии «кластер не готов → failure», списки брокеров, топики с репликами,
/// diff конфигов. Все мутации записываются — assert'ы процессов проверяют вызовы.
/// </summary>
public sealed class FakeKafkaAdminClient : IKafkaAdminClient
{
    public KafkaClusterView? ClusterView;
    public IReadOnlyList<KafkaTopicView>? Topics;
    public IReadOnlyDictionary<string, string>? BrokerConfigs = new Dictionary<string, string>();
    public Exception? ClusterError;
    public Exception? TopicsError;

    public List<(int BrokerId, IReadOnlyDictionary<string, string> Configs)> AlterCalls = [];

    public Task<Result<KafkaClusterView>> DescribeClusterAsync(CancellationToken ct)
        => ClusterError is not null
            ? Task.FromResult(Result<KafkaClusterView>.Failed(ClusterError))
            : Task.FromResult(ClusterView is not null
                ? Result<KafkaClusterView>.Success(ClusterView)
                : Result<KafkaClusterView>.Failed(new ApplicationException("cluster not ready")));

    public Task<Result<IReadOnlyList<KafkaTopicView>>> DescribeTopicsAsync(CancellationToken ct)
        => TopicsError is not null
            ? Task.FromResult(Result<IReadOnlyList<KafkaTopicView>>.Failed(TopicsError))
            : Task.FromResult(Result<IReadOnlyList<KafkaTopicView>>.Success(
                Topics ?? []));

    public Task<Result<IReadOnlyDictionary<string, string>>> DescribeBrokerConfigsAsync(int brokerId, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyDictionary<string, string>>.Success(
            BrokerConfigs ?? new Dictionary<string, string>()));

    public Task<Result> AlterBrokerConfigsAsync(int brokerId, IReadOnlyDictionary<string, string> configs, CancellationToken ct)
    {
        AlterCalls.Add((brokerId, configs));
        // Идемпотентность факта: alter сразу отражается в describe (converge-тесты).
        BrokerConfigs = new Dictionary<string, string>(BrokerConfigs ?? new Dictionary<string, string>())
            .Concat(configs)
            .GroupBy(p => p.Key, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.Last().Value);
        return Task.FromResult(Result.Success());
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
