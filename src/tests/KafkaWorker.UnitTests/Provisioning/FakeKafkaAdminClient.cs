using KafkaWorker.Core;
using KafkaWorker.Provisioning.Kafka;

namespace KafkaWorker.UnitTests.Provisioning;

/// <summary>
/// Hand-written fake IKafkaAdminClient для юнит-тестов процессов (A9+):
/// сценарии «кластер не готов → failure», списки брокеров, топики с репликами,
/// diff конфигов. Все мутации записываются — assert'ы процессов проверяют вызовы;
/// CallLog фиксирует СКВОЗНОЙ порядок (TopicSync: конфиги до partitions).
/// </summary>
public sealed class FakeKafkaAdminClient : IKafkaAdminClient
{
    public KafkaClusterView? ClusterView;
    public IReadOnlyList<KafkaTopicView>? Topics;

    // ISR топиков (по имени): подставляется в view при describe; null — ISR
    // не задан (semantics «данных о USR нет», HasUnderReplicated → false).
    public IReadOnlyDictionary<string, IReadOnlyList<IReadOnlyList<int>>>? Isr;

    public IReadOnlyDictionary<string, string>? BrokerConfigs = new Dictionary<string, string>();
    public Exception? ClusterError;
    public Exception? TopicsError;

    // Факт-конфиги топиков: topic → name → value (TopicSync волны C).
    public IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>>? TopicConfigs;

    public List<(int BrokerId, IReadOnlyDictionary<string, string> Configs)> AlterCalls = [];
    public List<(string Topic, IReadOnlyDictionary<string, string> Configs)> AlterTopicCalls = [];
    public List<(string Topic, int TotalPartitions)> CreatePartitionsCalls = [];
    public List<string> CallLog = [];

    // Управляемость: мутация падает первые N вызовов (транзиент — jitter-ретраи
    // процесса/тика должны добивать применение).
    public int AlterTopicFailCount { get; set; }
    private int _alterTopicFailures;

    // Lifecycle-журнал вызовов (t01): создание/удаление + транзиенты.
    public List<(string Topic, int Partitions, short ReplicationFactor, IReadOnlyDictionary<string, string>? Configs)> CreatedTopics = [];
    public List<string> DeletedTopics = [];
    public int CreateTopicFailCount { get; set; }
    public int DeleteTopicFailCount { get; set; }
    private int _createFails, _deleteFails;

    public Task<Result<KafkaClusterView>> DescribeClusterAsync(CancellationToken ct)
        => ClusterError is not null
            ? Task.FromResult(Result<KafkaClusterView>.Failed(ClusterError))
            : Task.FromResult(ClusterView is not null
                ? Result<KafkaClusterView>.Success(ClusterView)
                : Result<KafkaClusterView>.Failed(new ApplicationException("cluster not ready")));

    // Флаг includeInternal фейк игнорирует: фильтрация __-топиков живёт в
    // реальном адаптере (KafkaAdminClient), фейк отдаёт Topics как есть.
    public Task<Result<IReadOnlyList<KafkaTopicView>>> DescribeTopicsAsync(bool includeInternal, CancellationToken ct)
    {
        CallLog.Add("describe-topics");
        return TopicsError is not null
            ? Task.FromResult(Result<IReadOnlyList<KafkaTopicView>>.Failed(TopicsError))
            : Task.FromResult(Result<IReadOnlyList<KafkaTopicView>>.Success(
                (Topics ?? []).Select(WithIsr).ToList()));
    }

    private KafkaTopicView WithIsr(KafkaTopicView view)
        => Isr is not null && Isr.TryGetValue(view.Topic, out var isr)
            ? view with { IsrPerPartition = isr }
            : view;

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

    public Task<Result<IReadOnlyDictionary<string, string>>> DescribeTopicConfigsAsync(string topic, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyDictionary<string, string>>.Success(
            TopicConfigs is not null && TopicConfigs.TryGetValue(topic, out var configs)
                ? configs
                : new Dictionary<string, string>()));

    public Task<Result> AlterTopicConfigsAsync(string topic, IReadOnlyDictionary<string, string> configs, CancellationToken ct)
    {
        CallLog.Add($"alter-topic:{topic}");
        if (_alterTopicFailures < AlterTopicFailCount)
        {
            _alterTopicFailures++;
            return Task.FromResult(Result.Failed(new ApplicationException("kafka: alter timeout")));
        }

        AlterTopicCalls.Add((topic, configs));
        // Идемпотентность факта: alter отражается в describe (снятие desired тестами).
        var merged = new Dictionary<string, string>(
            TopicConfigs is not null && TopicConfigs.TryGetValue(topic, out var current)
                ? current
                : new Dictionary<string, string>());
        foreach (var (name, value) in configs)
            merged[name] = value;

        TopicConfigs = new Dictionary<string, IReadOnlyDictionary<string, string>>(
            TopicConfigs ?? new Dictionary<string, IReadOnlyDictionary<string, string>>())
        {
            [topic] = merged,
        };
        return Task.FromResult(Result.Success());
    }

    public Task<Result> CreatePartitionsAsync(string topic, int totalPartitions, CancellationToken ct)
    {
        CallLog.Add($"create-partitions:{topic}:{totalPartitions}");
        CreatePartitionsCalls.Add((topic, totalPartitions));
        return Task.FromResult(Result.Success());
    }

    public Task<Result<TopicCreateOutcome>> CreateTopicAsync(
        string topic, int partitions, short replicationFactor,
        IReadOnlyDictionary<string, string>? configs, CancellationToken ct)
    {
        CallLog.Add($"create-topic:{topic}");
        if (_createFails++ < CreateTopicFailCount)
            return Task.FromResult(Result<TopicCreateOutcome>.Failed(new ApplicationException("create transient")));

        if (Topics is not null && Topics.Any(t => t.Topic == topic))
            return Task.FromResult(Result<TopicCreateOutcome>.Success(TopicCreateOutcome.AlreadyExists));

        var views = (Topics ?? []).ToList();
        views.Add(new KafkaTopicView(topic, partitions,
            Enumerable.Repeat((IReadOnlyList<int>)[1], partitions).ToList()));
        Topics = views;
        CreatedTopics.Add((topic, partitions, replicationFactor, configs));
        return Task.FromResult(Result<TopicCreateOutcome>.Success(TopicCreateOutcome.Created));
    }

    public Task<Result<TopicDeleteOutcome>> DeleteTopicAsync(string topic, CancellationToken ct)
    {
        CallLog.Add($"delete-topic:{topic}");
        if (_deleteFails++ < DeleteTopicFailCount)
            return Task.FromResult(Result<TopicDeleteOutcome>.Failed(new ApplicationException("delete transient")));

        if (Topics is not null && Topics.Any(t => t.Topic == topic))
        {
            Topics = Topics.Where(t => !string.Equals(t.Topic, topic, StringComparison.Ordinal)).ToList();
            DeletedTopics.Add(topic);
            return Task.FromResult(Result<TopicDeleteOutcome>.Success(TopicDeleteOutcome.Deleted));
        }

        return Task.FromResult(Result<TopicDeleteOutcome>.Success(TopicDeleteOutcome.NotFound));
    }

    // Коллектор лагов (t04, arch/18 §4): процессы seam-методы не используют —
    // заглушки-провалы по умолчанию; настраиваемые данные добавит фейк коллектора.
    public Task<Result<IReadOnlyList<KafkaGroupView>>> ListGroupsAsync(CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<KafkaGroupView>>.Failed(
            new ApplicationException("not configured in this fake")));

    public Task<Result<IReadOnlyList<KafkaTopicPartitionOffset>>> ListConsumerGroupOffsetsAsync(
        string group, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<KafkaTopicPartitionOffset>>.Failed(
            new ApplicationException("not configured in this fake")));

    public Task<Result<IReadOnlyList<KafkaTopicPartitionOffset>>> ListOffsetsAsync(
        IReadOnlyList<KafkaTopicPartition> partitions, CancellationToken ct)
        => Task.FromResult(Result<IReadOnlyList<KafkaTopicPartitionOffset>>.Failed(
            new ApplicationException("not configured in this fake")));

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
