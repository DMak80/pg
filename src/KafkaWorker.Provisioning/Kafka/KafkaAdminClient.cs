using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaWorker.Core;

namespace KafkaWorker.Provisioning.Kafka;

// Адаптер над Confluent.Kafka IAdminClient — единственное место воркера с
// Confluent-типами (seam-паттерн Puzzle §7.2). SASL_PLAINTEXT/PLAIN по контракту
// дискавери arch/15 §5; RequestTimeout — из опций воркера (на вызов); любые
// исключения Kafka-клиента → Result.Failed (процессы решают, транзиент или нет).

/// <summary>Фабрика AdminClient-адаптеров; RequestTimeout из конфигурации.</summary>
public sealed class KafkaAdminClientFactory(TimeSpan requestTimeout) : IKafkaAdminClientFactory
{
    public IKafkaAdminClient Create(string bootstrap, string user, string password)
        => new KafkaAdminClient(bootstrap, user, password, requestTimeout);
}

public sealed class KafkaAdminClient(
    string bootstrap,
    string user,
    string password,
    TimeSpan requestTimeout) : IKafkaAdminClient
{
    // Ленивый клиент: создаётся при первом вызове (пустые операции не ходят в сеть).
    private IAdminClient? _client;

    public Task<Result<KafkaClusterView>> DescribeClusterAsync(CancellationToken ct)
        => RunAsync(async client =>
        {
            var cluster = await client.DescribeClusterAsync(new DescribeClusterOptions
            {
                RequestTimeout = requestTimeout,
            });
            return new KafkaClusterView(
                cluster.Nodes.Select(n => new KafkaBrokerView(n.Id, n.Host)).ToList(),
                cluster.Controller?.Id);
        }, ct);

    public Task<Result<IReadOnlyList<KafkaTopicView>>> DescribeTopicsAsync(CancellationToken ct)
        => RunAsync<IReadOnlyList<KafkaTopicView>>(client => Task.FromResult(DescribeTopicsViaMetadata(client)), ct);

    public Task<Result<IReadOnlyDictionary<string, string>>> DescribeBrokerConfigsAsync(int brokerId, CancellationToken ct)
        => RunAsync<IReadOnlyDictionary<string, string>>(async client =>
        {
            var resource = new ConfigResource { Type = ResourceType.Broker, Name = brokerId.ToString() };
            var described = await client.DescribeConfigsAsync(
                [resource],
                new DescribeConfigsOptions { RequestTimeout = requestTimeout });
            var entries = described.Single().Entries.Values
                .Where(e => e.Value is not null)
                .ToDictionary(e => e.Name, e => e.Value!);
            return (IReadOnlyDictionary<string, string>)entries;
        }, ct);

    public Task<Result> AlterBrokerConfigsAsync(int brokerId, IReadOnlyDictionary<string, string> configs, CancellationToken ct)
        => RunAsync(async client =>
        {
            var resource = new ConfigResource { Type = ResourceType.Broker, Name = brokerId.ToString() };
            var entries = configs
                .Select(pair => new ConfigEntry { Name = pair.Key, Value = pair.Value, IncrementalOperation = AlterConfigOpType.Set })
                .ToList();
            await client.IncrementalAlterConfigsAsync(
                new Dictionary<ConfigResource, List<ConfigEntry>> { [resource] = entries },
                new IncrementalAlterConfigsOptions { RequestTimeout = requestTimeout });
            return Result.Success();
        }, ct);

    public ValueTask DisposeAsync()
    {
        _client?.Dispose();
        return ValueTask.CompletedTask;
    }

    public Task<Result<IReadOnlyDictionary<string, string>>> DescribeTopicConfigsAsync(string topic, CancellationToken ct)
        => RunAsync<IReadOnlyDictionary<string, string>>(async client =>
        {
            var resource = new ConfigResource { Type = ResourceType.Topic, Name = topic };
            var described = await client.DescribeConfigsAsync(
                [resource],
                new DescribeConfigsOptions { RequestTimeout = requestTimeout });
            var entries = described.Single().Entries.Values
                .Where(e => e.Value is not null)
                .ToDictionary(e => e.Name, e => e.Value!);
            return (IReadOnlyDictionary<string, string>)entries;
        }, ct);

    public Task<Result> AlterTopicConfigsAsync(string topic, IReadOnlyDictionary<string, string> configs, CancellationToken ct)
        => RunAsync(async client =>
        {
            var resource = new ConfigResource { Type = ResourceType.Topic, Name = topic };
            var entries = configs
                .Select(pair => new ConfigEntry { Name = pair.Key, Value = pair.Value, IncrementalOperation = AlterConfigOpType.Set })
                .ToList();
            await client.IncrementalAlterConfigsAsync(
                new Dictionary<ConfigResource, List<ConfigEntry>> { [resource] = entries },
                new IncrementalAlterConfigsOptions { RequestTimeout = requestTimeout });
            return Result.Success();
        }, ct);

    public Task<Result> CreatePartitionsAsync(string topic, int totalPartitions, CancellationToken ct)
        => RunAsync(async client =>
        {
            await client.CreatePartitionsAsync(
                [new PartitionsSpecification { Topic = topic, IncreaseTo = totalPartitions }],
                new CreatePartitionsOptions { RequestTimeout = requestTimeout });
            return Result.Success();
        }, ct);

    // Полный список топиков + реплики партиций — через метаданные
    // (DescribeTopicsAsync требует имена заранее). Internal-топики __* — вне реестра.
    private IReadOnlyList<KafkaTopicView> DescribeTopicsViaMetadata(IAdminClient client)
    {
        var metadata = client.GetMetadata(requestTimeout);
        return metadata.Topics
            .Where(t => !t.Topic.StartsWith("__", StringComparison.Ordinal))
            .Select(t => new KafkaTopicView(
                t.Topic,
                t.Partitions.Count,
                t.Partitions.Select(p => (IReadOnlyList<int>)[.. p.Replicas]).ToList()))
            .ToList();
    }

    // Общий каркас: клиент создаётся один раз; исключения → Result.Failed.
    private async Task<Result> RunAsync(Func<IAdminClient, Task<Result>> action, CancellationToken ct)
    {
        try
        {
            ct.ThrowIfCancellationRequested();
            return await action(EnsureClient());
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException($"Kafka AdminClient ({bootstrap}): {e.Message}", e));
        }
    }

    private async Task<Result<T>> RunAsync<T>(Func<IAdminClient, Task<T>> action, CancellationToken ct)
    {
        try
        {
            ct.ThrowIfCancellationRequested();
            return Result<T>.Success(await action(EnsureClient()));
        }
        catch (Exception e)
        {
            return Result<T>.Failed(new ApplicationException($"Kafka AdminClient ({bootstrap}): {e.Message}", e));
        }
    }

    private IAdminClient EnsureClient()
    {
        if (_client is not null)
            return _client;

        _client = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = bootstrap,
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = user,
            SaslPassword = password,
        }).Build();
        return _client;
    }
}
