using AdminPanel.Infrastructure;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace AdminPanel.Probes.Kafka;

// Адаптер Confluent.Kafka для kafka-пробы: SASL/PLAIN + SASL_PLAINTEXT (arch/15 §5);
// DescribeCluster с RequestTimeout на вызов (прецедент KafkaAdminClient воркера);
// исключения → Result.Failed (проба не роняет панель). Единственное место
// Probes-сборки с Confluent-типами.
// Клиенты — из KafkaClientCache (t11): «один на вызов» с using-Dispose плодил
// rd_kafka-инстансы и жёг ядро на недоступных брокерах; Dispose — только при
// замене/выключении, фейл → Invalidate (пересоздание на следующей пробе).
public sealed class ConfluentKafkaProbeClient(KafkaClientCache cache) : IKafkaProbeClient
{
    public async Task<Result<KafkaProbeView>> DescribeClusterAsync(
        string bootstrap, string user, string password, TimeSpan timeout, CancellationToken ct)
    {
        try
        {
            ct.ThrowIfCancellationRequested();
            var admin = cache.GetAdmin(bootstrap, user, password);
            var cluster = await admin.DescribeClusterAsync(
                new DescribeClusterOptions { RequestTimeout = timeout });
            return Result<KafkaProbeView>.Success(new KafkaProbeView(
                cluster.Nodes.Select(n => new KafkaProbeBroker(n.Id, n.Host)).ToList(),
                cluster.Controller?.Id));
        }
        catch (Exception e)
        {
            cache.Invalidate(bootstrap, user, password);
            return Result<KafkaProbeView>.Failed(new InvalidOperationException(
                $"DescribeCluster ({bootstrap}): {e.Message}", e));
        }
    }
}

// Runtime-часть пробы (волна C): топики (USR по ISR), группы, лаги.
// Отдельный адаптер — DescribeCluster-путь пробы не меняется; исключения →
// Result.Failed. End-оффсеты — через IConsumer.GetWatermarkOffsets (у
// AdminClient в 2.14 нет ListOffsets); committed — ListConsumerGroupOffsets.
// Все операции — на кэшированных клиентах (t11): один AdminClient/Consumer
// на кластер на тик вместо отдельного инстанса на каждый вызов.
public sealed class ConfluentKafkaRuntimeProbeClient(KafkaClientCache cache) : IKafkaProbeRuntimeClient
{
    public async Task<Result<IReadOnlyList<KafkaProbeTopic>>> DescribeTopicsAsync(
        string bootstrap, string user, string password, TimeSpan timeout, CancellationToken ct)
    {
        try
        {
            ct.ThrowIfCancellationRequested();
            var admin = cache.GetAdmin(bootstrap, user, password);
            var metadata = admin.GetMetadata(timeout);
            var topics = metadata.Topics
                .Where(t => !t.Topic.StartsWith("__", StringComparison.Ordinal))
                .Select(t => new KafkaProbeTopic(
                    t.Topic,
                    t.Partitions.Count,
                    t.Partitions.Count == 0 ? null : t.Partitions.Max(p => p.Replicas.Length),
                    t.Partitions.Count(p => p.InSyncReplicas.Length < p.Replicas.Length)))
                .OrderBy(t => t.Topic, StringComparer.Ordinal)
                .ToList();
            return await Task.FromResult(Result<IReadOnlyList<KafkaProbeTopic>>.Success(topics));
        }
        catch (Exception e)
        {
            cache.Invalidate(bootstrap, user, password);
            return Result<IReadOnlyList<KafkaProbeTopic>>.Failed(new InvalidOperationException(
                $"DescribeTopics ({bootstrap}): {e.Message}", e));
        }
    }

    public async Task<Result<IReadOnlyList<string>>> ListGroupsAsync(
        string bootstrap, string user, string password, TimeSpan timeout, CancellationToken ct)
    {
        try
        {
            ct.ThrowIfCancellationRequested();
            var admin = cache.GetAdmin(bootstrap, user, password);
            var groups = await admin.ListConsumerGroupsAsync(
                new ListConsumerGroupsOptions { RequestTimeout = timeout });
            return Result<IReadOnlyList<string>>.Success(
                [.. groups.Valid.Select(g => g.GroupId).OrderBy(id => id, StringComparer.Ordinal)]);
        }
        catch (Exception e)
        {
            cache.Invalidate(bootstrap, user, password);
            return Result<IReadOnlyList<string>>.Failed(new InvalidOperationException(
                $"ListGroups ({bootstrap}): {e.Message}", e));
        }
    }

    public async Task<Result<IReadOnlyList<KafkaProbeGroupDetail>>> DescribeGroupsAsync(
        string bootstrap, string user, string password, IReadOnlyList<string> groups,
        TimeSpan timeout, CancellationToken ct)
    {
        try
        {
            ct.ThrowIfCancellationRequested();
            var admin = cache.GetAdmin(bootstrap, user, password);
            var described = await admin.DescribeConsumerGroupsAsync(
                groups.ToList(),
                new DescribeConsumerGroupsOptions { RequestTimeout = timeout });
            var details = described.ConsumerGroupDescriptions
                .Select(g => new KafkaProbeGroupDetail(
                    g.GroupId,
                    g.State.ToString(),
                    g.Members.Count,
                    [.. g.Members
                        .SelectMany(m => m.Assignment?.TopicPartitions ?? [])
                        .Select(tp => (Topic: tp.Topic, Partition: tp.Partition.Value))
                        .Distinct()
                        .OrderBy(p => p.Topic, StringComparer.Ordinal)
                        .ThenBy(p => p.Partition)]))
                .OrderBy(d => d.Group, StringComparer.Ordinal)
                .ToList();
            return Result<IReadOnlyList<KafkaProbeGroupDetail>>.Success(details);
        }
        catch (Exception e)
        {
            cache.Invalidate(bootstrap, user, password);
            return Result<IReadOnlyList<KafkaProbeGroupDetail>>.Failed(new InvalidOperationException(
                $"DescribeGroups ({bootstrap}): {e.Message}", e));
        }
    }

    public async Task<Result<IReadOnlyDictionary<(string Topic, int Partition), long>>> EndOffsetsAsync(
        string bootstrap, string user, string password,
        IReadOnlyList<(string Topic, int Partition)> partitions, TimeSpan timeout, CancellationToken ct)
    {
        try
        {
            ct.ThrowIfCancellationRequested();

            // group.id у консьюмера кэша — техническая (adminpanel-probe):
            // оффсеты не коммитятся и не читаются.
            var consumer = cache.GetConsumer(bootstrap, user, password);
            var result = new Dictionary<(string Topic, int Partition), long>();
            foreach (var (topic, partition) in partitions.Distinct().OrderBy(p => p.Topic).ThenBy(p => p.Partition))
            {
                var watermark = consumer.QueryWatermarkOffsets(
                    new TopicPartition(topic, new Partition(partition)), timeout);
                result[(topic, partition)] = watermark.High.Value;
            }

            return Result<IReadOnlyDictionary<(string Topic, int Partition), long>>.Success(result);
        }
        catch (Exception e)
        {
            cache.Invalidate(bootstrap, user, password);
            return Result<IReadOnlyDictionary<(string Topic, int Partition), long>>.Failed(
                new InvalidOperationException($"EndOffsets ({bootstrap}): {e.Message}", e));
        }
    }

    public async Task<Result<IReadOnlyDictionary<(string Topic, int Partition), long>>> CommittedAsync(
        string bootstrap, string user, string password, string group,
        IReadOnlyList<(string Topic, int Partition)> partitions, TimeSpan timeout, CancellationToken ct)
    {
        try
        {
            ct.ThrowIfCancellationRequested();
            var admin = cache.GetAdmin(bootstrap, user, password);

            // Пустой набор партиций = ВСЕ закоммиченные оффсеты группы (lag
            // мониторинга живёт и после смерти консьюмера: группа Empty с
            // committed — это и есть отставание, Burrow-семантика).
            List<TopicPartition>? requested = partitions.Count == 0
                ? null
                : [.. partitions
                    .Distinct()
                    .Select(p => new TopicPartition(p.Topic, new Partition(p.Partition)))];
            var result = await admin.ListConsumerGroupOffsetsAsync(
                [new ConsumerGroupTopicPartitions(group, requested)],
                new ListConsumerGroupOffsetsOptions { RequestTimeout = timeout });

            // Нет коммита (ErrorCode != None) — пропускаем: отсутствие ключа
            // трактуется KafkaGroupLag как «весь end в лаг».
            var committed = new Dictionary<(string Topic, int Partition), long>();
            foreach (var fetch in result.SelectMany(r => r.Partitions))
                if (!fetch.Error.IsError)
                    committed[(fetch.Topic, fetch.Partition.Value)] = fetch.Offset;

            return Result<IReadOnlyDictionary<(string Topic, int Partition), long>>.Success(committed);
        }
        catch (Exception e)
        {
            cache.Invalidate(bootstrap, user, password);
            return Result<IReadOnlyDictionary<(string Topic, int Partition), long>>.Failed(
                new InvalidOperationException($"Committed ({bootstrap}, group {group}): {e.Message}", e));
        }
    }
}
