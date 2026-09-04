using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaWorker.Core;

namespace KafkaWorker.Provisioning.Kafka;

// Адаптер над Confluent.Kafka IAdminClient — единственное место воркера с
// Confluent-типами (seam-паттерн Puzzle §7.2). SASL_SSL/PLAIN + доверие
// per-cluster CA по контракту дискавери arch/15 §5 (t03); RequestTimeout — из
// опций воркера (на вызов); любые исключения Kafka-клиента → Result.Failed
// (процессы решают, транзиент или нет).

/// <summary>Фабрика AdminClient-адаптеров; RequestTimeout из конфигурации.</summary>
public sealed class KafkaAdminClientFactory(TimeSpan requestTimeout) : IKafkaAdminClientFactory
{
    public IKafkaAdminClient Create(string bootstrap, string user, string password, string? caPem)
        => new KafkaAdminClient(bootstrap, user, password, caPem, requestTimeout);
}

public sealed class KafkaAdminClient(
    string bootstrap,
    string user,
    string password,
    string? caPem,
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

    public Task<Result<IReadOnlyList<KafkaTopicView>>> DescribeTopicsAsync(bool includeInternal, CancellationToken ct)
        => RunAsync<IReadOnlyList<KafkaTopicView>>(
            client => Task.FromResult(DescribeTopicsViaMetadata(client, includeInternal)), ct);

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

    public Task<Result<TopicCreateOutcome>> CreateTopicAsync(
        string topic, int partitions, short replicationFactor,
        IReadOnlyDictionary<string, string>? configs, CancellationToken ct)
        => RunAsync<TopicCreateOutcome>(async client =>
        {
            var spec = new TopicSpecification
            {
                Name = topic,
                NumPartitions = partitions,
                ReplicationFactor = replicationFactor,
            };
            if (configs is { Count: > 0 })
                spec.Configs = new Dictionary<string, string>(configs);

            try
            {
                // CreateTopicsAsync 2.14 не возвращает отчётов — исходы по исключению.
                await client.CreateTopicsAsync(
                    [spec], new CreateTopicsOptions { RequestTimeout = requestTimeout });
            }
            catch (CreateTopicsException e) when (
                e.Results.Any(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
            {
                return TopicCreateOutcome.AlreadyExists; // идемпотентность: исполнено (§3.1)
            }

            return TopicCreateOutcome.Created;
        }, ct);

    public Task<Result<TopicDeleteOutcome>> DeleteTopicAsync(string topic, CancellationToken ct)
        => RunAsync<TopicDeleteOutcome>(async client =>
        {
            try
            {
                // DeleteTopicsAsync возвращает Task без отчётов — исход по исключению.
                await client.DeleteTopicsAsync(
                    [topic], new DeleteTopicsOptions { RequestTimeout = requestTimeout });
            }
            catch (DeleteTopicsException e) when (
                e.Results.Any(r => r.Error.Code == ErrorCode.UnknownTopicOrPart))
            {
                return TopicDeleteOutcome.NotFound; // идемпотентность: исполнено (§3.1)
            }

            return TopicDeleteOutcome.Deleted;
        }, ct);

    // Полный список топиков + реплики/ISR партиций — через метаданные
    // (DescribeTopicsAsync требует имена заранее). Internal-топики __* — только
    // при includeInternal (drain/guard G по describe-all, arch/16 §5 I/G);
    // TopicSync ведёт реестр юзер-топиков — ему false. ISR — рядом с Replicas
    // (PartitionMetadata Confluent-клиента), адаптер заполняет всегда.
    private IReadOnlyList<KafkaTopicView> DescribeTopicsViaMetadata(IAdminClient client, bool includeInternal)
    {
        var metadata = client.GetMetadata(requestTimeout);
        return metadata.Topics
            .Where(t => includeInternal || !t.Topic.StartsWith("__", StringComparison.Ordinal))
            .Select(t => new KafkaTopicView(
                t.Topic,
                t.Partitions.Count,
                t.Partitions.Select(p => (IReadOnlyList<int>)[.. p.Replicas]).ToList(),
                t.Partitions.Select(p => (IReadOnlyList<int>)[.. p.InSyncReplicas]).ToList()))
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

        var config = new AdminClientConfig
        {
            BootstrapServers = bootstrap,
            SecurityProtocol = SecurityProtocol.SaslSsl, // t03: дискавери-канон arch/15 §5
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = user,
            SaslPassword = password,
        };
        if (caPem is not null)
            config.Set("ssl.ca.pem", caPem); // доверие per-cluster CA (librdkafka >= 1.5)
        _client = new AdminClientBuilder(config).Build();
        return _client;
    }

    // ===== ACL (t03, arch/16 §2.3/E): Describe/Create/Delete через seam-типы =====

    public Task<Result<IReadOnlyList<KafkaAclBinding>>> DescribeAclsAsync(CancellationToken ct)
        => RunAsync<IReadOnlyList<KafkaAclBinding>>(async client =>
        {
            var described = await client.DescribeAclsAsync(
                AnyAclFilter(), // все ACL кластера; фильтрацию делает AclPlan.Diff
                new DescribeAclsOptions { RequestTimeout = requestTimeout });
            return described.AclBindings.Select(ToBinding).ToList();
        }, ct);

    public Task<Result> CreateAclsAsync(IReadOnlyList<KafkaAclBinding> acls, CancellationToken ct)
        => RunAsync(async client =>
        {
            await client.CreateAclsAsync(
                acls.Select(ToConfluentBinding),
                new CreateAclsOptions { RequestTimeout = requestTimeout });
            return Result.Success();
        }, ct);

    public Task<Result> DeleteAclsAsync(IReadOnlyList<KafkaAclBinding> acls, CancellationToken ct)
        => RunAsync(async client =>
        {
            // По одному binding — exact-match фильтр (все поля заполнены).
            foreach (var acl in acls)
                await client.DeleteAclsAsync(
                    [ExactFilter(acl)],
                    new DeleteAclsOptions { RequestTimeout = requestTimeout });
            return Result.Success();
        }, ct);

    // Фильтр «любой ACL»: не заполненные поля — Any (семантика «все»).
    private static AclBindingFilter AnyAclFilter()
        => new()
        {
            PatternFilter = new ResourcePatternFilter
            {
                Type = ResourceType.Any,
                Name = null,
                ResourcePatternType = ResourcePatternType.Any,
            },
            EntryFilter = new AccessControlEntryFilter
            {
                Principal = null,
                Host = null,
                Operation = AclOperation.Any,
                PermissionType = AclPermissionType.Any,
            },
        };

    // Exact-match фильтр по всем полям binding'а (для удаления лишних ACL app).
    private static AclBindingFilter ExactFilter(KafkaAclBinding acl)
        => new()
        {
            PatternFilter = new ResourcePatternFilter
            {
                Type = ToConfluentResource(acl.ResourceType),
                Name = acl.ResourceName,
                ResourcePatternType = ToConfluentPattern(acl.PatternType),
            },
            EntryFilter = new AccessControlEntryFilter
            {
                Principal = acl.Principal,
                Host = null,
                Operation = ToConfluentOperation(acl.Operation),
                PermissionType = ToConfluentPermission(acl.Permission),
            },
        };

    private static KafkaAclBinding ToBinding(AclBinding binding)
        => new(
            ToKafkaResource(binding.Pattern.Type),
            binding.Pattern.Name,
            ToKafkaPattern(binding.Pattern.ResourcePatternType),
            binding.Entry.Principal,
            ToKafkaOperation(binding.Entry.Operation),
            ToKafkaPermission(binding.Entry.PermissionType));

    private static AclBinding ToConfluentBinding(KafkaAclBinding acl)
        => new()
        {
            Pattern = new ResourcePattern
            {
                Type = ToConfluentResource(acl.ResourceType),
                Name = acl.ResourceName,
                ResourcePatternType = ToConfluentPattern(acl.PatternType),
            },
            Entry = new AccessControlEntry
            {
                Principal = acl.Principal,
                Host = "*",
                Operation = ToConfluentOperation(acl.Operation),
                PermissionType = ToConfluentPermission(acl.Permission),
            },
        };

    // Порядок значений своих enum отличается от Confluent — явный маппинг
    // (каст числами невалиден, например AclOperation: IdempotentWrite/ClusterAction).
    // ResourceType: Confluent 2.x не именует значения librdkafka 4..7 — используем
    // числовые эквиваленты (Broker=4 это RESOURCE_CLUSTER librdkafka; 5/6/7 —
    // TransactionalId/DelegationToken/User), значение уходит в librdkafka как есть.
    private static ResourceType ToConfluentResource(KafkaAclResourceType type) => type switch
    {
        KafkaAclResourceType.Unknown => ResourceType.Unknown,
        KafkaAclResourceType.Any => ResourceType.Any,
        KafkaAclResourceType.Topic => ResourceType.Topic,
        KafkaAclResourceType.Group => ResourceType.Group,
        KafkaAclResourceType.Cluster => (ResourceType)4,       // Broker в enum 2.x
        KafkaAclResourceType.TransactionalId => (ResourceType)5,
        KafkaAclResourceType.DelegationToken => (ResourceType)6,
        KafkaAclResourceType.User => (ResourceType)7,
        _ => ResourceType.Unknown,
    };

    private static KafkaAclResourceType ToKafkaResource(ResourceType type) => ((int)type) switch
    {
        1 => KafkaAclResourceType.Any,
        2 => KafkaAclResourceType.Topic,
        3 => KafkaAclResourceType.Group,
        4 => KafkaAclResourceType.Cluster,                     // Broker (см. выше)
        5 => KafkaAclResourceType.TransactionalId,
        6 => KafkaAclResourceType.DelegationToken,
        7 => KafkaAclResourceType.User,
        _ => KafkaAclResourceType.Unknown,
    };

    private static ResourcePatternType ToConfluentPattern(KafkaAclPatternType type) => type switch
    {
        KafkaAclPatternType.Any => ResourcePatternType.Any,
        KafkaAclPatternType.Match => ResourcePatternType.Match,
        KafkaAclPatternType.Literal => ResourcePatternType.Literal,
        KafkaAclPatternType.Prefixed => ResourcePatternType.Prefixed,
        _ => ResourcePatternType.Unknown,
    };

    private static KafkaAclPatternType ToKafkaPattern(ResourcePatternType type) => type switch
    {
        ResourcePatternType.Any => KafkaAclPatternType.Any,
        ResourcePatternType.Match => KafkaAclPatternType.Match,
        ResourcePatternType.Literal => KafkaAclPatternType.Literal,
        ResourcePatternType.Prefixed => KafkaAclPatternType.Prefixed,
        _ => KafkaAclPatternType.Unknown,
    };

    private static AclOperation ToConfluentOperation(KafkaAclOperation op) => op switch
    {
        KafkaAclOperation.Any => AclOperation.Any,
        KafkaAclOperation.All => AclOperation.All,
        KafkaAclOperation.Read => AclOperation.Read,
        KafkaAclOperation.Write => AclOperation.Write,
        KafkaAclOperation.Create => AclOperation.Create,
        KafkaAclOperation.Delete => AclOperation.Delete,
        KafkaAclOperation.Alter => AclOperation.Alter,
        KafkaAclOperation.Describe => AclOperation.Describe,
        KafkaAclOperation.IdempotentWrite => AclOperation.IdempotentWrite,
        KafkaAclOperation.ClusterAction => AclOperation.ClusterAction,
        KafkaAclOperation.DescribeConfigs => AclOperation.DescribeConfigs,
        KafkaAclOperation.AlterConfigs => AclOperation.AlterConfigs,
        _ => AclOperation.Unknown,
    };

    private static KafkaAclOperation ToKafkaOperation(AclOperation op) => op switch
    {
        AclOperation.Any => KafkaAclOperation.Any,
        AclOperation.All => KafkaAclOperation.All,
        AclOperation.Read => KafkaAclOperation.Read,
        AclOperation.Write => KafkaAclOperation.Write,
        AclOperation.Create => KafkaAclOperation.Create,
        AclOperation.Delete => KafkaAclOperation.Delete,
        AclOperation.Alter => KafkaAclOperation.Alter,
        AclOperation.Describe => KafkaAclOperation.Describe,
        AclOperation.IdempotentWrite => KafkaAclOperation.IdempotentWrite,
        AclOperation.ClusterAction => KafkaAclOperation.ClusterAction,
        AclOperation.DescribeConfigs => KafkaAclOperation.DescribeConfigs,
        AclOperation.AlterConfigs => KafkaAclOperation.AlterConfigs,
        _ => KafkaAclOperation.Unknown,
    };

    private static AclPermissionType ToConfluentPermission(KafkaAclPermission permission) => permission switch
    {
        KafkaAclPermission.Any => AclPermissionType.Any,
        KafkaAclPermission.Allow => AclPermissionType.Allow,
        KafkaAclPermission.Deny => AclPermissionType.Deny,
        _ => AclPermissionType.Unknown,
    };

    private static KafkaAclPermission ToKafkaPermission(AclPermissionType permission) => permission switch
    {
        AclPermissionType.Any => KafkaAclPermission.Any,
        AclPermissionType.Allow => KafkaAclPermission.Allow,
        AclPermissionType.Deny => KafkaAclPermission.Deny,
        _ => KafkaAclPermission.Unknown,
    };
}
