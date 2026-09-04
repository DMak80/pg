using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaWorker.Core;
using Microsoft.Extensions.Logging;

namespace KafkaWorker.Provisioning.Kafka;

// Адаптер над Confluent.Kafka IAdminClient — единственное место воркера с
// Confluent-типами (seam-паттерн Puzzle §7.2). SASL_SSL/PLAIN + доверие
// per-cluster CA по контракту дискавери arch/15 §5 (t03); RequestTimeout — из
// опций воркера (на вызов); любые исключения Kafka-клиента → Result.Failed
// (процессы решают, транзиент или нет).

/// <summary>
/// Кэширующая фабрика AdminClient-адаптеров (t05, spec §3.1): один адаптер
/// per (bootstrap, user, password, caPem) вместо «клиент на тик» — churn
/// rd_kafka-инстансов и LongRunning-потоков на недоступном кластере съедал
/// ~100% ядра (инцидент as-kafkaworker 2026-09-04). Sharable: DisposeAsync
/// адаптера — no-op, владение у кэша; смена endpoints/кредов/TLS-доверия —
/// другой ключ (инвалидация по построению; t03: caPem в ключе — клиенты с
/// разным per-cluster CA не шарятся); Failed операции помечает запись
/// Unhealthy — следующий Create пересоздаёт, заменяемый Disposeится в фоне
/// (Dispose недоступного клиента может ждать poll-поток — не в горячем пути
/// тика); неактивные &gt; IdleEvictAfter вытесняются при Create; остановка
/// host'а — детерминированный Dispose всех (IDisposable через DI).
/// </summary>
public sealed class KafkaAdminClientFactory(
    TimeSpan requestTimeout,
    ILogger<KafkaAdminClientFactory>? logger = null,
    TimeProvider? clock = null) : IKafkaAdminClientFactory, IDisposable
{
    // Профиль librdkafka (t05, паттерн t11): дефолтные 100 мс backoff при
    // мгновенном connection-refusal дают reconnect-шторм («3/3 brokers are
    // down» каждую секунду) — затыкаем до ≥1 c.
    internal const int BackoffMs = 1000;
    internal const int BackoffMaxMs = 10000;

    // Вытеснение неактивных ключей (кластер удалён / креды сменены) —
    // нативные потоки не копятся; без таймеров: чистка с каждым Create.
    internal static readonly TimeSpan IdleEvictAfter = TimeSpan.FromMinutes(10);

    private readonly TimeProvider _clock = clock ?? TimeProvider.System;
    private readonly object _gate = new();
    private readonly Dictionary<(string Bootstrap, string User, string Password, string? CaPem), Entry> _entries = [];

    // Сколько адаптеров создано за жизнь фабрики — метрика churn'а
    // (public: интеграционные тесты строят на ней границы).
    public int CreatedClients { get; private set; }

    public IKafkaAdminClient Create(string bootstrap, string user, string password, string? caPem)
    {
        Entry entry;
        lock (_gate)
        {
            var now = _clock.GetUtcNow();
            EvictIdle(now);
            var key = (bootstrap, user, password, caPem);
            if (_entries.TryGetValue(key, out var current) && !current.Unhealthy)
            {
                current.LastUsedUtc = now;
                return current.Client;
            }

            CreatedClients++;
            Entry? marked = null;
            entry = new Entry(new KafkaAdminClient(
                bootstrap, user, password, caPem, requestTimeout, logger,
                onFailed: () => { if (marked is { } m) lock (_gate) m.Unhealthy = true; }))
            {
                LastUsedUtc = now,
            };
            marked = entry;
            if (_entries.Remove(key, out var replaced))
                Task.Run(replaced.Client.DisposeNative); // заменяемый — фон (Dispose ждёт poll-поток)
            _entries[key] = entry;
        }

        return entry.Client;
    }

    // Вытеснение неактивных: заменяемые Disposeятся в фоне (Dispose ждёт
    // poll-поток; не блокирует тик). Вызывается под _gate.
    private void EvictIdle(DateTimeOffset now)
    {
        List<Entry>? evicted = null;
        foreach (var (key, entry) in _entries)
        {
            if (now - entry.LastUsedUtc <= IdleEvictAfter)
                continue;
            evicted ??= [];
            evicted.Add(entry);
            _entries.Remove(key);
        }

        if (evicted is null)
            return;
        foreach (var entry in evicted)
            Task.Run(entry.Client.DisposeNative);
    }

    // Остановка host'а — детерминированно: клиенты с backoff-пинами не
    // штормуют, poll-потоки выходят быстро (паттерн t11).
    public void Dispose()
    {
        List<Entry> removed;
        lock (_gate)
        {
            removed = [.. _entries.Values];
            _entries.Clear();
        }

        foreach (var entry in removed)
            entry.Client.DisposeNative();
    }

    // Профиль конфига всех клиентов фабрики (internal — юнит-проверки пинов):
    // t03 дискавери-канон arch/15 §5 — SASL_SSL/PLAIN + доверие per-cluster CA
    // (caPem null — без ssl.ca.pem, системное доверие: тесты/fake).
    internal static AdminClientConfig BaseAdminConfig(string bootstrap, string user, string password, string? caPem)
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = bootstrap,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain,
            SaslUsername = user,
            SaslPassword = password,
            RetryBackoffMs = BackoffMs,
            ReconnectBackoffMs = BackoffMs,
            ReconnectBackoffMaxMs = BackoffMaxMs,
        };
        if (caPem is not null)
            config.Set("ssl.ca.pem", caPem); // доверие per-cluster CA (librdkafka >= 1.5)
        return config;
    }

    private sealed class Entry(KafkaAdminClient client)
    {
        public readonly KafkaAdminClient Client = client;
        public DateTimeOffset LastUsedUtc;
        public bool Unhealthy;
    }
}

public sealed class KafkaAdminClient(
    string bootstrap,
    string user,
    string password,
    string? caPem,
    TimeSpan requestTimeout,
    ILogger? log = null,
    Action? onFailed = null) : IKafkaAdminClient
{
    // Ленивый клиент: создаётся при первом вызове (пустые операции не ходят в сеть).
    private IAdminClient? _client;

    // Инициализация/Dispose нативного клиента взаимоисключены (ревью Ф7-2):
    // два параллельных первых вызова (supervise-тик + коллектор на одном ключе
    // кэша) без lock'а строили бы два rd_kafka-инстанса — проигравший сирота
    // до финализатора; Dispose кэша при shutdown vs параллельный первый вызов.
    private readonly object _clientGate = new();

    // Ошибка операции → unhealthy-инвалидация записи кэша (следующий Create
    // пересоздаёт клиент); отмена host'а — не фейл (см. IsHostCancellation).
    internal void NotifyFailed() => onFailed?.Invoke();

    // Отмена host'а (OCE при отменённом токене) не инвалидирует клиента:
    // остановка приложения — не проблема клиента.
    internal static bool IsHostCancellation(Exception e, CancellationToken ct)
        => e is OperationCanceledException && ct.IsCancellationRequested;

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

    // DisposeAsync — no-op (t05, spec §3.1): владение у кэша фабрики,
    // «клиент на тик» не уничтожает нативные потоки; реальный Dispose —
    // вытеснение/вытеснение-по-неактивности/остановка host'а (DisposeNative).
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    // Реальный Dispose нативного клиента (кэш фабрики; может ждать poll-поток —
    // зовётся только в фоне или при shutdown). Под _clientGate: Dispose vs
    // параллельная первая инициализация взаимоисключены (ревью Ф7-2).
    internal void DisposeNative()
    {
        lock (_clientGate)
        {
            _client?.Dispose();
            _client = null;
        }
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

    public Task<Result<IReadOnlyList<KafkaGroupView>>> ListGroupsAsync(CancellationToken ct)
        => RunAsync<IReadOnlyList<KafkaGroupView>>(async client =>
        {
            var groups = await client.ListConsumerGroupsAsync(new ListConsumerGroupsOptions
            {
                RequestTimeout = requestTimeout,
            });
            return (IReadOnlyList<KafkaGroupView>)groups.Valid
                .Select(g => new KafkaGroupView(g.GroupId, g.State.ToString()))
                .ToList();
        }, ct);

    public Task<Result<IReadOnlyList<KafkaTopicPartitionOffset>>> ListConsumerGroupOffsetsAsync(
        string group, CancellationToken ct)
        => RunAsync<IReadOnlyList<KafkaTopicPartitionOffset>>(async client =>
        {
            var committed = await client.ListConsumerGroupOffsetsAsync(
                // TopicPartitions = null → committed ВСЕХ партиций группы (контракт Confluent).
                [new ConsumerGroupTopicPartitions(group, null!)],
                new ListConsumerGroupOffsetsOptions { RequestTimeout = requestTimeout });
            return (IReadOnlyList<KafkaTopicPartitionOffset>)committed
                .SelectMany(c => c.Partitions)
                // Offset.Unset (−1001, OFFSET_INVALID) — committed'а нет: пропускаем
                // (лаг по партиции без committed не определён, консервативно не считаем).
                .Where(tpo => tpo.Offset.Value != Offset.Unset.Value)
                .Select(tpo => new KafkaTopicPartitionOffset(tpo.Topic, tpo.Partition.Value, tpo.Offset.Value))
                .ToList();
        }, ct);

    public Task<Result<IReadOnlyList<KafkaTopicPartitionOffset>>> ListOffsetsAsync(
        IReadOnlyList<KafkaTopicPartition> partitions, CancellationToken ct)
        => RunAsync<IReadOnlyList<KafkaTopicPartitionOffset>>(async client =>
        {
            var watermarks = await client.ListOffsetsAsync(
                partitions.Select(p => new TopicPartitionOffsetSpec
                {
                    TopicPartition = new TopicPartition(p.Topic, p.Partition),
                    OffsetSpec = OffsetSpec.Latest(),
                }),
                new ListOffsetsOptions { RequestTimeout = requestTimeout });
            return (IReadOnlyList<KafkaTopicPartitionOffset>)watermarks.ResultInfos
                .Select(info =>
                {
                    var tpo = info.TopicPartitionOffsetError;
                    return new KafkaTopicPartitionOffset(tpo.Topic, tpo.Partition.Value, tpo.Offset.Value);
                })
                .ToList();
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

    // Общий каркас: клиент создаётся один раз; исключения → Result.Failed
    // (+ unhealthy-пометка записи кэша, кроме отмены host'а).
    private async Task<Result> RunAsync(Func<IAdminClient, Task<Result>> action, CancellationToken ct)
    {
        try
        {
            ct.ThrowIfCancellationRequested();
            return await action(EnsureClient());
        }
        catch (Exception e)
        {
            if (!IsHostCancellation(e, ct))
                NotifyFailed();
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
            if (!IsHostCancellation(e, ct))
                NotifyFailed();
            return Result<T>.Failed(new ApplicationException($"Kafka AdminClient ({bootstrap}): {e.Message}", e));
        }
    }

    // Double-checked lock (ревью Ф7-2): быстрый путь без lock'а (после
    // инициализации стоимость нулевая); гонка первых вызовов строит ровно
    // один нативный клиент.
    private IAdminClient EnsureClient()
    {
        if (_client is not null)
            return _client;

        lock (_clientGate)
        {
            if (_client is not null)
                return _client;

            // Пины backoff + rdkafka-лог на Debug (профиль фабрики, t05 spec §3.1):
            // дефолтные 100 мс давали reconnect-шторм на лежащем кластере.
            // t03: конфиг — SASL_SSL + per-cluster CA (BaseAdminConfig).
            _client = new AdminClientBuilder(
                    KafkaAdminClientFactory.BaseAdminConfig(bootstrap, user, password, caPem))
                .SetLogHandler((_, m) => log?.LogDebug("rdkafka: {Message}", m.Message))
                .Build();
            return _client;
        }
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
