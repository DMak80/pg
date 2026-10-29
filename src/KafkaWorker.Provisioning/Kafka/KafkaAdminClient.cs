using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaWorker.Core;
using Microsoft.Extensions.Logging;
using System.IO;

namespace KafkaWorker.Provisioning.Kafka;

// Адаптер над Confluent.Kafka IAdminClient — единственное место воркера с
// Confluent-типами (seam-паттерн Puzzle §7.2). SASL_SSL/PLAIN + доверие
// per-cluster CA по контракту дискавери arch/15 §5 ; RequestTimeout — из
// опций воркера (на вызов); любые исключения Kafka-клиента → Result.Failed
// (процессы решают, транзиент или нет).

/// <summary>
/// Кэширующая фабрика AdminClient-адаптеров (spec §3.1): один адаптер
/// per (bootstrap, user, password, caPem) вместо «клиент на тик» — churn
/// rd_kafka-инстансов и LongRunning-потоков на недоступном кластере съедал
/// ~100% ядра (инцидент as-kafkaworker 2026-09-04). Sharable: DisposeAsync
/// адаптера — no-op, владение у кэша; смена endpoints/кредов/TLS-доверия —
/// другой ключ (инвалидация по построению; : caPem в ключе — клиенты с
/// разным per-cluster CA не шарятся); Failed нативной операции помечает запись
/// Unhealthy (протокольная операция без нативного клиента — нет), заменяемый
/// Disposeится в фоне ПОСЛЕ завершения его висящих операций — дрейн по
/// счётчику с бюджетом; бюджет не превращается в dispose под операцией;
/// вход операции и dispose-решение атомарны (один замок): dispose под
/// начатой операцией исключён, опоздавшая операция получает транзиентный
/// отказ «клиент в dispose» (arch/16 §6); неактивные
/// &gt; IdleEvictAfter вытесняются при Create; остановка host'а —
/// детерминированный Dispose всех (IDisposable через DI).
/// </summary>
public sealed class KafkaAdminClientFactory(
    TimeSpan requestTimeout,
    ILogger<KafkaAdminClientFactory>? logger = null,
    TimeProvider? clock = null) : IKafkaAdminClientFactory, IDisposable
{
    // Профиль librdkafka (паттерн): дефолтные 100 мс backoff при
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
            {
                // Замена немедленна (запись удалена из кеша); dispose
                // заменяемого — фоном ПОСЛЕ его висящих операций (arch/16 §6,
                // ERR__DESTROYED под операцией запрещён; бюджет не принуждает).
                Task.Run(() => replaced.Client.DisposeWhenIdleAsync(
                    KafkaAdminClient.DrainGrace(requestTimeout), CancellationToken.None,
                    forceOnBudgetExhausted: false));
            }
            _entries[key] = entry;
        }

        return entry.Client;
    }

    // Вытеснение неактивных: заменяемые Disposeятся в фоне через дрейн
    // (Dispose ждёт poll-поток и завершения операций; не блокирует тик).
    // Вызывается под _gate.
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
            Task.Run(() => entry.Client.DisposeWhenIdleAsync(
                KafkaAdminClient.DrainGrace(requestTimeout), CancellationToken.None,
                forceOnBudgetExhausted: false));
    }

    // Остановка host'а — детерминированно: тот же дрейн с бюджетом по
    // каждому клиенту (arch/16 §6) — без операций немедленно, с операциями
    // не дольше бюджета (записей немного — параллельно, WaitAll).
    public void Dispose()
        => Dispose(KafkaAdminClient.DrainGrace(requestTimeout));

    internal void Dispose(TimeSpan grace)
    {
        List<Entry> removed;
        lock (_gate)
        {
            removed = [.. _entries.Values];
            _entries.Clear();
        }

        Task.WaitAll(removed.Select(
            e => e.Client.DisposeWhenIdleAsync(
                grace, CancellationToken.None, forceOnBudgetExhausted: true)).ToArray());
    }

    // Профиль конфига всех клиентов фабрики (internal — юнит-проверки пинов):
    //  дискавери-канон arch/15 §5 — SASL_SSL/PLAIN + доверие per-cluster CA
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
            config.Set("ssl.ca.location", CaPemFile(caPem)); // файл: bundle окна ротации 
        return config;
    }

    // Truststore ФАЙЛОМ, а не inline ssl.ca.pem: librdkafka инлайн-PEM читает
    // ОДИН серт — bundle OLD+NEW окна ротации CA инлайн не работает.
    // Файл кешируется по содержимому (SHA-256) — инвалидируется сменой CA.
    internal static string CaPemFile(string caPem)
    {
        var hash = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(
            System.Text.Encoding.UTF8.GetBytes(caPem)));
        var path = Path.Combine(Path.GetTempPath(), $"kfw-ca-{hash[..16].ToLowerInvariant()}.pem");
        if (!File.Exists(path))
            File.WriteAllText(path, caPem);
        return path;
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

    // Счётчик активных операций и дрейн-dispose (arch/16 §6): dispose нативного
    // клиента под висящей операцией запрещён (ERR__DESTROYED) — заменяемый
    // клиент диспозится фоном по нулю операций (continuation последней).
    private int _activeOperations;

    // Dispose стартовал (замена/вытеснение/остановка): вход новых операций
    // закрыт. Живёт под _clientGate вместе со счётчиком — вход операции и
    // dispose-решение атомарны: окно «операция началась после проверки нуля,
    // но до DisposeNative» (ERR__DESTROYED под только что начатой операцией)
    // исключено по построению.
    private bool _disposing;

    // Ожидание «операций нет» (ставится DisposeWhenIdleAsync; завершается
    // EndOperation при нуле). RunContinuationsAsynchronously — continuation
    // дрейна не выполняется в потоке операции.
    private TaskCompletionSource? _drainTcs;

    // Тестовое наблюдение (юниты дрейна; InternalsVisibleTo): сколько раз
    // исполнялся DisposeNative и сколько операций висит сейчас.
    internal int DisposeNativeCount;

    internal int ActiveOperations => Volatile.Read(ref _activeOperations);

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

    // DisposeAsync — no-op (spec §3.1): владение у кэша фабрики,
    // «клиент на тик» не уничтожает нативные потоки; реальный Dispose —
    // вытеснение/вытеснение-по-неактивности/остановка host'а (DisposeNative).
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    // Попытка dispose: атомарна со стартом операции (TryBeginOperation под тем
    // же _clientGate) — либо операция раньше встала на счётчик (false, дрейн
    // ждёт её нуля), либо вход закрыт флагом и dispose исполняется. Может
    // ждать poll-поток — зовётся только в фоне или при shutdown.
    private bool TryDisposeNative()
    {
        lock (_clientGate)
        {
            if (_disposing || Volatile.Read(ref _activeOperations) != 0)
                return false;

            _disposing = true;
            DisposeNativeCount++;
            _client?.Dispose();
            _client = null;
            return true;
        }
    }

    // Принудительный dispose по исчерпании бюджета дрейна («не ждать вечно»):
    // закрывает вход новых операций флагом и диспозит даже при живом счётчике.
    private void ForceDisposeNative()
    {
        lock (_clientGate)
        {
            _disposing = true;
            DisposeNativeCount++;
            _client?.Dispose();
            _client = null;
        }
    }

    // Dispose после завершения висящих операций (arch/16 §6): операций нет —
    // немедленно (TryDisposeNative); есть — фоном ждём нуля (continuation
    // последней) с бюджетом. В режиме замены/вытеснения исчерпание бюджета —
    // warning и продолжение ожидания: нативный хендл не уничтожается под
    // операцией. Принудительный dispose допустим только при остановке host'а.
    // Операция, входящая между нулём счётчика и dispose (гэп пробуждения дрейна),
    // получает транзиентный отказ «клиент в dispose» — на уничтожаемый
    // хендл не стартует. Гарантии сети не меняются: замена и раздача
    // клиентов уже произошли (запись удалена из кеша ДО этого вызова).
    internal Task DisposeWhenIdleAsync(TimeSpan budget, CancellationToken ct)
        => DisposeWhenIdleAsync(budget, ct, forceOnBudgetExhausted: false);

    internal async Task DisposeWhenIdleAsync(
        TimeSpan budget,
        CancellationToken ct,
        bool forceOnBudgetExhausted)
    {
        while (!TryDisposeNative())
        {
            Task idle;
            lock (_clientGate)
            {
                _drainTcs ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                idle = _drainTcs.Task;
            }

            // Операции могли завершиться в гэпе установки ожидания — повтор
            // немедленно, не висеть до бюджета.
            if (TryDisposeNative())
                return;

            var finished = await Task.WhenAny(idle, Task.Delay(budget, ct)).ConfigureAwait(false);
            if (finished != idle)
            {
                log?.LogWarning(
                    "Kafka AdminClient ({Bootstrap}): drain dispose budget of {Budget:F0} s exhausted with {Active} active operations",
                    bootstrap, budget.TotalSeconds, Volatile.Read(ref _activeOperations));
                if (forceOnBudgetExhausted)
                {
                    // Только остановка host'а: детерминированный выход важнее
                    // гипотетически ещё живой операции; замена/вытеснение этот
                    // путь не исполняет (ERR__DESTROYED под операцией).
                    ForceDisposeNative();
                    return;
                }

                // Нативный клиент жив до фактического нуля операций; ожидание
                // завершит EndOperation. Итог фоновый, замена уже не блокирует.
                await idle.ConfigureAwait(false);
            }

            // Ноль достигнут — цикл повторит попытку; операция, начатая в гэпе
            // пробуждения, снова встанет на счётчик и дождётся своей очереди.
        }
    }

    // Бюджет дрейна dispose (arch/16 §6): операции сами ограничены
    // requestTimeout (передаётся в каждый Options.RequestTimeout), хвост —
    // на завершение continuation'ов/выход poll-потоков.
    internal static TimeSpan DrainGrace(TimeSpan requestTimeout)
        => requestTimeout + TimeSpan.FromSeconds(30);

    // Тестовый шов (юниты дрейна): действие в каркасе RunAsync — счётчик
    // операций и unhealthy-пометка работают как в проде; нативный клиент
    // не строится (действие решает само, звать ли EnsureClient).
    internal Task<Result> RunUnderOperationAsync(Func<IAdminClient, Task<Result>> action, CancellationToken ct)
        => RunAsync(action, ct);

    // Тестовый шов каркаса протокольной операции: unhealthy-пометка не ставится.
    internal Task<Result> RunLocalUnderOperationAsync(Func<Task<Result>> action, CancellationToken ct)
        => RunLocalAsync(action, ct);

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
    // (+ unhealthy-пометка записи кэша, кроме отмены host'а). Операция
    // стоит на счётчике всё время выполнения (arch/16 §6): дрейн-dispose
    // заменяемого ждёт нуля — dispose под висящей операцией исключён.
    // Клиент в dispose (замена/вытеснение/остановка) операцию не принимает:
    // транзиентный отказ без unhealthy-пометки — следующий Create раздаст
    // свежий клиент, повтор идемпотентен.
    private async Task<Result> RunAsync(Func<IAdminClient, Task<Result>> action, CancellationToken ct)
    {
        if (!TryBeginOperation())
            return Result.Failed(ClientDisposingError());
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
        finally
        {
            EndOperation();
        }
    }

    private async Task<Result<T>> RunAsync<T>(Func<IAdminClient, Task<T>> action, CancellationToken ct)
    {
        if (!TryBeginOperation())
            return Result<T>.Failed(ClientDisposingError());
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
        finally
        {
            EndOperation();
        }
    }

    // Каркас для операций БЕЗ нативного клиента (протокольный миниклиент
    // reassign-подачи): та же структура — TryBeginOperation/EndOperation
    // (дрейн-dispose честен и для протокольных операций), catch → IsHostCancellation
    // + Result.Failed с префиксом адаптера, — но EnsureClient не зовётся:
    // rd_kafka-инстанс для операции не строится. Unhealthy-пометки нет: отказ
    // короткоживущего протокольного соединения не говорит о здоровье кэшируемого
    // нативного клиента и не должен вызывать его replace+dispose.
    private async Task<Result> RunLocalAsync(Func<Task<Result>> action, CancellationToken ct)
    {
        if (!TryBeginOperation())
            return Result.Failed(ClientDisposingError());
        try
        {
            ct.ThrowIfCancellationRequested();
            return await action();
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException($"Kafka AdminClient ({bootstrap}): {e.Message}", e));
        }
        finally
        {
            EndOperation();
        }
    }

    // Вход операции: под _clientGate вместе с dispose-решением
    // (TryDisposeNative) — атомарность «операция началась ДО dispose» /
    // «dispose закрыл вход». Инкремент и декремент по-прежнему симметричны
    // (finally каркаса), исключение/отмена тоже освобождают слот.
    private bool TryBeginOperation()
    {
        lock (_clientGate)
        {
            if (_disposing)
                return false;

            _activeOperations++;
            return true;
        }
    }

    // Управляемый транзиентный отказ операции на диспозящемся клиенте: не
    // сбой клиента (unhealthy-пометка НЕ ставится) — плановая замена кэша.
    private ApplicationException ClientDisposingError()
        => new(
            $"Kafka AdminClient ({bootstrap}): client is in dispose (cache replacement/eviction) — operation not started, a retry by the next call will get a fresh client");

    // Счётчик операций: декремент в finally — исключение/отмена тоже
    // освобождают слот. Ноль при поставленном ожидании дрейна завершает его
    // (continuation последней операции).
    private void EndOperation()
    {
        if (Interlocked.Decrement(ref _activeOperations) != 0)
            return;

        TaskCompletionSource? waiter;
        lock (_clientGate)
        {
            waiter = _drainTcs;
            _drainTcs = null;
        }

        waiter?.TrySetResult();
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

            // Пины backoff + rdkafka-лог на Debug (профиль фабрики, spec §3.1):
            // дефолтные 100 мс давали reconnect-шторм на лежащем кластере.
            // конфиг — SASL_SSL + per-cluster CA (BaseAdminConfig).
            _client = new AdminClientBuilder(
                    KafkaAdminClientFactory.BaseAdminConfig(bootstrap, user, password, caPem))
                .SetLogHandler((_, m) => log?.LogDebug("rdkafka: {Message}", m.Message))
                .Build();
            return _client;
        }
    }

    // ===== ACL (arch/16 §2.3/E): Describe/Create/Delete через seam-типы =====

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

    // ===== Reassignment (arch/16 §2.4): подача AlterReassignments =====

    // Per-endpoint бюджет подач (connect+TLS+SASL+round-trip): подача
    // регистрируется контроллером и отвечает быстро — это НЕ ожидание
    // физического переноса данных (реквестный timeout_ms=60000 — таймаут
    // обработки заявки). Внешний бюджет процесса — ReassignSubmitSec.
    internal static readonly TimeSpan NetworkSubmitTimeout = TimeSpan.FromSeconds(30);

    // Клиент протокола per-call: соединение короткоживущее, кэшировать нечего
    // (нативного клиента нет — churn-проблемы не применимы). Каркас
    // RunLocalAsync — без EnsureClient: протокольная операция нативный клиент
    // не использует, строить rd_kafka-инстанс впустую не нужно. unhealthy-
    // пометка для протокольных операций не срабатывает — миниклиент конвертирует
    // любые отказы в Result.Failed (per-call клиенту нечего лечить); отмена
    // host'а не фейлит (проброс из миниклиента и IsHostCancellation).
    public Task<Result> AlterReassignmentsAsync(IReadOnlyList<ReassignMove> moves, CancellationToken ct)
        => RunLocalAsync(async () => await new KafkaProtocolClient().AlterReassignmentsAsync(
            bootstrap, user, password, caPem, moves, NetworkSubmitTimeout, ct), ct);

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
