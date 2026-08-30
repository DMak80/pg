using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Provisioning.Kafka;

namespace KafkaWorker.Provisioning.Processes;

/// <summary>
/// TopicSyncProcess (arch/16 §5 D, протокол arch/15 §3): автосинк реестра
/// topics/&lt;T&gt; с фактом Kafka + исполнение desired-заявок. Тик — только под
/// клэймом &lt;C&gt; и не чаще TopicSyncIntervalSec; describe→decide→act (decide —
/// чистые функции <see cref="TopicSyncDecision"/>). Запись — RMW txn по
/// mod_revision: проигрыш compare → топик пропускается до следующего тика
/// (панель успела переписать desired — применится свежий); свежая заявка
/// панели, поставленная после снапшота, не затирается. Транзиенты Kafka —
/// jitter-ретраи (порт Puzzle §7.4 поверх оркестрации, повтор безопасен).
/// </summary>
public sealed class TopicSyncProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    ClaimStore claims,
    WorkJournal journal,
    IKafkaAdminClientFactory adminFactory,
    TimeProvider timeProvider,
    int intervalSec)
{
    private const string Op = "topicsync";

    // Джиттер-ретраи: 3 попытки, экспонента от 300 мс ± половина (транзиенты
    // сети/перевыбора контроллера; повтор безопасен — операции идемпотентны).
    private const int RetryAttempts = 3;
    private const int RetryBaseMs = 300;

    private static readonly JsonSerializerOptions CanonicalJson = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    // Время последнего УСПЕШНОГО прогона по кластеру (троттлинг интервала;
    // провалившийся тик ретраится тиком Reconcile без штрафа ожидания).
    private readonly ConcurrentDictionary<string, long> _lastOk = new();

    public async Task<Result> RunAsync(KafkaClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Мутации — только держателем живого клэйма (arch/16 §5).
        if (!claims.IsMine(cluster))
            return Result.Failed(new ApplicationException(
                $"topicsync {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // Кластер не поднят — синкать нечего (endpoints появляются на K5).
        if (snap.Endpoints is null || snap.AppUser is null || snap.AppPassword is null)
            return Result.Success();

        // Троттлинг TopicSyncIntervalSec: подряд идущие тики Reconcile
        // (ScanIntervalSec) не должны дёргать Kafka чаще интервала.
        var now = timeProvider.GetUtcNow().ToUnixTimeSeconds();
        if (intervalSec > 0 && _lastOk.TryGetValue(cluster, out var last) && now - last < intervalSec)
            return Result.Success();

        var facts = await DescribeFactsAsync(snap, ct);
        if (!facts.IsSuccess)
            return facts;

        // Lifecycle-заявки — до факт-синка (порядок §3.1: чистка create → delete → create → sync).
        var lifecycle = TopicSyncDecision.DecideLifecycle(
            snap.LifecycleTickets ?? [], facts.Value, snap.Topics);
        foreach (var action in lifecycle)
        {
            var applied = await ActLifecycleAsync(snap, action, ct);
            if (!applied.IsSuccess)
                return applied; // транзиент: заявка жива, следующий тик повторит
        }

        var actions = TopicSyncDecision.Decide(facts.Value, snap.Topics);
        var factsByTopic = facts.Value.ToDictionary(f => f.Topic, StringComparer.Ordinal);
        var regsByTopic = snap.Topics.ToDictionary(r => r.Topic, StringComparer.Ordinal);

        foreach (var action in actions)
        {
            var applied = await ActAsync(snap, action, factsByTopic, regsByTopic, ct);
            if (!applied.IsSuccess)
                return applied; // транзиент: desired жив, следующий тик повторит
        }

        _lastOk[cluster] = now;
        return Result.Success();
    }

    // Describe-шаг: топики (метаданные) + факт-конфиги каждого не-__ топика.
    private async Task<Result<IReadOnlyList<TopicFact>>> DescribeFactsAsync(
        KafkaClusterSnapshot snap, CancellationToken ct)
    {
        await using var admin = adminFactory.Create(snap.Endpoints!, snap.AppUser!, snap.AppPassword!);

        var topics = await WithJitterRetryAsync(() => admin.DescribeTopicsAsync(includeInternal: false, ct));
        if (!topics.IsSuccess)
            return Result<IReadOnlyList<TopicFact>>.Failed(topics.Error!);

        var facts = new List<TopicFact>();
        foreach (var view in topics.Value)
        {
            // Internal-топики Kafka в реестр не попадают — конфиги их не читаем.
            if (view.Topic.StartsWith("__", StringComparison.Ordinal))
                continue;

            var configs = await WithJitterRetryAsync(() => admin.DescribeTopicConfigsAsync(view.Topic, ct));
            if (!configs.IsSuccess)
                return Result<IReadOnlyList<TopicFact>>.Failed(configs.Error!);

            facts.Add(new TopicFact(
                view.Topic,
                view.Partitions,
                (short?)view.ReplicasPerPartition.Select(p => p.Count).DefaultIfEmpty(0).Max(),
                ManagedOnly(configs.Value)));
        }

        return Result<IReadOnlyList<TopicFact>>.Success(facts);
    }

    // Исполнение lifecycle-действий (arch/15 §3.1): journal → Kafka-мутация →
    // txn-чистка (del заявки по mod_revision; при delete — вместе с факт-ключом).
    private async Task<Result> ActLifecycleAsync(KafkaClusterSnapshot snap, TopicSyncAction action, CancellationToken ct)
    {
        var cluster = snap.Cluster;
        switch (action)
        {
            case TopicSyncAction.LifecycleDelete del:
            {
                var ticketKey = LifecycleKey(cluster, del.Topic, TopicLifecycleOps.Delete);
                await using var admin = adminFactory.Create(snap.Endpoints!, snap.AppUser!, snap.AppPassword!);
                var journaled = await journal.WriteAsync(cluster, Op, $"deleting-topic:{del.Topic}", claims.InstanceId, null, ct);
                if (!journaled.IsSuccess)
                    return journaled;

                var deleted = await WithJitterRetryAsync(() => admin.DeleteTopicAsync(del.Topic, ct));
                if (!deleted.IsSuccess)
                    return deleted; // транзиент — заявка жива, тик повторит

                return await DeleteKeysAsync(
                    [TopicKey(cluster, del.Topic), ticketKey], ticketKey, ct);
            }

            case TopicSyncAction.LifecycleCreate create:
            {
                var ticketKey = LifecycleKey(cluster, create.Topic, TopicLifecycleOps.Create);
                await using var admin = adminFactory.Create(snap.Endpoints!, snap.AppUser!, snap.AppPassword!);
                var journaled = await journal.WriteAsync(cluster, Op, $"creating-topic:{create.Topic}", claims.InstanceId, null, ct);
                if (!journaled.IsSuccess)
                    return journaled;

                var created = await WithJitterRetryAsync(
                    () => admin.CreateTopicAsync(create.Topic, create.Partitions, create.ReplicationFactor, create.Configs, ct));
                if (!created.IsSuccess)
                    return created;

                // AlreadyExists = исполнено ранее; факт-ключ положит автосинк (§3.1).
                return await DeleteKeysAsync([ticketKey], ticketKey, ct);
            }

            case TopicSyncAction.LifecycleCleanup cleanup:
            {
                // Чистка без исполнения: журнал-примечание + del заявки (для
                // delete-ветки при отсутствующем топике — снести и missing-ключ).
                var journaled = await journal.WriteAsync(cluster, Op, $"ticket-cleanup:{cleanup.Topic}", claims.InstanceId, cleanup.Reason, ct);
                if (!journaled.IsSuccess)
                    return journaled;

                var keys = new List<string> { LifecycleKey(cluster, cleanup.Topic, cleanup.Op) };
                if (cleanup.Op == TopicLifecycleOps.Delete)
                    keys.Add(TopicKey(cluster, cleanup.Topic)); // missing-ключ висит без топика
                return await DeleteKeysAsync([.. keys], keys[0], ct);
            }

            default:
                return Result.Failed(new ApplicationException(
                    $"topicsync {cluster}: неизвестное lifecycle-действие {action.GetType().Name}"));
        }
    }

    // txn-удаление группы ключей с compare по mod_revision первого (заявки);
    // проигрыш compare — не ошибка (следующий тик).
    private async Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, string compareKey, CancellationToken ct)
    {
        var fresh = await GetAsync(compareKey, ct);
        if (!fresh.IsSuccess)
            return fresh;
        if (fresh.Value is null)
            return Result.Success(); // заявку уже снесли — идемпотентность

        var ops = keys.Select(k => new TxnOp.Delete(k, Prefix: false)).ToList();
        var txn = await TxnAsync(
            TxnRequest.Of([TxnCompare.ModRevisionEqual(compareKey, (long)fresh.Value.ModRevision)], ops), ct);
        if (!txn.IsSuccess)
            return txn;
        return Result.Success();
    }

    private static string LifecycleKey(string cluster, string topic, string op)
        => $"/kafka/clusters/{cluster}/topics/{topic}/desired.{op}";

    // Act: исполнение одного действия поверх СВЕЖЕГО значения ключа (RMW).
    private async Task<Result> ActAsync(        KafkaClusterSnapshot snap,
        TopicSyncAction action,
        IReadOnlyDictionary<string, TopicFact> factsByTopic,
        IReadOnlyDictionary<string, KafkaTopicReg> regsByTopic,
        CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var now = timeProvider.GetUtcNow().ToUnixTimeSeconds();

        switch (action)
        {
            case TopicSyncAction.Skip:
                return Result.Success();

            case TopicSyncAction.Sync sync:
            {
                var key = TopicKey(cluster, sync.Topic);
                var fresh = await GetAsync(key, ct);
                if (!fresh.IsSuccess)
                    return fresh;

                var desired = sync.Desired is null
                    ? DesiredAfterClear(fresh.Value, regsByTopic.GetValueOrDefault(sync.Topic))
                    : DesiredKept(fresh.Value, sync.Desired, regsByTopic.GetValueOrDefault(sync.Topic));
                var fact = factsByTopic.GetValueOrDefault(sync.Topic);
                return await WriteAsync(key, fresh.Value, new CanonicalTopic(
                    sync.Partitions,
                    sync.ReplicationFactor,
                    EmptyToNull(sync.Configs),
                    CanonicalDesired.Of(desired.Desired),
                    desired.DesiredUnix,
                    desired.DesiredBy,
                    now,
                    Missing: false), ct);
            }

            case TopicSyncAction.Apply apply:
            {
                var key = TopicKey(cluster, apply.Topic);
                await using var admin = adminFactory.Create(snap.Endpoints!, snap.AppUser!, snap.AppPassword!);

                // Конфиги ДО partitions (план C1: apply-порядок).
                if (apply.Configs.Count > 0)
                {
                    var altered = await WithJitterRetryAsync(
                        () => admin.AlterTopicConfigsAsync(apply.Topic, apply.Configs, ct));
                    if (!altered.IsSuccess)
                        return altered; // desired жив — ретрай тиком
                }

                if (apply.TotalPartitions is int total)
                {
                    var created = await WithJitterRetryAsync(() => admin.CreatePartitionsAsync(apply.Topic, total, ct));
                    if (!created.IsSuccess)
                        return created;
                }

                // Факт = заявке (partitions вырос, конфиги применены) + снятие
                // desired с защитой свежей заявки панели.
                var fresh = await GetAsync(key, ct);
                if (!fresh.IsSuccess)
                    return fresh;

                var fact = factsByTopic.GetValueOrDefault(apply.Topic);
                var merged = new Dictionary<string, string>(
                    fact?.Configs ?? new Dictionary<string, string>(StringComparer.Ordinal), StringComparer.Ordinal);
                foreach (var (name, value) in apply.Configs)
                    merged[name] = value;

                var applied = DesiredAfterClear(fresh.Value, regsByTopic.GetValueOrDefault(apply.Topic));
                return await WriteAsync(key, fresh.Value, new CanonicalTopic(
                    apply.TotalPartitions ?? fact?.Partitions ?? 0,
                    fact?.ReplicationFactor,
                    EmptyToNull(merged),
                    CanonicalDesired.Of(applied.Desired),
                    applied.DesiredUnix,
                    applied.DesiredBy,
                    now,
                    Missing: false), ct);
            }

            case TopicSyncAction.Reject reject:
            {
                var key = TopicKey(cluster, reject.Topic);

                // Перманентный отказ — журнал оператору (arch/16 §5 D), затем
                // факт без заявки (converge не буксует, заявка не висит вечно).
                var logged = await journal.WriteAsync(
                    cluster, Op, "rejected", claims.InstanceId, reject.Reason, ct);
                if (!logged.IsSuccess)
                    return logged;

                var fresh = await GetAsync(key, ct);
                if (!fresh.IsSuccess)
                    return fresh;

                var fact = factsByTopic.GetValueOrDefault(reject.Topic);
                var cleared = DesiredAfterClear(fresh.Value, regsByTopic.GetValueOrDefault(reject.Topic));
                return await WriteAsync(key, fresh.Value, new CanonicalTopic(
                    fact?.Partitions ?? 0,
                    fact?.ReplicationFactor,
                    EmptyToNull(fact?.Configs),
                    CanonicalDesired.Of(cleared.Desired),
                    cleared.DesiredUnix,
                    cleared.DesiredBy,
                    now,
                    Missing: false), ct);
            }

            case TopicSyncAction.MarkMissing missing:
                return await MarkMissingAsync(cluster, missing.Topic, ct);

            case TopicSyncAction.Forget forget:
                return await ForgetAsync(cluster, forget.Topic, ct);

            default:
                return Result.Failed(new ApplicationException(
                    $"topicsync {cluster}: неизвестное действие {action.GetType().Name}"));
        }
    }

    // missing=true: топик исчез при живой заявке (заявка не исполнима,
    // arch/15 §3); отмена заявки на месте превращается в удаление ключа.
    private async Task<Result> MarkMissingAsync(string cluster, string topic, CancellationToken ct)
    {
        var key = TopicKey(cluster, topic);
        var fresh = await GetAsync(key, ct);
        if (!fresh.IsSuccess)
            return fresh;
        if (fresh.Value is null)
            return Result.Success(); // ключ убран внешне — не наш случай

        var state = ParseKeyState(fresh.Value.Value);
        if (state.Desired is null)
            return await ForgetAsync(cluster, topic, ct); // панель отменила заявку
        if (state.Missing)
            return Result.Success(); // уже помечен

        return await WriteAsync(key, fresh.Value, new CanonicalTopic(
            state.Partitions,
            state.ReplicationFactor,
            state.Configs,
            CanonicalDesired.Of(state.Desired),
            state.DesiredUnix,
            state.DesiredBy,
            state.SyncedUnix, // факт не менялся — synced не тикаем
            Missing: true), ct);
    }

    // Удаление ключа: топик исчез, заявки нет. Если панель успела поставить
    // заявку ПОСЛЕ decide — превращаем в missing=true (заявка не исполнима).
    private async Task<Result> ForgetAsync(string cluster, string topic, CancellationToken ct)
    {
        var key = TopicKey(cluster, topic);
        var fresh = await GetAsync(key, ct);
        if (!fresh.IsSuccess)
            return fresh;
        if (fresh.Value is null)
            return Result.Success();

        if (ParseKeyState(fresh.Value.Value).Desired is not null)
            return await MarkMissingAsync(cluster, topic, ct);

        var txn = await TxnAsync(
            TxnRequest.Of(
                [TxnCompare.ModRevisionEqual(key, (long)fresh.Value.ModRevision)],
                [new TxnOp.Delete(key, Prefix: false)]),
            ct);
        if (!txn.IsSuccess)
            return txn;
        if (!txn.Value.Succeeded)
            return Result.Success(); // гонка с панелью — следующий тик разберёт

        return Result.Success();
    }

    /// <summary>
    /// Правило снятия заявки при записи факта: свежий desired из etcd
    /// сравнивается с заявкой из снапшота — равенство означает «наша решаемая
    /// заявка» (снимаем: исполнена/отклонена), отличие — панель переписала
    /// заявку после снапшота (сохраняем свежую — применится следующим тиком).
    /// </summary>
    private static (TopicDesired? Desired, long? DesiredUnix, string? DesiredBy) DesiredAfterClear(
        Kv? fresh, KafkaTopicReg? reg)
    {
        if (fresh is not null)
        {
            var state = ParseKeyState(fresh.Value);
            if (state.Desired is not null && !DesiredEquals(state.Desired, reg?.Desired))
                return (state.Desired, state.DesiredUnix, state.DesiredBy);
        }

        return (null, null, null);
    }

    // Сохранение живой заявки: свежие desired-поля etcd приоритетнее полей
    // снапшота (панель могла переписать заявку между снапшотом и актом).
    private static (TopicDesired? Desired, long? DesiredUnix, string? DesiredBy) DesiredKept(
        Kv? fresh, TopicDesired fallback, KafkaTopicReg? reg)
    {
        if (fresh is not null)
        {
            var state = ParseKeyState(fresh.Value);
            if (state.Desired is not null)
                return (state.Desired, state.DesiredUnix, state.DesiredBy);
        }

        return (fallback, reg?.DesiredUnix, reg?.DesiredBy);
    }

    // Запись ключа: отсутствующему — put (панель ключи topics/ не создаёт),
    // существующему — txn по mod_revision (проигрыш → следующий тик).
    private async Task<Result> WriteAsync(string key, Kv? fresh, CanonicalTopic entry, CancellationToken ct)
    {
        var value = JsonSerializer.Serialize(entry, CanonicalJson);

        if (fresh is null)
        {
            var put = await PutAsync(key, value, ct);
            return put.IsSuccess
                ? Result.Success()
                : put;
        }

        if (fresh.Value == value)
            return Result.Success(); // уже записано — идемпотентность повтора

        var txn = await TxnAsync(
            TxnRequest.Of(
                [TxnCompare.ModRevisionEqual(key, (long)fresh.ModRevision)],
                [new TxnOp.Put(key, value, null)]),
            ct);
        if (!txn.IsSuccess)
            return txn;
        if (!txn.Value.Succeeded)
            return Result.Success(); // панель переписала между read/write — не портим

        return Result.Success();
    }

    // Свежее состояние ключа topics/<T> (толерантный разбор, arch/15 §3).
    private sealed record TopicKeyState(
        int Partitions,
        short? ReplicationFactor,
        IReadOnlyDictionary<string, string>? Configs,
        TopicDesired? Desired,
        long? DesiredUnix,
        string? DesiredBy,
        long? SyncedUnix,
        bool Missing);

    private static TopicKeyState ParseKeyState(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        var root = doc.RootElement;

        TopicDesired? desired = null;
        if (root.TryGetProperty("desired", out var d) && d.ValueKind == JsonValueKind.Object)
        {
            IReadOnlyDictionary<string, string>? desiredConfigs = null;
            if (d.TryGetProperty("configs", out var dc) && dc.ValueKind == JsonValueKind.Object)
                desiredConfigs = dc.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.ToString());

            desired = new TopicDesired(ReadInt(d, "partitions"), desiredConfigs);
        }

        IReadOnlyDictionary<string, string>? configs = null;
        if (root.TryGetProperty("configs", out var fc) && fc.ValueKind == JsonValueKind.Object)
            configs = fc.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.ToString());

        return new TopicKeyState(
            ReadInt(root, "partitions") ?? 0,
            (short?)ReadInt(root, "replication_factor"),
            configs,
            desired,
            ReadLong(root, "desired_unix"),
            ReadString(root, "desired_by"),
            ReadLong(root, "synced_unix"),
            root.TryGetProperty("missing", out var m) && m.ValueKind == JsonValueKind.True);
    }

    private static bool DesiredEquals(TopicDesired? left, TopicDesired? right)
    {
        if (left is null || right is null)
            return left is null && right is null;
        if (left.Partitions != right.Partitions)
            return false;

        var lc = left.Configs ?? new Dictionary<string, string>();
        var rc = right.Configs ?? new Dictionary<string, string>();
        return lc.Count == rc.Count && lc.All(p => rc.TryGetValue(p.Key, out var v) && v == p.Value);
    }

    // Только управляемые конфиги попадают в факт реестра (arch/15 §3).
    private static IReadOnlyDictionary<string, string>? ManagedOnly(IReadOnlyDictionary<string, string> configs)
    {
        var managed = configs
            .Where(p => TopicSyncDecision.ManagedTopicConfigs.Contains(p.Key))
            .ToDictionary(p => p.Key, p => p.Value);
        return managed.Count > 0 ? managed : null;
    }

    private static IReadOnlyDictionary<string, string>? EmptyToNull(IReadOnlyDictionary<string, string>? configs)
        => configs is { Count: > 0 } ? configs : null;

    // Транзиент-ретраи с джиттером (порт Puzzle §7.4 поверх оркестрации).
    private static async Task<Result> WithJitterRetryAsync(Func<Task<Result>> call)
    {
        Result last = Result.Failed(new ApplicationException("unreachable"));
        for (var attempt = 0; attempt < RetryAttempts; attempt++)
        {
            if (attempt > 0)
                await JitterDelayAsync(attempt);

            last = await call();
            if (last.IsSuccess)
                return last;
        }

        return last;
    }

    private static async Task<Result<T>> WithJitterRetryAsync<T>(Func<Task<Result<T>>> call)
    {
        Result<T> last = Result<T>.Failed(new ApplicationException("unreachable"));
        for (var attempt = 0; attempt < RetryAttempts; attempt++)
        {
            if (attempt > 0)
                await JitterDelayAsync(attempt);

            last = await call();
            if (last.IsSuccess)
                return last;
        }

        return last;
    }

    private static Task JitterDelayAsync(int attempt)
    {
        var delay = RetryBaseMs * (1 << (attempt - 1));
        var jitter = Random.Shared.Next(-delay / 2, delay / 2 + 1);
        return Task.Delay(Math.Max(50, delay + jitter));
    }

    private static string? ReadString(JsonElement root, string name)
        => root.ValueKind == JsonValueKind.Object
            && root.TryGetProperty(name, out var element)
            && element.ValueKind is JsonValueKind.String
            ? element.GetString()
            : null;

    private static long? ReadLong(JsonElement root, string name)
        => root.ValueKind == JsonValueKind.Object
            && root.TryGetProperty(name, out var element)
            && element.ValueKind is JsonValueKind.Number
            && element.TryGetInt64(out var value)
            ? value
            : null;

    private static int? ReadInt(JsonElement root, string name)
    {
        var value = ReadLong(root, name);
        return value is null or > int.MaxValue or < int.MinValue ? null : (int?)value.Value;
    }

    private static string TopicKey(string cluster, string topic)
        => $"/kafka/clusters/{cluster}/topics/{topic}";

    // Failover-обёртки: первый успешный endpoint выигрывает.
    private async Task<Result<Kv?>> GetAsync(string key, CancellationToken ct)
        => await WithFailoverAsync(endpoint => etcd.GetAsync(endpoint, key, ct));

    private async Task<Result> PutAsync(string key, string value, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.PutAsync(endpoint, key, value, null, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private async Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
        => await WithFailoverAsync(endpoint => etcd.TxnAsync(endpoint, req, ct));

    private async Task<Result<T>> WithFailoverAsync<T>(Func<string, Task<Result<T>>> call)
    {
        Result<T>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    // Канонический JSON значения ключа topics/<T> (arch/15 §3): факт +
    // опциональная заявка desired + synced_unix/missing.
    private sealed record CanonicalTopic(
        [property: JsonPropertyName("partitions")] int Partitions,
        [property: JsonPropertyName("replication_factor")] int? ReplicationFactor,
        [property: JsonPropertyName("configs")] IReadOnlyDictionary<string, string>? Configs,
        [property: JsonPropertyName("desired")] CanonicalDesired? Desired,
        [property: JsonPropertyName("desired_unix")] long? DesiredUnix,
        [property: JsonPropertyName("desired_by")] string? DesiredBy,
        [property: JsonPropertyName("synced_unix")] long? SyncedUnix,
        [property: JsonPropertyName("missing")] bool Missing);

    // Заявка desired каноническим JSON (partitions?/configs? — null не пишем).
    private sealed record CanonicalDesired(
        [property: JsonPropertyName("partitions")] int? Partitions,
        [property: JsonPropertyName("configs")] IReadOnlyDictionary<string, string>? Configs)
    {
        public static CanonicalDesired? Of(TopicDesired? desired)
            => desired is null
                ? null
                : new CanonicalDesired(desired.Partitions, desired.Configs is { Count: > 0 } ? desired.Configs : null);
    }
}
