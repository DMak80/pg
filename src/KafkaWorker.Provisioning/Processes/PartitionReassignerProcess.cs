using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization;
using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Docker.Drivers;
using KafkaWorker.Etcd.Client;
using KafkaWorker.Etcd.Coordination;
using KafkaWorker.Provisioning.Kafka;

namespace KafkaWorker.Provisioning.Processes;

/// <summary>
/// Прогресс reassignment /kafkaworker/reassignments/&lt;C&gt; (arch/15 §4):
/// перестраиваемый кэш хода (источник истины — метаданные Kafka), camelCase
/// как WorkState. Отсутствие ключа = операции нет.
/// </summary>
public sealed record ReassignProgress(
    [property: JsonPropertyName("mode")] string Mode,
    [property: JsonPropertyName("drain_broker")] string? DrainBroker,
    [property: JsonPropertyName("partitions_total")] int PartitionsTotal,
    [property: JsonPropertyName("partitions_remaining")] int PartitionsRemaining,
    [property: JsonPropertyName("submitted_unix")] long SubmittedUnix,
    [property: JsonPropertyName("updated_unix")] long UpdatedUnix,
    [property: JsonPropertyName("instance")] string Instance,
    [property: JsonPropertyName("last_error")] string? LastError = null);

/// <summary>
/// PartitionReassignerProcess (arch/16 §5 I; spec t02 §5): перенос реплик
/// партиций между брокерами. Drain (приоритет): опустошение брокера
/// state=TO_REMOVE — каждым тиком план пересчитывается из свежих метаданных
/// describe-all (включая __-топики), подаются идемпотентные батчи ≤
/// BatchPartitions через kafka-reassign-partitions CLI docker-exec'ом в
/// контейнер брокера; завершение по факту (drain-брокер вне Replicas и нет
/// USR). Balance: converge к декларации по заявке /kafkaworker/rebalances/&lt;C&gt;
/// (панель), заявка снимается по сходимости. Слепая проба — никаких подач
/// (собственная слепота воркера не повод трогать партиции), прошлый
/// прогресс-ключ не трогается. Прогресс-ключ пишет только держатель клэйма.
/// </summary>
public sealed class PartitionReassignerProcess(
    IEtcdGateway etcd,
    string[] endpoints,
    IClusterDriver driver,
    ClaimStore claims,
    WorkJournal journal,
    IKafkaAdminClientFactory adminFactory,
    ReassignOptions options,
    TimeProvider timeProvider)
{
    public const string Op = "reassign";

    private static readonly JsonSerializerOptions ProgressJson = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    // Время последнего успешного тика (троттл IntervalSec; провал — без штрафа)
    // и последней подачи батча (дедуп переподачи RetrySubmitSec) по кластеру:
    // повтор ТОГО ЖЕ батча не чаще окна, новый батч (факт двинулся) — сразу.
    private readonly ConcurrentDictionary<string, long> _lastOk = new();
    private readonly ConcurrentDictionary<string, (string Signature, long Unix)> _lastSubmit = new();

    public async Task<Result> RunAsync(KafkaClusterSnapshot snap, CancellationToken ct)
    {
        var cluster = snap.Cluster;

        // Мутации — только держателем живого клэйма (arch/16 §5).
        if (!claims.IsMine(cluster))
            return Result.Failed(new ApplicationException(
                $"reassign {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

        // Троттл IntervalSec (как TopicSync): подряд идущие тики Reconcile
        // не дёргают кластер чаще интервала.
        var now = timeProvider.GetUtcNow().ToUnixTimeSeconds();
        if (options.IntervalSec > 0 && _lastOk.TryGetValue(cluster, out var lastOk) && now - lastOk < options.IntervalSec)
            return Result.Success();

        var ticket = await GetAsync(RebalanceKey(cluster), ct);
        if (!ticket.IsSuccess)
            return ticket.Error!;
        var hasTicket = ticket.Value is not null;

        // Кластер не поднят (endpoints/креды появляются на K5): жива заявка —
        // журнал-ожидание; иначе нечего двигать.
        if (snap.Endpoints is null || snap.AppUser is null || snap.AppPassword is null)
        {
            if (hasTicket)
                return await JournalAsync(cluster, "waiting-cluster",
                    "кластер не поднят — ребалансировка ждёт endpoints/кредов", ct);
            return Result.Success();
        }

        // D1: describe-all (включая __) — слепая проба: никаких подач,
        // прогресс-ключ НЕ трогается (spec §11.7: прошлый прогресс сохраняется).
        await using var admin = adminFactory.Create(snap.Endpoints, snap.AppUser, snap.AppPassword);
        var described = await admin.DescribeTopicsAsync(includeInternal: true, ct);
        if (!described.IsSuccess)
        {
            var blind = await journal.WriteAsync(cluster, Op, "waiting-cluster", claims.InstanceId,
                $"метаданные недоступны — слепая проба, подач нет: {described.Error!.Message}", ct);
            if (!blind.IsSuccess)
                return blind;
            _lastOk[cluster] = now;
            return Result.Success();
        }
        var all = described.Value;

        // Прогресс-ключ: total сохраняется между тиками, submitted — для дедупа.
        var progressKv = await GetAsync(ProgressKey(cluster), ct);
        if (!progressKv.IsSuccess)
            return progressKv.Error!;
        var previous = ParseProgress(progressKv.Value);

        // D2: drain-кандидаты — ТОЛЬКО по state=TO_REMOVE (без фильтра по
        // факту реплик: завершённость проверяется ниже по свежим метаданным
        // этого тика — иначе ветки done/waiting-sync недостижимы).
        var drainCandidate = snap.Brokers
            .Where(b => b.State == "TO_REMOVE")
            .OrderBy(b => b.Name, StringComparer.Ordinal)
            .FirstOrDefault();

        if (drainCandidate is not null)
        {
            // Заявка balance ждёт — сначала демонтаж (spec §5.3 B1).
            if (hasTicket)
            {
                var waiting = await journal.WriteAsync(cluster, Op, "waiting-drain", claims.InstanceId,
                    $"идёт drain {drainCandidate.Name} — заявка ребалансировки ждёт", ct);
                if (!waiting.IsSuccess)
                    return waiting;
            }

            return await RunDrainAsync(snap, all, drainCandidate.Name, previous, now, ct);
        }

        // B1: заявки нет, drain-кандидатов нет — живой прогресс-ключ = мусор
        // оборванного баланса/отмены: убираем (in-flight Kafka доиграет сам).
        if (!hasTicket)
        {
            if (progressKv.Value is not null)
            {
                var removed = await DeleteAsync(ProgressKey(cluster), prefix: false, ct);
                if (!removed.IsSuccess)
                    return Fail(cluster, removed.Error!, "deleting-progress");
            }

            _lastOk[cluster] = now;
            return await journal.WriteAsync(cluster, Op, "cancelled", claims.InstanceId,
                "заявка ребалансировки исчезла — подач больше нет, поданные батчи Kafka доиграет сама", ct);
        }

        return await RunBalanceAsync(snap, all, previous, now, ct);
    }

    // D3–D6: drain-сценарий.
    private async Task<Result> RunDrainAsync(
        KafkaClusterSnapshot snap,
        IReadOnlyList<KafkaTopicView> all,
        string drainBroker,
        ReassignProgress? previous,
        long now,
        CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var drainId = BrokerEnvBuilder.NodeId(drainBroker);

        // Цели — только RUNNING-брокеры (не drain, не демонтаж, не подъём).
        var targets = snap.Brokers
            .Where(b => b.State == "RUNNING")
            .Select(b => BrokerEnvBuilder.NodeId(b.Name))
            .OrderBy(id => id)
            .ToList();
        if (targets.Count == 0)
            return Fail(cluster,
                new ApplicationException($"reassign {cluster}: нет RUNNING-брокеров — цели переезда отсутствуют"),
                "no-targets");

        // D4: завершение — по факту метаданных: drain вне Replicas и нет USR.
        if (ReassignPlanner.DrainComplete(all, drainId))
        {
            if (!ReassignPlanner.HasUnderReplicated(all))
            {
                // Прогресс-ключ убираем; брокер остаётся TO_REMOVE — G демонтирует.
                var removed = await DeleteAsync(ProgressKey(cluster), prefix: false, ct);
                if (!removed.IsSuccess)
                    return Fail(cluster, removed.Error!, "deleting-progress");

                _lastOk[cluster] = now;
                return await journal.WriteAsync(cluster, Op, "done", claims.InstanceId,
                    $"drain {drainBroker} завершён — демонтаж продолжится процессом remove", ct);
            }

            // USR-критерий: затронутые топики ещё догоняют ISR.
            var synced = await PutProgressAsync(cluster, new ReassignProgress(
                "drain", drainBroker,
                previous?.PartitionsTotal ?? 0, 0,
                previous?.SubmittedUnix ?? 0, now, claims.InstanceId), ct);
            if (!synced.IsSuccess)
                return Fail(cluster, synced.Error!, "writing-progress");

            _lastOk[cluster] = now;
            return await journal.WriteAsync(cluster, Op, "waiting-sync", claims.InstanceId,
                $"реплик {drainBroker} больше нет — ждём догон ISR (under-replicated)", ct);
        }

        // D3: minISR-план. Юзер-топики: реестр topics/<T> min.insync.replicas
        // ?? config.min_insync; internal: min(2, targets) — формулы владения
        // воркера. Замечание: фактическое СНИЖЕНИЕ minISR internal через
        // AlterTopicConfigs не требуется — controller-ноды не демонтируются,
        // при всех достижимых B' >= 3 min(2,B') = 2 = прежнее значение; при
        // потере кворума процесс стоит по слепой пробе (спека t02 §10).
        var minIsr = BuildMinIsrByTopic(snap, all, targets);

        var plan = ReassignPlanner.PlanDrain(all, drainId, targets, minIsr);
        if (!plan.IsSuccess)
        {
            // Перманентное ожидание с человекочитаемой причиной (spec §5.2 D3).
            var blocked = await PutProgressAsync(cluster, new ReassignProgress(
                "drain", drainBroker,
                previous?.PartitionsTotal ?? 0,
                all.Count(t => t.ReplicasPerPartition.Any(p => p.Contains(drainId))),
                previous?.SubmittedUnix ?? 0, now, claims.InstanceId, plan.Error!.Message), ct);
            if (!blocked.IsSuccess)
                return Fail(cluster, blocked.Error!, "writing-progress");

            _lastOk[cluster] = now;
            return await journal.WriteAsync(cluster, Op, "waiting-minisr", claims.InstanceId,
                plan.Error.Message, ct);
        }

        return await SubmitBatchAsync(snap, all, plan.Value, "drain", drainBroker, previous, now, ct);
    }

    // B2–B3: balance-сценарий (converge к декларации, spec §3.4).
    private async Task<Result> RunBalanceAsync(
        KafkaClusterSnapshot snap,
        IReadOnlyList<KafkaTopicView> all,
        ReassignProgress? previous,
        long now,
        CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var targets = snap.Brokers
            .Where(b => b.State == "RUNNING")
            .Select(b => BrokerEnvBuilder.NodeId(b.Name))
            .OrderBy(id => id)
            .ToList();
        if (targets.Count == 0)
            return Fail(cluster,
                new ApplicationException($"reassign {cluster}: нет RUNNING-брокеров — цели ребалансировки отсутствуют"),
                "no-targets");

        var plan = ReassignPlanner.PlanBalance(all, targets, snap.Config.ReplicationFactor);
        if (ReassignPlanner.Pending(all, plan).Count == 0)
        {
            // Сходимость: сначала факт, потом del — повтор тика доиграет.
            var delTicket = await DeleteAsync(RebalanceKey(cluster), prefix: false, ct);
            if (!delTicket.IsSuccess)
                return Fail(cluster, delTicket.Error!, "deleting-ticket");
            var delProgress = await DeleteAsync(ProgressKey(cluster), prefix: false, ct);
            if (!delProgress.IsSuccess)
                return Fail(cluster, delProgress.Error!, "deleting-progress");

            _lastOk[cluster] = now;
            return await journal.WriteAsync(cluster, Op, "done", claims.InstanceId,
                "факт совпал с планом ребалансировки — заявка исполнена", ct);
        }

        return await SubmitBatchAsync(snap, all, plan, "balance", null, previous, now, ct);
    }

    // D5–D6: батч Pending-партиций + дедуп переподачи + exec + прогресс.
    private async Task<Result> SubmitBatchAsync(
        KafkaClusterSnapshot snap,
        IReadOnlyList<KafkaTopicView> all,
        IReadOnlyList<ReassignMove> plan,
        string mode,
        string? drainBroker,
        ReassignProgress? previous,
        long now,
        CancellationToken ct)
    {
        var cluster = snap.Cluster;
        var pending = ReassignPlanner.Pending(all, plan);
        var batch = pending.Take(options.BatchPartitions).ToList();

        // Дедуп (spec D5): переподача ТОГО ЖЕ батча не чаще RetrySubmitSec —
        // между ними только put прогресса (идемпотентность повторов —
        // семантика KIP-455); следующий батч (факт двинулся) — сразу.
        var signature = string.Join(";", batch.Select(m => $"{m.Topic}:{m.Partition}"));
        var submittedUnix = previous?.SubmittedUnix ?? 0;
        if (_lastSubmit.TryGetValue(cluster, out var lastSubmit)
            && lastSubmit.Signature == signature
            && now - lastSubmit.Unix < options.RetrySubmitSec)
        {
            var kept = await PutProgressAsync(cluster, new ReassignProgress(
                mode, drainBroker,
                previous?.PartitionsTotal is > 0 ? previous.PartitionsTotal : pending.Count,
                pending.Count, submittedUnix, now, claims.InstanceId), ct);
            if (!kept.IsSuccess)
                return Fail(cluster, kept.Error!, "writing-progress");

            _lastOk[cluster] = now;
            return Result.Success();
        }

        // Bootstrap: INTERNAL-имена живых брокеров (RUNNING + drain при drain:
        // его контейнер ещё жив и резолвится в kfw-net).
        var liveBrokerNames = snap.Brokers
            .Where(b => b.State == "RUNNING" || (mode == "drain" && b.Name == drainBroker))
            .OrderBy(b => b.Name, StringComparer.Ordinal)
            .Select(b => b.Name)
            .ToList();

        // Exec-цель: контейнер drain-брокера (drain) или первый RUNNING.
        var execNode = mode == "drain" && drainBroker is not null
            ? drainBroker
            : snap.Brokers.Where(b => b.State == "RUNNING")
                .OrderBy(b => b.Name, StringComparer.Ordinal)
                .Select(b => b.Name)
                .First();

        using var execCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        execCts.CancelAfter(TimeSpan.FromSeconds(options.ExecSec));
        var cmd = ReassignCli.BuildExecCommand(
            batch, ReassignCli.Bootstrap(liveBrokerNames), snap.AppUser!, snap.AppPassword!);
        var exec = await driver.ExecNodeAsync(cluster, execNode, cmd, execCts.Token);
        if (!exec.IsSuccess)
            return Fail(cluster, exec.Error!, "submitting-batch"); // следующий тик переподаст

        submittedUnix = now;
        _lastSubmit[cluster] = (signature, now);
        _lastOk[cluster] = now;

        // D6: прогресс-ключ (total от первого тика операции живёт до конца).
        var progress = new ReassignProgress(
            mode, drainBroker,
            previous?.PartitionsTotal is > 0 ? previous.PartitionsTotal : pending.Count,
            pending.Count, submittedUnix, now, claims.InstanceId);
        var written = await PutProgressAsync(cluster, progress, ct);
        if (!written.IsSuccess)
            return Fail(cluster, written.Error!, "writing-progress");

        return Result.Success();
    }

    // min.insync.replicas-guard планов: юзер — реестр ?? config; internal —
    // формула владения воркера min(2, цели).
    private static IReadOnlyDictionary<string, int> BuildMinIsrByTopic(
        KafkaClusterSnapshot snap, IReadOnlyList<KafkaTopicView> all, IReadOnlyList<int> targets)
    {
        var registry = snap.Topics.ToDictionary(
            r => r.Topic,
            r => r.Configs is not null
                && r.Configs.TryGetValue("min.insync.replicas", out var raw)
                && int.TryParse(raw, out var value)
                ? value
                : snap.Config.MinInSyncReplicas,
            StringComparer.Ordinal);

        var result = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var topic in all.Select(t => t.Topic).Distinct())
            result[topic] = topic.StartsWith("__", StringComparison.Ordinal)
                ? Math.Min(2, targets.Count)
                : registry.GetValueOrDefault(topic, snap.Config.MinInSyncReplicas);
        return result;
    }

    private static ReassignProgress? ParseProgress(Kv? kv)
    {
        if (kv is null)
            return null;
        try
        {
            return JsonSerializer.Deserialize<ReassignProgress>(kv.Value, ProgressJson);
        }
        catch (JsonException)
        {
            return null; // битый ключ — воркер просто перезапишет (spec §4)
        }
    }

    private async Task<Result> PutProgressAsync(string cluster, ReassignProgress progress, CancellationToken ct)
    {
        var value = JsonSerializer.Serialize(progress, ProgressJson);
        var put = await PutAsync(ProgressKey(cluster), value, ct);
        return put;
    }

    private async Task<Result> JournalAsync(string cluster, string phase, string? message, CancellationToken ct)
    {
        var written = await journal.WriteAsync(cluster, Op, phase, claims.InstanceId, message, ct);
        if (!written.IsSuccess)
            return written;
        return Result.Success();
    }

    private Result Fail(string cluster, Exception error, string phase)
    {
        journal.WriteAsync(cluster, Op, phase, claims.InstanceId, error.Message, CancellationToken.None)
            .GetAwaiter().GetResult();
        return Result.Failed(error);
    }

    public static string RebalanceKey(string cluster) => $"/kafkaworker/rebalances/{cluster}";

    public static string ProgressKey(string cluster) => $"/kafkaworker/reassignments/{cluster}";

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

    private async Task<Result> DeleteAsync(string keyOrPrefix, bool prefix, CancellationToken ct)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.DeleteAsync(endpoint, keyOrPrefix, prefix, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

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
}
