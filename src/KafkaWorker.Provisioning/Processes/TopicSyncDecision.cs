using KafkaWorker.Core.Model;

namespace KafkaWorker.Provisioning.Processes;

// Факт топика из Kafka (describe-шаг автосинка, arch/16 §5 D): партиции,
// RF и фактические значения управляемых конфигов (строковые, как отдаёт Kafka).
public sealed record TopicFact(
    string Topic,
    int Partitions,
    int? ReplicationFactor,
    IReadOnlyDictionary<string, string>? Configs);

/// <summary>
/// Действие автосинка одного топика (чистый decide-выход, arch/15 §3).
/// Один топик — ровно одно действие; акт исполняет его поверх свежего
/// значения ключа (RMW по mod_revision).
/// </summary>
public abstract record TopicSyncAction
{
    /// <summary>
    /// Записать факт в ключ (новый топик / дрейф факта / появление после
    /// missing / снятие уже исполненной заявки). Desired != null — сохранить
    /// живую заявку (свежие поля возьмёт акт из etcd); null — снять.
    /// </summary>
    public sealed record Sync(
        string Topic,
        int Partitions,
        int? ReplicationFactor,
        IReadOnlyDictionary<string, string>? Configs,
        TopicDesired? Desired,
        long? DesiredUnix,
        string? DesiredBy) : TopicSyncAction;

    /// <summary>Удалить ключ: топик исчез из Kafka, заявки нет (реестр = факт).</summary>
    public sealed record Forget(string Topic) : TopicSyncAction;

    /// <summary>Топик исчез при живой заявке: missing=true, ключ не удаляется.</summary>
    public sealed record MarkMissing(string Topic) : TopicSyncAction;

    /// <summary>
    /// Применить заявку к Kafka: сначала конфиги (IncrementalAlterConfigs),
    /// затем partitions (CreatePartitions, только увеличение); после — снять
    /// desired тем же RMW (акт).
    /// </summary>
    public sealed record Apply(
        string Topic,
        IReadOnlyDictionary<string, string> Configs,
        int? TotalPartitions) : TopicSyncAction;

    /// <summary>
    /// Перманентный отказ (уменьшение partitions — Kafka не умеет; панель
    /// отсекает раньше, это обход etcd-мусора): журнал + снятие заявки.
    /// </summary>
    public sealed record Reject(string Topic, string Reason) : TopicSyncAction;

    /// <summary>No-op: факт и реестр согласованы.</summary>
    public sealed record Skip(string Topic) : TopicSyncAction;
}

/// <summary>
/// Чистые decision-функции автосинка топиков (arch/15 §3, arch/16 §5 D;
/// describe→decide→act из Puzzle §7.2): сопоставление факта Kafka с реестром
/// etcd → план действий. Без побочных эффектов — таблица юнит-тестов покрывает
/// все ветки протокола.
/// </summary>
public static class TopicSyncDecision
{
    /// <summary>
    /// Управляемые конфиги топика (spec §3.2): только эти ключи воркер
    /// читает в факт и применяет из desired; прочие ключи заявок — не ours.
    /// </summary>
    public static readonly IReadOnlySet<string> ManagedTopicConfigs = new HashSet<string>(
    [
        "retention.ms",
        "min.insync.replicas",
    ], StringComparer.Ordinal);

    public static IReadOnlyList<TopicSyncAction> Decide(
        IReadOnlyList<TopicFact> facts,
        IReadOnlyList<KafkaTopicReg> registry)
    {
        var actions = new List<TopicSyncAction>();
        var byTopic = registry
            .GroupBy(r => r.Topic, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.First(), StringComparer.Ordinal);

        foreach (var fact in facts)
        {
            // Internal-топики Kafka (__*) в реестр не попадают (arch/15 §3).
            if (IsInternal(fact.Topic))
            {
                actions.Add(new TopicSyncAction.Skip(fact.Topic));
                continue;
            }

            if (!byTopic.Remove(fact.Topic, out var reg))
            {
                // Новый топик (создан CLI/клиентом) → ключ с фактом, заявки нет.
                actions.Add(FactSync(fact, desired: null, desiredUnix: null, desiredBy: null));
                continue;
            }

            // Пропавший топик появился снова: missing=false, заявка жива и
            // применится штатно (следующий decide по свежему факту).
            if (reg.Missing)
            {
                actions.Add(FactSync(fact, reg.Desired, reg.DesiredUnix, reg.DesiredBy));
                continue;
            }

            if (reg.Desired is null)
            {
                // Автосинк факта: ключ обновляется только при дрейфе (no-op
                // на стабильном кластере — synced_unix не тикает впустую).
                if (Drifted(reg, fact))
                    actions.Add(FactSync(fact, desired: null, desiredUnix: null, desiredBy: null));
                else
                    actions.Add(new TopicSyncAction.Skip(fact.Topic));

                continue;
            }

            // Перманентный отказ: уменьшение партиций Kafka не поддерживает
            // (spec §4.2 D); панель отсекает на постановке — это etcd-мусор.
            if (reg.Desired.Partitions is int down && down < fact.Partitions)
            {
                actions.Add(new TopicSyncAction.Reject(
                    fact.Topic,
                    $"partitions {down} < факт {fact.Partitions}: уменьшение партиций Kafka не поддерживает"));
                continue;
            }

            // Diff заявки с фактом по управляемым полям: конфиги + partitions↑.
            var configs = ConfigDiff(reg.Desired.Configs, fact.Configs);
            var partitions = reg.Desired.Partitions is int up && up > fact.Partitions ? up : (int?)null;
            if (configs.Count > 0 || partitions is not null)
            {
                actions.Add(new TopicSyncAction.Apply(fact.Topic, configs, partitions));
                continue;
            }

            // Заявка уже исполнена (равенство факту) → снять без применения
            // (идемпотентность: apply упал после Kafka-мутации, до записи).
            actions.Add(FactSync(fact, desired: null, desiredUnix: null, desiredBy: null));
        }

        // Топики реестра, которых нет в факте (удалены на стороне Kafka).
        foreach (var reg in byTopic.Values)
        {
            if (IsInternal(reg.Topic))
            {
                actions.Add(new TopicSyncAction.Skip(reg.Topic));
                continue;
            }

            if (reg.Desired is null)
                actions.Add(new TopicSyncAction.Forget(reg.Topic)); // реестр = факт
            else if (!reg.Missing)
                actions.Add(new TopicSyncAction.MarkMissing(reg.Topic)); // заявка не исполнима
            else
                actions.Add(new TopicSyncAction.Skip(reg.Topic)); // уже помечен
        }

        return actions;

        static bool IsInternal(string topic)
            => topic.StartsWith("__", StringComparison.Ordinal);
    }

    // Дрейф факта против записи реестра: partitions/RF/управляемые конфиги.
    private static bool Drifted(KafkaTopicReg reg, TopicFact fact)
        => reg.Partitions != fact.Partitions
            || reg.ReplicationFactor != fact.ReplicationFactor
            || !ManagedConfigsEqual(reg.Configs, fact.Configs);

    // Diff desired-конфигов с фактом: только управляемые ключи, только отличия.
    private static IReadOnlyDictionary<string, string> ConfigDiff(
        IReadOnlyDictionary<string, string>? desired,
        IReadOnlyDictionary<string, string>? fact)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        if (desired is null)
            return result;

        foreach (var (name, value) in desired)
        {
            if (!ManagedTopicConfigs.Contains(name))
                continue; // неуправляемый ключ заявки — не применяем (панель валидирует)

            var actual = fact is not null && fact.TryGetValue(name, out var current) ? current : null;
            if (actual != value)
                result[name] = value;
        }

        return result;
    }

    // Сравнение управляемых конфигов реестра и факта (только управляемые ключи).
    private static bool ManagedConfigsEqual(
        IReadOnlyDictionary<string, string>? left,
        IReadOnlyDictionary<string, string>? right)
    {
        var leftKeys = KeysOf(left);
        var rightKeys = KeysOf(right);
        if (leftKeys.Count != rightKeys.Count || !leftKeys.SetEquals(rightKeys))
            return false;

        foreach (var name in leftKeys)
            if (left![name] != right![name])
                return false;

        return true;

        HashSet<string> KeysOf(IReadOnlyDictionary<string, string>? configs)
            => [.. (configs ?? new Dictionary<string, string>()).Keys.Where(ManagedTopicConfigs.Contains)];
    }

    private static TopicSyncAction.Sync FactSync(
        TopicFact fact, TopicDesired? desired, long? desiredUnix, string? desiredBy)
        => new(
            fact.Topic,
            fact.Partitions,
            fact.ReplicationFactor,
            fact.Configs is null ? null : new Dictionary<string, string>(fact.Configs),
            desired,
            desiredUnix,
            desiredBy);
}
