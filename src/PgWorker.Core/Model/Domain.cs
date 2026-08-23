namespace PgWorker.Core.Model;

using System.Globalization;

// Доменная модель PgWorker (spec §4, §6.4): состояния и структуры, которые
// читаются из etcd / строятся планировщиками. Идентификаторы — английские.

/// <summary>config.state кластера: отсутствует = Active (контракт панели 02 §2.1).</summary>
public enum ClusterState
{
    Active,
    NotInitialized,
    ToRemove,
}

/// <summary>Значения /clusters/&lt;C&gt;/shards/&lt;X&gt;/nodes/&lt;n&gt;/state (arch/14 §5).</summary>
public enum NodeState
{
    NotInitialized,
    Provisioning,
    Running,
    Rebuilding,
    Unreachable,
    Quarantined,
    Removing,
}

/// <summary>Статус-ключ бакета; null (нет ключа) = ACTIVE (arch/11 §2).</summary>
public enum BucketMoveState
{
    NotInitialized,
    Syncing,
    Frozen,
    Aborting,
}

/// <summary>/clusters/&lt;C&gt;/config: константы создания + state.</summary>
public sealed record ClusterConfig(string Cluster, int Buckets, string DbName,
    long? CreatedUnix, ClusterState State);

/// <summary>Плановая нода шарда: имя = имя шарда + буква ("shard1", "shard1a").</summary>
public sealed record NodeSpec(string Shard, string Name, NodeState State);

/// <summary>Шард кластера: replicas — плановое число нод, Dsn/Master — runtime.</summary>
public sealed record ShardSpec(string Name, int Replicas, string? Dsn, string? Master,
    IReadOnlyList<NodeSpec> Nodes);

/// <summary>Маршрут бакета: владелец (шард) + статус переезда (null → ACTIVE).</summary>
public sealed record BucketRoute(int Id, string? Owner, BucketMoveState? Status);

/// <summary>Полный снапшот кластера: config + шарды + все N маршрутов бакетов.</summary>
public sealed record ClusterSnapshot(ClusterConfig Config, IReadOnlyList<ShardSpec> Shards,
    IReadOnlyList<BucketRoute> Routing);

/// <summary>Тройка портов ноды, выделенная аллокатором (pg/patroni/doorman).</summary>
public sealed record NodePorts(int Pg, int Patroni, int Doorman);

/// <summary>Адрес ноды: docker-хост + выделенные host-порты.</summary>
public sealed record NodeAddress(string Host, NodePorts Ports);

/// <summary>Адреса etcd (http://host:2379) — для lease-скрипта мастер-ключа ноды.</summary>
public sealed record EtcdEndpoints(IReadOnlyList<string> Http);

/// <summary>
/// Заявка ресурсов ноды из /service/&lt;scope&gt;/request_{cpu,mem} (rework №5):
/// становится лимитами контейнера (plain) / таска сервиса (swarm).
/// null = ключа нет/нечитаем — без лимита. request_disk аналога в docker нет —
/// лимита диска у контейнера с volume не существует (осознанный игнор).
/// </summary>
public sealed record NodeResources(double? CpuCores, long? MemoryBytes);

/// <summary>
/// Парсер заявок ресурсов панели: request_cpu — инвариант-десятичное число
/// ядер («2», «0.5»); request_mem — байты с суффиксами (без суффикса/B — байты;
/// K/M/G/T — десятичные 10^3..; Ki/Mi/Gi/Ti — двоичные 2^10.., как у панели
/// «8Gi»). Нечитаемое значение → null: заявка — не контракт, кластер обязан
/// подняться и без лимита.
/// </summary>
public static class NodeResourcesParser
{
    // Оба значения нечитаемы/отсутствуют → null (заявки нет — без лимита).
    public static NodeResources? Parse(string? requestCpu, string? requestMem)
    {
        var cpu = ParseCpu(requestCpu);
        var mem = ParseMem(requestMem);
        return cpu is null && mem is null ? null : new NodeResources(cpu, mem);
    }

    public static double? ParseCpu(string? raw)
        => double.TryParse(raw?.Trim().Replace(',', '.'), NumberStyles.Float,
               CultureInfo.InvariantCulture, out var cores) && cores > 0
            ? cores
            : null;

    public static long? ParseMem(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        var s = raw.Trim();
        var end = 0;
        while (end < s.Length && (char.IsAsciiDigit(s[end]) || s[end] is '.' or ','))
            end++;
        var number = s[..end].Replace(',', '.'); // инвариант-десятичное
        var suffix = s[end..].Trim();
        if (suffix.EndsWith("B", StringComparison.Ordinal))
            suffix = suffix[..^1]; // «GB»/«GiB» → «G»/«Gi»
        if (number.Length == 0
            || !double.TryParse(number, NumberStyles.Float, CultureInfo.InvariantCulture, out var value)
            || value <= 0)
            return null;

        var multiplier = suffix switch
        {
            "" => 1L,
            "K" => 1_000L, "Ki" => 1L << 10,
            "M" => 1_000_000L, "Mi" => 1L << 20,
            "G" => 1_000_000_000L, "Gi" => 1L << 30,
            "T" => 1_000_000_000_000L, "Ti" => 1L << 40,
            _ => -1L, // неизвестный суффикс — толерантно без лимита
        };
        return multiplier > 0 ? (long)(value * multiplier) : null;
    }
}
