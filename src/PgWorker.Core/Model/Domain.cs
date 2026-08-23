namespace PgWorker.Core.Model;

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
