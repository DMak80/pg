namespace S3Worker.Core.Model;

// Доменная модель S3-кластера (arch/s3-clusters/02-keys.md §1): снапшоты чтения /s3/ — чистые
// неизменяемые записи; битые значения — parseError-поля без падения тика
// (arch/s3-clusters/09-panel.md), неизвестные ключи — unknownKeys (arch/s3-clusters/02-keys.md §1).

/// <summary>JSON-значение ключа config (arch/s3-clusters/02-keys.md §1): State null = Active
/// (семантика pg/kafka/valkey), NOT_INITIALIZED/TO_REMOVE — заявки;
/// EtcdBackup — признак «бэкапный кластер etcd» (arch/s3-clusters/02-keys.md §1: bool, default
/// false, immutable, единственный в установке).</summary>
public sealed record S3ClusterConfig(
    string? Platform,
    int? Nodes,
    bool? EtcdBackup,
    long? CreatedUnix,
    string? State);

/// <summary>JSON-селектор атрибутов хоста (arch/s3-clusters/04-placement.md): docker_host (plain) |
/// swarm_node | swarm_labels | комбинация — привязка = конъюнкция равенств.</summary>
public sealed record S3NodePlacement(
    string? DockerHost,
    string? SwarmNode,
    IReadOnlyDictionary<string, string>? SwarmLabels);

/// <summary>JSON-значение ключа resources (arch/s3-clusters/02-keys.md §1): cpu — десятичные ядра
/// ("2"), mem/disk — целые GiB ("4Gi"); парсинг в лимиты — S3ProcessCommon.</summary>
public sealed record S3NodeResources(string? Cpu, string? Mem, string? Disk);

/// <summary>JSON-значение ключа usage (arch/s3-clusters/02-keys.md §2): snake_case etcd-канон —
/// маппинг из camelCase-грани GET /admin/storage делает надзор (C).</summary>
public sealed record S3NodeUsage(long UsedBytes, long TotalBytes, long UpdatedUnix);

/// <summary>Пара графа репликации (arch/s3-clusters/02-keys.md §1 links): связь {A,B} — неориентированная
/// (зеркальный дубль ≡ одна связь; реализуется join-механикой ownS3 двусторонне).</summary>
public sealed record S3ReplicationLink(string A, string B);

/// <summary>Снапшот ноды: декларации + статусы; ParseError — битый
/// JSON одного из ключей ноды (кластер жив).</summary>
public sealed record S3NodeSnapshot(
    string Node,
    string? State,
    string? PlacedOn,
    S3NodePlacement? Placement,
    S3NodeResources? Resources,
    S3NodeUsage? Usage,
    string? ParseError);

/// <summary>Снапшот кластера /s3/clusters/&lt;C&gt;/; ParseError — битый config,
/// links или denylist. Links/DenyList null = ключа нет ИЛИ битое значение
/// (битость — в ParseError); pki-материал в снапшот НЕ входит (spec §5.1:
/// чтение отдельным стором при выпуске env, не в снапшот тика).</summary>
public sealed record S3ClusterSnapshot(
    string Cluster,
    S3ClusterConfig? Config,
    IReadOnlyList<S3NodeSnapshot> Nodes,
    string? Endpoints,
    IReadOnlyList<S3ReplicationLink>? Links = null,
    IReadOnlyList<string>? DenyList = null,
    string? ParseError = null);

/// <summary>Итог разбора префикса /s3/: кластеры + неизвестные ключи
/// (счётчик unknownKeys — метрика spec §9).</summary>
public sealed record S3DomainSnapshot(
    IReadOnlyList<S3ClusterSnapshot> Clusters,
    IReadOnlyList<string> UnknownKeys);

/// <summary>Статусы нод (arch/s3-clusters/02-keys.md §1 / arch/s3worker/README.md): NOT_INITIALIZED — панель;
/// PROVISIONING/RUNNING/UNREACHABLE/REMOVING — воркер; TO_REMOVE — панель
/// (one-way маркер демонтажа).</summary>
public static class S3NodeStates
{
    public const string NotInitialized = "NOT_INITIALIZED";
    public const string Provisioning = "PROVISIONING";
    public const string Running = "RUNNING";
    public const string Unreachable = "UNREACHABLE";
    public const string Removing = "REMOVING";
    public const string ToRemove = "TO_REMOVE";
}
