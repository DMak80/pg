namespace ValkeyWorker.Core.Model;

// Домен-модель valkey-кластера (arch/20–21).

/// <summary>Лимиты контейнера ноды (inspect: NanoCpus/Memory; сверка автоконверге arch/21 §5 C).</summary>
public sealed record NodeLimits(decimal? CpuCores, long? MemoryBytes);

// raw-строки заявки как в etcd: cpu — "2"/"0.5", mem/disk — "1Gi"/"10Gi"
// (канон pg §9.3: только суффикс Gi). Парсер хранит raw (толерантность
// arch/20 §5); парсинг в лимиты — ProcessCommon (задача 7).
public sealed record ValkeyResources(string? Cpu, string? Mem, string? Disk);

/// <summary>
/// Декларация кластера (ключ config): nodes фиксируется при создании (=1 в v1),
/// maxmemory_* — mutable-конфиги (converge D), state — только у невыполненных
/// заявок (отсутствие = Active, arch/20 §2).
/// </summary>
public sealed record ValkeyClusterConfig(
    int Nodes,
    long MaxmemoryBytes,
    string MaxmemoryPolicy,
    long CreatedUnix,
    string? State);

/// <summary>Срез ноды кластера (nodes/node&lt;k&gt;/*).</summary>
public sealed record ValkeyNodeSnapshot(string Node, string? State, ValkeyResources? Resources);

/// <summary>
/// Снапшот кластера из etcd (вход процессов A–E). Config == null — config-ключа
/// нет/битый (→ ParseErrors). Неполный набор кредов → null-поля независимо
/// друг от друга (толерантность arch/20 §5).
/// </summary>
public sealed record ValkeyClusterSnapshot(
    string Cluster,
    ValkeyClusterConfig? Config,
    IReadOnlyDictionary<string, ValkeyNodeSnapshot> Nodes,
    string? Endpoints,
    string? AppUser,
    string? AppPassword,
    string? AdminUser,
    string? AdminPassword,
    IReadOnlyList<string> UnknownKeys,
    IReadOnlyList<string> ParseErrors);

/// <summary>Итог парсинга префикса /valkey/clusters/ (вход ReconcileLoop).</summary>
public sealed record ParsedValkeySnapshot(
    IReadOnlyList<ValkeyClusterSnapshot> Clusters,
    IReadOnlyList<string> UnknownKeys,
    IReadOnlyList<string> ParseErrors);
