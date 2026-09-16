namespace ValkeyWorker.Core.Model;

// Домен-модель valkey-кластера (arch/20–21). В каркасе (t02 фаза 2) —
// лимиты контейнера ноды; дополняется снапшот-моделью парсера (фаза 3).

/// <summary>Лимиты контейнера ноды (inspect: NanoCpus/Memory; сверка автоконверге arch/21 §5 C).</summary>
public sealed record NodeLimits(decimal? CpuCores, long? MemoryBytes);
