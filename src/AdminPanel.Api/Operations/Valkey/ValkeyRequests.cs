namespace AdminPanel.Api.Operations.Valkey;

// Тела запросов — зеркало DTO ValkeyWorker (arch/21 §1.1; панель НЕ валидирует —
// сервер источник истины, фронт дублирует для UX; spec §4.9).
public sealed record CreateValkeyClusterRequest(
    string? Name,
    int? Nodes = null,
    long? MaxmemoryBytes = null,
    string? MaxmemoryPolicy = null,
    ValkeyResourcesUpdateRequest? Resources = null);

public sealed record ValkeyResourcesUpdateRequest(
    decimal? Cpu = null,
    int? MemGi = null,
    int? DiskGi = null);

public sealed record ValkeyConfigUpdateRequest(
    long? MaxmemoryBytes = null,
    string? MaxmemoryPolicy = null);

public sealed record RotateValkeyPasswordRequest(string? Role);
