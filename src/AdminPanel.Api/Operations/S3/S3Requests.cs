namespace AdminPanel.Api.Operations.S3;

// Тела запросов — зеркало DTO S3Worker (arch/24 §1.1; панель НЕ валидирует —
// сервер источник истины, фронт дублирует для UX). platform="owns3" панель
// подставляет всегда (единственное значение канона arch/23 §2; UI поле не показывает).
public sealed record CreateS3ClusterPanelRequest(
    string? Name,
    string? SecretProfile,
    IReadOnlyList<S3InstancePanelRequest>? Instances,
    S3ResourcesPanelRequest? Resources,
    string? Platform = "owns3");

public sealed record S3InstancePanelRequest(
    string? Name,
    S3PlacementPanelRequest? Placement,
    S3ResourcesPanelRequest? Resources);

public sealed record S3PlacementPanelRequest(
    string? DockerHost,
    string? SwarmNode,
    IReadOnlyDictionary<string, string>? SwarmLabels);

public sealed record S3ResourcesPanelRequest(decimal? Cpu, int? MemGi, int? DiskGi);

public sealed record AddS3NodePanelRequest(
    string? Name,
    S3PlacementPanelRequest? Placement,
    S3ResourcesPanelRequest? Resources);

// PUT resources: null-поля = не менять (сервер RMW).
public sealed record UpdateS3ResourcesPanelRequest(decimal? Cpu, int? MemGi, int? DiskGi);

public sealed record PutS3LinksPanelRequest(IReadOnlyList<S3LinkPanelDto>? Links);

public sealed record S3LinkPanelDto(string? A, string? B);
