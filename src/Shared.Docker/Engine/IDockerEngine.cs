using Shared.Core;

namespace Shared.Docker;

// Тонкий клиент Docker Engine API: только нужные endpoints поверх HttpClient.
// Общий движок трёх воркеров (t07): union методов pg/kfw/vwk, канон-суперсет
// семантики. Идемпотентность: 404 на удаление = успех (объекта уже нет);
// 409 "already exists" на create = успех; 304 start = успех; create при
// 404 "No such image" — pull образа и повтор.
public interface IDockerEngine : IAsyncDisposable
{
    Task<Result> PingAsync(CancellationToken ct);
    Task<Result<IReadOnlyList<DockerContainer>>> ListContainersAsync(string namePrefix, bool all, CancellationToken ct);
    Task<Result<DockerContainerInspect>> InspectContainerAsync(string id, CancellationToken ct);
    Task<Result<string>> GetContainerLogsAsync(string idOrName, int tail, CancellationToken ct);
    Task<Result> CreateContainerAsync(ContainerSpec spec, string name, CancellationToken ct);
    Task<Result> StartContainerAsync(string idOrName, CancellationToken ct);
    Task<Result> StopContainerAsync(string idOrName, int timeoutSec, CancellationToken ct);
    Task<Result> RemoveContainerAsync(string idOrName, bool force, CancellationToken ct);
    Task<Result<string>> ExecAsync(string containerId, IReadOnlyList<string> cmd, CancellationToken ct);
    Task<Result> EnsureNetworkAsync(string name, CancellationToken ct);
    Task<Result> DeleteNetworkAsync(string name, CancellationToken ct);
    Task<Result> RemoveVolumeAsync(string name, CancellationToken ct);
    Task<Result<bool>> VolumeExistsAsync(string name, CancellationToken ct);
    Task<Result> EnsureVolumeAsync(string name, CancellationToken ct);
    Task<Result> DeleteVolumeAsync(string name, CancellationToken ct);
    Task<Result> PutVolumeArchiveAsync(string name, byte[] tar, string image, CancellationToken ct);
    Task<Result<byte[]?>> GetVolumeArchiveAsync(string name, string image, CancellationToken ct);
    Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct);
    Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct);
    Task<Result> RemoveServiceAsync(string name, CancellationToken ct);
    Task<Result<IReadOnlyList<string>>> ListServicesAsync(string namePrefix, CancellationToken ct);
    Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct);
    Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct);
    Task<Result<NodeLimits?>> InspectContainerResourcesAsync(string name, CancellationToken ct);
    Task<Result<NodeLimits?>> InspectServiceResourcesAsync(string name, CancellationToken ct);
    Task<Result<IReadOnlyDictionary<string, string>?>> InspectContainerEnvAsync(string idOrName, CancellationToken ct);
    Task<Result<IReadOnlyDictionary<string, string>?>> InspectServiceEnvAsync(string name, CancellationToken ct);
    Task<Result<IReadOnlyList<string>?>> InspectContainerCmdAsync(string idOrName, CancellationToken ct);
    Task<Result<IReadOnlyList<string>?>> InspectServiceCmdAsync(string name, CancellationToken ct);
    // containerPort — контейнерный порт клиентского listener'а (kfw: 9094, vwk: 6379).
    Task<Result<DockerNodeEndpoint?>> InspectNodeEndpointAsync(string name, int containerPort, CancellationToken ct);
}

// Контейнер из /containers/json (Names — с ведущим "/").
public sealed record DockerContainer(string Id, string[] Names, string State, string Image);

// Инспект контейнера GET /containers/<id>/json: hostname, сетевые алиасы, env
// и host-биндинги — вход матчинга усыновления (spec §3.1); Running/ExitCode —
// runtime-факт джоба бэкапа (t02: exit-код — истина итога, arch/19 §2).
public sealed record DockerContainerInspect(
    string Id, string Hostname, string[] Aliases, string[] Env, PortMap[] Ports,
    bool? Running = null, int? ExitCode = null,
    // t07 (arch/19 §6): docker-факт возраста running-джоба (StartedAt, RFC3339 →
    // unix) — бюджет verify-джоба; null при отсутствии/битой строке инспекта.
    long? StartedAtUnix = null);

// Swarm-нода из /nodes + число работающих тасков.
public sealed record DockerSwarmNode(string Id, string Hostname, string State, int RunningTasks);

// Таск swarm-сервиса; Host — hostname ноды (NodeId → /nodes), PublishedPort —
// publish mode=host, ContainerId — контейнер running-таска (t01: exec).
// union: vwk-копия без ContainerId — объединена с pg/kfw-вариантом (§7.6).
public sealed record DockerTask(string Id, string NodeId, string State, string? Host, int? PublishedPort,
    string? ContainerId = null);

// Пара портов контейнер→хост (tcp).
public sealed record PortMap(int ContainerPort, int HostPort);

// docker-факт лимитов (0 = без лимита); доменные конверсии — у потребителей (§4.6.2).
public sealed record NodeLimits(long NanoCpus, long MemoryBytes);

// Факт endpoint'а из docker inspect: published host-порт клиентского listener'а
// контейнера + State.Running (PortBindings персистят и у остановленного
// контейнера — Running отличает «жив» от «есть, но остановлен»). TaskHost —
// хост running-таска (swarm-фолбэк: порт и хост — из ОДНОГО вызова ListTasks
// движка); null в plain-ветке (host даёт перебор движков).
// union kfw/vwk; advertised-пара ушла в kfw-драйвер (§4.6.1).
public sealed record DockerNodeEndpoint(int ClientHostPort, bool Running, string? TaskHost = null);

// Спецификация swarm-сервиса ноды: constraint на конкретную ноду (node.id==<id>).
public sealed record ServiceSpec(string Name, ContainerSpec Template, string NodeConstraint);

// HTTP-ошибка Engine API: не-2xx (кроме идемпотентных 404/409).
public sealed class DockerHttpException(string method, string path, int statusCode, string body)
    : Exception($"docker {method} {path} ответил {statusCode}: {body}")
{
    public int StatusCode { get; } = statusCode;

    public string Body { get; } = body;
}

// Супер-спека трёх доменов (t07): домены передают своё, отсутствующее — null.
// ResetEntrypoint=true (pg-семантика): при заданном Cmd сбросить ENTRYPOINT
// образа (Entrypoint=[]) — Cmd выполняется как есть, а не аргументами
// образного entrypoint; false — Cmd как аргументы entrypoint (kfw/vwk).
public sealed record ContainerSpec(
    string Image,
    IReadOnlyList<PortMap> Ports,
    string Hostname,
    IReadOnlyDictionary<string, string>? Env = null,
    IReadOnlyList<string>? Cmd = null,
    bool ResetEntrypoint = false,
    string? VolumeName = null, string? VolumeDest = null,
    IReadOnlyList<string>? Binds = null,
    string? Network = null, IReadOnlyList<string>? NetworkAliases = null,
    IReadOnlyDictionary<string, string>? Tmpfs = null,
    IReadOnlyList<string>? ExtraHosts = null,
    double? CpuCores = null, long? MemoryBytes = null,
    string? RestartPolicy = null,
    string? Label = null, string? LabelKey = null);
