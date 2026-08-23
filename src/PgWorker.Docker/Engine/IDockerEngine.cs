using PgWorker.Core;

namespace PgWorker.Docker.Engine;

// Тонкий клиент Docker Engine API (Д3): только нужные endpoints поверх HttpClient.
// Идемпотентность: 404 на удаление = успех (объекта уже нет); 409 "already exists"
// на create = успех (объект уже есть).
public interface IDockerEngine : IAsyncDisposable
{
    // GET /_ping — живость docker-хоста.
    Task<Result> PingAsync(CancellationToken ct);

    // GET /containers/json?all=&filters={"name":["<prefix>"]}.
    Task<Result<IReadOnlyList<DockerContainer>>> ListContainersAsync(
        string namePrefix, bool all, CancellationToken ct);

    // POST /containers/create?name=<name> — env/порты/volume в HostConfig.
    Task<Result> CreateContainerAsync(ContainerSpec spec, string name, CancellationToken ct);

    // POST /containers/<id>/start (304 already-started = успех).
    Task<Result> StartContainerAsync(string idOrName, CancellationToken ct);

    // POST /containers/<id>/stop?t=<timeoutSec>.
    Task<Result> StopContainerAsync(string idOrName, int timeoutSec, CancellationToken ct);

    // DELETE /containers/<id>?force= (404 = успех).
    Task<Result> RemoveContainerAsync(string idOrName, bool force, CancellationToken ct);

    // DELETE /volumes/<name> (404 = успех).
    Task<Result> RemoveVolumeAsync(string name, CancellationToken ct);

    // swarm: GET /nodes (+ счётчик running-тасков по нодам).
    Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct);

    // swarm: POST /services/create (409 already exists = успех).
    Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct);

    // swarm: DELETE /services/<name> (404 = успех).
    Task<Result> RemoveServiceAsync(string name, CancellationToken ct);

    // swarm: GET /tasks?filters={"service":…} — таски сервиса с хостом ноды.
    Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct);

    // Занятые host:port publish-порты: контейнеры движка (plain) + таски на swarm-нодах.
    Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct);
}

// Контейнер из /containers/json (Names — с ведущим "/").
public sealed record DockerContainer(string Id, string[] Names, string State, string Image);

// Swarm-нода из /nodes + число работающих тасков.
public sealed record DockerSwarmNode(string Id, string Hostname, string State, int RunningTasks);

// Таск swarm-сервиса; Host — hostname ноды (NodeId → /nodes), PublishedPort — publish mode=host.
public sealed record DockerTask(string Id, string NodeId, string State, string? Host, int? PublishedPort);

// Пара портов контейнер→хост (tcp).
public sealed record PortMap(int ContainerPort, int HostPort);

// Спецификация контейнера ноды (env из NodeConfigBuilders, volume данных, publish-порты).
public sealed record ContainerSpec(
    string Image,
    IReadOnlyDictionary<string, string> Env,
    string VolumeName,
    string VolumeDest,
    IReadOnlyList<PortMap> Ports,
    string Hostname,
    double? CpuCores,
    long? MemoryBytes,
    string? Label);

// Спецификация swarm-сервиса ноды: constraint на конкретную ноду (node.id==<id>).
public sealed record ServiceSpec(string Name, ContainerSpec Template, string NodeConstraint);
