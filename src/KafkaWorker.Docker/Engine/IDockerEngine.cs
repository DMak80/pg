using KafkaWorker.Core;
using KafkaWorker.Core.Planning;

namespace KafkaWorker.Docker.Engine;

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

    // GET /volumes/<name> — существует ли volume (404 = нет; надзор arch/16 §5 C:
    // чистый том только при доказанной физической утрате, потери данных недопустимы).
    Task<Result<bool>> VolumeExistsAsync(string name, CancellationToken ct);

    // POST /containers/<id>/exec + /exec/<id>/start + /exec/<id>/json —
    // выполнить команду в контейнере, вернуть demultiplexed stdout;
    // exit != 0 → Failed со stderr в сообщении (t01: pg_dump-транспорт).
    Task<Result<string>> ExecAsync(string containerId, IReadOnlyList<string> cmd, CancellationToken ct);

    // POST /networks/create (409 already exists = успех) — сеть нод кластера.
    Task<Result> EnsureNetworkAsync(string name, CancellationToken ct);

    // DELETE /networks/<name> (404 = успех) — демонтаж сети кластера; пул
    // subnet'ов docker-хоста конечен — per-cluster сети не копим (t09-фикс).
    Task<Result> DeleteNetworkAsync(string name, CancellationToken ct);

    // swarm: GET /nodes (+ счётчик running-тасков по нодам).
    Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct);

    // swarm: POST /services/create (409 already exists = успех).
    Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct);

    // swarm: DELETE /services/<name> (404 = успех).
    Task<Result> RemoveServiceAsync(string name, CancellationToken ct);

    // swarm: GET /services?filters={"name":…} — имена сервисов по префиксу
    // (объекты нод кластера в swarm — сервисы; rework №4).
    Task<Result<IReadOnlyList<string>>> ListServicesAsync(string namePrefix, CancellationToken ct);

    // swarm: GET /tasks?filters={"service":…} — таски сервиса с хостом ноды.
    Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct);

    // Занятые host:port publish-порты: контейнеры движка (plain) + таски на swarm-нодах.
    Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct);

    // Лимиты контейнера (HostConfig.NanoCPUs/Memory; 0 = без лимита); 404 → null.
    Task<Result<NodeLimits?>> InspectContainerResourcesAsync(string name, CancellationToken ct);

    // Лимиты swarm-сервиса (TaskTemplate.Resources.Limits); 404 → null.
    Task<Result<NodeLimits?>> InspectServiceResourcesAsync(string name, CancellationToken ct);

    // Инспекция endpoint'а контейнера (t05 E9): published host-порт CLIENT (9094)
    // + клиентская пара из env KAFKA_ADVERTISED_LISTENERS; null = объекта нет.
    Task<Result<DockerNodeEndpoint?>> InspectNodeEndpointAsync(string name, CancellationToken ct);
}

// Факт endpoint'а из docker inspect (t05 E9): published-порт контейнера на хосте
// и клиентская пара из env (контрольная сверка; null — источник недоступен, swarm).
public sealed record DockerNodeEndpoint(int ClientHostPort, string? AdvertisedClient);

// Контейнер из /containers/json (Names — с ведущим "/").
public sealed record DockerContainer(string Id, string[] Names, string State, string Image);

// Swarm-нода из /nodes + число работающих тасков.
public sealed record DockerSwarmNode(string Id, string Hostname, string State, int RunningTasks);

// Таск swarm-сервиса; Host — hostname ноды (NodeId → /nodes), PublishedPort —
// publish mode=host, ContainerId — контейнер running-таска (t01: exec).
public sealed record DockerTask(string Id, string NodeId, string State, string? Host, int? PublishedPort,
    string? ContainerId = null);

// Пара портов контейнер→хост (tcp).
public sealed record PortMap(int ContainerPort, int HostPort);

// Спецификация контейнера ноды (env из NodeConfigBuilders, volume данных, publish-порты).
// Cmd — опциональная команда (не задаётся драйвером: у образа pgworker-node свой
// entrypoint; используется интеграционными тестами для alpine-контейнеров).
// Network/NetworkAliases — общая docker-сеть нод кластера (внутренние адреса
// Patroni-репликации; alias = имя ноды): вне user-defined сети контейнеры друг
// друга по hostname не резолвят.
public sealed record ContainerSpec(
    string Image,
    IReadOnlyDictionary<string, string> Env,
    string VolumeName,
    string VolumeDest,
    IReadOnlyList<PortMap> Ports,
    string Hostname,
    double? CpuCores,
    long? MemoryBytes,
    string? Label,
    IReadOnlyList<string>? Cmd = null,
    string? Network = null,
    IReadOnlyList<string>? NetworkAliases = null);

// Спецификация swarm-сервиса ноды: constraint на конкретную ноду (node.id==<id>).
public sealed record ServiceSpec(string Name, ContainerSpec Template, string NodeConstraint);
