using Shared.Core.Planning;
using Shared.Core;
using ValkeyWorker.Core.Model;

namespace ValkeyWorker.Docker.Engine;

// Тонкий клиент Docker Engine API (Д3; копия kfw-движка с упрощениями домена —
// arch/21 §2: без volume, без exec (RESP вместо exec), без env-инспекций
// (конфигурация ноды — args флаги valkey-server, сверка V3 — по Cmd)).
// Идемпотентность: 404 на удаление = успех (объекта уже нет); 409 "already exists"
// на create = успех (объект уже есть).
public interface IDockerEngine : IAsyncDisposable
{
    // GET /_ping — живость docker-хоста.
    Task<Result> PingAsync(CancellationToken ct);

    // GET /containers/json?all=&filters={"name":["<prefix>"]}.
    Task<Result<IReadOnlyList<DockerContainer>>> ListContainersAsync(
        string namePrefix, bool all, CancellationToken ct);

    // POST /containers/create?name=<name> — порты/лимиты в HostConfig.
    Task<Result> CreateContainerAsync(ContainerSpec spec, string name, CancellationToken ct);

    // POST /containers/<id>/start (304 already-started = успех).
    Task<Result> StartContainerAsync(string idOrName, CancellationToken ct);

    // POST /containers/<id>/stop?t=<timeoutSec>.
    Task<Result> StopContainerAsync(string idOrName, int timeoutSec, CancellationToken ct);

    // DELETE /containers/<id>?force= (404 = успех).
    Task<Result> RemoveContainerAsync(string idOrName, bool force, CancellationToken ct);

    // POST /networks/create (409 already exists = успех) — движок общий;
    // valkey-домен per-cluster сети не создаёт (arch/21 §2).
    Task<Result> EnsureNetworkAsync(string name, CancellationToken ct);

    // DELETE /networks/<name> (404 = успех).
    Task<Result> DeleteNetworkAsync(string name, CancellationToken ct);

    // POST /volumes/create (409 already exists = успех) — named volume TLS-секретов.
    Task<Result> EnsureVolumeAsync(string name, CancellationToken ct);

    // Запись tar в named volume (серты до старта контейнера). Транспорт —
    // helper-контейнер (image) с volume в /mnt + exec-запись файлов: сам
    // volume-archive API (PUT /volumes/{name}/archive) на демон без swarm
    // отвечает на локальные тома 503 «only valid for cluster volumes».
    Task<Result> PutVolumeArchiveAsync(string name, byte[] tar, string image, CancellationToken ct);

    // Чтение tar из named volume (GET /containers/<helper>/archive сквозь
    // mount); null = volume нет (слёт тома — положительное свидетельство
    // отсутствия, перевыпуск).
    Task<Result<byte[]?>> GetVolumeArchiveAsync(string name, string image, CancellationToken ct);

    // DELETE /volumes/{name} (404 = успех — идемпотентность демонтажа X1).
    Task<Result> DeleteVolumeAsync(string name, CancellationToken ct);

    // swarm: GET /nodes (+ счётчик running-тасков по нодам).
    Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct);

    // swarm: POST /services/create (409 already exists = успех).
    Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct);

    // swarm: DELETE /services/<name> (404 = успех).
    Task<Result> RemoveServiceAsync(string name, CancellationToken ct);

    // swarm: GET /services?filters={"name":…} — имена сервисов по префиксу
    // (объекты нод кластера в swarm — сервисы).
    Task<Result<IReadOnlyList<string>>> ListServicesAsync(string namePrefix, CancellationToken ct);

    // swarm: GET /tasks?filters={"service":…} — таски сервиса с хостом ноды.
    Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct);

    // Занятые host:port publish-порты: контейнеры движка (plain) + таски на swarm-нодах.
    Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct);

    // Лимиты контейнера (HostConfig.NanoCPUs/Memory; 0 = без лимита); 404 → null.
    Task<Result<NodeLimits?>> InspectContainerResourcesAsync(string name, CancellationToken ct);

    // Лимиты swarm-сервиса (TaskTemplate.Resources.Limits); 404 → null.
    Task<Result<NodeLimits?>> InspectServiceResourcesAsync(string name, CancellationToken ct);

    // Cmd (args) живого контейнера — сверка V3 (image+args+порт+лимиты);
    // null = контейнера нет.
    Task<Result<IReadOnlyList<string>?>> InspectContainerCmdAsync(string idOrName, CancellationToken ct);

    // Cmd swarm-сервиса ноды: Spec.TaskTemplate.ContainerSpec.Cmd; null = нет.
    Task<Result<IReadOnlyList<string>?>> InspectServiceCmdAsync(string name, CancellationToken ct);

    // Инспекция endpoint'а контейнера (E9/надзор C): published host-порт 6379
    // + флаг running; null = объекта нет.
    Task<Result<DockerNodeEndpoint?>> InspectNodeEndpointAsync(string name, CancellationToken ct);
}

// Факт endpoint'а из docker inspect (E9/надзор C): published-порт контейнера
// на хосте + State.Running (остановленный контейнер — положительное
// свидетельство живого docker-факта: PortBindings персистят — endpoint не-null
// при Running=false; надзор трактует отказ пробы как молчание, не слепоту).
// TaskHost — хост running-таска (swarm-фолбэк: порт и хост — из ОДНОГО вызова
// ListTasks движка); null в plain-ветке (host даёт перебор движков).
public sealed record DockerNodeEndpoint(int ClientHostPort, string? TaskHost = null, bool Running = true);

// Контейнер из /containers/json (Names — с ведущим "/").
public sealed record DockerContainer(string Id, string[] Names, string State, string Image);

// Swarm-нода из /nodes + число работающих тасков.
public sealed record DockerSwarmNode(string Id, string Hostname, string State, int RunningTasks);

// Таск swarm-сервиса; Host — hostname ноды (NodeId → /nodes), PublishedPort —
// publish mode=host.
public sealed record DockerTask(string Id, string NodeId, string State, string? Host, int? PublishedPort);

// Пара портов контейнер→хост (tcp).
public sealed record PortMap(int ContainerPort, int HostPort);

// Спецификация контейнера ноды (упрощение домена, arch/21 §2): БЕЗ env
// (конфигурация — Cmd-флаги valkey-server), без volume данных (persistence
// off); Binds — только named volume TLS-секретов (t06, формат
// "volume:/path"). БЕЗ сети (контейнер живёт в сети запуска воркера).
// Cmd — обязательный (вкл. пустой аргумент `--save ""` — элемент "" массива:
// экранирование решается массивом Engine API, не строкой).
public sealed record ContainerSpec(
    string Image,
    IReadOnlyList<string> Cmd,
    IReadOnlyList<PortMap> Ports,
    string Hostname,
    double? CpuCores,
    long? MemoryBytes,
    string? Label,
    IReadOnlyList<string>? Binds = null);

// Спецификация swarm-сервиса ноды: constraint на конкретную ноду (node.id==<id>).
public sealed record ServiceSpec(string Name, ContainerSpec Template, string NodeConstraint);
