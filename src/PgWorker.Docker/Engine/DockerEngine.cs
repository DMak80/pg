using System.Buffers.Binary;
using System.Globalization;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using PgWorker.Core;

namespace PgWorker.Docker.Engine;

// Фабрика движков: endpoint "unix:///var/run/docker.sock" | "tcp://host:2375".
// API-версия закреплена v1.44 (решение фазы plan №2; docker >= 23).
public class DockerEngineFactory
{
    // Транспортный handler: unix → ConnectCallback с UnixDomainSocketEndPoint.
    internal HttpMessageHandler CreateHandler(string endpoint)
    {
        var sockets = new SocketsHttpHandler
        {
            // docker-прокси держит соединения — не рвём их агрессивно
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
        };
        if (endpoint.StartsWith("unix://", StringComparison.Ordinal))
        {
            var socketPath = endpoint["unix://".Length..];
            sockets.ConnectCallback = async (context, ct) =>
            {
                var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                try
                {
                    await socket.ConnectAsync(new UnixDomainSocketEndPoint(socketPath), ct);
                    return new NetworkStream(socket, ownsSocket: true);
                }
                catch
                {
                    socket.Dispose();
                    throw;
                }
            };
        }

        return sockets;
    }

    // hostAlias — имя docker-хоста для BusyPorts plain-режима (swarm: null).
    public virtual IDockerEngine Create(string endpoint, string? hostAlias = null)
    {
        var baseAddress = endpoint.StartsWith("unix://", StringComparison.Ordinal)
            ? "http://localhost" // фиктивный хост: соединение уходит в unix-сокет через ConnectCallback
            : endpoint;
        var httpClient = new HttpClient(CreateHandler(endpoint)) { BaseAddress = new Uri(baseAddress) };
        return new DockerEngine(httpClient, hostAlias);
    }
}

// HTTP-ошибка Engine API: не-2xx (кроме идемпотентных 404/409).
public sealed class DockerHttpException(string method, string path, int statusCode, string body)
    : Exception($"docker {method} {path} ответил {statusCode}: {body}")
{
    public int StatusCode { get; } = statusCode;

    public string Body { get; } = body;
}

// Реализация: HttpClient + System.Text.Json по Engine API v1.44.
public sealed class DockerEngine(HttpClient httpClient, string? hostAlias) : IDockerEngine
{
    private const string Api = "/v1.44";

    // PascalCase-имена как в Engine API (Go-парсер матчит без учёта регистра,
    // но канонический вид надёжнее и читаемее в логах).
    private static readonly JsonSerializerOptions Json = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        NumberHandling = JsonNumberHandling.AllowReadingFromString,
    };

    public async Task<Result> PingAsync(CancellationToken ct)
        => await Result.FromAsync(async () => await SendAsync(HttpMethod.Get, "/_ping", ct: ct));

    public async Task<Result<IReadOnlyList<DockerContainer>>> ListContainersAsync(
        string namePrefix, bool all, CancellationToken ct)
    {
        var query = $"?all={(all ? 1 : 0)}";
        if (namePrefix.Length > 0)
            query += "&filters=" + Uri.EscapeDataString("{\"name\":[\"" + namePrefix + "\"]}");

        return await Result<IReadOnlyList<DockerContainer>>.FromAsync(async () =>
        {
            var list = await GetAsync<List<ContainerDto>>("/containers/json" + query, ct) ?? [];
            return (IReadOnlyList<DockerContainer>)list
                .Select(c => new DockerContainer(
                    c.Id,
                    (c.Names ?? []).Select(n => n.StartsWith("/", StringComparison.Ordinal) ? n[1..] : n).ToArray(),
                    c.State ?? string.Empty,
                    c.Image ?? string.Empty))
                .ToList();
        });
    }

    public async Task<Result> CreateContainerAsync(ContainerSpec spec, string name, CancellationToken ct)
        => await Result.FromAsync(async () =>
        {
            try
            {
                await SendAsync(HttpMethod.Post, $"/containers/create?name={Uri.EscapeDataString(name)}", BuildContainerBody(spec), ct);
            }
            catch (DockerHttpException e) when (e.StatusCode == 409 && e.Body.Contains("already", StringComparison.OrdinalIgnoreCase))
            {
                // идемпотентность: контейнер с именем уже существует
            }
        });

    public async Task<Result> StartContainerAsync(string idOrName, CancellationToken ct)
        => await Result.FromAsync(async () =>
            await SendAsync(HttpMethod.Post, $"/containers/{Uri.EscapeDataString(idOrName)}/start", ct: ct));

    public async Task<Result> StopContainerAsync(string idOrName, int timeoutSec, CancellationToken ct)
        => await Result.FromAsync(async () =>
        {
            try
            {
                await SendAsync(HttpMethod.Post, $"/containers/{Uri.EscapeDataString(idOrName)}/stop?t={timeoutSec}", ct: ct);
            }
            catch (DockerHttpException e) when (e.StatusCode is 304 or 404)
            {
                // 304 — уже остановлен; 404 — контейнера нет (идемпотентность карантина E3)
            }
        });

    public async Task<Result> RemoveContainerAsync(string idOrName, bool force, CancellationToken ct)
        => await Result.FromAsync(async () =>
        {
            try
            {
                await SendAsync(HttpMethod.Delete, $"/containers/{Uri.EscapeDataString(idOrName)}?force={(force ? 1 : 0)}&v=1", ct: ct);
            }
            catch (DockerHttpException e) when (e.StatusCode == 404)
            {
                // уже удалён — идемпотентность
            }
        });

    public async Task<Result> RemoveVolumeAsync(string name, CancellationToken ct)
        => await Result.FromAsync(async () =>
        {
            try
            {
                await SendAsync(HttpMethod.Delete, $"/volumes/{Uri.EscapeDataString(name)}", ct: ct);
            }
            catch (DockerHttpException e) when (e.StatusCode == 404)
            {
                // volume уже нет — идемпотентность
            }
        });

    public async Task<Result> EnsureNetworkAsync(string name, CancellationToken ct)
        => await Result.FromAsync(async () =>
        {
            try
            {
                await SendAsync(HttpMethod.Post, "/networks/create",
                    new Dictionary<string, object?> { ["Name"] = name }, ct);
            }
            catch (DockerHttpException e) when (e.StatusCode == 409)
            {
                // сеть с таким именем уже есть — идемпотентность
            }
        });

    // Exec в контейнере (t01): create → start (raw-stream) → inspect ExitCode.
    public async Task<Result<string>> ExecAsync(string containerId, IReadOnlyList<string> cmd, CancellationToken ct)
        => await Result<string>.FromAsync(async () =>
        {
            // 1) создать exec-инстанс (AttachStdout/Stderr — стрим в ответе /start).
            var exec = await PostAsync<ExecDto>(
                $"/containers/{Uri.EscapeDataString(containerId)}/exec",
                new Dictionary<string, object?>
                {
                    ["AttachStdout"] = true,
                    ["AttachStderr"] = true,
                    ["Cmd"] = cmd,
                }, ct);
            if (exec is not { Id.Length: > 0 })
                throw new DockerHttpException("POST", $"/containers/{containerId}/exec", 500, "пустой ответ exec create");

            // 2) старт: тело ответа — application/vnd.docker.raw-stream (мультиплексирован).
            var (stdout, stderr) = await StartExecAsync(exec.Id, ct);

            // 3) exit-код; ненулевой — ошибка со stderr (не выбрасываем его молча).
            var inspect = await GetAsync<ExecInspectDto>($"/exec/{Uri.EscapeDataString(exec.Id)}/json", ct);
            var exit = inspect?.ExitCode ?? -1;
            if (exit != 0)
                throw new ApplicationException($"exec {string.Join(' ', cmd)} → exit {exit}: {stderr}");

            return stdout;
        });

    // POST /exec/<id>/start {"Detach":false,"Tty":false} — чтение всего тела
    // байтами (raw-stream), демультиплексирование фреймов stdout/stderr.
    private async Task<(string Stdout, string Stderr)> StartExecAsync(string execId, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, Api + $"/exec/{Uri.EscapeDataString(execId)}/start")
        {
            Content = new StringContent("""{"Detach":false,"Tty":false}""", Encoding.UTF8, "application/json"),
        };
        using var response = await httpClient.SendAsync(request, ct);
        if (!response.IsSuccessStatusCode)
        {
            var errorBody = response.Content is null
                ? string.Empty
                : await response.Content.ReadAsStringAsync(ct);
            throw new DockerHttpException("POST", $"/exec/{execId}/start", (int)response.StatusCode, errorBody);
        }

        var payload = response.Content is null ? [] : await response.Content.ReadAsByteArrayAsync(ct);
        return Demux(payload);
    }

    // Демультиплексирование raw-stream: фрейм = 8-байтный заголовок
    // [stream-type,0,0,0, size BE32] + size байт payload; тип 1 = stdout, 2 = stderr.
    internal static (string Stdout, string Stderr) Demux(byte[] payload)
    {
        var stdout = new MemoryStream();
        var stderr = new MemoryStream();
        var offset = 0;
        while (offset + 8 <= payload.Length)
        {
            var type = payload[offset];
            var size = BinaryPrimitives.ReadInt32BigEndian(payload.AsSpan(offset + 4, 4));
            if (size < 0 || offset + 8 + size > payload.Length)
                break; // обрезанный фрейм — игнорируем хвост

            var target = type switch
            {
                1 => stdout,
                2 => stderr,
                _ => null, // stdin-заголовки и пр. — не наши стримы
            };
            if (target is not null)
                target.Write(payload, offset + 8, size);
            offset += 8 + size;
        }

        return (Encoding.UTF8.GetString(stdout.ToArray()), Encoding.UTF8.GetString(stderr.ToArray()));
    }

    public async Task<Result<IReadOnlyList<DockerSwarmNode>>> ListNodesAsync(CancellationToken ct)
    {
        return await Result<IReadOnlyList<DockerSwarmNode>>.FromAsync(async () =>
        {
            var nodes = await GetAsync<List<NodeDto>>("/nodes", ct) ?? [];
            var tasks = await TryGetTasksAsync(ct: ct);
            var runningByNode = tasks
                .Where(t => t.Status?.State == "running")
                .GroupBy(t => t.NodeId ?? string.Empty)
                .ToDictionary(g => g.Key, g => g.Count());
            return (IReadOnlyList<DockerSwarmNode>)nodes
                .Select(n => new DockerSwarmNode(
                    n.Id,
                    n.Description?.Hostname ?? string.Empty,
                    n.Status?.State ?? string.Empty,
                    runningByNode.TryGetValue(n.Id, out var count) ? count : 0))
                .ToList();
        });
    }

    public async Task<Result> CreateServiceAsync(ServiceSpec spec, CancellationToken ct)
        => await Result.FromAsync(async () =>
        {
            try
            {
                await SendAsync(HttpMethod.Post, "/services/create", BuildServiceBody(spec), ct);
            }
            catch (DockerHttpException e) when (e.StatusCode == 409 && e.Body.Contains("already", StringComparison.OrdinalIgnoreCase))
            {
                // идемпотентность: сервис с именем уже существует
            }
        });

    public async Task<Result> RemoveServiceAsync(string name, CancellationToken ct)
        => await Result.FromAsync(async () =>
        {
            try
            {
                await SendAsync(HttpMethod.Delete, $"/services/{Uri.EscapeDataString(name)}", ct);
            }
            catch (DockerHttpException e) when (e.StatusCode == 404)
            {
                // сервиса уже нет — идемпотентность
            }
        });

    // Имена swarm-сервисов по префиксу (rework №4): docker-фильтр name —
    // подстрочный, поэтому дублируем строгий StartsWith на клиенте.
    public async Task<Result<IReadOnlyList<string>>> ListServicesAsync(string namePrefix, CancellationToken ct)
    {
        var query = namePrefix.Length > 0
            ? "?filters=" + Uri.EscapeDataString("{\"name\":[\"" + namePrefix + "\"]}")
            : string.Empty;
        return await Result<IReadOnlyList<string>>.FromAsync(async () =>
        {
            var services = await GetAsync<List<ServiceDto>>("/services" + query, ct) ?? [];
            return (IReadOnlyList<string>)services
                .Select(s => s.Spec?.Name)
                .Where(name => name is { Length: > 0 }
                    && (namePrefix.Length == 0 || name.StartsWith(namePrefix, StringComparison.Ordinal)))
                .Select(name => name!)
                .OrderBy(name => name, StringComparer.Ordinal)
                .ToList();
        });
    }

    public async Task<Result<IReadOnlyList<DockerTask>>> ListTasksAsync(string serviceName, CancellationToken ct)
    {
        return await Result<IReadOnlyList<DockerTask>>.FromAsync(async () =>
        {
            var raw = await GetAsync<List<TaskDto>>("/tasks?filters=" + TaskFilter(serviceName), ct) ?? [];
            var nodes = await TryGetNodesAsync(ct);
            var published = await TryGetServicePublishedPortAsync(serviceName, ct);
            return (IReadOnlyList<DockerTask>)raw
                .Select(t => new DockerTask(
                    t.Id,
                    t.NodeId ?? string.Empty,
                    t.Status?.State ?? string.Empty,
                    t.NodeId is { } nodeId && nodes.TryGetValue(nodeId, out var host) ? host : null,
                    published,
                    t.Status?.ContainerStatus?.ContainerId))
                .ToList();
        });
    }

    public async Task<Result<IReadOnlySet<(string Host, int Port)>>> BusyPortsAsync(CancellationToken ct)
    {
        return await Result<IReadOnlySet<(string Host, int Port)>>.FromAsync(async () =>
        {
            var busy = new HashSet<(string, int)>();

            // 1) publish-порты контейнеров этого docker-хоста (plain: все — на hostAlias).
            var containers = await GetAsync<List<ContainerDto>>("/containers/json?all=1", ct) ?? [];
            if (hostAlias is not null)
            {
                foreach (var port in containers.SelectMany(c => c.Ports ?? []).Where(p => p.PublicPort > 0))
                    busy.Add((hostAlias, port.PublicPort));
            }

            // 2) swarm: publish-порты сервисов на нодах их running-тасков (mode=host).
            foreach (var (nodeHost, port) in await CollectSwarmPortsAsync(ct))
                busy.Add((nodeHost, port));

            return (IReadOnlySet<(string, int)>)busy;
        });
    }

    // (hostname ноды, published порт) по services×running tasks.
    private async Task<List<(string Host, int Port)>> CollectSwarmPortsAsync(CancellationToken ct)
    {
        var result = new List<(string, int)>();
        List<ServiceDto>? services;
        try
        {
            services = await GetAsync<List<ServiceDto>>("/services", ct);
        }
        catch (DockerHttpException)
        {
            return result; // не swarm-менеджер — publish-портов нод нет
        }

        if (services is null || services.Count == 0)
            return result;

        var nodes = await TryGetNodesAsync(ct);
        var tasks = (await TryGetTasksAsync(ct: ct)).Where(t => t.Status?.State == "running").ToList();
        foreach (var service in services)
        {
            var ports = (service.Endpoint?.Ports ?? [])
                .Where(p => p is { PublishedPort: > 0, PublishMode: "host" or null })
                .Select(p => p.PublishedPort!.Value)
                .ToList();
            if (ports.Count == 0)
                continue;

            foreach (var task in tasks.Where(t => t.ServiceId == service.Id))
            {
                if (task.NodeId is { } nodeId && nodes.TryGetValue(nodeId, out var host))
                    foreach (var port in ports)
                        result.Add((host, port));
            }
        }

        return result;
    }

    private static string TaskFilter(string serviceName)
        => Uri.EscapeDataString("{\"service\":{\"" + serviceName + "\":true}}");

    private async Task<Dictionary<string, string>> TryGetNodesAsync(CancellationToken ct)
    {
        try
        {
            var nodes = await GetAsync<List<NodeDto>>("/nodes", ct) ?? [];
            return nodes
                .Where(n => n.Description?.Hostname is { Length: > 0 })
                .ToDictionary(n => n.Id, n => n.Description!.Hostname!);
        }
        catch (DockerHttpException)
        {
            return []; // не swarm — хостов-нод нет
        }
    }

    private async Task<List<TaskDto>> TryGetTasksAsync(string? serviceName = null, CancellationToken ct = default)
    {
        try
        {
            var path = "/tasks";
            if (serviceName is not null)
                path += "?filters=" + TaskFilter(serviceName);
            return await GetAsync<List<TaskDto>>(path, ct) ?? [];
        }
        catch (DockerHttpException)
        {
            return []; // не swarm — тасков нет
        }
    }

    private async Task<int?> TryGetServicePublishedPortAsync(string serviceName, CancellationToken ct)
    {
        try
        {
            var filters = Uri.EscapeDataString("{\"name\":{\"" + serviceName + "\":true}}");
            var services = await GetAsync<List<ServiceDto>>("/services?filters=" + filters, ct);
            return services?
                .SelectMany(s => s.Endpoint?.Ports ?? [])
                .Where(p => p is { PublishedPort: > 0, PublishMode: "host" or null })
                .Select(p => p.PublishedPort)
                .FirstOrDefault();
        }
        catch (DockerHttpException)
        {
            return null;
        }
    }

    private static object BuildContainerBody(ContainerSpec spec)
    {
        var hostConfig = new Dictionary<string, object?>
        {
            ["RestartPolicy"] = new { Name = "unless-stopped" }, // docker сам поднимает после ребута хоста
        };
        if (spec.VolumeName.Length > 0)
            hostConfig["Binds"] = new[] { $"{spec.VolumeName}:{spec.VolumeDest}" };
        if (spec.Ports.Count > 0)
        {
            var bindings = spec.Ports.ToDictionary(
                p => $"{p.ContainerPort}/tcp",
                p => new[] { new { HostPort = p.HostPort.ToString(CultureInfo.InvariantCulture) } });
            hostConfig["PortBindings"] = bindings;
        }

        // Лимиты ресурсов (rework №5): поля HostConfig НАПРЯМУЮ — NanoCPUs/Memory;
        // вложенный HostConfig.Resources docker молча игнорирует.
        if (spec.CpuCores is { } cores)
            hostConfig["NanoCPUs"] = (long)(cores * 1_000_000_000);
        if (spec.MemoryBytes is { } memory)
            hostConfig["Memory"] = memory;

        var body = new Dictionary<string, object?>
        {
            ["Image"] = spec.Image,
            ["Env"] = spec.Env.Select(p => $"{p.Key}={p.Value}").OrderBy(v => v, StringComparer.Ordinal).ToArray(),
            ["Hostname"] = spec.Hostname,
            ["HostConfig"] = hostConfig,
        };
        if (spec.Network is { Length: > 0 } network)
        {
            // Общая сеть нод кластера: контейнеры резолвят друг друга по alias
            // (hostname) — внутренние адреса Patroni-репликации.
            hostConfig["NetworkMode"] = network;
            body["NetworkingConfig"] = new Dictionary<string, object?>
            {
                ["EndpointsConfig"] = new Dictionary<string, object?>
                {
                    [network] = new { Aliases = spec.NetworkAliases ?? [] },
                },
            };
        }
        if (spec.Cmd is { Count: > 0 } cmd)
            body["Cmd"] = cmd;
        if (spec.Label is { Length: > 0 } label)
            body["Labels"] = new Dictionary<string, string> { ["pgworker"] = label };
        return body;
    }

    private static object BuildServiceBody(ServiceSpec spec)
    {
        var container = new Dictionary<string, object?>
        {
            ["Image"] = spec.Template.Image,
            ["Env"] = spec.Template.Env.Select(p => $"{p.Key}={p.Value}").OrderBy(v => v, StringComparer.Ordinal).ToArray(),
            ["Hostname"] = spec.Template.Hostname,
        };
        if (spec.Template.VolumeName.Length > 0)
        {
            container["Mounts"] = new[]
            {
                new { Type = "volume", Source = spec.Template.VolumeName, Target = spec.Template.VolumeDest },
            };
        }

        if (spec.Template.Label is { Length: > 0 } label)
            container["Labels"] = new Dictionary<string, string> { ["pgworker"] = label };

        var taskTemplate = new Dictionary<string, object?>
        {
            ["ContainerSpec"] = container,
            // NodeConstraint — id swarm-ноды; полный вид constraint: node.id==<id> (spec §5.3)
            ["Placement"] = new { Constraints = new[] { "node.id==" + spec.NodeConstraint } },
        };

        // Лимиты таска (rework №5): TaskTemplate.Resources.Limits — поля уровня
        // задачи swarm (вложение в ContainerSpec docker игнорирует).
        if (spec.Template.CpuCores is { } cores || spec.Template.MemoryBytes is { } memory)
        {
            taskTemplate["Resources"] = new Dictionary<string, object?>
            {
                ["Limits"] = new Dictionary<string, object?>
                {
                    ["NanoCPUs"] = spec.Template.CpuCores is { } c ? (long)(c * 1_000_000_000) : null,
                    ["MemoryBytes"] = spec.Template.MemoryBytes,
                },
            };
        }

        object? endpoint = null;
        if (spec.Template.Ports.Count > 0)
        {
            endpoint = new
            {
                Ports = spec.Template.Ports.Select(p => new
                {
                    Protocol = "tcp",
                    TargetPort = p.ContainerPort,
                    PublishedPort = p.HostPort,
                    PublishMode = "host", // без ingress-балансировщика (spec §5.3)
                }),
            };
        }

        return new Dictionary<string, object?>
        {
            ["Name"] = spec.Name,
            ["TaskTemplate"] = taskTemplate,
            ["Endpoint"] = endpoint,
        };
    }

    // Команда без тела ответа: любой не-2xx → DockerHttpException.
    private async Task SendAsync(HttpMethod method, string path, object? body = null, CancellationToken ct = default)
    {
        using var request = new HttpRequestMessage(method, Api + path);
        if (body is not null)
        {
            request.Content = new StringContent(JsonSerializer.Serialize(body, Json), Encoding.UTF8, "application/json");
        }

        using var response = await httpClient.SendAsync(request, ct);
        if (!response.IsSuccessStatusCode)
        {
            var errorBody = response.Content is null
                ? string.Empty
                : await response.Content.ReadAsStringAsync(ct);
            throw new DockerHttpException(method.Method, path, (int)response.StatusCode, errorBody);
        }
    }

    // Команда с JSON-ответом (пустое тело → default).
    private async Task<T?> GetAsync<T>(string path, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, Api + path);
        using var response = await httpClient.SendAsync(request, ct);
        if (!response.IsSuccessStatusCode)
        {
            var errorBody = response.Content is null
                ? string.Empty
                : await response.Content.ReadAsStringAsync(ct);
            throw new DockerHttpException("GET", path, (int)response.StatusCode, errorBody);
        }

        var text = await response.Content.ReadAsStringAsync(ct);
        if (text.Length == 0)
            return default;

        return JsonSerializer.Deserialize<T>(text, Json);
    }

    // POST с JSON-ответом (exec create): любой не-2xx → DockerHttpException.
    private async Task<T?> PostAsync<T>(string path, object body, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, Api + path)
        {
            Content = new StringContent(JsonSerializer.Serialize(body, Json), Encoding.UTF8, "application/json"),
        };
        using var response = await httpClient.SendAsync(request, ct);
        if (!response.IsSuccessStatusCode)
        {
            var errorBody = response.Content is null
                ? string.Empty
                : await response.Content.ReadAsStringAsync(ct);
            throw new DockerHttpException("POST", path, (int)response.StatusCode, errorBody);
        }

        var text = await response.Content.ReadAsStringAsync(ct);
        if (text.Length == 0)
            return default;

        return JsonSerializer.Deserialize<T>(text, Json);
    }

    // Pull образа (POST /images/create): гарантирует наличие nodeImage перед create.
    internal Task PullImageAsync(string imageName, CancellationToken ct)
        => SendAsync(HttpMethod.Post, $"/images/create?fromImage={Uri.EscapeDataString(imageName)}", ct: ct);

    public ValueTask DisposeAsync()
    {
        httpClient.Dispose();
        return ValueTask.CompletedTask;
    }

    // DTO реальных ответов Engine API (только нужные поля).
    private sealed class ContainerDto
    {
        [JsonPropertyName("Id")] public string Id { get; set; } = "";

        [JsonPropertyName("Names")] public List<string>? Names { get; set; }

        [JsonPropertyName("Image")] public string? Image { get; set; }

        [JsonPropertyName("State")] public string? State { get; set; }

        [JsonPropertyName("Ports")] public List<PortDto>? Ports { get; set; }
    }

    private sealed class PortDto
    {
        [JsonPropertyName("PrivatePort")] public int PrivatePort { get; set; }

        [JsonPropertyName("PublicPort")] public int PublicPort { get; set; }
    }

    private sealed class NodeDto
    {
        [JsonPropertyName("ID")] public string Id { get; set; } = "";

        [JsonPropertyName("Description")] public NodeDescriptionDto? Description { get; set; }

        [JsonPropertyName("Status")] public NodeStatusDto? Status { get; set; }
    }

    private sealed class NodeDescriptionDto
    {
        [JsonPropertyName("Hostname")] public string? Hostname { get; set; }
    }

    private sealed class NodeStatusDto
    {
        [JsonPropertyName("State")] public string? State { get; set; }
    }

    private sealed class TaskDto
    {
        [JsonPropertyName("ID")] public string Id { get; set; } = "";

        [JsonPropertyName("ServiceID")] public string? ServiceId { get; set; }

        [JsonPropertyName("NodeID")] public string? NodeId { get; set; }

        [JsonPropertyName("Status")] public TaskStatusDto? Status { get; set; }
    }

    private sealed class TaskStatusDto
    {
        [JsonPropertyName("State")] public string? State { get; set; }

        [JsonPropertyName("ContainerStatus")] public TaskContainerStatusDto? ContainerStatus { get; set; }
    }

    private sealed class TaskContainerStatusDto
    {
        [JsonPropertyName("ContainerID")] public string? ContainerId { get; set; }
    }

    // exec-инстанс из POST /containers/<id>/exec.
    private sealed class ExecDto
    {
        [JsonPropertyName("Id")] public string Id { get; set; } = "";
    }

    // GET /exec/<id>/json — только exit-код.
    private sealed class ExecInspectDto
    {
        [JsonPropertyName("ExitCode")] public int? ExitCode { get; set; }
    }

    private sealed class ServiceDto
    {
        [JsonPropertyName("ID")] public string Id { get; set; } = "";

        [JsonPropertyName("Spec")] public ServiceSpecDto? Spec { get; set; }

        [JsonPropertyName("Endpoint")] public EndpointDto? Endpoint { get; set; }
    }

    private sealed class ServiceSpecDto
    {
        [JsonPropertyName("Name")] public string? Name { get; set; }
    }

    private sealed class EndpointDto
    {
        [JsonPropertyName("Ports")] public List<EndpointPortDto>? Ports { get; set; }
    }

    private sealed class EndpointPortDto
    {
        [JsonPropertyName("PublishedPort")] public int? PublishedPort { get; set; }

        [JsonPropertyName("PublishMode")] public string? PublishMode { get; set; }
    }
}
