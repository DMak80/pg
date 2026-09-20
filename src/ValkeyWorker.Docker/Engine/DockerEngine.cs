using Shared.Core.Planning;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Shared.Core;
using ValkeyWorker.Core.Model;

namespace ValkeyWorker.Docker.Engine;

// HTTP-ошибка Engine API: не-2xx (кроме идемпотентных 404/409).
public sealed class DockerHttpException(string method, string path, int statusCode, string body)
    : Exception($"docker {method} {path} ответил {statusCode}: {body}")
{
    public int StatusCode { get; } = statusCode;

    public string Body { get; } = body;
}

// Реализация: HttpClient + System.Text.Json по Engine API v1.44 (копия kfw
// с упрощениями домена — см. IDockerEngine).
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
            catch (DockerHttpException e) when (e.StatusCode == 404 && e.Body.Contains("No such image", StringComparison.OrdinalIgnoreCase))
            {
                // Образа нет на хосте — тянем и повторяем create (первый запуск на чистом хосте).
                await PullImageAsync(spec.Image, ct);
                await SendAsync(HttpMethod.Post, $"/containers/create?name={Uri.EscapeDataString(name)}", BuildContainerBody(spec), ct);
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
                // 304 — уже остановлен; 404 — контейнера нет (идемпотентность)
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

    // DELETE /networks/<name>; 404 = успех (идемпотентность). «Has active
    // endpoints» уходит наверх Failed — вызывающий решает (t09-фикс).
    public async Task<Result> DeleteNetworkAsync(string name, CancellationToken ct)
        => await Result.FromAsync(async () =>
        {
            try
            {
                await SendAsync(HttpMethod.Delete, $"/networks/{Uri.EscapeDataString(name)}", ct: ct);
            }
            catch (DockerHttpException e) when (e.StatusCode == 404)
            {
                // сети уже нет — идемпотентность
            }
        });

    // POST /volumes/create; 409 «volume already exists» = успех (идемпотентность).
    public async Task<Result> EnsureVolumeAsync(string name, CancellationToken ct)
        => await Result.FromAsync(async () =>
        {
            try
            {
                await SendAsync(HttpMethod.Post, "/volumes/create",
                    new Dictionary<string, object?> { ["Name"] = name }, ct);
            }
            catch (DockerHttpException e) when (e.StatusCode == 409)
            {
                // volume с таким именем уже есть — идемпотентность
            }
        });

    // Запись tar в named volume через helper-контейнер (серты до старта ноды).
    // Почему не PUT /volumes/{name}/archive: демон без swarm отвечает на
    // локальные тома 503 «volume update only valid for cluster volumes…»
    // (проверено на Engine 29.8; endpoint фактически cluster-only), а PUT
    // /containers/<id>/archive сквозь mount на Docker Desktop падает на xattr
    // (500 при фактически записанных данных). Рабочий транспорт: helper с
    // volume в /mnt + exec «printf %s <b64> | base64 -d > /mnt/<файл>» на
    // каждый entry tar. Helper живёт секунды, имя вне схемы vwk-<C>-node —
    // перечисление объектов кластера его не видит.
    public async Task<Result> PutVolumeArchiveAsync(string name, byte[] tar, string image, CancellationToken ct)
        => await Result.FromAsync(async () =>
        {
            var helper = HelperName(name);
            try
            {
                await CreateHelperAsync(helper, name, image, ct);
                var entries = new List<TarArchive.Entry>();
                foreach (var entry in TarArchive.ReadEntries(tar))
                {
                    // Имя файла — из нашего tar; защита от выхода за /mnt.
                    if (entry.Name.Contains('/') || entry.Name.StartsWith("..", StringComparison.Ordinal))
                        throw new ApplicationException($"tls-volume: недопустимое имя файла '{entry.Name}'");
                    entries.Add(entry);
                }

                // ОДИН exec (t06-ревью): атомарность НАБОРА — файлы пишутся под
                // временными именами, mv в конце переименовывает их на целевые
                // (rename внутри одного тома атомарен). set -e: любой сбой —
                // exit != 0 → Failed, прежний набор сертов не тронут (крах
                // между тремя exec'ами оставлял несовпадающую пару ключ↔серт).
                // Права — из заголовка tar (как делал volume-archive API):
                // процесс ноды в образе НЕ root (entrypoint gosu) — без chmod
                // файлы остаются 0600 root и нода не читает серты.
                var writes = entries.Select(e =>
                    $"printf %s {Convert.ToBase64String(e.Data)} | base64 -d > '/mnt/.{e.Name}.tmp' && chmod {Convert.ToString(e.Mode, 8)} '/mnt/.{e.Name}.tmp'");
                var moves = string.Join(" && ",
                    entries.Select(e => $"mv '/mnt/.{e.Name}.tmp' '/mnt/{e.Name}'"));
                await ExecInHelperAsync(helper,
                    "set -e; umask 077; " + string.Join(" && ", writes) + " && " + moves,
                    ct);
            }
            finally
            {
                // Чистка helper при любом исходе (даже отмене) — 404 = успех.
                await RemoveContainerAsync(helper, force: true, CancellationToken.None);
            }
        });

    // Чтение tar из named volume: инспекция volume (404 → null — факта сертов
    // нет) + helper с mount + GET container-archive сквозь /mnt (имена файлов
    // в корне архива). 404 helper-create при живом volume не бывает (после
    // инспекции) — прочие ошибки уходят наверх Failed.
    public async Task<Result<byte[]?>> GetVolumeArchiveAsync(string name, string image, CancellationToken ct)
        => await Result<byte[]?>.FromAsync(async () =>
        {
            try
            {
                await SendAsync(HttpMethod.Get, $"/volumes/{Uri.EscapeDataString(name)}", ct: ct);
            }
            catch (DockerHttpException e) when (e.StatusCode == 404)
            {
                return null; // volume нет — факта сертов нет
            }

            var helper = HelperName(name);
            try
            {
                await CreateHelperAsync(helper, name, image, ct);
                var raw = await GetBytesAsync(
                    $"/containers/{Uri.EscapeDataString(helper)}/archive?path=%2Fmnt", ct);
                // docker включает базовый каталог пути: /mnt → записи «mnt/…»
                // плюс сам каталог. Контракт драйвера — tar с файлами в корне:
                // переупаковка (mode записей сохраняется).
                var entries = TarArchive.ReadEntries(raw)
                    .Where(e => !e.Name.EndsWith('/'))
                    .Select(e => e.Name.StartsWith("mnt/", StringComparison.Ordinal)
                        ? e with { Name = e.Name["mnt/".Length..] }
                        : e)
                    .Where(e => e.Name.Length > 0)
                    .ToList();
                return TarArchive.Build(entries);
            }
            finally
            {
                await RemoveContainerAsync(helper, force: true, CancellationToken.None);
            }
        });

    // DELETE /volumes/{name}; 404 = успех (идемпотентность демонтажа X1).
    public async Task<Result> DeleteVolumeAsync(string name, CancellationToken ct)
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

    // Имена swarm-сервисов по префиксу: docker-фильтр name — подстрочный,
    // поэтому дублируем строгий StartsWith на клиенте.
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
                    published))
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

    // Лимиты контейнера (автоконверге C, arch/21 §5 C): те же поля, что пишет
    // CreateContainerAsync; 404 → null. Несимметричность тегов Docker:
    // create принимает «NanoCPUs», inspect отдаёт HostConfig.«NanoCpus» —
    // читаем оба варианта.
    public async Task<Result<NodeLimits?>> InspectContainerResourcesAsync(string name, CancellationToken ct)
        => await Result<NodeLimits?>.FromAsync(async () =>
        {
            try
            {
                var body = await GetAsync<JsonElement>(
                    $"/containers/{Uri.EscapeDataString(name)}/json", ct);
                if (body.ValueKind == JsonValueKind.Undefined)
                    return null; // пустое тело — факта для сверки нет
                var host = body.GetProperty("HostConfig");
                var cpu = TryReadNumber(host, "NanoCpus", "NanoCPUs");
                var mem = TryReadNumber(host, "Memory");
                return new NodeLimits(
                    cpu > 0 ? cpu / 1_000_000_000m : null, // NanoCPUs → decimal ядер
                    mem > 0 ? mem : null);
            }
            catch (DockerHttpException e) when (e.StatusCode == 404)
            {
                return null; // контейнера нет — факта для сверки нет
            }
        });

    // Лимиты swarm-сервиса ноды: TaskTemplate.Resources.Limits.{NanoCPUs,
    // MemoryBytes}; 404 → null. Swarm-теги — «NanoCPUs», но на асимметрию
    // Docker не полагаемся — читаем оба варианта.
    public async Task<Result<NodeLimits?>> InspectServiceResourcesAsync(string name, CancellationToken ct)
        => await Result<NodeLimits?>.FromAsync(async () =>
        {
            try
            {
                var body = await GetAsync<JsonElement>(
                    $"/services/{Uri.EscapeDataString(name)}", ct);
                if (body.ValueKind == JsonValueKind.Undefined)
                    return null; // пустое тело — факта для сверки нет
                var limits = body.GetProperty("Spec").GetProperty("TaskTemplate")
                    .GetProperty("Resources").GetProperty("Limits");
                var cpu = TryReadNumber(limits, "NanoCPUs", "NanoCpus");
                var mem = TryReadNumber(limits, "MemoryBytes", "Memory");
                return new NodeLimits(
                    cpu > 0 ? cpu / 1_000_000_000m : null, // NanoCPUs → decimal ядер
                    mem > 0 ? mem : null);
            }
            catch (DockerHttpException e) when (e.StatusCode == 404)
            {
                return null; // сервиса нет — факта для сверки нет
            }
        });

    // Cmd (args) живого контейнера — сверка V3: Config.Cmd; 404 → null (объекта нет).
    public async Task<Result<IReadOnlyList<string>?>> InspectContainerCmdAsync(
        string idOrName, CancellationToken ct)
        => await Result<IReadOnlyList<string>?>.FromAsync(async () =>
        {
            try
            {
                var body = await GetAsync<JsonElement>(
                    $"/containers/{Uri.EscapeDataString(idOrName)}/json", ct);
                if (body.ValueKind == JsonValueKind.Undefined)
                    return null;
                var cmd = body.GetProperty("Config").GetProperty("Cmd");
                return cmd.ValueKind == JsonValueKind.Array
                    ? (IReadOnlyList<string>?)[.. cmd.EnumerateArray().Select(e => e.GetString() ?? "")]
                    : null;
            }
            catch (DockerHttpException e) when (e.StatusCode == 404)
            {
                return null; // контейнера нет — факта нет
            }
        });

    // Cmd swarm-сервиса ноды (сверка V3): Spec.TaskTemplate.ContainerSpec.Cmd; 404 → null.
    public async Task<Result<IReadOnlyList<string>?>> InspectServiceCmdAsync(
        string name, CancellationToken ct)
        => await Result<IReadOnlyList<string>?>.FromAsync(async () =>
        {
            try
            {
                var body = await GetAsync<JsonElement>(
                    $"/services/{Uri.EscapeDataString(name)}", ct);
                if (body.ValueKind == JsonValueKind.Undefined)
                    return null;
                var cmd = body.GetProperty("Spec").GetProperty("TaskTemplate")
                    .GetProperty("ContainerSpec").GetProperty("Cmd");
                return cmd.ValueKind == JsonValueKind.Array
                    ? (IReadOnlyList<string>?)[.. cmd.EnumerateArray().Select(e => e.GetString() ?? "")]
                    : null;
            }
            catch (DockerHttpException e) when (e.StatusCode == 404)
            {
                return null; // сервиса нет — факта нет
            }
        });

    // Инспекция endpoint'а контейнера (E9/надзор C): published host-порт ноды
    // 6379 из HostConfig.PortBindings + State.Running (PortBindings персистят
    // и у остановленного контейнера — Running отличает «жив» от «есть, но
    // остановлен»). 404 → null (объекта нет).
    public async Task<Result<DockerNodeEndpoint?>> InspectNodeEndpointAsync(string name, CancellationToken ct)
        => await Result<DockerNodeEndpoint?>.FromAsync(async () =>
        {
            try
            {
                var body = await GetAsync<JsonElement>(
                    $"/containers/{Uri.EscapeDataString(name)}/json", ct);
                if (body.ValueKind == JsonValueKind.Undefined)
                    return null;

                var clientPort = ReadClientHostPort(body.GetProperty("HostConfig"));
                if (clientPort is not { } port)
                    return null; // привязки 6379 нет — endpoint-факта нет

                // State.Running: отсутствие поля (старый движок) трактуем как running.
                var running = body.TryGetProperty("State", out var state)
                              && state.ValueKind == JsonValueKind.Object
                              && state.TryGetProperty("Running", out var isRunning)
                              && isRunning.ValueKind == JsonValueKind.True;
                return new DockerNodeEndpoint(port, Running: running);
            }
            catch (DockerHttpException e) when (e.StatusCode == 404)
            {
                // Контейнера нет: swarm-фолбэк — только на swarm-движке (hostAlias
                // null; на plain-хосте /tasks даёт 503 not-a-swarm-manager).
                return hostAlias is null ? await InspectSwarmTaskEndpointAsync(name, ct) : null;
            }
        });

    // swarm-фолбэк: published-порт и хост running-таска сервиса. Один
    // ListTasks — порт и TaskHost из одного снимка таска. Сервиса/таска нет →
    // null (факта нет).
    private async Task<DockerNodeEndpoint?> InspectSwarmTaskEndpointAsync(string name, CancellationToken ct)
    {
        var tasks = await ListTasksAsync(name, ct);
        if (!tasks.IsSuccess)
            throw tasks.Error!;
        var running = tasks.Value.FirstOrDefault(t => t.State == "running" && t.PublishedPort > 0);
        return running is null
            ? null
            : new DockerNodeEndpoint(running.PublishedPort!.Value, running.Host);
    }

    // "6379/tcp" → первый HostPort (int).
    private static int? ReadClientHostPort(JsonElement hostConfig)
    {
        if (!hostConfig.TryGetProperty("PortBindings", out var bindings)
            || bindings.ValueKind != JsonValueKind.Object
            || !bindings.TryGetProperty("6379/tcp", out var slot)
            || slot.ValueKind != JsonValueKind.Array
            || slot.GetArrayLength() == 0)
            return null;

        var first = slot[0];
        return first.TryGetProperty("HostPort", out var hp)
            && int.TryParse(hp.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var port)
                ? port
                : null;
    }

    // Числовое поле по первому существующему имени (0 — нет/не число).
    private static long TryReadNumber(JsonElement parent, params string[] names)
    {
        foreach (var name in names)
        {
            if (parent.ValueKind == JsonValueKind.Object
                && parent.TryGetProperty(name, out var value)
                && value.ValueKind == JsonValueKind.Number)
                return value.GetInt64();
        }

        return 0;
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

    internal static object BuildContainerBody(ContainerSpec spec)
    {
        var hostConfig = new Dictionary<string, object?>
        {
            // arch/21 §2: docker сам поднимает контейнер ноды после ребута
            // хоста; служебные контейнеры (TLS-helper) задают «no» в спеке.
            ["RestartPolicy"] = new { Name = spec.RestartPolicy ?? "unless-stopped" },
        };
        if (spec.Ports.Count > 0)
        {
            var bindings = spec.Ports.ToDictionary(
                p => $"{p.ContainerPort}/tcp",
                p => new[] { new { HostPort = p.HostPort.ToString(CultureInfo.InvariantCulture) } });
            hostConfig["PortBindings"] = bindings;
        }

        // Лимиты ресурсов: поля HostConfig НАПРЯМУЮ — NanoCPUs/Memory;
        // вложенный HostConfig.Resources docker молча игнорирует.
        if (spec.CpuCores is { } cores)
            hostConfig["NanoCPUs"] = (long)(cores * 1_000_000_000);
        if (spec.MemoryBytes is { } memory)
            hostConfig["Memory"] = memory;

        // Named volume TLS-секретов (t06): Binds формата "volume:/path".
        if (spec.Binds is { Count: > 0 })
            hostConfig["Binds"] = spec.Binds.ToArray();

        var body = new Dictionary<string, object?>
        {
            ["Image"] = spec.Image,
            // Cmd — обязательный: флаги valkey-server (вкл. пустой элемент
            // `--save ""` — экранирование решается массивом Engine API).
            ["Cmd"] = spec.Cmd.ToArray(),
            ["Hostname"] = spec.Hostname,
            ["HostConfig"] = hostConfig,
        };
        if (spec.Label is { Length: > 0 } label)
            body["Labels"] = new Dictionary<string, string> { ["pgworker"] = label };
        return body;
    }

    private static object BuildServiceBody(ServiceSpec spec)
    {
        var container = new Dictionary<string, object?>
        {
            ["Image"] = spec.Template.Image,
            ["Cmd"] = spec.Template.Cmd.ToArray(),
            ["Hostname"] = spec.Template.Hostname,
        };
        if (spec.Template.Label is { Length: > 0 } label)
            container["Labels"] = new Dictionary<string, string> { ["pgworker"] = label };
        // Binds swarm-шаблона — Mounts (named volume TLS-секретов, t06).
        if (spec.Template.Binds is { Count: > 0 })
        {
            container["Mounts"] = spec.Template.Binds.Select(b => new
            {
                Type = "volume",
                Source = b[..b.IndexOf(':')],
                Target = b[(b.IndexOf(':') + 1)..],
            }).ToArray();
        }

        var taskTemplate = new Dictionary<string, object?>
        {
            ["ContainerSpec"] = container,
            // NodeConstraint — id swarm-ноды; полный вид constraint: node.id==<id>.
            ["Placement"] = new { Constraints = new[] { "node.id==" + spec.NodeConstraint } },
        };

        // Лимиты таска: TaskTemplate.Resources.Limits — поля уровня задачи swarm.
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
                    PublishMode = "host", // без ingress-балансировщика
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

    // GET байтового тела (tar из container-archive сквозь mount volume):
    // пустое тело → пустой массив.
    private async Task<byte[]> GetBytesAsync(string path, CancellationToken ct)
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

        return await response.Content.ReadAsByteArrayAsync(ct);
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

    // Pull образа (POST /images/create): гарантирует наличие nodeImage перед create.
    internal Task PullImageAsync(string imageName, CancellationToken ct)
        => SendAsync(HttpMethod.Post, $"/images/create?fromImage={Uri.EscapeDataString(imageName)}", ct: ct);

    // ── Helper-контейнер TLS-volume (t06): create+start «sleep», volume в /mnt ──

    private static string HelperName(string volume)
        => $"vwk-tls-writer-{Guid.NewGuid().ToString("N")[..12]}";

    private async Task CreateHelperAsync(string name, string volume, string image, CancellationToken ct)
    {
        // NodeImage помощника (тот же, что у ноды — локально гарантирован);
        // без портов/лимитов — только sleep и mount (объект пустой);
        // RestartPolicy «no» (t06-ревью): крах воркера в окне записи не
        // оставляет демону вечно рестартуемого держателя volume.
        var spec = new ContainerSpec(
            image, ["sleep", "120"], [], name, null, null, null,
            Binds: (IReadOnlyList<string>?)new[] { volume + ":/mnt" },
            RestartPolicy: "no");
        var created = await CreateContainerAsync(spec, name, ct);
        if (!created.IsSuccess)
            throw created.Error!;
        var started = await StartContainerAsync(name, ct);
        if (!started.IsSuccess)
            throw started.Error!;
    }

    // Exec «sh -c <команда>» в helper: Detach-start + поллинг inspect до
    // завершения (бюджет 30 с — записи PEM занимают миллисекунды);
    // ExitCode != 0 → исключение (Result-монада у вызывающего).
    private async Task ExecInHelperAsync(string container, string shellCommand, CancellationToken ct)
    {
        var exec = await PostForJsonAsync<ExecDto>(
            $"/containers/{Uri.EscapeDataString(container)}/exec",
            new { Cmd = new[] { "sh", "-c", shellCommand }, AttachStdout = true, AttachStderr = true },
            ct) ?? throw new ApplicationException($"docker exec: пустой ответ create ({container})");
        await SendAsync(HttpMethod.Post, $"/exec/{Uri.EscapeDataString(exec.Id)}/start",
            new { Detach = true, Tty = false }, ct);

        var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(30);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var state = await GetAsync<ExecDto>($"/exec/{Uri.EscapeDataString(exec.Id)}/json", ct);
            if (state is { Running: false, ExitCode: not null })
            {
                if (state.ExitCode != 0)
                    throw new ApplicationException(
                        $"docker exec (helper {container}): exit {state.ExitCode}");
                return;
            }

            await Task.Delay(250, ct);
        }

        throw new TimeoutException($"docker exec (helper {container}) не завершился за 30 с");
    }

    // POST с JSON-ответом (exec create; SendAsync тело ответа отбрасывает).
    private async Task<T?> PostForJsonAsync<T>(string path, object? body, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, Api + path);
        if (body is not null)
            request.Content = new StringContent(JsonSerializer.Serialize(body, Json), Encoding.UTF8, "application/json");
        using var response = await httpClient.SendAsync(request, ct);
        if (!response.IsSuccessStatusCode)
        {
            var errorBody = response.Content is null
                ? string.Empty
                : await response.Content.ReadAsStringAsync(ct);
            throw new DockerHttpException("POST", path, (int)response.StatusCode, errorBody);
        }

        var text = await response.Content.ReadAsStringAsync(ct);
        return text.Length == 0 ? default : JsonSerializer.Deserialize<T>(text, Json);
    }

    private sealed class ExecDto
    {
        [JsonPropertyName("Id")] public string Id { get; set; } = "";

        [JsonPropertyName("Running")] public bool Running { get; set; }

        // null, пока exec ещё работает (канон Engine API).
        [JsonPropertyName("ExitCode")] public int? ExitCode { get; set; }
    }

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
