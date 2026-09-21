using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Shared.Core;

namespace Shared.Docker;

// Реализация: HttpClient + System.Text.Json по Engine API v1.44.
// Общий движок трёх воркеров (t07): pg-канон + влитые методы kfw/vwk,
// канон-суперсет семантики (spec §7): start 304-идемпотентен, create при
// отсутствии образа — pull+retry, exec-ошибка включает stderr И stdout.
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

    // GET /containers/<id>/json — инспект контейнера для матчинга усыновления
    // (spec §3.1): hostname/aliases/env + host-биндинги NetworkSettings.Ports.
    public async Task<Result<DockerContainerInspect>> InspectContainerAsync(string id, CancellationToken ct)
        => await Result<DockerContainerInspect>.FromAsync(async () =>
        {
            var dto = await GetAsync<ContainerInspectDto>($"/containers/{Uri.EscapeDataString(id)}/json", ct)
                      ?? throw new ApplicationException($"инспект контейнера {id} пуст");
            var ports = new List<PortMap>();
            foreach (var (key, bindings) in dto.NetworkSettings?.Ports ?? [])
            {
                // ключ вида "<port>/<proto>" (например "5432/tcp"): только tcp-биндинги.
                var parts = key.Split('/');
                if (parts.Length != 2 || parts[1] != "tcp" || !int.TryParse(parts[0], out var containerPort))
                    continue;
                foreach (var binding in bindings ?? [])
                    if (int.TryParse(binding.HostPort, out var hostPort))
                        ports.Add(new PortMap(containerPort, hostPort));
            }

            var aliases = (dto.NetworkSettings?.Networks ?? new Dictionary<string, NetworkDto>())
                .Values.SelectMany(n => n.Aliases ?? []).Distinct().ToArray();
            // t07: StartedAt (RFC3339) → unix; отсутствие/битая строка → null
            // (бюджет verify-джоба не применяется).
            long? startedAtUnix = DateTimeOffset.TryParse(dto.State?.StartedAt,
                CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal
                | DateTimeStyles.AdjustToUniversal, out var startedAt)
                ? startedAt.ToUnixTimeSeconds()
                : null;
            return new DockerContainerInspect(dto.Id, dto.Config?.Hostname ?? "", aliases, dto.Config?.Env ?? [],
                ports.Distinct().ToArray(),
                dto.State?.Running, dto.State?.ExitCode, StartedAtUnix: startedAtUnix);
        });

    // GET /containers/<id>/logs — тело raw-stream (мультиплексировано), demux
    // как у exec; не-TTY контейнеры docker всегда шлют фреймами.
    public async Task<Result<string>> GetContainerLogsAsync(string idOrName, int tail, CancellationToken ct)
        => await Result<string>.FromAsync(async () =>
        {
            using var request = new HttpRequestMessage(HttpMethod.Get,
                Api + $"/containers/{Uri.EscapeDataString(idOrName)}/logs?stdout=1&stderr=1&tail={tail}");
            using var response = await httpClient.SendAsync(request, ct);
            if (!response.IsSuccessStatusCode)
            {
                var errorBody = response.Content is null
                    ? string.Empty
                    : await response.Content.ReadAsStringAsync(ct);
                throw new DockerHttpException("GET", $"/containers/{idOrName}/logs", (int)response.StatusCode, errorBody);
            }

            var payload = response.Content is null ? [] : await response.Content.ReadAsByteArrayAsync(ct);
            var (stdout, stderr) = Demux(payload);
            return stderr.Length == 0 ? stdout : stdout + "\n" + stderr;
        });

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
                // Образа нет на хосте — тянем и повторяем create (первый запуск
                // на чистом хосте; kfw/vwk-семантика, канон-суперсет t07 §7.2).
                // Локально-собираемые образы (не в registry) fallback не спасает —
                // поведение не хуже прежнего голого 404.
                await PullImageAsync(spec.Image, ct);
                await SendAsync(HttpMethod.Post, $"/containers/create?name={Uri.EscapeDataString(name)}", BuildContainerBody(spec), ct);
            }
        });

    public async Task<Result> StartContainerAsync(string idOrName, CancellationToken ct)
        => await Result.FromAsync(async () =>
        {
            try
            {
                await SendAsync(HttpMethod.Post, $"/containers/{Uri.EscapeDataString(idOrName)}/start", ct: ct);
            }
            catch (DockerHttpException e) when (e.StatusCode == 304)
            {
                // 304 — контейнер уже запущен (идемпотентность супервиза, t03:
                // контракт интерфейса «304 already-started = успех»; pg-семантика,
                // канон-суперсет t07 §7.1 для kfw/vwk)
            }
        });

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

    public async Task<Result<bool>> VolumeExistsAsync(string name, CancellationToken ct)
        => await Result<bool>.FromAsync(async () =>
        {
            try
            {
                await SendAsync(HttpMethod.Get, $"/volumes/{Uri.EscapeDataString(name)}", ct: ct);
                return true;
            }
            catch (DockerHttpException e) when (e.StatusCode == 404)
            {
                return false; // volume не существует — физически утрачен
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

            // 3) exit-код; ненулевой — ошибка со stderr И stdout (канон-суперсет
            // t07 §7.3, kfw-семантика: Kafka-CLI 4.x печатает диагностику,
            // включая stack trace, в stdout).
            var inspect = await GetAsync<ExecInspectDto>($"/exec/{Uri.EscapeDataString(exec.Id)}/json", ct);
            var exit = inspect?.ExitCode ?? -1;
            if (exit != 0)
                throw new ApplicationException(
                    $"exec {string.Join(' ', cmd)} → exit {exit}: {stderr}"
                    + (stdout.Length > 0 ? $" / stdout: {stdout}" : ""));

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

    // Лимиты контейнера (t06, spec §5.3; docker-факт — 0 = без лимита): те же
    // поля, что пишет CreateContainerAsync; 404 → null. Несимметричность тегов
    // Docker: create принимает «NanoCPUs», inspect отдаёт HostConfig.«NanoCpus» —
    // читаем оба варианта (факт: docker inspect возвращает NanoCpus).
    // Доменные конверсии (vwk: nanoCpus → cores) — у потребителей.
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
                return new NodeLimits(
                    TryReadNumber(host, "NanoCpus", "NanoCPUs"),
                    TryReadNumber(host, "Memory"));
            }
            catch (DockerHttpException e) when (e.StatusCode == 404)
            {
                return null; // контейнера нет — факта для сверки нет
            }
        });

    // Лимиты swarm-сервиса ноды (t06, spec §5.3): TaskTemplate.Resources.
    // Limits.{NanoCPUs, MemoryBytes}; 404 → null. Swarm-теги — «NanoCPUs»
    // (верхний регистр, api/types/swarm), но на асимметрию Docker не
    // полагаемся — читаем оба варианта.
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
                return new NodeLimits(
                    TryReadNumber(limits, "NanoCPUs", "NanoCpus"),
                    TryReadNumber(limits, "MemoryBytes", "Memory"));
            }
            catch (DockerHttpException e) when (e.StatusCode == 404)
            {
                return null; // сервиса нет — факта для сверки нет
            }
        });

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

    // Env живого контейнера (t03): GET /containers/{id}/json → Config.Env[];
    // 404 → null (объекта нет).
    public async Task<Result<IReadOnlyDictionary<string, string>?>> InspectContainerEnvAsync(
        string idOrName, CancellationToken ct)
        => await Result<IReadOnlyDictionary<string, string>?>.FromAsync(async () =>
        {
            try
            {
                var body = await GetAsync<JsonElement>(
                    $"/containers/{Uri.EscapeDataString(idOrName)}/json", ct);
                if (body.ValueKind == JsonValueKind.Undefined)
                    return null;
                var env = body.GetProperty("Config").GetProperty("Env");
                var result = new Dictionary<string, string>();
                foreach (var entry in env.EnumerateArray())
                {
                    var pair = entry.GetString();
                    if (pair is null)
                        continue;
                    var sep = pair.IndexOf('=');
                    if (sep <= 0)
                        continue;
                    result[pair[..sep]] = pair[(sep + 1)..];
                }

                return result;
            }
            catch (DockerHttpException e) when (e.StatusCode == 404)
            {
                return null; // контейнера нет — факта нет
            }
        });

    // Env swarm-сервиса ноды (t03): GET /services/{name} → Spec.TaskTemplate.
    // ContainerSpec.Env[] (KEY=VALUE); 404 → null.
    public async Task<Result<IReadOnlyDictionary<string, string>?>> InspectServiceEnvAsync(
        string name, CancellationToken ct)
        => await Result<IReadOnlyDictionary<string, string>?>.FromAsync(async () =>
        {
            try
            {
                var body = await GetAsync<JsonElement>(
                    $"/services/{Uri.EscapeDataString(name)}", ct);
                if (body.ValueKind == JsonValueKind.Undefined)
                    return null;
                var env = body.GetProperty("Spec").GetProperty("TaskTemplate")
                    .GetProperty("ContainerSpec").GetProperty("Env");
                var result = new Dictionary<string, string>();
                foreach (var entry in env.EnumerateArray())
                {
                    var pair = entry.GetString();
                    if (pair is null)
                        continue;
                    var sep = pair.IndexOf('=');
                    if (sep <= 0)
                        continue;
                    result[pair[..sep]] = pair[(sep + 1)..];
                }

                return result;
            }
            catch (DockerHttpException e) when (e.StatusCode == 404)
            {
                return null; // сервиса нет — факта нет
            }
        });

    // Cmd (args) живого контейнера — сверка V3 (vwk): Config.Cmd; 404 → null.
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

    // Инспекция endpoint'а контейнера (t05 E9 / надзор C): published host-порт
    // клиентского listener'а <containerPort>/tcp из HostConfig.PortBindings
    // (порт — параметр: kfw 9094, vwk 6379 — доменная константа у драйвера) +
    // State.Running (PortBindings персистят и у остановленного контейнера —
    // Running отличает «жив» от «есть, но остановлен»). 404 → null (объекта
    // нет). Advertised-чтение (kfw) — в доменном драйвере (t07 §4.6.1).
    public async Task<Result<DockerNodeEndpoint?>> InspectNodeEndpointAsync(
        string name, int containerPort, CancellationToken ct)
        => await Result<DockerNodeEndpoint?>.FromAsync(async () =>
        {
            try
            {
                var body = await GetAsync<JsonElement>(
                    $"/containers/{Uri.EscapeDataString(name)}/json", ct);
                if (body.ValueKind == JsonValueKind.Undefined)
                    return null;

                var clientPort = ReadClientHostPort(body.GetProperty("HostConfig"), containerPort);
                if (clientPort is not { } port)
                    return null; // привязки порта нет — endpoint-факта нет

                // State.Running: PortBindings персистят и у остановленного
                // контейнера — Running отличает «жив» от «есть, но остановлен»
                // (vwk-семантика).
                var running = body.TryGetProperty("State", out var state)
                              && state.ValueKind == JsonValueKind.Object
                              && state.TryGetProperty("Running", out var isRunning)
                              && isRunning.ValueKind == JsonValueKind.True;
                return new DockerNodeEndpoint(port, running);
            }
            catch (DockerHttpException e) when (e.StatusCode == 404)
            {
                // Контейнера нет: swarm-фолбэк — только на swarm-движке (hostAlias
                // null; на plain-хосте /tasks даёт 503 not-a-swarm-manager).
                // Движок один на endpoint — фолбэк выполняется лишь после plain-404.
                return hostAlias is null ? await InspectSwarmTaskEndpointAsync(name, ct) : null;
            }
        });

    // swarm-фолбэк: published-порт и хост running-таска сервиса. Один
    // ListTasks — порт и TaskHost из одного снимка таска (ревью Ф7-3).
    // Сервиса/таска нет → null (факта нет).
    private async Task<DockerNodeEndpoint?> InspectSwarmTaskEndpointAsync(string name, CancellationToken ct)
    {
        var tasks = await ListTasksAsync(name, ct);
        if (!tasks.IsSuccess)
            throw tasks.Error!;
        var running = tasks.Value.FirstOrDefault(t => t.State == "running" && t.PublishedPort > 0);
        return running is null
            ? null
            : new DockerNodeEndpoint(running.PublishedPort!.Value, Running: true, running.Host);
    }

    // "<port>/tcp" → первый HostPort (int).
    private static int? ReadClientHostPort(JsonElement hostConfig, int containerPort)
    {
        if (!hostConfig.TryGetProperty("PortBindings", out var bindings)
            || bindings.ValueKind != JsonValueKind.Object
            || !bindings.TryGetProperty($"{containerPort}/tcp", out var slot)
            || slot.ValueKind != JsonValueKind.Array
            || slot.GetArrayLength() == 0)
            return null;

        var first = slot[0];
        return first.TryGetProperty("HostPort", out var hp)
            && int.TryParse(hp.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var port)
                ? port
                : null;
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
            // RestartPolicy: узлы — unless-stopped (docker сам поднимает после
            // ребута хоста); ephemeral-джобы бэкапов (t02) и TLS-helper'ы (t06)
            // — "no" (перезапуск служебного контейнера docker'ом в обход
            // супервизии воркера запрещён / не нужен).
            ["RestartPolicy"] = new { Name = spec.RestartPolicy ?? "unless-stopped" },
        };
        // volume данных (pg/kfw): VolumeName:VolumeDest.
        if (spec.VolumeName is { Length: > 0 })
            hostConfig["Binds"] = new[] { $"{spec.VolumeName}:{spec.VolumeDest}" };
        // Named volume TLS-секретов (vwk, t06): Binds формата "volume:/path".
        if (spec.Binds is { Count: > 0 })
            hostConfig["Binds"] = spec.Binds.ToArray();
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

        // Джобы бэкапов (t02): tmpfs-квота staging (arch/19 §6) + extra_hosts
        // host.docker.internal:host-gateway (advertised-адресация).
        if (spec.Tmpfs is { Count: > 0 })
            hostConfig["Tmpfs"] = spec.Tmpfs;
        if (spec.ExtraHosts is { Count: > 0 })
            hostConfig["ExtraHosts"] = spec.ExtraHosts;

        var body = new Dictionary<string, object?>
        {
            ["Image"] = spec.Image,
            ["Hostname"] = spec.Hostname,
            ["HostConfig"] = hostConfig,
        };
        // Env — только когда задан (канон-суперсет t07 §7.4: vwk-контейнеры
        // без env не меняются; pg/kfw передают словарь — вкл. пустой).
        if (spec.Env is not null)
            body["Env"] = spec.Env.Select(p => $"{p.Key}={p.Value}").OrderBy(v => v, StringComparer.Ordinal).ToArray();
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
        {
            if (spec.ResetEntrypoint)
            {
                // t03/pg-семантика (флаг ResetEntrypoint супер-спеки t07): сброс
                // ENTRYPOINT образа — inline-команда агента/тест-контейнера
                // выполняется как есть, а не аргументами образного entrypoint
                // (образ pgworker-backup несёт ENTRYPOINT джоба t02).
                body["Entrypoint"] = Array.Empty<string>();
            }

            // Без флага — Cmd как аргументы образного entrypoint (kfw/vwk:
            // pgworker-node / docker-entrypoint.sh).
            body["Cmd"] = cmd;
        }
        // label-пара (канон-суперсет t07 §7.4): ключ — домен (pgworker/
        // kafkaworker/valkeyworker); неполная пара не пишется вовсе.
        if (spec.LabelKey is { Length: > 0 } labelKey && spec.Label is { Length: > 0 } label)
            body["Labels"] = new Dictionary<string, string> { [labelKey] = label };
        return body;
    }

    internal static object BuildServiceBody(ServiceSpec spec)
    {
        var container = new Dictionary<string, object?>
        {
            ["Image"] = spec.Template.Image,
            ["Hostname"] = spec.Template.Hostname,
        };
        if (spec.Template.Env is not null)
            container["Env"] = spec.Template.Env.Select(p => $"{p.Key}={p.Value}").OrderBy(v => v, StringComparer.Ordinal).ToArray();
        if (spec.Template.Cmd is { Count: > 0 } cmd)
        {
            if (spec.Template.ResetEntrypoint)
                container["Entrypoint"] = Array.Empty<string>(); // union-семантика как в BuildContainerBody
            container["Cmd"] = cmd;
        }
        // volume данных (pg/kfw) — Mounts.
        if (spec.Template.VolumeName is { Length: > 0 })
        {
            container["Mounts"] = new[]
            {
                new { Type = "volume", Source = spec.Template.VolumeName, Target = spec.Template.VolumeDest },
            };
        }

        // Binds swarm-шаблона (vwk, t06) — Mounts (формат "volume:/path").
        if (spec.Template.Binds is { Count: > 0 })
        {
            container["Mounts"] = spec.Template.Binds.Select(b => new
            {
                Type = "volume",
                Source = b[..b.IndexOf(':')],
                Target = b[(b.IndexOf(':') + 1)..],
            }).ToArray();
        }

        // label-пара — та же семантика, что в BuildContainerBody.
        if (spec.Template.LabelKey is { Length: > 0 } labelKey && spec.Template.Label is { Length: > 0 } label)
            container["Labels"] = new Dictionary<string, string> { [labelKey] = label };

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

    // Pull образа (POST /images/create): гарантирует наличие образа перед
    // повторным create (pull-fallback §7.2).
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
        // оставляет демону вечно рестартуемого держателя volume. Без label —
        // служебный объект вне перечислений кластера (как сегодня).
        var spec = new ContainerSpec(image, [], name,
            Cmd: ["sleep", "120"],
            Binds: new[] { volume + ":/mnt" },
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

    public ValueTask DisposeAsync()
    {
        httpClient.Dispose();
        return ValueTask.CompletedTask;
    }

    // exec-инстанс из POST /containers/<id>/exec; Detach-поллинг (helper)
    // читает Running/ExitCode из /exec/<id>/json.
    private sealed class ExecDto
    {
        [JsonPropertyName("Id")] public string Id { get; set; } = "";

        [JsonPropertyName("Running")] public bool Running { get; set; }

        // null, пока exec ещё работает (канон Engine API).
        [JsonPropertyName("ExitCode")] public int? ExitCode { get; set; }
    }

    // GET /exec/<id>/json — exit-код синхронной exec-цепочки.
    private sealed class ExecInspectDto
    {
        [JsonPropertyName("ExitCode")] public int? ExitCode { get; set; }
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

    // GET /containers/<id>/json — только поля матчинга усыновления (spec §3.1)
    // + State (runtime-факт джоба бэкапа, t02).
    private sealed class ContainerInspectDto
    {
        [JsonPropertyName("Id")] public string Id { get; set; } = "";

        [JsonPropertyName("Config")] public ContainerConfigDto? Config { get; set; }

        [JsonPropertyName("State")] public ContainerStateDto? State { get; set; }

        [JsonPropertyName("NetworkSettings")] public NetworkSettingsDto? NetworkSettings { get; set; }
    }

    private sealed class ContainerStateDto
    {
        [JsonPropertyName("Running")] public bool? Running { get; set; }

        [JsonPropertyName("ExitCode")] public int? ExitCode { get; set; }

        // t07: RFC3339-момент старта контейнера — бюджет running-джоба.
        [JsonPropertyName("StartedAt")] public string? StartedAt { get; set; }
    }

    private sealed class ContainerConfigDto
    {
        [JsonPropertyName("Hostname")] public string? Hostname { get; set; }

        [JsonPropertyName("Env")] public string[]? Env { get; set; }
    }

    private sealed class NetworkSettingsDto
    {
        [JsonPropertyName("Ports")] public Dictionary<string, List<PortBindingDto>?>? Ports { get; set; }

        [JsonPropertyName("Networks")] public Dictionary<string, NetworkDto>? Networks { get; set; }
    }

    private sealed class NetworkDto
    {
        [JsonPropertyName("Aliases")] public string[]? Aliases { get; set; }
    }

    private sealed class PortBindingDto
    {
        [JsonPropertyName("HostIp")] public string? HostIp { get; set; }

        [JsonPropertyName("HostPort")] public string? HostPort { get; set; }
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
