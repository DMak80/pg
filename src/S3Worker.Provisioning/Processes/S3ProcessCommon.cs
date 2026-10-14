using System.Globalization;
using System.Text.Json;
using S3Worker.Core.Model;
using Shared.Etcd.Client;

namespace S3Worker.Provisioning.Processes;

/// <summary>Исход процесса (arch/24 §5; порт ProcessOutcome valkey): для цикла
/// ReconcileLoop.</summary>
public enum ProcessOutcome
{
    /// <summary>Продолжить следующими тиками (ждём заявок панели, подъёма ноды).</summary>
    InProgress,

    /// <summary>Цель процесса достигнута.</summary>
    Done,
}

/// <summary>Опции процессов S3Worker (плоский срез S3WorkerOptions, порт
/// ValkeyProvisioningOptions; arch/23 §2/§4, arch/24 §8).</summary>
public sealed record S3ProvisioningOptions(
    int PortRangeFrom,
    int PortRangeTo,
    int NodeBootSec,
    int NodeDeadSec,
    int HealthIntervalSec,
    string? AdvertisedHost,
    string OwnS3Image,
    string Bucket,
    string? ContourNetwork = null)
{
    public static S3ProvisioningOptions Default { get; } =
        new(18000, 18999, 100, 90, 15, null, "owns3:dev", "pgworker-backups");
}

/// <summary>
/// Общие хелперы процессов S3Worker (порт ProcessCommon valkey): ключи etcd
/// домена /s3/ и координации /s3worker/ (arch/23 §8), сериализация portalloc,
/// чтение/запись state ноды (failover). Дополняется задачами 10–13
/// (ParseResources и др. — Task 9).
/// </summary>
public static class S3ProcessCommon
{
    public static string ConfigKey(string cluster)
        => $"/s3/clusters/{cluster}/config";

    public static string EndpointsKey(string cluster)
        => $"/s3/clusters/{cluster}/endpoints";

    public static string NodeStateKey(string cluster, string node)
        => $"/s3/clusters/{cluster}/nodes/{node}/state";

    public static string NodePlacementKey(string cluster, string node)
        => $"/s3/clusters/{cluster}/nodes/{node}/placement";

    public static string NodeResourcesKey(string cluster, string node)
        => $"/s3/clusters/{cluster}/nodes/{node}/resources";

    public static string NodePlacedOnKey(string cluster, string node)
        => $"/s3/clusters/{cluster}/nodes/{node}/placed_on";

    public static string NodeUsageKey(string cluster, string node)
        => $"/s3/clusters/{cluster}/nodes/{node}/usage";

    // Декларации mesh (arch/23 §2): граф репликации и отзыв ключей кластера.
    public static string LinksKey(string cluster)
        => $"/s3/clusters/{cluster}/links";

    public static string DenyListKey(string cluster)
        => $"/s3/clusters/{cluster}/denylist";

    // PKI репликации per-cluster (arch/23 §2/§6): CA, сертификат оркестратора,
    // материал инстанса (ленивый выпуск, txn put-if-absent).
    public static string ClusterPkiCaKey(string cluster)
        => $"/s3/clusters/{cluster}/pki/ca";

    public static string OrchestratorPkiKey(string cluster)
        => $"/s3/clusters/{cluster}/pki/orchestrator";

    public static string NodePkiKey(string cluster, string node)
        => $"/s3/clusters/{cluster}/nodes/{node}/pki";

    public static string PortAllocKey(string cluster)
        => $"/s3worker/portalloc/{cluster}";

    public static string RotationKey(string cluster)
        => $"/s3worker/rotations/{cluster}";

    public static string WorkKey(string cluster)
        => $"/s3worker/work/{cluster}";

    public static string ClusterPrefix(string cluster)
        => $"/s3/clusters/{cluster}/";

    // Первый адрес endpoints "h1:p1,h2:p2,…" → (host, port); порт без числа
    // (или без адреса) — канон-дефолт 9000 (пробы бессмысленны без порта).
    public static (string Host, int Port) ParseEndpoint(string endpoints)
    {
        var first = endpoints.Split(',', StringSplitOptions.TrimEntries)[0];
        var separator = first.LastIndexOf(':');
        return separator > 0 && int.TryParse(first[(separator + 1)..],
                   NumberStyles.Integer, CultureInfo.InvariantCulture, out var port)
            ? (first[..separator], port)
            : (first, 9000);
    }

    // Текущий state ноды (null — ключа нет).
    public static async Task<Result<string?>> ReadNodeStateAsync(
        IEtcdGateway gateway, string[] endpoints, string cluster, string node, CancellationToken ct)
    {
        Result<Kv?> last = default;
        foreach (var endpoint in endpoints)
        {
            var read = await gateway.GetAsync(endpoint, NodeStateKey(cluster, node), ct);
            if (!read.IsSuccess)
            {
                last = read;
                continue;
            }

            var value = read.Value?.Value;
            return Result<string?>.Success(string.IsNullOrWhiteSpace(value) ? null : value.Trim());
        }

        return Result<string?>.Failed(last.Error ?? new EtcdUnreachableException("etcd endpoints не заданы"));
    }

    // Запись state ноды (failover по endpoints).
    public static async Task<Result> WriteNodeStateAsync(
        IEtcdGateway gateway, string[] endpoints, string cluster, string node, string state, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in endpoints)
        {
            var put = await gateway.PutAsync(endpoint, NodeStateKey(cluster, node), state, null, ct);
            if (put.IsSuccess)
                return put;
            last = put;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints не заданы");
    }

    // Claimed-check: мутации — только держателем живого клэйма (arch/17 S5).
    public static Result EnsureClaimed(ClaimStore claims, string cluster, string op)
        => claims.IsMine(cluster)
            ? Result.Success()
            : Result.Failed(new ApplicationException(
                $"{op} {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

    /// <summary>
    /// Завершающее окно D/E (spec §2.2, arch/23 §2 после правки): RMW
    /// endpoints (txn по mod_revision) + актуализация config.nodes до
    /// фактического состава (txn по mod_revision, канонический JSON без
    /// state). Идемпотентность: endpoints уже канон и nodes уже равен
    /// составу — no-op (повторные тики не переписывают).
    /// </summary>
    public static async Task<Result> ConvergeTopologyAsync(
        IEtcdGateway gateway,
        string[] endpoints,
        string cluster,
        string platform,
        string? secretProfile,
        long? createdUnix,
        IReadOnlyDictionary<string, NodeAddress> addresses,
        CancellationToken ct)
    {
        // endpoints: advertised host:port Ordinal-списком (arch/23 §2.1).
        var endpointsValue = string.Join(",", addresses
            .OrderBy(p => p.Key, StringComparer.Ordinal)
            .Select(p => $"{p.Value.Host}:{p.Value.S3Port}"));
        var endpointsResult = await RmwKeyAsync(
            gateway, endpoints, EndpointsKey(cluster), endpointsValue, ct);
        if (!endpointsResult.IsSuccess)
            return endpointsResult;

        // config.nodes: канонический JSON Active с фактическим числом нод;
        // чужая state (TO_REMOVE) сохраняется — актуализация не снимает заявки.
        var configKey = ConfigKey(cluster);
        var current = await GetWithFailoverAsync(gateway, endpoints, configKey, ct);
        if (!current.IsSuccess)
            return current.Error!;
        if (current.Value is not { } kv)
            return Result.Success(); // ключа нет (внешняя очистка) — не наш случай

        string? state = null;
        try
        {
            using var doc = JsonDocument.Parse(kv.Value);
            if (doc.RootElement.TryGetProperty("state", out var st))
                state = st.GetString();
        }
        catch (JsonException)
        {
            // битый config — актуализировать нечем, тик повторит после починки
            return Result.Failed(new ApplicationException($"config {configKey}: битый JSON"));
        }

        var canonical = SerializeCanonicalConfig(platform, addresses.Count, secretProfile, createdUnix, state);
        if (kv.Value == canonical)
            return Result.Success(); // уже актуален (no-op)

        return await RmwKeyAsync(gateway, endpoints, configKey, canonical, ct);
    }

    /// <summary>Канонический JSON config (arch/23 §2.1, порядок полей канона;
    /// state — опционально для заявочных форм).</summary>
    public static string SerializeCanonicalConfig(
        string platform, int nodes, string? secretProfile, long? createdUnix, string? state = null)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteString("platform", platform);
            writer.WriteNumber("nodes", nodes);
            writer.WriteString("secret_profile", secretProfile);
            if (createdUnix is { } created)
                writer.WriteNumber("created_unix", created);
            if (state is { Length: > 0 })
                writer.WriteString("state", state);
            writer.WriteEndObject();
        }

        return System.Text.Encoding.UTF8.GetString(buffer.ToArray());
    }

    // RMW ключа: txn compare mod_revision (ключ отсутствует — NotExists).
    private static async Task<Result> RmwKeyAsync(
        IEtcdGateway gateway, string[] endpoints, string key, string value, CancellationToken ct)
    {
        var current = await GetWithFailoverAsync(gateway, endpoints, key, ct);
        if (!current.IsSuccess)
            return current.Error!;

        if (current.Value is { } kv && kv.Value == value)
            return Result.Success(); // уже канон (no-op)

        var compare = current.Value is { } existing
            ? TxnCompare.ModRevisionEqual(key, (long)existing.ModRevision)
            : TxnCompare.NotExists(key);
        Result<TxnResult> last = default;
        foreach (var endpoint in endpoints)
        {
            var txn = await gateway.TxnAsync(endpoint, TxnRequest.Of([compare], [new TxnOp.Put(key, value, null)]), ct);
            if (!txn.IsSuccess)
            {
                last = txn;
                continue;
            }

            return txn.Value.Succeeded
                ? Result.Success()
                : Result.Failed(new ApplicationException(
                    $"{key} изменился под нами — ретрай тиком"));
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints не заданы");
    }

    // Failover-чтение ключа (первый успешный endpoint).
    public static async Task<Result<Kv?>> GetWithFailoverAsync(
        IEtcdGateway gateway, string[] endpoints, string key, CancellationToken ct)
    {
        Result<Kv?> last = default;
        foreach (var endpoint in endpoints)
        {
            var read = await gateway.GetAsync(endpoint, key, ct);
            if (read.IsSuccess)
                return read;
            last = read;
        }

        return last.Error ?? new EtcdUnreachableException("etcd endpoints не заданы");
    }

    /// <summary>
    /// Ресурсы декларации → лимиты контейнера (cpu/mem; disk — декларация, не
    /// парсится — канон arch/23 §2). cpu — decimal-строка инвариантной культуры
    /// ("2", "0.5"; БЕЗ суффиксов); mem — "&lt;n&gt;Gi" → n × 2^30; незнакомый
    /// формат → null (валидация API гарантирует канон на входе, толерантность
    /// чтения сохраняется).
    /// </summary>
    public static (decimal? Cpu, long? MemBytes)? ParseResources(S3NodeResources? r)
    {
        if (r is null)
            return null;

        decimal? cpu = null;
        long? mem = null;
        if (r.Cpu is { } cpuRaw
            && decimal.TryParse(cpuRaw.Trim(), NumberStyles.Number, CultureInfo.InvariantCulture, out var cpuValue))
            cpu = cpuValue;
        if (r.Mem is { } memRaw
            && memRaw.Trim().EndsWith("Gi", StringComparison.Ordinal)
            && int.TryParse(memRaw.Trim()[..^2], NumberStyles.Integer, CultureInfo.InvariantCulture, out var gi))
            mem = gi * 1024L * 1024 * 1024;

        return (cpu, mem);
    }

    // Формат arch/23 §8: {"node<k>":{"host":"h","s3":18001}}.
    public static Dictionary<string, NodeAddress> ParsePortAlloc(string json)
    {
        var addresses = new Dictionary<string, NodeAddress>();
        using var doc = JsonDocument.Parse(json);
        foreach (var node in doc.RootElement.EnumerateObject())
            addresses[node.Name] = new NodeAddress(
                node.Value.GetProperty("host").GetString()!,
                node.Value.GetProperty("s3").GetInt32());
        return addresses;
    }

    // Канонический JSON links (arch/23 §2.1): [{"a":…,"b":…},…] в порядке запроса.
    public static string LinksJson(IReadOnlyList<(string A, string B)> links)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            foreach (var (a, b) in links)
            {
                writer.WriteStartObject();
                writer.WriteString("a", a);
                writer.WriteString("b", b);
                writer.WriteEndObject();
            }

            writer.WriteEndArray();
        }

        return System.Text.Encoding.UTF8.GetString(buffer.ToArray());
    }

    // Канонический JSON denylist (arch/23 §2.1): ["<I>",…] в Ordinal-порядке.
    public static string DenyListJson(IReadOnlyCollection<string> names)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            foreach (var name in names.OrderBy(n => n, StringComparer.Ordinal))
                writer.WriteStringValue(name);
            writer.WriteEndArray();
        }

        return System.Text.Encoding.UTF8.GetString(buffer.ToArray());
    }

    public static string SerializePortAlloc(IReadOnlyDictionary<string, NodeAddress> addresses)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            foreach (var (node, addr) in addresses.OrderBy(p => p.Key, StringComparer.Ordinal))
            {
                writer.WritePropertyName(node);
                writer.WriteStartObject();
                writer.WriteString("host", addr.Host);
                writer.WriteNumber("s3", addr.S3Port);
                writer.WriteEndObject();
            }

            writer.WriteEndObject();
        }

        return System.Text.Encoding.UTF8.GetString(buffer.ToArray());
    }
}
