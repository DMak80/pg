using S3Worker.Provisioning.Processes;
using Shared.Core;
using Shared.Etcd.Client;

namespace S3Worker.App.Api.Operations;

// DELETE /api/s3/clusters/{c}: config нет → 404; уже TO_REMOVE → 204;
// иначе RMW txn (mod_revision) put config с "state":"TO_REMOVE" → 202.
public sealed class DeleteClusterHandler(IEtcdGateway gateway, string[] endpoints)
{
    public async Task<Result<bool>> HandleAsync(string cluster, CancellationToken ct)
    {
        var current = await S3ApiHelpers.GetConfigAsync(gateway, endpoints, cluster, ct);
        if (!current.IsSuccess)
            return Result<bool>.Failed(current.Error!);
        if (current.Value is null)
            return Result<bool>.Failed(new S3ClusterNotFoundException(cluster));
        if (current.Value.Value.Contains("\"TO_REMOVE\"", StringComparison.Ordinal))
            return Result<bool>.Success(false); // already — 204

        // Канонический config с сохранением полей + state=TO_REMOVE (spec §6).
        var state = "TO_REMOVE";
        var canonical = ParseAndUpdate(current.Value.Value, cluster, state);
        var key = S3ProcessCommon.ConfigKey(cluster);
        var txn = await S3ApiHelpers.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.ModRevisionEqual(key, (long)current.Value.ModRevision)],
                [new TxnOp.Put(key, canonical, null)]),
            ct));
        if (!txn.IsSuccess)
            return Result<bool>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<bool>.Failed(new S3ConcurrentWriteException(key));
        return Result<bool>.Success(true); // 202
    }

    // config → канонический JSON arch/23 §2.1 с "state"=state; `etcd_backup` — признак бэкапного кластера etcd.
    private static string ParseAndUpdate(string json, string cluster, string state)
    {
        string? platform = S3Limits.Platform;
        int nodes = 1;
        bool etcdBackup = false;
        long? created = null;
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(json);
            var root = doc.RootElement;
            if (root.TryGetProperty("platform", out var p))
                platform = p.GetString();
            if (root.TryGetProperty("nodes", out var n))
                nodes = n.GetInt32();
            if (root.TryGetProperty("etcd_backup", out var e) && e.ValueKind == System.Text.Json.JsonValueKind.True)
                etcdBackup = true;
            if (root.TryGetProperty("created_unix", out var c))
                created = c.GetInt64();
        }
        catch (System.Text.Json.JsonException)
        {
            throw new InvalidS3ConfigException(cluster);
        }

        return S3ProcessCommon.SerializeCanonicalConfig(platform!, nodes, etcdBackup, created, state);
    }
}

// Битый config в etcd — 503.
public sealed class InvalidS3ConfigException(string cluster)
    : Exception($"config of S3 cluster {cluster} is unreadable (malformed JSON)");

// POST /api/s3/clusters/{c}/nodes: имя инстанса из тела (arch/24 §1.1) —
// гварды (Active; паттерн/резерв; <9) → txn NotExists(nodes/<I>/state) + пакет
// placement/resources/state → 201; проигрыш txn → 409 (коллизия имени).
public sealed class AddNodeHandler(IEtcdGateway gateway, string[] endpoints)
{
    public async Task<Result<string>> HandleAsync(string cluster, AddNodeRequest request, CancellationToken ct)
    {
        // Валидация (400): имя паттерна + не резерв; ресурсы; placement.
        var errors = new List<ValidationError>();
        if (request.Name is null || !S3Limits.InstanceNamePattern().IsMatch(request.Name))
            errors.Add(new("name", "name: [a-z][a-z0-9_]{0,62} (cluster pattern)"));
        else if (string.Equals(request.Name, S3Limits.ReservedInstanceName, StringComparison.Ordinal))
            errors.Add(new("name", $"«{S3Limits.ReservedInstanceName}» is reserved (orchestrator PKI)"));
        S3ClusterValidator.ValidateResources(request.Resources, errors);
        if (request.Placement is null || request.Placement.IsEmpty)
            errors.Add(new("placement", "placement: at least one attribute (dockerHost|swarmNode|swarmLabels)"));
        if (errors.Count > 0)
            return Result<string>.Failed(new S3ValidationException(errors));

        var config = await S3ApiHelpers.GetConfigAsync(gateway, endpoints, cluster, ct);
        if (!config.IsSuccess)
            return Result<string>.Failed(config.Error!);
        if (config.Value is null)
            return Result<string>.Failed(new S3ClusterNotFoundException(cluster));
        if (config.Value.Value.Contains("\"state\"", StringComparison.Ordinal))
            return Result<string>.Failed(new S3ClusterNotActiveException(cluster, "NOT_INITIALIZED/TO_REMOVE"));

        var names = await S3ApiHelpers.ListNodeNamesAsync(gateway, endpoints, cluster, ct);
        if (!names.IsSuccess)
            return Result<string>.Failed(names.Error!);
        if (names.Value.Count >= S3Limits.MaxNodes)
            return Result<string>.Failed(new S3LastNodeException(
                $"S3 cluster {cluster}: already {names.Value.Count} instances — limit {S3Limits.MaxNodes} (arch/23 §2)"));

        var node = request.Name!;
        var ops = new List<TxnOp>
        {
            new TxnOp.Put(S3ApiHelpers.NodeKey(cluster, node, "placement"), request.Placement!.ToJson(), null),
            new TxnOp.Put(S3ApiHelpers.NodeKey(cluster, node, "resources"), S3Limits.ResourcesJson(request.Resources ?? new(null, null, null)), null),
            new TxnOp.Put(S3ApiHelpers.NodeKey(cluster, node, "state"), "NOT_INITIALIZED", null),
        };
        var baseKey = S3ApiHelpers.NodeKey(cluster, node, "state");
        var txn = await S3ApiHelpers.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of([TxnCompare.NotExists(baseKey)], ops),
            ct));
        if (!txn.IsSuccess)
            return Result<string>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<string>.Failed(new S3NodeAlreadyExistsException(cluster, node));
        return Result<string>.Success(node);
    }
}

// DELETE /api/s3/clusters/{c}/nodes/{n}: инстанса нет → 404; already TO_REMOVE →
// 204; последний инстанс → 409 (0 инстансов валиден только созданием — путь
// к «пустому» кластеру — снос кластера целиком, spec §3.1/§9.3); иначе put
// state=TO_REMOVE → 202.
public sealed class DeleteNodeHandler(IEtcdGateway gateway, string[] endpoints)
{
    public async Task<Result<bool>> HandleAsync(string cluster, string node, CancellationToken ct)
    {
        var config = await S3ApiHelpers.GetConfigAsync(gateway, endpoints, cluster, ct);
        if (!config.IsSuccess)
            return Result<bool>.Failed(config.Error!);
        if (config.Value is null)
            return Result<bool>.Failed(new S3ClusterNotFoundException(cluster));

        var stateKey = S3ApiHelpers.NodeKey(cluster, node, "state");
        var state = await S3ApiHelpers.CallAsync(endpoints, endpoint =>
            gateway.GetAsync(endpoint, stateKey, ct));
        if (!state.IsSuccess)
            return Result<bool>.Failed(state.Error!);
        if (state.Value is null)
            return Result<bool>.Failed(new S3NodeNotFoundException(cluster, node));
        if (state.Value!.Value == "TO_REMOVE")
            return Result<bool>.Success(false); // already — 204

        var names = await S3ApiHelpers.ListNodeNamesAsync(gateway, endpoints, cluster, ct);
        if (!names.IsSuccess)
            return Result<bool>.Failed(names.Error!);
        if (names.Value.Count <= 1)
            return Result<bool>.Failed(new S3LastNodeException(
                $"S3 cluster {cluster}: removing the last instance is forbidden — 0 instances is valid " +
                "only via creation; the path to a cluster without instances is deleting the whole cluster"));

        var put = await S3ApiHelpers.CallAsync<bool>(endpoints, async endpoint =>
        {
            var write = await gateway.PutAsync(endpoint, stateKey, "TO_REMOVE", null, ct);
            return write.IsSuccess ? Result<bool>.Success(true) : Result<bool>.Failed(write.Error!);
        });
        if (!put.IsSuccess)
            return Result<bool>.Failed(put.Error!);
        return Result<bool>.Success(true); // 202
    }
}

// PUT /api/s3/clusters/{c}/nodes/{n}/resources: RMW resources-ключа → 200
// (применит надзор C rolling'ом; disk — декларация).
public sealed class UpdateNodeResourcesHandler(IEtcdGateway gateway, string[] endpoints)
{
    public async Task<Result<S3ResourcesJson>> HandleAsync(
        string cluster, string node, UpdateResourcesRequest request, CancellationToken ct)
    {
        var errors = new List<ValidationError>();
        S3ClusterValidator.ValidateResources(
            new S3ResourcesDto(request.Cpu, request.MemGi, request.DiskGi), errors);
        if (errors.Count > 0)
            return Result<S3ResourcesJson>.Failed(new S3ValidationException(errors));

        var resourcesKey = S3ApiHelpers.NodeKey(cluster, node, "resources");
        var current = await S3ApiHelpers.CallAsync(endpoints, endpoint =>
            gateway.GetAsync(endpoint, resourcesKey, ct));
        if (!current.IsSuccess)
            return Result<S3ResourcesJson>.Failed(current.Error!);
        if (current.Value is null)
            return Result<S3ResourcesJson>.Failed(new S3NodeNotFoundException(cluster, node));

        // RMW: частичная мутация — только указанные поля, прочие из факта.
        string cpu = S3Limits.CanonicalCpu(request.Cpu ?? 1m);
        string mem = $"{request.MemGi ?? S3Limits.MinGiB}Gi";
        string disk = $"{request.DiskGi ?? 10}Gi";
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(current.Value.Value);
            var root = doc.RootElement;
            if (request.Cpu is null && root.TryGetProperty("cpu", out var c))
                cpu = c.GetString() ?? cpu;
            if (request.MemGi is null && root.TryGetProperty("mem", out var m))
                mem = m.GetString() ?? mem;
            if (request.DiskGi is null && root.TryGetProperty("disk", out var d))
                disk = d.GetString() ?? disk;
        }
        catch (System.Text.Json.JsonException)
        {
            // битый факт — перезапишем указанными значениями
        }

        var canonical = System.Text.Json.JsonSerializer.Serialize(new S3ResourcesJson(cpu, mem, disk));
        var txn = await S3ApiHelpers.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.ModRevisionEqual(resourcesKey, (long)current.Value.ModRevision)],
                [new TxnOp.Put(resourcesKey, canonical, null)]),
            ct));
        if (!txn.IsSuccess)
            return Result<S3ResourcesJson>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<S3ResourcesJson>.Failed(new S3ConcurrentWriteException(resourcesKey));
        return Result<S3ResourcesJson>.Success(new S3ResourcesJson(cpu, mem, disk));
    }
}

// POST /api/s3/clusters/{c}/rotations: txn [version(rotations/<C>)==0][put
// {requested_unix, requested_by}] → 201; повтор (проигранный compare) → 200
// идемпотентно; кластера нет → 404.
public sealed class PostRotationHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    public async Task<Result<bool>> HandleAsync(string cluster, string requestedBy, CancellationToken ct)
    {
        var config = await S3ApiHelpers.GetConfigAsync(gateway, endpoints, cluster, ct);
        if (!config.IsSuccess)
            return Result<bool>.Failed(config.Error!);
        if (config.Value is null)
            return Result<bool>.Failed(new S3ClusterNotFoundException(cluster));

        var key = S3ApiHelpers.RotationKey(cluster);
        var value = System.Text.Json.JsonSerializer.Serialize(
            new RotationRequest(time.GetUtcNow().ToUnixTimeSeconds(), requestedBy));
        var txn = await S3ApiHelpers.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of([TxnCompare.NotExists(key)], [new TxnOp.Put(key, value, null)]),
            ct));
        if (!txn.IsSuccess)
            return Result<bool>.Failed(txn.Error!);
        return Result<bool>.Success(txn.Value.Succeeded); // true → 201, false → 200
    }

    private sealed record RotationRequest(
        [property: System.Text.Json.Serialization.JsonPropertyName("requested_unix")] long RequestedUnix,
        [property: System.Text.Json.Serialization.JsonPropertyName("requested_by")] string RequestedBy);
}

// POST /api/s3/clusters/{c}/sync-probe: 202-заявка /s3worker/probes/<C>
// put-if-absent (arch/24 §1.1, arch/23 §8); already → 202 идемпотентно
// (заявка первого заявителя не перезаписывается); кластера нет → 404;
// state=NOT_INITIALIZED/TO_REMOVE → 409; результат — ключи arch/23 §3
// (исполняет зонд F ближайшим тиком держателя клэйма).
public sealed class SyncProbeHandler(IEtcdGateway gateway, string[] endpoints, TimeProvider time)
{
    public async Task<Result<bool>> HandleAsync(string cluster, string requestedBy, CancellationToken ct)
    {
        var config = await S3ApiHelpers.GetConfigAsync(gateway, endpoints, cluster, ct);
        if (!config.IsSuccess)
            return Result<bool>.Failed(config.Error!);
        if (config.Value is null)
            return Result<bool>.Failed(new S3ClusterNotFoundException(cluster));
        if (config.Value.Value.Contains("\"state\"", StringComparison.Ordinal))
            return Result<bool>.Failed(new S3ClusterNotActiveException(cluster, "NOT_INITIALIZED/TO_REMOVE"));

        var key = S3ProcessCommon.ProbeRequestKey(cluster);
        var value = System.Text.Json.JsonSerializer.Serialize(
            new ProbeRequest(time.GetUtcNow().ToUnixTimeSeconds(), requestedBy));
        var txn = await S3ApiHelpers.CallAsync(endpoints, endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of([TxnCompare.NotExists(key)], [new TxnOp.Put(key, value, null)]),
            ct));
        if (!txn.IsSuccess)
            return Result<bool>.Failed(txn.Error!);
        return Result<bool>.Success(txn.Value.Succeeded); // true → заявка поставлена; false → уже жива
    }

    private sealed record ProbeRequest(
        [property: System.Text.Json.Serialization.JsonPropertyName("requested_unix")] long RequestedUnix,
        [property: System.Text.Json.Serialization.JsonPropertyName("requested_by")] string RequestedBy);
}
