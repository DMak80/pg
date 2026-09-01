using System.Text.Json;
using System.Text.RegularExpressions;
using PgWorker.Core;
using PgWorker.Core.Writing;
using PgWorker.Etcd.Client;

namespace PgWorker.App.Api.Operations;

// Ответ 201 POST /api/ha/{scope}/nodes/{node}/recreate (дубль панельного DTO осознан, t08).
public sealed record NodeRecreatedDto(string Scope, string Node, string State, string Mode);

// Тело POST /api/ha/{scope}/nodes/{node}/recreate (mode опционален: soft).
public sealed record RecreateNodeRequest(string? Mode);

// Пересоздание ноды через API воркера (task etcd-via-worker-api): ставит маркер
// nodes/<n>/state=TO_RECREATE + режим nodes/<n>/recreate=soft|hard; rebuild
// выполнит NodeSupervisor (soft — switchover живого лидера; hard — снос сразу).
// Порт панельного RecreateNodeCommandHandler: скоп/ноды читаются напрямую etcd
// (панель брала снапшот HaScopes/Clusters); requested_by не участвует (ключи
// маркеров его не содержат — заголовок X-Requested-By игнорируется).
public sealed partial class RecreateNodeHandler(IEtcdGateway gateway, string[] endpoints)
{
    public const string ToRecreateState = "TO_RECREATE";

    // Scope = "<cluster>-<shard>" — дефис разрешён; node = "shard1a" — без дефиса.
    // Имена кластера/шарда без дефисов (§9.3) — первый '-' однозначно разделяет.
    [GeneratedRegex("^[a-z][a-z0-9_-]{0,62}$")]
    private static partial Regex ScopePattern();

    [GeneratedRegex("^[a-z][a-z0-9_]{0,30}$")]
    private static partial Regex NodePattern();

    public async Task<Result<NodeRecreatedDto>> HandleAsync(
        string scope, string node, string? mode, CancellationToken ct)
    {
        // 1) Канонические имена.
        if (!ScopePattern().IsMatch(scope) || !NodePattern().IsMatch(node))
            return Result<NodeRecreatedDto>.Failed(new ScopeNotFoundException(scope));

        // 2) Скоп существует (ключи /service/<scope>/ живы — образец панельных
        //    HaScopes из снапшота) → cluster/shard из имени скопа.
        var dash = scope.IndexOf('-');
        var (cluster, shard) = (scope[..dash], scope[(dash + 1)..]);
        var serviceRange = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.RangeAsync(endpoint, $"/service/{scope}/", ct));
        if (!serviceRange.IsSuccess)
            return Result<NodeRecreatedDto>.Failed(serviceRange.Error!);
        if (serviceRange.Value.Count == 0)
            return Result<NodeRecreatedDto>.Failed(new ScopeNotFoundException(scope));

        // 3) Guard-данные кластера: config → 404/409; ноды декларации шарда.
        var data = await ClusterGuardData.ReadAsync(gateway, endpoints, cluster, ct);
        if (!data.IsSuccess)
            return Result<NodeRecreatedDto>.Failed(data.Error!);
        var info = data.Value;
        if (info.ConfigRaw is null)
            return Result<NodeRecreatedDto>.Failed(new ScopeNotFoundException(scope));
        string? state;
        try
        {
            state = ReadState(info.ConfigRaw);
        }
        catch (JsonException)
        {
            return Result<NodeRecreatedDto>.Failed(new InvalidClusterConfigException(cluster));
        }
        if (state is not null)
            return Result<NodeRecreatedDto>.Failed(new ClusterNotActiveException(cluster, state));

        // 4) Нода существует в декларации шарда.
        var prefix = $"{shard}/";
        var nodes = info.NodeStates.Where(n => n.Key.StartsWith(prefix, StringComparison.Ordinal))
            .Select(n => (Name: n.Key[prefix.Length..], n.Value))
            .ToList();
        if (!info.Shards.Contains(shard) || nodes.All(n => n.Name != node))
            return Result<NodeRecreatedDto>.Failed(new NodeNotFoundException(scope, node));

        // 5) Guard: не последняя нода и не все остальные в процессе пересоздания.
        var recreatingStates = new HashSet<string>(StringComparer.Ordinal) { "REBUILDING", "TO_RECREATE" };
        var others = nodes.Where(n => n.Name != node).ToList();
        if (others.Count == 0)
            return Result<NodeRecreatedDto>.Failed(new LastNodeException(scope, node));
        var othersRecreating = others.All(n => n.Value is not null && recreatingStates.Contains(n.Value));
        if (othersRecreating)
            return Result<NodeRecreatedDto>.Failed(new AllOthersRecreatingException(scope, node));

        // 6) Идемпотентность + смена режима: уже TO_RECREATE → режим всё равно
        //    (пере)записываем — оператор может передумать soft↔hard на висящем
        //    маркере; state не трогаем (REBUILDING-переходы — дело воркера).
        var markerKey = $"/clusters/{cluster}/shards/{shard}/nodes/{node}/state";
        var modeKey = $"/clusters/{cluster}/shards/{shard}/nodes/{node}/recreate";
        var normalizedMode = mode is null or "" ? "soft" : mode.Trim().ToLowerInvariant();
        if (normalizedMode is not ("soft" or "hard"))
            return Result<NodeRecreatedDto>.Failed(new InvalidRecreateModeException(mode!));

        var putMode = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.PutAsync(endpoint, modeKey, normalizedMode, null, ct));
        if (!putMode.IsSuccess)
            return Result<NodeRecreatedDto>.Failed(putMode.Error!);

        var marker = await ReadKeyAsync(markerKey, ct);
        if (!marker.IsSuccess)
            return Result<NodeRecreatedDto>.Failed(marker.Error!);
        if (marker.Value == ToRecreateState)
            return Result<NodeRecreatedDto>.Success(new NodeRecreatedDto(scope, node, ToRecreateState, normalizedMode));

        // 7) PUT маркера.
        var put = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.PutAsync(endpoint, markerKey, ToRecreateState, null, ct));
        if (!put.IsSuccess)
            return Result<NodeRecreatedDto>.Failed(put.Error!);
        return Result<NodeRecreatedDto>.Success(new NodeRecreatedDto(scope, node, ToRecreateState, normalizedMode));
    }

    private async Task<Result<string?>> ReadKeyAsync(string key, CancellationToken ct)
    {
        var range = await EtcdFailover.CallAsync(endpoints,
            endpoint => gateway.RangeAsync(endpoint, key, ct));
        if (!range.IsSuccess)
            return Result<string?>.Failed(range.Error!);
        return Result<string?>.Success(range.Value.FirstOrDefault(kv => kv.Key == key)?.Value);
    }

    private static string? ReadState(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        return doc.RootElement.TryGetProperty("state", out var s) && s.ValueKind == JsonValueKind.String
            ? s.GetString()
            : null;
    }
}
