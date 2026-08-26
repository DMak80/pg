using System.Text.Json;
using System.Text.RegularExpressions;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Writing;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// Команда пересоздания ноды — шестая мутация панели: ставит маркер
// nodes/<n>/state=TO_RECREATE + режим nodes/<n>/recreate=soft|hard.
// NodeSupervisor PgWorker выполнит rebuild: soft — живой лидер сначала
// переезжает switchover'ом; hard — снос сразу, failover делает Patroni.
public sealed record RecreateNodeCommand(string Scope, string Node, string? Mode = null) : ICommand<NodeRecreatedDto>;

public sealed record NodeRecreatedDto(string Scope, string Node, string State, string Mode);

// Неизвестный режим пересоздания (допустимы только soft|hard).
public sealed class InvalidRecreateModeException(string mode)
    : Exception($"режим пересоздания «{mode}» недопустим: только soft или hard");

// Тело POST /api/ha/{scope}/nodes/{node}/recreate (mode опционален: soft).
public sealed record RecreateNodeRequest(string? Mode);

public sealed class ScopeNotFoundException(string scope)
    : Exception($"HA-скоп {scope} не найден");

public sealed class NodeNotFoundException(string scope, string node)
    : Exception($"нода {node} не найдена в скопе {scope}");

// Последняя живая нода: пересоздание невозможно (нет источника для basebackup).
public sealed class LastNodeException(string scope, string node)
    : Exception($"нода {node} — последняя в скопе {scope}, пересоздание невозможно")
{
    public string Node { get; } = node;
}

// Все остальные ноды уже в процессе пересоздания (REBUILDING/TO_RECREATE).
public sealed class AllOthersRecreatingException(string scope, string node)
    : Exception($"все остальные ноды скопа {scope} уже пересоздаются — дождитесь завершения")
{
    public string Node { get; } = node;
}

[InjectAsScoped]
public sealed partial class RecreateNodeCommandHandler(ISnapshotStore store, IEtcdGateway gateway)
    : ICommandHandler<RecreateNodeCommand, NodeRecreatedDto>
{
    public const string ToRecreateState = "TO_RECREATE";

    // Scope = "<cluster>-<shard>" — дефис разрешён; node = "shard1a" — без дефиса.
    [GeneratedRegex("^[a-z][a-z0-9_-]{0,62}$")]
    private static partial Regex ScopePattern();

    [GeneratedRegex("^[a-z][a-z0-9_]{0,30}$")]
    private static partial Regex NodePattern();

    public async ValueTask<Result<NodeRecreatedDto>> Handle(RecreateNodeCommand command, CancellationToken ct)
    {
        var (scope, node) = (command.Scope, command.Node);

        // 1) Канонические имена.
        if (!ScopePattern().IsMatch(scope) || !NodePattern().IsMatch(node))
            return Result<NodeRecreatedDto>.Failed(new ScopeNotFoundException(scope));

        // 2) Активный endpoint.
        var snapshot = store.Current;
        if (snapshot?.Etcd.ActiveEndpoint is not { } endpoint)
            return Result<NodeRecreatedDto>.Failed(new EtcdWriteUnavailableException());

        // 3) Скоп существует и matched → cluster/shard.
        var haScope = snapshot.HaScopes.FirstOrDefault(s => s.Scope == scope);
        if (haScope is null)
            return Result<NodeRecreatedDto>.Failed(new ScopeNotFoundException(scope));
        if (haScope.Cluster is null || haScope.Shard is null)
            return Result<NodeRecreatedDto>.Failed(new ScopeNotFoundException(scope));

        var cluster = haScope.Cluster;
        var shard = haScope.Shard;

        // 4) Config напрямую: нет → 404; не Active → 409.
        var config = await ReadKeyAsync(endpoint, $"/clusters/{cluster}/config", ct);
        if (!config.IsSuccess)
            return Result<NodeRecreatedDto>.Failed(config.Error!);
        if (config.Value is null)
            return Result<NodeRecreatedDto>.Failed(new ClusterNotFoundException(cluster));
        string? state;
        try
        {
            state = ReadState(config.Value);
        }
        catch (JsonException)
        {
            return Result<NodeRecreatedDto>.Failed(new InvalidClusterConfigException(cluster));
        }
        if (state is not null)
            return Result<NodeRecreatedDto>.Failed(new ClusterNotActiveException(cluster, state));

        // 5) Нода существует в декларации шарда.
        var shardInfo = snapshot.Clusters.FirstOrDefault(c => c.Name == cluster)
            ?.Shards.FirstOrDefault(s => s.Name == shard);
        if (shardInfo is null)
            return Result<NodeRecreatedDto>.Failed(new ScopeNotFoundException(scope));
        var nodeInfo = shardInfo.Nodes.FirstOrDefault(n => n.Name == node);
        if (nodeInfo is null)
            return Result<NodeRecreatedDto>.Failed(new NodeNotFoundException(scope, node));

        // 6) Guard: не последняя нода и не все остальные в процессе пересоздания.
        var recreatingStates = new HashSet<string>(StringComparer.Ordinal) { "REBUILDING", "TO_RECREATE" };
        var others = shardInfo.Nodes.Where(n => n.Name != node).ToList();
        if (others.Count == 0)
            return Result<NodeRecreatedDto>.Failed(new LastNodeException(scope, node));
        var othersRecreating = others.All(n => n.State is not null && recreatingStates.Contains(n.State));
        if (othersRecreating)
            return Result<NodeRecreatedDto>.Failed(new AllOthersRecreatingException(scope, node));

        // 7) Идемпотентность + смена режима: уже TO_RECREATE → режим всё равно
        // (пере)записываем — оператор может передумать soft↔hard на висящем
        // маркере; state не трогаем (REBUILDING-переходы — дело воркера).
        var markerKey = $"/clusters/{cluster}/shards/{shard}/nodes/{node}/state";
        var modeKey = $"/clusters/{cluster}/shards/{shard}/nodes/{node}/recreate";
        var mode = command.Mode is null or "" ? "soft" : command.Mode.Trim().ToLowerInvariant();
        if (mode is not ("soft" or "hard"))
            return Result<NodeRecreatedDto>.Failed(new InvalidRecreateModeException(command.Mode!));

        var putMode = await gateway.PutAsync(endpoint, modeKey, mode, ct);
        if (!putMode.IsSuccess)
            return Result<NodeRecreatedDto>.Failed(putMode.Error!);

        var marker = await ReadKeyAsync(endpoint, markerKey, ct);
        if (!marker.IsSuccess)
            return Result<NodeRecreatedDto>.Failed(marker.Error!);
        if (marker.Value == ToRecreateState)
            return Result<NodeRecreatedDto>.Success(new NodeRecreatedDto(scope, node, ToRecreateState, mode));

        // 8) PUT маркера.
        var put = await gateway.PutAsync(endpoint, markerKey, ToRecreateState, ct);
        if (!put.IsSuccess)
            return Result<NodeRecreatedDto>.Failed(put.Error!);
        return Result<NodeRecreatedDto>.Success(new NodeRecreatedDto(scope, node, ToRecreateState, mode));
    }

    private async Task<Result<string?>> ReadKeyAsync(string endpoint, string key, CancellationToken ct)
    {
        var range = await gateway.RangeAsync(endpoint, key, ct);
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
