using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Inspection;

// Запрос детализации кластера (arch/03 §1; state уже провалидирован эндпоинтом — spec §3.9).
public sealed record ClusterDetailsQuery(string Cluster, string? Owner, BucketState? State)
    : IQuery<ClusterDto>;

// Ответ GET /api/clusters/{cluster} (arch/03 §2): всё сразу, N <= тысяч — грид на клиенте.
public sealed record ClusterDto(
    string Name,
    string? DbName,
    int BucketsCount,
    long? CreatedUnix,
    bool Incomplete,
    string State,
    bool Sharded,
    IReadOnlyList<ShardDto> Shards,
    IReadOnlyList<BucketDto> Buckets,
    IReadOnlyList<HealDto> Heals,
    IReadOnlyList<StandNodeDto> StandNodes);

// arch/03 §2: masterLeaseAlive — семантика lease (arch/02 §1); hosts — multi-host из DsnParser t03.
public sealed record ShardDto(
    string Name,
    string Dsn,
    IReadOnlyList<string> Hosts,
    int? ReplicasDeclared,
    string? MasterAddress,
    bool MasterLeaseAlive,
    IReadOnlyList<NodeDto> Nodes,
    NodeRequestsDto? Requests,
    ShardRuntimeDto? Runtime);

// Плановая нода шарда (arch/02 §9.1); state — raw-строка.
public sealed record NodeDto(string Name, string? State);

// Заявка ресурсов на ноду scope /service/<C>-<X>/request_* (arch/02 §2.2, §9.1).
public sealed record NodeRequestsDto(string Cpu, string Mem, string Disk);

// Канон state кластера (arch/03 §2).
public static class ClusterStates
{
    public static string Name(ClusterState state)
        => state switch
        {
            ClusterState.NotInitialized => "NOT_INITIALIZED",
            ClusterState.ToRemove => "TO_REMOVE", // arch/02 §9.4
            _ => "ACTIVE",
        };
}

// Контракт runtime фиксируется сейчас (фронтенд t08 типизирует сразу), данные — t06 (spec §3.14).
public sealed record ShardRuntimeDto(
    int? StandbiesSync,
    long? SlotsLagMaxBytes,
    IReadOnlyList<string> WalStatusLost,
    IReadOnlyList<string> Subscriptions,
    IReadOnlyList<string> BucketSchemas,
    string? Error);

// state — строка канона статус-ключей (spec §3.8); move/ageSec — null у ACTIVE (spec §3.7).
public sealed record BucketDto(
    int Id,
    string? Owner,
    string State,
    MoveDto? Move,
    long? AgeSec);

public sealed record MoveDto(
    string? Owner,
    string? Target,
    long? StartedUnix,
    long? UpdatedUnix,
    string? Phase,
    string? LastError);

public sealed record HealDto(string Bucket, string? Was, string? Now, string? Reason, long? TsUnix);

// Стендовая топология (arch/02 §2.3): реестр /cluster/nodes/ — глобален для всех кластеров, обычно пуст.
public sealed record StandNodeDto(string Name, string? Address);

// Строки state — верхний регистр канона (arch/02 §2.1); общий источник мапперов и валидации query.
public static class BucketStates
{
    public static string Name(BucketState state)
        => state switch
        {
            BucketState.Syncing => "SYNCING",
            BucketState.Frozen => "FROZEN",
            BucketState.Aborting => "ABORTING",
            BucketState.NotInitialized => "NOT_INITIALIZED",
            _ => "ACTIVE",
        };

    public static bool TryParse(string? text, out BucketState state)
    {
        switch (text)
        {
            case "ACTIVE": state = BucketState.Active; return true;
            case "SYNCING": state = BucketState.Syncing; return true;
            case "FROZEN": state = BucketState.Frozen; return true;
            case "ABORTING": state = BucketState.Aborting; return true;
            case "NOT_INITIALIZED": state = BucketState.NotInitialized; return true;
            default: state = BucketState.Active; return false;
        }
    }
}

// Core → DTO: чистая функция; фильтры buckets, возраст MoveAge, heals по TsUnix desc (spec §3.3, §3.7, §3.9).
public static class ClusterDetailsMapper
{
    public static ClusterDto Map(
        ClusterInfo cluster, long nowUnix, string? owner, BucketState? state,
        IReadOnlyList<StandNode> standNodes, IReadOnlyList<HaScope> haScopes)
    {
        var buckets = cluster.Buckets
            .Where(b => owner is null || b.Owner == owner)
            .Where(b => state is null || b.State == state);
        return new ClusterDto(
            cluster.Name,
            cluster.DbName,
            cluster.BucketsCount,
            cluster.CreatedUnix,
            cluster.Incomplete,
            ClusterStates.Name(cluster.State),
            // sharded — вычисляемое поле отображения (arch/03 §2): false ⟺ ровно 1
            // бакет и не более 1 шарда; признак «тип БД» в etcd не хранится (02 §9.1).
            !(cluster.BucketsCount == 1 && cluster.Shards.Count <= 1),
            [.. cluster.Shards.Select(s =>
            {
                // Заявка шарда — join scope "<C>-<X>" (все три ключа обязательны)
                var requests = haScopes
                    .Where(h => h.Matched && h.Cluster == cluster.Name && h.Shard == s.Name
                        && h.RequestCpu is not null && h.RequestMem is not null && h.RequestDisk is not null)
                    .Select(h => new NodeRequestsDto(h.RequestCpu!, h.RequestMem!, h.RequestDisk!))
                    .FirstOrDefault();
                return new ShardDto(
                    s.Name, s.Dsn, s.DsnHosts, s.ReplicasDeclared, s.MasterAddress, s.MasterLeaseAlive,
                    [.. s.Nodes.Select(n => new NodeDto(n.Name, n.State))],
                    requests,
                    s.Runtime is null ? null : MapRuntime(s.Runtime));
            })],
            [.. buckets.Select(b => new BucketDto(
                b.Id,
                b.Owner,
                BucketStates.Name(b.State),
                b.Move is null ? null : new MoveDto(
                    b.Move.Owner, b.Move.Target, b.Move.StartedUnix, b.Move.UpdatedUnix,
                    b.Move.Phase, b.Move.LastError),
                MoveAge.Seconds(b, nowUnix)))],
            [.. cluster.Heals
                .OrderByDescending(h => h.TsUnix) // журнал: новые сверху; null — в конец (spec §3.3)
                .Select(h => new HealDto(h.Bucket, h.Was, h.Now, h.Reason, h.TsUnix))],
            [.. standNodes.Select(n => new StandNodeDto(n.Name, n.Address))]);
    }

    // Маппинг runtime — по стабильной модели t03; поля arch/03 §2 (spec §3.14).
    public static ShardRuntimeDto MapRuntime(ShardRuntime runtime)
        => new(
            runtime.Standbies.Count(s => s.SyncState is "sync" or "quorum"),
            runtime.Slots.Count == 0 ? null : runtime.Slots.Max(s => s.LagBytes),
            [.. runtime.Slots.Where(s => s.WalStatus == "lost").Select(s => s.SlotName)],
            [.. runtime.Subscriptions.Select(s => s.Name)],
            runtime.BucketSchemas,
            runtime.Error);
}

// Хендлер: 503 «снапшота нет» / 404 «кластер не найден» / маппер (spec §3.10, §3.12).
[InjectAsScoped]
public sealed class ClusterDetailsQueryHandler(ISnapshotStore store, TimeProvider time)
    : IQueryHandler<ClusterDetailsQuery, ClusterDto>
{
    public ValueTask<Result<ClusterDto>> Handle(ClusterDetailsQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        if (snapshot is null)
            return ValueTask.FromResult(Result<ClusterDto>.Failed(
                new InspectionModule.SnapshotNotReadyException()));

        var cluster = snapshot.Clusters.FirstOrDefault(c => c.Name == query.Cluster);
        return ValueTask.FromResult(cluster is null
            ? Result<ClusterDto>.Failed(new InspectionModule.ClusterNotFoundException(query.Cluster))
            : Result<ClusterDto>.Success(ClusterDetailsMapper.Map(
                cluster, time.GetUtcNow().ToUnixTimeSeconds(), query.Owner, query.State, snapshot.StandNodes,
                snapshot.HaScopes)));
    }
}
