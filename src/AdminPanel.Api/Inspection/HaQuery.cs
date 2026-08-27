using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Inspection;

// Запрос сводного списка HA-скопов (arch/03 §1).
public sealed record HaScopesQuery : IQuery<IReadOnlyList<HaScopeSummaryDto>>;

// Сводка скопа — UI-таблица HA (03 §3; spec §3.17): агрегаты по членам.
public sealed record HaScopeSummaryDto(
    string Scope,
    string? Cluster,
    string? Shard,
    bool Matched,
    string? LeaderName,
    int MembersTotal,
    int MembersHealthy,
    long? LagMaxBytes);

// Запрос деталей HA-скопа (arch/03 §1).
public sealed record HaScopeDetailsQuery(string Scope) : IQuery<HaScopeDto>;

// Детали скопа — arch/03 §2 HaScopeDto дословно (spec §3.18); Initialized модели
// в контракт 03 §2 не входит и не отдаётся.
public sealed record HaScopeDto(
    string Scope,
    string? Cluster,
    string? Shard,
    bool Matched,
    string? LeaderName,
    long? OptimeLeader,
    NodeRequestsDto? Requests,
    IReadOnlyList<HaMemberDto> Members,
    string? RawConfig);

public sealed record HaMemberDto(
    string Name,
    string Host,
    int? Port,
    string? Role,
    string? State,
    long? Timeline,
    long? LagBytes,
    DateTimeOffset? ProbeAtUtc,
    string? ProbeError,
    string? NodeState);

// Core → DTO: чистые функции; порядок — как в снапшоте (парсер Scope Ordinal, t03).
public static class HaMappers
{
    public static IReadOnlyList<HaScopeSummaryDto> MapSummaries(IReadOnlyList<HaScope> scopes)
        => [.. scopes.Select(scope => new HaScopeSummaryDto(
            scope.Scope,
            scope.Cluster,
            scope.Shard,
            scope.Matched,
            scope.LeaderName,
            scope.Members.Count,
            scope.Members.Count(m => m.State is "running" or "streaming"),
            scope.Members.Any(m => m.LagBytes is not null) ? scope.Members.Max(m => m.LagBytes) : null))];

    public static HaScopeDto MapDetails(HaScope scope)
        => new(
            scope.Scope,
            scope.Cluster,
            scope.Shard,
            scope.Matched,
            scope.LeaderName,
            scope.OptimeLeader,
            // Заявка есть только при всех трёх ключах request_* (arch/02 §9.1)
            scope.RequestCpu is null || scope.RequestMem is null || scope.RequestDisk is null
                ? null
                : new NodeRequestsDto(scope.RequestCpu, scope.RequestMem, scope.RequestDisk),
            [.. scope.Members.Select(m => new HaMemberDto(
                m.Name, m.Host, m.Port, m.Role, m.State, m.Timeline, m.LagBytes, m.ProbeAtUtc, m.ProbeError, m.NodeState))],
            scope.RawConfig);
}

[InjectAsScoped]
public sealed class HaScopesQueryHandler(ISnapshotStore store)
    : IQueryHandler<HaScopesQuery, IReadOnlyList<HaScopeSummaryDto>>
{
    public ValueTask<Result<IReadOnlyList<HaScopeSummaryDto>>> Handle(HaScopesQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        return ValueTask.FromResult(snapshot is null
            ? Result<IReadOnlyList<HaScopeSummaryDto>>.Failed(new InspectionModule.SnapshotNotReadyException())
            : Result<IReadOnlyList<HaScopeSummaryDto>>.Success(HaMappers.MapSummaries(snapshot.HaScopes)));
    }
}

// Хендлер деталей: 503 «снапшота нет» / 404 «скоп не найден» (spec §3.18).
[InjectAsScoped]
public sealed class HaScopeDetailsQueryHandler(ISnapshotStore store)
    : IQueryHandler<HaScopeDetailsQuery, HaScopeDto>
{
    public ValueTask<Result<HaScopeDto>> Handle(HaScopeDetailsQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        if (snapshot is null)
            return ValueTask.FromResult(Result<HaScopeDto>.Failed(new InspectionModule.SnapshotNotReadyException()));

        var scope = snapshot.HaScopes.FirstOrDefault(s => s.Scope == query.Scope);
        return ValueTask.FromResult(scope is null
            ? Result<HaScopeDto>.Failed(new InspectionModule.ScopeNotFoundException(query.Scope))
            : Result<HaScopeDto>.Success(HaMappers.MapDetails(scope)));
    }
}
