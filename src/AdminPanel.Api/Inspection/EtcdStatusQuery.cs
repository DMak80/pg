using AdminPanel.Core;
using AdminPanel.Core.Alerting.Rules;
using AdminPanel.Etcd;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Inspection;

// Запрос статуса кластера etcd (arch/03 §1 GET /api/etcd/status).
public sealed record EtcdStatusQuery : IQuery<EtcdStatusDto>;

// Ответ GET /api/etcd/status (arch/03 §2; id — decimal-строки, spec §3.11).
public sealed record EtcdStatusDto(
    IReadOnlyList<EtcdEndpointDto> Endpoints,
    IReadOnlyList<EtcdMemberDto> Members,
    IReadOnlyList<EtcdAlarmDto> Alarms,
    bool QuorumSuspected,
    DateTimeOffset LastRefreshUtc);

public sealed record EtcdEndpointDto(
    string Url,
    bool Reachable,
    double? LatencyMs,
    string? Version,
    long? DbSizeBytes,
    string? LeaderMemberId,
    ulong? RaftTerm,
    IReadOnlyList<string> Errors,
    bool Active);

public sealed record EtcdMemberDto(
    string Id, string? Name, IReadOnlyList<string> PeerUrls, IReadOnlyList<string> ClientUrls, bool IsLeader);

public sealed record EtcdAlarmDto(string MemberId, string Type);

// EtcdStatus → DTO: чистая функция (spec §6.2, §3.14).
public static class EtcdStatusMapper
{
    public static EtcdStatusDto Map(EtcdStatus etcd)
    {
        // Лидер: первый живой endpoint с валидным leader > 0, иначе первый любой не-null.
        var leaderId = etcd.Endpoints
            .FirstOrDefault(e => e.Reachable && e.LeaderMemberId is > 0)?.LeaderMemberId
            ?? etcd.Endpoints.FirstOrDefault(e => e.LeaderMemberId is > 0)?.LeaderMemberId;
        return new EtcdStatusDto(
            [.. etcd.Endpoints.Select(e => new EtcdEndpointDto(
                e.Url,
                e.Reachable,
                e.LatencyMs,
                e.Version,
                e.DbSizeBytes,
                e.LeaderMemberId?.ToString(),
                e.RaftTerm,
                e.Errors,
                e.Url == etcd.ActiveEndpoint))],
            [.. etcd.Members.Select(m => new EtcdMemberDto(
                m.Id.ToString(),
                m.Name,
                m.PeerUrls,
                m.ClientUrls,
                leaderId is not null && m.Id == leaderId))],
            [.. etcd.Alarms.Select(a => new EtcdAlarmDto(
                a.MemberId.ToString(),
                EtcdAlarmRule.AlarmTypeName(a.Type)))],
            etcd.QuorumSuspected,
            etcd.LastRefreshUtc);
    }
}

// Хендлер: store → отказ «снапшота нет» или маппер (spec §3.12).
[InjectAsScoped]
public sealed class EtcdStatusQueryHandler(ISnapshotStore store)
    : IQueryHandler<EtcdStatusQuery, EtcdStatusDto>
{
    public ValueTask<Result<EtcdStatusDto>> Handle(EtcdStatusQuery query, CancellationToken ct)
    {
        var snapshot = store.Current;
        return ValueTask.FromResult(snapshot is null
            ? Result<EtcdStatusDto>.Failed(new InspectionModule.SnapshotNotReadyException())
            : Result<EtcdStatusDto>.Success(EtcdStatusMapper.Map(snapshot.Etcd)));
    }
}
