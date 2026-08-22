using AdminPanel.Core;
using AdminPanel.Infrastructure;

namespace AdminPanel.Etcd.Client;

// Read-only клиент etcd через HTTP JSON gateway /v3/* (arch/02 §1).
// Методы принимают endpoint явно: выбор/ротация «активного» — задача refresher (arch/02 §4).
// Панель не пишет: put/lease в интерфейсе отсутствуют принципиально.
public interface IEtcdGateway
{
    // Префиксный range: POST /v3/kv/range {"key": b64, "range_end": b64}.
    Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct);

    // POST /v3/maintenance/status — персонально на указанный endpoint (arch/02 §2.4).
    Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct);

    // POST /v3/cluster/member/list.
    Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct);

    // POST /v3/maintenance/alarm.
    Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct);
}

// Данные status-ответа без контекста endpoint (url/latency добавляет refresher; spec §17).
public sealed record EtcdStatusPayload(
    string? Version,
    long? DbSizeBytes,
    ulong? LeaderMemberId,
    ulong? RaftIndex,
    ulong? RaftTerm);
