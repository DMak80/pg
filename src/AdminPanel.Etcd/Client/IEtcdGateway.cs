using AdminPanel.Core;
using AdminPanel.Infrastructure;

namespace AdminPanel.Etcd.Client;

// Read-only клиент etcd через HTTP JSON gateway /v3/* (arch/02 §1).
// Методы принимают endpoint явно: выбор/ротация «активного» — задача refresher (arch/02 §4).
// Единственная запись — создание кластера (§9).
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

    // POST /v3/kv/txn: compare + success-puts. compare не сошёлся → Succeeded=false (arch/02 §9.2).
    Task<Result<TxnResult>> TxnAsync(
        string endpoint, IReadOnlyList<TxnCompare> compares, IReadOnlyList<KvPut> puts, CancellationToken ct);

    // POST /v3/kv/put — одиночная запись (пакет создания кластера, arch/02 §9.2).
    Task<Result> PutAsync(string endpoint, string key, string value, CancellationToken ct);

    // POST /v3/kv/deleterange — точечное (prefix=false) или префиксное удаление (компенсация, arch/02 §9.2).
    Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct);
}

// Compare-условие txn: версия ключа (0 = ключа нет).
public sealed record TxnCompare(string Key, long Version);

// Один put внутри txn либо самостоятельный.
public sealed record KvPut(string Key, string Value);

// Итог txn: сошёлся ли compare.
public sealed record TxnResult(bool Succeeded);

// Данные status-ответа без контекста endpoint (url/latency добавляет refresher; spec §17).
public sealed record EtcdStatusPayload(
    string? Version,
    long? DbSizeBytes,
    ulong? LeaderMemberId,
    ulong? RaftIndex,
    ulong? RaftTerm);
