using PgWorker.Core;

namespace PgWorker.Etcd.Client;

// Клиент etcd через HTTP JSON gateway /v3/* (адаптация AdminPanel.Etcd, arch/14 §3).
// Методы принимают endpoint явно: выбор/ротация «активного» — задача цикла App.
// Все мутации /clusters/ — только держателем клэйма и только через txn-compare (spec §4.3).
public interface IEtcdGateway
{
    // Префиксный range: POST /v3/kv/range {"key": b64, "range_end": b64}.
    Task<Result<IReadOnlyList<Kv>>> RangeAsync(string endpoint, string prefix, CancellationToken ct);

    // Точечное чтение: range по ключу с инкрементированным range_end; null = ключа нет.
    Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct);

    // POST /v3/kv/put — запись (lease != null → ключ исчезнет по истечении TTL).
    Task<Result> PutAsync(string endpoint, string key, string value, long? lease, CancellationToken ct);

    // POST /v3/kv/deleterange — точечное (prefix=false) или префиксное удаление.
    Task<Result> DeleteAsync(string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct);

    // POST /v3/kv/txn: compare + success/failure-ветки (put/delete); compare не сошёлся → Succeeded=false.
    Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct);

    // POST /v3/lease/grant → ID нового lease (TTL секунд).
    Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct);

    // POST /v3/lease/revoke — досрочное освобождение lease (ключи под ним удаляются).
    Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct);

    // POST /v3/lease/keepalive — один цикл продления; TTL<=0/ошибка → Failed (lease потерян).
    Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct);

    // POST /v3/snapshot/save — бинарный слепок БД etcd (P12).
    Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct);

    // POST /v3/maintenance/status — текущая ревизия кластера (header.revision).
    Task<Result<long>> StatusAsync(string endpoint, CancellationToken ct);

    // POST /v3/kv/compaction — сжатие истории до указанной ревизии (кластерная операция).
    Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct);

    // POST /v3/maintenance/defragment — дефрагментация БД на конкретной ноде.
    Task<Result> DefragmentAsync(string endpoint, CancellationToken ct);
}

// Цель сравнения в txn-compare.
public enum TxnTarget
{
    Version,
    Value,
    ModRevision,
}

// Предикат сравнения (proto: EQUAL=0, GREATER=1).
public enum TxnPredicate
{
    Equal,
    Greater,
}

// Compare-условие txn: для Value — Arg (plain-строка), для Version/ModRevision — Num.
public sealed record TxnCompare(string Key, TxnTarget Target, TxnPredicate Pred, string Arg, long Num)
{
    // Ключа нет (version==0) — примитив захвата клэймов (spec §4.3).
    public static TxnCompare NotExists(string key)
        => new(key, TxnTarget.Version, TxnPredicate.Equal, string.Empty, 0);

    // Значение ключа равно ожидаемому — примитив конкурентного flip routing (arch/11 §5).
    public static TxnCompare ValueEqual(string key, string expected)
        => new(key, TxnTarget.Value, TxnPredicate.Equal, expected, 0);

    // Ключ не менялся с момента чтения — примитив перезаписи config (spec §4.2).
    public static TxnCompare ModRevisionEqual(string key, long modRevision)
        => new(key, TxnTarget.ModRevision, TxnPredicate.Equal, string.Empty, modRevision);
}

// Операция txn-ветки: put (с lease) либо delete (точечный/префиксный).
public abstract record TxnOp
{
    public sealed record Put(string Key, string Value, long? Lease) : TxnOp;

    public sealed record Delete(string Key, bool Prefix) : TxnOp;
}

// Тело txn: compare-условия + ветки success/failure.
public sealed record TxnRequest(
    IReadOnlyList<TxnCompare> Compare,
    IReadOnlyList<TxnOp> Success,
    IReadOnlyList<TxnOp> Failure)
{
    public static TxnRequest Of(IReadOnlyList<TxnCompare> compare, IReadOnlyList<TxnOp> success)
        => new(compare, success, []);
}

// Итог txn: сошёлся ли compare.
public sealed record TxnResult(bool Succeeded);
