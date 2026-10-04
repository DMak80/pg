using Shared.Core;

namespace Shared.Etcd.Maintenance;

/// <summary>Получатель выгрузки etcd-слепка во внешнее хранилище (t08, spec §3.4).
/// Вызывается SnapshotJob.TakeAsync сразу после записи локального файла и
/// локальной ретенции — каждый слепок (плановый и внеочередной) уезжает целиком.
/// Контракт: попытка доставить; Result — успех/транзиент-отказ (статус-ключ,
/// ретенция — внутри реализации); сбой НЕ роняет снятие (локальный слепок —
/// истина). Бюджет времени попытки — забота реализации (TimeoutSec).</summary>
public interface ISnapshotSink
{
    Task<Result> ExportAsync(string snapshotFileName, byte[] data, long? revision, CancellationToken ct);
}
