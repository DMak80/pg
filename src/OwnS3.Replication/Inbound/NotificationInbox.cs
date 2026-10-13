using OwnS3.Replication.Transport;
using OwnS3.Storage;

namespace OwnS3.Replication.Inbound;

// Точки инкремента счётчиков исходов приёма (глава 07 §10) — единый контракт
// inbox'а и фетчера. Реализация: no-op-заглушка (до wiring'а координатора Task 12)
// и боевой ReplicationMetrics (серии applied_total{source,result}/fetched/conflicts).
public interface IOutcomeCounters
{
    void Applied(string sourceName);    // каждое применённое событие (самодостаточные inbox'а И apply-ветка фетчера)
    void Skipped(string sourceName);    // LWW-предфильтр-skip inbox'а (вкл. дедуп-повторы) И skip-ветка фетчера
    void Conflict(string sourceName);   // флаг конфликта LWW: компаратор предфильтра (inbox) И Conflict*-исходы применения (фетчер)
    void Fetched(string sourceName);    // успешный фетч тела объекта (только фетчер)
}

// No-op-заглушка: стоит в inbox'е до wiring'а боевых метрик (координатор, Task 12).
public sealed class NullOutcomeCounters : IOutcomeCounters
{
    public static readonly NullOutcomeCounters Instance = new();

    public void Applied(string sourceName) { }
    public void Skipped(string sourceName) { }
    public void Conflict(string sourceName) { }
    public void Fetched(string sourceName) { }
}

// Приёмник уведомлений (t35, глава 07 §6): дедуп по (sourceName, seqNo) — per-source
// lastAccepted in-memory (повторная рассылка источника гасится; безопасно — применение
// идемпотентно, LWW-предфильтр); самодостаточные события применяются ReplicaApplier'ом;
// object-put — LWW-предфильтр: локально (запись|tombstone) не старее → skip, старее/нет
// → fetch-очередь источника; очередь полна → null (429). Исходы — в IOutcomeCounters:
// applied — каждое применённое самодостаточное; skipped — предфильтр-skip (вкл. дедуп);
// conflict — флаг компаратора предфильтра. Точки работают и в WAF-конфигурации
// с фиктивным соседом (без фетча) — на них опираются ассерты счётчиков Task 14.
public sealed class NotificationInbox(FetchQueueRegistry queues, OwnS3.Replication.Lww.ReplicaApplier applier,
    IReplicaStore store, OwnS3.Replication.Lww.TombstoneStore tombstones, IOutcomeCounters counters)
{
    // null — 429 (приёмный буфер переполнен); иначе acceptedToSeqNo = последний
    // поставленный (применённый|в очередь) seqNo батча; повторы ≤ lastAccepted —
    // skip без применения (повторный ACK).
    public long? Handle(string sourceName, NotificationBatchRequest batch)
    {
        lock (_lock)
        {
            var lastAccepted = _lastAccepted.TryGetValue(sourceName, out var known) ? known : -1;
            foreach (var e in batch.Events)
            {
                if (e.SeqNo <= lastAccepted)
                {
                    counters.Skipped(sourceName); // дедуп-повтор — skip без применения
                    continue;
                }
                switch (e.Type)
                {
                    case ReplicationEventType.ObjectPut:
                        if (!HandleObjectPut(sourceName, e))
                            return null; // fetch-очередь полна — 429
                        break;
                    case ReplicationEventType.ObjectDelete:
                        applier.ApplyDelete(e.Bucket, e.Key, e.ModTimeUnixMs);
                        counters.Applied(sourceName);
                        break;
                    case ReplicationEventType.BucketCreate:
                        applier.ApplyBucketCreate(e.Bucket, e.ModTimeUnixMs);
                        counters.Applied(sourceName);
                        break;
                    case ReplicationEventType.BucketDelete:
                        applier.ApplyBucketDelete(e.Bucket);
                        counters.Applied(sourceName);
                        break;
                    default:
                        break; // неизвестный тип — пропускаем (совместимость версий)
                }
                lastAccepted = e.SeqNo;
                _lastAccepted[sourceName] = e.SeqNo;
            }
            return lastAccepted;
        }
    }

    // LWW-предфильтр object-put (глава 07 §3 «самодостаточность»): локально не старее
    // → событие закрыто без фетча (skip); старее/нет → fetch-очередь источника.
    private bool HandleObjectPut(string sourceName, ReplicationEvent e)
    {
        // мост t38: демонтаж в задаче перевода ReplicaApplier (Failed — Throw(),
        // поведение прежнее)
        var localLookup = store.ReadKeyState(e.Bucket, e.Key);
        var local = localLookup.IsSuccess
            ? localLookup.Value is ReplicaKeyLookup.Found f ? f.State : null
            : throw localLookup.Error!;
        var tombstone = tombstones.GetDeleteTime(e.Bucket, e.Key);
        if (local is not null || tombstone is not null)
        {
            var localMod = local?.ModTime.ToUnixTimeMilliseconds() ?? tombstone!.Value;
            var localSha = local?.ContentSha256Hex ?? string.Empty;
            var (outcome, conflict) = Lww.LwwComparator.Compare(e.ModTimeUnixMs, e.ContentSha256,
                localMod, localSha);
            if (conflict)
                counters.Conflict(sourceName);
            if (outcome != Lww.LwwOutcome.IncomingWins)
            {
                counters.Skipped(sourceName);
                return true;
            }
        }
        // старее/нет → фетч данных у источника (применение — работа фетчера, Task 12)
        return queues.ForSource(sourceName).TryEnqueue(new FetchTask(e.Bucket, e.Key, e.ModTimeUnixMs));
    }

    private readonly object _lock = new();
    private readonly Dictionary<string, long> _lastAccepted = new(StringComparer.Ordinal);
}
