using OwnS3.Replication.Cursors;
using OwnS3.Storage;

namespace OwnS3.Replication.Lww;

// LWW-слой применения поверх IReplicaStore + TombstoneStore (t35, глава 07 §7).
// Результат применения — ReplicaApplyResult {Applied, Conflict}: вызывающий
// (inbox-предфильтр / фетчер) инкрементирует счётчики исходов по результату
// СВОЕГО вызова. Событие ConflictDetected (bucket, key, обе версии) — ТОЛЬКО для
// лога (подписчик — координатор): метрику по нему НЕ инкрементировать — аплайер
// один на приёмник, событие без sourceName (мульти-счёт при ≥2 источниках).
public sealed class ReplicaApplier : IDisposable
{
    public readonly record struct ReplicaApplyResult(bool Applied, bool Conflict); // Applied = IncomingWins

    // bucket, key, обе версии (время/ша входящая, время/ша локальная) — только лог.
    public event Action<string, string, long, string, long, string>? ConflictDetected;

    private const int RetryQueueMax = 256;
    private static readonly TimeSpan RetryBase = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan RetryMax = TimeSpan.FromSeconds(60);

    private readonly IReplicaStore _store;
    private readonly TombstoneStore _tombstones;
    private readonly TimeProvider _timeProvider;
    // Сериализация применений (глава 07 §7, R4): фетчер (долгий staging-коммит) и
    // inbox (ApplyDelete из батча) конкурируют — без взаимного исключения DELETE,
    // приземлившийся между ReadKeyState и Commit, воскрешает старую версию и стирает
    // tombstone. Применения строго последовательны.
    private readonly SemaphoreSlim _applyGate = new(1, 1);
    private readonly Queue<(string Bucket, int Attempt, long NextRetryMs)> _retryQueue = new();
    private readonly object _retryLock = new();
    private readonly ITimer? _retryTimer;

    public ReplicaApplier(IReplicaStore store, TombstoneStore tombstones, TimeProvider timeProvider)
    {
        _store = store;
        _tombstones = tombstones;
        _timeProvider = timeProvider;
        // Повторы отложенных бакетных удалений раз в секунду (глава 07 §7);
        // тесты зовут RetryTick вручную — таймер здесь для прод-исполнения
        _retryTimer = timeProvider.CreateTimer(static self => ((ReplicaApplier)self!).RetryTick(), this,
            RetryBase, RetryBase);
    }

    // object-put (после фетча): локальное (запись|tombstone) vs incoming →
    // IncomingWins → применить (перезапись после tombstone: in.mod > tomb.deleteTime →
    // применить и Remove tombstone); Identical/LocalWins → skip; конфликт (равные mod,
    // разный sha) → победитель по sha.
    public async Task<ReplicaApplyResult> ApplyObjectAsync(string bucket, string key, Stream body,
        long contentLength, ReplicaObjectMetadata metadata, string expectedSha256, CancellationToken ct)
    {
        await _applyGate.WaitAsync(ct);
        try
        {
            return await ApplyObjectCoreAsync(bucket, key, body, contentLength, metadata, expectedSha256, ct);
        }
        finally
        {
            _applyGate.Release();
        }
    }

    private async Task<ReplicaApplyResult> ApplyObjectCoreAsync(string bucket, string key, Stream body,
        long contentLength, ReplicaObjectMetadata metadata, string expectedSha256, CancellationToken ct)
    {
        var local = _store.ReadKeyState(bucket, key);
        var tombstone = _tombstones.GetDeleteTime(bucket, key);
        long localMod;
        string localSha;
        if (local is not null)
        {
            localMod = local.ModTime.ToUnixTimeMilliseconds();
            localSha = local.ContentSha256Hex;
        }
        else if (tombstone is not null)
        {
            localMod = tombstone.Value; // локальное знание — tombstone (записи нет)
            localSha = string.Empty;
        }
        else
        {
            // знаний нет — входящее применяется свободно
            return await ApplyAsync(conflict: false);
        }

        var (outcome, conflict) = LwwComparator.Compare(
            metadata.ModTime.ToUnixTimeMilliseconds(), metadata.ContentSha256Hex, localMod, localSha);
        if (conflict)
            ConflictDetected?.Invoke(bucket, key,
                metadata.ModTime.ToUnixTimeMilliseconds(), metadata.ContentSha256Hex, localMod, localSha);
        return outcome == LwwOutcome.IncomingWins
            ? await ApplyAsync(conflict)
            : new ReplicaApplyResult(Applied: false, Conflict: conflict);

        async Task<ReplicaApplyResult> ApplyAsync(bool conflict)
        {
            await _store.ApplyObjectAsync(bucket, key, body, contentLength, metadata, expectedSha256, ct);
            _tombstones.Remove(bucket, key); // запись жива — негативное знание снято
            return new ReplicaApplyResult(Applied: true, Conflict: conflict);
        }
    }

    // object-delete (tombstone-время из события): локальная запись modTime <= deleteTime
    // → DeleteObject + Put(tombstone); записи нет → Put(tombstone) (защита от воскрешения);
    // локальная новее → skip.
    public void ApplyDelete(string bucket, string key, long deleteTimeUnixMs)
    {
        _applyGate.Wait();
        try
        {
            var local = _store.ReadKeyState(bucket, key);
            if (local is not null && local.ModTime.ToUnixTimeMilliseconds() > deleteTimeUnixMs)
                return; // локальная новее — skip
            _store.DeleteObject(bucket, key);
            _tombstones.Put(bucket, key, deleteTimeUnixMs);
        }
        finally
        {
            _applyGate.Release();
        }
    }

    // bucket-create: идемпотентно (есть → skip, первая CreationDate сохраняется).
    public void ApplyBucketCreate(string bucket, long creationDateUnixMs)
    {
        _applyGate.Wait();
        try
        {
            _store.ApplyBucketCreate(bucket, DateTimeOffset.FromUnixTimeMilliseconds(creationDateUnixMs));
        }
        finally
        {
            _applyGate.Release();
        }
    }

    // bucket-delete: пустой → удалить; непустой → отложить (RetryQueue in-memory
    // bounded 256, повтор по бэкоффу 1 с → 60 с — недоставленные объектные события
    // опустошат бакет, глава 07 §7).
    public void ApplyBucketDelete(string bucket)
    {
        _applyGate.Wait();
        try
        {
            _store.ApplyBucketDelete(bucket);
        }
        catch (ObjectStoreException ex) when (ex.Code == ObjectStoreErrorCode.BucketNotEmpty)
        {
            EnqueueBucketRetry(bucket, nextAttempt: 0);
        }
        finally
        {
            _applyGate.Release();
        }
    }

    public void Dispose()
    {
        _retryTimer?.Dispose();
    }

    // Тик ретрая (таймер 1 с; тесты зовут вручную): бакет ещё непуст → бэкофф
    // экспоненциальный 1 с → 60 с (недоставленные объектные события опустошат бакет).
    internal void RetryTick()
    {
        List<(string Bucket, int Attempt)> ready;
        lock (_retryLock)
        {
            ready = [];
            var nowMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
            var count = _retryQueue.Count;
            while (count-- > 0)
            {
                var head = _retryQueue.Peek();
                if (head.NextRetryMs > nowMs)
                    break; // очередь упорядочена по сроку — хвосты позже
                _retryQueue.Dequeue();
                ready.Add((head.Bucket, head.Attempt));
            }
        }
        foreach (var (bucket, attempt) in ready)
        {
            try
            {
                _applyGate.Wait();
                try
                {
                    _store.ApplyBucketDelete(bucket);
                }
                finally
                {
                    _applyGate.Release();
                }
            }
            catch (ObjectStoreException ex) when (ex.Code == ObjectStoreErrorCode.BucketNotEmpty)
            {
                EnqueueBucketRetry(bucket, attempt + 1);
            }
        }
    }

    // Бэкофф: попытка 0 — немедленный ретрай (следующий тик); далее 1,2,4… до 60 с.
    private void EnqueueBucketRetry(string bucket, int nextAttempt)
    {
        lock (_retryLock)
        {
            if (_retryQueue.Count >= RetryQueueMax)
                return; // bounded: не разрастаемся (повторный delete-репликой придёт снова)
            var raw = nextAttempt <= 0
                ? TimeSpan.Zero
                : TimeSpan.FromSeconds(1) * (1 << Math.Min(nextAttempt - 1, 6));
            var delay = TimeSpan.FromTicks(Math.Min(raw.Ticks, RetryMax.Ticks));
            var nextMs = _timeProvider.GetUtcNow().Add(delay).ToUnixTimeMilliseconds();
            _retryQueue.Enqueue((bucket, nextAttempt, nextMs));
        }
    }
}
