using System.Diagnostics;
using Microsoft.Extensions.Logging;
using OwnS3.Replication.Inbound;
using OwnS3.Storage;
using OwnS3.Replication.Lww;
using OwnS3.Replication.Transport;

namespace OwnS3.Replication.Fetching;

// Фетчер per-source (глава 07 §7): очередь → (disk-free ≥ DiskFreeMinBytes? нет →
// пауза: fetch_paused{source}=1, поллинг улучшения) → FetchObjectAsync → 200-объект:
// применение штатным xl-коммитом (expectedSha из заголовка, сверка на лету —
// ReplicationChecksumMismatchException); tombstone → ApplyDelete; 404 → ничего.
// Ошибка → бэкофф 1 с → BackoffMaxSec (повтор той же задачи). Parallelism: N воркеров
// над одной очередью (порядок не гарантирован — LWW идемпотентен). Исходы — в
// IOutcomeCounters ПО РЕЗУЛЬТАТУ СВОЕГО применения: Applied → applied, !Applied →
// skipped, Conflict → conflict; успешное тело → fetched.
public sealed class ObjectFetcher
{
    // 250 мс — измерение A15 (глава 07 §10) вскрыло: «сон 5 с» при пустой очереди
    // давал сквозную латентность ~5 с (у порога A15 ≤5 с); 250 мс — ~0.75 с с запасом.
    internal static readonly TimeSpan DiskPausePoll = TimeSpan.FromMilliseconds(250);
    internal static readonly TimeSpan BackoffBase = TimeSpan.FromSeconds(1);

    private readonly TargetRecord _source;
    private readonly FetchQueue _queue;
    private readonly IReplicationTransport _transport;
    private readonly ReplicaApplier _applier;
    private readonly IOutcomeCounters _counters;
    private readonly Func<long> _getFreeDiskBytes;
    private readonly ReplicationOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<ObjectFetcher> _logger;
    private readonly TimeSpan _diskPoll;
    private readonly TimeSpan _backoffBase;
    private volatile bool _paused;
    // Упавшие (не применённые) задачи — НЕограниченная retry-коллекция воркера:
    // источник событие не повторит (курсор ушёл за ACK приёма) — потеря = расхождение
    // до reconcile (до 24 ч). Неограниченность безопасна: ключей на источнике конечное
    // число; bounded-очередь для retry нельзя (TryEnqueue в полную очередь теряет).
    private readonly List<FetchTask> _retryTasks = [];
    private readonly object _retryLock = new();
    // Есть не применённые задачи (для решения о чистом маркере при остановке).

    public ObjectFetcher(TargetRecord source, FetchQueue queue, IReplicationTransport transport,
        ReplicaApplier applier, IOutcomeCounters counters, Func<long> getFreeDiskBytes,
        ReplicationOptions options, TimeProvider timeProvider, ILogger<ObjectFetcher> logger,
        ReplicationMetrics metrics)
        : this(source, queue, transport, applier, counters, getFreeDiskBytes, options, timeProvider, logger,
              metrics, DiskPausePoll, BackoffBase)
    {
    }

    // internal-ctor: тесты задают малые паузы (детерминизм без ожиданий).
    internal ObjectFetcher(TargetRecord source, FetchQueue queue, IReplicationTransport transport,
        ReplicaApplier applier, IOutcomeCounters counters, Func<long> getFreeDiskBytes,
        ReplicationOptions options, TimeProvider timeProvider, ILogger<ObjectFetcher> logger,
        ReplicationMetrics metrics, TimeSpan diskPoll, TimeSpan backoffBase)
    {
        _source = source;
        _queue = queue;
        _transport = transport;
        _applier = applier;
        _counters = counters;
        _getFreeDiskBytes = getFreeDiskBytes;
        _options = options;
        _timeProvider = timeProvider;
        _logger = logger;
        _metrics = metrics;
        _diskPoll = diskPoll;
        _backoffBase = backoffBase;
    }

    private readonly ReplicationMetrics _metrics;

    // Фетчи на паузе (нет места на диске — глава 07 §7).
    public TargetRecord Source => _source;

    public bool Paused => _paused;

    // Есть НЕ применённые ключи (в очереди или retry-коллекции) — признак
    // незавершённой работы при остановке (чистый маркер не ставить, глава 07 §7).
    public bool HasUnapplied
    {
        get
        {
            lock (_retryLock)
                return _queue.Depth > 0 || _retryTasks.Count > 0;
        }
    }

    // Воркеры (Parallelism) над одной очередью — до cancel (graceful, без исключений).
    public async Task RunAsync(CancellationToken ct)
    {
        var workers = Math.Max(1, _options.Parallelism);
        var tasks = Enumerable.Range(0, workers)
            .Select(_ => WorkerLoopAsync(ct))
            .ToList();
        try
        {
            await Task.WhenAll(tasks);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // graceful stop — штатный выход
        }
    }

    // Доработать очередь и retry-коллекцию до пустоты (graceful stop): упавшие
    // задачи РЕТРАЯТСЯ с бэкоффом до исчерпания бюджета вызывающего — drain не
    // бросает задачи с одной попытки (источник их не повторит, глава 07 §7).
    public async Task DrainAsync(CancellationToken ct)
    {
        var backoff = _backoffBase;
        while (!ct.IsCancellationRequested)
        {
            var task = _queue.TryDequeue(out var queued) ? queued : null;
            lock (_retryLock)
            {
                if (task is null && _retryTasks.Count > 0)
                {
                    task = _retryTasks[0];
                    _retryTasks.RemoveAt(0);
                }
            }
            if (task is null)
                return; // всё применено — drain завершён
            var failed = await TryFetchOneAsync(ct, task);
            if (failed)
            {
                lock (_retryLock)
                    _retryTasks.Add(task);
                await Task.Delay(backoff, ct);
                backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2,
                    TimeSpan.FromSeconds(_options.BackoffMaxSec).Ticks));
            }
            else
            {
                backoff = _backoffBase;
            }
        }
    }

    private async Task WorkerLoopAsync(CancellationToken ct)
    {
        var backoff = _backoffBase;
        while (!ct.IsCancellationRequested)
        {
            // Backpressure по месту: ниже watermark — пауза (метрика fetch_paused, §7)
            if (_getFreeDiskBytes() < _options.DiskFreeMinBytes)
            {
                if (!_paused)
                {
                    _paused = true;
                    _metrics.SetFetchPaused(_source.Name, true);
                    _logger.LogWarning("Фетчи {Source} на паузе: свободного места меньше {Min} байт",
                        _source.Name, _options.DiskFreeMinBytes);
                }
                await Task.Delay(_diskPoll, ct);
                continue;
            }
            if (_paused)
            {
                _paused = false;
                _metrics.SetFetchPaused(_source.Name, false);
                _logger.LogInformation("Место освободилось — фетчи {Source} продолжаются", _source.Name);
            }

            // Упавшая задача — из retry-коллекции воркера (НЕ из bounded-очереди:
            // повторная постановка в полную очередь молча теряет задачу)
            FetchTask? task = null;
            lock (_retryLock)
            {
                if (_retryTasks.Count > 0)
                {
                    task = _retryTasks[0];
                    _retryTasks.RemoveAt(0);
                }
            }
            if (task is null && !_queue.TryDequeue(out task))
            {
                // Метрики очереди/лага данных (глава 07 §10): 0 — очередь пуста
                _metrics.SetFetchQueueDepth(_source.Name, 0);
                _metrics.SetFetchLagSeconds(_source.Name, 0);
                await Task.Delay(_diskPoll, ct);
                continue;
            }
            _metrics.SetFetchQueueDepth(_source.Name, _queue.Depth);
            _metrics.SetFetchLagSeconds(_source.Name,
                Math.Max(0, _timeProvider.GetUtcNow().ToUnixTimeMilliseconds() - task!.NotifiedModMs) / 1000.0);

            var failed = await TryFetchOneAsync(ct, task);
            if (failed)
            {
                // Бэкофф эксп: повтор той же задачи из retry-коллекции (неограниченной —
                // не теряется при переполнении bounded-очереди)
                lock (_retryLock)
                    _retryTasks.Add(task);
                await Task.Delay(backoff, ct);
                backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2,
                    TimeSpan.FromSeconds(_options.BackoffMaxSec).Ticks));
            }
            else
            {
                backoff = _backoffBase;
            }
        }
    }

    // Одна попытка фетча/применения; true — ошибка (повтор), false — успех/неустранимое.
    private async Task<bool> TryFetchOneAsync(CancellationToken ct, FetchTask? task = null)
    {
        task ??= _queue.TryDequeue(out var queued) ? queued : null;
        if (task is null)
            return false;
        try
        {
            var result = await _transport.FetchObjectAsync(_source, task.Bucket, task.Key, ct);
            if (result.TombstoneUnixMs is long deleteTime)
            {
                // Tombstone-ответ: ключ удалён у источника (глава 07 §7)
                _applier.ApplyDelete(task.Bucket, task.Key, deleteTime);
                _counters.Applied(_source.Name);
                return false;
            }
            if (result.Body is null)
            {
                // 404: ключ неизвестен — состояние приёмника не меняется
                return false;
            }

            // 200-объект: применение с исходными метаданными; sha-сверка тела на лету —
            // внутри ApplyObjectAsync (expectedSha из заголовка); чтение тела — под
            // троттлингом MaxRateBytes (глава 07 §7; 0 — безлимит)
            await using (result.Body)
            {
                // Троттлинг чтения тела MaxRateBytes байт/с (глава 07 §7; 0 — безлимит)
                Stream body = _options.MaxRateBytes > 0
                    ? new ThrottledStream(result.Body, _options.MaxRateBytes)
                    : result.Body;
                var applyResult = await _applier.ApplyObjectAsync(task.Bucket, task.Key, body,
                    contentLength: result.ContentLength,
                    new ReplicaObjectMetadata(
                        DateTimeOffset.FromUnixTimeMilliseconds(result.ModTimeUnixMs),
                        result.ETag.Trim('"'), result.ContentSha256, result.ContentType,
                        result.UserMetadata),
                    expectedSha256: result.ContentSha256, ct);
                _counters.Fetched(_source.Name);
                if (applyResult.Conflict)
                    _counters.Conflict(_source.Name);
                if (applyResult.Applied)
                    _counters.Applied(_source.Name);
                else
                    _counters.Skipped(_source.Name);
            }
            return false;
        }
        catch (ReplicationChecksumMismatchException ex)
        {
            // sha тела ≠ заголовка — фетч-ошибка → повтор (глава 07 §6)
            _logger.LogWarning(ex, "sha тела фетча {Source}/{Bucket}/{Key} не совпал — повтор",
                _source.Name, task.Bucket, task.Key);
            return true;
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // graceful stop — не ошибка фетча
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Фетч {Source}/{Bucket}/{Key} не удался — повтор по бэкоффу",
                _source.Name, task.Bucket, task.Key);
            return true;
        }
    }

// Троттлинг-поток (глава 07 §7): пропускает байты не быстрее rateLimit байт/с —
// чтение тела фетча не конкурирует с S3-гранью за диск сверх лимита.
internal sealed class ThrottledStream(Stream inner, long rateLimitBytesPerSec) : Stream
{
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
    private long _allowedBytes;

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

    public override int Read(byte[] buffer, int offset, int count)
    {
        var permitted = Reserve(count);
        return inner.Read(buffer, offset, permitted);
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count,
        CancellationToken cancellationToken)
    {
        var permitted = Reserve(count);
        return await inner.ReadAsync(buffer.AsMemory(offset, permitted), cancellationToken);
    }

    // Резерв байтов: лимит пополняется со скоростью rateLimit; чтение сверх
    // накопленного — ожидание (TokenBucket-семантика).
    private int Reserve(int requested)
    {
        if (requested <= 0)
            return 0;
        while (true)
        {
            var available = (long)(_stopwatch.Elapsed.TotalSeconds * rateLimitBytesPerSec);
            if (available > _allowedBytes)
                _allowedBytes = available;
            if (_allowedBytes > 0)
            {
                var take = (int)Math.Min(requested, _allowedBytes);
                _allowedBytes -= take;
                return take;
            }
            Thread.Sleep(10); // гранулярность накопления
        }
    }

    public override void Flush() => inner.Flush();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    protected override void Dispose(bool disposing) => inner.Dispose();
}
}
