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
    internal static readonly TimeSpan DiskPausePoll = TimeSpan.FromSeconds(5);
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

    public ObjectFetcher(TargetRecord source, FetchQueue queue, IReplicationTransport transport,
        ReplicaApplier applier, IOutcomeCounters counters, Func<long> getFreeDiskBytes,
        ReplicationOptions options, TimeProvider timeProvider, ILogger<ObjectFetcher> logger)
        : this(source, queue, transport, applier, counters, getFreeDiskBytes, options, timeProvider, logger,
              DiskPausePoll, BackoffBase)
    {
    }

    // internal-ctor: тесты задают малые паузы (детерминизм без ожиданий).
    internal ObjectFetcher(TargetRecord source, FetchQueue queue, IReplicationTransport transport,
        ReplicaApplier applier, IOutcomeCounters counters, Func<long> getFreeDiskBytes,
        ReplicationOptions options, TimeProvider timeProvider, ILogger<ObjectFetcher> logger,
        TimeSpan diskPoll, TimeSpan backoffBase)
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
        _diskPoll = diskPoll;
        _backoffBase = backoffBase;
    }

    // Фетчи на паузе (нет места на диске — глава 07 §7).
    public bool Paused => _paused;

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

    // Доработать текущую очередь до пустоты (graceful stop, бюджет — вызывающий).
    public async Task DrainAsync(CancellationToken ct)
    {
        while (_queue.Depth > 0 && !ct.IsCancellationRequested)
            await TryFetchOneAsync(ct);
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
                    _logger.LogWarning("Фетчи {Source} на паузе: свободного места меньше {Min} байт",
                        _source.Name, _options.DiskFreeMinBytes);
                }
                await Task.Delay(_diskPoll, ct);
                continue;
            }
            if (_paused)
            {
                _paused = false;
                _logger.LogInformation("Место освободилось — фетчи {Source} продолжаются", _source.Name);
            }

            if (!_queue.TryDequeue(out var task))
            {
                await Task.Delay(_diskPoll, ct);
                continue;
            }

            var failed = await TryFetchOneAsync(ct, task);
            if (failed)
            {
                // Бэкофф эксп: повтор той же задачи (идемпотентно)
                await Task.Delay(backoff, ct);
                backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2,
                    TimeSpan.FromSeconds(_options.BackoffMaxSec).Ticks));
                _queue.TryEnqueue(task);
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
            // внутри ApplyObjectAsync (expectedSha из заголовка)
            await using (result.Body)
            {
                var applyResult = await _applier.ApplyObjectAsync(task.Bucket, task.Key, result.Body,
                    contentLength: result.Body.Length,
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
}
