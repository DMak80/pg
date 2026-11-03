using Shared.Core;
using Shared.Etcd.Client;

namespace Shared.Etcd.Coordination;

// Дельта применённых изменений (arch/17 S10): вставленные/обновлённые ключи
// (ChangedKeys), удалённые (RemovedKeys) и revision события.
public sealed record EtcdWatchedCacheChange(
    IReadOnlyList<string> ChangedKeys,
    IReadOnlyList<string> RemovedKeys,
    long Revision);

// Кэш дельт etcd-префикса (arch/17 S10): первичная загрузка — один полный
// Range через EtcdEndpointFailover (единая точка failover S7), далее — только
// изменения: watch-поток с start_revision = последний применённый + 1; события
// применяются к кэшу строго по порядку revision (PUT — вставить/обновить,
// DELETE — удалить; устаревшее событие ≤ последнего применённого — пропуск).
// Обрыв потока/ошибка → переподключение к СЛЕДУЮЩЕМУ endpoint (ротация S7)
// с паузой ≤5 с; compacted → повторная первичная загрузка (кэш пересбрасывается
// целиком) и watch заново. Потребитель: Snapshot (потокобезопасный слепок),
// Changed (дельта), StartAsync/StopAsync. Тик потребителя работает от кэша,
// не от etcd.
public sealed class EtcdWatchedCache
{
    private readonly string _prefix;
    private readonly IEtcdGateway _gateway;
    private readonly IEtcdWatchApi _watch;
    private readonly string[] _endpoints;
    private readonly TimeProvider _clock;
    private readonly object _lock = new();
    private readonly Dictionary<string, (string Value, long ModRevision)> _cache = [];

    private CancellationTokenSource? _cts;
    private Task? _loop;
    private long _appliedRevision;
    private int _endpointIndex;

    /// <summary>Пауза между попытками reconnect (≤5 с); переопределяется тестами.</summary>
    internal TimeSpan ReconnectDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Дельта применённых изменений (изменённые/удалённые ключи + revision);
    /// сбой обработчика не роняет watch-цикл (пишется в лог).</summary>
    public event Action<EtcdWatchedCacheChange>? Changed;

    /// <summary>Потокобезопасный слепок кэша: ключ → (значение, mod_revision).</summary>
    public IReadOnlyDictionary<string, (string Value, long ModRevision)> Snapshot
    {
        get
        {
            lock (_lock)
                return new Dictionary<string, (string Value, long ModRevision)>(_cache);
        }
    }

    public EtcdWatchedCache(string prefix, IEtcdGateway gateway, string[] endpoints, TimeProvider? clock = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(prefix);
        ArgumentNullException.ThrowIfNull(gateway);
        ArgumentNullException.ThrowIfNull(endpoints);
        if (endpoints.Length == 0)
            throw new ArgumentException("etcd endpoints are not set", nameof(endpoints));
        _prefix = prefix;
        _gateway = gateway;
        _watch = gateway as IEtcdWatchApi
            ?? throw new ArgumentException("gateway не поддерживает watch-стрим (IEtcdWatchApi)", nameof(gateway));
        _endpoints = endpoints;
        _clock = clock ?? TimeProvider.System;
    }

    // Старт: ПЕРВИЧНАЯ ЗАГРУЗКА полного префикса (единая точка failover S7),
    // затем фоновый watch-цикл. Ошибка загрузки — наружу (старт не маскируем).
    public async Task StartAsync(CancellationToken ct)
    {
        if (_cts is not null)
            throw new InvalidOperationException("etcd watched cache already started");
        var range = await LoadRangeAsync(ct);
        range.Throw();
        ReplaceCache(range.Value);
        _cts = new CancellationTokenSource();
        _loop = Task.Run(() => WatchLoopAsync(_cts.Token));
    }

    // Штатный останов: отмена watch-цикла и ожидание его завершения.
    public async Task StopAsync(CancellationToken ct = default)
    {
        var cts = Interlocked.Exchange(ref _cts, null);
        if (cts is null)
            return;
        var loop = _loop;
        cts.Cancel();
        if (loop is not null)
            await loop.WaitAsync(ct);
        cts.Dispose();
    }

    // Полный Range префикса через единую точку failover (arch/17 S7).
    private Task<Result<IReadOnlyList<Kv>>> LoadRangeAsync(CancellationToken ct)
        => EtcdEndpointFailover.CallAsync(
            _endpoints,
            (endpoint, token) => _gateway.RangeAsync(endpoint, _prefix, token),
            ct,
            startIndex: Volatile.Read(ref _endpointIndex));

    // Полная пересборка кэша из Range-ответа (первичная загрузка / compacted-reset).
    private void ReplaceCache(IReadOnlyList<Kv> kvs)
    {
        lock (_lock)
        {
            _cache.Clear();
            foreach (var kv in kvs)
                _cache[kv.Key] = (kv.Value, (long)kv.ModRevision);
            _appliedRevision = kvs.Count == 0 ? 0 : kvs.Max(kv => (long)kv.ModRevision);
        }

        Console.WriteLine($"etcd-watch: первичная загрузка {_prefix}: ключей {kvs.Count}, revision {AppliedRevision}");
    }

    // Последний применённый revision (первичная загрузка либо событие).
    private long AppliedRevision
    {
        get
        {
            lock (_lock)
                return _appliedRevision;
        }
    }

    // Watch-цикл: разрыв/ошибка → следующий endpoint (ротация S7) после паузы;
    // compacted → полная перезагрузка и watch заново с того же endpoint.
    private async Task WatchLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            WatchNext next;
            try
            {
                next = await WatchOnceAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e)
            {
                Console.WriteLine($"etcd-watch: {_prefix} сбой цикла: {e.Message} — переподключение");
                next = WatchNext.Rotate;
            }

            if (next == WatchNext.Exit)
                return;
            if (next == WatchNext.Rotate)
            {
                Interlocked.Increment(ref _endpointIndex);
                try
                {
                    await Task.Delay(ReconnectDelay, _clock, ct);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    // Одна попытка watch с последнего применённого revision + 1.
    private async Task<WatchNext> WatchOnceAsync(CancellationToken ct)
    {
        var index = Volatile.Read(ref _endpointIndex) % _endpoints.Length;
        var endpoint = _endpoints[index];
        var start = AppliedRevision + 1;
        var attempt = await _watch.WatchAsync(endpoint, _prefix, start, ApplyEventAsync, ct);

        if (attempt.IsSuccess)
        {
            switch (attempt.Value.Reason)
            {
                case EtcdWatchStopReason.Cancelled:
                    return WatchNext.Exit; // StopAsync — штатное завершение
                case EtcdWatchStopReason.Compacted:
                    // История недоступна: кэш пересбрасывается целиком, watch заново.
                    Console.WriteLine(
                        $"etcd-watch: {_prefix} compacted (revision {attempt.Value.CompactRevision}) — повторная первичная загрузка");
                    var reload = await LoadRangeAsync(ct);
                    if (reload.IsSuccess)
                    {
                        ReplaceCache(reload.Value);
                        return WatchNext.SameEndpoint;
                    }

                    Console.WriteLine($"etcd-watch: {_prefix} перезагрузка не удалась: {reload.Error!.Message}");
                    return WatchNext.Rotate;
                case EtcdWatchStopReason.Closed:
                    Console.WriteLine($"etcd-watch: {_prefix} поток разорван ({endpoint}) — переподключение");
                    return WatchNext.Rotate;
            }
        }

        Console.WriteLine($"etcd-watch: {_prefix} ошибка watch ({endpoint}): {attempt.Error!.Message} — переподключение");
        return WatchNext.Rotate;
    }

    private enum WatchNext
    {
        Exit,
        SameEndpoint,
        Rotate,
    }

    // Применение события к кэшу строго по порядку revision (S10): устаревшее
    // (≤ последнего применённого — дубликат после reconnect) — пропускается.
    private Task ApplyEventAsync(EtcdWatchEvent e)
    {
        List<string> changed = [];
        List<string> removed = [];
        lock (_lock)
        {
            if (e.ModRevision <= _appliedRevision)
                return Task.CompletedTask;
            if (e.Type == EtcdWatchEventType.Put)
            {
                _cache[e.Key] = (e.Value ?? string.Empty, e.ModRevision);
                changed.Add(e.Key);
            }
            else
            {
                _cache.Remove(e.Key);
                removed.Add(e.Key);
            }

            _appliedRevision = e.ModRevision;
        }

        RaiseChanged(new EtcdWatchedCacheChange(changed, removed, e.ModRevision));
        return Task.CompletedTask;
    }

    // Обработчик потребителя не должен ронять watch-цикл: сбой — в лог.
    private void RaiseChanged(EtcdWatchedCacheChange change)
    {
        if (Changed is not { } handlers)
            return;
        try
        {
            handlers.Invoke(change);
        }
        catch (Exception e)
        {
            Console.WriteLine($"etcd-watch: {_prefix} обработчик Changed упал: {e.Message}");
        }
    }
}
