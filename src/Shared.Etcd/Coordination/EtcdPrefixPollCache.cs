using Shared.Core;
using Shared.Etcd.Client;

namespace Shared.Etcd.Coordination;

// Дельта различий опроса (arch/synchronization/05-delta-application.md §S10): добавленные/изменённые ключи
// (ChangedKeys), удалённые (RemovedKeys — видны только сравнением с кэшем,
// range-дельта существующие ключи не отдаёт) и revision опроса
// (max mod_revision прочитанного, 0 — префикс пуст).
public sealed record EtcdPrefixPollChange(
    IReadOnlyList<string> ChangedKeys,
    IReadOnlyList<string> RemovedKeys,
    long Revision);

// Опросный кэш etcd-префикса (arch/synchronization/05-delta-application.md §S10): периодический Range префикса
// (единая точка failover S7 — EtcdEndpointFailover) + локальный кэш последнего
// прочитанного состояния (ключ → значение + mod_revision) + сравнение
// прочитанного с кэшем. Различия → кэш обновляется и поднимается Changed
// (дельта одной пачкой на опрос); различий нет — события нет, потребитель
// ничего не делает. Удаления обнаруживаются сравнением с кэшем. Первичная
// загрузка (StartAsync) кэш заполняет БЕЗ события — потребитель читает
// Snapshot. Потребитель: Snapshot (потокобезопасный слепок), Changed (дельта),
// StartAsync/StopAsync; тик потребителя работает от кэша, не от etcd.
public sealed class EtcdPrefixPollCache
{
    /// <summary>Верхняя граница шага опроса (≤5 с, правила ожиданий).</summary>
    internal static readonly TimeSpan MaxInterval = TimeSpan.FromSeconds(5);

    private static readonly TimeSpan DefaultInterval = TimeSpan.FromSeconds(5);

    private readonly string _prefix;
    private readonly IEtcdGateway _gateway;
    private readonly string[] _endpoints;
    private readonly TimeSpan _interval;
    private readonly TimeProvider _clock;
    private readonly object _lock = new();
    private readonly Dictionary<string, (string Value, long ModRevision)> _cache = [];

    private CancellationTokenSource? _cts;
    private Task? _loop;

    /// <summary>Дельта различий опроса (изменённые/удалённые ключи + revision);
    /// сбой обработчика не роняет цикл опроса (пишется в лог).</summary>
    public event Action<EtcdPrefixPollChange>? Changed;

    /// <summary>Потокобезопасный слепок кэша: ключ → (значение, mod_revision).</summary>
    public IReadOnlyDictionary<string, (string Value, long ModRevision)> Snapshot
    {
        get
        {
            lock (_lock)
                return new Dictionary<string, (string Value, long ModRevision)>(_cache);
        }
    }

    public EtcdPrefixPollCache(
        string prefix, IEtcdGateway gateway, string[] endpoints, TimeSpan? interval = null, TimeProvider? clock = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(prefix);
        ArgumentNullException.ThrowIfNull(gateway);
        ArgumentNullException.ThrowIfNull(endpoints);
        if (endpoints.Length == 0)
            throw new ArgumentException("etcd endpoints are not set", nameof(endpoints));
        _prefix = prefix;
        _gateway = gateway;
        _endpoints = endpoints;
        // Шаг опроса ≤5 с (правила ожиданий): больше — ужимаем к границе,
        // не-положительный — дефолт.
        var effective = interval ?? DefaultInterval;
        if (effective <= TimeSpan.Zero)
            effective = DefaultInterval;
        _interval = effective > MaxInterval ? MaxInterval : effective;
        _clock = clock ?? TimeProvider.System;
    }

    // Эффективный шаг опроса (после ужимания к границе ≤5 с); для тестов.
    internal TimeSpan Interval => _interval;

    // Старт: ПЕРВИЧНАЯ ЗАГРУЗКА полного префикса (единая точка failover S7),
    // затем фоновый цикл опроса. Ошибка загрузки — наружу (старт не маскируем).
    public async Task StartAsync(CancellationToken ct)
    {
        if (_cts is not null)
            throw new InvalidOperationException("etcd prefix poll cache already started");
        var range = await PollRangeAsync(ct);
        range.Throw();
        ReplaceCache(range.Value);
        _cts = new CancellationTokenSource();
        _loop = Task.Run(() => PollLoopAsync(_cts.Token));
    }

    // Штатный останов: отмена цикла опроса и ожидание его завершения.
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

    // Полный Range префикса через единую точку failover (arch/synchronization/03-fault-tolerance.md §S7).
    private Task<Result<IReadOnlyList<Kv>>> PollRangeAsync(CancellationToken ct)
        => EtcdEndpointFailover.CallAsync(
            _endpoints,
            (endpoint, token) => _gateway.RangeAsync(endpoint, _prefix, token),
            ct);

    // Первичная загрузка: полная пересборка кэша, БЕЗ события.
    private void ReplaceCache(IReadOnlyList<Kv> kvs)
    {
        lock (_lock)
        {
            _cache.Clear();
            foreach (var kv in kvs)
                _cache[kv.Key] = (kv.Value, (long)kv.ModRevision);
        }

        Console.WriteLine($"etcd-poll: первичная загрузка {_prefix}: ключей {kvs.Count}, revision {MaxRevision(kvs)}");
    }

    // Цикл опроса: Range → diff с кэшем → при различиях обновить кэш и поднять
    // Changed. Опрос не удался (endpoints недоступны) — кэш сохраняется, событие
    // не поднимается; ретрай на следующем шаге.
    private async Task PollLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var read = await PollRangeAsync(ct);
                if (read.IsSuccess)
                {
                    Diff(read.Value);
                }
                else
                {
                    Console.WriteLine(
                        $"etcd-poll: {_prefix} опрос не удался: {read.Error!.Message} — повтор на следующем шаге");
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e)
            {
                Console.WriteLine($"etcd-poll: {_prefix} сбой цикла: {e.Message} — повтор на следующем шаге");
            }

            try
            {
                await Task.Delay(_interval, _clock, ct);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    // Сравнение прочитанного с кэшем: отсутствовал/другое значение/другой
    // mod_revision — изменён/добавлен; ключ кэша, отсутствующий в прочитанном —
    // удалён. Различий нет — события нет.
    private void Diff(IReadOnlyList<Kv> read)
    {
        List<string> changed = [];
        List<string> removed = [];
        var readKeys = new HashSet<string>(read.Select(kv => kv.Key));
        lock (_lock)
        {
            foreach (var kv in read)
            {
                if (_cache.TryGetValue(kv.Key, out var current)
                    && current.Value == kv.Value
                    && current.ModRevision == (long)kv.ModRevision)
                    continue;
                _cache[kv.Key] = (kv.Value, (long)kv.ModRevision);
                changed.Add(kv.Key);
            }

            foreach (var key in _cache.Keys.Where(k => !readKeys.Contains(k)).ToList())
            {
                _cache.Remove(key);
                removed.Add(key);
            }
        }

        if (changed.Count == 0 && removed.Count == 0)
            return;

        RaiseChanged(new EtcdPrefixPollChange(changed, removed, MaxRevision(read)));
    }

    private static long MaxRevision(IReadOnlyList<Kv> kvs)
        => kvs.Count == 0 ? 0 : kvs.Max(kv => (long)kv.ModRevision);

    // Обработчик потребителя не должен ронять цикл опроса: сбой — в лог.
    private void RaiseChanged(EtcdPrefixPollChange change)
    {
        if (Changed is not { } handlers)
            return;
        try
        {
            handlers.Invoke(change);
        }
        catch (Exception e)
        {
            Console.WriteLine($"etcd-poll: {_prefix} обработчик Changed упал: {e.Message}");
        }
    }
}
