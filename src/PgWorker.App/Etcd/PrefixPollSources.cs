using Microsoft.Extensions.Options;
using PgWorker.Core.Model;
using Shared.Etcd.Client;
using Shared.Etcd.Coordination;

namespace PgWorker.App;

// Источник контроль-плейна тика ReconcileLoop (arch/synchronization/05-delta-application.md §S10, arch/pgworker/00-etcd-contract.md):
// опросные кэши наблюдаемых префиксов вместо полных Range в каждом тике.
// Тик читает слепки и счётчик изменений (Version — растёт на любой дельте
// любого префикса); гейт тика сравнивает Version с baseline последнего
// обработанного тика. Префиксы: /clusters/, /service/, /pgworker/backups/ —
// контракта тика; /pgworker/claims/ — координация (смерть инстанса гасит
// его клэймы-lease: исчезновение ключей = дельта = тик живёт = takeover
// «≤ TTL 15 с + тик» без изменений, arch/pgworker/00-etcd-contract.md). Журналы /pgworker/work/
// НАМЕРЕННО не опрашиваются (надзор пишет фазу каждый тик — дельта была бы
// вечной); самопоиск ошибок — трекинг фейлов в самом ReconcileLoop.
internal interface IPrefixPollSources
{
    /// <summary>Кэши запущены (первичная загрузка прошла).</summary>
    bool Started { get; }

    /// <summary>Запуск опросных кэшей (идемпотентен; ошибка — наружу, ретрай тиком).</summary>
    Task StartAsync(CancellationToken ct);

    /// <summary>Штатный останов всех кэшей (идемпотентен).</summary>
    Task StopAsync(CancellationToken ct);

    /// <summary>Счётчик изменений: ++ на каждую дельту любого префикса.</summary>
    long Version { get; }

    IReadOnlyDictionary<string, (string Value, long ModRevision)> ClustersSnapshot { get; }

    IReadOnlyDictionary<string, (string Value, long ModRevision)> ServiceSnapshot { get; }

    IReadOnlyDictionary<string, (string Value, long ModRevision)> BackupsSnapshot { get; }

    IReadOnlyDictionary<string, (string Value, long ModRevision)> ClaimsSnapshot { get; }
}

// Прод-реализация: четыре EtcdPrefixPollCache (интервал = Loops:ScanIntervalSec,
// кэш ужмёт к ≤5 с). Сбой опроса кэш переживает сам (слепок сохраняется, дельта
// не поднимается — тик пропускается; ретрай на следующем шаге опроса).
internal sealed class EtcdPrefixSources(
    IOptionsMonitor<PgWorkerOptions> options,
    IEtcdGateway gateway) : IPrefixPollSources, IClustersCacheSnapshot
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly List<EtcdPrefixPollCache> _caches = [];
    private EtcdPrefixPollCache? _clusters;
    private EtcdPrefixPollCache? _service;
    private EtcdPrefixPollCache? _backups;
    private EtcdPrefixPollCache? _claims;
    private long _version;

    public bool Started { get; private set; }

    public long Version => Interlocked.Read(ref _version);

    public IReadOnlyDictionary<string, (string Value, long ModRevision)> ClustersSnapshot
        => _clusters?.Snapshot ?? EmptySnapshot();

    public IReadOnlyDictionary<string, (string Value, long ModRevision)> ServiceSnapshot
        => _service?.Snapshot ?? EmptySnapshot();

    public IReadOnlyDictionary<string, (string Value, long ModRevision)> BackupsSnapshot
        => _backups?.Snapshot ?? EmptySnapshot();

    public IReadOnlyDictionary<string, (string Value, long ModRevision)> ClaimsSnapshot
        => _claims?.Snapshot ?? EmptySnapshot();

    // Слепок /clusters/ для читателей тика (ClusterSecretEnsurer — arch/synchronization/05-delta-application.md §S10).
    IReadOnlyDictionary<string, (string Value, long ModRevision)> IClustersCacheSnapshot.Snapshot
        => ClustersSnapshot;

    public async Task StartAsync(CancellationToken ct)
    {
        if (Started)
            return;
        await _gate.WaitAsync(ct);
        try
        {
            if (Started)
                return;

            var endpoints = options.CurrentValue.Etcd.Endpoints.ToArray();
            if (endpoints.Length == 0)
                throw new ApplicationException("PgWorker:Etcd:Endpoints are not set");
            // Интервал опроса = интервал тика воркера (S10: интервалы цикла
            // не меняются моделью); кэш ужмёт к границе ≤5 с.
            var interval = TimeSpan.FromSeconds(options.CurrentValue.Loops.ScanIntervalSec);

            // Кэши создаются на попытку старта: частично запущенный набор после
            // сбоя не переиспользуем — следующая попытка начинает с чистого.
            var created = new List<EtcdPrefixPollCache>();
            try
            {
                _clusters = CreateAndSubscribe(created, "/clusters/", endpoints, interval);
                _service = CreateAndSubscribe(created, "/service/", endpoints, interval);
                _backups = CreateAndSubscribe(created, "/pgworker/backups/", endpoints, interval);
                _claims = CreateAndSubscribe(created, "/pgworker/claims/", endpoints, interval);
                foreach (var cache in created)
                    await cache.StartAsync(ct);
                _caches.AddRange(created);
                Started = true;
            }
            catch
            {
                // Частичный старт гасим целиком — состояние «не запущено».
                foreach (var cache in created)
                {
                    try
                    {
                        await cache.StopAsync(CancellationToken.None);
                    }
                    catch
                    {
                        // останов лучшего эффекта — старт будет повторён тиком
                    }
                }

                _clusters = _service = _backups = _claims = null;
                throw;
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task StopAsync(CancellationToken ct)
    {
        var caches = _caches;
        _caches.Clear();
        _clusters = _service = _backups = _claims = null;
        Started = false;
        foreach (var cache in caches)
            await cache.StopAsync(ct);
    }

    private EtcdPrefixPollCache CreateAndSubscribe(
        List<EtcdPrefixPollCache> created, string prefix, string[] endpoints, TimeSpan interval)
    {
        var cache = new EtcdPrefixPollCache(prefix, gateway, endpoints, interval);
        cache.Changed += _ => Interlocked.Increment(ref _version); // любая дельта — тик живёт
        created.Add(cache);
        return cache;
    }

    private static IReadOnlyDictionary<string, (string Value, long ModRevision)> EmptySnapshot()
        => new Dictionary<string, (string Value, long ModRevision)>();
}
