using Microsoft.Extensions.Options;
using Shared.Etcd.Client;
using Shared.Etcd.Coordination;

namespace S3Worker.App;

// Источник контроль-плейна тика ReconcileLoop (arch/17 S10, arch/24 §5):
// опросные кэши наблюдаемых префиксов вместо полных Range в каждом тике.
// Тик читает слепок и счётчик изменений (Version — растёт на любой дельте
// любого префикса); гейт тика сравнивает Version с baseline последнего
// обработанного тика. Префиксы: /s3/ — домен (декларации, states,
// links/denylist, ключи наблюдения зонда connectivity/sync); /s3worker/
// {claims,locks,rotations,probes}/ — координация и внешние заявки (смерть
// инстанса гасит его клэймы и locks-lease: удаление ключей = дельта = тик
// живёт = takeover ≤ TTL 15 с + тик). Журналы /s3worker/work/ НАМЕРЕННО не
// опрашиваются — воркер пишет их сам.
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

    /// <summary>Слепок домена /s3/ (ключ → значение + mod_revision).</summary>
    IReadOnlyDictionary<string, (string Value, long ModRevision)> ClustersSnapshot { get; }
}

// Прод-реализация: опросные кэши EtcdPrefixPollCache (интервал =
// Loops:ScanIntervalSec, кэш ужмёт к границе ≤5 с). Сбой опроса кэш переживает
// сам (слепок сохраняется, дельта не поднимается — тик без изменений; ретрай
// на следующем шаге опроса).
internal sealed class EtcdPrefixSources(
    IOptionsMonitor<S3WorkerOptions> options,
    IEtcdGateway gateway) : IPrefixPollSources
{
    // Наблюдаемые префиксы: домен + координация/внешние заявки (arch/24 §5).
    private static readonly string[] Prefixes =
    [
        "/s3/",
        "/s3worker/claims/",
        "/s3worker/locks/",
        "/s3worker/rotations/",
        "/s3worker/probes/",
    ];

    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly List<EtcdPrefixPollCache> _caches = [];
    private long _version;

    public bool Started { get; private set; }

    public long Version => Interlocked.Read(ref _version);

    public IReadOnlyDictionary<string, (string Value, long ModRevision)> ClustersSnapshot
        => _caches.Count == 0 ? EmptySnapshot() : _caches[0].Snapshot;

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
                throw new ApplicationException("S3Worker:Etcd:Endpoints are not configured");
            // Интервал опроса = интервал тика воркера (S10: интервалы цикла
            // не меняются моделью); кэш ужмёт к границе ≤5 с.
            var interval = TimeSpan.FromSeconds(options.CurrentValue.Loops.ScanIntervalSec);

            // Кэши создаются на попытку старта: частично запущенный набор после
            // сбоя не переиспользуем — следующая попытка начинает с чистого.
            var created = new List<EtcdPrefixPollCache>();
            try
            {
                foreach (var prefix in Prefixes)
                    CreateAndSubscribe(created, prefix, endpoints, interval);
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
        Started = false;
        foreach (var cache in caches)
            await cache.StopAsync(ct);
    }

    private void CreateAndSubscribe(
        List<EtcdPrefixPollCache> created, string prefix, string[] endpoints, TimeSpan interval)
    {
        var cache = new EtcdPrefixPollCache(prefix, gateway, endpoints, interval);
        cache.Changed += _ => Interlocked.Increment(ref _version); // любая дельта — тик живёт
        created.Add(cache);
    }

    private static IReadOnlyDictionary<string, (string Value, long ModRevision)> EmptySnapshot()
        => new Dictionary<string, (string Value, long ModRevision)>();
}
