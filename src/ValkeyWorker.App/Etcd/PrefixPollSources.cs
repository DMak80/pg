using Microsoft.Extensions.Options;
using Shared.Etcd.Client;
using Shared.Etcd.Coordination;

namespace ValkeyWorker.App;

// Источник контроль-плейна тика ReconcileLoop (arch/synchronization/05-delta-application.md §S10, arch/valkeyworker/README.md):
// опросный кэш домена вместо полных Range в каждом тике. Тик работает
// КАЖДЫЙ раз (self-healing ждёт не-etcd события — гейтов по дельте нет) и
// читает слепок кэша: etcd-чтений в тике ноль (E10 — чтение уходит в
// фоновый опрос, единая точка failover S7). Применения etcd-значений
// процессами — по факту изменения значения (S10). Заявки ротаций, клэймы и
// portalloc-клэйм процессы читают из etcd напрямую при исполнении; журналы
// /valkeyworker/work/ (вкл. стейт ротации work/<C>/rotation) воркер пишет сам.
internal interface IPrefixPollSources
{
    /// <summary>Кэш запущен (первичная загрузка прошла).</summary>
    bool Started { get; }

    /// <summary>Запуск опросного кэша (идемпотентен; ошибка — наружу, ретрай тиком).</summary>
    Task StartAsync(CancellationToken ct);

    /// <summary>Штатный останов кэша (идемпотентен).</summary>
    Task StopAsync(CancellationToken ct);

    /// <summary>Слепок домена /valkey/clusters/ (ключ → значение + mod_revision).</summary>
    IReadOnlyDictionary<string, (string Value, long ModRevision)> ClustersSnapshot { get; }
}

// Прод-реализация: опросный кэш EtcdPrefixPollCache (интервал =
// Loops:ScanIntervalSec, кэш ужмёт к границе ≤5 с). Сбой опроса кэш переживает
// сам (слепок сохраняется; ретрай на следующем шаге опроса).
internal sealed class EtcdPrefixSources(
    IOptionsMonitor<ValkeyWorkerOptions> options,
    IEtcdGateway gateway) : IPrefixPollSources
{
    // Наблюдаемый префикс — домен (arch/valkeyworker/README.md).
    private const string DomainPrefix = "/valkey/clusters/";

    private readonly SemaphoreSlim _gate = new(1, 1);
    private EtcdPrefixPollCache? _cache;

    public bool Started { get; private set; }

    public IReadOnlyDictionary<string, (string Value, long ModRevision)> ClustersSnapshot
        => _cache?.Snapshot ?? EmptySnapshot();

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
                throw new ApplicationException("ValkeyWorker:Etcd:Endpoints are not set");
            // Интервал опроса = интервал тика воркера (S10: интервалы цикла
            // не меняются моделью); кэш ужмёт к границе ≤5 с.
            var interval = TimeSpan.FromSeconds(options.CurrentValue.Loops.ScanIntervalSec);

            // Кэш создаётся на попытку старта: частично запущенное состояние
            // после сбоя не переиспользуем — следующая попытка начинает с чистого.
            var cache = new EtcdPrefixPollCache(DomainPrefix, gateway, endpoints, interval);
            try
            {
                await cache.StartAsync(ct);
            }
            catch
            {
                await cache.StopAsync(CancellationToken.None);
                throw;
            }

            _cache = cache;
            Started = true;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task StopAsync(CancellationToken ct)
    {
        var cache = _cache;
        _cache = null;
        Started = false;
        if (cache is not null)
            await cache.StopAsync(ct);
    }

    private static IReadOnlyDictionary<string, (string Value, long ModRevision)> EmptySnapshot()
        => new Dictionary<string, (string Value, long ModRevision)>();
}
