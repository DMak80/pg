using Shared.Core;
using Shared.Etcd.Client;

namespace PgWorker.WalReceiver;

/// <summary>Lease-сессия статус-ключа приёмника /pgworker/backups/&lt;C&gt;/&lt;X&gt;/agent/&lt;N&gt;
/// (arch/19 §4): lease TTL 15 c → первичный put → фоновый цикл каждые 5 c —
/// LeaseKeepalive + put актуального значения (свежесть статуса «не реже раза
/// в 5 c» — spec §3.1 п.7; ключ жив = приёмник жив, значение не застаивается).
/// Все переходы lease (перевыпуск при потере) и put'ы значения — под ОДНИМ
/// operation-семафором: in-flight put старого lease не может перезаписать ключ
/// после re-grant (гонка lease↔put исключена — ключ всегда на последнем
/// lease). Потеря lease (ошибка keepalive) — revoke старого (идемпотентно) +
/// grant нового + немедленный put значения. Отказ put статуса —
/// diagnostic-log, keepalive продолжается (put не рвёт стрим и не убивает
/// сессию). SIGTERM-путь — revoke (ключ гаснет сразу, не по TTL), в т.ч. при
/// отмене в окне регистрации lease в StartAsync. Все вызовы —
/// EtcdEndpointFailover (ротация старта + лог переключений).</summary>
public sealed class WalReceiverStatusSession : IAsyncDisposable
{
    /// <summary>TTL lease статус-ключа (arch/19 §4): 15 c.</summary>
    public const int LeaseTtlSec = 15;

    /// <summary>Период keepalive + put статуса по умолчанию (arch/19 §4): 5 c.</summary>
    public static readonly TimeSpan DefaultKeepalivePeriod = TimeSpan.FromSeconds(5);

    private readonly string _key;
    private readonly string[] _endpoints;
    private readonly IEtcdGateway _etcd;
    private readonly TimeSpan _keepalivePeriod;
    private readonly SemaphoreSlim _gate = new(1, 1);

    // Сериализация операцией: put'ы значения (FlushAsync/keepalive) и переходы
    // lease (re-grant) не пересекаются — in-flight put старого lease всегда
    // завершается до re-grant, ключ не остаётся на истекающем lease.
    private readonly SemaphoreSlim _ops = new(1, 1);

    private long? _lease;
    private Task? _loop;
    private CancellationTokenSource? _cts;
    private WalReceiverAgentStatus? _current;

    /// <summary>Прод: endpoints из identity, ключ — статус-слот ноды.</summary>
    public WalReceiverStatusSession(
        IEtcdGateway etcd, string[] endpoints, string key, TimeSpan? keepalivePeriod = null)
    {
        _etcd = etcd;
        _endpoints = endpoints;
        _key = key;
        _keepalivePeriod = keepalivePeriod ?? DefaultKeepalivePeriod;
    }

    /// <summary>Grant lease + первичный put. false — etcd недоступен (цикл
    /// приёмника ретраит через период ожидания; статус-ключа в etcd нет).
    /// Отмена (SIGTERM) в любом окне до регистрации lease — revoke: «живой»
    /// ключ не остаётся на TTL.</summary>
    public async Task<bool> StartAsync(WalReceiverAgentStatus initial, CancellationToken ct)
    {
        var grant = await GrantAsync(ct);
        if (!grant.IsSuccess)
            return false;
        var lease = grant.Value;
        try
        {
            var json = WalReceiverAgentStatusJson.ToJson(_key[(_key.LastIndexOf('/') + 1)..], initial);
            var put = await PutAsync(json, lease, ct);
            if (!put.IsSuccess)
            {
                // Первичный put не удался — lease без ключа бессмыслен: revoke и
                // ретрай StartAsync циклом (статус-ключа в etcd нет).
                await RevokeQuietlyAsync(lease, ct);
                return false;
            }

            // Запись _lease — под семафором (снапшот-паттерн: keepalive-цикл и
            // FlushAsync читают пару согласованно под тем же гейтом).
            await _gate.WaitAsync(ct);
            try
            {
                _lease = lease;
                _current = initial;
            }
            finally
            {
                _gate.Release();
            }

            _cts = new CancellationTokenSource();
            _loop = Task.Run(() => KeepaliveLoopAsync(_cts.Token), CancellationToken.None);
            return true;
        }
        catch (OperationCanceledException)
        {
            // SIGTERM в окне регистрации: lease уже выдан — revoke, ключ не доживает до TTL.
            await RevokeQuietlyAsync(lease, CancellationToken.None);
            throw;
        }
    }

    /// <summary>Текущий статус для следующего put (потокобезопасно: put читает
    /// последнее выставленное значение снапшотом под семафором).</summary>
    public void Update(WalReceiverAgentStatus status)
    {
        _gate.Wait();
        try
        {
            _current = status;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Немедленный put выставленного статуса (смена состояния/фактов,
    /// arch/19 §4); отказ — false (keepalive продолжается). Сериализовано с
    /// keepalive-циклом (общий _ops — гонки put↔re-grant исключены). Сессия ещё
    /// не запущена (StartAsync не удался) — no-op true.</summary>
    public async Task<bool> FlushAsync(CancellationToken ct)
    {
        if (_lease is null)
            return true; // ключ ещё не заведён — put сделает StartAsync/цикл
        await _ops.WaitAsync(ct);
        try
        {
            return await PutCurrentCoreAsync(ct);
        }
        finally
        {
            _ops.Release();
        }
    }

    // Put актуального статуса — ВЫЗЫВАЕТСЯ ТОЛЬКО под _ops (FlushAsync и
    // keepalive-цикл сериализованы): снапшот пары (статус, lease) под _gate,
    // put сети — сеть _gate не держит (Update не ждёт сетевых вызовов).
    private async Task<bool> PutCurrentCoreAsync(CancellationToken ct)
    {
        WalReceiverAgentStatus? status;
        long? leaseSnapshot;
        await _gate.WaitAsync(ct);
        try
        {
            status = _current;
            leaseSnapshot = _lease;
        }
        finally
        {
            _gate.Release();
        }

        if (status is not { } current || leaseSnapshot is not { } lease)
            return true; // статуса/lease нет — put не нужен

        var json = WalReceiverAgentStatusJson.ToJson(
            _key[(_key.LastIndexOf('/') + 1)..], current);
        var put = await PutAsync(json, lease, ct);
        if (!put.IsSuccess)
            await Console.Error.WriteLineAsync(
                $"status put failed: {put.Error!.Message.Split('\n')[0]}");
        return put.IsSuccess;
    }

    // Фоновый цикл сессии: каждые 5 c под _ops — LeaseKeepalive + put
    // актуального значения (свежесть статуса, spec §3.1 п.7; put идемпотентен).
    // Ошибка keepalive — lease потерян (истёк или etcd-транспорт): revoke
    // старого (идемпотентно, истёкший — 404-ок), grant нового + немедленный put
    // значения — ключ не остаётся на непродлеваемом lease.
    private async Task KeepaliveLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_keepalivePeriod, ct);
            }
            catch (OperationCanceledException)
            {
                return; // SIGTERM-путь — DisposeAsync делает revoke
            }

            await _ops.WaitAsync(ct);
            try
            {
                long? leaseSnapshot;
                await _gate.WaitAsync(ct);
                try
                {
                    leaseSnapshot = _lease;
                }
                finally
                {
                    _gate.Release();
                }
                if (leaseSnapshot is not { } lease)
                    continue;

                var keepalive = await FailoverAsync(
                    endpoint => _etcd.LeaseKeepaliveAsync(endpoint, lease, ct), ct);
                if (keepalive.IsSuccess)
                {
                    // Значение ключа — не реже периода keepalive (spec §3.1 п.7):
                    // updated_unix/last_delivered не застаиваются при живом стриме.
                    await PutCurrentCoreAsync(ct);
                    continue;
                }

                await Console.Error.WriteLineAsync(
                    $"status keepalive failed: {keepalive.Error!.Message.Split('\n')[0]} — re-granting lease");
                var grant = await GrantAsync(ct);
                if (!grant.IsSuccess)
                    continue; // etcd по-прежнему недоступен — ретрай следующим тиком

                // Старый lease больше не продлевается: revoke идемпотентен
                // (истёкший — 404-успех); ключ обязан уйти на новый lease сразу.
                await RevokeQuietlyAsync(lease, CancellationToken.None);
                await _gate.WaitAsync(ct);
                try
                {
                    _lease = grant.Value;
                }
                finally
                {
                    _gate.Release();
                }
                await PutCurrentCoreAsync(ct);
            }
            finally
            {
                _ops.Release();
            }
        }
    }

    // Grant lease — EtcdEndpointFailover (ротация старта, лог переключений).
    private Task<Result<long>> GrantAsync(CancellationToken ct)
        => EtcdEndpointFailover.CallAsync(
            _endpoints, (endpoint, token) => _etcd.LeaseGrantAsync(endpoint, LeaseTtlSec, token), ct);

    // Put статуса (на переданном lease) — EtcdEndpointFailover.
    private Task<Result> PutAsync(string json, long lease, CancellationToken ct)
        => EtcdEndpointFailover.CallAsync(
            _endpoints, (endpoint, token) => _etcd.PutAsync(endpoint, _key, json, lease, token), ct);

    private Task<Result> FailoverAsync(
        Func<string, Task<Result>> call, CancellationToken ct)
        => EtcdEndpointFailover.CallAsync(_endpoints, (endpoint, token) => call(endpoint), ct);

    // Revoke без ошибок наружу (Dispose/SIGTERM: ключ гаснет досрочно; уже
    // истёкший lease — 404, тоже успех по семантике идемпотентности).
    private async Task RevokeQuietlyAsync(long lease, CancellationToken ct)
    {
        var revoke = await FailoverAsync(
            endpoint => _etcd.LeaseRevokeAsync(endpoint, lease, ct), ct);
        if (!revoke.IsSuccess)
            await Console.Error.WriteLineAsync(
                $"status revoke failed: {revoke.Error!.Message.Split('\n')[0]}");
    }

    public async ValueTask DisposeAsync()
    {
        if (_cts is { } cts)
        {
            await cts.CancelAsync();
            if (_loop is { } loop)
            {
                try
                {
                    await loop.WaitAsync(TimeSpan.FromSeconds(5));
                }
                catch
                {
                    // Цикл уже умер/не успел за бюджетом — revoke ниже всё равно идёт.
                }
            }
            cts.Dispose();
            _cts = null;
        }

        // Сериализация с in-flight put'ом: дождаться (с бюджетом) активной
        // операции — иначе put старого lease может увести ключ после revoke.
        try
        {
            await _ops.WaitAsync(TimeSpan.FromSeconds(10));
        }
        catch
        {
            // Бюджет истёк — revoke всё равно идёт (put старого lease истечёт по TTL).
        }

        try
        {
            // Цикл остановлен — конкурентных записей _lease нет; revoke по
            // последнему значению (ключ гаснет сразу, не по TTL).
            var lease = _lease;
            _lease = null;
            if (lease is { } value)
                await RevokeQuietlyAsync(value, CancellationToken.None);
        }
        finally
        {
            _gate.Dispose();
            _ops.Dispose();
        }
    }
}
