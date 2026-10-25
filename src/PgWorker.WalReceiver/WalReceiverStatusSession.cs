using Shared.Core;
using Shared.Etcd.Client;

namespace PgWorker.WalReceiver;

/// <summary>Lease-сессия статус-ключа приёмника /pgworker/backups/&lt;C&gt;/&lt;X&gt;/agent/&lt;N&gt;
/// (arch/19 §4): lease TTL 15 c → первичный put → фоновый цикл ТОЛЬКО
/// LeaseKeepalive («ключ жив = приёмник жив» без доверия часам нод); значение
/// ключа пишется единым путём Update+FlushAsync/PutCurrentAsync — снапшот пары
/// (статус, lease) под семафором, put вне семафора (гонки put-поверх-put
/// исключены). Отказ put статуса — diagnostic-log, keepalive продолжается (put
/// не рвёт стрим и не убивает сессию); потеря lease (ошибка keepalive — TTL&lt;=0)
/// — перепуск grant+put в том же цикле. SIGTERM-путь — revoke (ключ гаснет
/// сразу, не по TTL). Все вызовы — failover-перебор endpoints (образец
/// WalStatusWriter.ReadAsync).</summary>
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
    /// приёмника ретраит через период ожидания; статус-ключа в etcd нет).</summary>
    public async Task<bool> StartAsync(WalReceiverAgentStatus initial, CancellationToken ct)
    {
        var grant = await GrantAsync(ct);
        if (!grant.IsSuccess)
            return false;
        var lease = grant.Value;
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
    /// arch/19 §4); отказ — false (keepalive продолжается). Сессия ещё не
    /// запущена (StartAsync не удался) — no-op true.</summary>
    public async Task<bool> FlushAsync(CancellationToken ct)
    {
        if (_lease is null)
            return true; // ключ ещё не заведён — put сделает StartAsync/цикл
        return await PutCurrentAsync(ct);
    }

    // Put актуального статуса ОБЩИМ путём: снапшот пары (статус, lease) —
    // согласованно под _gate; put — ВНЕ семафора (сеть не держит гейт:
    // Update не ждёт чужих сетевых вызовов; значение пишет только этот путь —
    // гонки put-поверх-put исключены).
    private async Task<bool> PutCurrentAsync(CancellationToken ct)
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

    // Фоновый цикл сессии: каждые 5 c — ТОЛЬКО LeaseKeepalive (значение ключа
    // цикл не перезаписывает: значение пишет лишь Update+FlushAsync/PutCurrentAsync —
    // lost-update гонка put↔put исключена; свежесть значения — публикациями
    // цикла приёмника при смене состояния). Потеря lease (ошибка keepalive —
    // TTL<=0) — перепуск grant + первичный put значения на новом lease.
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
            if (!keepalive.IsSuccess)
            {
                // Lease потерян (истёк/протух) — перевыпуск: без этого ключ
                // умрёт по TTL и приёмник «исчезнет» из etcd при живом процессе.
                await Console.Error.WriteLineAsync(
                    $"status keepalive failed: {keepalive.Error!.Message.Split('\n')[0]} — re-granting lease");
                var grant = await GrantAsync(ct);
                if (!grant.IsSuccess)
                    continue; // etcd по-прежнему недоступен — ретрай следующим тиком

                // Запись нового lease — под семафором; ключ на новом lease
                // обязан появиться сразу (до истечения TTL): put актуального
                // значения общим путём.
                await _gate.WaitAsync(ct);
                try
                {
                    _lease = grant.Value;
                }
                finally
                {
                    _gate.Release();
                }
                await PutCurrentAsync(ct);
            }
        }
    }

    // Grant lease — failover-перебор endpoints (образец WalStatusWriter.ReadAsync).
    private async Task<Result<long>> GrantAsync(CancellationToken ct)
    {
        Result<long> last = default;
        foreach (var endpoint in _endpoints)
        {
            var result = await _etcd.LeaseGrantAsync(endpoint, LeaseTtlSec, ct);
            if (!result.IsSuccess)
            {
                last = result; // отказ — пробуем следующий endpoint (failover)
                continue;
            }
            return result;
        }

        return Result<long>.Failed(last.Error
            ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    // Put статуса (на переданном lease) — failover-перебор endpoints.
    private async Task<Result> PutAsync(string json, long lease, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in _endpoints)
        {
            var put = await _etcd.PutAsync(endpoint, _key, json, lease, ct);
            if (!put.IsSuccess)
            {
                last = put; // отказ — пробуем следующий endpoint (failover)
                continue;
            }
            return put;
        }

        return Result.Failed(last.Error
            ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    private async Task<Result> FailoverAsync(
        Func<string, Task<Result>> call, CancellationToken ct)
    {
        Result last = default;
        foreach (var endpoint in _endpoints)
        {
            var result = await call(endpoint);
            if (!result.IsSuccess)
            {
                last = result; // отказ — пробуем следующий endpoint (failover)
                continue;
            }
            return result;
        }

        return Result.Failed(last.Error
            ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

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

        // Цикл остановлен — конкурентных записей _lease нет; revoke по
        // последнему значению (ключ гаснет сразу, не по TTL).
        var lease = _lease;
        _lease = null;
        try
        {
            if (lease is { } value)
                await RevokeQuietlyAsync(value, CancellationToken.None);
        }
        finally
        {
            _gate.Dispose();
        }
    }
}
