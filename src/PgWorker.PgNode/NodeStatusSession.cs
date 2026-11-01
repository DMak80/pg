using Shared.Core;
using Shared.Etcd.Client;

namespace PgWorker.PgNode;

/// <summary>Писатель статусного ключа машины (arch/14 §3.3): развязка
/// PgNodeMachine от lease-сессии для юнит-тестов (фейк-писатель по образцу
/// internal-seam плана; прод — NodeStatusSession).</summary>
internal interface INodeStatusWriter : IAsyncDisposable
{
    Task<bool> StartAsync(PgNodeStatus initial, CancellationToken ct);
    void Update(PgNodeStatus status);
    Task<bool> FlushAsync(CancellationToken ct);
    Task<bool> TryTransitionAsync(string newValueJson, string expectedValueJson, CancellationToken ct);
}

/// <summary>Lease-сессия статусного ключа агента /pgworker/nodes/&lt;C&gt;/&lt;X&gt;/&lt;N&gt;/status
/// (arch/14 §3.3, spec §3.6): lease TTL 15 c → первичный put → фоновый цикл
/// каждые 5 c — LeaseKeepalive + put актуального значения (ключ жив = агент
/// жив, статус не «застревает» мёртвого процесса). КОПИЯ-адаптация
/// WalReceiverStatusSession: тот же семафор-паттерн (put'ы значения и переходы
/// lease — под ОДНИМ operation-семофором; потеря lease — revoke старого +
/// grant нового + немедленный put; SIGTERM — revoke). Значение — PgNodeStatus
/// (сериализация ToJson; node-суффикс в ключ не пишется — контракт arch/14
/// §3.3). Атомарный txn-переход машины — TryTransitionAsync (compare по
/// VALUE — устойчив к keepalive-put'ам того же значения).</summary>
public sealed class NodeStatusSession : INodeStatusWriter
{
    /// <summary>TTL lease статус-ключа (arch/14 §3.3): 15 c.</summary>
    public const int LeaseTtlSec = 15;

    /// <summary>Период keepalive + put статуса по умолчанию (arch/14 §3.3): 5 c.</summary>
    public static readonly TimeSpan DefaultKeepalivePeriod = TimeSpan.FromSeconds(5);

    private readonly string _key;
    private readonly string[] _endpoints;
    private readonly IEtcdGateway _etcd;
    private readonly TimeSpan _keepalivePeriod;
    private readonly SemaphoreSlim _gate = new(1, 1);

    // Сериализация операцией: put'ы значения (FlushAsync/keepalive/txn-переход)
    // и переходы lease (re-grant) не пересекаются — in-flight put старого lease
    // всегда завершается до re-grant, ключ не остаётся на истекающем lease.
    private readonly SemaphoreSlim _ops = new(1, 1);

    private long? _lease;
    private Task? _loop;
    private CancellationTokenSource? _cts;
    private PgNodeStatus? _current;

    /// <summary>Прод: endpoints из identity, ключ — статус-слот ноды.</summary>
    public NodeStatusSession(
        IEtcdGateway etcd, string[] endpoints, string key, TimeSpan? keepalivePeriod = null)
    {
        _etcd = etcd;
        _endpoints = endpoints;
        _key = key;
        _keepalivePeriod = keepalivePeriod ?? DefaultKeepalivePeriod;
    }

    /// <summary>Grant lease + первичный put. false — etcd недоступен (цикл
    /// машины ретраит через период ожидания; статус-ключа в etcd нет).
    /// Отмена (SIGTERM) в любом окне до регистрации lease — revoke: «живой»
    /// ключ не остаётся на TTL.</summary>
    public async Task<bool> StartAsync(PgNodeStatus initial, CancellationToken ct)
    {
        var grant = await GrantAsync(ct);
        if (!grant.IsSuccess)
            return false;
        var lease = grant.Value;
        try
        {
            var json = initial.ToJson();
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
    public void Update(PgNodeStatus status)
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

    /// <summary>Немедленный put выставленного статуса (смена состояния/фактов);
    /// отказ — false (keepalive продолжается). Сериализовано с keepalive-циклом
    /// (общий _ops — гонки put↔re-grant исключены). Сессия ещё не запущена
    /// (StartAsync не удался) — no-op true.</summary>
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

    /// <summary>Атомарный переход машины txn'ом (spec §3.6-2, §8.16,
    /// arch/14 §3.3): compare по VALUE — прочитанное значение статуса (где
    /// state была not_initialized) обязано быть неизменным; put нового
    /// значения на lease сессии. Compare по значению, а не по mod_revision —
    /// keepalive-put'ы сессии ТОГО ЖЕ значения (каждые 5 c, value не меняется,
    /// mod_revision — да) переход не срывают. false — compare проигран
    /// (параллельный писатель ИЗМЕНИЛ значение) или etcd недоступен.</summary>
    public async Task<bool> TryTransitionAsync(string newValueJson, string expectedValueJson, CancellationToken ct)
    {
        await _ops.WaitAsync(ct);
        try
        {
            var leaseSnapshot = await ReadLeaseAsync(ct);
            if (leaseSnapshot is not { } lease)
                return false;
            var txn = TxnRequest.Of(
                [TxnCompare.ValueEqual(_key, expectedValueJson)],
                [new TxnOp.Put(_key, newValueJson, lease)]);
            // Txn возвращает Result<TxnResult>: не-generic FailoverAsync
            // копируемой сессии (Func<string, Task<Result>>) сюда не годится —
            // прямой generic-вызов (перегрузка EtcdEndpointFailover.CallAsync<T>;
            // прецеденты KafkaWorker NodeSupervisor.cs:428-429,
            // PortAllocHealer.cs:361-362).
            var result = await EtcdEndpointFailover.CallAsync(
                _endpoints, (endpoint, token) => _etcd.TxnAsync(endpoint, txn, token), ct);
            // Успешный txn-переход обновляет in-memory _current (MR-#11):
            // keepalive-put'ы сессии публикуют НОВОЕ значение, гвард повторного
            // handshake решается по текущему состоянию без второго запуска фаз.
            if (result.IsSuccess && result.Value.Succeeded)
                _current = PgNodeStatus.FromJson(newValueJson);
            return result.IsSuccess && result.Value.Succeeded;
        }
        finally
        {
            _ops.Release();
        }
    }

    // Снапшот lease под _gate (по образцу PutCurrentCoreAsync): txn-переход
    // сериализован с keepalive/re-grant через _ops — lease согласован.
    private async Task<long?> ReadLeaseAsync(CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            return _lease;
        }
        finally
        {
            _gate.Release();
        }
    }

    // Put актуального статуса — ВЫЗЫВАЕТСЯ ТОЛЬКО под _ops (FlushAsync и
    // keepalive-цикл сериализованы): снапшот пары (статус, lease) под _gate,
    // put сети — сеть _gate не держит (Update не ждёт сетевых вызовов).
    private async Task<bool> PutCurrentCoreAsync(CancellationToken ct)
    {
        PgNodeStatus? status;
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

        var json = current.ToJson();
        var put = await PutAsync(json, lease, ct);
        if (!put.IsSuccess)
            await Console.Error.WriteLineAsync(
                $"status put failed: {put.Error!.Message.Split('\n')[0]}");
        return put.IsSuccess;
    }

    // Фоновый цикл сессии: каждые 5 c под _ops — LeaseKeepalive + put
    // актуального значения (свежесть статуса; put идемпотентен).
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
                    // Значение ключа — не реже периода keepalive:
                    // updated_unix не застаивается при живом процессе.
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
