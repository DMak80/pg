using System.Text.Json;
using System.Text.Json.Serialization;
using KafkaWorker.Core;
using KafkaWorker.Etcd.Client;

namespace KafkaWorker.Etcd.Coordination;

// Координация инстансов KafkaWorker в etcd (spec §4.3, Д2): пер-кластерные lease-клэймы
// + глобальный лидер для singleton-задач. Захват — txn compare version==0 +
// put-with-lease TTL 15с; держатель продлевает keepalive-тиком (5с); смерть
// инстанса гасит lease ≤15с — ключ исчезает сам, другой инстанс захватывает (takeover).
public sealed class ClaimStore(string[] endpoints, IEtcdGateway gateway, TimeProvider clock, string? advertiseApiUrl = null)
    : IAsyncDisposable
{
    private const int ClaimTtlSec = 15;
    private static readonly TimeSpan KeepaliveInterval = TimeSpan.FromSeconds(5);

    private readonly string? _advertiseApiUrl = advertiseApiUrl;

    private readonly object _sync = new();
    private readonly Dictionary<string, long> _clusterLeases = []; // cluster → lease (live)
    private long? _leaderLease;
    private long? _instanceLease;
    private CancellationTokenSource? _loopCts;
    private Task? _loop;

    public string InstanceId { get; } = Guid.NewGuid().ToString("N")[..12];

    // Пер-кластерный клэйм (spec §4.3): txn compare version==0 → put lease TTL 15с.
    public async Task<Result<bool>> TryClaimClusterAsync(string cluster, CancellationToken ct)
    {
        lock (_sync)
        {
            if (_clusterLeases.ContainsKey(cluster))
                return Result<bool>.Success(true); // уже наш и продлевается
        }

        var grant = await GrantAsync(ct);
        if (!grant.IsSuccess)
            return Result<bool>.Failed(grant.Error!);

        var claimed = await TryPutLeasedKeyAsync(ClaimKey(cluster), new ClaimPayload(InstanceId, Now(), null), grant.Value, ct);
        if (claimed is { IsSuccess: false })
            return claimed;

        if (!claimed.Value)
        {
            await RevokeSilentlyAsync(grant.Value);
            return Result<bool>.Success(false); // занят другим инстансом — не ошибка
        }

        lock (_sync)
        {
            _clusterLeases[cluster] = grant.Value;
        }

        return Result<bool>.Success(true);
    }

    // Глобальный лидер (снапшоты P12): тот же примитив на /kafkaworker/leader.
    public async Task<Result<bool>> TryBecomeLeaderAsync(CancellationToken ct)
    {
        lock (_sync)
        {
            if (_leaderLease is not null)
                return Result<bool>.Success(true);
        }

        var grant = await GrantAsync(ct);
        if (!grant.IsSuccess)
            return Result<bool>.Failed(grant.Error!);

        var claimed = await TryPutLeasedKeyAsync("/kafkaworker/leader", new ClaimPayload(InstanceId, Now(), null), grant.Value, ct);
        if (claimed is { IsSuccess: false })
            return claimed;

        if (!claimed.Value)
        {
            await RevokeSilentlyAsync(grant.Value);
            return Result<bool>.Success(false);
        }

        lock (_sync)
        {
            _leaderLease = grant.Value;
        }

        return Result<bool>.Success(true);
    }

    // Наш ли клэйм: lease жив (keepalive его продлевает; провал = потеря).
    public bool IsMine(string cluster)
    {
        lock (_sync)
        {
            return _clusterLeases.ContainsKey(cluster);
        }
    }

    public bool IsLeader
    {
        get
        {
            lock (_sync)
            {
                return _leaderLease is not null;
            }
        }
    }

    // Освободить клэйм сейчас (deprovisioning D3): del ключа + revoke lease — не ждём TTL.
    public async Task ReleaseClusterAsync(string cluster, CancellationToken ct)
    {
        long lease;
        lock (_sync)
        {
            if (_clusterLeases.TryGetValue(cluster, out var l))
                _clusterLeases.Remove(cluster);
            else
                return;
            lease = l;
        }

        await WithFailoverAsync(endpoint => gateway.DeleteAsync(endpoint, ClaimKey(cluster), prefix: false, ct));
        await RevokeSilentlyAsync(lease);
    }

    // Фоновый keepalive-цикл (тик 5с): все мои lease + instance-ключ /kafkaworker/instances/<id>.
    public Task StartAsync(CancellationToken ct)
    {
        if (_loop is not null)
            return Task.CompletedTask;

        _loopCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = _loopCts.Token;
        _loop = Task.Run(async () =>
        {
            // Instance-ключ живости (диагностика): первая попытка сразу при старте;
            // при отказе (etcd недоступен) ретрай каждым тиком — см. KeepaliveTickAsync.
            await EnsureInstanceKeyAsync(token);
            while (!token.IsCancellationRequested)
            {
                await KeepaliveTickAsync(token);
                try
                {
                    await Task.Delay(KeepaliveInterval, token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }, CancellationToken.None);
        return Task.CompletedTask;
    }

    // Один keepalive-тик: продлевает instance/leader/claims; провал = клэйм потерян
    // (следующий TryClaim пере-захватывает). Публичен для цикла App и тестов.
    public async Task KeepaliveTickAsync(CancellationToken ct)
    {
        // Восстановление instance/api-ключей: могли не поставиться при старте (etcd
        // был недоступен) или потерять lease в рантайме (недоступность дольше TTL) —
        // ретрай каждым тиком ~5с; guard внутри делает успешный путь дешёвым. Без
        // этого воркер жив, а панель не резолвит его API (worker-api-unreachable).
        await EnsureInstanceKeyAsync(ct);

        List<long> toKeep;
        lock (_sync)
        {
            toKeep = [.. _clusterLeases.Values];
            if (_leaderLease is { } leader)
                toKeep.Add(leader);
            if (_instanceLease is { } instance)
                toKeep.Add(instance);
        }

        foreach (var lease in toKeep)
        {
            var ok = await WithFailoverAsync(endpoint => gateway.LeaseKeepaliveAsync(endpoint, lease, ct));
            if (ok.IsSuccess)
                continue;

            // lease истёк/отозван: помечаем потерю, ключ под ним etcd уже удалил сам
            lock (_sync)
            {
                foreach (var (cluster, l) in _clusterLeases.Where(p => p.Value == lease).ToList())
                    _clusterLeases.Remove(cluster);
                if (_leaderLease == lease)
                    _leaderLease = null;
                if (_instanceLease == lease)
                    _instanceLease = null;
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        _loopCts?.Cancel();
        if (_loop is not null)
        {
            try
            {
                await _loop;
            }
            catch (OperationCanceledException)
            {
                // штатное завершение цикла
            }
        }

        _loopCts?.Dispose();

        List<long> leases;
        lock (_sync)
        {
            leases = [.. _clusterLeases.Values];
            if (_leaderLease is { } leader)
                leases.Add(leader);
            if (_instanceLease is { } instance)
                leases.Add(instance);
            _clusterLeases.Clear();
            _leaderLease = null;
            _instanceLease = null;
        }

        foreach (var lease in leases)
            await RevokeSilentlyAsync(lease);
    }

    private async Task EnsureInstanceKeyAsync(CancellationToken ct)
    {
        lock (_sync)
        {
            if (_instanceLease is not null)
                return;
        }

        var grant = await GrantAsync(ct);
        if (!grant.IsSuccess)
            return; // диагностика — не блокируем работу клэймов

        var put = await WithFailoverAsync(endpoint => gateway.PutAsync(
            endpoint, $"/kafkaworker/instances/{InstanceId}", InstanceId, grant.Value, ct));
        if (!put.IsSuccess)
        {
            await RevokeSilentlyAsync(grant.Value);
            return;
        }

        // Ключ доступа API (arch/16 §1.1): тем же lease, что instances/<id>, —
        // гаснут вместе; панель резолвит URL воркера только по этому ключу.
        if (_advertiseApiUrl is { Length: > 0 } url)
        {
            var payload = JsonSerializer.Serialize(
                new ApiDiscoveryPayload(url, InstanceId, Now()), PayloadJson.Json);
            var apiPut = await WithFailoverAsync(endpoint => gateway.PutAsync(
                endpoint, $"/kafkaworker/api/{InstanceId}", payload, grant.Value, ct));
            if (!apiPut.IsSuccess)
            {
                await RevokeSilentlyAsync(grant.Value);
                return; // оба ключа ставятся на одном lease: отказ = ни одного
            }
        }

        lock (_sync)
        {
            _instanceLease = grant.Value;
        }
    }

    // Захват leased-ключа через txn compare version==0; false = уже занят.
    private async Task<Result<bool>> TryPutLeasedKeyAsync(
        string key, ClaimPayload payload, long lease, CancellationToken ct)
    {
        var value = JsonSerializer.Serialize(payload, PayloadJson.Json);
        var txn = await WithFailoverAsync(endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of([TxnCompare.NotExists(key)], [new TxnOp.Put(key, value, lease)]),
            ct));
        if (!txn.IsSuccess)
            return Result<bool>.Failed(txn.Error!);

        return Result<bool>.Success(txn.Value.Succeeded);
    }

    private Task<Result<long>> GrantAsync(CancellationToken ct)
        => WithFailoverAsync(endpoint => gateway.LeaseGrantAsync(endpoint, ClaimTtlSec, ct));

    private async Task RevokeSilentlyAsync(long lease)
    {
        try
        {
            await WithFailoverAsync(endpoint => gateway.LeaseRevokeAsync(endpoint, lease, CancellationToken.None));
        }
        catch
        {
            // best-effort: истечёт по TTL
        }
    }

    private long Now() => clock.GetUtcNow().ToUnixTimeSeconds();

    private static string ClaimKey(string cluster) => $"/kafkaworker/claims/{cluster}";

    // Failover по endpoints: первый успешный ответ выигрывает; все недоступны → последняя ошибка.
    private async Task<Result<T>> WithFailoverAsync<T>(Func<string, Task<Result<T>>> call)
    {
        Result<T>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private async Task<Result> WithFailoverAsync(Func<string, Task<Result>> call)
    {
        Result? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    // Value claim'а: {"instance","since_unix"} (spec §4.3).
    private sealed record ClaimPayload(
        [property: JsonPropertyName("instance")] string Instance,
        [property: JsonPropertyName("since_unix")] long SinceUnix,
        [property: JsonPropertyName("phase")] string? Phase);

    // Value ключа /kafkaworker/api/<id> (arch/16 §1.1): {"url","instance","since_unix"}.
    // ВАЖНО: PayloadJson.Json НЕ задаёт PropertyNamingPolicy (дефолт PascalCase) —
    // поля маппим атрибутами, как у ClaimPayload, иначе парсер панели (контракт
    // arch/02 §2.3.1/§2.3.2 ждёт snake_case) не распарсит значение.
    private sealed record ApiDiscoveryPayload(
        [property: JsonPropertyName("url")] string Url,
        [property: JsonPropertyName("instance")] string Instance,
        [property: JsonPropertyName("since_unix")] long SinceUnix);
}

// Общие JSON-настройки payload координации (camelCase/snake_case по контракту §4.3).
internal static class PayloadJson
{
    public static readonly JsonSerializerOptions Json = new() { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull };
}
