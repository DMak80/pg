using System.Text.Json;
using System.Text.Json.Serialization;
using PgWorker.Core;
using PgWorker.Etcd.Client;

namespace PgWorker.Etcd.Coordination;

/// <summary>
/// Глобальный portalloc-клэйм (t90, arch/14 §2.4/§3.3): взаимоисключение секции
/// довыделения портов «чтение занятости → выбор троек → запись portalloc» —
/// пер-кластерные клэймы кросс-кластерную гонку не закрывают (два параллельно
/// сеемых кластера читают /pgworker/portalloc/* до первой записи соседа и
/// выбирают одинаковые порты). Захват — txn version==0 + put-with-lease TTL 15 с
/// (паттерн /pgworker/leader); keepalive не нужен — секция короткая (единицы
/// секунд ≪ TTL). Освобождение — явное: del под compare ValueEqual(наш value;
/// lease истёк и лок перехвачен — чужой ключ не трогаем) + revoke lease.
/// «Занят» (чужим инстансом или параллельным тиком этого же — объект в DI один
/// на процесс) — не ошибка: вызывающий возвращает InProgress, следующий тик
/// (~5 с) повторяет; смерть держателя гасит TTL ≤ 15 с — takeover без оператора.
/// </summary>
public sealed class PortAllocLock(
    string[] endpoints, IEtcdGateway gateway, TimeProvider clock, string instanceId)
{
    public const string Key = "/pgworker/locks/portalloc";
    private const int TtlSec = 15;

    private readonly object _sync = new();
    private long? _lease;
    private string? _payload; // наш value — compare «чужой-не-трогаем» при release

    /// <summary>Захват: true — держим; false — занят (другим инстансом либо
    /// параллельным тиком ЭТОГО инстанса — клэйм-объект DI-синглтон; НЕ ошибка).</summary>
    public async Task<Result<bool>> TryAcquireAsync(CancellationToken ct)
    {
        // t90 (ревью-блокер): объект — DI-синглтон, ReconcileLoop тикает кластеры
        // ПАРАЛЛЕЛЬНО (MaxClusters) — для второго тика того же инстанса клэйм
        // «занят» так же, как для чужого: пока тик A держит секцию (поля
        // _lease/_payload гасятся только в ReleaseAsync), тик B получает false →
        // PortLockBusyException → waiting-portalloc-lock → следующий тик
        // (тиковая модель spec §2/§3.2). Локальная проверка ДО etcd-раунда, а не
        // reentrant-true: (1) второй конкурент НЕ входит в секцию concurrently —
        // иначе обе секции читают busy и пишут portalloc (сама гонка t90);
        // (2) не тратим grant+txn на заведомо занятый клэйм; (3) _lease/_payload
        // пишет ровно один держатель инстанса — перехват по истёкшему TTL не
        // перезапишет поля, и release зависшего тика не удалит чужой живой ключ
        // под ValueEqual с перезаписанным payload.
        lock (_sync)
        {
            if (_lease is not null)
                return Result<bool>.Success(false); // держит параллельный тик — ждём следующим тиком
        }

        var grant = await WithFailoverAsync(endpoint => gateway.LeaseGrantAsync(endpoint, TtlSec, ct));
        if (!grant.IsSuccess)
            return Result<bool>.Failed(grant.Error!);

        var payload = JsonSerializer.Serialize(new LockPayload(instanceId, Now()));
        var txn = await WithFailoverAsync(endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.NotExists(Key)],
                [new TxnOp.Put(Key, payload, grant.Value)]),
            ct));
        if (!txn.IsSuccess)
        {
            await RevokeSilentlyAsync(grant.Value);
            return Result<bool>.Failed(txn.Error!);
        }

        if (!txn.Value.Succeeded)
        {
            await RevokeSilentlyAsync(grant.Value);
            return Result<bool>.Success(false); // занят другим инстансом — не ошибка
        }

        lock (_sync)
        {
            _lease = grant.Value;
            _payload = payload;
        }

        return Result<bool>.Success(true);
    }

    /// <summary>Освобождение: del под compare ValueEqual(наш value) + revoke lease.
    /// Отказ del — best-effort (ключ гаснет по TTL); повтор/без захвата — no-op.</summary>
    public async Task ReleaseAsync()
    {
        long lease;
        string payload;
        lock (_sync)
        {
            if (_lease is not { } l)
                return;
            lease = l;
            payload = _payload!;
            _lease = null;
            _payload = null;
        }

        // Чужой лок не трогаем: compare не сошёлся (lease истёк, ключ перезаписан) → del не выполнится.
        _ = await WithFailoverAsync(endpoint => gateway.TxnAsync(
            endpoint,
            TxnRequest.Of(
                [TxnCompare.ValueEqual(Key, payload)],
                [new TxnOp.Delete(Key, Prefix: false)]),
            CancellationToken.None));
        await RevokeSilentlyAsync(lease);
    }

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

    // Failover по endpoints: первый успешный ответ выигрывает (паттерн ClaimStore).
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

    // Value ключа /pgworker/locks/portalloc (arch/14 §3.3).
    private sealed record LockPayload(
        [property: JsonPropertyName("instance")] string Instance,
        [property: JsonPropertyName("since_unix")] long SinceUnix);
}

/// <summary>Сигнал «глобальный portalloc-клэйм занят другим инстансом» (t90):
/// НЕ фейл — без бэкоффа; процесс возвращает InProgress (waiting-portalloc-lock),
/// следующий тик повторяет. Маркер-тип для ветки обработки рядом с FailAsync.</summary>
public sealed class PortLockBusyException() : Exception(
    $"{PortAllocLock.Key}: занят другим инстансом — повторить следующим тиком");
