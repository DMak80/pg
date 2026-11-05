using Shared.Core;
using Shared.Etcd.Client;

namespace PgWorker.Etcd.Backups;

/// <summary>Счётчик жизни цепочки WAL шарда (arch/backups/05 §1, spec §4.6):
/// `/pgworker/backups/&lt;C&gt;/&lt;X&gt;/wal-life`; отсутствие ключа = жизнь 0;
/// единственный писатель — воркер. Инкремент ТОЛЬКО при разрыве цепочки
/// (пересоздание шарда, restore/DR в тот же префикс, свежий детект чужой
/// цепочки). Идемпотентность: reason-гейт для foreign-chain (повторные детекты
/// той же лежащей чужой цепочки не считают), маркер времени для
/// restore/shard-rebuilt (`bumped_unix` ≥ старта заявки — уже сделано);
/// конкурентная запись — txn с ModRevisionEqual и одна повторная итерация.
/// Чтение/мутация — failover-перебор endpoints (паттерн ReadPasswordAsync).</summary>
public sealed class WalLifeStore(IEtcdGateway etcd, string[] endpoints)
{
    /// <summary>Текущее значение счётчика + мод-ревизия ключа (для txn-компарера);
    /// null — ключа нет (жизнь 0, запись не создана); битый JSON трактуется как
    /// жизнь 0 (толерантный читатель).</summary>
    public async Task<Result<(WalLifeState State, ulong ModRevision)?>> ReadAsync(
        string cluster, string shard, CancellationToken ct)
    {
        Result<Kv?> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, WalLifeKeys.Key(cluster, shard), ct);
            if (!result.IsSuccess)
            {
                last = result; // отказ — пробуем следующий endpoint (failover)
                continue;
            }
            if (result.Value is not { } kv)
                return Result<(WalLifeState, ulong)?>.Success(null);
            var modRevision = kv.ModRevision;
            return Result<(WalLifeState, ulong)?>.Success((
                WalLifeStateJson.TryParse(kv.Value) ?? new WalLifeState(0, null, null),
                modRevision));
        }

        return Result<(WalLifeState, ulong)?>.Failed(last.Error
            ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }

    /// <summary>Текущий номер жизни; отсутствие ключа/битый JSON — 0.</summary>
    public async Task<Result<uint>> ReadValueAsync(
        string cluster, string shard, CancellationToken ct)
    {
        var read = await ReadAsync(cluster, shard, ct);
        return read.IsSuccess
            ? Result<uint>.Success(read.Value?.State.ChainLife ?? 0)
            : Result<uint>.Failed(read.Error!);
    }

    /// <summary>Инкремент счётчика (spec §4.6, решения №31/№32): гейты «уже
    /// сделано» (reason-гейт foreign-chain, маркер времени заявки), затем txn
    /// (нет ключа — NotExists + chain_life=1; есть — ModRevisionEqual + N+1);
    /// гонка (compare не сошёлся) — ОДНА повторная итерация от свежего значения.</summary>
    public async Task<Result<uint>> BumpAsync(
        string cluster, string shard, WalLifeReason reason, long? startedUnix, CancellationToken ct)
    {
        var key = WalLifeKeys.Key(cluster, shard);
        for (var iteration = 0; iteration < 2; iteration++)
        {
            var read = await ReadAsync(cluster, shard, ct);
            if (!read.IsSuccess)
                return Result<uint>.Failed(read.Error!);
            var current = read.Value;

            // (2) Гейты «уже сделано».
            if (current is { } existing)
            {
                if (reason == WalLifeReason.ForeignChain
                    && existing.State.Reason == WalLifeReason.ForeignChain)
                    return Result<uint>.Success(existing.State.ChainLife); // повторный детект — молчит
                if (reason != WalLifeReason.ForeignChain
                    && startedUnix is { } started
                    && existing.State.Reason == reason
                    && existing.State.BumpedUnix is { } bumped
                    && bumped >= started)
                    return Result<uint>.Success(existing.State.ChainLife); // bump заявки уже был
            }

            var next = new WalLifeState(
                (current?.State.ChainLife ?? 0) + 1,
                DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                reason);
            // (3) txn: ключа нет — NotExists (жизнь 1); есть — ревизия не менялась.
            var compare = current is { } present
                ? new[] { TxnCompare.ModRevisionEqual(key, (long)present.ModRevision) }
                : [TxnCompare.NotExists(key)];
            var txn = TxnRequest.Of(
                compare, [new TxnOp.Put(key, WalLifeStateJson.Serialize(next), null)]);

            var result = await TxnWithFailoverAsync(txn, ct);
            if (!result.IsSuccess)
                return Result<uint>.Failed(result.Error!);
            if (result.Value.Succeeded)
                return Result<uint>.Success(next.ChainLife);
            // (4) гонка с другим bump'ом — повторная итерация от свежего чтения
        }

        return Result<uint>.Failed(new ApplicationException(
            $"wal-life bump {cluster}/{shard}: concurrent modification (two iterations lost)"));
    }

    /// <summary>Свежий факт гварда «чужая цепочка» в статус-ключе приёмника
    /// (arch/backups/02-wal-archive.md §1): state failed_permanent с причиной foreign WAL chain — триггер
    /// инкремента жизни (только PgWorker пишет счётчик). Предикат над полями
    /// факта: WalReceiverAgentFacts живёт в слое воркера (Backups).</summary>
    public static bool IsForeignChainFact(string? state, string? error)
        => state == "failed_permanent"
           && error?.Contains("foreign WAL chain", StringComparison.Ordinal) == true;

    // txn с failover-перебором endpoints: транспортный отказ — следующая нода;
    // Succeeded=false — достоверный ответ (compare не сошёлся), не ретраится здесь.
    private async Task<Result<TxnResult>> TxnWithFailoverAsync(TxnRequest request, CancellationToken ct)
    {
        Result<TxnResult> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.TxnAsync(endpoint, request, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return Result<TxnResult>.Failed(last.Error
            ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }
}
