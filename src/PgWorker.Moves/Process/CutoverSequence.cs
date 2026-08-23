using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Provisioning.Endpoints;

namespace PgWorker.Moves;

/// <summary>Фазы cutover-блока (значения — как в move-bucket.sh; пишутся в статус-ключ).</summary>
public static class CutoverPhases
{
    public const string Frozen = "frozen";
    public const string Verify = "verify";
    public const string Flip = "flip";
    public const string FreezeFailed = "freeze-failed";
    public const string LsnFailed = "lsn-failed";
    public const string CatchupTimeout = "catchup-timeout";
    public const string SequencesFailed = "sequences-failed";
    public const string VerifyFailed = "verify-failed";
}

/// <summary>
/// Перманентный отказ cutover (t01 задача 12, ревью №1, spec §6.2 п.6/п.7):
/// verify-failed (дефектная копия P8 — разморозка сделана) и flip-conflict
/// (routing изменился под руками — заморозка ОСТАВЛЕНА). MoveProcess по этому
/// исключению удаляет заявку и пишет подсказку в журнал; обычные исключения —
/// transient (заявка жива, ретраи тиками).
/// </summary>
public sealed class CutoverPermanentException(string reason) : Exception(reason);

/// <summary>Параметры cutover-блока (spec §6.2): общий для move и rollback.</summary>
/// <param name="Cluster">Кластер (ключи etcd).</param>
/// <param name="Bucket">Бакет-схема.</param>
/// <param name="Cur">Текущий владелец = источник cutover.</param>
/// <param name="New">Новый владелец = приёмник.</param>
/// <param name="Slot">Слот подтверждения (живёт на Cur — создан подпиской приёмника).</param>
/// <param name="FailState">Статус при отказе до flip ("SYNCING" для move).</param>
/// <param name="DropStatusOnFail">Rollback-семантика: fail-пути удаляют статус-ключ (нет ключа = ACTIVE).</param>
public sealed record CutoverContext(
    string Cluster,
    string Bucket,
    string Cur,
    string New,
    string Slot,
    string FailState,
    bool DropStatusOnFail = false);

/// <summary>
/// Cutover — единый непрерывный блок одного тика (t01 задача 12, spec §6.2; точный
/// перенос cutover_flip move-bucket.sh): заморозка P1/P5 (REVOKE×3 + барьер LOCK в
/// одной транзакции, до FreezeLockTries) → FROZEN/frozen + пауза роутера → LSN
/// источника → ожидание слота (лаг 0, таймаут CutoverTimeoutSec) → sequences P6
/// (setval только вперёд) → сверка строк P8 → атомарный flip-txn (compare routing
/// → put+delete status). Отказ до flip: разморозка + возврат FailState (transient);
/// verify-failed и flip-conflict — CutoverPermanentException (ревью №1).
/// </summary>
public sealed class CutoverSequence(
    IMoveSqlExecutor sql,
    MoveStatusStore status,
    InstallSecrets secrets)
{
    /// <summary>
    /// true = flip прошёл. Failed-исходы: transient (обычное исключение; заморозка снята,
    /// статус = FailState/фаза) — freeze-failed / lsn-failed / catchup-timeout /
    /// sequences-failed; permanent (CutoverPermanentException) — verify-failed
    /// (разморозка сделана, статус FailState/verify-failed) и flip-conflict
    /// (заморозка ОСТАВЛЕНА — P1-призраки до разбора вручную).
    /// </summary>
    public async Task<Result<bool>> RunAsync(
        ShardEndpoints shards,
        ClusterSnapshot snap,
        CutoverContext c,
        MovesRuntimeOptions o,
        CancellationToken ct,
        Func<CancellationToken, Task<Result>>? snapshot = null)
    {
        // DSN мастеров cur/new (SQL — к мастерам, P2).
        var dsns = await ResolveDsnsAsync(shards, snap, c, ct);
        if (!dsns.IsSuccess)
            return Result<bool>.Failed(dsns.Error!);
        var (curDsn, newDsn) = dsns.Value;

        // База статусов cutover: started_unix наследуется от текущего переезда.
        var existing = await status.GetAsync(c.Cluster, c.Bucket, ct);
        if (!existing.IsSuccess)
            return Result<bool>.Failed(existing.Error!);
        var startedUnix = existing.Value?.StartedUnix ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        // 1. Заморозка P1/P5 — до FreezeLockTries, пауза PollIntervalSec между попытками.
        var freeze = Result.Failed(new ApplicationException("заморозка не выполнялась"));
        for (var attempt = 1; attempt <= Math.Max(1, o.FreezeLockTries); attempt++)
        {
            var names = await sql.ScalarAsync(curDsn, MoveSql.TableNames(c.Bucket), ct);
            if (names.IsSuccess)
            {
                freeze = await sql.ExecuteTransactionalAsync(
                    curDsn, MoveSql.Freeze(c.Bucket, MoveNames.AppRole, ToText(names.Value)),
                    o.FreezeLockTimeoutSec, ct);
                if (freeze.IsSuccess)
                    break;
            }
            else
            {
                freeze = Result.Failed(names.Error!);
            }

            if (attempt < o.FreezeLockTries)
                await Task.Delay(TimeSpan.FromSeconds(o.PollIntervalSec), ct);
        }

        // Транзакция заморозки атомарна: fail = откат — разморозка не нужна.
        if (!freeze.IsSuccess)
            return await FailAsync(c, curDsn, startedUnix, CutoverPhases.FreezeFailed, freeze.Error!, unfreeze: false, ct);

        // 2. FROZEN/frozen + пауза TTL кэша роутера (роутер перестаёт писать в старого).
        var frozen = await PutPhaseAsync(c, startedUnix, MoveStates.Frozen, CutoverPhases.Frozen, ct);
        if (!frozen.IsSuccess)
            return Result<bool>.Failed(frozen.Error!);
        await Task.Delay(TimeSpan.FromSeconds(o.FreezeWaitSec), ct);

        // 3. Целевой LSN последней записи источника.
        var lsn = await sql.ScalarAsync(curDsn, MoveSql.CurrentWalLsn(), ct);
        if (!lsn.IsSuccess)
            return await FailAsync(c, curDsn, startedUnix, CutoverPhases.LsnFailed, lsn.Error!, unfreeze: true, ct);
        if (ToText(lsn.Value) is not { } lsnText)
            return await FailAsync(c, curDsn, startedUnix, CutoverPhases.LsnFailed,
                new ApplicationException("пустой pg_current_wal_lsn на источнике"), unfreeze: true, ct);

        // 4. Ожидание слота: активен и подтвердил LSN; таймаут → разморозка + transient.
        var waited = 0;
        while (true)
        {
            var caught = await sql.ScalarAsync(curDsn, MoveSql.SlotCaughtUp(c.Slot, lsnText), ct);
            if (caught.IsSuccess && ToBool(caught.Value) == true)
                break;

            if (waited >= o.CutoverTimeoutSec)
                return await FailAsync(c, curDsn, startedUnix, CutoverPhases.CatchupTimeout,
                    new ApplicationException(
                        $"слот {c.Slot} не подтвердил LSN за {o.CutoverTimeoutSec}с — разморозил, репликация продолжает догонять (перезапусти позже)"),
                    unfreeze: true, ct);

            await Task.Delay(TimeSpan.FromSeconds(o.PollIntervalSec), ct);
            waited += o.PollIntervalSec;
        }

        // 5. Sequences P6: issued на источнике → next на приёмнике; setval только вперёд.
        var sequences = await sql.ListAsync(curDsn, MoveSql.SequenceNames(c.Bucket), ct);
        if (!sequences.IsSuccess)
            return await FailAsync(c, curDsn, startedUnix, CutoverPhases.SequencesFailed, sequences.Error!, unfreeze: true, ct);
        foreach (var sequence in sequences.Value)
        {
            var issued = await sql.ScalarAsync(curDsn, MoveSql.SequenceIssued(c.Bucket, sequence), ct);
            if (!issued.IsSuccess)
                return await FailAsync(c, curDsn, startedUnix, CutoverPhases.SequencesFailed, issued.Error!, unfreeze: true, ct);
            var next = await sql.ScalarAsync(newDsn, MoveSql.SequenceNext(c.Bucket, sequence), ct);
            if (!next.IsSuccess)
                return await FailAsync(c, curDsn, startedUnix, CutoverPhases.SequencesFailed,
                    new ApplicationException($"sequence '{sequence}' отсутствует на '{c.New}' (дрейф P5?) — {next.Error!.Message}"),
                    unfreeze: true, ct);

            if (ToLong(next.Value) <= ToLong(issued.Value))
            {
                var setval = await sql.ExecuteAsync(
                    newDsn, MoveSql.SetvalForward(c.Bucket, sequence, ToLong(issued.Value)), ct);
                if (!setval.IsSuccess)
                    return await FailAsync(c, curDsn, startedUnix, CutoverPhases.SequencesFailed, setval.Error!, unfreeze: true, ct);
            }
        }

        // 6. Сверка строк P8: лаг 0 не гарантирует полноты копии после failover приёмника.
        var verify = await PutPhaseAsync(c, startedUnix, MoveStates.Frozen, CutoverPhases.Verify, ct);
        if (!verify.IsSuccess)
            return Result<bool>.Failed(verify.Error!);
        var tables = await sql.ScalarAsync(curDsn, MoveSql.TableNames(c.Bucket), ct);
        if (!tables.IsSuccess)
            return await FailAsync(c, curDsn, startedUnix, CutoverPhases.VerifyFailed, tables.Error!, unfreeze: true, ct);
        foreach (var table in ParseTables(ToText(tables.Value)))
        {
            var src = await sql.ScalarAsync(curDsn, MoveSql.RowCount(c.Bucket, table), ct);
            if (!src.IsSuccess)
                return await FailAsync(c, curDsn, startedUnix, CutoverPhases.VerifyFailed, src.Error!, unfreeze: true, ct);
            var dst = await sql.ScalarAsync(newDsn, MoveSql.RowCount(c.Bucket, table), ct);
            if (!dst.IsSuccess)
                return await FailAsync(c, curDsn, startedUnix, CutoverPhases.VerifyFailed, dst.Error!, unfreeze: true, ct);
            if (ToLong(src.Value) != ToLong(dst.Value))
                return await FailAsync(c, curDsn, startedUnix, CutoverPhases.VerifyFailed,
                    new CutoverPermanentException(
                        $"сверка строк не сошлась — копия дефектна (P8, таблица {table}: {ToLong(src.Value)} против {ToLong(dst.Value)}): abort + повторный move"),
                    unfreeze: true, ct);
        }

        // 7. FROZEN/flip + атомарный flip-txn: compare routing=cur → put new + delete status.
        var flipping = await PutPhaseAsync(c, startedUnix, MoveStates.Frozen, CutoverPhases.Flip, ct);
        if (!flipping.IsSuccess)
            return Result<bool>.Failed(flipping.Error!);

        var flip = await status.FlipAsync(c.Cluster, c.Bucket, c.Cur, c.New, ct);
        if (!flip.IsSuccess)
            return Result<bool>.Failed(flip.Error!); // etcd-сбой: заморозка оставлена, тик повторит (transient)
        if (flip.Value != true)
            return Result<bool>.Failed(new CutoverPermanentException(
                "flip-conflict: routing изменился под руками — заморозка оставлена, разбор вручную"));

        // P12-снапшот точки «переключил на нового владельца» — best-effort (flip уже
        // случился, сбоем cutover не роняем; журнал/лог — задача вызывающего процесса).
        if (snapshot is not null)
            await snapshot(ct);

        return Result<bool>.Success(true);
    }

    // Отказ до flip: разморозка best-effort (GRANT-симметрия, как unfreeze || true
    // скрипта) + статус FailState/фаза (или удаление при DropStatusOnFail) + Failed.
    private async Task<Result<bool>> FailAsync(
        CutoverContext c, string curDsn, long startedUnix, string phase, Exception error,
        bool unfreeze, CancellationToken ct)
    {
        if (unfreeze)
            await sql.ExecuteAsync(curDsn, MoveSql.Unfreeze(c.Bucket, MoveNames.AppRole), ct);

        if (c.DropStatusOnFail)
        {
            var deleted = await status.DeleteAsync(c.Cluster, c.Bucket, ct);
            if (!deleted.IsSuccess)
                return Result<bool>.Failed(deleted.Error!);
        }
        else
        {
            var put = await PutPhaseAsync(c, startedUnix, c.FailState, phase, ct);
            if (!put.IsSuccess)
                return Result<bool>.Failed(put.Error!);
        }

        return Result<bool>.Failed(error);
    }

    private Task<Result> PutPhaseAsync(
        CutoverContext c, long startedUnix, string state, string phase, CancellationToken ct)
        => status.PutAsync(c.Cluster, new MoveStatus(
            c.Bucket, state, c.Cur, c.New, startedUnix, DateTimeOffset.UtcNow.ToUnixTimeSeconds(), phase), ct);

    private async Task<Result<(string Cur, string New)>> ResolveDsnsAsync(
        ShardEndpoints shards, ClusterSnapshot snap, CutoverContext c, CancellationToken ct)
    {
        var curShard = snap.Shards.FirstOrDefault(s => s.Name == c.Cur);
        if (curShard is null)
            return Result<(string, string)>.Failed(new ApplicationException(
                $"cutover {c.Cluster}/{c.Bucket}: шард-источник '{c.Cur}' не найден в снапшоте"));
        var newShard = snap.Shards.FirstOrDefault(s => s.Name == c.New);
        if (newShard is null)
            return Result<(string, string)>.Failed(new ApplicationException(
                $"cutover {c.Cluster}/{c.Bucket}: шард-приёмник '{c.New}' не найден в снапшоте"));

        var addresses = await shards.ReadPortAllocAsync(c.Cluster, ct);
        if (!addresses.IsSuccess)
            return Result<(string, string)>.Failed(addresses.Error!);

        var curMaster = await shards.ResolveMasterAsync(curShard, addresses.Value, ct);
        if (!curMaster.IsSuccess)
            return Result<(string, string)>.Failed(curMaster.Error!);
        if (curMaster.Value is null)
            return Result<(string, string)>.Failed(new ApplicationException(
                $"cutover {c.Cluster}/{c.Bucket}: мастер '{c.Cur}' не определён — ждём"));

        var newMaster = await shards.ResolveMasterAsync(newShard, addresses.Value, ct);
        if (!newMaster.IsSuccess)
            return Result<(string, string)>.Failed(newMaster.Error!);
        if (newMaster.Value is null)
            return Result<(string, string)>.Failed(new ApplicationException(
                $"cutover {c.Cluster}/{c.Bucket}: мастер '{c.New}' не определён — ждём"));

        return Result<(string, string)>.Success((
            ShardEndpoints.AdminDsn(curMaster.Value, snap.Config.DbName, secrets),
            ShardEndpoints.AdminDsn(newMaster.Value, snap.Config.DbName, secrets)));
    }

    // Агрегат TableNames ("sch.\"t1\", sch.\"t2\"") → имена таблиц без схемы/кавычек.
    private static IReadOnlyList<string> ParseTables(string? aggregated)
    {
        if (string.IsNullOrWhiteSpace(aggregated))
            return [];
        return aggregated
            .Split(", ", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(entry =>
            {
                var dot = entry.IndexOf('.');
                return (dot < 0 ? entry : entry[(dot + 1)..]).Trim('"');
            })
            .ToList();
    }

    // Скаляры Npgsql типизированы (bool/long/string); бэш-скрипты — t/f.
    private static string? ToText(object? value) => value?.ToString();

    private static bool? ToBool(object? value) => value switch
    {
        bool b => b,
        "t" or "true" or "True" => true,
        "f" or "false" or "False" => false,
        _ => long.TryParse(ToText(value), out var number) ? number != 0 : null,
    };

    private static long ToLong(object? value)
        => long.TryParse(ToText(value), out var number) ? number : 0;
}
