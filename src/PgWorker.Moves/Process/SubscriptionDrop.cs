using PgWorker.Core;

namespace PgWorker.Moves;

/// <summary>
/// Срез подписки с fallback при недоступном источнике (t01 задача 15, ревью №3;
/// перенос drop_sub abort-move.sh): DROP SUBSCRIPTION → при сбое (источник
/// недоступен — срез слота удалённо невозможен): ALTER … DISABLE →
/// SET (slot_name = NONE) → DROP. Слот на источнике остаётся сиротой (имя слота =
/// имя подписки, PG-конвенция) и добивается вызывающим кодом отдельным шагом слотов.
/// Используют finalize (задача 15) и abort (задача 16).
/// </summary>
public static class SubscriptionDrop
{
    public static async Task<Result> DropAsync(IMoveSqlExecutor sql, string dsn, string sub, CancellationToken ct)
    {
        var drop = await sql.ExecuteAsync(dsn, MoveSql.DropSubscription(sub), ct);
        if (drop.IsSuccess)
            return drop; // слот на источнике срезан удалённо самой подпиской

        // Источник недоступен — срезаем подписку локально, отвязав слот.
        var disabled = await sql.ExecuteAsync(dsn, MoveSql.DisableSubscription(sub), ct);
        if (!disabled.IsSuccess)
            return Result.Failed(new ApplicationException(
                $"подписка {sub}: DROP и DISABLE не прошли — {drop.Error!.Message}", disabled.Error));

        var detached = await sql.ExecuteAsync(dsn, MoveSql.SetSlotNone(sub), ct);
        if (!detached.IsSuccess)
            return Result.Failed(new ApplicationException(
                $"подписка {sub}: не отвязать слот (slot_name=NONE) — {detached.Error!.Message}", detached.Error));

        var retry = await sql.ExecuteAsync(dsn, MoveSql.DropSubscription(sub), ct);
        return retry.IsSuccess
            ? retry
            : Result.Failed(new ApplicationException(
                $"подписка {sub}: локальный срез не дошёл до DROP — {retry.Error!.Message}", retry.Error));
    }
}
