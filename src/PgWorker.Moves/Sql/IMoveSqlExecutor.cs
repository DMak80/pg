using PgWorker.Core;

namespace PgWorker.Moves;

/// <summary>
/// SQL-грань процессов переезда (мокабельная, образец ISqlExecutor):
/// скаляры/списки/батчи над DSN шардов + транзакция заморозки с lock_timeout.
/// </summary>
public interface IMoveSqlExecutor
{
    Task<Result<object?>> ScalarAsync(string dsn, string sql, CancellationToken ct);

    // Пустой результат → [] (построчные списки скриптов: sequences/инвентарь/слоты).
    Task<Result<IReadOnlyList<string>>> ListAsync(string dsn, string sql, CancellationToken ct);

    Task<Result> ExecuteAsync(string dsn, string sql, CancellationToken ct);

    // Freeze: ОДНА транзакция: SET LOCAL lock_timeout → батч; longrun-транзакция
    // без Polly-обёртки (LOCK сам ждёт lock_timeout — ретраи ставит процесс).
    Task<Result> ExecuteTransactionalAsync(string dsn, string sql, int lockTimeoutSec, CancellationToken ct);
}
