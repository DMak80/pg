using System.Text.RegularExpressions;
using Npgsql;
using PgWorker.Core;
using PgWorker.Core.Retry;

namespace PgWorker.Moves;

/// <summary>
/// Npgsql-реализация IMoveSqlExecutor (образец — DatabaseProvisioner):
/// короткие операции под Polly SqlRetry(3, 1s); транзакция заморозки — без
/// обёртки (LOCK сам ждёт lock_timeout). Ошибки — через WrapError: DSN в
/// сообщении с ред-паролем (P12/P17).
/// </summary>
public sealed partial class NpgsqlMoveSqlExecutor : IMoveSqlExecutor
{
    private const int RetryCount = 3;

    private static readonly TimeSpan FirstRetryDelay = TimeSpan.FromSeconds(1);

    // Скалярный запрос (префлайт/пробы/счётчики; DBNull → null).
    public async Task<Result<object?>> ScalarAsync(string dsn, string sql, CancellationToken ct)
    {
        var pipeline = RetryPolicies.SqlRetry(RetryCount, FirstRetryDelay);
        try
        {
            var value = await pipeline.ExecuteAsync(async token =>
            {
                await using var conn = new NpgsqlConnection(dsn);
                await conn.OpenAsync(token);
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                return await cmd.ExecuteScalarAsync(token);
            }, ct);
            return Result<object?>.Success(value is DBNull ? null : value);
        }
        catch (Exception e)
        {
            return Result<object?>.Failed(WrapError(dsn, e).Error!);
        }
    }

    // Построчный список (sequences/инвентарь/слоты): пустой ответ → [].
    public async Task<Result<IReadOnlyList<string>>> ListAsync(string dsn, string sql, CancellationToken ct)
    {
        var pipeline = RetryPolicies.SqlRetry(RetryCount, FirstRetryDelay);
        try
        {
            var rows = await pipeline.ExecuteAsync(async token =>
            {
                await using var conn = new NpgsqlConnection(dsn);
                await conn.OpenAsync(token);
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                await using var reader = await cmd.ExecuteReaderAsync(token);
                var items = new List<string>();
                while (await reader.ReadAsync(token))
                {
                    items.Add(reader.IsDBNull(0) ? string.Empty : reader.GetString(0));
                }

                return items;
            }, ct);
            return Result<IReadOnlyList<string>>.Success(rows);
        }
        catch (Exception e)
        {
            return Result<IReadOnlyList<string>>.Failed(WrapError(dsn, e).Error!);
        }
    }

    // Исполнить батч (ExecuteNonQuery: REVOKE/GRANT/CREATE/DROP).
    public async Task<Result> ExecuteAsync(string dsn, string sql, CancellationToken ct)
    {
        var pipeline = RetryPolicies.SqlRetry(RetryCount, FirstRetryDelay);
        try
        {
            await pipeline.ExecuteAsync(async token =>
            {
                await using var conn = new NpgsqlConnection(dsn);
                await conn.OpenAsync(token);
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                await cmd.ExecuteNonQueryAsync(token);
            }, ct);
            return Result.Success();
        }
        catch (Exception e)
        {
            return WrapError(dsn, e);
        }
    }

    // Транзакция заморозки: SET LOCAL lock_timeout и тело батча — в ОДНОЙ
    // транзакции (барьер LOCK обязан видеть те же REVOKE, что и читатели);
    // без Polly: LOCK сам ждёт lock_timeout, внешние ретраи — задача процесса
    // (FreezeLockTries, пауза PollIntervalSec).
    public async Task<Result> ExecuteTransactionalAsync(string dsn, string sql, int lockTimeoutSec, CancellationToken ct)
    {
        try
        {
            await using var conn = new NpgsqlConnection(dsn);
            await conn.OpenAsync(ct);
            await using var tx = await conn.BeginTransactionAsync(ct);
            await using (var setup = conn.CreateCommand())
            {
                setup.CommandText = $"SET LOCAL lock_timeout = '{lockTimeoutSec}s'";
                await setup.ExecuteNonQueryAsync(ct);
            }

            await using (var body = conn.CreateCommand())
            {
                body.CommandText = sql;
                await body.ExecuteNonQueryAsync(ct);
            }

            await tx.CommitAsync(ct);
            return Result.Success();
        }
        catch (Exception e)
        {
            return WrapError(dsn, e);
        }
    }

    // Ошибка с ред-DSN в сообщении: пароль не должен попадать в логи/журнал.
    internal static Result WrapError(string dsn, Exception e)
        => Result.Failed(new ApplicationException($"SQL не выполнен [{Redact(dsn)}]: {e.Message}", e));

    // Дубль паттерна DatabaseProvisioner.Redact: «;»-Npgsql-пароли и libpq
    // «password=…» (в т.ч. quoted '…'); регистр не важен.
    [GeneratedRegex("password=(?:'[^']*'|[^; ]*)", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex PasswordRegex();

    private static string Redact(string dsn) => PasswordRegex().Replace(dsn, "password=***");
}
