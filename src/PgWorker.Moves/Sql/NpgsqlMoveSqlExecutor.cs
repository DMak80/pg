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

    // Поллинг-инвариант (arch/14 §6): одиночная SQL-операция не молчит дольше
    // окна проверки watchdog — таймауты подключения и команды = половина окна
    // (7 c при дефолтах 15 c); зависшая операция → исключение → transient-фейл,
    // а не молчание до дефолтных Npgsql 15/30 c.
    private const int SqlTimeoutSec = 7;

    private static NpgsqlConnection OpenConnection(string dsn)
    {
        var csb = new NpgsqlConnectionStringBuilder(dsn) { Timeout = SqlTimeoutSec };
        return new NpgsqlConnection(csb.ConnectionString);
    }

    private static NpgsqlCommand Command(string sql, NpgsqlConnection conn, int commandTimeoutSec)
        => new(sql, conn) { CommandTimeout = commandTimeoutSec };

    // Скалярный запрос (префлайт/пробы/счётчики; DBNull → null).
    public async Task<Result<object?>> ScalarAsync(string dsn, string sql, CancellationToken ct)
    {
        var pipeline = SqlRetryPolicies.SqlRetry(RetryCount, FirstRetryDelay);
        try
        {
            var value = await pipeline.ExecuteAsync(async token =>
            {
                await using var conn = OpenConnection(dsn);
                await conn.OpenAsync(token);
                await using var cmd = Command(sql, conn, SqlTimeoutSec);
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
        var pipeline = SqlRetryPolicies.SqlRetry(RetryCount, FirstRetryDelay);
        try
        {
            var rows = await pipeline.ExecuteAsync(async token =>
            {
                await using var conn = OpenConnection(dsn);
                await conn.OpenAsync(token);
                await using var cmd = Command(sql, conn, SqlTimeoutSec);
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
        var pipeline = SqlRetryPolicies.SqlRetry(RetryCount, FirstRetryDelay);
        try
        {
            await pipeline.ExecuteAsync(async token =>
            {
                await using var conn = OpenConnection(dsn);
                await conn.OpenAsync(token);
                await using var cmd = Command(sql, conn, SqlTimeoutSec);
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
            await using var conn = OpenConnection(dsn);
            await conn.OpenAsync(ct);
            await using var tx = await conn.BeginTransactionAsync(ct);
            // Command-бюджет обязан ПОКРЫВАТЬ lock_timeout: LOCK-барьер сам
            // ждёт lock_timeout (ретраи — задача процесса, не SQL-слоя); +2 —
            // запас тела батча (SET LOCAL + REVOKE×3); нижняя граница 7 —
            // поллинг-инвариант arch/14 §6 (дефолтный FreezeLockTimeoutSec = 5
            // → ровно 7 с, симметрия с остальными методами).
            var bodyTimeoutSec = Math.Max(SqlTimeoutSec, lockTimeoutSec + 2);
            await using (var setup = Command($"SET LOCAL lock_timeout = '{lockTimeoutSec}s'", conn, bodyTimeoutSec))
            {
                await setup.ExecuteNonQueryAsync(ct);
            }

            await using (var body = Command(sql, conn, bodyTimeoutSec))
            {
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
        => Result.Failed(new ApplicationException($"SQL failed [{Redact(dsn)}]: {e.Message}", e));

    // Дубль паттерна DatabaseProvisioner.Redact: «;»-Npgsql-пароли и libpq
    // «password=…» (в т.ч. quoted '…'); регистр не важен.
    [GeneratedRegex("password=(?:'[^']*'|[^; ]*)", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex PasswordRegex();

    private static string Redact(string dsn) => PasswordRegex().Replace(dsn, "password=***");
}
