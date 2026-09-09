using Npgsql;
using PgWorker.Core;

namespace PgWorker.Backups.Sql;

/// <summary>Npgsql-исполнение слот/LSN-SQL (t03): без ретраев — процесс ретраит
/// тиками; ошибка → Result.Failed (transient).</summary>
public sealed class NpgsqlWalSqlExecutor : IWalSqlExecutor
{
    public async Task<Result<bool>> SlotExistsAsync(string adminDsn, string slot, CancellationToken ct)
    {
        try
        {
            await using var connection = new NpgsqlConnection(adminDsn);
            await connection.OpenAsync(ct);
            await using var command = new NpgsqlCommand(
                "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
                connection) { Parameters = { new() { Value = slot } } };
            var exists = (bool)(await command.ExecuteScalarAsync(ct))!;
            return Result<bool>.Success(exists);
        }
        catch (Exception e)
        {
            return Result<bool>.Failed(new ApplicationException($"слот-зонд {slot}: {e.Message}", e));
        }
    }

    public async Task<Result> EnsureSlotAsync(string adminDsn, string slot, CancellationToken ct)
    {
        try
        {
            await using var connection = new NpgsqlConnection(adminDsn);
            await connection.OpenAsync(ct);
            // immediate+reserved: WAL копится на мастере до подтверждения приёма (§3);
            // повтор при живом слоте падает duplicate_object — идемпотентность проверкой выше.
            await using var command = new NpgsqlCommand(
                "SELECT pg_create_physical_replication_slot($1, true)", connection)
            {
                Parameters = { new() { Value = slot } },
            };
            await command.ExecuteNonQueryAsync(ct);
            return Result.Success();
        }
        catch (PostgresException e) when (e.SqlState == "42710") // duplicate_object
        {
            return Result.Success(); // слот уже есть — идемпотентность
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException($"ensure слота {slot}: {e.Message}", e));
        }
    }

    public async Task<Result<(string Lsn, int Tli)>> CurrentWalAsync(string adminDsn, CancellationToken ct)
    {
        try
        {
            await using var connection = new NpgsqlConnection(adminDsn);
            await connection.OpenAsync(ct);
            await using var command = new NpgsqlCommand(
                "SELECT pg_current_wal_lsn()::text, (pg_control_checkpoint()).timeline_id", connection);
            await using var reader = await command.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            return Result<(string, int)>.Success((reader.GetString(0), reader.GetInt32(1)));
        }
        catch (Exception e)
        {
            return Result<(string, int)>.Failed(new ApplicationException($"LSN-зонд: {e.Message}", e));
        }
    }
}
