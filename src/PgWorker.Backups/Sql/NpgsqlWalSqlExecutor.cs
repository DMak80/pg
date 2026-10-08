using Npgsql;
using PgWorker.Core;

namespace PgWorker.Backups.Sql;

/// <summary>Npgsql-исполнение слот/LSN-SQL: без ретраев — процесс ретраит
/// тиками; ошибка → Result.Failed (transient).</summary>
public sealed class NpgsqlWalSqlExecutor : IWalSqlExecutor
{
    public async Task<Result<(bool Exists, string? WalStatus)>> SlotProbeAsync(string adminDsn, string slot, CancellationToken ct)
    {
        try
        {
            await using var connection = new NpgsqlConnection(adminDsn);
            await connection.OpenAsync(ct);
            await using var command = new NpgsqlCommand(
                "SELECT wal_status FROM pg_replication_slots WHERE slot_name = $1", connection)
            {
                Parameters = { new() { Value = slot } },
            };
            await using var reader = await command.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
                return Result<(bool, string?)>.Success((false, null)); // строки нет
            // null-статус существующего слота (старые PG/edge) — как живой: лечим только 'lost'
            var status = reader.IsDBNull(0) ? null : reader.GetString(0);
            return Result<(bool, string?)>.Success((true, status));
        }
        catch (Exception e)
        {
            return Result<(bool, string?)>.Failed(new ApplicationException($"слот-зонд {slot}: {e.Message}", e));
        }
    }

    public async Task<Result> EnsureSlotAliveAsync(string adminDsn, string slot, CancellationToken ct)
    {
        var probe = await SlotProbeAsync(adminDsn, slot, ct);
        if (!probe.IsSuccess)
            return Result.Failed(probe.Error!);
        if (!probe.Value.Exists)
            return await CreateSlotAsync(adminDsn, slot, ct); // отсутствует → create
        if (probe.Value.WalStatus == "lost")
            return await RecreateSlotAsync(adminDsn, slot, ct); // потерян → drop+create
        return Result.Success(); // жив — не трогать
    }

    public async Task<Result> RecreateSlotAsync(string adminDsn, string slot, CancellationToken ct)
    {
        try
        {
            await using var connection = new NpgsqlConnection(adminDsn);
            await connection.OpenAsync(ct);
            try
            {
                await using var drop = new NpgsqlCommand(
                    "SELECT pg_drop_replication_slot($1)", connection)
                {
                    Parameters = { new() { Value = slot } },
                };
                await drop.ExecuteNonQueryAsync(ct);
            }
            catch (PostgresException e) when (e.SqlState == "42704") // undefined_object
            {
                // слота нет — drop пропущен, create ниже создаст
            }

            return await CreateSlotAsync(adminDsn, slot, ct);
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException($"recreate слота {slot}: {e.Message}", e));
        }
    }

    // immediate+reserved (arch/19 §3): повтор при живом слоте — duplicate_object →
    // успех (идемпотентность); общий create для ensure/recreate.
    private static async Task<Result> CreateSlotAsync(string adminDsn, string slot, CancellationToken ct)
    {
        try
        {
            await using var connection = new NpgsqlConnection(adminDsn);
            await connection.OpenAsync(ct);
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
            return Result.Failed(new ApplicationException($"create слота {slot}: {e.Message}", e));
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
