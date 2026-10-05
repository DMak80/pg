using Npgsql;
using PgWorker.Backups;
using Shared.Core;

namespace PgWorker.WalReceiver;

/// <summary>Чтение restart_lsn слота источника через SQL (t27, arch/19 §3.1):
/// пустой префикс S3 — стартовая позиция приёмника. Слот отсутствует → Failed
/// (permanent-ветка ядра, код 4).</summary>
public sealed class NpgsqlSlotPositionReader(WalReceiverOptions o) : ISlotPositionReader
{
    public async Task<Result<ulong>> ReadRestartLsnAsync(string slot, CancellationToken ct)
    {
        try
        {
            await using var connection = new NpgsqlConnection(
                $"Host={o.PgHost};Port={o.PgPort};Username={o.PgUser};Password={o.PgPassword};" +
                $"Database={o.PgDbname};SSL Mode=Require;Trust Server Certificate=true;Timeout=10");
            await connection.OpenAsync(ct);
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name = $1";
            command.Parameters.AddWithValue(slot);
            var raw = await command.ExecuteScalarAsync(ct);
            if (raw is not string lsnText)
                return Result<ulong>.Failed(new ApplicationException($"слот {slot} отсутствует"));

            return Result<ulong>.Success(ParseLsn(lsnText));
        }
        catch (Exception e)
        {
            return Result<ulong>.Failed(new ApplicationException(
                $"restart_lsn {slot} @ {o.PgHost}:{o.PgPort}: {e.Message}", e));
        }
    }

    /// <summary>LSN PG-формата «X/Y» (hex) → ulong.</summary>
    internal static ulong ParseLsn(string lsn)
    {
        var parts = lsn.Split('/');
        return (ulong.Parse(parts[0], System.Globalization.NumberStyles.HexNumber) << 32)
               | ulong.Parse(parts[1], System.Globalization.NumberStyles.HexNumber);
    }
}
