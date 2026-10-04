using Npgsql.Replication;
using NpgsqlTypes;
using PgWorker.Backups;
using Shared.Core;

namespace PgWorker.WalReceiver;

/// <summary>Транспорт физической репликации Npgsql 10 (t27, arch/19 §3.1):
/// SQL-коннект — сверка wal_segment_size (permanent при расхождении с
/// WalFileName.SegmentBytes — математика имён не должна молча врать);
/// PhysicalReplicationConnection — START_REPLICATION SLOT ... PHYSICAL <lsn>
/// с явной стартовой позицией; Stream — XLogData → XLogChunk; SetConfirmed —
/// standby status update: apply/flush = подтверждённая LSN (Npgsql отвечает
/// серверным keepalive последним выставленным статусом, потому подтверждение
/// ставится ТОЛЬКО после успешного S3-put — инвариант RPO); TimelineHistory —
/// публичный API Npgsql (вердикт t27: экспонируется — fallback не нужен).</summary>
public sealed class NpgsqlXLogSource(WalReceiverOptions o) : IXLogReplicationSource
{
    private const string ConnStringTemplate =
        "Host={0};Port={1};Username={2};Password={3};Database={4};SSL Mode=Require;Trust Server Certificate=true;Timeout=10";

    private PhysicalReplicationConnection? _replication;
    private IAsyncEnumerable<XLogDataMessage>? _messages;

    private string ConnString => string.Format(
        ConnStringTemplate, o.PgHost, o.PgPort, o.PgUser, o.PgPassword, o.PgDbname);

    public async Task<Result> OpenAsync(CancellationToken ct)
    {
        try
        {
            // Сверка wal_segment_size обычным SQL-коннектом (replication-режим
            // не обязан для SHOW; проверка ДО старта репликации).
            await using var connection = new Npgsql.NpgsqlConnection(ConnString);
            await connection.OpenAsync(ct);
            await using var command = connection.CreateCommand();
            command.CommandText = "SHOW wal_segment_size";
            var raw = (string)(await command.ExecuteScalarAsync(ct))!;
            if (ParseSegmentSize(raw) is not { } segmentBytes || segmentBytes != WalFileName.SegmentBytes)
                return Result.Failed(new ApplicationException(
                    $"wal_segment_size источника {raw} ≠ {WalFileName.SegmentBytes} байт"));

            // Репликационное соединение (walsender-режим Npgsql).
            var replication = new PhysicalReplicationConnection(ConnString);
            await replication.Open(ct);
            _replication = replication;
            return Result.Success();
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException($"open {o.PgHost}:{o.PgPort}: {e.Message}", e));
        }
    }

    public async Task<Result> StartReplicationAsync(string slot, ulong startLsn, CancellationToken ct)
    {
        try
        {
            var replication = _replication ?? throw new InvalidOperationException("OpenAsync не выполнен");
            var physicalSlot = new PhysicalReplicationSlot(slot);
            _messages = replication.StartReplication(
                physicalSlot, (NpgsqlLogSequenceNumber)startLsn, ct);
            return Result.Success();
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException($"START_REPLICATION {slot}: {e.Message}", e));
        }
    }

    public IAsyncEnumerable<XLogChunk> Stream => ReadChunksAsync();

    private async IAsyncEnumerable<XLogChunk> ReadChunksAsync()
    {
        var messages = _messages ?? throw new InvalidOperationException("StartReplicationAsync не выполнен");
        await foreach (var message in messages)
        {
            // Data — поток, валиден только до следующего сообщения: вычитываем целиком
            // (буфер сегмента в памяти — кэш без durability, MULTI-HOST).
            using var buffer = new MemoryStream();
            await message.Data.CopyToAsync(buffer);
            yield return new XLogChunk((ulong)message.WalStart, buffer.ToArray());
        }
    }

    public async Task<Result> SetConfirmedAsync(ulong confirmedLsn, CancellationToken ct)
    {
        try
        {
            var replication = _replication ?? throw new InvalidOperationException("OpenAsync не выполнен");
            replication.SetReplicationStatus((NpgsqlLogSequenceNumber)confirmedLsn);
            await replication.SendStatusUpdate(ct);
            return Result.Success();
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException(
                $"status update {WalReceiverMarkers.LsnText(confirmedLsn)}: {e.Message}", e));
        }
    }

    public async Task<Result<byte[]?>> ReadTimelineHistoryAsync(uint tli, CancellationToken ct)
    {
        // TIMELINE_HISTORY — replication-команда: сервер обслуживает её ТОЛЬКО до
        // START_REPLICATION (E2E-факт t27: запрос по стрим-коннекту не работает) —
        // отдельное replication-соединение на каждый запрос (короткоживущее).
        try
        {
            await using var history = new PhysicalReplicationConnection(ConnString);
            await history.Open(ct);
            var file = await history.TimelineHistory(tli, ct);
            return Result<byte[]?>.Success(file.Content);
        }
        catch (Exception)
        {
            // Рантайм не смог отдать историю (нет файла/ошибка протокола) — валидное
            // «нет»: fallback-контроль воркера (arch/19 §3).
            return Result<byte[]?>.Success(null);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_replication is { } replication)
        {
            await replication.DisposeAsync();
            _replication = null;
        }
    }

    /// <summary>Разбор SHOW wal_segment_size: «16MB»/«16777216» → байты
    /// (двоичные суффиксы PostgreSQL: kB=1024, MB=1024², GB=1024³).</summary>
    internal static long? ParseSegmentSize(string raw)
    {
        var text = raw.Trim();
        if (long.TryParse(text, out var plain))
            return plain;

        if (text.Length < 3 || !long.TryParse(text[..^2].Trim(), out var value))
            return null;

        return text[^2..].ToLowerInvariant() switch
        {
            "kb" => value * 1024L,
            "mb" => value * 1024L * 1024,
            "gb" => value * 1024L * 1024 * 1024,
            "tb" => value * 1024L * 1024 * 1024 * 1024,
            _ => null,
        };
    }
}
