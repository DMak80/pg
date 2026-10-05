using Npgsql.Replication;
using NpgsqlTypes;
using PgWorker.Backups;
using Shared.Core;

namespace PgWorker.WalReceiver;

/// <summary>Replication-соединение Npgsql за интерфейсом: прод-обёртка — ниже;
/// интерфейс — для юнит-фейка со счётчиками открытий/закрытий (инвариант:
/// transient-цикл приёмника не течёт соединениями — иначе исчерпание
/// max_wal_senders источника ломает репликацию всего кластера).</summary>
internal interface IReplicationConnection : IAsyncDisposable
{
    Task Open(CancellationToken ct);

    IAsyncEnumerable<XLogDataMessage> StartReplication(
        PhysicalReplicationSlot slot, NpgsqlLogSequenceNumber startLsn, CancellationToken ct);

    void SetReplicationStatus(NpgsqlLogSequenceNumber lsn);

    Task SendStatusUpdate(CancellationToken ct);

    Task<TimelineHistoryFile> TimelineHistory(uint tli, CancellationToken ct);
}

/// <summary>Прод-обёртка PhysicalReplicationConnection (запечатан Npgsql —
/// композиция): walsender-режим репликации.</summary>
internal sealed class NpgsqlPhysicalReplicationConnection(string connectionString)
    : IReplicationConnection
{
    private readonly PhysicalReplicationConnection _inner = new(connectionString);

    public Task Open(CancellationToken ct) => _inner.Open(ct);

    public IAsyncEnumerable<XLogDataMessage> StartReplication(
        PhysicalReplicationSlot slot, NpgsqlLogSequenceNumber startLsn, CancellationToken ct)
        => _inner.StartReplication(slot, startLsn, ct);

    public void SetReplicationStatus(NpgsqlLogSequenceNumber lsn)
        => _inner.SetReplicationStatus(lsn);

    public Task SendStatusUpdate(CancellationToken ct) => _inner.SendStatusUpdate(ct);

    public Task<TimelineHistoryFile> TimelineHistory(uint tli, CancellationToken ct)
        => _inner.TimelineHistory(tli, ct);

    public ValueTask DisposeAsync() => _inner.DisposeAsync();
}

/// <summary>Транспорт физической репликации Npgsql 10 (t27, arch/19 §3.1):
/// SQL-коннект — сверка wal_segment_size (permanent при расхождении с
/// WalFileName.SegmentBytes — математика имён не должна молча врать);
/// PhysicalReplicationConnection — START_REPLICATION SLOT ... PHYSICAL <lsn>
/// с явной стартовой позицией; Stream — XLogData → XLogChunk; SetConfirmed —
/// standby status update: apply/flush = подтверждённая LSN (Npgsql отвечает
/// серверным keepalive последним выставленным статусом, потому подтверждение
/// ставится ТОЛЬКО после успешного S3-put — инвариант RPO); TimelineHistory —
/// публичный API Npgsql (вердикт t27: экспонируется — fallback не нужен).
/// Жизненный цикл соединения: повторный OpenAsync гасит прежнее, сбой
/// START_REPLICATION гасит сломанное — каждое соединение живёт ровно до
/// своей замены (утечка walsender'ов исчерпывает max_wal_senders источника).</summary>
public sealed class NpgsqlXLogSource : IXLogReplicationSource
{
    private const string ConnStringTemplate =
        "Host={0};Port={1};Username={2};Password={3};Database={4};SSL Mode=Require;Trust Server Certificate=true;Timeout=10";

    private readonly WalReceiverOptions _o;
    private readonly Func<string, IReplicationConnection>? _connectionFactory;
    private readonly Func<CancellationToken, Task<string>>? _rawSegmentSizeProbe;

    /// <summary>Прод: реальные SQL-проба сегмента и replication-соединения.</summary>
    public NpgsqlXLogSource(WalReceiverOptions o)
        : this(o, null, null)
    {
    }

    /// <summary>Тестовый: фейковые фабрики (счётчики соединений юнита).</summary>
    internal NpgsqlXLogSource(
        WalReceiverOptions o,
        Func<string, IReplicationConnection>? connectionFactory,
        Func<CancellationToken, Task<string>>? rawSegmentSizeProbe)
    {
        _o = o;
        _connectionFactory = connectionFactory;
        _rawSegmentSizeProbe = rawSegmentSizeProbe;
    }

    private IReplicationConnection? _replication;
    private IAsyncEnumerable<XLogDataMessage>? _messages;

    private string ConnString => string.Format(
        ConnStringTemplate, _o.PgHost, _o.PgPort, _o.PgUser, _o.PgPassword, _o.PgDbname);

    private IReplicationConnection NewConnection()
        => _connectionFactory is { } factory
            ? factory(ConnString)
            : new NpgsqlPhysicalReplicationConnection(ConnString);

    public async Task<Result> OpenAsync(CancellationToken ct)
    {
        try
        {
            // Сверка wal_segment_size обычным SQL-коннектом (replication-режим
            // не обязан для SHOW; проверка ДО старта репликации; проба
            // подменяема — юниты транспорта без живого PG).
            var raw = _rawSegmentSizeProbe is { } probe
                ? await probe(ct)
                : await ShowSegmentSizeAsync(ct);
            if (ParseSegmentSize(raw) is not { } segmentBytes || segmentBytes != WalFileName.SegmentBytes)
                return Result.Failed(new ApplicationException(
                    $"wal_segment_size источника {raw} ≠ {WalFileName.SegmentBytes} байт"));

            // Прежнее replication-соединение обязано умереть ДО создания
            // нового: transient-цикл приёмника (обрыв источника/рестарт воркера)
            // иначе копит живые walsender'ы — утечка соединений.
            await KillReplicationAsync();
            var replication = NewConnection();
            await replication.Open(ct);
            _replication = replication;
            return Result.Success();
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException($"open {_o.PgHost}:{_o.PgPort}: {e.Message}", e));
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
            // Стрим не начался — соединение непригодно к переиспользованию:
            // гасим (следующий цикл OpenAsync создаст свежее), не оставляя
            // источнику полуживой walsender.
            await KillReplicationAsync();
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
            await using var history = NewConnection();
            await history.Open(ct);
            var file = await history.TimelineHistory(tli, ct);
            return Result<byte[]?>.Success(file.Content);
        }
        catch (Exception e)
        {
            // Рантайм не смог отдать историю (нет файла/ошибка протокола) — валидное
            // «нет»: fallback-контроль воркера (arch/19 §3). Причина — в stderr
            // контейнера (диагностика без перезапуска).
            Console.Error.WriteLine($"history_error tli={tli}: {e.GetType().Name}: {e.Message}");
            return Result<byte[]?>.Success(null);
        }
    }

    public async ValueTask DisposeAsync()
        => await KillReplicationAsync();

    /// <summary>Гашение текущего replication-соединения (идемпотентно): ошибки
    /// добивания (уже мёртв при обрыве источника) не мешают пересозданию.</summary>
    private async Task KillReplicationAsync()
    {
        if (_replication is not { } replication)
            return;
        _replication = null;
        try
        {
            await replication.DisposeAsync();
        }
        catch
        {
            // соединение уже мертво — целимся в пересоздание
        }
    }

    /// <summary>Прод-проба wal_segment_size обычным SQL-коннектом к источнику.</summary>
    private async Task<string> ShowSegmentSizeAsync(CancellationToken ct)
    {
        await using var connection = new Npgsql.NpgsqlConnection(ConnString);
        await connection.OpenAsync(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = "SHOW wal_segment_size";
        return (string)(await command.ExecuteScalarAsync(ct))!;
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
