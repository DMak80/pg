using System.Reflection;
using System.Text;
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

    /// <summary>IDENTIFY_SYSTEM: (Timeline, XLogPos) сервера.</summary>
    Task<(uint Tli, ulong XLogPos)> IdentifySystem(CancellationToken ct);

    IAsyncEnumerable<XLogDataMessage> StartReplication(
        PhysicalReplicationSlot slot, NpgsqlLogSequenceNumber startLsn,
        CancellationToken ct, uint? timeline);

    void SetReplicationStatus(NpgsqlLogSequenceNumber lsn);

    Task SendStatusUpdate(CancellationToken ct);

    /// <summary>TIMELINE_HISTORY: байты файла истории; null — источника нет.</summary>
    Task<byte[]?> TimelineHistory(uint tli, CancellationToken ct);
}

/// <summary>Прод-обёртка PhysicalReplicationConnection (запечатан Npgsql —
/// композиция): walsender-режим репликации.</summary>
internal sealed class NpgsqlPhysicalReplicationConnection(string connectionString)
    : IReplicationConnection
{
    private readonly PhysicalReplicationConnection _inner = new(connectionString);

    public Task Open(CancellationToken ct) => _inner.Open(ct);

    public async Task<(uint Tli, ulong XLogPos)> IdentifySystem(CancellationToken ct)
    {
        var identity = await _inner.IdentifySystem(ct);
        return (identity.Timeline, (ulong)identity.XLogPos);
    }

    public IAsyncEnumerable<XLogDataMessage> StartReplication(
        PhysicalReplicationSlot slot, NpgsqlLogSequenceNumber startLsn,
        CancellationToken ct, uint? timeline)
        => timeline is { } tli
            ? _inner.StartReplication(slot, startLsn, ct, tli)
            : _inner.StartReplication(slot, startLsn, ct);

    public void SetReplicationStatus(NpgsqlLogSequenceNumber lsn)
        => _inner.SetReplicationStatus(lsn);

    public Task SendStatusUpdate(CancellationToken ct) => _inner.SendStatusUpdate(ct);

    // TIMELINE_HISTORY: сырые байты файла истории. Публичный TimelineHistory
    // Npgsql 10.0.3 НЕ используется: сервер (PG ≥ 14) объявляет колонку
    // content как text, неся сырые байты файла, — публичный парсер кастует её
    // к byte[] и падает InvalidCastException'ом на КАЖДОМ запросе (апстрим-фикс
    // npgsql#6709 не выпущен). Читаем приватный ReadSingleRow Npgsql
    // (simple-протокол: replication-команды сервер маршрутизирует только в нём,
    // extended-протокол их не исполняет); строку декодируем в байты — .history
    // пишется постгресом в ASCII, декодировка обратима. Смена приватного API
    // апстримом → исключение → catch вызывающего (history_error + fallback ФС).
    public async Task<byte[]?> TimelineHistory(uint tli, CancellationToken ct)
    {
        // ReadSingleRow объявлен в БАЗОВОМ ReplicationConnection: приватные
        // методы базового класса через GetMethod на наследнике не видны.
        var replicationType = typeof(PhysicalReplicationConnection).BaseType;
        var readSingleRow = replicationType?.GetMethod(
            "ReadSingleRow", BindingFlags.NonPublic | BindingFlags.Instance,
            [typeof(string), typeof(CancellationToken)]);
        if (readSingleRow is null)
            throw new ApplicationException(
                "Npgsql ReadSingleRow(string, CancellationToken) not found — upstream API changed");
        object[] row;
        try
        {
            row = await (Task<object[]>)readSingleRow.Invoke(_inner,
                [$"TIMELINE_HISTORY {tli:D}", ct])!;
        }
        catch (TargetInvocationException e)
        {
            throw e.InnerException ?? e; // настоящий отказ протокола, не обёртка
        }

        return row[1] switch
        {
            byte[] bytes => bytes, // bytea-декларация (PG < 14)
            string text => Encoding.UTF8.GetBytes(text), // text-декларация (PG ≥ 14), ASCII
            { } other => throw new ApplicationException(
                $"TIMELINE_HISTORY {tli}: unexpected content column {other.GetType().Name}"),
            _ => throw new ApplicationException($"TIMELINE_HISTORY {tli}: empty content"),
        };
    }

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
/// сырой ReadSingleRow Npgsql (публичный TimelineHistory не годится: сервер
/// с PG ≥ 14 объявляет content как text при сырых байтах — публичный каст к
/// byte[] падает на каждом запросе; fallback к ФС ноды остаётся страховкой).
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
    private IAsyncEnumerator<XLogDataMessage>? _stream;
    private XLogDataMessage? _pendingFirst;

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
                    $"source wal_segment_size {raw} ≠ {WalFileName.SegmentBytes} bytes"));

            // Прежнее replication-соединение обязано умереть ДО создания
            // нового: transient-цикл приёмника (обрыв источника/рестарт воркера)
            // иначе копит живые walsender'ы — утечка соединений.
            await KillReplicationAsync();
            var replication = NewConnection();
            try
            {
                await replication.Open(ct);
            }
            catch
            {
                // Открытие не удалось — свежесозданное соединение гасим
                // (симметрия с остальными ветками: ни одно соединение
                // не переживает свой сбой).
                try
                {
                    await replication.DisposeAsync();
                }
                catch
                {
                    // уже мертво
                }
                throw;
            }
            _replication = replication;
            return Result.Success();
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException($"open {_o.PgHost}:{_o.PgPort}: {e.Message}", e));
        }
    }

    public async Task<Result> StartReplicationAsync(string slot, ulong startLsn, uint? timeline, CancellationToken ct)
    {
        try
        {
            var replication = _replication ?? throw new InvalidOperationException("OpenAsync not called");
            var physicalSlot = new PhysicalReplicationSlot(slot);
            // Npgsql выполняет протокольный exchange START_REPLICATION лениво:
            // ошибка старта (58P01) выходит при ПЕРВОМ MoveNextAsync, а не из
            // вызова StartReplication. Форсируем первый шаг перечисления ЗДЕСЬ —
            // отказ 58P01 тогда выходит из самого старта (типизированный
            // WalStartRefused наружу — tli-процедура ядра), а не обрывом потока
            // в цикле чтения (вечные stream_broken-циклы). Полученный первый чанк
            // буферизуется — Stream отдаёт его первым (порядок данных не меняется).
            var messages = replication.StartReplication(
                physicalSlot, (NpgsqlLogSequenceNumber)startLsn, ct, timeline);
            var enumerator = messages.GetAsyncEnumerator(ct);
            _stream = enumerator;
            _pendingFirst = await enumerator.MoveNextAsync() ? enumerator.Current : null;
            return Result.Success();
        }
        catch (Npgsql.PostgresException e) when (e.SqlState == "42704")
        {
            // Слота нет (SQLSTATE 42704, «replication slot … does not exist») —
            // типизированный маркер: ядро уходит в WaitingSlot (воркер ensure'ит).
            await KillReplicationAsync();
            return Result.Failed(new WalSlotMissing(slot));
        }
        catch (Npgsql.PostgresException e) when (e.SqlState == "58P01")
        {
            // Отказ старта по 58P01: типизированный маркер наружу (матч по SqlState,
            // не по тексту) — tli-процедура ядра; соединение гасим как при любом
            // сбое старта (не переиспользуем полуживой walsender; процедуре нужен
            // свежий OpenAsync перед её стартом).
            await KillReplicationAsync();
            return Result.Failed(new WalStartRefused(e.MessageText));
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

    public async Task<Result<ServerIdentity>> IdentifySystemAsync(CancellationToken ct)
    {
        // IDENTIFY_SYSTEM — replication-команда: короткоживущее соединение по
        // образцу ReadTimelineHistoryAsync (стрим-коннект после failed-старта
        // уже погашен); каждая попытка живёт ровно до своего Dispose.
        try
        {
            await using var connection = NewConnection();
            await connection.Open(ct);
            var (tli, xlogPos) = await connection.IdentifySystem(ct);
            return Result<ServerIdentity>.Success(new ServerIdentity(tli, xlogPos));
        }
        catch (Exception e)
        {
            return Result<ServerIdentity>.Failed(new ApplicationException(
                $"IDENTIFY_SYSTEM {_o.PgHost}:{_o.PgPort}: {e.Message}", e));
        }
    }

    public IAsyncEnumerable<XLogChunk> Stream => ReadChunksAsync();

    // Вычитка тела XLogData одним массивом ТОЧНОГО размера: MemoryStream+
    // ToArray давали двойную аллокацию и лишнюю копию каждого куска hot-path.
    private static async Task<byte[]> ReadAllBytesAsync(System.IO.Stream data)
    {
        var buffer = new byte[data.Length];
        await data.ReadExactlyAsync(buffer);
        return buffer;
    }

    private async IAsyncEnumerable<XLogChunk> ReadChunksAsync()
    {
        var enumerator = _stream ?? throw new InvalidOperationException("StartReplicationAsync not called");
        if (_pendingFirst is { } first)
        {
            _pendingFirst = null;
            // Data — поток, валиден только до следующего сообщения: вычитываем целиком
            // (буфер сегмента в памяти — кэш без durability, MULTI-HOST).
            yield return new XLogChunk(
                (ulong)first.WalStart, (ulong)first.WalEnd, await ReadAllBytesAsync(first.Data));
        }

        while (await enumerator.MoveNextAsync())
        {
            yield return new XLogChunk(
                (ulong)enumerator.Current.WalStart, (ulong)enumerator.Current.WalEnd,
                await ReadAllBytesAsync(enumerator.Current.Data));
        }
    }

    public async Task<Result> SetConfirmedAsync(ulong confirmedLsn, CancellationToken ct)
    {
        try
        {
            var replication = _replication ?? throw new InvalidOperationException("OpenAsync not called");
            replication.SetReplicationStatus((NpgsqlLogSequenceNumber)confirmedLsn);
            await replication.SendStatusUpdate(ct);
            return Result.Success();
        }
        catch (Exception e)
        {
            return Result.Failed(new ApplicationException(
                $"status update {WalLsn.Text(confirmedLsn)}: {e.Message}", e));
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
            var content = await history.TimelineHistory(tli, ct);
            return Result<byte[]?>.Success(content);
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
        // Буфер первого чанка и enumerator прежнего стрима умирают вместе с
        // соединением — новый START_REPLICATION создаёт их заново.
        _stream = null;
        _pendingFirst = null;
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
