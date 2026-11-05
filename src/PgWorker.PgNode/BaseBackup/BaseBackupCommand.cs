using System.Buffers.Binary;
using System.Text;

namespace PgWorker.PgNode.BaseBackup;

/// <summary>Отказ сервера PG на BASE_BACKUP (ErrorResponse): SQLState + текст
/// как есть — несовместимость версии/опций диагностируется без маскировки
/// (spec t01 §6.20), набор не создаётся.</summary>
public sealed class BaseBackupErrorException(string sqlState, string message)
    : Exception($"[{sqlState}] {message}")
{
    public string SqlState { get; } = sqlState;
    public string ServerMessage { get; } = message;
}

/// <summary>Команда BASE_BACKUP и автомат разбора ответа (spec t01 §3.1,
/// протокол PG18): стартовый result set (позиция WAL) → tablespace list
/// (более одной строки — fail-fast «multiple tablespaces not supported») →
/// ОДИН COPY OUT-поток с внутренним фреймингом архивов (`n` — начало архива
/// с именем base.tar/pg_wal.tar, `m` — манифест, `d` — данные, `p` —
/// прогресс) → финальный result set. Границы — сообщения протокола, а не
/// разделители result-set'ов.</summary>
public sealed class BaseBackupCommand(IBaseBackupSource source)
{
    private const byte DataRow = (byte)'D';
    private const byte CopyOutResponse = (byte)'H';
    private const byte CopyDone = (byte)'c';
    private const byte CopyData = (byte)'d';
    private const byte CopyFail = (byte)'f';
    private const byte CommandComplete = (byte)'C';
    private const byte ReadyForQuery = (byte)'Z';
    private const byte ErrorResponse = (byte)'E';
    private const byte NoticeResponse = (byte)'N';
    private const byte ParameterStatus = (byte)'S';
    private const byte RowDescription = (byte)'T';

    // Теги внутреннего фрейминга CopyData-потока (протокол BASE_BACKUP).
    private const byte ArchiveStart = (byte)'n';
    private const byte ManifestStart = (byte)'m';
    private const byte ArchiveData = (byte)'d';
    private const byte ProgressReport = (byte)'p';

    private const string MainArchiveName = "base.tar";
    private const string WalArchiveName = "pg_wal.tar";

    // Полей в стартовом/финальном result set (recptr, tli); tablespace list — 3.
    private const int PositionFields = 2;

    /// <summary>Индекс sink'а манифеста: сырые байты JSON (backup_manifest
    /// приходит отдельной секцией COPY-потока, не tar-архивом).</summary>
    public const int ManifestIndex = 2;

    private readonly IBaseBackupSource _source = source;

    // Сообщение, прочитанное архивным потоком «вперёд» (граница секции) и
    // подлежащее разбору внешним автоматом.
    private PgMessage? _pending;

    /// <summary>Выполнить BASE_BACKUP: tarSink(index, stream) вызывается на
    /// каждый архив COPY-потока (0 — основной tablespace base.tar, 1 — pg_wal,
    /// <see cref="ManifestIndex"/> — манифест); поток обязан быть вычитан
    /// целиком (команда дочитывает остаток за sink'ом).</summary>
    public async Task RunAsync(
        string backupId, Func<int, Stream, CancellationToken, Task> tarSink, CancellationToken ct)
    {
        // LABEL — маркер нашего полного в backup_label сервера; MANIFEST 'yes'
        // обязателен для MANIFEST_CHECKSUMS; TARGET — строчное 'client'
        // (регистр значим у сервера), WAL — булев (PG15+). LABEL экранируется
        // (одинарная кавычка удваивается): граница операций уже валидирует id
        // allowlist'ом (arch/pgnode/02 §2), экранирование — независимая защита
        // контракта рендера (LABEL уходит в backup_label и парсится restore).
        await _source.SendAsync(
            $"BASE_BACKUP ( LABEL '{EscapeLiteral($"pgw-full-{backupId}")}', CHECKPOINT 'fast', " +
            "MANIFEST 'yes', MANIFEST_CHECKSUMS 'SHA256', TARGET 'client', WAL true, WAIT false )", ct);

        var phase = ResultPhase.Position;
        var headerRows = 0;
        var copyOuts = 0;
        while (true)
        {
            var message = await ReadNextAsync(ct)
                          ?? throw new ApplicationException(
                              "BASE_BACKUP connection closed before CommandComplete");
            switch (message.Type)
            {
                case RowDescription:
                    // Два поля — result set позиции WAL, три — tablespace list.
                    phase = FieldCount(message.Payload) == PositionFields
                        ? ResultPhase.Position
                        : ResultPhase.TablespaceList;
                    break;
                case DataRow:
                    // Строки tablespace list; более одной — дополнительный
                    // tablespace (fail-fast). Прочие result set'ы — пропуск.
                    if (phase == ResultPhase.TablespaceList && ++headerRows > 1)
                        throw MultipleTablespaces();
                    break;
                case CopyOutResponse:
                    // PG шлёт ОДИН COPY OUT-поток на все архивы; второй —
                    // не наш формат (tablespace-архивы = лишние tablespace).
                    if (++copyOuts > 1)
                        throw MultipleTablespaces();
                    await ReadCopyOutAsync(tarSink, ct);
                    break;
                case CommandComplete:
                    // Result set'ы позиции/таблиц завершаются ДО COPY OUT;
                    // конец команды — CommandComplete после потока архивов.
                    if (copyOuts > 0)
                        return;
                    break;
                case ReadyForQuery:
                    return;
                case ErrorResponse:
                    ThrowServerError(message.Payload);
                    break;
                case NoticeResponse:
                case ParameterStatus:
                    break; // служебные — пропуск
                default:
                    throw new ApplicationException(
                        $"unexpected message '{(char)message.Type}' in BASE_BACKUP response");
            }
        }
    }

    // Разбор COPY OUT-потока: последовательность секций с внутренним
    // фреймингом CopyData ('n'/'m'/'d'/'p'); конец — CopyDone.
    private async Task ReadCopyOutAsync(
        Func<int, Stream, CancellationToken, Task> tarSink, CancellationToken ct)
    {
        while (true)
        {
            var message = await ReadNextAsync(ct)
                          ?? throw new ApplicationException(
                              "BASE_BACKUP copy stream closed before CopyDone");
            switch (message.Type)
            {
                case CopyDone:
                    return;
                case CopyData:
                {
                    if (message.Payload.Length == 0)
                        throw new ApplicationException("empty CopyData in BASE_BACKUP copy stream");
                    switch (message.Payload[0])
                    {
                        case ArchiveStart:
                        {
                            var name = ParseArchiveName(message.Payload);
                            var index = name == MainArchiveName ? 0
                                : name == WalArchiveName ? 1
                                : throw MultipleTablespaces();
                            await ConsumeSectionAsync(index, tarSink, ct);
                            break;
                        }
                        case ManifestStart:
                            await ConsumeSectionAsync(ManifestIndex, tarSink, ct);
                            break;
                        case ArchiveData:
                            throw new ApplicationException(
                                "BASE_BACKUP archive data before archive start");
                        case ProgressReport:
                            break; // прогресс сервера — не нужен (свой счётчик)
                        default:
                            throw new ApplicationException(
                                $"unexpected CopyData tag '{(char)message.Payload[0]}' in BASE_BACKUP copy stream");
                    }

                    break;
                }
                case CopyFail:
                    throw new ApplicationException(
                        $"BASE_BACKUP COPY failed: {Encoding.UTF8.GetString(message.Payload).TrimEnd('\0')}");
                case ErrorResponse:
                    ThrowServerError(message.Payload);
                    break;
                case NoticeResponse:
                case ParameterStatus:
                    break; // служебные — пропуск (симметрия внешнему автомату)
                default:
                    throw new ApplicationException(
                        $"unexpected message '{(char)message.Type}' in BASE_BACKUP copy stream");
            }
        }
    }

    // Секция архива/манифеста: sink вычитывает данные 'd' до следующего
    // 'n'/'m'/CopyDone (границу команда забирает себе через pushback).
    private async Task ConsumeSectionAsync(
        int index, Func<int, Stream, CancellationToken, Task> tarSink, CancellationToken ct)
    {
        var data = new CopyOutDataStream(this, ct);
        await tarSink(index, data, ct);
        await data.DrainAsync(ct);
    }

    // 'n' + имя архива (null-terminated) + каталог (null-terminated).
    private static string ParseArchiveName(byte[] payload)
    {
        const int pos = 1;
        var end = Array.IndexOf(payload, (byte)0, pos);
        if (end < 0)
            throw new ApplicationException("malformed BASE_BACKUP archive start message");
        return Encoding.ASCII.GetString(payload, pos, end - pos);
    }

    // Число полей RowDescription (int16, network order).
    private static int FieldCount(byte[] payload)
        => payload.Length >= 2 ? BinaryPrimitives.ReadInt16BigEndian(payload) : 0;

    private async Task<PgMessage?> ReadNextAsync(CancellationToken ct)
    {
        var pending = _pending;
        _pending = null;
        return pending ?? await _source.ReadAsync(ct);
    }

    private static ApplicationException MultipleTablespaces()
        => new("multiple tablespaces not supported");

    // Экранирование строкового литерала simple-query: `'` → `''`.
    internal static string EscapeLiteral(string value)
        => value.Replace("'", "''", StringComparison.Ordinal);

    private static void ThrowServerError(byte[] payload)
    {
        var (sqlState, message) = PgWireReplicationClient.ParseErrorResponse(payload);
        throw new BaseBackupErrorException(sqlState, message);
    }

    private enum ResultPhase
    {
        Position,
        TablespaceList,
    }

    /// <summary>Поток данных одной секции COPY OUT-потока (архив или
    /// манифест): CopyData-фреймы 'd' до границы секции ('n'/'m'/CopyDone,
    /// сообщение возвращается автомату); 'p' (прогресс) пропускается.
    /// Dispose ничего не делает — владение потоком сообщений у команды.</summary>
    private sealed class CopyOutDataStream(BaseBackupCommand command, CancellationToken ct) : Stream
    {
        private byte[] _current = [];
        private int _offset;
        private bool _done;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
            => ReadAsync(buffer.AsMemory(offset, count), ct).AsTask().GetAwaiter().GetResult();

        public override async ValueTask<int> ReadAsync(
            Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var token = cancellationToken.CanBeCanceled ? cancellationToken : ct;
            while (true)
            {
                if (_offset < _current.Length)
                {
                    var count = Math.Min(buffer.Length, _current.Length - _offset);
                    _current.AsMemory(_offset, count).CopyTo(buffer);
                    _offset += count;
                    return count;
                }

                if (_done)
                    return 0;

                var message = await command.ReadNextAsync(token)
                              ?? throw new ApplicationException(
                                  "BASE_BACKUP copy stream closed before CopyDone");
                switch (message.Type)
                {
                    case CopyData:
                        if (message.Payload.Length == 0)
                            throw new ApplicationException(
                                "empty CopyData in BASE_BACKUP copy stream");
                        switch (message.Payload[0])
                        {
                            case ArchiveData:
                                _current = message.Payload;
                                _offset = 1;
                                continue;
                            case ProgressReport:
                                continue; // прогресс сервера — пропуск
                            case ArchiveStart:
                            case ManifestStart:
                                // Граница секции: сообщение — внешнему автомату.
                                _done = true;
                                command._pending = message;
                                return 0;
                            default:
                                throw new ApplicationException(
                                    $"unexpected CopyData tag '{(char)message.Payload[0]}' in BASE_BACKUP copy stream");
                        }
                    case CopyDone:
                        _done = true;
                        command._pending = message;
                        return 0;
                    case CopyFail:
                        throw new ApplicationException(
                            $"BASE_BACKUP COPY failed: {Encoding.UTF8.GetString(message.Payload).TrimEnd('\0')}");
                    case ErrorResponse:
                        ThrowServerError(message.Payload);
                        return 0; // недостижимо: ThrowServerError всегда бросает
                    default:
                        throw new ApplicationException(
                            $"unexpected message '{(char)message.Type}' in BASE_BACKUP copy stream");
                }
            }
        }

        /// <summary>Дочитать секцию (sink мог не вычитать целиком).</summary>
        public async Task DrainAsync(CancellationToken cancellationToken)
        {
            var buffer = new byte[64 * 1024];
            while (await ReadAsync(buffer, cancellationToken) > 0)
            {
                // Данные не нужны — поток принадлежит команде.
            }
        }

        public override void Flush()
        {
            // Только чтение — сброс нечего.
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
