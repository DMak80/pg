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

/// <summary>Команда BASE_BACKUP и автомат разбора ответа (spec t01 §3.1):
/// DataRow'ы заголовка (tablespace list; более одного — fail-fast
/// «multiple tablespaces not supported») → COPY OUT tar-потоки (первый —
/// основной tablespace, второй — pg_wal при WAL STREAM) → итог
/// CommandComplete. Границы потоков — CopyOutResponse/CopyDone, не
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

    // Основной tablespace + pg_wal (WAL STREAM) — два потока максимум.
    private const int MaxStreams = 2;

    private readonly IBaseBackupSource _source = source;

    /// <summary>Выполнить BASE_BACKUP: tarSink(index, stream) вызывается на
    /// каждый COPY OUT поток (0 — основной tablespace, 1 — pg_wal); данные
    /// потока читаются из source на лету, sink обязан вычитать поток целиком.</summary>
    public async Task RunAsync(
        string backupId, Func<int, Stream, CancellationToken, Task> tarSink, CancellationToken ct)
    {
        // LABEL — маркер нашего полного в backup_label/манифесте сервера.
        await _source.SendAsync(
            $"BASE_BACKUP ( LABEL 'pgw-full-{backupId}', CHECKPOINT 'fast', WAIT false, " +
            "MANIFEST_CHECKSUMS 'SHA256', TARGET 'CLIENT', WAL 'STREAM' )", ct);

        var headerRows = 0;
        var streams = 0;
        while (true)
        {
            var message = await _source.ReadAsync(ct)
                          ?? throw new ApplicationException(
                              "BASE_BACKUP connection closed before CommandComplete");
            switch (message.Type)
            {
                case DataRow:
                    // Строки заголовка (tablespace list) — до первого COPY OUT;
                    // строки итога после потоков пропускаются.
                    if (streams == 0 && ++headerRows > 1)
                        throw MultipleTablespaces();
                    break;
                case CopyOutResponse:
                {
                    streams++;
                    if (streams > MaxStreams)
                        throw MultipleTablespaces();
                    var data = new CopyOutDataStream(this, ct);
                    await tarSink(streams - 1, data, ct);
                    // Sink мог не вычитать поток: команда владеет сообщениями —
                    // дочитываем, иначе следующий Read разберёт CopyData как
                    // ответ BASE_BACKUP.
                    await data.DrainAsync(ct);
                    break;
                }
                case CommandComplete:
                    return;
                case ReadyForQuery:
                    return; // если сервер уже прислал — выходим
                case ErrorResponse:
                    ThrowServerError(message.Payload);
                    break;
                case NoticeResponse:
                case ParameterStatus:
                case RowDescription:
                    break; // служебные — пропуск
                default:
                    throw new ApplicationException(
                        $"unexpected message '{(char)message.Type}' in BASE_BACKUP response");
            }
        }
    }

    private static ApplicationException MultipleTablespaces()
        => new("multiple tablespaces not supported");

    private static void ThrowServerError(byte[] payload)
    {
        var (sqlState, message) = PgWireReplicationClient.ParseErrorResponse(payload);
        throw new BaseBackupErrorException(sqlState, message);
    }

    /// <summary>Поток одного COPY OUT tar-потока BASE_BACKUP: подряд идущие
    /// CopyData-сообщения до CopyDone; CopyFail/ErrorResponse — исключение.
    /// Владение потоком сообщений — у команды (Dispose ничего не делает).</summary>
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

                var message = await command._source.ReadAsync(token)
                              ?? throw new ApplicationException(
                                  "BASE_BACKUP copy stream closed before CopyDone");
                switch (message.Type)
                {
                    case CopyData:
                        _current = message.Payload;
                        _offset = 0;
                        continue;
                    case CopyDone:
                        _done = true;
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

        /// <summary>Дочитать поток до CopyDone (sink мог не вычитать целиком).</summary>
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
