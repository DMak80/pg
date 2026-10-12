using System.Buffers.Binary;
using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Logging;

namespace OwnS3.Replication.Outbox;

// Сегментный append-only журнал доставки (t35, глава 07 §4):
// .owns3.sys/replication/outbox/seg-<startSeqNo:020>.log; сегмент закрывается
// (открывается новый) по 64 МиБ или 5 мин возраста; seqNo сквозной монотонный
// (при открытии — скан хвоста последнего сегмента; пусто → seqNo=1); Append+
// Flush+fsync до ответа клиенту (события маленькие, тела не хранятся).
// Тримминг/бюджет зовёт цикл обслуживания (§8-а), НЕ путь записи.
public sealed class OutboxJournal : IDisposable
{
    // Константы ротации — канон главы 07 §4.
    public const long MaxSegmentBytes = 64L * 1024 * 1024;      // 64 МиБ
    public static readonly TimeSpan MaxSegmentAge = TimeSpan.FromMinutes(5);

    private readonly string _outboxDir;
    private readonly long _budgetBytes;
    private readonly long _maxSegmentBytes;      // тесты переопределяют (internal-ctor)
    private readonly TimeSpan _maxSegmentAge;
    private readonly ILogger<OutboxJournal> _logger;
    private readonly object _lock = new();

    private FileStream? _writer;             // активный сегмент (append)
    private long _writerStartSeqNo;          // startSeqNo активного сегмента
    private long _writerOpenedTimestamp;     // возраст сегмента — от открытия
    private long _lastSeqNo;                 // сквозной seqNo последнего события
    private long _closedBytes;               // сумма размеров закрытых сегментов
    private bool _enumerated;                // каталог просканирован (ленивое создание)

    public OutboxJournal(string outboxDir, long budgetBytes, ILogger<OutboxJournal> logger)
        : this(outboxDir, budgetBytes, logger, MaxSegmentBytes, MaxSegmentAge)
    {
    }

    // internal-ctor: тесты задают малые лимиты ротации (пособытийная ротация).
    internal OutboxJournal(string outboxDir, long budgetBytes, ILogger<OutboxJournal> logger,
        long maxSegmentBytes, TimeSpan maxSegmentAge)
    {
        _outboxDir = outboxDir;
        _budgetBytes = budgetBytes;
        _maxSegmentBytes = maxSegmentBytes;
        _maxSegmentAge = maxSegmentAge;
        _logger = logger;
    }

    // Append+Flush+fsync; создаёт сегмент/каталог при первом Append (лениво —
    // неактивная репликация не оставляет следов на томе, глава 07 §4).
    public ReplicationEvent Append(ReplicationEventType type, string bucket, string key,
        long modTimeUnixMs, string etag, long size, string contentSha256, string originHostId)
    {
        lock (_lock)
        {
            EnsureOpened();
            if (_writer is not null
                && (_writer.Length >= _maxSegmentBytes
                    || Stopwatch.GetElapsedTime(_writerOpenedTimestamp) >= _maxSegmentAge))
                RotateLocked();
            var seqNo = _lastSeqNo + 1;
            var e = new ReplicationEvent(seqNo, type, bucket, key, modTimeUnixMs, etag, size, contentSha256,
                originHostId);
            var payload = OutboxEntryCodec.Encode(e);
            _writer!.Write(payload);
            _writer.Flush(flushToDisk: true); // fsync: событие ДО ответа клиенту (R2)
            _lastSeqNo = seqNo;
            return e;
        }
    }

    // Чтение батчами через границы сегментов (источник notifier'а, глава 07 §8).
    public IReadOnlyList<ReplicationEvent> ReadBatch(long fromSeqNoExclusive, int maxCount)
    {
        lock (_lock)
        {
            var result = new List<ReplicationEvent>();
            if (maxCount <= 0)
                return result;
            EnsureOpened(); // ленивый скан существующих сегментов (рестарт источника, глава 07 §4)
            foreach (var startSeqNo in ScanSegmentsLocked())
            {
                if (result.Count >= maxCount)
                    break;
                var path = SegmentPath(startSeqNo);
                var bytes = ReadSegment(path);
                var offset = 0;
                while (offset < bytes.Length && result.Count < maxCount)
                {
                    if (!OutboxEntryCodec.TryDecode(bytes, offset, out var e, out offset))
                    {
                        _logger.LogWarning("Битая запись журнала {Path} на смещении {Offset} — чтение прервано",
                            path, offset);
                        break;
                    }
                    if (e.SeqNo > fromSeqNoExclusive)
                        result.Add(e);
                }
            }
            return result;
        }
    }

    public long LastSeqNo
    {
        get { lock (_lock) { EnsureOpened(); return _lastSeqNo; } }
    }

    // Сумма размеров сегментов (метрика ownS3_replication_outbox_bytes, глава 07 §10).
    public long TotalBytes
    {
        get { lock (_lock) { EnsureOpened(); return TotalBytesLocked(); } }
    }

    // Бюджет превышен (метрика ownS3_replication_outbox_budget_exceeded, глава 07 §10).
    public bool BudgetExceeded
    {
        get { lock (_lock) { EnsureOpened(); return TotalBytesLocked() > _budgetBytes; } }
    }

    // Штатный тримминг: удалить сегменты, чей конец (максимальный seqNo) <
    // minAcceptedSeqNo (min watermark приёма активных соседей, глава 07 §4);
    // активный сегмент не трогается. true — сегменты удалены.
    public bool Trim(long minAcceptedSeqNo)
    {
        lock (_lock)
        {
            EnsureOpened(); // скан существующих сегментов ДО вычисления конца (иначе end=0 → ложное удаление всего)
            var removed = false;
            var starts = ScanSegmentsLocked();
            foreach (var startSeqNo in starts)
            {
                if (IsActiveLocked(startSeqNo))
                    continue; // активный не трогаем
                var endSeqNo = EndOf(starts, startSeqNo);
                if (endSeqNo >= minAcceptedSeqNo)
                    continue;
                DeleteSegment(SegmentPath(startSeqNo));
                removed = true;
            }
            return removed;
        }
    }

    // Принудительный тримминг (бюджет превышен, глава 07 §4): удалять старейшие
    // ЗАКРЫТЫЕ сегменты, пока TotalBytes <= budget; активный не трогаем — запись
    // клиента не отказывает. true — принудительный тримм выполнен (вызывающий
    // пишет лог-событие потерь уведомлений).
    public bool ForceTrimToBudget()
    {
        lock (_lock)
        {
            EnsureOpened();
            var removed = false;
            while (TotalBytesLocked() > _budgetBytes)
            {
                var closed = ScanSegmentsLocked().Where(s => !IsActiveLocked(s)).ToArray();
                if (closed.Length == 0)
                    break; // только активный — резать нечего
                DeleteSegment(SegmentPath(closed[0]));
                removed = true;
            }
            return removed;
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            _writer?.Dispose();
            _writer = null;
        }
    }

    // ── внутренняя механика ──

    private long TotalBytesLocked() =>
        _closedBytes + (_writer is not null ? _writer.Length : 0);

    private bool IsActiveLocked(long startSeqNo) =>
        _writer is not null && startSeqNo == _writerStartSeqNo;

    private string SegmentPath(long startSeqNo) => Path.Combine(_outboxDir, $"seg-{startSeqNo:D20}.log");

    private void EnsureOpened()
    {
        if (_enumerated)
        {
            if (_writer is null)
                OpenSegment(_lastSeqNo + 1);
            return;
        }
        _enumerated = true;
        var starts = ScanSegmentsLocked();
        if (starts.Length == 0)
        {
            OpenSegment(1); // пусто → seqNo=1 (глава 07 §4); каталог создаётся здесь
            return;
        }
        // Закрытые — все, кроме последнего (последний открывается активным)
        _closedBytes = starts.Take(starts.Length - 1).Sum(s => new FileInfo(SegmentPath(s)).Length);
        // seqNo сквозной: скан хвоста последнего сегмента (глава 07 §4)
        var lastStart = starts[^1];
        var lastSeq = ScanTailSeqNo(SegmentPath(lastStart));
        _lastSeqNo = lastSeq ?? lastStart - 1; // пустой хвост → конец предыдущего сегмента
        OpenSegment(lastStart);
    }

    private void OpenSegment(long startSeqNo)
    {
        Directory.CreateDirectory(_outboxDir);
        _writer = new FileStream(SegmentPath(startSeqNo), FileMode.Append, FileAccess.Write, FileShare.Read);
        _writerStartSeqNo = startSeqNo;
        _writerOpenedTimestamp = Stopwatch.GetTimestamp();
    }

    private void RotateLocked()
    {
        _closedBytes += _writer!.Length;
        _writer.Dispose();
        _writer = null;
        OpenSegment(_lastSeqNo + 1);
    }

    private void DeleteSegment(string path)
    {
        var size = File.Exists(path) ? new FileInfo(path).Length : 0;
        File.Delete(path);
        _closedBytes -= size;
        if (_closedBytes < 0)
            _closedBytes = 0;
        _logger.LogInformation("Сегмент журнала удалён: {Path} ({Size} байт)", path, size);
    }

    // startSeqNo всех существующих сегментов по возрастанию.
    private long[] ScanSegmentsLocked()
    {
        if (!Directory.Exists(_outboxDir))
            return [];
        return Directory.GetFiles(_outboxDir, "seg-*.log")
            .Select(file => Path.GetFileName(file))
            .Select(name => long.TryParse(name.AsSpan(4, name.Length - 8), out var v) ? v : 0)
            .Where(v => v > 0)
            .OrderBy(v => v)
            .ToArray();
    }

    // Конец сегмента: start следующего − 1; активный (последний) — LastSeqNo.
    private long EndOf(long[] starts, long startSeqNo)
    {
        var index = Array.IndexOf(starts, startSeqNo);
        return index < 0 ? startSeqNo - 1
            : index == starts.Length - 1 ? _lastSeqNo
            : starts[index + 1] - 1;
    }

    private static long? ScanTailSeqNo(string path)
    {
        if (!File.Exists(path))
            return null;
        var bytes = File.ReadAllBytes(path);
        long? last = null;
        var offset = 0;
        while (offset < bytes.Length)
        {
            if (!OutboxEntryCodec.TryDecode(bytes, offset, out var e, out offset))
                break;
            last = e.SeqNo;
        }
        return last;
    }

    private static byte[] ReadSegment(string path)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var ms = new MemoryStream();
        stream.CopyTo(ms);
        return ms.ToArray();
    }
}
