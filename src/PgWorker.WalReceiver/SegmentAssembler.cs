using System.Collections.Concurrent;
using PgWorker.Backups;

namespace PgWorker.WalReceiver;

/// <summary>Кусок XLogData потока физической репликации: LSN начала куска,
/// serverWalEnd — позиция записи сервера в момент посылки сообщения (база
/// backlog-гейджа, arch/18 §2.8), байты (→ wal-агент, arch/19 §3). WalStart
/// обязан продолжать поток без разрывов.</summary>
public readonly record struct XLogChunk(ulong WalStart, ulong WalEnd, ReadOnlyMemory<byte> Data);

/// <summary>LSN → PG-формат `X/Y`: старшие 32 бита / младшие (hex) — формат
/// диагностики сообщений приёмника (stdout-маркеры упразднены).</summary>
public static class WalLsn
{
    public static string Text(ulong value)
        => $"{value >> 32:X}/{value & 0xFFFFFFFF:X}";
}

/// <summary>Закрытый WAL-сегмент, готовый к put в S3: TLI, 24-hex имя (WalFileName),
/// ровно SegmentBytes байт, LSN конца сегмента (для подтверждения слота).
/// Data — буфер из SegmentBufferPool в исключительном владении до возврата
/// (после завершения put байты сегмента никому не нужны).</summary>
public sealed record ClosedSegment(uint Tli, string Name, byte[] Data, ulong EndLsn);

/// <summary>Пул сегментных буферов hot-path : буфер = wal_segment_size
/// байт (16 МиБ — Large Object Heap при прямой аллокации каждого сегмента);
/// Rent/Return переиспользуют буферы между закрытыми сегментами — закрытие
/// сегмента отдаёт сам буфер без копии. Потокобезопасён (очередь); владение
/// исключительное с Rent до Return. Не ограничен по размеру: максимум живых
/// буферов = сегментов «в полёте» (стрим последователен — единицы).</summary>
public static class SegmentBufferPool
{
    private static readonly ConcurrentQueue<byte[]> Free = new();

    public static byte[] Rent()
        => Free.TryDequeue(out var buffer) ? buffer : new byte[WalFileName.SegmentBytes];

    public static void Return(byte[] buffer) => Free.Enqueue(buffer);
}

/// <summary>Сборка WAL-сегментов от LSN (→ wal-агент, arch/19 §3, спека §3.1 п.3):
/// накопление байтов чанков напрямую в сегментном буфере пула (байт копируется
/// один раз: чанк → сегмент; закрытый сегмент отдаёт буфер без копии — в
/// hot-path нет List/ToArray/memmove-сдвигов и LOH-аллокаций на каждый
/// сегмент). Старт с произвольной позиции: байты до ближайшей границы сегмента
/// отбрасываются (неполный головной сегмент не выгружается — цепочку закрепляет
/// первым ПОЛНЫМ сегментом). TLI — из long page header сегмента (LE u32 @4),
/// валидация по xlp_pageaddr (LE u64 @8) == границе сегмента; без валидного
/// заголовка TLI наследуется. Имена сверяются с математикой WalFileName.</summary>
public sealed class SegmentAssembler(ulong startLsn)
{
    private ulong _lastAppended = startLsn;
    private byte[]? _buffer;   // сегментный буфер пула (SegmentBytes), владение до Close
    private int _bufferCount;  // байт накоплено в текущем (незакрытом) сегменте
    private ulong _segmentStart;
    private uint? _currentTli;

    /// <summary>LSN последнего принятого байта (первый — startLsn).</summary>
    public ulong LastAppendedLsn => _lastAppended;

    /// <summary>serverWalEnd последнего принятого куска — позиция записи
    /// сервера (arch/18 §2.8: база backlog-гейджа live-стрима).</summary>
    public ulong ServerWalEnd => _serverWalEnd;

    private ulong _serverWalEnd;

    public IReadOnlyList<ClosedSegment> Append(XLogChunk chunk)
    {
        _serverWalEnd = chunk.WalEnd;
        if (chunk.WalStart != _lastAppended)
            throw new ApplicationException(
                $"WAL stream gap: WalStart {WalLsn.Text(chunk.WalStart)} " +
                $"≠ expected {WalLsn.Text(_lastAppended)}");

        var closed = new List<ClosedSegment>();
        var data = chunk.Data.Span;
        var offset = 0;

        // Головной сегмент не полон: байты до ближайшей границы отбрасываются
        // (неполный головной сегмент не выгружается).
        if (_bufferCount == 0 && _lastAppended % (ulong)WalFileName.SegmentBytes != 0)
        {
            var boundary = _lastAppended
                + (ulong)WalFileName.SegmentBytes
                - _lastAppended % (ulong)WalFileName.SegmentBytes;
            offset = (int)Math.Min(boundary - _lastAppended, (ulong)data.Length);
        }

        // Начало нового сегмента — позиция первого записанного байта.
        if (offset < data.Length && _bufferCount == 0)
            _segmentStart = chunk.WalStart + (ulong)offset;

        // Копирование чанка в сегментный буфер порциями до границ сегментов;
        // закрытие — ровно на SegmentBytes (буфер отдан ClosedSegment, следующий
        // Rent — лениво следующим куском).
        while (offset < data.Length)
        {
            _buffer ??= SegmentBufferPool.Rent();
            var write = Math.Min(
                data.Length - offset, (int)WalFileName.SegmentBytes - _bufferCount);
            data.Slice(offset, write).CopyTo(_buffer.AsSpan(_bufferCount, write));
            _bufferCount += write;
            offset += write;
            if (_bufferCount == (int)WalFileName.SegmentBytes)
                closed.Add(Close());
        }

        _lastAppended = chunk.WalStart + (ulong)chunk.Data.Length;
        return closed;
    }

    /// <summary>Закрытие сегмента: имя от границы (WalFileName), TLI из long page
    /// header (валидация pageaddr == границе) или наследование. Длина всегда ровно
    /// SegmentBytes (буфер пула; проверка — internal-инвариант). Буфер отдан
    /// сегменту — сборщик берёт следующий из пула лениво.</summary>
    private ClosedSegment Close()
    {
        var segmentBytes = _buffer!;
        _buffer = null;
        _bufferCount = 0;
        if (segmentBytes.Length != (int)WalFileName.SegmentBytes)
            throw new ApplicationException(
                $"internal: segment {segmentBytes.Length} bytes ≠ {WalFileName.SegmentBytes}");

        uint tli;
        if (LeU64(segmentBytes, 8) == _segmentStart)
            tli = LeU32(segmentBytes, 4);
        else if (_currentTli is { } inherited)
            tli = inherited;
        else
            throw new ApplicationException(
                $"first uploadable segment at {WalLsn.Text(_segmentStart)} " +
                "without a valid page header — TLI undefined");

        _currentTli = tli;
        var segId = (uint)(_segmentStart / (ulong)WalFileName.SegmentBytes);
        var name = new WalFileName(tli, segId / WalFileName.SegsPerLog, segId % WalFileName.SegsPerLog).Name;
        var endLsn = _segmentStart + (ulong)WalFileName.SegmentBytes;
        _segmentStart = endLsn;
        return new ClosedSegment(tli, name, segmentBytes, endLsn);
    }

    private static uint LeU32(byte[] b, int offset)
        => b[offset] | (uint)b[offset + 1] << 8 | (uint)b[offset + 2] << 16 | (uint)b[offset + 3] << 24;

    private static ulong LeU64(byte[] b, int offset)
    {
        ulong value = 0;
        for (var i = 7; i >= 0; i--)
            value = (value << 8) | b[offset + i];
        return value;
    }
}
