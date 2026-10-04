using PgWorker.Backups;

namespace PgWorker.WalReceiver;

/// <summary>Кусок XLogData потока физической репликации: LSN начала куска + байты
/// (t27, arch/19 §3). WalStart обязан продолжать поток без разрывов.</summary>
public readonly record struct XLogChunk(ulong WalStart, ReadOnlyMemory<byte> Data);

/// <summary>Закрытый WAL-сегмент, готовый к put в S3: TLI, 24-hex имя (WalFileName),
/// ровно SegmentBytes байт, LSN конца сегмента (для подтверждения слота).</summary>
public sealed record ClosedSegment(uint Tli, string Name, byte[] Data, ulong EndLsn);

/// <summary>Сборка WAL-сегментов от LSN (t27, arch/19 §3, спека §3.1 п.3): буфер
/// текущего незакрытого сегмента в памяти (кэш без durability — MULTI-HOST),
/// чанк может пересекать границы — сплит на 0..N закрытых сегментов ровно
/// SegmentBytes. Старт с произвольной позиции: байты до ближайшей границы сегмента
/// отбрасываются (неполный головной сегмент не выгружается — цепочку закрепляет
/// первым ПОЛНЫМ сегментом). TLI — из long page header сегмента (LE u32 @4),
/// валидация по xlp_pageaddr (LE u64 @8) == границе сегмента; без валидного
/// заголовка TLI наследуется. Имена сверяются с математикой WalFileName.</summary>
public sealed class SegmentAssembler(ulong startLsn)
{
    private ulong _lastAppended = startLsn;
    private readonly List<byte> _buffer = [];
    private ulong _segmentStart;
    private uint? _currentTli;
    private bool _segmentBegun;

    /// <summary>LSN последнего принятого байта (первый — startLsn).</summary>
    public ulong LastAppendedLsn => _lastAppended;

    public IReadOnlyList<ClosedSegment> Append(XLogChunk chunk)
    {
        if (chunk.WalStart != _lastAppended)
            throw new ApplicationException(
                $"разрыв потока WAL: WalStart {WalReceiverMarkers.LsnText(chunk.WalStart)} " +
                $"≠ ожидаемого {WalReceiverMarkers.LsnText(_lastAppended)}");

        var closed = new List<ClosedSegment>();
        var data = chunk.Data.Span;
        var offset = 0;

        if (!_segmentBegun && _lastAppended % (ulong)WalFileName.SegmentBytes != 0)
        {
            // Головной сегмент не полон: байты до ближайшей границы отбрасываются
            // (неполный головной сегмент не выгружается).
            var boundary = _lastAppended
                + (ulong)WalFileName.SegmentBytes
                - _lastAppended % (ulong)WalFileName.SegmentBytes;
            var skip = (int)Math.Min(boundary - _lastAppended, (ulong)data.Length);
            offset += skip;
        }

        if (offset < data.Length)
        {
            _segmentBegun = true;
            if (_buffer.Count == 0)
                _segmentStart = _lastAppended + (ulong)offset;
            _buffer.AddRange(data[offset..].ToArray());
        }

        // Пока буфер содержит полный сегмент — закрываем ровно SegmentBytes.
        while (_buffer.Count >= (int)WalFileName.SegmentBytes)
        {
            var segmentBytes = _buffer.Take((int)WalFileName.SegmentBytes).ToArray();
            _buffer.RemoveRange(0, (int)WalFileName.SegmentBytes);
            closed.Add(Close(segmentBytes));
        }

        _lastAppended = chunk.WalStart + (ulong)chunk.Data.Length;
        return closed;
    }

    /// <summary>Закрытие сегмента: имя от границы (WalFileName), TLI из long page
    /// header (валидация pageaddr == границе) или наследование. Длина всегда ровно
    /// SegmentBytes (проверка при целостности буфера — internal-инвариант).</summary>
    private ClosedSegment Close(byte[] segmentBytes)
    {
        if (segmentBytes.Length != (int)WalFileName.SegmentBytes)
            throw new ApplicationException(
                $"internal: сегмент {segmentBytes.Length} байт ≠ {WalFileName.SegmentBytes}");

        uint tli;
        if (LeU64(segmentBytes, 8) == _segmentStart)
            tli = LeU32(segmentBytes, 4);
        else if (_currentTli is { } inherited)
            tli = inherited;
        else
            throw new ApplicationException(
                $"первый выгружаемый сегмент на {WalReceiverMarkers.LsnText(_segmentStart)} " +
                "без валидного page header — TLI не определён");

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
