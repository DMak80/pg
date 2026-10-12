using System.Buffers.Binary;
using System.Text;

namespace OwnS3.Replication.Outbox;

// Бинарная запись журнала v1 (глава 07 §4):
// [int32 totalLen][byte version=1][byte type][int64 seqNo]
// [int64 modTimeUnixMs][int64 size][str bucket][str key][str etag][str contentSha256][str originHostId]
// (str = int32 len + utf8; totalLen — длина payload после поля totalLen).
public static class OutboxEntryCodec
{
    private const byte Version = 1;

    public static byte[] Encode(ReplicationEvent e)
    {
        var bucket = Encoding.UTF8.GetBytes(e.Bucket);
        var key = Encoding.UTF8.GetBytes(e.Key);
        var etag = Encoding.UTF8.GetBytes(e.ETag);
        var sha = Encoding.UTF8.GetBytes(e.ContentSha256);
        var origin = Encoding.UTF8.GetBytes(e.OriginHostId);

        var payloadLen = 1 + 1 + 8 + 8 + 8
            + 4 + bucket.Length + 4 + key.Length + 4 + etag.Length + 4 + sha.Length + 4 + origin.Length;
        var buffer = new byte[4 + payloadLen];
        var span = buffer.AsSpan();
        BinaryPrimitives.WriteInt32LittleEndian(span, payloadLen);
        var off = 4;
        span[off++] = Version;
        span[off++] = (byte)e.Type;
        BinaryPrimitives.WriteInt64LittleEndian(span.Slice(off, 8), e.SeqNo); off += 8;
        BinaryPrimitives.WriteInt64LittleEndian(span.Slice(off, 8), e.ModTimeUnixMs); off += 8;
        BinaryPrimitives.WriteInt64LittleEndian(span.Slice(off, 8), e.Size); off += 8;
        off = WriteString(span, off, bucket);
        off = WriteString(span, off, key);
        off = WriteString(span, off, etag);
        off = WriteString(span, off, sha);
        WriteString(span, off, origin);
        return buffer;
    }

    // Декод записи с offset; false — неполная/битая запись (nextOffset не определён).
    public static bool TryDecode(byte[] buffer, int offset, out ReplicationEvent @event, out int nextOffset)
    {
        @event = null!;
        nextOffset = offset;
        if (offset + 4 > buffer.Length)
            return false;
        var payloadLen = BinaryPrimitives.ReadInt32LittleEndian(buffer.AsSpan(offset, 4));
        if (payloadLen < 3 || offset + 4 + payloadLen > buffer.Length)
            return false;
        var payload = buffer.AsSpan(offset + 4, payloadLen);
        if (payload[0] != Version)
            return false;
        if (!Enum.IsDefined(typeof(ReplicationEventType), payload[1]))
            return false;
        var type = (ReplicationEventType)payload[1];
        var seqNo = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(2, 8));
        var modTime = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(10, 8));
        var size = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(18, 8));
        if (!TryReadString(payload, 26, out var bucket, out var off)) return false;
        if (!TryReadString(payload, off, out var key, out off)) return false;
        if (!TryReadString(payload, off, out var etag, out off)) return false;
        if (!TryReadString(payload, off, out var sha, out off)) return false;
        if (!TryReadString(payload, off, out var origin, out _)) return false;
        @event = new ReplicationEvent(seqNo, type, bucket, key, modTime, etag, size, sha, origin);
        nextOffset = offset + 4 + payloadLen;
        return true;
    }

    private static int WriteString(Span<byte> span, int off, byte[] value)
    {
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(off, 4), value.Length);
        value.CopyTo(span.Slice(off + 4, value.Length));
        return off + 4 + value.Length;
    }

    private static bool TryReadString(ReadOnlySpan<byte> payload, int off, out string value, out int next)
    {
        value = string.Empty;
        next = off;
        if (off + 4 > payload.Length)
            return false;
        var len = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(off, 4));
        if (len < 0 || off + 4 + len > payload.Length)
            return false;
        value = Encoding.UTF8.GetString(payload.Slice(off + 4, len));
        next = off + 4 + len;
        return true;
    }
}
