using System.IO;
using System.Text;

namespace KafkaWorker.Provisioning.Kafka;

// Писатель примитивов Kafka-протокола (arch/16 §2.4, spec §4.1): всё
// big-endian; compact-примитивы (uvarint) — flexible-тела, old-style (int16/
// int32 длины) — заголовки и не-flexible тела. Пишет в память; ToFrame()
// отдаёт кадр «int32 длина + payload».
internal sealed class KafkaProtocolWriter
{
    private readonly MemoryStream _buffer = new();

    public void WriteInt16(short value) => WriteRaw([(byte)(value >> 8), (byte)value]);
    public void WriteInt32(int value) => WriteRaw([(byte)(value >> 24), (byte)(value >> 16), (byte)(value >> 8), (byte)value]);
    public void WriteInt64(long value)
    {
        WriteInt32((int)(value >> 32));
        WriteInt32((int)value);
    }

    // Unsigned varint: 7 бит на байт, старший бит — продолжение (KIP-482).
    public void WriteUVarint(ulong value)
    {
        while (value >= 0x80)
        {
            _buffer.WriteByte((byte)(value | 0x80));
            value >>= 7;
        }
        _buffer.WriteByte((byte)value);
    }

    // Old-style строка (int16 длина + UTF-8; -1 = null) — заголовки, не-flexible тела.
    public void WriteString(string? value)
    {
        if (value is null)
        {
            WriteInt16(-1);
            return;
        }
        WriteInt16((short)Encoding.UTF8.GetByteCount(value));
        WriteRaw(Encoding.UTF8.GetBytes(value));
    }

    // Compact-строка (uvarint длина+1; 0 = null) — flexible тела.
    public void WriteCompactString(string? value)
    {
        if (value is null)
        {
            WriteUVarint(0);
            return;
        }
        WriteUVarint((ulong)Encoding.UTF8.GetByteCount(value) + 1);
        WriteRaw(Encoding.UTF8.GetBytes(value));
    }

    // Байты (int32 длина + данные) — SaslAuthenticate.AuthBytes.
    public void WriteBytes(byte[] value)
    {
        WriteInt32(value.Length);
        WriteRaw(value);
    }

    // Old-style массив: int32 счётчик.
    public void WriteArrayHeader(int count) => WriteInt32(count);

    // Compact-массив: uvarint счётчик+1 (0 = null — не используется писателем).
    public void WriteCompactArrayHeader(int count) => WriteUVarint((ulong)count + 1);

    // Пустая секция tagged-полей (наши сообщения tagged-полей не несут).
    public void WriteTaggedSection() => WriteUVarint(0);

    // RequestHeader v1 (не-flexible запросы): api_key, api_version,
    // correlation_id, client_id old-style (int16 — ВСЕГДА old-style, даже в
    // flexible-запросах, апстрим RequestHeader.json).
    public void WriteRequestHeaderV1(short apiKey, short apiVersion, int correlationId, string? clientId)
    {
        WriteInt16(apiKey);
        WriteInt16(apiVersion);
        WriteInt32(correlationId);
        WriteString(clientId);
    }

    // RequestHeader v2 (flexible запросы): v1 + пустая tagged-секция.
    public void WriteRequestHeaderV2(short apiKey, short apiVersion, int correlationId, string? clientId)
    {
        WriteRequestHeaderV1(apiKey, apiVersion, correlationId, clientId);
        WriteTaggedSection();
    }

    // Payload без кадра-длины.
    public byte[] ToPayload() => _buffer.ToArray();

    // Кадр: int32 длина + payload (запрос; ответ имеет тот же формат).
    public byte[] ToFrame()
    {
        var payload = _buffer.ToArray();
        var frame = new byte[4 + payload.Length];
        frame[0] = (byte)(payload.Length >> 24);
        frame[1] = (byte)(payload.Length >> 16);
        frame[2] = (byte)(payload.Length >> 8);
        frame[3] = (byte)payload.Length;
        payload.CopyTo(frame, 4);
        return frame;
    }

    private void WriteRaw(byte[] bytes) => _buffer.Write(bytes, 0, bytes.Length);
}

// Читатель примитивов Kafka-протокола (arch/16 §2.4, spec §4.1): поверх
// payload-байтов (после кадра-длины); любое повреждение (нехватка данных,
// чрезмерная длина) — ApplicationException, не зависание.
internal sealed class KafkaProtocolReader(byte[] payload)
{
    private int _position;

    public int Remaining => payload.Length - _position;

    public short ReadInt16() => (short)((ReadByte() << 8) | ReadByte());
    public int ReadInt32() => (ReadInt16() << 16) | (ushort)ReadInt16();
    // Int64-читателя нет: потребителей нет; WriteInt64 оставлен — симметрия
    // для golden-тестов.

    public ulong ReadUVarint()
    {
        ulong value = 0;
        for (var shift = 0; ; shift += 7)
        {
            if (shift >= 64)
                throw new ApplicationException("Kafka protocol: uvarint is too long");
            var b = ReadByte();
            value |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return value;
        }
    }

    // Old-style nullable строка (int16 длина; -1 = null).
    public string? ReadString()
    {
        var length = ReadInt16();
        if (length < 0)
            return null;
        return Encoding.UTF8.GetString(ReadRaw(length));
    }

    // Compact nullable строка (uvarint длина+1; 0 = null).
    public string? ReadCompactString()
    {
        var length = (int)ReadUVarint();
        if (length == 0)
            return null;
        return Encoding.UTF8.GetString(ReadRaw(length - 1));
    }

    // Байты (int32 длина + данные).
    public byte[] ReadBytes()
    {
        var length = ReadInt32();
        if (length < 0)
            throw new ApplicationException($"Kafka protocol: negative bytes length {length}");
        return ReadRaw(length);
    }

    // Old-style массив: int32 счётчик.
    public int ReadArrayHeader() => ReadInt32();

    // Compact-массив: uvarint счётчик+1 (0 = null → -1).
    public int ReadCompactArrayHeader()
    {
        var value = (int)ReadUVarint();
        return value - 1;
    }

    // Скип tagged-полей ответа (unknown tags): uvarint count, для каждого
    // uvarint tag, uvarint size, size байт (spec §4.1).
    public void SkipTaggedSection()
    {
        var count = (int)ReadUVarint();
        for (var i = 0; i < count; i++)
        {
            _ = ReadUVarint(); // tag
            var size = (int)ReadUVarint();
            _ = ReadRaw(size);
        }
    }

    // ResponseHeader v0 (не-flexible ответы): int32 correlation_id.
    public int ReadResponseHeaderV0() => ReadInt32();

    // ResponseHeader v1 (flexible ответы): correlation_id + tagged-секция (скип).
    public int ReadResponseHeaderV1()
    {
        var correlationId = ReadInt32();
        SkipTaggedSection();
        return correlationId;
    }

    private byte ReadByte()
    {
        if (_position >= payload.Length)
            throw new ApplicationException("Kafka protocol: unexpected end of payload");
        return payload[_position++];
    }

    private byte[] ReadRaw(int length)
    {
        // Сравнение БЕЗ сложения: _position + length в unchecked-контексте при
        // length ≈ int.MaxValue переполнилось бы в отрицательное и проходило
        // бы проверку (попытка аллокации ~2 ГБ из битого ответа брокера);
        // payload.Length - _position ≥ 0 всегда — инвариант _position ≤ length.
        if (length < 0 || length > payload.Length - _position)
            throw new ApplicationException("Kafka protocol: unexpected end of payload");
        var result = new byte[length];
        Array.Copy(payload, _position, result, 0, length);
        _position += length;
        return result;
    }
}
