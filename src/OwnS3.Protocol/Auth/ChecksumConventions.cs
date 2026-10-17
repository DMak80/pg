using System.IO.Hashing;

namespace OwnS3.Protocol.Auth;

// Байтовая конвенция AWS контрольных сумм x-amz-checksum-* (глава 03 §3):
// crc32 — big-endian (BCL Crc32 отдаёт little-endian — разворот); crc32c —
// OwnS3.Protocol.Auth.Crc32C, big-endian по построению (в System.IO.Hashing
// 10.x тип Crc32C отсутствует — своя реализация). Единая точка конвенции для
// трейлерной (AwsChunkedReader) и заголовочной (App.HashingBodyStream) сверки
// — пути не расходятся; фиксация — known-answer-векторы RFC 3720 и вектор 6
// (AwsChunkedReaderTests) + литеральные векторы HashingBodyStream-тестов.
public static class ChecksumConventions
{
    // big-endian байты crc32 от инкрементального состояния BCL.
    public static byte[] Crc32BigEndian(Crc32 crc)
    {
        var hash = crc.GetCurrentHash();
        Array.Reverse(hash);
        return hash;
    }
}
