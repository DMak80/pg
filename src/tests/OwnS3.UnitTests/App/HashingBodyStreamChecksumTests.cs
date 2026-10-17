using System.IO.Hashing;
using System.Security.Cryptography;
using System.Text;
using OwnS3.App.Handlers;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.Errors;

namespace OwnS3.UnitTests.App;

// Сверка заголовочной контрольной суммы в HashingBodyStream (глава 02 PutObject):
// вычисление ровно выбранного алгоритма за один проход, сверка на EOF в порядке
// sha256-подпись → checksum → MD5. Векторы: crc32/crc32c — литеральные эталоны
// «123456789» (ISO-HDLC 0xCBF43926 big-endian / RFC 3720 0xE3069283); sha1/sha256 —
// BCL (тестируется сборка и конвенция, не BCL); crc32-вектор 6 трейлерного пути —
// та же константа, что у AwsChunkedReader (идентичность путей, критерий 8).
// Типы: crc32 — System.IO.Hashing.Crc32 (little-endian → helper разворота),
// crc32c — OwnS3.Protocol.Auth.Crc32C (big-endian по построению).
public sealed class HashingBodyStreamChecksumTests
{
    private static readonly byte[] Data = Encoding.ASCII.GetBytes("123456789");

    // Эталонная сумма по алгоритму; CRC-конвенция — та же, что у трейлерного
    // пути: crc32 — big-endian (общий helper), crc32c — OwnS3.Protocol.Auth.Crc32C.
    private static string ChecksumOf(ChecksumAlgorithm algorithm, byte[] data)
    {
        switch (algorithm)
        {
            case ChecksumAlgorithm.Crc32:
                var crc32 = new Crc32();
                crc32.Append(data);
                return Convert.ToBase64String(ChecksumConventions.Crc32BigEndian(crc32));
            case ChecksumAlgorithm.Crc32C:
                var crc32c = new Crc32C();
                crc32c.Append(data);
                return Convert.ToBase64String(crc32c.GetCurrentHash());
            case ChecksumAlgorithm.Sha1:
                return Convert.ToBase64String(SHA1.HashData(data));
            default:
                return Convert.ToBase64String(SHA256.HashData(data));
        }
    }

    // Чтение до EOF — сверка выполняется на нулевом Read (конструктор 3 аргумента
    // + checksum). Дочитывание обязательным элементом кейса.
    private static void ReadAll(ChecksumAlgorithm algorithm, string expected)
    {
        using var stream = new HashingBodyStream(new MemoryStream(Data), null, null,
            new ChecksumExpectation(algorithm, expected));
        var buffer = new byte[8];
        while (stream.Read(buffer, 0, buffer.Length) > 0) { }
    }

    // AAA: корректная сумма каждого алгоритма — сверка проходит (EOF без исключения);
    // crc32/crc32c сверены против ЛИТЕРАЛЬНЫХ эталонов (фиксация конвенции).
    [Theory]
    [InlineData(ChecksumAlgorithm.Crc32,  "y/Q5Jg==")] // 0xCBF43926 big-endian
    [InlineData(ChecksumAlgorithm.Crc32C, "4waSgw==")] // 0xE3069283
    public void Verify_CorrectChecksum_Passes(ChecksumAlgorithm algorithm, string expected)
    {
        // Arrange / Act / Assert
        ChecksumOf(algorithm, Data).Should().Be(expected, "независимый эталон конвенции");
        var act = () => ReadAll(algorithm, expected);
        act.Should().NotThrow();
    }

    // AAA: sha1/sha256 — BCL-эталон (конвенция sha — нативный порядок байтов BCL).
    [Theory]
    [InlineData(ChecksumAlgorithm.Sha1)]
    [InlineData(ChecksumAlgorithm.Sha256)]
    public void Verify_CorrectShaChecksum_Passes(ChecksumAlgorithm algorithm)
    {
        // Arrange / Act / Assert
        var act = () => ReadAll(algorithm, ChecksumOf(algorithm, Data));
        act.Should().NotThrow();
    }

    // AAA: битое значение (валидный base64 корректной длины, чужая сумма) — BadDigest
    // с текстом трейлерной сверки.
    [Theory]
    [InlineData(ChecksumAlgorithm.Crc32)]
    [InlineData(ChecksumAlgorithm.Crc32C)]
    [InlineData(ChecksumAlgorithm.Sha1)]
    [InlineData(ChecksumAlgorithm.Sha256)]
    public void Verify_CorruptedChecksum_ThrowsBadDigest(ChecksumAlgorithm algorithm)
    {
        // Arrange: сумма ДРУГОГО тела той же длины
        var wrong = ChecksumOf(algorithm, "other"u8.ToArray());

        // Act
        var act = () => ReadAll(algorithm, wrong);

        // Assert
        act.Should().Throw<S3ProtocolException>()
            .Which.Code.Should().Be(S3ErrorCode.BadDigest);
        act.Should().Throw<S3ProtocolException>()
            .Which.Message.Should().Be("The checksum you specified did not match the calculated checksum.");
    }

    // AAA: пустое тело + корректная сумма пустоты — сверка проходит на первом же Read.
    [Theory]
    [InlineData(ChecksumAlgorithm.Crc32,  "AAAAAA==")]
    [InlineData(ChecksumAlgorithm.Crc32C, "AAAAAA==")]
    [InlineData(ChecksumAlgorithm.Sha1,   "2jmj7l5rSw0yVb/vlWAYkK/YBwk=")]
    [InlineData(ChecksumAlgorithm.Sha256, "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=")]
    public void Verify_EmptyBody_EmptyChecksum_Passes(ChecksumAlgorithm algorithm, string emptySum)
    {
        // Arrange
        using var stream = new HashingBodyStream(new MemoryStream(), null, null,
            new ChecksumExpectation(algorithm, emptySum));

        // Act / Assert: первый Read — 0 → VerifyHashes без исключения
        stream.Read(new byte[1], 0, 1).Should().Be(0);
    }

    // AAA: порядок сверк — битая sha256-подпись И корректный checksum → InvalidRequest
    // (подпись первее).
    [Fact]
    public void Verify_Sha256SignatureCheckedFirst()
    {
        // Arrange: подпись от другого тела (64 hex), checksum корректный
        var wrongPayload = Convert.ToHexString(SHA256.HashData("other"u8.ToArray())).ToLowerInvariant();
        var correctCrc32 = "y/Q5Jg==";

        // Act
        var act = () =>
        {
            using var stream = new HashingBodyStream(new MemoryStream(Data), wrongPayload, null,
                new ChecksumExpectation(ChecksumAlgorithm.Crc32, correctCrc32));
            var buffer = new byte[8];
            while (stream.Read(buffer, 0, buffer.Length) > 0) { }
        };

        // Assert: InvalidRequest перекрывает — сверка подписи стоит первой
        act.Should().Throw<S3ProtocolException>()
            .Which.Code.Should().Be(S3ErrorCode.InvalidRequest);
    }
}
