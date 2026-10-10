using System.Buffers.Text;
using System.IO.Hashing;
using System.Security.Cryptography;
using System.Text;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.Errors;

namespace OwnS3.UnitTests;

// Чанковая потоковая подпись (arch/owns3/03 §3): фрейминг, цепочка от seed,
// 0-чанк, трейлеры (вектор 6), лимиты и отказы. Тела строит TestSigV4Signer.
public sealed class AwsChunkedReaderTests
{
    private static readonly byte[] Body = Encoding.UTF8.GetBytes(TestVectors.PutBody);

    private static AwsChunkedReadingContext Context(long decodedLength, bool trailers = false,
        IReadOnlyList<string>? trailerNames = null) =>
        new(TestVectors.SecretKey, TestVectors.AmzDate, TestVectors.Scope, TestVectors.GetObjSignature,
            decodedLength, trailers, trailerNames ?? []);

    private static byte[] ReadAll(AwsChunkedReader reader)
    {
        using var output = new MemoryStream();
        reader.CopyTo(output);
        return output.ToArray();
    }

    [Fact]
    public void Read_MultiChunkStream_ReturnsConcatenatedDataAndEof()
    {
        // Arrange: 2 чанка данных + 0-чанк (chunkSize делит тело на 2 части)
        var data = Encoding.UTF8.GetBytes("0123456789abcdefghij");
        var body = TestSigV4Signer.BuildChunkedBody(TestVectors.SecretKey, data,
            TestVectors.GetObjSignature, TestSigV4Signer.DefaultDate, TestVectors.Region, chunkSize: 10);
        using var reader = new AwsChunkedReader(new MemoryStream(body), Context(data.Length));

        // Act
        var read = ReadAll(reader);
        var secondRead = reader.Read(new byte[8], 0, 8);

        // Assert: содержимое = конкатенация чанков; после 0-чанка — EOF
        read.Should().Equal(data);
        secondRead.Should().Be(0);
    }

    [Fact]
    public void Read_AwsVector5_ChunkChainSignaturesValid()
    {
        // Arrange: вектор 5 — один чанк «Welcome to Amazon S3.» с подписью
        // c115618c… и 0-чанком b01db302… (подписи фреймов в теле — литералы вектора)
        var body = TestSigV4Signer.BuildChunkedBody(TestVectors.SecretKey, Body,
            TestVectors.GetObjSignature, TestSigV4Signer.DefaultDate, TestVectors.Region);
        var bodyText = Encoding.ASCII.GetString(body);
        bodyText.Should().Contain(TestVectors.Chunk1Signature, "вектор 5 фиксирует подпись чанка 1");
        bodyText.Should().Contain(TestVectors.Chunk0Signature, "вектор 5 фиксирует подпись 0-чанка");
        using var reader = new AwsChunkedReader(new MemoryStream(body), Context(Body.Length));

        // Act
        var read = ReadAll(reader);

        // Assert: цепочка от seed вектора 1 верифицирована
        read.Should().Equal(Body);
    }

    [Fact]
    public void Read_BrokenChunk2Signature_ThrowsOnSecondChunkOnly()
    {
        // Arrange: 2-чанковое тело, hex подписи второго фрейма испорчен
        var data = Encoding.UTF8.GetBytes("0123456789abcdefghij");
        var body = TestSigV4Signer.BuildChunkedBody(TestVectors.SecretKey, data,
            TestVectors.GetObjSignature, TestSigV4Signer.DefaultDate, TestVectors.Region, chunkSize: 10);
        // Портим первый hex-символ подписи ВТОРОГО фрейма (после первого блока данных).
        var firstFrameEnd = Encoding.ASCII.GetBytes("a;chunk-signature=64hex\r\n").Length; // заголовок 1
        var secondSigStart = body.Length; // вычислим позицию: найдём второе вхождение ";chunk-signature="
        var marker = Encoding.ASCII.GetBytes(";chunk-signature=");
        var firstMarker = IndexOf(body, marker, 0);
        var secondMarker = IndexOf(body, marker, firstMarker + 1);
        body[secondMarker + marker.Length] = body[secondMarker + marker.Length] == '0' ? (byte)'1' : (byte)'0';
        _ = firstFrameEnd; _ = secondSigStart;

        using var reader = new AwsChunkedReader(new MemoryStream(body), Context(data.Length));

        // Act: читаем первый чанк — он валиден и уже выдан; затем падение на чанке 2
        var firstChunk = new byte[10];
        var readFirst = reader.Read(firstChunk, 0, 10);
        var act = () => ReadAll(reader);

        // Assert
        readFirst.Should().Be(10);
        firstChunk.Should().Equal("0123456789"u8.ToArray());
        act.Should().Throw<S3ProtocolException>()
            .Which.Code.Should().Be(S3ErrorCode.SignatureDoesNotMatch);
    }

    [Fact]
    public void Read_MalformedFrame_InvalidRequest()
    {
        // Arrange: заголовок фрейма без «;chunk-signature=»
        var body = Encoding.ASCII.GetBytes("5\r\nhello\r\n");
        using var reader = new AwsChunkedReader(new MemoryStream(body), Context(5));

        // Act
        var act = () => ReadAll(reader);

        // Assert
        act.Should().Throw<S3ProtocolException>()
            .Which.Code.Should().Be(S3ErrorCode.InvalidRequest);
    }

    [Fact]
    public void Read_NonHexFrameSignature_InvalidRequest()
    {
        // Arrange: подпись фрейма — 64 символа, но не hex (hex-валидация
        // до декодирования/сравнения — impl-фикс код-ревью)
        var body = Encoding.ASCII.GetBytes("a;chunk-signature=" + new string('Z', 64) + "\r\n");
        using var reader = new AwsChunkedReader(new MemoryStream(body), Context(1));

        // Act
        var act = () => ReadAll(reader);

        // Assert
        act.Should().Throw<S3ProtocolException>()
            .Which.Code.Should().Be(S3ErrorCode.InvalidRequest);
    }

    [Theory]
    [InlineData(63)]
    [InlineData(65)]
    public void Read_FrameSignatureWrongLength_InvalidRequest(int signatureLength)
    {
        // Arrange: подпись фрейма не 64 hex-символов (63/65)
        var body = Encoding.ASCII.GetBytes("a;chunk-signature=" + new string('a', signatureLength) + "\r\n");
        using var reader = new AwsChunkedReader(new MemoryStream(body), Context(1));

        // Act
        var act = () => ReadAll(reader);

        // Assert
        act.Should().Throw<S3ProtocolException>()
            .Which.Code.Should().Be(S3ErrorCode.InvalidRequest);
    }

    [Fact]
    public void Checksums_KnownAnswerVectors_Rfc3720()
    {
        // Arrange: known-answer «123456789» — фиксация против endian-ошибок
        // при base64-упаковке; функции те же, что в сверке трейлеров ридера
        var data = Encoding.ASCII.GetBytes("123456789");

        // Act: Crc32C.Hash уже big-endian; Crc32 — little-endian + Reverse
        var crc32c = Convert.ToBase64String(Crc32C.Hash(data));
        var crc32 = Convert.ToBase64String(Crc32.Hash(data).Reverse().ToArray());

        // Assert: CRC32C = 0xE3069283 → «4waSgw==» (эталон RFC 3720);
        // CRC32 = 0xCBF43926 → «y/Q5Jg==» (эталон ISO-HDLC)
        crc32c.Should().Be("4waSgw==");
        crc32.Should().Be("y/Q5Jg==");
    }

    [Fact]
    public void Read_NonHexSize_InvalidRequest()
    {
        // Arrange: размер не в hex
        var body = Encoding.ASCII.GetBytes("zz;chunk-signature=" + new string('a', 64) + "\r\n");
        using var reader = new AwsChunkedReader(new MemoryStream(body), Context(0));

        // Act
        var act = () => ReadAll(reader);

        // Assert
        act.Should().Throw<S3ProtocolException>()
            .Which.Code.Should().Be(S3ErrorCode.InvalidRequest);
    }

    [Fact]
    public void Read_MissingCrlfAfterData_InvalidRequest()
    {
        // Arrange: после данных нет \r\n (поток обрезан)
        var sig = TestSigV4Signer.ChunkSignature(TestVectors.SecretKey, TestVectors.GetObjSignature,
            Body, TestSigV4Signer.DefaultDate, TestVectors.Region);
        var body = Encoding.ASCII.GetBytes($"{Body.Length:x};chunk-signature={sig}\r\n")
            .Concat(Body).ToArray(); // без завершающего \r\n
        using var reader = new AwsChunkedReader(new MemoryStream(body), Context(Body.Length));

        // Act
        var act = () => ReadAll(reader);

        // Assert
        act.Should().Throw<S3ProtocolException>()
            .Which.Code.Should().Be(S3ErrorCode.InvalidRequest);
    }

    [Fact]
    public void Read_ChunkOver16MiB_InvalidRequest()
    {
        // Arrange: фрейм объявляет размер 0x1000001 (> 16 МиБ) — отказ по заголовку фрейма
        var body = Encoding.ASCII.GetBytes("1000001;chunk-signature=" + new string('a', 64) + "\r\n");
        using var reader = new AwsChunkedReader(new MemoryStream(body), Context(0));

        // Act
        var act = () => ReadAll(reader);

        // Assert: лимит 16 МиБ (arch-правка 3)
        act.Should().Throw<S3ProtocolException>()
            .Which.Code.Should().Be(S3ErrorCode.InvalidRequest);
    }

    [Fact]
    public void Read_DecodedLengthMismatch_InvalidRequest()
    {
        // Arrange: сумма данных ≠ x-amz-decoded-content-length (заявлено меньше)
        var body = TestSigV4Signer.BuildChunkedBody(TestVectors.SecretKey, Body,
            TestVectors.GetObjSignature, TestSigV4Signer.DefaultDate, TestVectors.Region);
        using var reader = new AwsChunkedReader(new MemoryStream(body), Context(Body.Length - 1));

        // Act
        var act = () => ReadAll(reader);

        // Assert: несоответствие ловится на финальной сверке (EOF)
        act.Should().Throw<S3ProtocolException>()
            .Which.Code.Should().Be(S3ErrorCode.InvalidRequest);
    }

    [Fact]
    public void Read_TrailerMode_Crc32Vector6_Ok()
    {
        // Arrange: вектор 6 — 0-чанк + x-amz-checksum-crc32:Ox7nCg== + подпись 570042c8…
        // .NET Crc32 отдаёт little-endian; AWS-формат контрольных сумм — big-endian.
        var crc = Crc32.Hash(Body).Reverse().ToArray();
        Convert.ToBase64String(crc).Should().Be(TestVectors.Crc32B64, "вектор 6 фиксирует crc32");
        var body = TestSigV4Signer.BuildChunkedBody(TestVectors.SecretKey, Body,
            TestVectors.GetObjSignature, TestSigV4Signer.DefaultDate, TestVectors.Region,
            trailerName: "x-amz-checksum-crc32", trailerChecksum: crc);
        var bodyText = Encoding.ASCII.GetString(body);
        bodyText.Should().Contain("x-amz-trailer-signature:" + TestVectors.TrailerSignature,
            "вектор 6 фиксирует trailer-подпись");
        using var reader = new AwsChunkedReader(new MemoryStream(body),
            Context(Body.Length, trailers: true, trailerNames: ["x-amz-checksum-crc32"]));

        // Act
        var read = ReadAll(reader);

        // Assert: сверка crc32 пройдена, EOF после трейлеров
        read.Should().Equal(Body);
    }

    [Fact]
    public void Read_TrailerMode_Sha256Checksum_Ok()
    {
        // Arrange: трейлер sha256 = base64(sha256 тела) — значение вектора 6
        var checksum = SHA256.HashData(Body);
        Convert.ToBase64String(checksum).Should().Be(TestVectors.Sha256B64);
        var body = TestSigV4Signer.BuildChunkedBody(TestVectors.SecretKey, Body,
            TestVectors.GetObjSignature, TestSigV4Signer.DefaultDate, TestVectors.Region,
            trailerName: "x-amz-checksum-sha256", trailerChecksum: checksum);
        using var reader = new AwsChunkedReader(new MemoryStream(body),
            Context(Body.Length, trailers: true, trailerNames: ["x-amz-checksum-sha256"]));

        // Act
        var read = ReadAll(reader);

        // Assert
        read.Should().Equal(Body);
    }

    [Fact]
    public void Read_TrailerMode_Crc32CChecksum_Ok()
    {
        // Arrange: трейлер crc32c; значение сверено независимо (python-эталон, big-endian)
        var checksum = Crc32C.Hash(Body);
        Convert.ToBase64String(checksum).Should().Be("5G2V+Q==");
        var body = TestSigV4Signer.BuildChunkedBody(TestVectors.SecretKey, Body,
            TestVectors.GetObjSignature, TestSigV4Signer.DefaultDate, TestVectors.Region,
            trailerName: "x-amz-checksum-crc32c", trailerChecksum: checksum);
        using var reader = new AwsChunkedReader(new MemoryStream(body),
            Context(Body.Length, trailers: true, trailerNames: ["x-amz-checksum-crc32c"]));

        // Act
        var read = ReadAll(reader);

        // Assert
        read.Should().Equal(Body);
    }

    [Fact]
    public void Read_TrailerMode_Sha1Checksum_Ok()
    {
        // Arrange: трейлер sha1 (BCL)
        var checksum = SHA1.HashData(Body);
        var body = TestSigV4Signer.BuildChunkedBody(TestVectors.SecretKey, Body,
            TestVectors.GetObjSignature, TestSigV4Signer.DefaultDate, TestVectors.Region,
            trailerName: "x-amz-checksum-sha1", trailerChecksum: checksum);
        using var reader = new AwsChunkedReader(new MemoryStream(body),
            Context(Body.Length, trailers: true, trailerNames: ["x-amz-checksum-sha1"]));

        // Act
        var read = ReadAll(reader);

        // Assert
        read.Should().Equal(Body);
    }

    [Fact]
    public void Read_TrailerChecksumMismatch_BadDigest()
    {
        // Arrange: crc32-трейлер с чужим base64 (подпись трейлера корректна по значению)
        var wrong = Crc32.Hash(Encoding.UTF8.GetBytes("other body"));
        var body = TestSigV4Signer.BuildChunkedBody(TestVectors.SecretKey, Body,
            TestVectors.GetObjSignature, TestSigV4Signer.DefaultDate, TestVectors.Region,
            trailerName: "x-amz-checksum-crc32", trailerChecksum: wrong);
        using var reader = new AwsChunkedReader(new MemoryStream(body),
            Context(Body.Length, trailers: true, trailerNames: ["x-amz-checksum-crc32"]));

        // Act
        var act = () => ReadAll(reader);

        // Assert: несовпадение контрольной суммы с телом — BadDigest
        act.Should().Throw<S3ProtocolException>()
            .Which.Code.Should().Be(S3ErrorCode.BadDigest);
    }

    [Fact]
    public void Read_UnsupportedTrailerName_InvalidRequest()
    {
        // Arrange: неподдерживаемое имя в x-amz-trailer (глава 03 §3)
        var body = TestSigV4Signer.BuildChunkedBody(TestVectors.SecretKey, Body,
            TestVectors.GetObjSignature, TestSigV4Signer.DefaultDate, TestVectors.Region);
        var act = () => new AwsChunkedReader(new MemoryStream(body),
            Context(Body.Length, trailers: true, trailerNames: ["x-amz-checksum-crc64nvme"]));

        // Act / Assert: отказ уже при создании ридера
        act.Should().Throw<S3ProtocolException>()
            .Which.Code.Should().Be(S3ErrorCode.InvalidRequest);
    }

    [Fact]
    public void Read_BrokenTrailerSignature_SignatureDoesNotMatch()
    {
        // Arrange: trailer-подпись испорчена (последний hex заменён)
        var crc = Crc32.Hash(Body);
        var body = TestSigV4Signer.BuildChunkedBody(TestVectors.SecretKey, Body,
            TestVectors.GetObjSignature, TestSigV4Signer.DefaultDate, TestVectors.Region,
            trailerName: "x-amz-checksum-crc32", trailerChecksum: crc);
        var marker = Encoding.ASCII.GetBytes("x-amz-trailer-signature:");
        var index = IndexOf(body, marker, 0);
        // Портим последний hex-символ подписи (перед завершающим \r\n)
        body[^3] = body[^3] == '0' ? (byte)'1' : (byte)'0';
        _ = index;
        using var reader = new AwsChunkedReader(new MemoryStream(body),
            Context(Body.Length, trailers: true, trailerNames: ["x-amz-checksum-crc32"]));

        // Act
        var act = () => ReadAll(reader);

        // Assert
        act.Should().Throw<S3ProtocolException>()
            .Which.Code.Should().Be(S3ErrorCode.SignatureDoesNotMatch);
    }

    private static int IndexOf(byte[] haystack, byte[] needle, int start)
    {
        for (var i = start; i <= haystack.Length - needle.Length; i++)
        {
            var match = true;
            for (var j = 0; j < needle.Length; j++)
            {
                if (haystack[i + j] != needle[j]) { match = false; break; }
            }
            if (match) return i;
        }
        return -1;
    }
}
