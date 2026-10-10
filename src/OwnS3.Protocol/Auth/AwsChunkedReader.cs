using System.Buffers.Text;
using System.Globalization;
using System.IO.Hashing;
using System.Security.Cryptography;
using System.Text;
using OwnS3.Protocol.Errors;

namespace OwnS3.Protocol.Auth;

// Ридер aws-chunked-потока (arch/owns3/03 §3): фрейминг
// <hex>;chunk-signature=<sig>\r\n<данные>\r\n, цепочка подписей от seed,
// финальный 0-чанк, трейлеры с подписью AWS4-HMAC-SHA256-TRAILER и сверкой
// контрольных сумм декодированного тела. Отказы: SignatureDoesNotMatch (чанк/
// трейлер), InvalidRequest (битый фрейм, чанк > 16 МиБ, decoded-length),
// BadDigest (trailer-checksum).
// Чтение — через внутренний буфер: асинхронная дорога (Kestrel запрещает
// синхронный IO тела) и синхронная (in-memory источники) разделяют парсер.
public sealed class AwsChunkedReader : Stream
{
    private const int MaxChunkSize = 16 << 20;   // maxChunkSize референса
    private const string ChunkStringToSignAlgorithm = "AWS4-HMAC-SHA256-PAYLOAD";
    private const string TrailerStringToSignAlgorithm = "AWS4-HMAC-SHA256-TRAILER";
    private const string TrailerSignatureHeader = "x-amz-trailer-signature:";

    // Поддерживаемые трейлеры-контрольные суммы (глава 03 §3).
    private static readonly IReadOnlySet<string> SupportedTrailers = new HashSet<string>(StringComparer.Ordinal)
    {
        "x-amz-checksum-crc32", "x-amz-checksum-crc32c", "x-amz-checksum-sha1", "x-amz-checksum-sha256",
    };

    private readonly Stream _inner;
    private readonly AwsChunkedReadingContext _ctx;
    private readonly byte[] _chunkBuffer = new byte[MaxChunkSize];
    private readonly byte[] _io = new byte[64 * 1024];
    private readonly Dictionary<string, string> _receivedTrailers = new(StringComparer.Ordinal);
    private readonly byte[] _lineBuffer = new byte[4096];

    // Инкрементальные контрольные суммы по декодированным данным (только из x-amz-trailer).
    private readonly Crc32? _crc32;
    private readonly Crc32C? _crc32c;
    private readonly IncrementalHash? _sha1;
    private readonly IncrementalHash? _sha256;

    private string _previousSignature;
    private int _chunkOffset;
    private int _chunkLength;
    private long _totalDecoded;
    private bool _finished;
    private int _ioStart;
    private int _ioEnd;

    public AwsChunkedReader(Stream inner, AwsChunkedReadingContext context)
    {
        _inner = inner;
        _ctx = context;
        _previousSignature = context.SeedSignature;

        // Имена трейлеров сверяются с поддерживаемым набором сразу (глава 03 §3:
        // прочие имена — 400 InvalidRequest «Unsupported trailer header»).
        foreach (var name in context.TrailerNames)
        {
            if (!SupportedTrailers.Contains(name))
                throw new S3ProtocolException(S3ErrorCode.InvalidRequest, "Unsupported trailer header");
            if (name == "x-amz-checksum-crc32") _crc32 = new();
            if (name == "x-amz-checksum-crc32c") _crc32c = new();
            if (name == "x-amz-checksum-sha1") _sha1 = IncrementalHash.CreateHash(HashAlgorithmName.SHA1);
            if (name == "x-amz-checksum-sha256") _sha256 = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        }
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => _ctx.DecodedContentLength;
    public override long Position { get => _totalDecoded; set => throw new NotSupportedException(); }

    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override int Read(byte[] buffer, int offset, int count)
    {
        if (_finished)
            return 0;
        if (_chunkOffset == _chunkLength)
        {
            ReadNextFrameSync();
            if (_finished)
                return 0;
        }
        return CopyFromChunk(buffer, offset, count);
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        if (_finished)
            return 0;
        if (_chunkOffset == _chunkLength)
        {
            await ReadNextFrameAsync(cancellationToken);
            if (_finished)
                return 0;
        }
        return CopyFromChunk(buffer, offset, count);
    }

    private int CopyFromChunk(byte[] buffer, int offset, int count)
    {
        var toCopy = Math.Min(_chunkLength - _chunkOffset, count);
        Array.Copy(_chunkBuffer, _chunkOffset, buffer, offset, toCopy);
        _chunkOffset += toCopy;
        return toCopy;
    }

    // — загрузка фрейма (общий парсер, синх/асинк источники байтов) —

    private void ReadNextFrameSync()
    {
        ParseFrameHeader(ReadLineSync());
        ReadExactSync(_chunkBuffer, _frameSize);
        ExpectCrlfSync();
        VerifyFrame(_frameSize);
        if (_frameSize == 0)
        {
            if (_ctx.WithTrailers)
                ReadTrailers();
            else
                Finish();
        }
    }

    private async Task ReadNextFrameAsync(CancellationToken ct)
    {
        ParseFrameHeader(await ReadLineAsync(ct));
        await ReadExactAsync(_chunkBuffer, _frameSize, ct);
        await ExpectCrlfAsync(ct);
        VerifyFrame(_frameSize);
        if (_frameSize == 0)
        {
            if (_ctx.WithTrailers)
                await ReadTrailersAsync(ct);
            else
                Finish();
        }
    }

    // Разбор заголовка фрейма: размер + подпись (заполняет _frameSize/_frameSignature).
    private void ParseFrameHeader(string header)
    {
        var separatorIndex = header.IndexOf(";chunk-signature=", StringComparison.Ordinal);
        if (separatorIndex <= 0 || separatorIndex + ";chunk-signature=".Length + 64 != header.Length)
            throw new S3ProtocolException(S3ErrorCode.InvalidRequest, "Malformed chunked encoding");

        var sizeHex = header[..separatorIndex];
        _frameSignature = header[(separatorIndex + ";chunk-signature=".Length)..];

        // Hex-валидация подписи фрейма ДО декодирования/сравнения: ровно 64
        // символа [0-9a-f] (нижний hex референса); не-hex вход в декодер
        // недопустим (иначе FormatException вместо канонического 400).
        if (_frameSignature.Any(c => c is not ((>= '0' and <= '9') or (>= 'a' and <= 'f'))))
            throw new S3ProtocolException(S3ErrorCode.InvalidRequest, "Malformed chunked encoding");

        if (!TryParseHexSize(sizeHex, out _frameSize) || _frameSize > MaxChunkSize)
            throw new S3ProtocolException(S3ErrorCode.InvalidRequest, "Malformed chunked encoding");
    }

    // Сверка подписи чанка цепочкой + обновление контрольных сумм (общее ядро).
    private void VerifyFrame(int size)
    {
        _totalDecoded += size;

        // Данные чанка читаются целиком до сверки подписи (в подписи — sha256 чанка).
        var chunkSha = SigV4Core.HexSha256(size == 0 ? [] : _chunkBuffer[..size]);
        var stringToSign =
            $"{ChunkStringToSignAlgorithm}\n{_ctx.AmzDate}\n{_ctx.Scope}\n{_previousSignature}\n" +
            $"{SigV4Core.EmptySha256}\n{chunkSha}";
        var (scopeDate, scopeRegion) = SplitScope();
        var expected = SigV4Core.SignHex(
            SigV4Core.SigningKey(_ctx.SecretKey, scopeDate, scopeRegion), stringToSign);

        if (!CryptographicOperations.FixedTimeEquals(
                Convert.FromHexString(expected), Convert.FromHexString(_frameSignature)))
            throw new S3ProtocolException(S3ErrorCode.SignatureDoesNotMatch);

        _previousSignature = expected;
        _chunkOffset = 0;
        _chunkLength = size;

        // Контрольные суммы трейлеров — по тем же декодированным данным.
        _crc32?.Append(_chunkBuffer.AsSpan(0, size));
        _crc32c?.Append(_chunkBuffer.AsSpan(0, size));
        _sha1?.AppendData(_chunkBuffer, 0, size);
        _sha256?.AppendData(_chunkBuffer, 0, size);
    }

    private int _frameSize;
    private string _frameSignature = string.Empty;

    // — трейлеры (sync/async дороги; парсинг строки — общий) —

    private void ReadTrailers()
    {
        var trailerString = new StringBuilder();
        while (true)
        {
            if (ParseTrailerLine(ReadLineSync(), trailerString, out _))
                break;
        }
        VerifyTrailers(trailerString.ToString());
    }

    private async Task ReadTrailersAsync(CancellationToken ct)
    {
        var trailerString = new StringBuilder();
        while (true)
        {
            if (ParseTrailerLine(await ReadLineAsync(ct), trailerString, out _))
                break;
        }
        VerifyTrailers(trailerString.ToString());
    }

    // Одна строка трейлеров; true — строка была x-amz-trailer-signature (конец).
    private bool ParseTrailerLine(string line, StringBuilder trailerString, out string? signature)
    {
        if (line.StartsWith(TrailerSignatureHeader, StringComparison.Ordinal))
        {
            signature = line[TrailerSignatureHeader.Length..].Trim();
            _pendingTrailerSignature = signature;
            return true;
        }

        var colon = line.IndexOf(':', StringComparison.Ordinal);
        if (colon <= 0)
            throw new S3ProtocolException(S3ErrorCode.InvalidRequest, "Malformed chunked encoding");
        var name = line[..colon].Trim().ToLowerInvariant();
        if (!_ctx.TrailerNames.Contains(name, StringComparer.Ordinal))
            throw new S3ProtocolException(S3ErrorCode.InvalidRequest, "Malformed chunked encoding");
        _receivedTrailers[name] = line[(colon + 1)..].Trim();

        trailerString.Append(name).Append(':').Append(_receivedTrailers[name]).Append('\n');
        signature = null;
        return false;
    }

    private string? _pendingTrailerSignature;

    private void VerifyTrailers(string trailerString)
    {
        var (scopeDate, scopeRegion) = SplitScope();
        var stringToSign =
            $"{TrailerStringToSignAlgorithm}\n{_ctx.AmzDate}\n{_ctx.Scope}\n{_previousSignature}\n" +
            $"{SigV4Core.HexSha256(Encoding.UTF8.GetBytes(trailerString))}";
        var expected = SigV4Core.SignHex(
            SigV4Core.SigningKey(_ctx.SecretKey, scopeDate, scopeRegion), stringToSign);
        byte[] expectedBytes, signatureBytes;
        try
        {
            expectedBytes = Convert.FromHexString(expected);
            signatureBytes = Convert.FromHexString(_pendingTrailerSignature ?? string.Empty);
        }
        catch (FormatException)
        {
            throw new S3ProtocolException(S3ErrorCode.InvalidRequest, "Malformed chunked encoding");
        }
        if (!CryptographicOperations.FixedTimeEquals(expectedBytes, signatureBytes))
            throw new S3ProtocolException(S3ErrorCode.SignatureDoesNotMatch);

        Finish();
    }

    // Финальные сверки: контрольные суммы трейлеров + декодированная длина.
    private void Finish()
    {
        foreach (var name in _ctx.TrailerNames)
        {
            if (!_receivedTrailers.TryGetValue(name, out var value))
                throw new S3ProtocolException(S3ErrorCode.InvalidRequest, "Malformed chunked encoding");

            var actual = name switch
            {
                // .NET Crc32 отдаёт байты little-endian — канонический AWS-формат big-endian.
                "x-amz-checksum-crc32" => Convert.ToHexString(Reverse(_crc32!.GetCurrentHash())),
                "x-amz-checksum-crc32c" => Convert.ToHexString(_crc32c!.GetCurrentHash()),
                "x-amz-checksum-sha1" => Convert.ToHexString(_sha1!.GetHashAndReset()),
                "x-amz-checksum-sha256" => Convert.ToHexString(_sha256!.GetHashAndReset()),
                _ => throw new S3ProtocolException(S3ErrorCode.InvalidRequest, "Unsupported trailer header"),
            };
            var expectedValue = TryBase64ToHex(value)
                ?? throw new S3ProtocolException(S3ErrorCode.InvalidRequest, "Malformed chunked encoding");
            if (!string.Equals(actual, expectedValue, StringComparison.OrdinalIgnoreCase))
                throw new S3ProtocolException(S3ErrorCode.BadDigest, "The checksum you specified did not match the calculated checksum.");
        }

        if (_totalDecoded != _ctx.DecodedContentLength)
            throw new S3ProtocolException(S3ErrorCode.InvalidRequest,
                "The x-amz-decoded-content-length does not match the decoded content length.");

        _finished = true;
    }

    private (string Date, string Region) SplitScope()
    {
        var parts = _ctx.Scope.Split('/');
        return (parts[0], parts[1]);
    }

    private static byte[] Reverse(byte[] bytes)
    {
        Array.Reverse(bytes);
        return bytes;
    }

    private static bool TryParseHexSize(string hex, out int size) =>
        Utf8Parser.TryParse(Encoding.ASCII.GetBytes(hex), out size, out _, 'x')
        && hex.Length > 0 && hex.All(c => c is (>= '0' and <= '9') or (>= 'a' and <= 'f') or (>= 'A' and <= 'F'));

    private static string? TryBase64ToHex(string base64)
    {
        try
        {
            return Convert.ToHexString(Convert.FromBase64String(base64));
        }
        catch (FormatException)
        {
            return null;
        }
    }

    // — источники байтов: синхронная и асинхронная дороги через общий буфер —

    private void FillSync()
    {
        _ioStart = 0;
        _ioEnd = _inner.Read(_io, 0, _io.Length);
    }

    private async Task FillAsync(CancellationToken ct)
    {
        _ioStart = 0;
        _ioEnd = await _inner.ReadAsync(_io.AsMemory(0, _io.Length), ct);
    }

    private void Compact()
    {
        if (_ioStart == 0)
            return;
        Array.Copy(_io, _ioStart, _io, 0, _ioEnd - _ioStart);
        _ioEnd -= _ioStart;
        _ioStart = 0;
    }

    private int LineFromBuffer()
    {
        // Возвращает длину строки (без \n) или -1, если полной строки нет в буфере.
        for (var i = _ioStart; i < _ioEnd; i++)
        {
            if (_io[i] == '\n')
                return i - _ioStart;
        }
        return -1;
    }

    private string DecodeLine(int length)
    {
        var span = _io.AsSpan(_ioStart, length);
        if (length > 0 && span[^1] == '\r')
            span = span[..^1];
        _ioStart += length + 1;   // терминатор съедается
        return Encoding.ASCII.GetString(span);
    }

    private string ReadLineSync()
    {
        while (true)
        {
            var length = LineFromBuffer();
            if (length >= 0)
                return DecodeLine(length);
            Compact();
            if (_ioEnd == _io.Length)
                throw new S3ProtocolException(S3ErrorCode.InvalidRequest, "Malformed chunked encoding");
            FillSync();
            if (_ioEnd == 0)
                throw new S3ProtocolException(S3ErrorCode.InvalidRequest, "Malformed chunked encoding");
        }
    }

    private async Task<string> ReadLineAsync(CancellationToken ct)
    {
        while (true)
        {
            var length = LineFromBuffer();
            if (length >= 0)
                return DecodeLine(length);
            Compact();
            if (_ioEnd == _io.Length)
                throw new S3ProtocolException(S3ErrorCode.InvalidRequest, "Malformed chunked encoding");
            await FillAsync(ct);
            if (_ioEnd == 0)
                throw new S3ProtocolException(S3ErrorCode.InvalidRequest, "Malformed chunked encoding");
        }
    }

    private void ReadExactSync(byte[] target, int count)
    {
        var offset = 0;
        while (offset < count)
        {
            if (_ioStart == _ioEnd)
            {
                FillSync();
                if (_ioEnd == 0)
                    throw new S3ProtocolException(S3ErrorCode.InvalidRequest, "Malformed chunked encoding");
            }
            var toCopy = Math.Min(count - offset, _ioEnd - _ioStart);
            Array.Copy(_io, _ioStart, target, offset, toCopy);
            _ioStart += toCopy;
            offset += toCopy;
        }
    }

    private async Task ReadExactAsync(byte[] target, int count, CancellationToken ct)
    {
        var offset = 0;
        while (offset < count)
        {
            if (_ioStart == _ioEnd)
            {
                await FillAsync(ct);
                if (_ioEnd == 0)
                    throw new S3ProtocolException(S3ErrorCode.InvalidRequest, "Malformed chunked encoding");
            }
            var toCopy = Math.Min(count - offset, _ioEnd - _ioStart);
            Array.Copy(_io, _ioStart, target, offset, toCopy);
            _ioStart += toCopy;
            offset += toCopy;
        }
    }

    private void ExpectCrlfSync()
    {
        ReadExactSync(_lineBuffer, 2);
        ExpectCrlfBytes();
    }

    private async Task ExpectCrlfAsync(CancellationToken ct)
    {
        await ReadExactAsync(_lineBuffer, 2, ct);
        ExpectCrlfBytes();
    }

    private void ExpectCrlfBytes()
    {
        if (_lineBuffer[0] != '\r' || _lineBuffer[1] != '\n')
            throw new S3ProtocolException(S3ErrorCode.InvalidRequest, "Malformed chunked encoding");
    }
}
