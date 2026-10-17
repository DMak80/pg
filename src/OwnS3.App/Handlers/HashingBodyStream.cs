using System.IO.Hashing;
using System.Security.Cryptography;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.Errors;

namespace OwnS3.App.Handlers;

// Обёртка тела запроса (spec §3.4 шаг 6): сквозное чтение с одновременным
// подсчётом SHA-256 (hex-режим x-amz-content-sha256) и MD5 (при Content-MD5);
// по завершении чтения (EOF) — сверка: sha256 ≠ заявленному → InvalidRequest,
// MD5 ≠ заголовку → BadDigest. Сверка происходит ДО передачи данных дальше
// (drain заглушки дочитывает тело целиком — исход наблюдаем в t36).
public sealed class HashingBodyStream : Stream
{
    private readonly Stream _inner;
    private readonly string? _expectedSha256Hex;
    private readonly IncrementalHash _sha256 = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
    private readonly IncrementalHash? _md5;
    private readonly string? _expectedMd5Base64;
    private readonly ChecksumExpectation? _checksum;
    private readonly Crc32? _checksumCrc32;          // алгоритм Crc32 (System.IO.Hashing)
    private readonly Crc32C? _checksumCrc32c;        // алгоритм Crc32C (OwnS3.Protocol.Auth)
    private readonly IncrementalHash? _checksumSha1; // алгоритм Sha1 (Sha256 — существующий _sha256)
    private bool _checked;

    public HashingBodyStream(Stream inner, string? expectedSha256Hex, string? expectedMd5Base64,
        ChecksumExpectation? checksum = null)
    {
        _inner = inner;
        _expectedSha256Hex = expectedSha256Hex;
        _expectedMd5Base64 = expectedMd5Base64;
        _md5 = expectedMd5Base64 is null ? null : IncrementalHash.CreateHash(HashAlgorithmName.MD5);
        // Ровно один инкрементальный хэш поверх прохода чтения; Sha256 — переиспользование
        // уже существующего _sha256 (второй инстанс не создаётся).
        _checksum = checksum;
        if (checksum is not null)
            switch (checksum.Algorithm)
            {
                case ChecksumAlgorithm.Crc32: _checksumCrc32 = new(); break;
                case ChecksumAlgorithm.Crc32C: _checksumCrc32c = new(); break;
                case ChecksumAlgorithm.Sha1: _checksumSha1 = IncrementalHash.CreateHash(HashAlgorithmName.SHA1); break;
            }
    }

    public override bool CanRead => _inner.CanRead;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => _inner.Length;
    public override long Position { get => _inner.Position; set => throw new NotSupportedException(); }

    public override int Read(byte[] buffer, int offset, int count)
    {
        var read = _inner.Read(buffer, offset, count);
        if (read > 0)
        {
            _sha256.AppendData(buffer, offset, read);
            _md5?.AppendData(buffer, offset, read);
            _checksumCrc32?.Append(buffer.AsSpan(offset, read));
            _checksumCrc32c?.Append(buffer.AsSpan(offset, read));
            _checksumSha1?.AppendData(buffer, offset, read);
        }
        else
        {
            VerifyHashes();
        }
        return read;
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        var read = await _inner.ReadAsync(buffer, offset, count, cancellationToken);
        if (read > 0)
        {
            _sha256.AppendData(buffer, offset, read);
            _md5?.AppendData(buffer, offset, read);
            _checksumCrc32?.Append(buffer.AsSpan(offset, read));
            _checksumCrc32c?.Append(buffer.AsSpan(offset, read));
            _checksumSha1?.AppendData(buffer, offset, read);
        }
        else
        {
            VerifyHashes();
        }
        return read;
    }

    public override void Flush() => _inner.Flush();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_checked && _inner.CanRead)
        {
            // Dispose без дочитывания — сверка не выполняется (частичное чтение).
        }
        _checksumSha1?.Dispose();
        base.Dispose(disposing);
    }

    private void VerifyHashes()
    {
        if (_checked)
            return;
        _checked = true;

        if (_expectedSha256Hex is not null)
        {
            var actual = Convert.ToHexString(_sha256.GetCurrentHash()).ToLowerInvariant();
            if (!string.Equals(actual, _expectedSha256Hex, StringComparison.Ordinal))
                throw new S3ProtocolException(S3ErrorCode.InvalidRequest,
                    "The provided x-amz-content-sha256 does not match what was computed.");
        }

        if (_checksum is not null)
        {
            var actualChecksum = _checksum.Algorithm switch
            {
                ChecksumAlgorithm.Sha256 => Convert.ToBase64String(_sha256.GetCurrentHash()),
                ChecksumAlgorithm.Crc32 => Convert.ToBase64String(
                    ChecksumConventions.Crc32BigEndian(_checksumCrc32!)),
                ChecksumAlgorithm.Crc32C => Convert.ToBase64String(_checksumCrc32c!.GetCurrentHash()),
                _ => Convert.ToBase64String(_checksumSha1!.GetCurrentHash()),
            };
            if (!string.Equals(actualChecksum, _checksum.ExpectedBase64, StringComparison.Ordinal))
                throw new S3ProtocolException(S3ErrorCode.BadDigest,
                    "The checksum you specified did not match the calculated checksum.");
        }

        if (_md5 is not null && _expectedMd5Base64 is not null)
        {
            var actual = Convert.ToBase64String(_md5.GetCurrentHash());
            if (!string.Equals(actual, _expectedMd5Base64, StringComparison.Ordinal))
                throw new S3ProtocolException(S3ErrorCode.BadDigest,
                    "The Content-Md5 you specified did not match what we received.");
        }
    }
}
