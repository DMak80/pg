using System.Net;
using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;

namespace OwnS3.IntegrationTests.E2e;

// Класс 2 (spec §4.5): multipart реальным SDK — полный цикл со сборкой
// составного ETag, Range через стыки частей, Abort-семантика, перезагрузка
// части (last-writer-wins), DeleteBucket-гейт живой загрузки.
[Collection(OwnS3E2eCollection.Name)]
public sealed class AwsSdkMultipartScenarios
{
    private const int MiB = 1024 * 1024;

    [Fact]
    public async Task Multipart_FullCycle()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: Initiate → UploadPart×3 (2×5 МиБ + 1 МиБ хвост) → ListParts →
        // Complete (составной ETag «3-md5(concat)» по формуле канона 02 §1) →
        // GET = конкатенация частей; HEAD Size; ObjectParts — реальные 3 части
        await OwnS3E2eEnvironment.RunScenarioAsync("m1", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}mp";
            var key = "multipart.bin";
            var part1 = OwnS3E2eBytes.Deterministic(5L * MiB, 101);
            var part2 = OwnS3E2eBytes.Deterministic(5L * MiB, 102);
            var part3 = OwnS3E2eBytes.Deterministic(1L * MiB, 103);
            byte[] body = [.. part1, .. part2, .. part3];
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);

            // Act: полный цикл
            var uploadId = (await s3.InitiateMultipartUploadAsync(
                new InitiateMultipartUploadRequest { BucketName = bucket, Key = key }, ct)).UploadId;
            var partEtags = new List<string>();
            foreach (var (i, part) in new[] { (1, part1), (2, part2), (3, part3) })
            {
                partEtags.Add((await s3.UploadPartAsync(new UploadPartRequest
                {
                    BucketName = bucket, Key = key, UploadId = uploadId, PartNumber = i,
                    InputStream = new MemoryStream(part), PartSize = part.Length,
                }, ct)).ETag!.Trim('"'));
            }

            // Assert: ListParts до Complete — 3 записи с номерами/размерами
            var parts = await s3.ListPartsAsync(new ListPartsRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId,
            }, ct);
            parts.Parts.Should().HaveCount(3);
            parts.Parts.Select(p => p.PartNumber).Should().ContainInOrder(1, 2, 3);
            parts.Parts.Select(p => p.Size).Should()
                .ContainInOrder(part1.Length, part2.Length, part3.Length);

            // Act: Complete → составной ETag
            var completed = await s3.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId,
                PartETags = partEtags.Select((e, idx) => new PartETag(idx + 1, e)).ToList(),
            }, ct);
            completed.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Multipart(3, partEtags),
                "составной ETag = «3-md5(concat бинарных ETag'ов частей)» (канон 02 §1)");

            // Assert: GET = конкатенация; HEAD Size; ObjectParts = 3
            using var get = await s3.GetObjectAsync(
                new GetObjectRequest { BucketName = bucket, Key = key }, ct);
            using var ms = new MemoryStream();
            await get.ResponseStream.CopyToAsync(ms, ct);
            ms.ToArray().Should().Equal(body);
            (await s3.GetObjectMetadataAsync(
                new GetObjectMetadataRequest { BucketName = bucket, Key = key }, ct))
                .ContentLength.Should().Be(body.Length);
            var attrs = await s3.GetObjectAttributesAsync(new GetObjectAttributesRequest
            {
                BucketName = bucket, Key = key,
                ObjectAttributes = new List<ObjectAttributes> { ObjectAttributes.ObjectParts },
            }, ct);
            attrs.ObjectParts.Should().NotBeNull();
            attrs.ObjectParts!.Parts.Should().HaveCount(3, "атрибуты несут реальные части");
        }, ct);
    }

    [Fact]
    public async Task Multipart_Range_Over_Part_Boundaries()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: bytes через стык частей (граница 5 МиБ) → 206, точный срез по
        // границам частей (механика t38 — реальным клиентом)
        await OwnS3E2eEnvironment.RunScenarioAsync("m2", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}mr";
            var key = "mp-range.bin";
            var part1 = OwnS3E2eBytes.Deterministic(5L * MiB, 201);
            var part2 = OwnS3E2eBytes.Deterministic(5L * MiB, 202);
            byte[] body = [.. part1, .. part2];
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            var uploadId = (await s3.InitiateMultipartUploadAsync(
                new InitiateMultipartUploadRequest { BucketName = bucket, Key = key }, ct)).UploadId;
            var etags = new List<string>();
            foreach (var (i, part) in new[] { (1, part1), (2, part2) })
            {
                etags.Add((await s3.UploadPartAsync(new UploadPartRequest
                {
                    BucketName = bucket, Key = key, UploadId = uploadId, PartNumber = i,
                    InputStream = new MemoryStream(part), PartSize = part.Length,
                }, ct)).ETag!.Trim('"'));
            }
            await s3.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId,
                PartETags = etags.Select((e, idx) => new PartETag(idx + 1, e)).ToList(),
            }, ct);

            // Act: диапазон через стык частей 1|2
            using var get = await s3.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket, Key = key, ByteRange = new ByteRange("bytes=5242860-5242930"),
            }, ct);

            // Assert
            get.HttpStatusCode.Should().Be(HttpStatusCode.PartialContent);
            using var ms = new MemoryStream();
            await get.ResponseStream.CopyToAsync(ms, ct);
            ms.ToArray().Should().Equal(body[5242860..5242931]);
        }, ct);
    }

    [Fact]
    public async Task Multipart_Abort_Semantics()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: Abort → 204; повторный Abort → NoSuchUpload 404; Complete
        // после Abort → NoSuchUpload; ListParts после Abort → NoSuchUpload
        await OwnS3E2eEnvironment.RunScenarioAsync("m3", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}ma";
            var key = "mp-abort.bin";
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            var uploadId = (await s3.InitiateMultipartUploadAsync(
                new InitiateMultipartUploadRequest { BucketName = bucket, Key = key }, ct)).UploadId;
            var etag = (await s3.UploadPartAsync(new UploadPartRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId, PartNumber = 1,
                InputStream = new MemoryStream(OwnS3E2eBytes.Deterministic(1 * MiB, 301)),
            }, ct)).ETag!;

            // Act / Assert: Abort → 204; повторный → NoSuchUpload 404
            (await s3.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId,
            }, ct)).HttpStatusCode.Should().Be(HttpStatusCode.NoContent);
            (await s3.Invoking(x => x.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId,
            }, ct)).Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.ErrorCode == "NoSuchUpload" && e.StatusCode == HttpStatusCode.NotFound);

            // Act / Assert: Complete после Abort → NoSuchUpload
            (await s3.Invoking(x => x.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId,
                PartETags = [new(1, etag.Trim('"'))],
            }, ct)).Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.ErrorCode == "NoSuchUpload");

            // Act / Assert: ListParts после Abort → NoSuchUpload
            (await s3.Invoking(x => x.ListPartsAsync(new ListPartsRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId,
            }, ct)).Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.ErrorCode == "NoSuchUpload");
        }, ct);
    }

    [Fact]
    public async Task Multipart_PartReupload_LastWriterWins()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: повторная загрузка того же partNumber другим телом (5 МиБ —
        // лимит не-последней части) → Complete собирает ПОСЛЕДНЮЮ версию
        await OwnS3E2eEnvironment.RunScenarioAsync("m4", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}mw";
            var key = "mp-reupload.bin";
            var stale = OwnS3E2eBytes.Deterministic(5L * MiB, 401);
            var fresh = OwnS3E2eBytes.Deterministic(5L * MiB, 402);
            var tail = OwnS3E2eBytes.Deterministic(1 * MiB, 403);
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            var uploadId = (await s3.InitiateMultipartUploadAsync(
                new InitiateMultipartUploadRequest { BucketName = bucket, Key = key }, ct)).UploadId;
            async Task<string> UploadAsync(int number, byte[] part) => (await s3.UploadPartAsync(
                new UploadPartRequest
                {
                    BucketName = bucket, Key = key, UploadId = uploadId, PartNumber = number,
                    InputStream = new MemoryStream(part), PartSize = part.Length,
                }, ct)).ETag!.Trim('"');

            // Act: part1 дважды (stale → fresh), part2 — хвост
            _ = await UploadAsync(1, stale);
            var etag1 = await UploadAsync(1, fresh);
            var etag2 = await UploadAsync(2, tail);
            var completed = await s3.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId,
                PartETags = [new(1, etag1), new(2, etag2)],
            }, ct);

            // Assert: объект = fresh + tail; составной ETag по последним частям
            using var get = await s3.GetObjectAsync(
                new GetObjectRequest { BucketName = bucket, Key = key }, ct);
            using var ms = new MemoryStream();
            await get.ResponseStream.CopyToAsync(ms, ct);
            ms.ToArray().Should().Equal([.. fresh, .. tail], "побеждает последняя версия части");
            completed.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Multipart(2, [etag1, etag2]));
        }, ct);
    }

    [Fact]
    public async Task DeleteBucket_BlockedByLiveUpload()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: гейт t38 — DeleteBucket при живой multipart-загрузке → 409
        // BucketNotEmpty; после Abort → 204
        await OwnS3E2eEnvironment.RunScenarioAsync("m5", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}mg";
            var key = "mp-live.bin";
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            var uploadId = (await s3.InitiateMultipartUploadAsync(
                new InitiateMultipartUploadRequest { BucketName = bucket, Key = key }, ct)).UploadId;
            await s3.UploadPartAsync(new UploadPartRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId, PartNumber = 1,
                InputStream = new MemoryStream(OwnS3E2eBytes.Deterministic(1 * MiB, 501)),
            }, ct);

            // Act / Assert: живая загрузка блокирует удаление бакета
            (await admin.Invoking(x => x.DeleteBucketAsync(
                    new DeleteBucketRequest { BucketName = bucket }, ct))
                .Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.ErrorCode == "BucketNotEmpty" && e.StatusCode == HttpStatusCode.Conflict);

            // Act / Assert: Abort → бакет пуст → удаление 204
            await s3.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
            {
                BucketName = bucket, Key = key, UploadId = uploadId,
            }, ct);
            (await admin.DeleteBucketAsync(new DeleteBucketRequest { BucketName = bucket }, ct))
                .HttpStatusCode.Should().Be(HttpStatusCode.NoContent);
        }, ct);
    }
}
