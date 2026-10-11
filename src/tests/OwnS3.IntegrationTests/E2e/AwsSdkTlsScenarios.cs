using Amazon.S3;
using Amazon.S3.Model;
using OwnS3.IntegrationTests.E2e;
using Xunit;

namespace OwnS3.IntegrationTests.E2e;

// SDK TLS-smoke (t42, AC2): AWS SDK .NET против HTTPS-only грани, доверие CA —
// фабрика HttpClient SDK-клиента с валидацией корня цепочки по отпечатку CA
// (механизм SDK воркера бэкапов; env AWS_CA_BUNDLE линейкой AWSSDK.Core 3.7.x
// не читается). Роли по матрице arch/owns3/05 §3: бакетные операции — root,
// объектные/multipart — writer.
[Collection(OwnS3E2eCollection.Name)]
public class AwsSdkTlsScenarios
{
    // AAA: полный smoke по https — put/get/head/list/multipart + /healthz;
    // доверие per-install CA — фабрика HttpClient SDK-клиента (механизм
    // BackupS3, spec §5 fallback: AWSSDK.Core env AWS_CA_BUNDLE не читает —
    // вскрыто этим E2E). Бакет создаёт/удаляет root (матрица 05 §3),
    // объекты/multipart — writer.
    [Fact]
    public async Task Sdk_CaFactory_ПолныйSmoke()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await OwnS3TlsE2eEnvironment.RunScenarioAsync("t1", async fx =>
        {
            using var root = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}tls";
            await root.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            var body = OwnS3E2eBytes.Deterministic(256 * 1024, 42);
            await s3.PutObjectAsync(new PutObjectRequest
            { BucketName = bucket, Key = "smoke.bin", InputStream = new MemoryStream(body) }, ct);

            // GET: чтение тела — MemoryStream + CopyToAsync (эталон AwsSdkSimpleScenarios)
            using var get = await s3.GetObjectAsync(
                new GetObjectRequest { BucketName = bucket, Key = "smoke.bin" }, ct);
            using var ms = new MemoryStream();
            await get.ResponseStream.CopyToAsync(ms, ct);
            ms.ToArray().Should().Equal(body, "GET возвращает байты PUT без искажений");

            // HEAD — GetObjectMetadataRequest с BucketName+Key (эталон)
            (await s3.GetObjectMetadataAsync(
                new GetObjectMetadataRequest { BucketName = bucket, Key = "smoke.bin" }, ct))
                .ContentLength.Should().Be(256 * 1024);
            (await s3.ListObjectsV2Async(new ListObjectsV2Request { BucketName = bucket }, ct))
                .S3Objects.Should().Contain(o => o.Key == "smoke.bin");

            // multipart (2 части × 5 МиБ) — writer: read-write имеет право (05 §3)
            var upload = await s3.InitiateMultipartUploadAsync(
                new InitiateMultipartUploadRequest { BucketName = bucket, Key = "mp.bin" }, ct);
            var parts = new List<PartETag>();
            for (var i = 1; i <= 2; i++)
            {
                var resp = await s3.UploadPartAsync(new UploadPartRequest
                {
                    BucketName = bucket, Key = "mp.bin", UploadId = upload.UploadId, PartNumber = i,
                    InputStream = new MemoryStream(OwnS3E2eBytes.Deterministic(5 * 1024 * 1024, i)),
                }, ct);
                parts.Add(new PartETag { PartNumber = i, ETag = resp.ETag });
            }
            await s3.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            { BucketName = bucket, Key = "mp.bin", UploadId = upload.UploadId, PartETags = parts }, ct);

            // Бакет пустеет удалением обоих объектов перед DeleteBucket
            // (незачищенный mp.bin давал бы 409 BucketNotEmpty).
            await s3.DeleteObjectAsync(new DeleteObjectRequest { BucketName = bucket, Key = "smoke.bin" }, ct);
            await s3.DeleteObjectAsync(new DeleteObjectRequest { BucketName = bucket, Key = "mp.bin" }, ct);
            await root.DeleteBucketAsync(new DeleteBucketRequest { BucketName = bucket }, ct);
            // /healthz по https доверенным HttpClient (хелпер окружения шага 5.1)
            (await fx.HealthzAsync(ct)).Should().BeTrue();
        }, ct);
    }
}
