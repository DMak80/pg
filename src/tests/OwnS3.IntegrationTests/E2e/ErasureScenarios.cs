using System.Net;
using System.Text.Json;
using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;
using OwnS3.IntegrationTests.Api;
using OwnS3.Protocol.Auth;

namespace OwnS3.IntegrationTests.E2e;

// docker-E2E erasure-серия (t40, спека §6.3): EC-контур 4×3+1 против реальных
// клиентов — базовый цикл AWS SDK, деградированный рестарт (detached-том),
// потеря кворума (503), возврат тома + heal, замена диска пустым томом,
// админ-грань. Каждый кейс — своё окружение RunScenarioAsync-обёрткой
// (docs/e2e-isolation.md); телеметрия/зачистка — OwnS3EcE2eEnvironment.
public sealed class ErasureScenarios
{
    // Тело на несколько erasure-блоков (2.5 МиБ) + мелочь
    private static byte[] Body(int seed) => OwnS3E2eBytes.Deterministic(5 * 512 * 1024 + 123, seed);

    [Fact]
    public async Task Erasure_BasicCycle()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: полный цикл AWS SDK против EC-контура 4×3+1: PUT/GET/HEAD,
        // conditional, Range, листинги, multipart (составной ETag), Copy —
        // семантики идентичны single (контракт 21 операции не зависит от режима)
        await OwnS3EcE2eEnvironment.RunScenarioAsync("ec1", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Admin);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3ec{fx.RunTag}bc";
            var key = "ec/basic/объект.dat";
            var body = Body(1);
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);

            // Act: PUT + GET round-trip
            var putResp = await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket, Key = key, InputStream = new MemoryStream(body),
                ContentType = "application/x-os3-ec",
            }, ct);
            var getResp = await s3.GetObjectAsync(new GetObjectRequest { BucketName = bucket, Key = key }, ct);
            using var getBody = new MemoryStream();
            await getResp.ResponseStream.CopyToAsync(getBody, ct);

            // Assert: байты/ETag-MD5/ContentType (семантики single)
            putResp.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Md5Hex(body));
            getResp.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Md5Hex(body));
            getBody.ToArray().Should().Equal(body);
            getResp.Headers["Content-Type"].Should().Be("application/x-os3-ec");

            // Act: HEAD + If-Match верный → 200; If-None-Match совпал → 304
            var head = await s3.GetObjectMetadataAsync(
                new GetObjectMetadataRequest { BucketName = bucket, Key = key }, ct);
            var ifMatch = await s3.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket, Key = key, EtagToMatch = OwnS3E2eETag.Quote(OwnS3E2eETag.Md5Hex(body)),
            }, ct);

            // Assert: HEAD размер/ETag; If-Match — 200 с телом; If-None-Match — 304
            head.ContentLength.Should().Be(body.Length);
            head.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Md5Hex(body));
            ifMatch.HttpStatusCode.Should().Be(HttpStatusCode.OK);
            (await s3.Invoking(x => x.GetObjectAsync(new GetObjectRequest
                 {
                     BucketName = bucket, Key = key, EtagToNotMatch = OwnS3E2eETag.Quote(OwnS3E2eETag.Md5Hex(body)),
                 }, ct))
                .Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.StatusCode == HttpStatusCode.NotModified);

            // Act: Range через границу первого erasure-блока (1 МиБ)
            var range = await s3.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket, Key = key,
                ByteRange = new ByteRange(1024 * 1024 - 2, 1024 * 1024 + 1),
            }, ct);
            using var rangeBody = new MemoryStream();
            await range.ResponseStream.CopyToAsync(rangeBody, ct);

            // Assert: срез через блок — реконструкция не нужна (все диски живы)
            range.HttpStatusCode.Should().Be(HttpStatusCode.PartialContent);
            rangeBody.ToArray().Should().Equal(body[(1024 * 1024 - 2)..(1024 * 1024 + 2)]);

            // Act: листинги видят ключ
            var listing = await s3.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = bucket, Prefix = "ec/",
            }, ct);

            // Assert
            listing.S3Objects.Should().ContainSingle(o => o.Key == key)
                .Which.Size.Should().Be(body.Length);

            // Act: multipart 2×5 МиБ + Complete → составной ETag; GET равен
            var mpuKey = "ec/multipart.bin";
            var part1 = OwnS3E2eBytes.Deterministic(5 * 1024 * 1024, 2);
            var part2 = OwnS3E2eBytes.Deterministic(1024, 3);
            var create = await s3.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
            {
                BucketName = bucket, Key = mpuKey, ContentType = "application/x-os3-mp",
            }, ct);
            var e1 = (await s3.UploadPartAsync(new UploadPartRequest
            {
                BucketName = bucket, Key = mpuKey, UploadId = create.UploadId, PartNumber = 1,
                InputStream = new MemoryStream(part1),
            }, ct)).ETag;
            var e2 = (await s3.UploadPartAsync(new UploadPartRequest
            {
                BucketName = bucket, Key = mpuKey, UploadId = create.UploadId, PartNumber = 2,
                InputStream = new MemoryStream(part2),
            }, ct)).ETag;
            var complete = await s3.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = bucket, Key = mpuKey, UploadId = create.UploadId,
                PartETags =
                [
                    new PartETag(1, e1!), new PartETag(2, e2!),
                ],
            }, ct);
            var mpuGet = await s3.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket, Key = mpuKey,
            }, ct);
            using var mpuBody = new MemoryStream();
            await mpuGet.ResponseStream.CopyToAsync(mpuBody, ct);

            // Assert: составной ETag канона 02 §1; тело = конкатенация частей
            complete.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Multipart(2, [e1!, e2!]));
            mpuBody.ToArray().Should().Equal([.. part1, .. part2]);
            mpuGet.ETag!.Trim('"').Should().Be(complete.ETag!.Trim('"'));

            // Act: Copy наследует ETag источника
            var copy = await s3.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucket = bucket, SourceKey = key,
                DestinationBucket = bucket, DestinationKey = "ec/copy.dat",
            }, ct);

            // Assert
            copy.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Md5Hex(body));
        }, ct);
    }

    [Fact]
    public async Task Erasure_DegradedRestart()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: записи при 4 томах → рестарт с томами 1–3 (4-й detached — путь
        // отсутствует, диск offline) → GET равен исходнику (реконструкция),
        // PUT нового объекта жив; метрики отражают деградацию
        await OwnS3EcE2eEnvironment.RunScenarioAsync("ec2", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Admin);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3ec{fx.RunTag}dr";
            var body = Body(4);
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket, Key = "before.bin", InputStream = new MemoryStream(body),
            }, ct);

            // Act: рестарт с 3 томами (позиция /data4 не смонтирована — offline)
            await fx.RestartWithVolumesAsync(1, 2, 3);

            // Assert: GET реконструирует (3 онлайн ≥ k=3)
            var get = await s3.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket, Key = "before.bin",
            }, ct);
            using var got = new MemoryStream();
            await get.ResponseStream.CopyToAsync(got, ct);
            got.ToArray().Should().Equal(body);

            // Act: запись при деградации (writeQuorum=3)
            var after = Body(5);
            await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket, Key = "after.bin", InputStream = new MemoryStream(after),
            }, ct);
            var degradedGet = await s3.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket, Key = "after.bin",
            }, ct);
            using var degradedBody = new MemoryStream();
            await degradedGet.ResponseStream.CopyToAsync(degradedBody, ct);
            degradedBody.ToArray().Should().Equal(after);

            // Assert: метрики деградации — drive="3" (позиция /data4) offline
            using var http = new HttpClient();
            var metrics = await ScrapeUntilAsync(http, fx, "ownS3_ec_drive_online");
            metrics.Should().Contain("""ownS3_ec_drive_online{otel_scope_name="ownS3",drive="3",set="0"} 0""");
            metrics.Should().Contain("""ownS3_ec_drive_online{otel_scope_name="ownS3",drive="0",set="0"} 1""");
        }, ct);
    }

    [Fact]
    public async Task Erasure_QuorumUnavailable()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: записи при 4 томах → рестарт с томами 1–2 (онлайн 2 < k=3) →
        // GET/PUT/ListObjects → 503 InsufficientReadQuorum (кворумная
        // недоступность, не 404)
        await OwnS3EcE2eEnvironment.RunScenarioAsync("ec3", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Admin);
            var bucket = $"os3ec{fx.RunTag}qu";
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);

            // Act: рестарт с 2 томами
            await fx.RestartWithVolumesAsync(1, 2);

            // Assert: GET несуществующего ключа → 503 (не 404); без SDK-ретраев
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer, maxErrorRetry: 0);
            (await s3.Invoking(x => x.GetObjectAsync(new GetObjectRequest
                 {
                     BucketName = bucket, Key = "any.bin",
                 }, ct))
                .Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.StatusCode == HttpStatusCode.ServiceUnavailable
                            && e.ErrorCode == "InsufficientReadQuorum");

            // Assert: PUT → 503 (бакетная граница — read-кворум существования)
            (await s3.Invoking(x => x.PutObjectAsync(new PutObjectRequest
                 {
                     BucketName = bucket, Key = "any.bin", InputStream = new MemoryStream("x"u8.ToArray()),
                 }, ct))
                .Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.StatusCode == HttpStatusCode.ServiceUnavailable);

            // Assert: ListObjects → 503
            (await s3.Invoking(x => x.ListObjectsV2Async(new ListObjectsV2Request
                 {
                     BucketName = bucket,
                 }, ct))
                .Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.StatusCode == HttpStatusCode.ServiceUnavailable
                            && e.ErrorCode == "InsufficientReadQuorum");
        }, ct);
    }

    [Fact]
    public async Task Erasure_VolumeReturn()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: записи → деградированный рестарт (3 тома) → запись ещё одного
        // объекта без 4-го → возврат всех 4 томов (данные на месте) →
        // POST /admin/heal → /admin/storage полнота; ВСЕ данные читаются
        await OwnS3EcE2eEnvironment.RunScenarioAsync("ec4", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Admin);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3ec{fx.RunTag}vr";
            var bodyBefore = Body(6);
            var bodyDuring = Body(7);
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket, Key = "before.bin", InputStream = new MemoryStream(bodyBefore),
            }, ct);

            // Act: деградация → запись при деградации → возврат тома 4
            await fx.RestartWithVolumesAsync(1, 2, 3);
            await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket, Key = "during.bin", InputStream = new MemoryStream(bodyDuring),
            }, ct);
            await fx.RestartWithVolumesAsync(1, 2, 3, 4);

            // Act: heal + ожидание полноты
            var heal = await AdminRequestAsync(fx, "POST", "/admin/heal",
                new OwnS3TestClient.Credentials(OwnS3E2eEnvironment.AdminAccessKey, OwnS3E2eEnvironment.AdminSecretKey));
            heal.StatusCode.Should().Be(HttpStatusCode.Accepted);
            using var http = new HttpClient();
            var metrics = await ScrapeUntilAsync(http, fx, "ownS3_heal_passes_total");

            // Assert: метрики полноты; оба объекта читаются (4 диска)
            metrics.Should().Contain("""ownS3_ec_drive_online{otel_scope_name="ownS3",drive="3",set="0"} 1""");
            metrics.Should().Contain("""ownS3_ec_set_readable{otel_scope_name="ownS3",set="0"} 1""");
            foreach (var (key, expected) in new[] { ("before.bin", bodyBefore), ("during.bin", bodyDuring) })
            {
                var get = await s3.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = bucket, Key = key,
                }, ct);
                using var body = new MemoryStream();
                await get.ResponseStream.CopyToAsync(body, ct);
                body.ToArray().Should().Equal(expected, "heal доливает недостающее на вернувшийся диск");
            }
        }, ct);
    }

    [Fact]
    public async Task Erasure_DiskReplace()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: записи → 4-й том подменён ПУСТЫМ (volume rm + новый) → рестарт
        // со всеми 4 → старт с заменой диска (живых 3 ≥ k — новый diskId,
        // канон 06 §9) → heal доливает → все данные читаются
        await OwnS3EcE2eEnvironment.RunScenarioAsync("ec5", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Admin);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3ec{fx.RunTag}rp";
            var body = Body(8);
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket, Key = "survive.bin", InputStream = new MemoryStream(body),
            }, ct);
            var adminCreds = new OwnS3TestClient.Credentials(
                OwnS3E2eEnvironment.AdminAccessKey, OwnS3E2eEnvironment.AdminSecretKey);

            // Arrange: diskId диска 4 до замены
            var before = JsonDocument.Parse(await (await AdminRequestAsync(fx, "GET", "/admin/storage", adminCreds))
                .Content.ReadAsStringAsync(ct)).RootElement;
            var beforeDiskId = before.GetProperty("sets")[0].GetProperty("drives")
                .EnumerateArray().Single(d => d.GetProperty("position").GetInt32() == 3)
                .GetProperty("diskId").GetString();

            // Act: подмена тома 4 пустым (том нельзя rm под смонтировавшим его
            // контейнером — сначала отмонтируем рестартом с 3 томами) + рестарт
            // со всеми 4
            await fx.RestartWithVolumesAsync(1, 2, 3);
            await fx.ReplaceVolumeEmptyAsync(4);
            await fx.RestartWithVolumesAsync(1, 2, 3, 4);

            // Assert: старт с заменой — диск 4 получил НОВЫЙ diskId, сет полный
            var after = JsonDocument.Parse(await (await AdminRequestAsync(fx, "GET", "/admin/storage", adminCreds))
                .Content.ReadAsStringAsync(ct)).RootElement;
            after.GetProperty("mode").GetString().Should().Be("XL Erasure");
            var drives4 = after.GetProperty("sets")[0].GetProperty("drives");
            drives4.EnumerateArray().Should().OnlyContain(d => d.GetProperty("online").GetBoolean());
            var replacedDiskId = drives4.EnumerateArray()
                .Single(d => d.GetProperty("position").GetInt32() == 3)
                .GetProperty("diskId").GetString();
            replacedDiskId.Should().NotBe(beforeDiskId, "пустой том — кандидат замены, живых 3 ≥ k");

            // Act: heal доливает данные на заменённый диск
            var heal = await AdminRequestAsync(fx, "POST", "/admin/heal", adminCreds);
            heal.StatusCode.Should().Be(HttpStatusCode.Accepted);
            using var http = new HttpClient();
            await ScrapeUntilAsync(http, fx, "ownS3_heal_passes_total");

            // Assert: все данные читаются после доливки
            var get = await s3.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket, Key = "survive.bin",
            }, ct);
            using var restored = new MemoryStream();
            await get.ResponseStream.CopyToAsync(restored, ct);
            restored.ToArray().Should().Equal(body);
        }, ct);
    }

    [Fact]
    public async Task Erasure_AdminEdge()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: админ-грань docker-контура: /admin/storage 200 root/admin
        // (режим XL Erasure, деградация корректна после detached-рестарта);
        // POST /admin/heal 202; read-write ключ — 403 на обоих
        await OwnS3EcE2eEnvironment.RunScenarioAsync("ec6", async fx =>
        {
            var root = new OwnS3TestClient.Credentials(
                OwnS3E2eEnvironment.RootUser, OwnS3E2eEnvironment.RootPassword);
            var admin = new OwnS3TestClient.Credentials(
                OwnS3E2eEnvironment.AdminAccessKey, OwnS3E2eEnvironment.AdminSecretKey);
            var writer = new OwnS3TestClient.Credentials(
                OwnS3E2eEnvironment.WriterAccessKey, OwnS3E2eEnvironment.WriterSecretKey);

            // Act / Assert: root и admin — 200 + схема EC
            foreach (var credentials in new[] { root, admin })
            {
                var storage = await AdminRequestAsync(fx, "GET", "/admin/storage", credentials);
                storage.StatusCode.Should().Be(HttpStatusCode.OK);
                var json = JsonDocument.Parse(await storage.Content.ReadAsStringAsync(ct)).RootElement;
                json.GetProperty("mode").GetString().Should().Be("XL Erasure");
                json.GetProperty("sets")[0].GetProperty("drives").GetArrayLength().Should().Be(4);
            }

            // Act: detached-рестарт → деградация корректна в статусе
            await fx.RestartWithVolumesAsync(1, 2, 3);
            var degraded = await AdminRequestAsync(fx, "GET", "/admin/storage", admin);
            degraded.StatusCode.Should().Be(HttpStatusCode.OK);
            var degradedJson = JsonDocument.Parse(await degraded.Content.ReadAsStringAsync(ct)).RootElement;
            var drives = degradedJson.GetProperty("sets")[0].GetProperty("drives");
            drives.EnumerateArray().Single(d => d.GetProperty("position").GetInt32() == 3)
                .GetProperty("online").GetBoolean().Should().BeFalse("4-й том detached — offline");

            // Act / Assert: heal 202; read-write — 403 на обоих эндпоинтах
            var heal = await AdminRequestAsync(fx, "POST", "/admin/heal", admin);
            heal.StatusCode.Should().Be(HttpStatusCode.Accepted);
            (await AdminRequestAsync(fx, "GET", "/admin/storage", writer)).StatusCode
                .Should().Be(HttpStatusCode.Forbidden);
            (await AdminRequestAsync(fx, "POST", "/admin/heal", writer)).StatusCode
                .Should().Be(HttpStatusCode.Forbidden);
        }, ct);
    }

    // Админ-запрос SigV4 (docker-хост: подпись текущим UTC — фиксации времени WAF нет)
    private static async Task<HttpResponseMessage> AdminRequestAsync(
        OwnS3EcE2eEnvironment fx, string method, string path, OwnS3TestClient.Credentials credentials)
    {
        using var http = new HttpClient { BaseAddress = new Uri(fx.S3BaseUrl) };
        var client = new OwnS3TestClient(http);
        return await client.SendSignedAsync(method, path, credentials: credentials, at: DateTimeOffset.UtcNow);
    }

    // Поллинг /metrics до появления серии (тик reader OTel; бюджет 15 с)
    private static async Task<string> ScrapeUntilAsync(HttpClient http, OwnS3EcE2eEnvironment fx,
        string series)
    {
        var deadline = Environment.TickCount64 + 15_000;
        while (true)
        {
            var body = await http.GetStringAsync($"{fx.S3BaseUrl}/metrics", TestContext.Current.CancellationToken);
            if (body.Contains(series, StringComparison.Ordinal) || Environment.TickCount64 >= deadline)
                return body;
            await Task.Delay(200, TestContext.Current.CancellationToken);
        }
    }
}
