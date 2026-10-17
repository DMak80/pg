using System.Net;
using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;

namespace OwnS3.IntegrationTests.E2e;

// Класс 1 (spec §4.5): AWS SDK .NET — простые операции реальным клиентом
// (PUT/GET/HEAD, conditional, Range, Copy, Delete/batch, листинги, оба
// режима checksum, матрица ролей). Данные детерминированы (seed); AAA.
[Collection(OwnS3E2eCollection.Name)]
public sealed class AwsSdkSimpleScenarios
{
    [Fact]
    public async Task Buckets_Lifecycle_ByAdmin()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: полный жизненный цикл бакета admin-клиентом SDK: создание,
        // заголовок, список, локация, повторное создание → 409, удаление
        // пустого → 204, удаление несуществующего → 404
        await OwnS3E2eEnvironment.RunScenarioAsync("s1", async fx =>
        {
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Admin);
            var bucket = $"os3e2e{fx.RunTag}lc";

            // Act / Assert
            await s3.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            (await s3.HeadBucketAsync(new HeadBucketRequest { BucketName = bucket }, ct))
                .HttpStatusCode.Should().Be(HttpStatusCode.OK);
            (await s3.ListBucketsAsync(ct)).Buckets.Should().Contain(b => b.BucketName == bucket);
            (await s3.GetBucketLocationAsync(new GetBucketLocationRequest { BucketName = bucket }, ct))
                .Location.Value.Should().BeEmpty("us-east-1 кодируется пустой Location (канон 02)");

            // Act / Assert: повторное создание уже своего бакета → 409
            (await s3.Invoking(x => x.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct))
                .Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.ErrorCode == "BucketAlreadyOwnedByYou" && e.StatusCode == HttpStatusCode.Conflict);

            // Act / Assert: удаление пустого → 204; несуществующего → 404
            (await s3.DeleteBucketAsync(new DeleteBucketRequest { BucketName = bucket }, ct))
                .HttpStatusCode.Should().Be(HttpStatusCode.NoContent);
            (await s3.Invoking(x => x.DeleteBucketAsync(new DeleteBucketRequest { BucketName = bucket }, ct))
                .Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.ErrorCode == "NoSuchBucket" && e.StatusCode == HttpStatusCode.NotFound);
        }, ct);
    }

    [Fact]
    public async Task PutGet_Roundtrip_Metadata_And_Etag()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: PUT (256 КиБ, ключ с UTF-8/слэшами, ContentType, 2 метаданные)
        // → GET возвращает байты/ETag-MD5/ContentType/метаданные; HEAD —
        // Size/ETag/LastModified; ListObjectsV2 видит ключ
        await OwnS3E2eEnvironment.RunScenarioAsync("s2", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}rt";
            var key = "backups/os3-e2e/файл+1.dat";
            var body = OwnS3E2eBytes.Deterministic(256 * 1024, 42);
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);

            // Act: PUT (MetadataCollection SDK сама ставит префикс x-amz-meta-)
            var put = new PutObjectRequest
            {
                BucketName = bucket, Key = key, InputStream = new MemoryStream(body),
                ContentType = "application/x-os3-e2e",
            };
            put.Metadata.Add("src", "e2e");
            put.Metadata.Add("run", fx.RunTag);
            var putResp = await s3.PutObjectAsync(put, ct);

            // Assert: ETag = MD5 тела (канон 02 §1)
            putResp.HttpStatusCode.Should().Be(HttpStatusCode.OK);
            putResp.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Md5Hex(body));

            // Act / Assert: GET — байты/ETag/ContentType/метаданные
            using var get = await s3.GetObjectAsync(new GetObjectRequest { BucketName = bucket, Key = key }, ct);
            using var ms = new MemoryStream();
            await get.ResponseStream.CopyToAsync(ms, ct);
            ms.ToArray().Should().Equal(body, "GET возвращает байты PUT без искажений");
            get.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Md5Hex(body));
            get.Headers["Content-Type"].Should().Be("application/x-os3-e2e");
            get.Metadata["src"].Should().Be("e2e");
            get.Metadata["run"].Should().Be(fx.RunTag);

            // Act / Assert: HEAD — размер/ETag/LastModified; листинг видит ключ
            var head = await s3.GetObjectMetadataAsync(
                new GetObjectMetadataRequest { BucketName = bucket, Key = key }, ct);
            head.ContentLength.Should().Be(body.Length);
            head.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Md5Hex(body));
            head.LastModified.Should().BeAfter(DateTime.UtcNow.AddDays(-1));
            (await s3.ListObjectsV2Async(new ListObjectsV2Request { BucketName = bucket }, ct))
                .S3Objects.Should().Contain(o => o.Key == key);
        }, ct);
    }

    [Fact]
    public async Task Conditional_Requests()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: conditional GET — If-Match верный 200 / чужой 412;
        // If-None-Match верный 304; If-Modified-Since будущее 304;
        // If-Unmodified-Since прошлое 412 (канон 02 §1)
        await OwnS3E2eEnvironment.RunScenarioAsync("s3", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}cd";
            var key = "cond.bin";
            var body = OwnS3E2eBytes.Deterministic(64 * 1024, 7);
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            var etag = (await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket, Key = key, InputStream = new MemoryStream(body),
            }, ct)).ETag!.Trim('"');

            // Act / Assert: If-Match верный → 200
            (await s3.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket, Key = key, EtagToMatch = OwnS3E2eETag.Quote(etag),
            }, ct)).HttpStatusCode.Should().Be(HttpStatusCode.OK);

            // Act / Assert: If-Match чужой → 412 PreconditionFailed
            (await s3.Invoking(x => x.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket, Key = key, EtagToMatch = "\"deadbeef\"",
            }, ct)).Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.StatusCode == HttpStatusCode.PreconditionFailed
                            && e.ErrorCode == "PreconditionFailed");

            // Act / Assert: If-None-Match верный → 304 (SDK бросает исключение)
            (await s3.Invoking(x => x.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket, Key = key, EtagToNotMatch = OwnS3E2eETag.Quote(etag),
            }, ct)).Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.StatusCode == HttpStatusCode.NotModified);

            // Act / Assert: If-Modified-Since (будущее) → 304
            (await s3.Invoking(x => x.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket, Key = key, ModifiedSinceDateUtc = DateTime.UtcNow.AddDays(1),
            }, ct)).Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.StatusCode == HttpStatusCode.NotModified);

            // Act / Assert: If-Unmodified-Since (прошлое) → 412
            (await s3.Invoking(x => x.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket, Key = key,
                UnmodifiedSinceDateUtc = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            }, ct)).Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.StatusCode == HttpStatusCode.PreconditionFailed);
        }, ct);
    }

    [Fact]
    public async Task Range_Requests()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: Range — закрытый bytes=10-20 (206 + срез), открытый bytes=N-,
        // суффикс -5, end за размером → 206 с обрезкой. Кейсов условного
        // диапазона нет: поддержка снята с ownS3 (spec §3.3/§4.6), SDK
        // заголовок не шлёт
        await OwnS3E2eEnvironment.RunScenarioAsync("s4", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}rg";
            var key = "range.bin";
            var body = OwnS3E2eBytes.Deterministic(100_000, 11);
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket, Key = key, InputStream = new MemoryStream(body),
            }, ct);

            async Task<byte[]> RangeAsync(string range)
            {
                using var resp = await s3.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = bucket, Key = key, ByteRange = new ByteRange(range),
                }, ct);
                resp.HttpStatusCode.Should().Be(HttpStatusCode.PartialContent,
                    $"диапазон {range} обязан отвечать 206");
                using var ms = new MemoryStream();
                await resp.ResponseStream.CopyToAsync(ms, ct);
                return ms.ToArray();
            }

            // Act / Assert
            (await RangeAsync("bytes=10-20")).Should().Equal(body[10..21]);
            (await RangeAsync("bytes=10-")).Should().Equal(body[10..]);
            (await RangeAsync("bytes=-5")).Should().Equal(body[^5..]);
            (await RangeAsync("bytes=99990-999999")).Should().Equal(body[99990..],
                "end за размером обрезается до конца объекта");
        }, ct);
    }

    [Fact]
    public async Task CopyObject_Copy()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: CopyObject COPY наследует тело (ETag = MD5 того же тела) и
        // метаданные; REPLACE меняет ContentType/metadata; DeleteObject
        // идемпотентен (204); GET удалённого → NoSuchKey 404
        await OwnS3E2eEnvironment.RunScenarioAsync("s5", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}cp";
            var src = "copy-src.bin";
            var body = OwnS3E2eBytes.Deterministic(128 * 1024, 21);
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            var put = new PutObjectRequest
            {
                BucketName = bucket, Key = src, InputStream = new MemoryStream(body),
                ContentType = "text/plain",
            };
            put.Metadata.Add("gen", "1");
            await s3.PutObjectAsync(put, ct);

            // Act / Assert: COPY — байты и ETag наследуются
            await s3.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucket = bucket, SourceKey = src,
                DestinationBucket = bucket, DestinationKey = "copy-dst.bin",
            }, ct);
            using var copy = await s3.GetObjectAsync(
                new GetObjectRequest { BucketName = bucket, Key = "copy-dst.bin" }, ct);
            using var ms = new MemoryStream();
            await copy.ResponseStream.CopyToAsync(ms, ct);
            ms.ToArray().Should().Equal(body);
            copy.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Md5Hex(body));
            copy.Metadata["gen"].Should().Be("1", "COPY наследует метаданные");

            // Act / Assert: REPLACE — новые ContentType/metadata
            var replace = new CopyObjectRequest
            {
                SourceBucket = bucket, SourceKey = src,
                DestinationBucket = bucket, DestinationKey = "copy-rep.bin",
                MetadataDirective = S3MetadataDirective.REPLACE,
                ContentType = "application/json",
            };
            replace.Metadata.Add("gen", "2");
            await s3.CopyObjectAsync(replace, ct);
            using var rep = await s3.GetObjectAsync(
                new GetObjectRequest { BucketName = bucket, Key = "copy-rep.bin" }, ct);
            rep.Headers["Content-Type"].Should().Be("application/json");
            rep.Metadata["gen"].Should().Be("2");

            // Act / Assert: DeleteObject → 204; GET → 404 NoSuchKey; повтор → 204
            (await s3.DeleteObjectAsync(
                new DeleteObjectRequest { BucketName = bucket, Key = "copy-dst.bin" }, ct))
                .HttpStatusCode.Should().Be(HttpStatusCode.NoContent);
            (await s3.Invoking(x => x.GetObjectAsync(
                    new GetObjectRequest { BucketName = bucket, Key = "copy-dst.bin" }, ct))
                .Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.ErrorCode == "NoSuchKey" && e.StatusCode == HttpStatusCode.NotFound);
            (await s3.DeleteObjectAsync(
                new DeleteObjectRequest { BucketName = bucket, Key = "copy-dst.bin" }, ct))
                .HttpStatusCode.Should().Be(HttpStatusCode.NoContent, "удаление идемпотентно");
        }, ct);
    }

    [Fact]
    public async Task DeleteObjects_Batch_And_Listings()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: 12 ключей → DeleteObjects 3 → перечислены; delimiter →
        // CommonPrefixes; пагинация max-keys=5 до исчерпания;
        // GetObjectAttributes → ETag/ObjectSize/StorageClass
        await OwnS3E2eEnvironment.RunScenarioAsync("s6", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}ls";
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            for (var i = 0; i < 12; i++)
                await s3.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = bucket, Key = $"list/prefix{i % 3}/obj{i}",
                    InputStream = new MemoryStream(OwnS3E2eBytes.Deterministic(1024, i)),
                }, ct);

            // Act / Assert: батч-удаление 3 ключей — перечислены в ответе
            var batch = await s3.DeleteObjectsAsync(new DeleteObjectsRequest
            {
                BucketName = bucket,
                Objects = new List<KeyVersion>
                {
                    new() { Key = "list/prefix0/obj0" },
                    new() { Key = "list/prefix1/obj1" },
                    new() { Key = "list/prefix2/obj2" },
                },
            }, ct);
            batch.DeletedObjects.Select(d => d.Key).Should()
                .Contain(new[] { "list/prefix0/obj0", "list/prefix1/obj1", "list/prefix2/obj2" });

            // Act / Assert: delimiter без Prefix — свёртка по ПЕРВОМУ "/"
            // после префикса (канон 02 §3, ListWalker): все 9 ключей живут
            // под "list/" → единственный CommonPrefix, объектов нет
            // (CommonPrefixes SDK — List<string>: префиксы без обёртки)
            var root = await s3.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = bucket, Delimiter = "/",
            }, ct);
            root.CommonPrefixes.Should().BeEquivalentTo(new[] { "list/" });
            root.S3Objects.Should().BeEmpty("все ключи свернуты в префикс list/");

            // Act / Assert: Prefix="list/" — следующий уровень префиксов
            var nested = await s3.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = bucket, Delimiter = "/", Prefix = "list/",
            }, ct);
            nested.CommonPrefixes.Should()
                .BeEquivalentTo(new[] { "list/prefix0/", "list/prefix1/", "list/prefix2/" });

            // Act / Assert: пагинация max-keys=5 до исчерпания (9 ключей)
            var seen = new List<string>();
            string? token = null;
            while (true)
            {
                var page = await s3.ListObjectsV2Async(new ListObjectsV2Request
                {
                    BucketName = bucket, MaxKeys = 5, ContinuationToken = token,
                }, ct);
                seen.AddRange(page.S3Objects.Select(o => o.Key));
                if (!page.IsTruncated)
                    break;
                page.NextContinuationToken.Should().NotBeNull("truncated страница несёт токен");
                token = page.NextContinuationToken;
            }
            seen.Should().HaveCount(9, "12 создано − 3 удалено пакетом");

            // Act / Assert: GetObjectAttributes — ETag/ObjectSize/StorageClass
            var body = OwnS3E2eBytes.Deterministic(4096, 99);
            await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket, Key = "attr.bin", InputStream = new MemoryStream(body),
            }, ct);
            var attrs = await s3.GetObjectAttributesAsync(new GetObjectAttributesRequest
            {
                BucketName = bucket, Key = "attr.bin",
                ObjectAttributes = new List<ObjectAttributes>
                    { ObjectAttributes.ETag, ObjectAttributes.ObjectSize, ObjectAttributes.StorageClass },
            }, ct);
            attrs.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Md5Hex(body));
            attrs.ObjectSize.Should().Be(body.Length);
            attrs.StorageClass.Value.Should().Be("STANDARD");
        }, ct);
    }

    [Fact]
    public async Task DeleteObjects_AfterOverwrite_WalPattern_BigObjects()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: паттерн WAL-агентов подсистемы бэкапов PgWorker — глубокий
        // префикс cluster/shard1/wal/, 24-hex имена, ТЕЛО 16 МБ (реальный
        // сегмент), ПЕРЕЗАПИСЬ живого объекта (рестарт агента перезаливает
        // с чекпойнта) → batch-delete середины цепи → листинги не видят
        // удалённый ключ и НЕ ДУБЛИРУЮТ перезаписанный (инцидент прогона
        // 2026-10-16: wal-контур PgWorker — batch-удалённая жертва видна
        // листингу дрилла, перезаписанный сегмент — дважды).
        await OwnS3E2eEnvironment.RunScenarioAsync("s9", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}wl";
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            var big = OwnS3E2eBytes.Deterministic(16 * 1024 * 1024, 11);
            string K(string seg) => $"e2ecluster{fx.RunTag}/shard1/wal/{seg}";

            // Arrange: цепь 5 сегментов по 16 МБ + ПЕРЕЗАПИСЬ середины
            // (дубль-паттерн рестарта агента: тот же ключ новым телом)
            foreach (var seg in new[]
                     {
                         "000000010000000000000002", "000000010000000000000003",
                         "000000010000000000000004", "000000010000000000000005",
                         "000000010000000000000006",
                     })
                await s3.PutObjectAsync(new PutObjectRequest
                { BucketName = bucket, Key = K(seg), InputStream = new MemoryStream(big) }, ct);
            await s3.PutObjectAsync(new PutObjectRequest
            { BucketName = bucket, Key = K("000000010000000000000003"), InputStream = new MemoryStream(big) }, ct);

            // Act: batch-delete середины цепи (жертва валидационного дрилла)
            var victimKey = K("000000010000000000000004");
            var batch = await s3.DeleteObjectsAsync(new DeleteObjectsRequest
            {
                BucketName = bucket,
                Objects = new List<KeyVersion> { new() { Key = victimKey } },
            }, ct);

            // Assert: удаление подтверждено ответом
            batch.DeletedObjects.Should().ContainSingle(
                d => d.Key == victimKey, "жертва удалена пакетно");

            // Assert: листинг — жертвы НЕТ, перезаписанный ровно ОДИН
            var list = await s3.ListObjectsV2Async(new ListObjectsV2Request
            { BucketName = bucket, Prefix = $"e2ecluster{fx.RunTag}/shard1/wal/" }, ct);
            list.S3Objects.Select(o => o.Key).Should().HaveCount(4, "5 − жертва")
                .And.NotContain(victimKey);
            list.S3Objects.Count(o => o.Key == K("000000010000000000000003")).Should().Be(1,
                "перезапись не создаёт дубль ключа в листинге");

            // Assert: HEAD жертвы — 404 (физически удалена)
            (await s3.Invoking(x => x.GetObjectMetadataAsync(
                    new GetObjectMetadataRequest { BucketName = bucket, Key = victimKey }, ct))
                .Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.StatusCode == HttpStatusCode.NotFound);
        }, ct);
    }

    [Fact]
    public async Task DeleteObjects_WhileParallelOverwrite_LinearizableOutcome()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: гонка batch-delete × параллельная перезапись того же ключа
        // (паттерн двух WAL-агентов, перезаливающих хвост с чекпойнта поверх
        // удаления). Итог линеен: ключ либо удалён (HEAD 404), либо жив
        // РОВНО ОДИН раз и GET отдаёт ЦЕЛЫЕ байты одной из записанных версий
        // — «delete вернул успех при рассогласованном живом объекте»
        // (живой xl.meta поверх убранного dataDir) невозможен.
        await OwnS3E2eEnvironment.RunScenarioAsync("s10", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            var bucket = $"os3e2e{fx.RunTag}go";
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            var key = $"e2e{fx.RunTag}/shard1/wal/000000010000000000000099";

            // Arrange: начальная версия + 5 перезаписей параллельно с batch-delete
            var versions = Enumerable.Range(0, 6)
                .Select(i => OwnS3E2eBytes.Deterministic(1024 * 1024, 21 + i))
                .ToList();
            await s3.PutObjectAsync(new PutObjectRequest
            { BucketName = bucket, Key = key, InputStream = new MemoryStream(versions[0]) }, ct);
            var overwrites = Task.WhenAll(Enumerable.Range(1, 5).Select(async i =>
            {
                await Task.Delay(i * 40, ct);
                await s3.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = bucket, Key = key, InputStream = new MemoryStream(versions[i]),
                }, ct);
            }));
            var deleted = await s3.DeleteObjectsAsync(new DeleteObjectsRequest
            {
                BucketName = bucket,
                Objects = new List<KeyVersion> { new() { Key = key } },
            }, ct);
            await overwrites;
            deleted.DeletedObjects.Should().ContainSingle(d => d.Key == key, "объект существовал на момент delete");

            // Act: финальный листинг после успокоения гонки
            await Task.Delay(TimeSpan.FromSeconds(1), ct);
            var listed = (await s3.ListObjectsV2Async(
                    new ListObjectsV2Request { BucketName = bucket, Prefix = $"e2e{fx.RunTag}/" }, ct))
                .S3Objects.Where(o => o.Key == key).ToList();

            // Assert: линеаризованный исход — либо удалён, либо один и целый
            listed.Should().HaveCountLessThanOrEqualTo(1, "дублей ключа в листинге быть не может");
            if (listed.Count == 0)
            {
                (await s3.Invoking(x => x.GetObjectMetadataAsync(
                        new GetObjectMetadataRequest { BucketName = bucket, Key = key }, ct))
                    .Should().ThrowAsync<AmazonS3Exception>())
                    .Where(e => e.StatusCode == HttpStatusCode.NotFound);
                return;
            }

            using var get = await s3.GetObjectAsync(new GetObjectRequest { BucketName = bucket, Key = key }, ct);
            using var ms = new MemoryStream();
            await get.ResponseStream.CopyToAsync(ms, ct);
            var bytes = ms.ToArray();
            bytes.Length.Should().Be(1024 * 1024, "размер целой версии");
            versions.Skip(1).Should().Contain(v => v.SequenceEqual(bytes),
                "байты — ЦЕЛАЯ версия одной из перезаписей (целостность не зависит от гонки delete)");
        }, ct);
    }

    [Fact]
    public async Task ChecksumTrailer_PutGet_WhenSupported()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: приёмка канона 03 §3 реальным SDK в режиме WHEN_SUPPORTED:
        // SDK сам использует чанковую подпись с трейлерами
        // (STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER + x-amz-checksum-*);
        // ETag-правило не меняется (MD5 тела)
        await OwnS3E2eEnvironment.RunScenarioAsync("s7", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer,
                OwnS3ChecksumMode.WhenSupported);
            var bucket = $"os3e2e{fx.RunTag}cs";
            var key = "trailer.bin";
            var body = OwnS3E2eBytes.Deterministic(512 * 1024, 5);
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);

            // Act: PUT — SDK вычисляет чексуммы сам (трейлерный чанковый режим)
            var put = await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket, Key = key, InputStream = new MemoryStream(body),
            }, ct);

            // Assert
            put.HttpStatusCode.Should().Be(HttpStatusCode.OK,
                "трейлерный режим WHEN_SUPPORTED обязан приниматься ownS3 (канон 03 §3)");
            using var get = await s3.GetObjectAsync(
                new GetObjectRequest { BucketName = bucket, Key = key }, ct);
            using var ms = new MemoryStream();
            await get.ResponseStream.CopyToAsync(ms, ct);
            ms.ToArray().Should().Equal(body);
            get.ETag!.Trim('"').Should().Be(OwnS3E2eETag.Md5Hex(body),
                "трейлеры не меняют ETag-правило простого PUT");
        }, ct);
    }

    [Fact]
    public async Task ReaderRole_Denied_On_Write()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;

        // AAA: статические ключи работают в докер-контуре (матрица 05 §3):
        // reader пишет → 403 AccessDenied; читает writer-объект → 200
        await OwnS3E2eEnvironment.RunScenarioAsync("s8", async fx =>
        {
            using var admin = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
            using var writer = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
            using var reader = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Reader);
            var bucket = $"os3e2e{fx.RunTag}ro";
            var key = "protected.bin";
            var body = OwnS3E2eBytes.Deterministic(2048, 13);
            await admin.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
            await writer.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket, Key = key, InputStream = new MemoryStream(body),
            }, ct);

            // Act / Assert: reader PUT → 403
            (await reader.Invoking(x => x.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket, Key = "denied.bin", InputStream = new MemoryStream(body),
            }, ct)).Should().ThrowAsync<AmazonS3Exception>())
                .Where(e => e.ErrorCode == "AccessDenied" && e.StatusCode == HttpStatusCode.Forbidden);

            // Act / Assert: reader GET writer-объекта → 200
            using var get = await reader.GetObjectAsync(
                new GetObjectRequest { BucketName = bucket, Key = key }, ct);
            using var ms = new MemoryStream();
            await get.ResponseStream.CopyToAsync(ms, ct);
            ms.ToArray().Should().Equal(body);
        }, ct);
    }
}
