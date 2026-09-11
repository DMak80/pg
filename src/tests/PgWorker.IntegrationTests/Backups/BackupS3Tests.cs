using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// S3-листер против живого MinIO (testcontainers, динамический порт): list/
// пагинация/отсутствие префикса — t03 spec Ф2.
[Collection(MinioCollection.Name)]
public class BackupS3Tests(MinioFixture fixture) : IAsyncLifetime
{
    // Прямой клиент-помощник для сида объектов (AWSSDK, не тестируемый код).
    private AmazonS3Client SeedClient(MinioFixture f) => new(
        new BasicAWSCredentials(MinioFixture.AccessKey, MinioFixture.SecretKey),
        new AmazonS3Config { ServiceURL = f.HostEndpoint, ForcePathStyle = true });

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    [Fact]
    public async Task BucketExists_живой_bucket()
    {
        // Arrange
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act
        var exists = await s3.BucketExistsAsync(TestContext.Current.CancellationToken);

        // Assert
        exists.IsSuccess.Should().BeTrue();
        exists.Value.Should().BeTrue();
    }

    [Fact]
    public async Task ListWal_пустой_префикс_пустой_список()
    {
        // Arrange
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act
        var listed = await s3.ListWalAsync("c1", "shard1", ct: TestContext.Current.CancellationToken);

        // Assert — отсутствие объектов — валидный пустой результат (не ошибка)
        listed.IsSuccess.Should().BeTrue();
        listed.Value.Should().BeEmpty();
    }

    [Fact]
    public async Task ListWal_возвращает_только_wal_префикс_шарда()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        using var client = SeedClient(fixture);
        await client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = MinioFixture.Bucket,
            Key = "c1/shard1/wal/000000010000000000000001",
            ContentBody = "x",
        }, ct);
        await client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = MinioFixture.Bucket,
            Key = "c1/shard2/wal/000000010000000000000001", // чужой шард
            ContentBody = "x",
        }, ct);
        await client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = MinioFixture.Bucket,
            Key = "c1/shard1/full/20260910120000Z/base.tar", // не-wal
            ContentBody = "x",
        }, ct);
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act
        var listed = await s3.ListWalAsync("c1", "shard1", ct: TestContext.Current.CancellationToken);

        // Assert
        listed.Value.Should().ContainSingle(o => o.Name == "000000010000000000000001");
    }

    [Fact]
    public async Task ListWal_пагинация_maxKeys_2_собирает_все()
    {
        // Arrange — 5 объектов, страница по 2 → 3 запроса
        var ct = TestContext.Current.CancellationToken;
        using var client = SeedClient(fixture);
        for (var i = 1; i <= 5; i++)
            await client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = MinioFixture.Bucket,
                Key = $"c2/shard1/wal/0000000100000000000000{i:x2}",
                ContentBody = "x",
            }, ct);
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act
        var listed = await s3.ListWalAsync("c2", "shard1", maxKeysPerTest: 2,
            ct: TestContext.Current.CancellationToken);

        // Assert
        listed.Value.Should().HaveCount(5);
    }

    [Fact]
    public async Task ListWal_отдает_LastModified_объекта()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        using var client = SeedClient(fixture);
        var put = await client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = MinioFixture.Bucket,
            Key = "c3/shard1/wal/000000010000000000000001",
            ContentBody = "x",
        }, ct);
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act
        var listed = await s3.ListWalAsync("c3", "shard1", ct: TestContext.Current.CancellationToken);

        // Assert — время модификации = время загрузки (для last_uploaded_unix)
        listed.Value.Should().ContainSingle();
        listed.Value[0].LastModified.Should().BeWithin(TimeSpan.FromMinutes(1)).After(DateTimeOffset.UtcNow.AddMinutes(-1));
    }

    // ---- t06: ListPrefixAsync / DeleteKeysAsync ----

    [Fact]
    public async Task ListPrefix_размеры_и_полные_ключи()
    {
        // Arrange — объект полного (тело 10 байт) + wal-объект соседнего префикса
        var ct = TestContext.Current.CancellationToken;
        using var client = SeedClient(fixture);
        await client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = MinioFixture.Bucket,
            Key = "c9/shard1/full/20260901/base.tar",
            ContentBody = "0123456789",
        }, ct);
        await client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = MinioFixture.Bucket,
            Key = "c9/shard1/wal/000000010000000000000001",
            ContentBody = "x",
        }, ct);
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act — list по префиксу full/<id>/
        var listed = await s3.ListPrefixAsync("c9/shard1/full/", ct: ct);

        // Assert — один объект, полный ключ и фактический размер тела
        listed.IsSuccess.Should().BeTrue();
        listed.Value.Should().ContainSingle();
        listed.Value[0].Key.Should().Be("c9/shard1/full/20260901/base.tar");
        listed.Value[0].SizeBytes.Should().Be(10);
    }

    [Fact]
    public async Task ListPrefix_пустой_префикс_весь_bucket()
    {
        // Arrange — объекты в двух кластер-префиксах
        var ct = TestContext.Current.CancellationToken;
        using var client = SeedClient(fixture);
        await client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = MinioFixture.Bucket,
            Key = "c10/shard1/full/20260901/base.tar",
            ContentBody = "x",
        }, ct);
        await client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = MinioFixture.Bucket,
            Key = "c11/shard9/wal/000000010000000000000001",
            ContentBody = "x",
        }, ct);
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act — пустой префикс = весь bucket (база used_bytes)
        var listed = await s3.ListPrefixAsync("", ct: ct);

        // Assert — оба чужих префикса видны
        listed.IsSuccess.Should().BeTrue();
        listed.Value.Select(o => o.Key).Should().Contain([
            "c10/shard1/full/20260901/base.tar",
            "c11/shard9/wal/000000010000000000000001",
        ]);
    }

    [Fact]
    public async Task ListPrefix_пагинация()
    {
        // Arrange — 5 объектов, страница по 2
        var ct = TestContext.Current.CancellationToken;
        using var client = SeedClient(fixture);
        for (var i = 1; i <= 5; i++)
            await client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = MinioFixture.Bucket,
                Key = $"c12/shard1/full/2026090{i}/base.tar",
                ContentBody = "x",
            }, ct);
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act — list с инъекцией размера страницы
        var listed = await s3.ListPrefixAsync("c12/shard1/full/", maxKeysPerTest: 2, ct: ct);

        // Assert — все 5 собраны через continuation-token
        listed.IsSuccess.Should().BeTrue();
        listed.Value.Should().HaveCount(5);
    }

    [Fact]
    public async Task DeleteKeys_удаляет_и_идемпотентен()
    {
        // Arrange — два объекта под удаление
        var ct = TestContext.Current.CancellationToken;
        using var client = SeedClient(fixture);
        var keys = new[]
        {
            "c13/shard1/full/20260901/base.tar",
            "c13/shard1/full/20260902/base.tar",
        };
        foreach (var key in keys)
            await client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = MinioFixture.Bucket,
                Key = key,
                ContentBody = "x",
            }, ct);
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act — batch-delete, затем повтор тех же ключей
        var deleted = await s3.DeleteKeysAsync(keys, ct);
        var repeat = await s3.DeleteKeysAsync(keys, ct);
        var listed = await s3.ListPrefixAsync("c13/shard1/full/", ct: ct);

        // Assert — первый delete успех и префикс пуст; повтор (несуществующие)
        // — Success без ошибок (идемпотентность)
        deleted.IsSuccess.Should().BeTrue();
        repeat.IsSuccess.Should().BeTrue();
        listed.Value.Should().BeEmpty();
    }

    [Fact]
    public async Task DeleteKeys_пустой_список()
    {
        // Arrange — пустой список ключей
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act — delete без ключей
        var deleted = await s3.DeleteKeysAsync([], TestContext.Current.CancellationToken);

        // Assert — Success без вызова S3
        deleted.IsSuccess.Should().BeTrue();
    }

    [Fact]
    public async Task DeleteKeys_чанки_1002_ключа()
    {
        // Arrange — 1002 объекта (два чанка: 1000+2)
        var ct = TestContext.Current.CancellationToken;
        using var client = SeedClient(fixture);
        var keys = new List<string>();
        for (var i = 0; i < 1002; i++)
        {
            var key = $"c14/shard1/full/20260901/obj{i:0000}";
            keys.Add(key);
            await client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = MinioFixture.Bucket,
                Key = key,
                ContentBody = "x",
            }, ct);
        }

        await using var s3 = new BackupS3(fixture.Runtime());

        // Act — batch-delete всех ключей
        var deleted = await s3.DeleteKeysAsync(keys, ct);
        var listed = await s3.ListPrefixAsync("c14/shard1/full/", ct: ct);

        // Assert — чанки покрыли всё, префикс пуст
        deleted.IsSuccess.Should().BeTrue();
        listed.Value.Should().BeEmpty();
    }

    // ---- t04: ListAsync / GetObjectAsync (verify) ----

    // AAA: ListAsync произвольного префикса full/<id>/pg_wal/ — только объекты префикса
    [Fact]
    public async Task ListAsync_ПрефиксНабора_ВозвращаетТолькоЕгоОбъекты()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        using var client = SeedClient(fixture);
        await client.PutObjectAsync(new PutObjectRequest
        { BucketName = MinioFixture.Bucket, Key = "v1/shard1/full/20260911120000Z/pg_wal/000000010000000000000001", ContentBody = "x" }, ct);
        await client.PutObjectAsync(new PutObjectRequest
        { BucketName = MinioFixture.Bucket, Key = "v1/shard1/wal/000000010000000000000001", ContentBody = "x" }, ct);
        await client.PutObjectAsync(new PutObjectRequest
        { BucketName = MinioFixture.Bucket, Key = "v1/shard1/full/20260911120000Z/PG_VERSION", ContentBody = "x" }, ct);
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act
        var listed = await s3.ListAsync("v1", "shard1", "full/20260911120000Z/pg_wal/", ct: ct);

        // Assert
        listed.IsSuccess.Should().BeTrue();
        listed.Value.Select(o => o.Name).Should().BeEquivalentTo(["000000010000000000000001"]);
    }

    // AAA: ListAsync пагинация на произвольном префиксе (maxKeys=2, 5 объектов)
    [Fact]
    public async Task ListAsync_Пагинация_СобираетВсе()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        using var client = SeedClient(fixture);
        for (var i = 1; i <= 5; i++)
            await client.PutObjectAsync(new PutObjectRequest
            { BucketName = MinioFixture.Bucket, Key = $"v2/shard1/wal/0000000100000000000000{i:x2}", ContentBody = "x" }, ct);
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act
        var listed = await s3.ListAsync("v2", "shard1", "wal/", maxKeysPerTest: 2, ct: ct);

        // Assert
        listed.Value.Should().HaveCount(5);
    }

    // AAA: GetObjectAsync — содержимое history-объекта (крошечный GET)
    [Fact]
    public async Task GetObject_ОтдаетСодержимое()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        using var client = SeedClient(fixture);
        await client.PutObjectAsync(new PutObjectRequest
        { BucketName = MinioFixture.Bucket, Key = "v3/shard1/wal/00000002.history", ContentBody = "1\t0/2000000\n" }, ct);
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act
        var got = await s3.GetObjectAsync("v3", "shard1", "wal/00000002.history", ct);

        // Assert
        got.IsSuccess.Should().BeTrue();
        got.Value.Should().Be("1\t0/2000000\n");
    }

    // AAA: GetObjectAsync несуществующего — Failed (не исключение мимо Result)
    [Fact]
    public async Task GetObject_Отсутствует_Failed()
    {
        // Arrange
        await using var s3 = new BackupS3(fixture.Runtime());

        // Act
        var got = await s3.GetObjectAsync("v4", "shard1", "wal/00000009.history", TestContext.Current.CancellationToken);

        // Assert
        got.IsSuccess.Should().BeFalse();
    }
}
