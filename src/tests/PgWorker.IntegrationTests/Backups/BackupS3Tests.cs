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
}
