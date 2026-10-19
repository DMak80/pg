using AdminPanel.Probes.S3;
using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Перебор целей инвентаря (t36, adminpanel/02 §2.5): первая цель отказывает
// соединением → list-вызовы успешны со второй; обе отказывают → Failed
// (грань недоступна этим тиком). Цели подаются клиенту UpdateTargets'ом
// из снапшота S3 (t31); клиенты per-цели подменяются internal-швом
// (фейк-подкласс AmazonS3Client — методы виртуальные).
public class S3StorageClientFailoverTests
{
    // Options без кредов и endpoints: цели — из снапшота, не из конфига.
    private static BackupsS3Options Options() => new()
    {
        S3 = new BackupsS3Options.S3Options { Bucket = "pgworker-backups" },
        TimeoutSec = 2,
    };

    private static S3StorageClient Client(BackupsS3Options options, FakeTargetClient first, FakeTargetClient second)
    {
        // Цели подаются после конструирования — резолв снапшота перед тиком.
        var client = new S3StorageClient(options, new NeverHttpClientFactory(),
            ep => ep == "http://a:9000" ? first : second);
        client.UpdateTargets(new S3StorageTargets(
            "demo", ["http://a:9000", "http://b:9000"], "k", "s"));
        return client;
    }

    // AAA: первая цель отказывает соединением → ListBuckets успешен со второй;
    // каждая цель опрошена ровно один раз (отказ цели грань не выключает).
    [Fact]
    public async Task ListBuckets_FirstTargetDown_SecondWins()
    {
        // Arrange — цель a мертва, цель b жива.
        var first = new FakeTargetClient(failure: new HttpRequestException("connection refused (a)"));
        var second = new FakeTargetClient(failure: null);
        var client = Client(Options(), first, second);

        // Act
        var result = await client.ListBucketsAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().ContainSingle().Which.Should().Be("pgworker-backups");
        first.ListBucketsCalls.Should().Be(1);
        second.ListBucketsCalls.Should().Be(1);
    }

    // AAA: первая цель отказывает → ListPage успешен со второй (клиент per-цели).
    [Fact]
    public async Task ListPage_FirstTargetDown_SecondWins()
    {
        // Arrange
        var first = new FakeTargetClient(failure: new HttpRequestException("connection refused (a)"));
        var second = new FakeTargetClient(failure: null);
        var client = Client(Options(), first, second);

        // Act
        var result = await client.ListPageAsync(null, null, 100, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Items.Should().ContainSingle().Which.Key.Should().Be("demo/s1/full/2026/f1");
        result.Value.NextContinuationToken.Should().BeNull();
    }

    // AAA: обе цели отказывают → Failed, текст «все цели инвентаря недоступны»
    // (грань недоступна этим тиком — алерт backup-s3-unreachable видит ошибку).
    [Fact]
    public async Task AllTargetsDown_Failed()
    {
        // Arrange
        var first = new FakeTargetClient(failure: new HttpRequestException("connection refused (a)"));
        var second = new FakeTargetClient(failure: new HttpRequestException("connection refused (b)"));
        var client = Client(Options(), first, second);

        // Act
        var result = await client.ListBucketsAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("all inventory targets are unreachable");
        result.Error.Message.Should().Contain("connection refused (b)");
    }

    // Фейк одной цели: failure != null — отказ соединения на каждом вызове;
    // иначе — канонические list-ответы (один bucket/объект).
    private sealed class FakeTargetClient(Exception? failure) : AmazonS3Client(
        new AmazonS3Config { ServiceURL = "http://fake", AuthenticationRegion = "us-east-1" })
    {
        public int ListBucketsCalls;

        public override Task<ListBucketsResponse> ListBucketsAsync(CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref ListBucketsCalls);
            if (failure is not null)
                return Task.FromException<ListBucketsResponse>(failure);
            var response = new ListBucketsResponse();
            response.Buckets.Add(new S3Bucket { BucketName = "pgworker-backups" });
            return Task.FromResult(response);
        }

        public override Task<ListObjectsV2Response> ListObjectsV2Async(
            ListObjectsV2Request request, CancellationToken cancellationToken = default)
        {
            if (failure is not null)
                return Task.FromException<ListObjectsV2Response>(failure);
            var response = new ListObjectsV2Response();
            response.S3Objects.Add(new S3Object { Key = "demo/s1/full/2026/f1", Size = 10 });
            return Task.FromResult(response);
        }
    }

    // list-тесты не доходят до health-проб — фабрика отдаёт клиент с
    // роняющим обработчиком (страхование от сетевых выходов).
    private sealed class NeverHttpClientFactory : System.Net.Http.IHttpClientFactory
    {
        public System.Net.Http.HttpClient CreateClient(string name)
            => new(new ThrowingHandler());

        private sealed class ThrowingHandler() : System.Net.Http.HttpMessageHandler
        {
            protected override Task<System.Net.Http.HttpResponseMessage> SendAsync(
                System.Net.Http.HttpRequestMessage request, CancellationToken cancellationToken)
                => throw new HttpRequestException("health probe must not be called in list tests");
        }
    }
}
