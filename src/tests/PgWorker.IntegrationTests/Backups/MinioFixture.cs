using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// MinIO testcontainers для S3-интеграций бэкапов (t03): ДИНАМИЧЕСКИЙ хост-порт
// (никаких литералов), bucket создаётся фикстурой прямым AWSSDK-клиентом.
public sealed class MinioFixture : IAsyncLifetime
{
    public const string AccessKey = "minioadmin";
    public const string SecretKey = "minioadmin";
    public const string Bucket = "pgw-backups-test";

    private IContainer? _minio;

    // Endpoint для ХОСТА-клиента (тест): localhost:<динамический порт>.
    public string HostEndpoint { get; private set; } = "";

    // Endpoint для КОНТЕЙНЕРОВ (агент): host.docker.internal:<тот же порт>.
    public string ContainerEndpoint { get; private set; } = "";

    public async ValueTask InitializeAsync()
    {
        // Порт публикуется на свободный хост-порт (GetMappedPublicPort ниже).
        _minio = new ContainerBuilder("minio/minio:RELEASE.2025-09-07T16-13-09Z")
            .WithCommand("server", "/data", "--console-address", ":9001")
            .WithEnvironment("MINIO_ROOT_USER", AccessKey)
            .WithEnvironment("MINIO_ROOT_PASSWORD", SecretKey)
            .WithPortBinding(9000, assignRandomHostPort: true)
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilCommandIsCompleted(["mc", "ready", "local"],
                    w => w.WithTimeout(TimeSpan.FromSeconds(45)))) // ≤ 100 c: падаем быстро
            .Build();
        await _minio.StartAsync(TestContext.Current.CancellationToken);

        var port = _minio.GetMappedPublicPort(9000);
        HostEndpoint = $"http://localhost:{port}";
        ContainerEndpoint = $"http://host.docker.internal:{port}";

        // bucket per-install — прямой AWSSDK-клиент (создание bucket — НЕ операция
        // подсистемы: spec §3.1; тестовая утилита фикстуры).
        var client = new AmazonS3Client(
            new BasicAWSCredentials(AccessKey, SecretKey),
            new AmazonS3Config { ServiceURL = HostEndpoint, ForcePathStyle = true });
        await client.PutBucketAsync(new PutBucketRequest { BucketName = Bucket },
            TestContext.Current.CancellationToken);
        client.Dispose();
    }

    public BackupsRuntimeOptions Runtime() => new(
        "pgworker-backup:test", HostEndpoint, ContainerEndpoint, null,
        Bucket, AccessKey, SecretKey, S3PathStyle: true,
        "/backup-staging", null, null, null,
        WalVerifyIntervalSec: 30, WalLagMaxSegments: 1024, WalStaleSec: 300);

    public async ValueTask DisposeAsync()
    {
        if (_minio is not null)
            await _minio.DisposeAsync();
    }
}

[CollectionDefinition(Name)]
public sealed class MinioCollection : ICollectionFixture<MinioFixture>
{
    public const string Name = "minio";
}
