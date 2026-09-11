using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using PgWorker.Backups;
using PgWorker.IntegrationTests.E2e;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

/// <summary>
/// СОБСТВЕННЫЙ MinIO одного Fact (e2e-isolation §1/§3, docs/e2e-isolation.md):
/// guid-имя pgw-em-{guid}, динамический хост-порт, свой bucket — тесты не делят
/// хранилище ни с соседними классами, ни с чужими прогонами, посев одного теста
/// физически не виден другому. Готовность — wait-стратегия /minio/health/live;
/// bucket создаётся прямым AWSSDK-клиентом (создание bucket — не операция
/// подсистемы, spec §3.1). Teardown в DisposeAsync при ЛЮБОМ исходе (фикстура
/// в await using тела Fact) + АССЕРТ ЧИСТОТЫ: pgw-em-{guid} отсутствует в
/// docker ps -a (чужие pgw-* не трогаем и в ассерт не включаем).
/// </summary>
public sealed class OwnMinio : IAsyncDisposable
{
    public const string AccessKey = "minioadmin";
    public const string SecretKey = "minioadmin";

    /// <summary>Bucket своего окружения (внутри pgw-em-{guid} имя может быть
    /// константным — снаружи контейнер не виден никому).</summary>
    public const string Bucket = "pgw-backups-test";

    private const string Image = "minio/minio:RELEASE.2025-09-07T16-13-09Z";

    private readonly IContainer _container;

    private OwnMinio(string slug, string runId, IContainer container)
    {
        Slug = slug;
        RunId = runId;
        _container = container;
    }

    public string Slug { get; }

    /// <summary>Идентификатор прогона (полный guid): имя контейнера pgw-em-{guid};
    /// никаких константных имён окружений.</summary>
    public string RunId { get; }

    private string ContainerName => $"pgw-em-{RunId}";

    /// <summary>Endpoint для ХОСТА-клиента (тест): localhost:<динамический порт>.</summary>
    public string HostEndpoint { get; private set; } = "";

    /// <summary>Endpoint для КОНТЕЙНЕРОВ (агент): host.docker.internal:<тот же порт>.</summary>
    public string ContainerEndpoint { get; private set; } = "";

    /// <summary>Подъём своего MinIO + bucket: pgw-em-{guid}, порт
    /// assignRandomHostPort (динамические порты везде), готовность —
    /// wait-стратегия health/live (бюджет 45 c: падаем быстро, не висим).</summary>
    public static async Task<OwnMinio> StartAsync(string slug, CancellationToken ct = default)
    {
        var runId = Guid.NewGuid().ToString("N");
        var container = new ContainerBuilder(Image)
            .WithName($"pgw-em-{runId}")
            .WithCommand("server", "/data")
            .WithEnvironment("MINIO_ROOT_USER", AccessKey)
            .WithEnvironment("MINIO_ROOT_PASSWORD", SecretKey)
            .WithPortBinding(9000, assignRandomHostPort: true)
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(
                    request => request.ForPort(9000).ForPath("/minio/health/live"),
                    wait => wait.WithTimeout(TimeSpan.FromSeconds(45))))
            .Build();
        await container.StartAsync(ct);
        var fx = new OwnMinio(slug, runId, container)
        {
            HostEndpoint = $"http://localhost:{container.GetMappedPublicPort(9000)}",
            ContainerEndpoint = $"http://host.docker.internal:{container.GetMappedPublicPort(9000)}",
        };
        await fx.EnsureBucketAsync(ct);
        return fx;
    }

    // Bucket per-окружение — прямой AWSSDK-клиент (тестовая утилита фикстуры).
    private async Task EnsureBucketAsync(CancellationToken ct)
    {
        var client = new AmazonS3Client(
            new BasicAWSCredentials(AccessKey, SecretKey),
            new AmazonS3Config { ServiceURL = HostEndpoint, ForcePathStyle = true });
        try
        {
            await client.PutBucketAsync(new PutBucketRequest { BucketName = Bucket }, ct);
        }
        finally
        {
            client.Dispose();
        }
    }

    /// <summary>Runtime-опции подсистемы на своё окружение (хост-клиент).</summary>
    public BackupsRuntimeOptions Runtime() => new(
        Enabled: true,
        S3Endpoint: HostEndpoint,
        S3AdvertisedEndpoint: ContainerEndpoint,
        S3Bucket: Bucket,
        S3AccessKey: AccessKey,
        S3SecretKey: SecretKey,
        S3PathStyle: true,
        JobImage: "pgworker-backup:test",
        StagingDir: "/backup-staging",
        WalVerifyIntervalSec: 30,
        WalLagMaxSegments: 1024,
        WalStaleSec: 300);

    /// <summary>Teardown при любом исходе: стоп/rm СВОЕГО контейнера → АССЕРТ
    /// ЧИСТОТЫ: pgw-em-{guid} в docker ps -a отсутствует.</summary>
    public async ValueTask DisposeAsync()
    {
        try
        {
            await _container.DisposeAsync();
        }
        catch
        {
            // падение dispose не маскируем — ассерт чистоты ниже отчитается
        }

        var left = await E2eFixture.RunProcessAsync(
            "docker",
            ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name={ContainerName}"],
            CancellationToken.None);
        left.Should().BeEmpty(
            $"{Slug}: teardown окружения неполный — остался контейнер {ContainerName}");
    }
}
