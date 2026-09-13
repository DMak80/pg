using System.Diagnostics;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// СОБСТВЕННЫЙ MinIO одного тест-класса (t08, план Task 9; e2e-isolation §1/§3,
// образец OwnMinio PgWorker.IntegrationTests): guid-имя apm-minio-{guid} (префикс
// apm- отличает панельную серию от pgw-*), динамический хост-порт, свой bucket —
// сценарии панели не делят хранилище ни с соседними классами, ни с чужими
// прогонами: посев одного теста физически не виден другому. Готовность —
// wait-стратегия /minio/health/live (бюджет 45 c — правило быстрых падений);
// bucket и сеяные объекты — прямым AWSSDK-клиентом (утилита теста, не операция
// грани: панель в S3 не пишет). Teardown в DisposeAsync при ЛЮБОМ исходе
// (IClassFixture/IAsyncLifetime) + АССЕРТ ЧИСТОТЫ: apm-minio-{guid} отсутствует
// в docker ps -a (чужие контейнеры не трогаем и в ассерт не включаем).
public sealed class MinioContainerFixture : IAsyncLifetime
{
    public const string AccessKey = "minioadmin";
    public const string SecretKey = "minioadmin";

    // Тот же образ, что у OwnMinio (уже зеркалирован в локальный registry —
    // новых внешних образов серия не тянет, docs/runbook.md).
    private const string Image = "minio/minio:RELEASE.2025-09-07T16-13-09Z";

    private readonly IContainer _container;

    public MinioContainerFixture()
    {
        RunId = Guid.NewGuid().ToString("N");
        Bucket = $"apm-backups-{RunId}";
        _container = new ContainerBuilder(Image)
            .WithName($"apm-minio-{RunId}")
            .WithCommand("server", "/data")
            .WithEnvironment("MINIO_ROOT_USER", AccessKey)
            .WithEnvironment("MINIO_ROOT_PASSWORD", SecretKey)
            .WithPortBinding(9000, assignRandomHostPort: true)
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(
                    request => request.ForPort(9000).ForPath("/minio/health/live"),
                    wait => wait.WithTimeout(TimeSpan.FromSeconds(45))))
            .Build();
    }

    /// <summary>Идентификатор окружения (полный guid): имя контейнера
    /// apm-minio-{guid} — никаких константных имён.</summary>
    public string RunId { get; }

    /// <summary>Bucket своего окружения (снаружи контейнер не виден никому).</summary>
    public string Bucket { get; }

    private string ContainerName => $"apm-minio-{RunId}";

    /// <summary>Endpoint для хост-клиентов (фабрика панели и AWSSDK-сеятель):
    /// localhost:&lt;динамический порт&gt; — литералов портов в тестах нет.</summary>
    public string HostEndpoint { get; private set; } = "";

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await _container.StartAsync(ct);
        HostEndpoint = $"http://localhost:{_container.GetMappedPublicPort(9000)}";
        await EnsureBucketAsync(ct);
    }

    // Bucket per-окружение — прямой AWSSDK-клиент (создание bucket — не операция грани).
    private async Task EnsureBucketAsync(CancellationToken ct)
    {
        using var client = NewClient();
        await client.PutBucketAsync(new PutBucketRequest { BucketName = Bucket }, ct);
    }

    private AmazonS3Client NewClient() => new(
        new BasicAWSCredentials(AccessKey, SecretKey),
        new AmazonS3Config { ServiceURL = HostEndpoint, ForcePathStyle = true });

    /// <summary>Посев объектов сценария — прямой AWSSDK-клиент (put — утилита
    /// теста; содержимое ASCII, размер в байтах = длине строки).</summary>
    public async Task SeedObjectsAsync(
        IReadOnlyList<(string Key, string Content)> objects, CancellationToken ct)
    {
        using var client = NewClient();
        foreach (var (key, content) in objects)
        {
            await client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = Bucket,
                Key = key,
                ContentBody = content,
                ContentType = "application/octet-stream",
            }, ct);
        }
    }

    /// <summary>Остановка своего MinIO — сценарий AC3 «MinIO недоступен».</summary>
    public async Task StopAsync(CancellationToken ct = default)
        => await _container.StopAsync(ct);

    // Teardown при любом исходе: стоп/rm СВОЕГО контейнера → АССЕРТ ЧИСТОТЫ:
    // apm-minio-{guid} в docker ps -a отсутствует.
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

        var left = await DockerPsNamesAsync(ContainerName);
        left.Should().BeEmpty(
            $"teardown окружения неполный — остался контейнер {ContainerName}");
    }

    // docker ps -a --format {{.Names}} --filter name=<filter> (список имён, trim).
    private static async Task<string> DockerPsNamesAsync(string nameFilter)
    {
        var psi = new ProcessStartInfo("docker")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        psi.ArgumentList.Add("ps");
        psi.ArgumentList.Add("-a");
        psi.ArgumentList.Add("--format");
        psi.ArgumentList.Add("{{.Names}}");
        psi.ArgumentList.Add("--filter");
        psi.ArgumentList.Add($"name={nameFilter}");
        using var process = Process.Start(psi)!;
        var output = await process.StandardOutput.ReadToEndAsync();
        await process.WaitForExitAsync();
        return output.Trim();
    }
}
