using System.Diagnostics;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// СОБСТВЕННАЯ ownS3-нода одного тест-класса (t36: пул ownS3 — хранилище и есть
// эта нода, отдельный контейнер хранилища не поднимается; e2e-isolation §1/§3): guid-имя apm-s3-{guid} (префикс
// apm- отличает панельную серию от pgw-*), динамический хост-порт, свой bucket —
// сценарии панели не делят хранилище ни с соседними классами, ни с чужими
// прогонами: посев одного теста физически не виден другому. Образ — ВНЕШНИЙ
// owns3:dev (репозиторий ../OwnS3, сборка — deploy/build-image.sh там; в
// registry не кладётся) — проверка наличия раз на процесс. Готовность —
// wait-стратегия /healthz (бюджет
// 45 c — правило быстрых падений); bucket и сеяные объекты — прямым
// AWSSDK-клиентом (утилита теста, не операция грани: панель в S3 не пишет).
// Teardown в DisposeAsync при ЛЮБОМ исходе (IClassFixture/IAsyncLifetime) +
// АССЕРТ ЧИСТОТЫ: apm-s3-{guid} отсутствует в docker ps -a (чужие контейнеры
// не трогаем и в ассерт не включаем).
public sealed class S3NodeContainerFixture : IAsyncLifetime
{
    // Root-пара ownS3-ноды (fail-fast старта ownS3) — ею же сеет тест и читает
    // панель (AdminPanel__Backups:S3:* в BackupsWebFactory). Генерируется в
    // рантайме: секреты не зашиваются в исходник; одна пара на процесс —
    // переиспользуется всеми обращениями.
    public static readonly string AccessKey = $"apm-owns3-{ShortToken()}";
    public static readonly string SecretKey = $"apm-owns3-root-{ShortToken()}-{ShortToken()}";

    private static string ShortToken()
    {
        // Криптостойкий генератор: 20 символов букв/цифр (формат ownS3-кредов).
        const string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        var bytes = System.Security.Cryptography.RandomNumberGenerator.GetBytes(20);
        return new string(bytes.Select(b => alphabet[b % alphabet.Length]).ToArray());
    }

    public const string Image = "owns3:dev";

    private static readonly SemaphoreSlim ImageGate = new(1, 1);
    private static bool _imageReady;

    private readonly IContainer _container;

    public S3NodeContainerFixture()
    {
        RunId = Guid.NewGuid().ToString("N");
        Bucket = $"apm-backups-{RunId}";
        _container = new ContainerBuilder(Image)
            .WithName($"apm-s3-{RunId}")
            .WithEnvironment("OWNS3_ROOT_USER", AccessKey)
            .WithEnvironment("OWNS3_ROOT_PASSWORD", SecretKey)
            .WithPortBinding(9000, assignRandomHostPort: true)
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(
                    request => request.ForPort(9000).ForPath("/healthz"),
                    wait => wait.WithTimeout(TimeSpan.FromSeconds(45))))
            .Build();
    }

    /// <summary>Идентификатор окружения (полный guid): имя контейнера
    /// apm-s3-{guid} — никаких константных имён.</summary>
    public string RunId { get; }

    /// <summary>Bucket своего окружения (снаружи контейнер не виден никому).</summary>
    public string Bucket { get; }

    private string ContainerName => $"apm-s3-{RunId}";

    /// <summary>Endpoint для хост-клиентов (фабрика панели и AWSSDK-сеятель):
    /// localhost:&lt;динамический порт&gt; — литералов портов в тестах нет.</summary>
    public string HostEndpoint { get; private set; } = "";

    public async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        await EnsureImageAsync(ct);
        await _container.StartAsync(ct);
        HostEndpoint = $"http://localhost:{_container.GetMappedPublicPort(9000)}";
        await EnsureBucketAsync(ct);
    }

    // Внешний образ owns3:dev (репозиторий ../OwnS3, сборка —
    // deploy/build-image.sh там; pg его не собирает) — проверка наличия
    // раз на процесс, fail-fast с командой сборки.
    private static async Task EnsureImageAsync(CancellationToken ct)
    {
        if (_imageReady)
            return;
        await ImageGate.WaitAsync(ct);
        try
        {
            if (_imageReady)
                return;
            // Список аргументов без shell: имя образа не попадает в строку команды.
            var inspect = new ProcessStartInfo("docker")
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
            };
            inspect.ArgumentList.Add("image");
            inspect.ArgumentList.Add("inspect");
            inspect.ArgumentList.Add(Image);
            using var process = new Process { StartInfo = inspect };
            process.Start();
            await process.WaitForExitAsync(ct);
            if (process.ExitCode != 0)
                throw new ApplicationException(
                    "image owns3:dev is missing — build it in ../OwnS3: bash ../OwnS3/deploy/build-image.sh");
            _imageReady = true;
        }
        finally
        {
            ImageGate.Release();
        }
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

    /// <summary>Остановка своей ownS3-ноды — сценарий «S3-нода недоступен».</summary>
    public async Task StopAsync(CancellationToken ct = default)
        => await _container.StopAsync(ct);

    // Teardown при любом исходе: стоп/rm СВОЕГО контейнера → АССЕРТ ЧИСТОТЫ:
    // apm-s3-{guid} в docker ps -a отсутствует.
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
            $"teardown is incomplete — container {ContainerName} remains");
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
        using var process = new Process { StartInfo = psi };
        process.Start();
        var output = await process.StandardOutput.ReadToEndAsync();
        await process.WaitForExitAsync();
        return output.Trim();
    }
}
