using System.Diagnostics;
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
/// СОБСТВЕННАЯ ownS3-нода одного Fact (t36: хранилище — пул ownS3; e2e-isolation
/// §1/§3, docs/e2e-isolation.md): guid-имя pgw-eo-{guid}, динамический хост-порт,
/// свой bucket — тесты не делят хранилище ни с соседними классами, ни с чужими
/// прогонами, посев одного теста физически не виден другому. Готовность —
/// wait-стратегия /healthz; bucket создаётся прямым AWSSDK-клиентом (создание
/// bucket — не операция подсистемы, spec §3.1). Teardown в DisposeAsync при
/// ЛЮБОМ исходе (фикстура в await using тела Fact) + АССЕРТ ЧИСТОТЫ:
/// pgw-eo-{guid} отсутствует в docker ps -a (чужие pgw-* не трогаем и в ассерт
/// не включаем).
/// </summary>
public sealed class OwnS3 : IAsyncDisposable
{
    // Root-пара ownS3-ноды (fail-fast старта) — ею же сеет тест. Генерируется
    // в рантайме: секреты не зашиваются в исходник; одна пара на процесс.
    public static readonly string AccessKey = $"owns3-test-root-{ShortToken()}";
    public static readonly string SecretKey = $"owns3-test-root-pwd-{ShortToken()}-{ShortToken()}";

    private static string ShortToken()
    {
        // Криптостойкий генератор: 20 символов букв/цифр (формат ownS3-кредов).
        const string alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        var bytes = System.Security.Cryptography.RandomNumberGenerator.GetBytes(20);
        return new string(bytes.Select(b => alphabet[b % alphabet.Length]).ToArray());
    }

    /// <summary>Bucket своего окружения (внутри pgw-em-{guid} имя может быть
    /// константным — снаружи контейнер не виден никому).</summary>
    public const string Bucket = "pgw-backups-test";

    // Внешний образ owns3:dev (репозиторий ../OwnS3) — тот же, что у ноды
    // пула E2E-фикстуры.
    private const string Image = E2eEnvironment.OwnS3Image;

    private static readonly SemaphoreSlim ImageGate = new(1, 1);
    private static bool _imageReady;

    private readonly IContainer _container;

    private OwnS3(string slug, string runId, IContainer container)
    {
        Slug = slug;
        RunId = runId;
        _container = container;
    }

    public string Slug { get; }

    /// <summary>Идентификатор прогона (полный guid): имя контейнера pgw-eo-{guid};
    /// никаких константных имён окружений.</summary>
    public string RunId { get; }

    private string ContainerName => $"pgw-eo-{RunId}";

    /// <summary>Endpoint для ХОСТА-клиента (тест): localhost:<динамический порт>.</summary>
    public string HostEndpoint { get; private set; } = "";

    /// <summary>Endpoint для КОНТЕЙНЕРОВ (агент): host.docker.internal:<тот же порт>.</summary>
    public string ContainerEndpoint { get; private set; } = "";

    /// <summary>Подъём своей ownS3-ноды + bucket: pgw-eo-{guid}, порт
    /// assignRandomHostPort (динамические порты везде), готовность —
    /// wait-стратегия /healthz (бюджет 45 c: падаем быстро, не висим).</summary>
    public static async Task<OwnS3> StartAsync(string slug, CancellationToken ct = default)
    {
        await EnsureImageAsync(ct);
        var runId = Guid.NewGuid().ToString("N");
        var container = new ContainerBuilder(Image)
            .WithName($"pgw-eo-{runId}")
            .WithEnvironment("OWNS3_ROOT_USER", AccessKey)
            .WithEnvironment("OWNS3_ROOT_PASSWORD", SecretKey)
            .WithPortBinding(9000, assignRandomHostPort: true)
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(
                    request => request.ForPort(9000).ForPath("/healthz"),
                    wait => wait.WithTimeout(TimeSpan.FromSeconds(45))))
            .Build();
        await container.StartAsync(ct);
        var fx = new OwnS3(slug, runId, container)
        {
            HostEndpoint = $"http://localhost:{container.GetMappedPublicPort(9000)}",
            ContainerEndpoint = $"http://host.docker.internal:{container.GetMappedPublicPort(9000)}",
        };
        await fx.EnsureBucketAsync(ct);
        return fx;
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

    /// <summary>S3-цель своего окружения (t36: S3ClientOptions для BackupS3).</summary>
    public S3ClientOptions S3() => new(HostEndpoint, Bucket, AccessKey, SecretKey);

    /// <summary>Runtime-опции подсистемы на своё окружение (bucket per-install).</summary>
    public BackupsRuntimeOptions Runtime() => new(
        Enabled: true,
        S3Bucket: Bucket,
        JobImage: "pgworker-backup:test",
        StagingDir: "/backup-staging",
        WalVerifyIntervalSec: 30,
        WalLagMaxSegments: 1024,
        WalStaleSec: 300);

    /// <summary>Тестовый обрыв S3 (AC6): docker pause — put/list зависают, контейнер жив.
    /// </summary>
    public async Task PauseAsync(CancellationToken ct = default)
        => await _container.PauseAsync(ct);

    /// <summary>Восстановление S3 после паузы (AC6).</summary>
    public async Task UnpauseAsync(CancellationToken ct = default)
        => await _container.UnpauseAsync(ct);

    /// <summary>Teardown при любом исходе: стоп/rm СВОЕГО контейнера → АССЕРТ
    /// ЧИСТОТЫ: pgw-eo-{guid} в docker ps -a отсутствует.</summary>
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
            $"{Slug}: the environment teardown is incomplete — container {ContainerName} is left");
    }
}
