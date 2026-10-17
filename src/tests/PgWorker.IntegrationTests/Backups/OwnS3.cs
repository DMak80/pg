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
    // Root-пара ownS3-ноды (fail-fast старта) — ею же сеет тест.
    public const string AccessKey = "owns3-test-root";
    public const string SecretKey = "owns3-test-root-pwd";

    /// <summary>Bucket своего окружения (внутри pgw-em-{guid} имя может быть
    /// константным — снаружи контейнер не виден никому).</summary>
    public const string Bucket = "pgw-backups-test";

    // Локальная сборка ownS3 (publish-COPY, канон E2E-образов; в registry
    // не кладётся) — та же, что у ноды пула E2E-фикстуры.
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

    // Локальная сборка ownS3 (publish НА ХОСТЕ → образ publish-COPY; канон
    // E2E-образов, в registry не кладётся) — один раз на процесс.
    private static async Task EnsureImageAsync(CancellationToken ct)
    {
        if (_imageReady)
            return;
        await ImageGate.WaitAsync(ct);
        try
        {
            if (_imageReady)
                return;
            var root = FindRepoRoot(AppContext.BaseDirectory);
            var outDir = Path.Combine(root, "artifacts", "e2e", "owns3");
            using var publish = Process.Start(new ProcessStartInfo("dotnet",
                ["publish", Path.Combine(root, "src", "OwnS3.App", "OwnS3.App.csproj"),
                    "-c", "Release", "-o", outDir, "--nologo"])
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            })!;
            var publishLog = await publish.StandardOutput.ReadToEndAsync() +
                             await publish.StandardError.ReadToEndAsync();
            await publish.WaitForExitAsync(ct);
            if (publish.ExitCode != 0)
                throw new ApplicationException(
                    $"publish OwnS3.App не удался: {Tail(publishLog)}");
            using var build = Process.Start(new ProcessStartInfo("docker",
                ["build", "-f", Path.Combine(root, "docker", "OwnS3.Dockerfile"),
                    "-t", Image, outDir])
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            })!;
            var buildLog = await build.StandardOutput.ReadToEndAsync() +
                           await build.StandardError.ReadToEndAsync();
            await build.WaitForExitAsync(ct);
            if (build.ExitCode != 0)
                throw new ApplicationException($"build {Image} не удался: {Tail(buildLog)}");
            _imageReady = true;
        }
        finally
        {
            ImageGate.Release();
        }
    }

    // Хвост лога без исключения: ^2000.. на логе короче 2000 символов бросает
    // ArgumentOutOfRangeException и МАСКИРУЕТ реальную ошибку (прецедент
    // мерж-гейта t36 — MSB1009 не был виден в упавших интеграциях).
    private static string Tail(string log)
        => log.Length <= 2000 ? log : log[^2000..];

    // Корень репозитория — подъём от каталога сборки (образец E2eFixture.FindRoot).
    private static string FindRepoRoot(string start)
    {
        // Маркер КОРНЯ репозитория (docker/node/Dockerfile) — как E2eFixture.FindRoot:
        // PgWorker.slnx лежит в src/ — по нему корень резолвится в src (прецедент
        // мерж-гейта t36: publish MSB1009 по несуществующему src/src/…-пути).
        var dir = new DirectoryInfo(start);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "docker", "node", "Dockerfile")))
            dir = dir.Parent;
        return dir?.FullName
            ?? throw new ApplicationException("корень репозитория (docker/node/Dockerfile) не найден");
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
            $"{Slug}: teardown окружения неполный — остался контейнер {ContainerName}");
    }
}
