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
// прогонами: посев одного теста физически не виден другому. Образ — owns3:e2e,
// ЛОКАЛЬНАЯ сборка publish-COPY (канон E2E-образов; в registry не кладётся) —
// лениво, один раз на процесс. Готовность — wait-стратегия /healthz (бюджет
// 45 c — правило быстрых падений); bucket и сеяные объекты — прямым
// AWSSDK-клиентом (утилита теста, не операция грани: панель в S3 не пишет).
// Teardown в DisposeAsync при ЛЮБОМ исходе (IClassFixture/IAsyncLifetime) +
// АССЕРТ ЧИСТОТЫ: apm-s3-{guid} отсутствует в docker ps -a (чужие контейнеры
// не трогаем и в ассерт не включаем).
public sealed class S3NodeContainerFixture : IAsyncLifetime
{
    // Root-пара ownS3-ноды (fail-fast старта ownS3) — ею же сеет тест и читает
    // панель (AdminPanel__Backups:S3:* в BackupsWebFactory).
    public const string AccessKey = "apm-owns3-root";
    public const string SecretKey = "apm-owns3-root-pwd";

    public const string Image = "owns3:e2e";

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

    // Локальная сборка owns3:e2e (publish НА ХОСТЕ → образ publish-COPY;
    // канон E2E-образов, в registry не кладётся) — один раз на процесс.
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

            var publish = new ProcessStartInfo("dotnet")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            publish.ArgumentList.Add("publish");
            publish.ArgumentList.Add(Path.Combine(root, "src", "OwnS3.App", "OwnS3.App.csproj"));
            publish.ArgumentList.Add("-c");
            publish.ArgumentList.Add("Release");
            publish.ArgumentList.Add("-o");
            publish.ArgumentList.Add(outDir);
            publish.ArgumentList.Add("--nologo");
            await RunToolAsync(publish, "publish OwnS3.App", ct);

            var build = new ProcessStartInfo("docker")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            build.ArgumentList.Add("build");
            build.ArgumentList.Add("-f");
            build.ArgumentList.Add(Path.Combine(root, "docker", "OwnS3.Dockerfile"));
            build.ArgumentList.Add("-t");
            build.ArgumentList.Add(Image);
            build.ArgumentList.Add(outDir);
            await RunToolAsync(build, $"docker build {Image}", ct);

            _imageReady = true;
        }
        finally
        {
            ImageGate.Release();
        }
    }

    // Запуск host-инструмента: ОБА перенаправленных пайпа дренируются
    // ПАРАЛЛЕЛЬНО + бюджет 5 мин с Kill всего дерева процессов.
    // Последовательные await ReadToEndAsync(stdout)-then-stderr — классический
    // pipe-deadlock: ребёнок блокируется на заполненном stderr, родитель ждёт
    // конца stdout — оба висят вечно (инцидент мерж-гейта t36: «зависший»
    // старт интеграций при сборке owns3:e2e). Таймаут — правило быстрых падений.
    private static async Task RunToolAsync(ProcessStartInfo psi, string what, CancellationToken ct)
    {
        var started = Stopwatch.GetTimestamp();
        using var p = Process.Start(psi)!;
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromMinutes(5));
        var stdoutTask = p.StandardOutput.ReadToEndAsync(timeout.Token);
        var stderrTask = p.StandardError.ReadToEndAsync(timeout.Token);
        try
        {
            await p.WaitForExitAsync(timeout.Token);
            var log = (await stdoutTask) + (await stderrTask);
            if (p.ExitCode != 0)
                throw new ApplicationException($"{what} не удался (код {p.ExitCode}): {Tail(log)}");
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            p.Kill(entireProcessTree: true);
            throw new ApplicationException($"{what} превысил бюджет 5 мин — процесс убит деревом");
        }
        Console.WriteLine($"[PHASE] {what}: {Stopwatch.GetElapsedTime(started).TotalSeconds:F1} с");
    }

    // Хвост лога без исключения: ^2000.. на коротком логе маскирует ошибку
    // ArgumentOutOfRangeException'ом (прецедент мерж-гейта t36 — MSB1009
    // не был виден в упавших интеграциях).
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
