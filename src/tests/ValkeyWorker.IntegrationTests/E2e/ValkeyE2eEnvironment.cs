using System.Diagnostics;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Shared.Etcd.Client;
using ValkeyWorker.IntegrationTests.Valkey;
using Xunit;

namespace ValkeyWorker.IntegrationTests.E2e;

/// <summary>
/// Изолированное docker-E2E окружение ValkeyWorker (docs/e2e-isolation.md +
/// docs/e2e-launch.md): собственная docker-сеть vwk-en-{runId}, свой etcd
/// vwk-ee-{runId}, свой воркер-контейнер vwk-ew-{runId} (образ
/// valkeyworker:e2e — свежий Release из docker/ValkeyWorker.Dockerfile).
/// Идентификатор прогона — полный guid во всех именах; чистка и ассерт чистоты
/// — строго по нему. Телеметрия: docker-логи/inspect всех контейнеров
/// окружения снимаются в /tmp/pgw-e2e-artifacts-{runId}/ ДО удаления;
/// MarkFailed() → teardown ОСТАНАВЛИВАЕТ контейнеры, но не удаляет
/// (README-cleanup.txt с own-only командами). Retry старта против гонок prune.
/// Гейт запуска: PGW_TEST_DOCKER=1 (нет — Skip); PGW_TEST_E2E_NOBUILD=1 —
/// пропустить сборку образа (только бисект).
/// </summary>
public sealed class ValkeyE2eEnvironment : IAsyncDisposable
{
    private const string EtcdImage = "quay.io/coreos/etcd:v3.5.21";
    private const string WorkerImage = "valkeyworker:e2e";

    private readonly IContainer _etcd;
    private readonly IContainer _worker;
    private readonly IContainer? _worker2; // двухинстансный контур (StartTwoAsync)
    private readonly IContainer[] _workers; // воркеры окружения: 1 или 2 (teardown/телеметрия)
    private readonly string[] _workerNames; // docker-имена воркеров (ассерт чистоты)
    private readonly INetwork _net;
    private readonly HttpClient _gatewayHttp = new();

    private bool _failed;

    private readonly string _runId;
    private readonly int _api1Port;
    private readonly int? _api2Port; // двухинстансный контур; null — одиночный

    private ValkeyE2eEnvironment(
        string slug, string runId, string netName, INetwork net, IContainer etcd, IContainer worker,
        string etcdEndpoint, string artifactsDir, string apiBaseUrl, string clientPem, string clientKeyPem,
        IContainer? worker2 = null, int? api2Port = null)
    {
        Slug = slug;
        _runId = runId;
        NetName = netName;
        _net = net;
        _etcd = etcd;
        _worker = worker;
        _worker2 = worker2;
        _workers = worker2 is null ? [worker] : [worker, worker2];
        _workerNames = worker2 is null
            ? [$"vwk-ew-{runId}"]
            : [$"vwk-ew1-{runId}", $"vwk-ew2-{runId}"];
        EtcdEndpoint = etcdEndpoint;
        ArtifactsDir = artifactsDir;
        ApiBaseUrl = apiBaseUrl;
        Gateway = new EtcdGateway(_gatewayHttp);
        (ClientPem, ClientKeyPem) = (clientPem, clientKeyPem);
        _api1Port = new Uri(apiBaseUrl).Port;
        _api2Port = api2Port;
    }

    public string Slug { get; }

    public string ArtifactsDir { get; }

    /// <summary>etcd окружения: published порт на хосте.</summary>
    public string EtcdEndpoint { get; }

    /// <summary>База API воркера (https://localhost:{динамический порт}).</summary>
    public string ApiBaseUrl { get; }

    // ── Двухинстансный контур (StartTwoAsync): базовые URL/порты/имена ОБИХ
    // воркеров — основа резолва держателя клэйма (порт из claims.instance →
    // api-ключ → url → сопоставление с Api1/Api2Port). В одиночном контуре
    // (StartAsync) Api1* дублирует ApiBaseUrl, вторые члены — исключение.

    /// <summary>База API первого воркера (vwk-ew1): в одиночном — сам контур.</summary>
    public string Api1BaseUrl => ApiBaseUrl;

    /// <summary>Хост-порт API первого воркера (резолв держателя клэйма).</summary>
    public int Api1Port => _api1Port;

    /// <summary>База API второго воркера (vwk-ew2) — только StartTwoAsync.</summary>
    public string Api2BaseUrl
        => _api2Port is { } port
            ? $"https://localhost:{port}"
            : throw new InvalidOperationException("второго API нет: окружение поднято StartAsync (одиночный контур)");

    /// <summary>Хост-порт API второго воркера — только StartTwoAsync.</summary>
    public int Api2Port
        => _api2Port
           ?? throw new InvalidOperationException("второго API нет: окружение поднято StartAsync (одиночный контур)");

    /// <summary>Docker-имя первого воркера (vwk-ew1-{runId}; одиночный — vwk-ew).</summary>
    public string Worker1Name => _workerNames[0];

    /// <summary>Docker-имя второго воркера (vwk-ew2-{runId}) — только StartTwoAsync.</summary>
    public string Worker2Name
        => _worker2 is null
            ? throw new InvalidOperationException("второго воркера нет: окружение поднято StartAsync (одиночный контур)")
            : _workerNames[1];

    public string ClusterTag => _runId[..8];

    public string NetName { get; }

    public string ClientPem { get; }

    public string ClientKeyPem { get; }

    public EtcdGateway Gateway { get; }

    public void MarkFailed() => _failed = true;

    /// <summary>Подъём окружения (3 попытки — против гонок глобального prune).</summary>
    public static async Task<ValkeyE2eEnvironment> StartAsync(string slug)
    {
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
            throw new InvalidOperationException("E2E требует PGW_TEST_DOCKER=1 (иначе Skip в тесте)");

        var runId = Guid.NewGuid().ToString("N");
        var artifactsDir = Path.Combine(Path.GetTempPath(), $"pgw-e2e-artifacts-{runId}");
        Directory.CreateDirectory(Path.Combine(artifactsDir, "tls"));

        // Статический ассет процесса: образ valkeyworker:e2e (Release из Dockerfile).
        await BuildImageAsync();

        // TLS-пакет окружения: self-signed CA + server + client (PEM в артефакты).
        var (caPem, caKeyPem) = GenerateCa("vwk-e2e");
        var (serverCert, serverKey) = IssueCert(caPem, caKeyPem, "valkeyworker");
        var (clientCert, clientKey) = IssueCert(caPem, caKeyPem, "e2e-client");
        var tlsDir = Path.Combine(artifactsDir, "tls");
        await File.WriteAllTextAsync(Path.Combine(tlsDir, "ca.pem"), caPem);
        await File.WriteAllTextAsync(Path.Combine(tlsDir, "server.crt"), serverCert);
        await File.WriteAllTextAsync(Path.Combine(tlsDir, "server.key"), serverKey);
        await File.WriteAllTextAsync(Path.Combine(tlsDir, "client.key"), clientKey);

        for (var attempt = 1; ; attempt++)
        {
            var netName = $"vwk-en-{runId}";
            var net = new NetworkBuilder().WithName(netName).Build();
            var etcdPort = FreePort();
            var etcd = new ContainerBuilder(EtcdImage)
                .WithName($"vwk-ee-{runId}")
                .WithPortBinding(etcdPort, 2379)
                .WithCommand("etcd", "--name=test", "--data-dir=/etcd-data",
                    "--listen-client-urls=http://0.0.0.0:2379",
                    "--advertise-client-urls=http://127.0.0.1:2379")
                .Build();
            var apiPort = FreePort();
            var portRange = ValkeyWorker.IntegrationTests.Valkey.FreePortWindow.Find();
            var worker = new ContainerBuilder(WorkerImage)
                .WithName($"vwk-ew-{runId}")
                .WithNetwork(net)
                .WithPortBinding(apiPort, 8080)
                .WithBindMount("/var/run/docker.sock", "/var/run/docker.sock")
                .WithBindMount(tlsDir, "/tls")
                .WithEnvironment(new Dictionary<string, string>
                {
                    ["ValkeyWorker__Etcd__Endpoints__0"] = $"http://host.docker.internal:{etcdPort}",
                    ["ValkeyWorker__AdvertisedClientHost"] = "host.docker.internal",
                    ["ValkeyWorker__Api__AdvertiseUrl"] = $"https://host.docker.internal:{apiPort}",
                    ["ValkeyWorker__Docker__PortRange__From"] = portRange.From.ToString(),
                    ["ValkeyWorker__Docker__PortRange__To"] = (portRange.From + 64).ToString(),
                    ["VWK_API_TLS_CERT_PATH"] = "/tls/server.crt",
                    ["VWK_API_TLS_KEY_PATH"] = "/tls/server.key",
                    ["VWK_API_TLS_CLIENT_CA_PATH"] = "/tls/ca.pem",
                })
                .WithExtraHost("host.docker.internal", "host-gateway")
                .Build();

            try
            {
                await net.CreateAsync();
                await etcd.StartAsync();
                await worker.StartAsync();
                var environment = new ValkeyE2eEnvironment(slug, runId, netName, net, etcd, worker,
                    $"http://localhost:{etcdPort}", artifactsDir,
                    $"https://localhost:{apiPort}", clientCert, clientKey);
                await environment.WriteAsync("README-cleanup.txt",
                    "# Зачистка окружения прогона (own-only):\n"
                    + $"docker rm -f vwk-ew-{runId} vwk-ee-{runId}\n"
                    + $"docker ps -aq --filter name=vwk-{environment.ClusterTag} | xargs -r docker rm -f\n"
                    + $"docker network rm {netName}\n");
                return environment;
            }
            catch (Exception ex) when (attempt < 3
                && ex.Message.Contains("network", StringComparison.OrdinalIgnoreCase))
            {
                // ретрай только на распознанную гонку сети — подъём заново
                await SafeStopAsync(worker, remove: true);
                await SafeStopAsync(etcd, remove: true);
                try { await net.DeleteAsync(); } catch { /* уже нет */ }
                continue;
            }
            catch (Exception)
            {
                await SafeStopAsync(worker, remove: true);
                await SafeStopAsync(etcd, remove: true);
                try { await net.DeleteAsync(); } catch { /* уже нет */ }
                throw;
            }
        }
    }

    /// <summary>Подъём ДВУХИНСТАНСНОГО контура (spec t20 §5.2): тот же каркас,
    /// что StartAsync (TLS-пакет, сеть, etcd, образ, retry), но ДВА контейнера
    /// vwk-ew1/ew2-{runId} с общими TLS/etcd/PortRange (гонки portalloc закрывает
    /// клэйм кластера) и УНИКАЛЬНЫМ Api__AdvertiseUrl каждому (одинаковые URL
    /// гасят оба дискавери-ключа). Ускоренные циклы/пороги — env-оверрайдами.
    /// Поведение одиночного StartAsync не меняется.</summary>
    public static async Task<ValkeyE2eEnvironment> StartTwoAsync(string slug)
    {
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
            throw new InvalidOperationException("E2E требует PGW_TEST_DOCKER=1 (иначе Skip в тесте)");

        var runId = Guid.NewGuid().ToString("N");
        var artifactsDir = Path.Combine(Path.GetTempPath(), $"pgw-e2e-artifacts-{runId}");
        Directory.CreateDirectory(Path.Combine(artifactsDir, "tls"));

        // Статический ассет процесса: образ valkeyworker:e2e (Release из Dockerfile).
        await BuildImageAsync();

        // TLS-пакет окружения: self-signed CA + server + client (PEM в артефакты);
        // пакет ОБЩИЙ для обоих инстансов — один mTLS-клиент годит обоим
        // (спека t20 §8 — mTLS-клиент к выжившему).
        var (caPem, caKeyPem) = GenerateCa("vwk-e2e");
        var (serverCert, serverKey) = IssueCert(caPem, caKeyPem, "valkeyworker");
        var (clientCert, clientKey) = IssueCert(caPem, caKeyPem, "e2e-client");
        var tlsDir = Path.Combine(artifactsDir, "tls");
        await File.WriteAllTextAsync(Path.Combine(tlsDir, "ca.pem"), caPem);
        await File.WriteAllTextAsync(Path.Combine(tlsDir, "server.crt"), serverCert);
        await File.WriteAllTextAsync(Path.Combine(tlsDir, "server.key"), serverKey);
        await File.WriteAllTextAsync(Path.Combine(tlsDir, "client.key"), clientKey);

        for (var attempt = 1; ; attempt++)
        {
            var netName = $"vwk-en-{runId}";
            var net = new NetworkBuilder().WithName(netName).Build();
            var etcdPort = FreePort();
            var etcd = new ContainerBuilder(EtcdImage)
                .WithName($"vwk-ee-{runId}")
                .WithPortBinding(etcdPort, 2379)
                .WithCommand("etcd", "--name=test", "--data-dir=/etcd-data",
                    "--listen-client-urls=http://0.0.0.0:2379",
                    "--advertise-client-urls=http://127.0.0.1:2379")
                .Build();
            var api1Port = FreePort();
            var api2Port = FreePort();
            var portRange = ValkeyWorker.IntegrationTests.Valkey.FreePortWindow.Find();

            IContainer WorkerContainer(int apiPort, string name)
                => new ContainerBuilder(WorkerImage)
                    .WithName(name)
                    .WithNetwork(net)
                    .WithPortBinding(apiPort, 8080)
                    .WithBindMount("/var/run/docker.sock", "/var/run/docker.sock")
                    .WithBindMount(tlsDir, "/tls")
                    .WithEnvironment(new Dictionary<string, string>
                    {
                        ["ValkeyWorker__Etcd__Endpoints__0"] = $"http://host.docker.internal:{etcdPort}",
                        ["ValkeyWorker__AdvertisedClientHost"] = "host.docker.internal",
                        // Уникальный AdvertiseUrl каждому инстансу (t07): одинаковые
                        // URL гасят оба дискавери-ключа.
                        ["ValkeyWorker__Api__AdvertiseUrl"] = $"https://host.docker.internal:{apiPort}",
                        ["ValkeyWorker__Docker__PortRange__From"] = portRange.From.ToString(),
                        ["ValkeyWorker__Docker__PortRange__To"] = (portRange.From + 64).ToString(),
                        ["VWK_API_TLS_CERT_PATH"] = "/tls/server.crt",
                        ["VWK_API_TLS_KEY_PATH"] = "/tls/server.key",
                        ["VWK_API_TLS_CLIENT_CA_PATH"] = "/tls/ca.pem",
                        // Ускоренные циклы/пороги takeover-сценария (spec §3 — env-оверрайды,
                        // §12-Р2): тик держателя 1 с, бут ноды ≤100 с.
                        ["ValkeyWorker__Loops__ScanIntervalSec"] = "1",
                        ["ValkeyWorker__Loops__KeepaliveSec"] = "1",
                        ["ValkeyWorker__Loops__ErrorDelayMs"] = "500",
                        ["ValkeyWorker__Thresholds__NodeBootSec"] = "100",
                    })
                    .WithExtraHost("host.docker.internal", "host-gateway")
                    .Build();

            var worker1 = WorkerContainer(api1Port, $"vwk-ew1-{runId}");
            var worker2 = WorkerContainer(api2Port, $"vwk-ew2-{runId}");

            try
            {
                await net.CreateAsync();
                await etcd.StartAsync();
                await worker1.StartAsync();
                await worker2.StartAsync();
                var environment = new ValkeyE2eEnvironment(slug, runId, netName, net, etcd, worker1,
                    $"http://localhost:{etcdPort}", artifactsDir,
                    $"https://localhost:{api1Port}", clientCert, clientKey,
                    worker2, api2Port);
                await environment.WriteAsync("README-cleanup.txt",
                    "# Зачистка окружения прогона (own-only):\n"
                    + $"docker rm -f vwk-ew1-{runId} vwk-ew2-{runId} vwk-ee-{runId}\n"
                    + $"docker ps -aq --filter name=vwk-{environment.ClusterTag} | xargs -r docker rm -f\n"
                    + $"docker volume ls -q --filter name=vwk-{environment.ClusterTag} | xargs -r docker volume rm -f\n"
                    + $"docker network rm {netName}\n");
                return environment;
            }
            catch (Exception ex) when (attempt < 3
                && ex.Message.Contains("network", StringComparison.OrdinalIgnoreCase))
            {
                // ретрай только на распознанную гонку сети — подъём заново
                await SafeStopAsync(worker2, remove: true);
                await SafeStopAsync(worker1, remove: true);
                await SafeStopAsync(etcd, remove: true);
                try { await net.DeleteAsync(); } catch { /* уже нет */ }
                continue;
            }
            catch (Exception)
            {
                await SafeStopAsync(worker2, remove: true);
                await SafeStopAsync(worker1, remove: true);
                await SafeStopAsync(etcd, remove: true);
                try { await net.DeleteAsync(); } catch { /* уже нет */ }
                throw;
            }
        }
    }

    private static async Task BuildImageAsync()
    {
        if (Environment.GetEnvironmentVariable("PGW_TEST_E2E_NOBUILD") == "1")
            return;
        var root = FindRepoRoot();
        var build = new ProcessStartInfo("docker",
            $"build -f docker/ValkeyWorker.Dockerfile -t {WorkerImage} .")
        {
            WorkingDirectory = root,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        using var process = Process.Start(build)!;
        await process.WaitForExitAsync();
        if (process.ExitCode != 0)
            throw new InvalidOperationException(
                $"сборка {WorkerImage} не удалась: {await process.StandardError.ReadToEndAsync()}");
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "docker", "ValkeyWorker.Dockerfile")))
            dir = dir.Parent;
        return dir?.FullName
            ?? throw new InvalidOperationException("корень репо (docker/ValkeyWorker.Dockerfile) не найден");
    }

    // ── Хелперы docker-CLI (канон KafkaE2eEnvironment.RunProcessAsync:
    // таймаут убивает дерево, вывод — в исключение) ──

    /// <summary>docker-CLI: вывод stdout; ненулевой exit/таймаут — исключение
    /// с выводом процесса (зависший процесс умирает по бюджету 2 мин).</summary>
    public Task<string> RunDockerAsync(IReadOnlyList<string> args, CancellationToken ct)
        => RunProcessAsync("docker", [.. args], ct, TimeSpan.FromMinutes(2));

    private static async Task<string> RunProcessAsync(
        string file, string[] args, CancellationToken ct, TimeSpan timeout)
    {
        var psi = new ProcessStartInfo(file, args)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        using var process = Process.Start(psi)
            ?? throw new ApplicationException($"не удалось запустить {file}");
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(timeout);
        // Оба потока параллельно и БЕЗ токена бюджета: при kill по бюджету пайпы
        // закрываются и накопленный вывод доступен (канон E2eFixture).
        var outTask = process.StandardOutput.ReadToEndAsync();
        var errTask = process.StandardError.ReadToEndAsync();
        try
        {
            await process.WaitForExitAsync(timeoutCts.Token);
            var output = await outTask;
            var error = await errTask;
            if (process.ExitCode != 0)
                throw new ApplicationException($"{file} {string.Join(' ', args)} → {process.ExitCode}: {error.Trim()}");
            return output.Trim();
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // Бюджет исчерпан: убиваем дерево, вывод — в диагностику.
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch
            {
                // процесс мог уже выйти
            }

            var drained = "";
            try
            {
                drained = string.Join("", await Task.WhenAll(outTask, errTask).WaitAsync(TimeSpan.FromSeconds(5)));
            }
            catch (TimeoutException)
            {
                // пайп завис — вывод недоступен
            }

            throw new ApplicationException(
                $"{file} {string.Join(' ', args)} не завершился за {timeout.TotalSeconds:0} c — убит; хвост:\n{drained}");
        }
    }

    // mTLS-клиент API (клиентский серт окружения — от per-install CA;
    // PFX round-trip — macOS SslStream требует экспортируемый ключ).
    public HttpClient CreateApiHttpClient()
    {
        var pemCert = X509Certificate2.CreateFromPem(ClientPem, ClientKeyPem);
        var clientCert = X509CertificateLoader.LoadPkcs12(
            pemCert.Export(X509ContentType.Pkcs12), null);
        return new HttpClient(new SocketsHttpHandler
        {
            SslOptions = new()
            {
                EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                ClientCertificates = [clientCert],
                RemoteCertificateValidationCallback = (_, _, _, _) => true, // self-signed e2e-CA
            },
        })
        { BaseAddress = new Uri(ApiBaseUrl) };
    }

    // Перегрузка для двухинстансного контура (спека t20 §8 — mTLS-клиент к выжившему): тот же mTLS-клиент
    // (TLS-пакет общий — один клиент годится ЛЮБОМУ из двух инстансов), но с
    // заданным BaseAddress (базовый URL выжившего резолвится по факту kill).
    public HttpClient CreateApiHttpClient(string baseUrl)
    {
        var pemCert = X509Certificate2.CreateFromPem(ClientPem, ClientKeyPem);
        var clientCert = X509CertificateLoader.LoadPkcs12(
            pemCert.Export(X509ContentType.Pkcs12), null);
        return new HttpClient(new SocketsHttpHandler
        {
            SslOptions = new()
            {
                EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                ClientCertificates = [clientCert],
                RemoteCertificateValidationCallback = (_, _, _, _) => true, // self-signed e2e-CA
            },
        })
        { BaseAddress = new Uri(baseUrl) };
    }

    // docker-факт: имя живого контейнера окружения (префикс/тег прогона).
    public async Task<bool> ContainerAliveAsync(string name)
    {
        var inspect = new ProcessStartInfo("docker", $"inspect --format {{{{.State.Running}}}} {name}")
        {
            RedirectStandardOutput = true,
        };
        using var process = Process.Start(inspect)!;
        var output = await process.StandardOutput.ReadToEndAsync();
        await process.WaitForExitAsync();
        return process.ExitCode == 0 && output.Trim() == "true";
    }

    // Args контейнера как compact-JSON ({{json .Args}}) — маркер TLS t06:
    // в args ноды обязаны быть --tls-port 6379/--port 0; пусто = объекта нет.
    // Шаблон В КАВЫЧКАХ: в нём пробел, а .NET режет Arguments по пробелам.
    public async Task<string> ContainerArgsJsonAsync(string name)
    {
        var inspect = new ProcessStartInfo("docker",
            $"inspect --format \"{{{{json .Args}}}}\" {name}")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        using var process = Process.Start(inspect)!;
        var output = await process.StandardOutput.ReadToEndAsync();
        await process.WaitForExitAsync();
        return process.ExitCode == 0 ? output.Trim() : string.Empty;
    }

    // Факт существования named volume (точное имя) — в env-модели используется
    // как проверка ОТСУТСТВИЯ тома (per-cluster TLS-тома не создаётся).
    public async Task<bool> VolumeExistsAsync(string name)
    {
        var inspect = new ProcessStartInfo("docker", $"volume inspect {name}")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        using var process = Process.Start(inspect)!;
        await process.StandardOutput.ReadToEndAsync();
        await process.WaitForExitAsync();
        return process.ExitCode == 0;
    }

    // Телеметрия (docs/e2e-launch.md §1): docker-логи+inspect всех контейнеров
    // окружения (воркеры — 1 или 2 + etcd) — ДО любого удаления; вызывается в
    // teardown и на slow-phase.
    public async Task CollectDiagnosticsAsync(string mark)
    {
        foreach (var name in _workerNames.Append($"vwk-ee-{_runId}"))
        {
            await ShellToFileAsync("docker", $"logs --tail 2000 {name}",
                Path.Combine(ArtifactsDir, $"container-{name}-{mark}.log"));
            await ShellToFileAsync("docker", $"inspect {name}",
                Path.Combine(ArtifactsDir, $"container-{name}-{mark}.json"));
        }

        // Ноды кластера сценария (vwk-<C>-*) — по тегу прогона.
        var ps = new ProcessStartInfo("docker",
            $"ps -a --format {{{{.Names}}}} --filter name=vwk-{ClusterTag}")
        { RedirectStandardOutput = true };
        using var listing = Process.Start(ps)!;
        var names = (await listing.StandardOutput.ReadToEndAsync())
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        await listing.WaitForExitAsync();
        await File.WriteAllLinesAsync(Path.Combine(ArtifactsDir, $"nodes-{mark}.txt"), names);
        foreach (var name in names)
            await ShellToFileAsync("docker", $"logs --tail 2000 {name}",
                Path.Combine(ArtifactsDir, $"node-{name}-{mark}.log"));
    }

    /// <summary>Отчёт по фазе (docs/e2e-launch.md §2): замер + строка [PHASE];
    /// фаза дольше 60 с — немедленный сбор логов в артефакты.</summary>
    public async Task<TimeSpan> WaitPhaseAsync(string phase, Func<Task<bool>> condition,
        TimeSpan budget, CancellationToken ct)
    {
        var started = Stopwatch.GetTimestamp();
        var deadline = DateTimeOffset.UtcNow + budget;
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (await condition())
            {
                var elapsed = Stopwatch.GetElapsedTime(started);
                await WriteAsync($"phase-{phase}.txt",
                    $"[PHASE] {phase}: ok=true, elapsed={elapsed.TotalSeconds:F1}s");
                if (elapsed.TotalSeconds > 60)
                    await CollectDiagnosticsAsync($"slow-phase-{phase}");
                return elapsed;
            }

            await Task.Delay(500, ct);
        }

        await CollectDiagnosticsAsync($"timeout-{phase}");
        throw new TimeoutException($"[PHASE] {phase}: бюджет {budget.TotalSeconds:F0} c исчерпан");
    }

    public async ValueTask DisposeAsync()
    {
        // Телеметрия ДО любого удаления (docs/e2e-launch.md §1).
        try
        {
            await CollectDiagnosticsAsync("teardown");
        }
        catch
        {
            // телеметрия — лучшее усилие
        }

        if (_failed)
        {
            // docs/e2e-launch.md §3: упавший сценарий — контейнеры ОСТАНОВИТЬ,
            // не удалить (тома/сети/etcd остаются для разбора).
            foreach (var worker in _workers)
                await SafeStopAsync(worker, remove: false);
            await SafeStopAsync(_etcd, remove: false);
            return;
        }

        foreach (var worker in _workers)
            await SafeStopAsync(worker, remove: true);
        await SafeStopAsync(_etcd, remove: true);
        try { await _net.DeleteAsync(); } catch { /* уже нет */ }
        _gatewayHttp.Dispose();

        // Ассерт чистоты: ни контейнера, ни сети, ни тома своего окружения
        // (томов тега быть не должно — per-cluster TLS-том в env-модели не
        // создаётся); двухинстансный контур проверяет ОБА имени воркеров.
        foreach (var name in _workerNames.Append($"vwk-ee-{_runId}"))
        {
            var inspect = new ProcessStartInfo("docker", $"inspect {name}")
            { RedirectStandardOutput = true, RedirectStandardError = true };
            using var process = Process.Start(inspect)!;
            await process.WaitForExitAsync();
            process.ExitCode.Should().NotBe(0, $"контейнер {name} удалён teardown'ом");
        }

        var volumeList = new ProcessStartInfo("docker",
            $"volume ls --format {{{{.Name}}}} --filter name=vwk-{ClusterTag}")
        { RedirectStandardOutput = true };
        using var volumeListing = Process.Start(volumeList)!;
        var volumeNames = await volumeListing.StandardOutput.ReadToEndAsync();
        await volumeListing.WaitForExitAsync();
        volumeNames.Trim().Should().BeEmpty($"после teardown не осталось томов тега {ClusterTag}");
    }

    private static async Task SafeStopAsync(IContainer container, bool remove)
    {
        try
        {
            await container.StopAsync();
        }
        catch
        {
            // уже остановлен/не стартовал
        }

        if (remove)
        {
            try
            {
                await container.DisposeAsync();
            }
            catch
            {
                // уже удалён
            }
        }
    }

    private async Task WriteAsync(string fileName, string content)
        => await File.WriteAllTextAsync(Path.Combine(ArtifactsDir, fileName), content);

    private async Task ShellToFileAsync(string program, string args, string path)
    {
        try
        {
            var startInfo = new ProcessStartInfo(program, args)
            { RedirectStandardOutput = true, RedirectStandardError = true };
            using var process = Process.Start(startInfo)!;
            var output = await process.StandardOutput.ReadToEndAsync();
            await process.WaitForExitAsync();
            await File.WriteAllTextAsync(path, output);
        }
        catch
        {
            // телеметрия — лучшее усилие
        }
    }

    private static int FreePort()
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    // ── TLS-генерация окружения (self-signed, минимум для e2e) ──

    private static (string CertPem, string KeyPem) GenerateCa(string cn)
    {
        using var rsa = System.Security.Cryptography.RSA.Create(2048);
        var request = new System.Security.Cryptography.X509Certificates.CertificateRequest(
            $"CN={cn} CA", rsa, System.Security.Cryptography.HashAlgorithmName.SHA256,
            System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(
            new System.Security.Cryptography.X509Certificates.X509BasicConstraintsExtension(true, false, 0, true));
        using var ca = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1),
            DateTimeOffset.UtcNow.AddYears(5));
        return (Pem(ca), Key(rsa));
    }

    private static (string CertPem, string KeyPem) IssueCert(string caPem, string caKeyPem, string cn)
    {
        // PFX round-trip (macOS): эфемерный ключ CreateFromPem не годится для
        // подписи сертификата — пере-импорт через PKCS#12.
        using var ca = System.Security.Cryptography.X509Certificates.X509CertificateLoader.LoadPkcs12(
            System.Security.Cryptography.X509Certificates.X509Certificate2
                .CreateFromPem(caPem, caKeyPem)
                .Export(System.Security.Cryptography.X509Certificates.X509ContentType.Pkcs12), null);
        using var rsa = System.Security.Cryptography.RSA.Create(2048);
        var request = new System.Security.Cryptography.X509Certificates.CertificateRequest(
            $"CN={cn}", rsa, System.Security.Cryptography.HashAlgorithmName.SHA256,
            System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        using var cert = request.Create(ca, DateTimeOffset.UtcNow.AddDays(-1),
            DateTimeOffset.UtcNow.AddYears(3), Guid.NewGuid().ToByteArray());
        return (Pem(cert), Key(rsa));
    }

    private static string Pem(System.Security.Cryptography.X509Certificates.X509Certificate2 cert)
        => cert.ExportCertificatePem().ReplaceLineEndings("\n").TrimEnd() + "\n";

    private static string Key(System.Security.Cryptography.RSA rsa)
        => rsa.ExportPkcs8PrivateKeyPem().ReplaceLineEndings("\n").TrimEnd() + "\n";
}
