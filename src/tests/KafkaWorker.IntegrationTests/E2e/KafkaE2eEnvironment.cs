using System.Diagnostics;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using FluentAssertions;
using Shared.Etcd.Client;
using KafkaWorker.IntegrationTests.Kafka;
using Xunit;

namespace KafkaWorker.IntegrationTests.E2e;

/// <summary>
/// Изолированное docker-E2E окружение KafkaWorker с ДВУМЯ инстансами сразу
/// (порт ValkeyE2eEnvironment; docs/e2e-isolation.md + docs/e2e-launch.md):
/// своя docker-сеть kfw-en-{runId}, свой etcd kfw-ee-{runId}, два
/// воркер-контейнера kfw-ew1/ew2-{runId} (образ kafkaworker:e2e из
/// docker/KafkaWorker.Dockerfile). Идентификатор прогона — полный guid во всех
/// именах; чистка и ассерт чистоты — строго по нему/тегу (own-only: голые
/// префиксы kfw-* запрещены — на хосте жив dev-стенд). Телеметрия: docker-логи/
/// inspect etcd/ew1/ew2 и всех kfw-{tag}* — в /tmp/pgw-e2e-artifacts-{runId}/
/// ДО удалений; MarkFailed() → teardown ОСТАНАВЛИВАЕТ контейнеры, но не
/// удаляет (README-cleanup.txt). Retry старта против гонок prune (3 попытки).
/// Гейт запуска: PGW_TEST_DOCKER=1; PGW_TEST_E2E_NOBUILD=1 — пропустить сборку
/// образа (только бисект).
/// </summary>
public sealed class KafkaE2eEnvironment : IAsyncDisposable
{
    private const string EtcdImage = "quay.io/coreos/etcd:v3.5.21";
    private const string WorkerImage = "kafkaworker:e2e";

    private readonly IContainer _etcd;
    private readonly IContainer _worker1;
    private readonly IContainer _worker2;
    private readonly INetwork _net;
    private readonly HttpClient _gatewayHttp = new();

    private bool _failed;

    private readonly string _runId;

    private KafkaE2eEnvironment(
        string slug, string runId, string netName, INetwork net,
        IContainer etcd, IContainer worker1, IContainer worker2,
        string etcdEndpoint, string artifactsDir, int api1Port, int api2Port)
    {
        Slug = slug;
        _runId = runId;
        NetName = netName;
        _net = net;
        _etcd = etcd;
        _worker1 = worker1;
        _worker2 = worker2;
        EtcdEndpoint = etcdEndpoint;
        ArtifactsDir = artifactsDir;
        Gateway = new EtcdGateway(_gatewayHttp);
        Api1BaseUrl = $"https://localhost:{api1Port}";
        Api2BaseUrl = $"https://localhost:{api2Port}";
        (Api1Port, Api2Port) = (api1Port, api2Port);
    }

    public string Slug { get; }

    public string ArtifactsDir { get; }

    /// <summary>etcd окружения: published порт на хосте.</summary>
    public string EtcdEndpoint { get; }

    /// <summary>База API первого воркера (https://localhost:{динамический порт}).</summary>
    public string Api1BaseUrl { get; }

    /// <summary>База API второго воркера (https://localhost:{динамический порт}).</summary>
    public string Api2BaseUrl { get; }

    /// <summary>Хост-порт API ew1 — резолв держателя клэйма (A4).</summary>
    public int Api1Port { get; }

    /// <summary>Хост-порт API ew2 — резолв держателя клэйма (A4).</summary>
    public int Api2Port { get; }

    /// <summary>Docker-имя первого воркера (kfw-ew1-{runId}).</summary>
    public string Worker1Name => $"kfw-ew1-{_runId}";

    /// <summary>Docker-имя второго воркера (kfw-ew2-{runId}).</summary>
    public string Worker2Name => $"kfw-ew2-{_runId}";

    /// <summary>Тег прогона (8 hex): кластер сценария tkw{tag} — все движковые
    /// объекты kfw-{C}-broker*/kfw-net-{C} содержат его (own-only чистка).</summary>
    public string ClusterTag => _runId[..8];

    public string NetName { get; }

    public EtcdGateway Gateway { get; }

    public void MarkFailed() => _failed = true;

    /// <summary>Подъём двухинстансного контура (spec t20 §5.1): сеть + etcd +
    /// ew1/ew2 (общие TLS-пакет/etcd/PortRange — гонки portalloc закрывает
    /// клэйм кластера; УНИКАЛЬНЫЙ Api__AdvertiseUrl каждому). 3 попытки —
    /// против гонок глобального prune.</summary>
    public static async Task<KafkaE2eEnvironment> StartAsync(string slug)
    {
        if (Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") != "1")
            throw new InvalidOperationException("E2E требует PGW_TEST_DOCKER=1 (иначе Skip в тесте)");

        var runId = Guid.NewGuid().ToString("N");
        var artifactsDir = Path.Combine(Path.GetTempPath(), $"pgw-e2e-artifacts-{runId}");
        Directory.CreateDirectory(Path.Combine(artifactsDir, "tls"));

        // Статический ассет процесса: образ kafkaworker:e2e (продовый Dockerfile).
        await BuildImageAsync(runId, artifactsDir);

        // TLS-пакет окружения: self-signed CA + server + client (PEM в артефакты);
        // пакет ОБЩИЙ для обоих инстансов и клиентских проб.
        var (caPem, caKeyPem) = GenerateCa("kfw-e2e");
        var (serverCert, serverKey) = IssueCert(caPem, caKeyPem, "kafkaworker");
        var tlsDir = Path.Combine(artifactsDir, "tls");
        await File.WriteAllTextAsync(Path.Combine(tlsDir, "ca.pem"), caPem);
        await File.WriteAllTextAsync(Path.Combine(tlsDir, "server.crt"), serverCert);
        await File.WriteAllTextAsync(Path.Combine(tlsDir, "server.key"), serverKey);

        for (var attempt = 1; ; attempt++)
        {
            var netName = $"kfw-en-{runId}";
            var net = new NetworkBuilder().WithName(netName).Build();
            var etcdPort = FreePort();
            var etcd = new ContainerBuilder(EtcdImage)
                .WithName($"kfw-ee-{runId}")
                .WithPortBinding(etcdPort, 2379)
                .WithCommand("etcd", "--name=test", "--data-dir=/etcd-data",
                    "--listen-client-urls=http://0.0.0.0:2379",
                    "--advertise-client-urls=http://127.0.0.1:2379")
                .Build();
            var api1Port = FreePort();
            var api2Port = FreePort();
            var portRange = FreePortWindow.Find();

            IContainer WorkerContainer(int apiPort, string name)
                => new ContainerBuilder(WorkerImage)
                    .WithName(name)
                    .WithNetwork(net)
                    .WithPortBinding(apiPort, 8080)
                    .WithBindMount("/var/run/docker.sock", "/var/run/docker.sock")
                    .WithBindMount(tlsDir, "/tls")
                    .WithEnvironment(new Dictionary<string, string>
                    {
                        ["KafkaWorker__Etcd__Endpoints__0"] = $"http://host.docker.internal:{etcdPort}",
                        ["KafkaWorker__AdvertisedClientHost"] = "host.docker.internal",
                        // Уникальный AdvertiseUrl каждому инстансу (t07): одинаковые
                        // URL гасят оба дискавери-ключа.
                        ["KafkaWorker__Api__AdvertiseUrl"] = $"https://host.docker.internal:{apiPort}",
                        ["KafkaWorker__Docker__Mode"] = "Plain",
                        ["KafkaWorker__Docker__Hosts__0__Name"] = "local",
                        ["KafkaWorker__Docker__Hosts__0__Endpoint"] = "unix:///var/run/docker.sock",
                        ["KafkaWorker__Docker__PortRange__From"] = portRange.From.ToString(),
                        ["KafkaWorker__Docker__PortRange__To"] = portRange.To.ToString(),
                        // Ускоренные циклы/пороги takeover-сценария (spec §3, §5.1):
                        // тик держателя 1 с, бут брокеров ≤100 с (канон).
                        ["KafkaWorker__Loops__ScanIntervalSec"] = "1",
                        ["KafkaWorker__Loops__KeepaliveSec"] = "1",
                        ["KafkaWorker__Loops__ErrorDelayMs"] = "500",
                        ["KafkaWorker__Thresholds__BrokerBootSec"] = "100",
                        ["KFW_API_TLS_CERT_PATH"] = "/tls/server.crt",
                        ["KFW_API_TLS_KEY_PATH"] = "/tls/server.key",
                        ["KFW_API_TLS_CLIENT_CA_PATH"] = "/tls/ca.pem",
                        ["ASPNETCORE_URLS"] = "https://+:8080",
                    })
                    .WithExtraHost("host.docker.internal", "host-gateway")
                    .Build();

            var worker1 = WorkerContainer(api1Port, $"kfw-ew1-{runId}");
            var worker2 = WorkerContainer(api2Port, $"kfw-ew2-{runId}");

            try
            {
                await net.CreateAsync();
                await etcd.StartAsync();
                await worker1.StartAsync();
                await worker2.StartAsync();
                var environment = new KafkaE2eEnvironment(slug, runId, netName, net,
                    etcd, worker1, worker2,
                    $"http://localhost:{etcdPort}", artifactsDir, api1Port, api2Port);
                await File.WriteAllTextAsync(Path.Combine(artifactsDir, "README-cleanup.txt"),
                    "# Зачистка окружения прогона (own-only, по тегу/идентификатору):\n"
                    + $"docker rm -f kfw-ew1-{runId} kfw-ew2-{runId} kfw-ee-{runId}\n"
                    + $"docker ps -aq --filter name={environment.ClusterTag} | xargs -r docker rm -f\n"
                    + $"docker volume ls -q --filter name={environment.ClusterTag} | xargs -r docker volume rm -f\n"
                    + $"docker network rm {netName}\n"
                    + $"docker network ls --format {{{{.Name}}}} --filter name={environment.ClusterTag} | grep kfw-net | xargs -r docker network rm\n");
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

    // ── Сборка образа (прецедент E2eSecondInstanceScenarios): [PHASE]-метки
    // ДО/ПОСЛЕ с таймингом (тихая фаза запрещена), полный вывод — в
    // process-build-{tag}.log, бюджет CLI 120 c; гейт NOBUILD — пропустить.
    private static async Task BuildImageAsync(string runId, string artifactsDir)
    {
        if (Environment.GetEnvironmentVariable("PGW_TEST_E2E_NOBUILD") == "1")
            return;
        var root = FindRepoRoot();
        Console.WriteLine($"[PHASE] build {WorkerImage}: старт (продовый Dockerfile, бюджет 120 c)…");
        var sw = Stopwatch.StartNew();
        await RunProcessAsync("docker",
            ["build", "-f", "docker/KafkaWorker.Dockerfile", "-t", WorkerImage, "."],
            CancellationToken.None, TimeSpan.FromSeconds(120),
            workingDirectory: root,
            logFile: Path.Combine(artifactsDir, $"process-build-{runId[..8]}.log"));
        Console.WriteLine($"[PHASE] build {WorkerImage}: {sw.Elapsed.TotalSeconds:F0} c");
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "docker", "KafkaWorker.Dockerfile")))
            dir = dir.Parent;
        return dir?.FullName
            ?? throw new InvalidOperationException("корень репо (docker/KafkaWorker.Dockerfile) не найден");
    }

    // ── Хелперы docker-CLI (канон RunProcessAsync: таймаут убивает дерево,
    // вывод — в исключение, полный лог — в logFile) ──

    /// <summary>docker-CLI: вывод stdout; ненулевой exit/таймаут — исключение
    /// с выводом процесса (зависший процесс умирает по бюджету 2 мин).</summary>
    public Task<string> RunDockerAsync(IReadOnlyList<string> args, CancellationToken ct)
        => RunProcessAsync("docker", [.. args], ct, TimeSpan.FromMinutes(2));

    /// <summary>Имена контейнеров по префиксу (вкл. stopped при all) — фильтр
    /// StartsWith отсекает частичные совпадения docker --filter.</summary>
    public async Task<List<string>> ListContainerNamesAsync(string prefix, bool all = false)
    {
        var args = new List<string> { "ps", "--format", "{{.Names}}" };
        if (all)
            args.Add("-a");
        args.AddRange(["--filter", $"name={prefix}"]);
        var output = await RunDockerAsync([.. args], CancellationToken.None);
        return output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(n => n.StartsWith(prefix, StringComparison.Ordinal))
            .ToList();
    }

    private static async Task<string> RunProcessAsync(
        string file, string[] args, CancellationToken ct, TimeSpan timeout,
        string? workingDirectory = null, string? logFile = null)
    {
        var psi = new ProcessStartInfo(file, args)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        if (workingDirectory is not null)
            psi.WorkingDirectory = workingDirectory;
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
            if (logFile is not null)
                await File.AppendAllTextAsync(logFile, output + error);
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

            if (logFile is not null)
                await File.AppendAllTextAsync(logFile, drained);
            throw new ApplicationException(
                $"{file} {string.Join(' ', args)} не завершился за {timeout.TotalSeconds:0} c — убит; хвост:\n{drained}");
        }
    }

    // ── Хелперы etcd (обёртки над Gateway) ──

    /// <summary>Get-значение или null (ключа нет).</summary>
    public async Task<string?> GetOrNullAsync(string key, CancellationToken ct)
        => (await Gateway.GetAsync(EtcdEndpoint, key, ct)).Value?.Value;

    /// <summary>Range по префиксу: пары ключ-значение.</summary>
    public async Task<IReadOnlyList<Kv>> RangeAsync(string prefix, CancellationToken ct)
        => (await Gateway.RangeAsync(EtcdEndpoint, prefix, ct)).Value;

    /// <summary>Put без lease (сид кластера в стиле панели).</summary>
    public Task PutAsync(string key, string value, CancellationToken ct)
        => Gateway.PutAsync(EtcdEndpoint, key, value, lease: null, ct);

    // Телеметрия (docs/e2e-launch.md §1): docker-логи+inspect etcd/ew1/ew2 и
    // всех kfw-{tag}* — ДО любого удаления; вызывается в teardown и на
    // slow/failed-phase.
    public async Task CollectDiagnosticsAsync(string mark)
    {
        foreach (var name in new[] { Worker1Name, Worker2Name, $"kfw-ee-{_runId}" })
        {
            await ShellToFileAsync($"logs --tail 2000 {name}",
                Path.Combine(ArtifactsDir, $"container-{name}-{mark}.log"));
            await ShellToFileAsync($"inspect {name}",
                Path.Combine(ArtifactsDir, $"container-{name}-{mark}.json"));
        }

        // Объекты кластера сценария — по тегу прогона (вхождение в имя: тег
        // сидит в середине «kfw-tkw{tag}-broker1», поэтому без StartsWith-
        // префикса; ew/etcd уже сняты выше — повторный снимок не страшен).
        var listing = await RunDockerOrNullAsync(
            ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name={ClusterTag}"]);
        var names = listing.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .ToList();
        await File.WriteAllLinesAsync(Path.Combine(ArtifactsDir, $"nodes-{mark}.txt"), names);
        foreach (var name in names)
            await ShellToFileAsync($"logs --tail 2000 {name}",
                Path.Combine(ArtifactsDir, $"node-{name}-{mark}.log"));
    }

    /// <summary>Полный teardown при любом исходе: телеметрия ДО удалений →
    /// упавший сценарий (MarkFailed): только docker stop ew1/ew2/etcd, ничего
    /// не удалять → иначе rm своих контейнеров + own-only остатки тега
    /// (брокеры/тома/сети, если демонтаж не успел) → ассерт чистоты.</summary>
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
            // не удалить (брокеры/тома/сети/etcd остаются для разбора).
            await SafeStopAsync(_worker2, remove: false);
            await SafeStopAsync(_worker1, remove: false);
            await SafeStopAsync(_etcd, remove: false);
            return;
        }

        await SafeStopAsync(_worker2, remove: true);
        await SafeStopAsync(_worker1, remove: true);
        await SafeStopAsync(_etcd, remove: true);

        // Own-only остатки тега прогона (движковые объекты кластера, если
        // демонтаж A6 не успел): контейнеры → тома → сети. Фильтр — тег
        // (содержится во всех именах серии), голый префикс kfw-* запрещён.
        var leftover = await ListContainerNamesAsync(ClusterTag, all: true);
        foreach (var name in leftover)
            await TryDockerAsync(["rm", "-f", name]);

        var volumes = await RunDockerOrNullAsync(
            ["volume", "ls", "--format", "{{.Name}}", "--filter", $"name={ClusterTag}"]);
        foreach (var volume in volumes.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            await TryDockerAsync(["volume", "rm", "-f", volume]);

        var networks = await RunDockerOrNullAsync(
            ["network", "ls", "--format", "{{.Name}}", "--filter", $"name={ClusterTag}"]);
        foreach (var network in networks.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            await TryDockerAsync(["network", "rm", network]);

        try { await _net.DeleteAsync(); } catch { /* уже нет */ }
        _gatewayHttp.Dispose();

        // Ассерт чистоты: ни контейнера, ни тома, ни сети с идентификатором
        // прогона (runId/тег содержится во всех именах серии).
        (await ListContainerNamesAsync(ClusterTag, all: true))
            .Should().BeEmpty($"teardown убрал все контейнеры тега {ClusterTag}");
        (await RunDockerOrNullAsync(
                ["volume", "ls", "--format", "{{.Name}}", "--filter", $"name={ClusterTag}"]))
            .Trim().Should().BeEmpty($"после teardown не осталось томов тега {ClusterTag}");
        (await RunDockerOrNullAsync(
                ["network", "ls", "--format", "{{.Name}}", "--filter", $"name={ClusterTag}"]))
            .Trim().Should().BeEmpty($"после teardown не осталось сетей тега {ClusterTag}");
    }

    // docker-вызов телеметрии/чистки: ошибки не всплывают (прогон завершён,
    // объект мог быть уже удалён параллельной операцией).
    private async Task TryDockerAsync(IReadOnlyList<string> args)
    {
        try
        {
            await RunDockerAsync(args, CancellationToken.None);
        }
        catch
        {
            // чистка — лучшее усилие
        }
    }

    private async Task<string> RunDockerOrNullAsync(IReadOnlyList<string> args)
    {
        try
        {
            return await RunDockerAsync(args, CancellationToken.None);
        }
        catch
        {
            return "";
        }
    }

    private async Task ShellToFileAsync(string args, string path)
    {
        try
        {
            var output = await RunDockerAsync(args.Split(' '), CancellationToken.None);
            await File.WriteAllTextAsync(path, output);
        }
        catch
        {
            // телеметрия — лучшее усилие
        }
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

    private static int FreePort()
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    // ── TLS-генерация окружения (self-signed, минимум для e2e; как у Valkey) ──

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
