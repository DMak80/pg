using System.Diagnostics;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using DotNet.Testcontainers.Volumes;
using FluentAssertions;
using Xunit;

namespace OwnS3.IntegrationTests.E2e;

/// <summary>
/// Изолированное docker-E2E TLS-окружение ownS3 (t42, docs/e2e-isolation.md +
/// docs/e2e-launch.md; каркас — OwnS3E2eEnvironment): своя сеть os3-en-{runId},
/// свой контейнер os3-es-{runId} (образ owns3:e2e), свой том os3-vd-{runId}.
/// Отличия от HTTP-эталона (spec §3.5): тест генерирует СВОЮ PKI на хосте
/// (BCL CertificateRequest: CA + серверный серт, SAN localhost/127.0.0.1/имя
/// контейнера), серты — ro bind-mount в /tls, грань поднимается по env
/// OWNS3_TLS_CERT_PATH/_KEY_PATH (проверка env-биндингов end-to-end) и
/// слушает HTTPS-only. Готовность — БЕЗ http-wait-стратегии: полл /healthz
/// своим HttpClient с доверенным тестовым CA (бюджет 30 с, полл 1 с).
/// Телеметрия/MarkFailed/retry×3/teardown-ассерты own-only — каркас эталона.
/// </summary>
public sealed class OwnS3TlsE2eEnvironment : IAsyncDisposable
{
    private const string ServiceImage = "owns3:e2e";

    private readonly INetwork _net;
    private readonly IContainer _service;
    private readonly string _runId;
    private readonly string _containerName;
    private readonly string _volumeName;
    private readonly int _hostPort;
    private readonly string _tlsDir;
    private readonly string _caThumbprint;
    private readonly X509Certificate2 _ca; // ExtraStore валидации цепочки (macOS: без стора цепочка строится leaf-only)
    private bool _failed;

    private OwnS3TlsE2eEnvironment(string slug, string runId, INetwork net,
        string containerName, string volumeName, IContainer service,
        string artifactsDir, int hostPort, string tlsDir, X509Certificate2 ca)
    {
        Slug = slug;
        _runId = runId;
        _net = net;
        _containerName = containerName;
        _volumeName = volumeName;
        _service = service;
        ArtifactsDir = artifactsDir;
        _hostPort = hostPort;
        _tlsDir = tlsDir;
        _ca = ca;
        _caThumbprint = ca.Thumbprint;
    }

    public string Slug { get; }

    /// <summary>Артефакты телеметрии прогона (логи/inspect/phases.log) + каталог PKI.</summary>
    public string ArtifactsDir { get; }

    /// <summary>Docker-имя сети окружения (os3-en-{runId}).</summary>
    public string NetName => $"os3-en-{_runId}";

    /// <summary>Docker-имя контейнера сервиса (os3-es-{runId}).</summary>
    public string ContainerName => _containerName;

    /// <summary>S3-грань с хоста: HTTPS-only, динамический порт; SAN серта
    /// покрывает localhost — SDK с AWS_CA_BUNDLE и HttpClient с тестовым CA.</summary>
    public string S3BaseUrl => $"https://localhost:{_hostPort}";

    /// <summary>S3-грань изнутри docker-сети (mc: docker run --network); SAN
    /// покрывает имя контейнера.</summary>
    public string S3InternalUrl => $"https://{_containerName}:9000";

    /// <summary>Файл ca.pem тестовой CA (bind-mount для mc-сценария, SSL_CERT_FILE).</summary>
    public string CaPemPath => Path.Combine(_tlsDir, "ca.pem");

    /// <summary>PEM тестовой CA строкой (AWS_CA_BUNDLE — материал значением).</summary>
    public string CaPem { get; private set; } = "";

    /// <summary>Тег прогона (8 hex) — компонент имён бакетов сценариев.</summary>
    public string RunTag => _runId[..8];

    /// <summary>Полный guid прогона (ассерт чистоты, README-cleanup).</summary>
    public string RunId => _runId;

    public void MarkFailed() => _failed = true;

    /// <summary>Генерация тестовой PKI (BCL, на хосте теста — spec §3.5): CA
    /// самоподписанный (basicConstraints CA:true) + серверный серт им CA,
    /// SAN localhost/127.0.0.1/имя контейнера (mc изнутри сети).</summary>
    private static (string CaPem, X509Certificate2 Ca, string CertPem, string KeyPem) NewPki(string serverDns)
    {
        using var caRsa = RSA.Create(2048);
        var caReq = new CertificateRequest("CN=os3-e2e-ca", caRsa,
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caReq.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        using var ca = caReq.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(2));
        using var srvRsa = RSA.Create(2048);
        var srvReq = new CertificateRequest("CN=os3-e2e-server", srvRsa,
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName("localhost");
        san.AddIpAddress(IPAddress.Loopback);
        san.AddDnsName(serverDns); // имя контейнера — mc изнутри сети
        srvReq.CertificateExtensions.Add(san.Build());
        // serial: byte[] → неявная конвертация в ReadOnlyMemory<byte> параметра Create
        // (using НЕЛЬЗЯ — byte[] не IDisposable, CS1674)
        var serial = BitConverter.GetBytes(DateTime.UtcNow.Ticks);
        // Подпись — приватным ключом ИЗ issuer-сертификата CA (CreateSelfSigned
        // несёт ключ); генератор X509SignatureGenerator.CreateForRSASignature
        // в net10.0 отсутствует (проверено компиляцией) — перегрузка с ключом CA.
        // Границы — ОТ серта CA (не от часов): notAfter серта строго внутри
        // валидности CA, иначе Create отсекает (дельта часов между созданиями).
        using var srv = srvReq.Create(ca,
            ca.NotBefore.AddMinutes(1), ca.NotAfter.AddMinutes(-1), serial);
        return (ca.ExportCertificatePem(), new X509Certificate2(ca),
            srv.ExportCertificatePem(), srvRsa.ExportPkcs8PrivateKeyPem());
    }

    /// <summary>Подъём окружения: статическая сборка образа → PKI → сеть → том →
    /// контейнер (ro bind-mount сертов, env OWNS3_TLS_*_PATH) → полл /healthz
    /// с доверенным CA (≤ 30 с, полл 1 с); 3 попытки против распознанных гонок.</summary>
    public static async Task<OwnS3TlsE2eEnvironment> StartAsync(
        string slug, CancellationToken ct = default)
    {
        if (Environment.GetEnvironmentVariable(DockerTrait.EnvVar) != "1")
            throw new InvalidOperationException(
                $"E2E требует {DockerTrait.EnvVar}=1 (иначе Skip в тесте DockerTrait'ом)");

        var runId = Guid.NewGuid().ToString("N");
        var artifactsDir = Path.Combine(Path.GetTempPath(), $"pgw-e2e-artifacts-{runId}");
        var tlsDir = Path.Combine(artifactsDir, "tls");
        Directory.CreateDirectory(tlsDir);

        // Статический ассет процесса: owns3:e2e (publish на хосте + CLI-build).
        await OwnS3E2eImage.BuildOnceAsync(artifactsDir, ct);

        for (var attempt = 1; ; attempt++)
        {
            var net = new NetworkBuilder().WithName($"os3-en-{runId}").Build();
            var volumeName = $"os3-vd-{runId}";
            var volume = new VolumeBuilder().WithName(volumeName).Build();
            var containerName = $"os3-es-{runId}";
            var hostPort = FreePort();
            var (caPem, ca, certPem, keyPem) = NewPki(containerName);
            File.WriteAllText(Path.Combine(tlsDir, "ca.pem"), caPem);
            File.WriteAllText(Path.Combine(tlsDir, "server.crt"), certPem);
            File.WriteAllText(Path.Combine(tlsDir, "server.key"), keyPem);
            var service = new ContainerBuilder(ServiceImage)
                .WithName(containerName)
                .WithNetwork(net)
                .WithPortBinding(hostPort, 9000)
                .WithVolumeMount(volumeName, "/data")
                // TLS-материал — ro bind-mount каталога сертов + env-биндинги
                // OWNS3_TLS_*_PATH (spec §3.5: env-биндинги проверяются end-to-end).
                .WithBindMount(tlsDir, "/tls", AccessMode.ReadOnly)
                .WithEnvironment("OWNS3_TLS_CERT_PATH", "/tls/server.crt")
                .WithEnvironment("OWNS3_TLS_KEY_PATH", "/tls/server.key")
                .WithEnvironment("OWNS3_ROOT_USER", OwnS3E2eEnvironment.RootUser)
                .WithEnvironment("OWNS3_ROOT_PASSWORD", OwnS3E2eEnvironment.RootPassword)
                .WithEnvironment("OWNS3_ACCESS_KEYS__0__ACCESSKEY", OwnS3E2eEnvironment.WriterAccessKey)
                .WithEnvironment("OWNS3_ACCESS_KEYS__0__SECRETKEY", OwnS3E2eEnvironment.WriterSecretKey)
                .WithEnvironment("OWNS3_ACCESS_KEYS__0__POLICY", "read-write")
                .WithEnvironment("OWNS3_ACCESS_KEYS__1__ACCESSKEY", OwnS3E2eEnvironment.ReaderAccessKey)
                .WithEnvironment("OWNS3_ACCESS_KEYS__1__SECRETKEY", OwnS3E2eEnvironment.ReaderSecretKey)
                .WithEnvironment("OWNS3_ACCESS_KEYS__1__POLICY", "read-only")
                .WithEnvironment("OWNS3_ACCESS_KEYS__2__ACCESSKEY", OwnS3E2eEnvironment.AdminAccessKey)
                .WithEnvironment("OWNS3_ACCESS_KEYS__2__SECRETKEY", OwnS3E2eEnvironment.AdminSecretKey)
                .WithEnvironment("OWNS3_ACCESS_KEYS__2__POLICY", "admin")
                .WithEnvironment("OWNS3_HOST_ID", $"os3-e2e-{runId[..8]}")
                .Build();

            async Task RemoveAttemptAsync()
            {
                await SafeStopAsync(service, remove: true);
                await TryDockerAsync(["volume", "rm", "-f", volumeName]);
                try { await net.DeleteAsync(ct); } catch { /* уже нет */ }
            }

            try
            {
                var sw = Stopwatch.StartNew();
                await net.CreateAsync(ct);
                await volume.CreateAsync(ct);
                await service.StartAsync(ct);
                var env = new OwnS3TlsE2eEnvironment(slug, runId, net, containerName,
                    volumeName, service, artifactsDir, hostPort, tlsDir, ca)
                {
                    CaPem = caPem,
                };
                // Готовность БЕЗ http-wait: полл /healthz с доверенным CA
                // (HTTPS-only грань; бюджет 30 с, полл 1 с — spec §3.5).
                var ready = false;
                for (var i = 0; i < 30; i++)
                {
                    if (await env.HealthzAsync(CancellationToken.None))
                    {
                        ready = true;
                        break;
                    }
                    await Task.Delay(TimeSpan.FromSeconds(1), ct);
                }
                if (!ready)
                {
                    // Телеметрия старта (docs/e2e-launch.md): логи контейнера —
                    // ДО чистки попытки («почему healthz не поднялся» — по
                    // артефактам, без повторного прогона).
                    var logs = await env.RunDockerOrNullAsync(["logs", containerName], CancellationToken.None);
                    await File.WriteAllTextAsync(
                        Path.Combine(artifactsDir, $"container-{containerName}-start-fail.log"),
                        logs + "\n");
                    Console.WriteLine($"[PHASE] start: healthz недоступен 30 с — docker logs в {artifactsDir}");
                    throw new ApplicationException(
                        $"TLS-грань {env.S3BaseUrl} не поднялась за 30 с (healthz недоступен)");
                }

                await env.LogPhaseAsync("start", sw);
                await File.WriteAllTextAsync(Path.Combine(artifactsDir, "README-cleanup.txt"),
                    "# Зачистка окружения прогона (own-only, по runId):\n"
                    + $"docker rm -f {containerName}\n"
                    + $"docker volume rm -f {volumeName}\n"
                    + $"docker network rm {env.NetName}\n"
                    + $"rm -rf {tlsDir}\n", ct);
                return env;
            }
            catch (Exception ex) when (attempt < 3
                && (ex.Message.Contains("network", StringComparison.OrdinalIgnoreCase)
                    || ex.Message.Contains("bind", StringComparison.OrdinalIgnoreCase)
                    || ex.Message.Contains("port", StringComparison.OrdinalIgnoreCase)))
            {
                Console.WriteLine($"[PHASE] start: попытка {attempt} не удалась ({ex.Message.Split('\n')[0]}) — ретрай с чистого листа");
                await RemoveAttemptAsync();
                continue;
            }
            catch
            {
                await RemoveAttemptAsync();
                throw;
            }
        }
    }

    /// <summary>Сценарий под политикой телеметрии (docs/e2e-launch.md §3):
    /// исключение тела → телеметрия "failed" ДО остановки → MarkFailed →
    /// rethrow; teardown — DisposeAsync при любом исходе.</summary>
    public static async Task RunScenarioAsync(
        string slug, Func<OwnS3TlsE2eEnvironment, Task> body, CancellationToken ct = default)
    {
        var fx = await StartAsync(slug, ct);
        try
        {
            await body(fx);
        }
        catch
        {
            try { await fx.CollectDiagnosticsAsync("failed"); }
            catch { /* телеметрия — лучшее усилие */ }
            fx.MarkFailed();
            throw;
        }
        finally
        {
            await fx.DisposeAsync();
        }
    }

    /// <summary>docker-CLI: stdout; ненулевой exit/таймаут — исключение с
    /// выводом процесса (бюджет 2 мин — зависший процесс умирает).</summary>
    public Task<string> RunDockerAsync(IReadOnlyList<string> args, CancellationToken ct)
        => RunProcessAsync("docker", [.. args], ct, TimeSpan.FromMinutes(2));

    /// <summary>GET /healthz по https с доверенным тестовым CA (отпечаток CA —
    /// корень цепочки): true при 2xx; сетевые/TLS-отказы — false.</summary>
    public async Task<bool> HealthzAsync(CancellationToken ct)
    {
        try
        {
            using var handler = new SocketsHttpHandler
            {
                SslOptions = new SslClientAuthenticationOptions
                {
                    RemoteCertificateValidationCallback = ValidateAgainstTestCa,
                },
            };
            using var http = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(5) };
            using var response = await http.GetAsync($"{S3BaseUrl}/healthz", ct);
            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    // Валидация цепочки сервера против тестовой CA: корень цепочки обязан
    // нести отпечаток сгенерированной CA (иначе TLS-отказ).
    private bool ValidateAgainstTestCa(object? sender, X509Certificate? certificate,
        X509Chain? chain, SslPolicyErrors sslPolicyErrors)
    {
        if (certificate is null || chain is null)
            return false;
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;
        // CA — в ExtraStore: без доверенного стора (macOS) цепочка строится
        // ОДНИМ элементом (leaf), корень отсутствует и сверка не срабатывает
        // (эмпирический факт прогона t42) — с ExtraStore цепочка [leaf, ca].
        chain.ChainPolicy.ExtraStore.Add(_ca);
        if (!chain.Build(new X509Certificate2(certificate)))
            return false;
        var root = chain.ChainElements[^1].Certificate;
        return string.Equals(root.Thumbprint, _caThumbprint, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Телеметрия ДО любых удалений (docs/e2e-launch.md §1).</summary>
    public async Task CollectDiagnosticsAsync(string mark)
    {
        await ShellToFileAsync($"logs --tail 2000 {_containerName}",
            Path.Combine(ArtifactsDir, $"container-{_containerName}-{mark}.log"));
        await ShellToFileAsync($"inspect {_containerName}",
            Path.Combine(ArtifactsDir, $"container-{_containerName}-{mark}.json"));
    }

    /// <summary>Финал фазы (docs/e2e-launch.md §2): [PHASE]-метка с таймингом
    /// в Console + UTC-запись в phases.log; фаза > 60 с — немедленный сбор
    /// docker-диагностики (slow-phase-*) независимо от исхода фазы.</summary>
    private async Task LogPhaseAsync(string name, Stopwatch sw)
    {
        sw.Stop();
        var line = $"phase {name}: ok, {sw.Elapsed.TotalSeconds:F1}s";
        Console.WriteLine($"[PHASE] {line}");
        await File.AppendAllTextAsync(Path.Combine(ArtifactsDir, "phases.log"),
            $"{DateTime.UtcNow:O} {line}\n");
        if (sw.Elapsed > TimeSpan.FromSeconds(60))
        {
            try { await CollectDiagnosticsAsync($"slow-phase-{name}-{(int)sw.Elapsed.TotalSeconds}s"); }
            catch { /* телеметрия — лучшее усилие */ }
        }
    }

    /// <summary>Полный teardown при любом исходе: телеметрия → dispose
    /// контейнера → volume rm с ретраями → сеть → каталог PKI → ассерт
    /// чистоты по runId (own-only).</summary>
    public async ValueTask DisposeAsync()
    {
        var sw = Stopwatch.StartNew();
        try { await CollectDiagnosticsAsync("teardown"); }
        catch { /* телеметрия не маскирует исход сценария */ }

        if (_failed)
        {
            // docs/e2e-launch.md §3: контейнер ОСТАНОВИТЬ, не удалить (том/
            // сеть/PKI остаются для разбора; чистка — README-cleanup.txt).
            await SafeStopAsync(_service, remove: false);
            await LogPhaseAsync("teardown", sw);
            return;
        }

        await SafeStopAsync(_service, remove: true);

        // volume: ретраи до ~30 с (полл 2 с).
        for (var i = 0; ; i++)
        {
            var left = await RunDockerOrNullAsync(
                ["volume", "ls", "--format", "{{.Name}}", "--filter", $"name={_volumeName}"],
                CancellationToken.None);
            if (string.IsNullOrWhiteSpace(left) || i >= 15)
                break;
            await TryDockerAsync(["volume", "rm", "-f", _volumeName]);
            await Task.Delay(TimeSpan.FromSeconds(2));
        }

        try { await _net.DeleteAsync(); } catch { /* уже нет */ }

        // Каталог PKI — лучшее усилие (серты одноразовые; на упавшем прогоне
        // остаются с README-cleanup).
        try { Directory.Delete(_tlsDir, recursive: true); }
        catch { /* разбора ради — только на failed-прогоне; на зелёном пусто */ }

        // Ассерт чистоты: ни контейнера, ни тома, ни сети с runId прогона.
        var containers = await RunDockerOrNullAsync(
            ["ps", "-a", "--format", "{{.Names}}", "--filter", $"name={_runId}"],
            CancellationToken.None);
        containers.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(n => n.Contains(_runId, StringComparison.Ordinal))
            .ToList()
            .Should().BeEmpty($"teardown убрал все контейнеры прогона {_runId}");
        (await RunDockerOrNullAsync(
                ["volume", "ls", "--format", "{{.Name}}", "--filter", $"name={_volumeName}"],
                CancellationToken.None))
            .Trim().Should().BeEmpty($"после teardown не осталось тома {_volumeName}");
        (await RunDockerOrNullAsync(
                ["network", "ls", "--format", "{{.Name}}", "--filter", $"name={NetName}"],
                CancellationToken.None))
            .Trim().Should().BeEmpty($"после teardown не осталось сети {NetName}");

        await LogPhaseAsync("teardown", sw);
    }

    // ── Процесс-хелперы (канон RunProcessAsync: таймаут убивает дерево,
    //    вывод — в исключение, полный лог — в logFile) ──

    private static async Task<string> RunProcessAsync(
        string file, string[] args, CancellationToken ct, TimeSpan timeout,
        IReadOnlyDictionary<string, string>? env = null, string? logFile = null)
    {
        var psi = new ProcessStartInfo(file, args)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        if (env is not null)
            foreach (var (key, value) in env)
                psi.Environment[key] = value;
        using var process = Process.Start(psi)
            ?? throw new ApplicationException($"не удалось запустить {file}");
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(timeout);
        var outTask = process.StandardOutput.ReadToEndAsync();
        var errTask = process.StandardError.ReadToEndAsync();
        try
        {
            await process.WaitForExitAsync(timeoutCts.Token);
            var output = await outTask;
            var error = await errTask;
            if (logFile is not null)
                await File.AppendAllTextAsync(logFile, output + error, ct);
            if (process.ExitCode != 0)
                throw new ApplicationException($"{file} {string.Join(' ', args)} → {process.ExitCode}: {error.Trim()}");
            return output.Trim();
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            try { process.Kill(entireProcessTree: true); }
            catch { /* процесс мог уже выйти */ }
            string drained = "";
            try
            {
                drained = string.Join("", await Task.WhenAll(outTask, errTask).WaitAsync(TimeSpan.FromSeconds(5)));
            }
            catch (TimeoutException)
            {
                // пайп завис — вывод недоступен
            }
            if (logFile is not null)
                await File.AppendAllTextAsync(logFile, drained, CancellationToken.None);
            throw new ApplicationException(
                $"{file} {string.Join(' ', args)} не завершился за {timeout.TotalSeconds:0} c — убит; хвост:\n{drained}");
        }
    }

    private static async Task TryDockerAsync(IReadOnlyList<string> args)
    {
        try { await RunProcessAsync("docker", [.. args], CancellationToken.None, TimeSpan.FromMinutes(2)); }
        catch { /* чистка — лучшее усилие */ }
    }

    private async Task<string> RunDockerOrNullAsync(IReadOnlyList<string> args, CancellationToken ct)
    {
        try { return await RunDockerAsync(args, ct); }
        catch { return ""; }
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
        try { await container.StopAsync(); }
        catch { /* уже остановлен/не стартовал */ }
        if (remove)
        {
            try { await container.DisposeAsync(); }
            catch { /* уже удалён */ }
        }
    }

    // Зонд свободного порта (динамические порты везде; прецедент FreePort).
    private static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
