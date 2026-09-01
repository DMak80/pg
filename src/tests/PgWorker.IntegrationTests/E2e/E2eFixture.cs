using System.Diagnostics;
using System.Net;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using PgWorker.Etcd.Client;
using PgWorker.IntegrationTests.Docker;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

/// <summary>
/// E2E-стенд (задача 26; spec §11, §9): etcd-контейнер + собранный образ
/// pgworker-node + запуск PgWorker.App хост-процессами (два-четыре инстанса —
/// критерии AC2–AC7). Стенд воспроизводит dev-stand: один docker-хост, режим
/// plain (план №5: анти-аффинити вырождается в «порты разные»).
/// </summary>
public sealed class E2eFixture : IAsyncLifetime
{
    // Секреты e2e-установки (Д7): передаются обоим процессам и SQL-пробам теста.
    public const string SuPassword = "pgw-e2e-su";
    public const string StandbyPassword = "pgw-e2e-standby";
    public const string BucketAdminPassword = "pgw-e2e-admin";
    public const string MoverPassword = "pgw-e2e-mover";

    private IContainer? _etcd;

    public EtcdGateway Gateway { get; private set; } = null!;

    public string EtcdEndpoint { get; private set; } = "";

    public string NodeImage { get; private set; } = "pgworker-node:e2e";

    public string Root { get; private set; } = "";

    public string AppDll { get; private set; } = "";

    private readonly HttpClient _healthHttp = new() { Timeout = TimeSpan.FromSeconds(3) };

    public async ValueTask InitializeAsync()
    {
        DockerTrait.SkipIfUnavailable();

        // Корень репозитория и артефакты: от каталога тестовой сборки вверх.
        Root = FindRoot(AppContext.BaseDirectory);
        AppDll = Path.Combine(Root, "src", "PgWorker.App", "bin", "Release", "net10.0", "PgWorker.App.dll");
        if (!File.Exists(AppDll))
            throw new ApplicationException(
                $"нет {AppDll} — соберите решение: dotnet build src/PgWorker.slnx -c Release");

        // Чистим контейнеры/volume прошлых прогонов (порты должны быть свободны).
        foreach (var id in (await RunDockerAsync(["ps", "-aq", "--filter", "name=pgw-"])).Split(
                     ['\n', ' '], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            await RunDockerAsync(["rm", "-f", id]);
        // docker rm -f возвращает управление до фактического освобождения
        // volume-ссылки демоном (гонка Docker Desktop: GC ссылки может идти
        // секундами) — ретраим с бюджетом ~20 с, иначе уборка остатков
        // прошлого прогона роняет инициализацию фикстуры целиком.
        // Фильтр docker — SUBSTRING: ловит и чужой deploy_pgw-snapshots стенка
        // (compose-префикс проекта deploy), который смонтирован живым контейнером
        // и не удаляем в принципе; якорим префикс pgw- (артефакты e2e) в C#.
        foreach (var id in (await RunDockerAsync(["volume", "ls", "-q", "--filter", "name=pgw-"])).Split(
                     ['\n', ' '], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                     .Where(v => v.StartsWith("pgw-", StringComparison.Ordinal)))
            for (var attempt = 0; ; attempt++)
            {
                try
                {
                    await RunDockerAsync(["volume", "rm", "-f", id]);
                    break;
                }
                catch (ApplicationException) when (attempt < 10)
                {
                    await Task.Delay(2000, TestContext.Current.CancellationToken);
                }
            }

        // Образ узла (задача 25): собирается ДО запуска процессов (Д4, R1:
        // без DOORMAN_URL — узел без пулера, PgWorker запускается с EnableDoorman=false).
        await RunProcessAsync("docker", ["build", "-q", "-f", $"{Root}/docker/node/Dockerfile", "-t", NodeImage, Root]);

        // etcd (внешний слой стенда). Фиксированный хост-порт + advertise
        // host.docker.internal: Patroni-ноды узнают адреса членов кластера из
        // advertise-client-urls — они обязаны быть достижимы ИЗ контейнеров.
        var etcdPort = FreePort();
        _etcd = new ContainerBuilder("quay.io/coreos/etcd:v3.5.21")
            .WithCommand(
                "etcd", "--name=e2e", "--data-dir=/etcd-data",
                "--listen-client-urls=http://0.0.0.0:2379",
                $"--advertise-client-urls=http://host.docker.internal:{etcdPort}")
            .WithPortBinding(etcdPort, 2379) // (hostPort, containerPort)
            .Build();
        var ct = TestContext.Current.CancellationToken;
        await _etcd.StartAsync(ct);
        EtcdEndpoint = $"http://localhost:{_etcd.GetMappedPublicPort(2379)}";
        Gateway = new EtcdGateway(new HttpClient());

        using var probeClient = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
        for (var i = 0; i < 30; i++)
        {
            try
            {
                using var probe = await probeClient.PostAsync(
                    EtcdEndpoint + "/v3/maintenance/status",
                    new StringContent("{}", Encoding.UTF8, "application/json"), ct);
                if (probe.IsSuccessStatusCode)
                    return;
            }
            catch (HttpRequestException)
            {
                // etcd ещё поднимается
            }

            await Task.Delay(1000, ct);
        }

        throw new InvalidOperationException($"etcd в {EtcdEndpoint} не поднялся за 30 c");
    }

    public async ValueTask DisposeAsync()
    {
        _healthHttp.Dispose();
        if (_etcd is not null)
            await _etcd.DisposeAsync();
    }

    /// <summary>Запуск инстанса PgWorker.App с e2e-конфигурацией (быстрые тики).</summary>
    public async Task<HostInstance> StartHostAsync(
        string name, int snapshotIntervalMin = 360, CancellationToken ct = default)
    {
        var port = FreePort();
        var snapshotsDir = Path.Combine(Path.GetTempPath(), $"pgw-e2e-{name}-{port}");
        Directory.CreateDirectory(snapshotsDir);

        var env = new Dictionary<string, string>
        {
            // Секреты установки (Д7).
            ["PGW_PG_SUPERUSER_PASSWORD"] = SuPassword,
            ["PGW_PG_STANDBY_PASSWORD"] = StandbyPassword,
            ["PGW_BUCKET_ADMIN_PASSWORD"] = BucketAdminPassword,
            ["PGW_BUCKET_MOVER_PASSWORD"] = MoverPassword,

            // Конфигурация (env-оверрайды appsettings): один docker-хост plain.
            // AdvertisedEndpoints: контейнеры нод ходят в etcd через docker-сеть
            // (host.docker.internal), а сам PgWorker — по localhost.
            ["PgWorker__Etcd__Endpoints__0"] = EtcdEndpoint,
            ["PgWorker__Etcd__AdvertisedEndpoints__0"] = EtcdEndpoint.Replace(
                "localhost:", "host.docker.internal:", StringComparison.Ordinal),
            ["PgWorker__Docker__Mode"] = "Plain",
            ["PgWorker__Docker__Hosts__0__Name"] = "localhost",
            ["PgWorker__Docker__Hosts__0__Endpoint"] = "unix:///var/run/docker.sock",
            ["PgWorker__Docker__PortRange__From"] = "15100",
            ["PgWorker__Docker__PortRange__To"] = "15200",
            ["PgWorker__Docker__Images__Node"] = NodeImage,
            ["PgWorker__Docker__EnableDoorman"] = "false",

            // Ускоренные циклы/пороги для e2e (критерии ждут секунды, не минуты).
            ["PgWorker__Loops__ScanIntervalSec"] = "1",
            ["PgWorker__Loops__KeepaliveSec"] = "1",
            ["PgWorker__Loops__ErrorDelayMs"] = "500",
            ["PgWorker__Loops__SnapshotIntervalMin"] = snapshotIntervalMin.ToString(),
            ["PgWorker__Thresholds__NodeDeadSec"] = "6",
            ["PgWorker__Thresholds__ShardDeadSec"] = "5",
            ["PgWorker__Thresholds__PatroniBootSec"] = "600",

            // Переезды (t01): spilo-18 → FailoverSlots=true (штатный путь PG17+,
            // R1/Д11); короткие
            // паузы заморозки/поллинга — окно FROZEN в e2e измеряется секундами;
            // AbortMinAgeSec=3 — abort-сценарий без долгого ожидания свежести.
            // AdvertisedPublisherHost: подписки ходят ИЗ контейнеров приёмников —
            // на single-host стенде издатель виден как host.docker.internal.
            ["PgWorker__Moves__FailoverSlots"] = "true",
            ["PgWorker__Moves__FreezeWaitSec"] = "1",
            ["PgWorker__Moves__PollIntervalSec"] = "1",
            ["PgWorker__Moves__AbortMinAgeSec"] = "3",
            ["PgWorker__Moves__AdvertisedPublisherHost"] = "host.docker.internal",
            ["PgWorker__Thresholds__CutoverTimeoutSec"] = "60",
            ["PgWorker__Thresholds__ConnFailBudgetSec"] = "15",
            ["PgWorker__Parallelism__MaxClusters"] = "2",
            ["PgWorker__Snapshots__Dir"] = snapshotsDir,
            ["PgWorker__Snapshots__RetentionFiles"] = "10",

            // HTTP API воркера (arch/14 §1.1): advertise-URL той же Kestrel-грани
            // (fail-fast при пустом — e2e-процесс обязан его задать).
            ["PgWorker__Api__AdvertiseUrl"] = $"http://127.0.0.1:{port}",

            ["ASPNETCORE_URLS"] = $"http://127.0.0.1:{port}",
            ["DOTNET_ENVIRONMENT"] = "Production",
        };

        var process = new Process
        {
            StartInfo = new ProcessStartInfo("dotnet", [AppDll])
            {
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                WorkingDirectory = snapshotsDir,
            },
        };
        foreach (var (key, value) in env)
            process.StartInfo.Environment[key] = value;

        if (!process.Start())
            throw new ApplicationException($"не удалось запустить инстанс {name}");

        // Читаем вывод в фоне (иначе буфер пайпа переполнится и процесс зависнет);
        // последние строки попадают в диагностику при неудачном старте, полный
        // лог — в host.log каталога снапшотов (writer живёт столько же, сколько
        // инстанс — закрывается в HostInstance.DisposeAsync).
        var tail = new Queue<string>();
        var logWriter = new StreamWriter(Path.Combine(snapshotsDir, "host.log"), append: false) { AutoFlush = true };
        process.OutputDataReceived += (_, e) => { Collect(tail, e.Data); if (e.Data is not null) logWriter.WriteLine(e.Data); };
        process.ErrorDataReceived += (_, e) => { Collect(tail, e.Data); if (e.Data is not null) logWriter.WriteLine(e.Data); };
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        var instance = new HostInstance(name, process, snapshotsDir, _healthHttp, logWriter);

        // Готовность: /healthz отвечает (любой статус, кроме 404 = маршрут жив).
        var ready = await WaitForAsync(async () =>
        {
            if (process.HasExited)
                throw new ApplicationException(
                    $"инстанс {name} упал при старте (exit {process.ExitCode}):\n{string.Join("\n", tail)}");
            try
            {
                using var response = await _healthHttp.GetAsync(
                    $"http://127.0.0.1:{port}/healthz", CancellationToken.None);
                return response.StatusCode != HttpStatusCode.NotFound;
            }
            catch (HttpRequestException)
            {
                return false;
            }
        }, TimeSpan.FromSeconds(30), ct);
        if (!ready)
        {
            await instance.DisposeAsync();
            throw new ApplicationException($"инстанс {name} не поднялся за 30 с:\n{string.Join("\n", tail)}");
        }

        return instance;
    }

    /// <summary>
    /// Пароль app-роли кластера из etcd (spec §3.1): e2e-сценарии читают секрет
    /// тем же путём, что и приложение — /clusters/&lt;C&gt;/app_password.
    /// </summary>
    public async Task<string> GetAppPasswordAsync(string cluster, CancellationToken ct = default)
    {
        var result = await Gateway.GetAsync(EtcdEndpoint, $"/clusters/{cluster}/app_password", ct);
        result.IsSuccess.Should().BeTrue("app-секрет обязан появиться после provisioning");
        return result.Value!.Value;
    }

    private static void Collect(Queue<string> tail, string? line)
    {
        if (line is null)
            return;
        lock (tail)
        {
            tail.Enqueue(line);
            while (tail.Count > 200)
                tail.Dequeue();
        }
    }

    // Хелпер ожидания условия (полл 500 мс).
    public static async Task<bool> WaitForAsync(
        Func<Task<bool>> condition, TimeSpan timeout, CancellationToken ct = default)
    {
        var deadline = Stopwatch.StartNew();
        while (deadline.Elapsed < timeout)
        {
            if (await condition())
                return true;
            await Task.Delay(500, ct);
        }

        return false;
    }

    public Task<string> RunDockerAsync(string[] args, CancellationToken ct = default)
        => RunProcessAsync("docker", args, ct);

    private static async Task<string> RunProcessAsync(string file, string[] args, CancellationToken ct = default)
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
        var output = await process.StandardOutput.ReadToEndAsync(ct);
        var error = await process.StandardError.ReadToEndAsync(ct);
        await process.WaitForExitAsync(ct);
        if (process.ExitCode != 0)
            throw new ApplicationException($"{file} {string.Join(' ', args)} → {process.ExitCode}: {error.Trim()}");
        return output.Trim();
    }

    private static int FreePort()
    {
        var listener = System.Net.Sockets.TcpListener.Create(0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    // Корень репозитория: первый каталог вверх с docker/node/Dockerfile.
    private static string FindRoot(string start)
    {
        var dir = new DirectoryInfo(start);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "docker", "node", "Dockerfile")))
            dir = dir.Parent;
        return dir?.FullName
            ?? throw new ApplicationException("корень репозитория (docker/node/Dockerfile) не найден");
    }
}

/// <summary>Запущенный инстанс PgWorker.App (имя, процесс, каталог снапшотов).</summary>
public sealed class HostInstance(
    string name,
    Process process,
    string snapshotsDir,
    HttpClient healthHttp,
    StreamWriter? logWriter = null) : IAsyncDisposable
{
    public string Name { get; } = name;

    public Process Process { get; } = process;

    public string SnapshotsDir { get; } = snapshotsDir;

    /// <summary>Мгновенный kill (смерть контроллера, AC3) — lease истекают ≤15 с.</summary>
    public void Kill()
    {
        try
        {
            Process.Kill(entireProcessTree: true);
        }
        catch (InvalidOperationException)
        {
            // уже завершён
        }
    }

    public async ValueTask DisposeAsync()
    {
        Kill();
        try
        {
            await Process.WaitForExitAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10), CancellationToken.None);
        }
        catch (TimeoutException)
        {
            // не дождались — Process.Dispose разорвёт дескрипторы
        }
        catch (InvalidOperationException)
        {
            // процесс уже заверён сам (крах/kill ранее) — дескрипторы чистит Dispose
        }

        logWriter?.Dispose();
        Process.Dispose();
        _ = healthHttp; // dispose делает фикстура
    }
}
