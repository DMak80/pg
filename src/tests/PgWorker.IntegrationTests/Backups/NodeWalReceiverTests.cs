using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using PgWorker.IntegrationTests.Docker;
using PgWorker.IntegrationTests.E2e;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Sanity-прогон образа pgworker-node с приёмником WAL (spec §3.2/§6 п.2;
// docker, динамические порты, guid-окружение, полный teardown при любом исходе
// + ассерт чистоты). Кейсы:
// 1. Приёмник жив под supervisord (RUNNING), /metrics внутри контейнера отдаёт
//    словарь arch/18 §2.8 (минимум walreceiver_streaming), в etcd появляется
//    ключ /pgworker/backups/<C>/<X>/agent/<N> с "state":"waiting_role" (REST
//    ноды не поднят — роль none). SIGKILL приёмника → supervisord перезапускает
//    (падение ≠ exit 0), ключ возвращается свежим lease-put'ом нового процесса.
// 2. Без env идентичности — старт-отказ процесса (exit 1), supervisord НЕ лупит
//    (автозапуски не повторяются — состояние FATAL/EXITED стабильно).
public class NodeWalReceiverTests
{
    // Per-worktree тег (канон E2eEnvironment): чужой/устаревший образ другой
    // ветки в серию не попадает; свежесть гарантирует сборка на прогон (ниже).
    private static readonly string NodeImage = $"pgworker-node:sanity-{E2eEnvironment.ImageTagSuffix}";
    private const string EtcdImage = "quay.io/coreos/etcd:v3.5.21";
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(90);

    private readonly ITestOutputHelper _output;

    public NodeWalReceiverTests(ITestOutputHelper output) => _output = output;

    // 9.5 (ревью): серия зелёная на чистом хосте и всегда на свежем бинаре —
    // образ собирается ДВУХШАГОВО на КАЖДЫЙ прогон серии (один раз за ран, канон
    // «publish на хосте, в контейнер — только артефакты»): (1) self-contained
    // publish WalReceiver/NodeTools (RID хоста, без trimming; инкрементальный)
    // в artifacts/node-wal-dev и artifacts/node-tools-dev; (2) docker build ноды
    // с publish-аргументами (docker-кэш слоёв делает ребилд быстрым).
    // inspect-гейта нет: `docker image inspect` отсутствующего образа даёт
    // exit 1 → RunProcessAsync бросает — fallback-ветка была недостижима.
    // Фазы — [PHASE]-метки и тайминг (канон e2e-launch); логи процессов — в файлы.
    private static readonly SemaphoreSlim ImageGate = new(1, 1);
    private static bool _nodeImageReady;

    private static async Task EnsureNodeImageAsync(CancellationToken ct)
    {
        if (_nodeImageReady)
            return;
        await ImageGate.WaitAsync(ct);
        try
        {
            if (_nodeImageReady)
                return;

            var root = E2eFixture.FindRoot(AppContext.BaseDirectory);
            var walDir = Path.Combine(root, "artifacts", "node-wal-dev");
            var toolsDir = Path.Combine(root, "artifacts", "node-tools-dev");
            var rid = System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture
                == System.Runtime.InteropServices.Architecture.Arm64 ? "linux-arm64" : "linux-x64";
            Console.WriteLine($"[PHASE] node-image: host publish WalReceiver ({rid})…");
            var sw = System.Diagnostics.Stopwatch.StartNew();
            await E2eFixture.RunProcessAsync("dotnet",
            [
                "publish", $"{root}/src/PgWorker.WalReceiver/PgWorker.WalReceiver.csproj",
                "-c", "Release", "-r", rid, "--self-contained", "-o", walDir, "--nologo",
            ], ct, timeout: TimeSpan.FromMinutes(10), env: E2eFixture.NoMsBuildReuseEnv,
                logFile: "/tmp/pgw-node-wal-dev-publish.log");
            Console.WriteLine("[PHASE] node-image: dotnet publish NodeTools self-contained…");
            await E2eFixture.RunProcessAsync("dotnet",
            [
                "publish", $"{root}/src/PgWorker.NodeTools/PgWorker.NodeTools.csproj",
                "-c", "Release", "-r", rid, "--self-contained", "-o", toolsDir, "--nologo",
            ], ct, timeout: TimeSpan.FromMinutes(10), env: E2eFixture.NoMsBuildReuseEnv,
                logFile: "/tmp/pgw-node-tools-dev-publish.log");
            Console.WriteLine($"[PHASE] node-image: publish ready in {sw.Elapsed.TotalSeconds:F0} s — docker build (context root)…");
            await E2eFixture.RunProcessAsync("docker",
            [
                "build", "-q", "-f", $"{root}/docker/node/Dockerfile",
                "--build-arg", "WAL_PUBLISH_DIR=artifacts/node-wal-dev",
                "--build-arg", "NODE_TOOLS_PUBLISH_DIR=artifacts/node-tools-dev", "-t", NodeImage, root,
            ], ct, timeout: TimeSpan.FromMinutes(10),
                logFile: "/tmp/pgw-node-wal-dev-docker.log");
            Console.WriteLine($"[PHASE] node-image: {NodeImage} ready in {sw.Elapsed.TotalSeconds:F0} s");

            _nodeImageReady = true;
        }
        finally
        {
            ImageGate.Release();
        }
    }

    // Ожидание условия с бюджетом и печатной диагностикой при фейле.
    private async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan budget, Func<Task<string>>? debug = null)
    {
        var deadline = DateTime.UtcNow + budget;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
                return;
            await Task.Delay(500, TestContext.Current.CancellationToken);
        }

        var info = debug is null ? "none" : await debug();
        Assert.Fail($"the condition was not met within the {budget.TotalSeconds} s budget; debug: {info}");
    }

    // docker-вывод (logs/inspect) — через CLI (паттерн E2eFixture.RunProcessAsync).
    private Task<string> DockerAsync(params string[] args)
        => E2eFixture.RunProcessAsync("docker", args, TestContext.Current.CancellationToken);

    /// <summary>Статус программы в supervisord ноды (docker exec supervisorctl).</summary>
    private static async Task<string> SupervisordStatusAsync(IContainer node, string program, CancellationToken ct)
    {
        var result = await node.ExecAsync(["supervisorctl", "status", program], ct: ct);
        return $"{result.ExitCode}|{result.Stdout}|{result.Stderr}";
    }

    [Fact]
    public async Task ReceiverRuns_StatusKeyAndMetrics_LeaseRecoversAfterKill()
    {
        // Arrange — своё окружение: сеть + etcd (alias) + нода pgworker-node:dev
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await EnsureNodeImageAsync(ct);
        var runId = Guid.NewGuid().ToString("N");
        var netName = $"pgw-node-net-{runId}";
        var etcdName = $"pgw-node-etcd-{runId}";
        var nodeName = $"pgw-node-{runId}";
        var cluster = $"c{runId[..8]}";
        var agentKey = $"/pgworker/backups/{cluster}/shard1/agent/s1a";

        await using var network = new NetworkBuilder().WithName(netName).Build();
        await network.CreateAsync(ct);
        await using var etcd = new ContainerBuilder(EtcdImage)
            .WithName(etcdName)
            .WithNetwork(network)
            .WithNetworkAliases("etcd-test")
            .WithCommand(
                "etcd", "--name=e2e", "--data-dir=/etcd-data",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://etcd-test:2379")
            .WithPortBinding(2379, assignRandomHostPort: true)
            .Build();
        await etcd.StartAsync(ct);
        var hostEndpoint = $"http://localhost:{etcd.GetMappedPublicPort(2379)}";

        await using var node = new ContainerBuilder(NodeImage)
            .WithName(nodeName)
            .WithNetwork(network)
            .WithEnvironment(new Dictionary<string, string>
            {
                ["PGW_BACKUP_CLUSTER"] = cluster,
                ["PGW_BACKUP_SHARD"] = "shard1",
                ["PGW_BACKUP_NODE"] = "s1a",
                ["PGW_ETCD"] = "http://etcd-test:2379",
                ["PGW_BACKUP_S3_BUCKET"] = "backups",
            })
            .Build();
        await node.StartAsync(ct);

        var gateway = new EtcdGateway(new HttpClient { Timeout = TimeSpan.FromSeconds(10) });

        try
        {
            // Act/Assert — приёмник жив: supervisord доводит его до RUNNING
            // (startsecs=5); спило-шум контейнера не важен — проверяем программу.
            await WaitUntilAsync(
                async () => (await SupervisordStatusAsync(node, "walreceiver", ct)).Contains("RUNNING"),
                Budget,
                async () => await SupervisordStatusAsync(node, "walreceiver", ct));

            // Assert — статус-ключ агента в etcd: waiting_role (REST ноды не
            // поднят — роль none; гейты/пароль/policy отсутствуют — но роль
            // проверяется ПЕРВЫЙ, ожидание waiting_role канонично)
            await WaitUntilAsync(
                async () =>
                {
                    var read = await gateway.GetAsync(hostEndpoint, agentKey, ct);
                    return read.IsSuccess && read.Value is { } kv
                           && kv.Value.Contains("\"state\":\"waiting_role\"", StringComparison.Ordinal);
                },
                Budget,
                async () => $"logs=[{await DockerAsync("logs", "--tail", "40", nodeName)}]");

            // Assert — /metrics внутри контейнера отдаёт словарь arch/18 §2.8
            // (минимум walreceiver_streaming — серия есть при любом состоянии)
            var metrics = await node.ExecAsync(
                ["curl", "-fsS", "http://127.0.0.1:8080/metrics"], ct: ct);
            metrics.Stdout.Should().Contain("walreceiver_streaming",
                "the receiver exposes its metric dictionary inside the container");

            // Act — SIGKILL приёмника: supervisord перезапускает (падение ≠ exit 0).
            // pkill -9 (не SIGTERM): graceful SIGTERM — exit 0, supervisord его НЕ
            // перезапускает (exitcodes=0); контракт кейса — рестарт ПАДЕНИЯ.
            var statusBefore = await SupervisordStatusAsync(node, "walreceiver", ct);
            var kill = await node.ExecAsync(["pkill", "-9", "-f", "/opt/pgw-wal/walreceiver"], ct: ct);
            kill.ExitCode.Should().Be(0, $"pkill must find the receiver process; status before: {statusBefore}");

            // Assert — процесс поднят supervisord'ом заново: ключ возвращается
            // свежим lease-put'ом нового процесса (updated_unix новее убитого).
            // Быстрый рестарт (~5–10 c) перекрывает TTL 15 c — окно «ключа нет»
            // может не наблюдаться: старый lease гаснет по TTL уже ПОСЛЕ
            // перезаписи новым put'ом; наблюдаемый контракт — ключ жив и
            // обновлён новым процессом.
            var updatedBefore = await ReadUpdatedUnixAsync(gateway, hostEndpoint, agentKey, ct);
            await WaitUntilAsync(
                async () =>
                {
                    var updated = await ReadUpdatedUnixAsync(gateway, hostEndpoint, agentKey, ct);
                    return updated is { } u && u > updatedBefore;
                },
                Budget,
                async () => $"updated_before={updatedBefore} logs=[{await DockerAsync("logs", "--tail", "40", nodeName)}]");

            // Assert — supervisord перезапустил процесс: после startsecs=5 статус
            // снова RUNNING (сразу после kill — STARTING, ждём перехода)
            await WaitUntilAsync(
                async () => (await SupervisordStatusAsync(node, "walreceiver", ct)).Contains("RUNNING"),
                Budget,
                async () => await SupervisordStatusAsync(node, "walreceiver", ct));
        }
        finally
        {
            // Teardown при любом исходе + ассерт чистоты (свои pgw-node-*)
            await node.DisposeAsync();
            await etcd.DisposeAsync();
            await network.DeleteAsync(ct);
        }

        var left = await DockerAsync(
            "ps", "-a", "--format", "{{.Names}}", "--filter", $"name={runId}");
        left.Should().BeEmpty($"{runId}: node sanity teardown is incomplete");
    }

    private static async Task<long?> ReadUpdatedUnixAsync(
        EtcdGateway gateway, string endpoint, string key, CancellationToken ct)
    {
        var read = await gateway.GetAsync(endpoint, key, ct);
        if (!read.IsSuccess || read.Value is not { } kv)
            return null;
        var parsed = PgWorker.WalReceiver.WalReceiverAgentStatusJson.Parse(kv.Value);
        return parsed?.Status.UpdatedUnix;
    }

    // 9.2 (ревью, arch/19 §4 «ключ жив = приёмник жив»): supervisorctl stop
    // walreceiver — процесс получает SIGTERM (graceful: lease revoke — ключ
    // гаснет сразу), ПОТОЛОК ожидания — lease TTL 15 c; supervisorctl start —
    // ключ возвращается свежим процессом. Смерть процесса не оставляет
    // фантомного приёмника в etcd дольше TTL.
    [Fact]
    public async Task SupervisorStop_AgentKeyGoesOutWithinTtl_StartBringsItBack()
    {
        // Arrange — окружение первого кейса: сеть + etcd + нода с приёмником
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await EnsureNodeImageAsync(ct);
        var runId = Guid.NewGuid().ToString("N");
        var netName = $"pgw-node-net-{runId}";
        var etcdName = $"pgw-node-etcd-{runId}";
        var nodeName = $"pgw-node-{runId}";
        var cluster = $"c{runId[..8]}";
        var agentKey = $"/pgworker/backups/{cluster}/shard1/agent/s1a";

        await using var network = new NetworkBuilder().WithName(netName).Build();
        await network.CreateAsync(ct);
        await using var etcd = new ContainerBuilder(EtcdImage)
            .WithName(etcdName)
            .WithNetwork(network)
            .WithNetworkAliases("etcd-test")
            .WithCommand(
                "etcd", "--name=e2e", "--data-dir=/etcd-data",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://etcd-test:2379")
            .WithPortBinding(2379, assignRandomHostPort: true)
            .Build();
        await etcd.StartAsync(ct);
        var hostEndpoint = $"http://localhost:{etcd.GetMappedPublicPort(2379)}";

        await using var node = new ContainerBuilder(NodeImage)
            .WithName(nodeName)
            .WithNetwork(network)
            .WithEnvironment(new Dictionary<string, string>
            {
                ["PGW_BACKUP_CLUSTER"] = cluster,
                ["PGW_BACKUP_SHARD"] = "shard1",
                ["PGW_BACKUP_NODE"] = "s1a",
                ["PGW_ETCD"] = "http://etcd-test:2379",
                ["PGW_BACKUP_S3_BUCKET"] = "backups",
            })
            .Build();
        await node.StartAsync(ct);

        var gateway = new EtcdGateway(new HttpClient { Timeout = TimeSpan.FromSeconds(10) });

        try
        {
            // Arrange — приёмник RUNNING, статус-ключ жив (waiting_role)
            await WaitUntilAsync(
                async () => (await SupervisordStatusAsync(node, "walreceiver", ct)).Contains("RUNNING"),
                Budget,
                async () => await SupervisordStatusAsync(node, "walreceiver", ct));
            await WaitUntilAsync(
                async () =>
                {
                    var read = await gateway.GetAsync(hostEndpoint, agentKey, ct);
                    return read.IsSuccess && read.Value is { } kv
                           && kv.Value.Contains("\"state\":\"waiting_role\"", StringComparison.Ordinal);
                },
                Budget,
                async () => $"logs=[{await DockerAsync("logs", "--tail", "40", nodeName)}]");

            // Act — supervisorctl stop: SIGTERM процессу (не kill: graceful-путь
            // воркера — revoke lease; TTL 15 c — потолок при любом исходе)
            var stop = await node.ExecAsync(["supervisorctl", "stop", "walreceiver"], ct: ct);
            stop.ExitCode.Should().Be(0, $"supervisorctl stop must succeed: {stop.Stdout} {stop.Stderr}");

            // Assert — ключ гаснет ≤ TTL 15 c (graceful revoke делает это
            // практически мгновенно; TTL-потолок закрывает и его потерю)
            await WaitUntilAsync(
                async () =>
                {
                    var read = await gateway.GetAsync(hostEndpoint, agentKey, ct);
                    return read.IsSuccess && read.Value is null;
                },
                TimeSpan.FromSeconds(15),
                async () =>
                    $"key={(await gateway.GetAsync(hostEndpoint, agentKey, ct)).Value?.Value ?? "<absent>"} " +
                    $"status={await SupervisordStatusAsync(node, "walreceiver", ct)} " +
                    $"logs=[{await DockerAsync("logs", "--tail", "40", nodeName)}]");

            // Act — supervisorctl start: процесс поднят, ключ вернулся
            var start = await node.ExecAsync(["supervisorctl", "start", "walreceiver"], ct: ct);
            start.ExitCode.Should().Be(0, $"supervisorctl start must succeed: {start.Stdout} {start.Stderr}");
            await WaitUntilAsync(
                async () =>
                {
                    var read = await gateway.GetAsync(hostEndpoint, agentKey, ct);
                    return read.IsSuccess && read.Value is { } kv
                           && kv.Value.Contains("\"state\":\"waiting_role\"", StringComparison.Ordinal);
                },
                Budget,
                async () => $"logs=[{await DockerAsync("logs", "--tail", "40", nodeName)}]");
        }
        finally
        {
            // Teardown при любом исходе + ассерт чистоты (свои pgw-node-*)
            await node.DisposeAsync();
            await etcd.DisposeAsync();
            await network.DeleteAsync(ct);
        }

        var left = await DockerAsync(
            "ps", "-a", "--format", "{{.Names}}", "--filter", $"name={runId}");
        left.Should().BeEmpty($"{runId}: node sanity teardown is incomplete");
    }

    [Fact]
    public async Task NoIdentityEnv_StartFails_NoRestartLoop()
    {
        // Arrange — нода без env идентичности (PGW_BACKUP_CLUSTER отсутствует)
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await EnsureNodeImageAsync(ct);
        var runId = Guid.NewGuid().ToString("N");
        var nodeName = $"pgw-node-{runId}";

        await using var node = new ContainerBuilder(NodeImage)
            .WithName(nodeName)
            .Build();
        await node.StartAsync(ct);

        try
        {
            // Act/Assert — приёмник вышел (старт-отказ), supervisord НЕ лупит:
            // статус FATAL/EXITED (не RUNNING) и стабилен во времени
            await WaitUntilAsync(
                async () =>
                {
                    var status = await SupervisordStatusAsync(node, "walreceiver", ct);
                    return status.Contains("FATAL") || status.Contains("EXITED");
                },
                Budget,
                async () => $"logs=[{await DockerAsync("logs", "--tail", "40", nodeName)}]");

            // Assert — диагностика старт-отказа ровно ОДИН раз в stdout контейнера
            // (повторные автозапуски размножили бы строку — лупинг исключён);
            // через 5 c состояние то же (не перезапущено).
            await Task.Delay(TimeSpan.FromSeconds(5), ct);
            (await SupervisordStatusAsync(node, "walreceiver", ct))
                .Should().NotContain("RUNNING", "exit 1 is not restarted (exitcodes=0)");
            var logs = await DockerAsync("logs", nodeName);
            System.Text.RegularExpressions.Regex.Matches(logs, "invalid identity env")
                .Should().HaveCount(1, "the start-failure is not restarted by supervisord");
        }
        finally
        {
            // Teardown при любом исходе + ассерт чистоты
            await node.DisposeAsync();
        }

        var left = await DockerAsync(
            "ps", "-a", "--format", "{{.Names}}", "--filter", $"name={runId}");
        left.Should().BeEmpty($"{runId}: node sanity teardown is incomplete");
    }
}
