using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Core.Templates;
using PgWorker.Docker.Drivers;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.E2e;
using PgWorker.Provisioning.Probes;
using PgWorker.Provisioning.Processes;
using Shared.Etcd.Client;
using Xunit;
using ISqlExecutor = PgWorker.Provisioning.Sql.ISqlExecutor;

namespace PgWorker.IntegrationTests.Docker;

// Общий контур docker-серии REST-TLS (t22): живой etcd фикстуры + РЕАЛЬНЫЙ
// PlainClusterDriver (unix-сокет; alpine-заглушки нод — НЕ spilo, имитация
// легаси-подъёма) + процессы напрямую (NodeSupervisor/ClusterSecretRotator).
// Окружение per-guid (e2e-isolation §1): контейнеры/volume/сеть/etcd-префиксы
// own-only; teardown с ассертом чистоты.
internal static class RestTlsDockerRig
{
    public const string PythonImage = "python:3.12-alpine";

    public sealed class Contour : IAsyncDisposable
    {
        public required string Cluster { get; init; }
        public required EtcdGateway Gateway { get; init; }
        public required string Endpoint { get; init; }
        public required PlainClusterDriver Driver { get; init; }
        public required NodeSupervisor Supervisor { get; init; }
        public required ClusterSecretRotator Rotator { get; init; }
        public required WorkJournal Journal { get; init; }
        public required string ContainerA { get; init; }
        public required string VolumeA { get; init; }
        public required string StubImage { get; init; }
        public required string BuildContext { get; init; }

        public async ValueTask DisposeAsync()
        {
            var ct = CancellationToken.None;
            await E2eFixture.RunDockerAsync(["rm", "-f", ContainerA,
                $"pgw-{Cluster}-shard1-shard1b"], ct);
            await E2eFixture.RunDockerAsync(["volume", "rm", "-f", VolumeA,
                $"pgw-{Cluster}-shard1-shard1b-data"], ct);
            await E2eFixture.RunDockerAsync(["network", "rm", $"pgw-net-{Cluster}"], ct);
            await E2eFixture.RunDockerAsync(["rmi", "-f", StubImage], ct);
            try
            {
                Directory.Delete(BuildContext, recursive: true);
            }
            catch (DirectoryNotFoundException)
            {
                // контекст уже убран — идемпотентность teardown
            }
            await Gateway.DeleteAsync(Endpoint, $"/clusters/{Cluster}/", prefix: true, ct);
            await Gateway.DeleteAsync(Endpoint, $"/pgworker/portalloc/{Cluster}", prefix: false, ct);
            await Gateway.DeleteAsync(Endpoint, $"/pgworker/work/{Cluster}", prefix: false, ct);
            await Gateway.DeleteAsync(Endpoint, $"/pgworker/claims/{Cluster}", prefix: false, ct);
            await Gateway.DeleteAsync(Endpoint, $"/pgworker/rotations/{Cluster}", prefix: false, ct);
            await Gateway.DeleteAsync(Endpoint, $"/service/{Cluster}-shard1/", prefix: true, ct);

            // Ассерт чистоты (e2e-isolation): ни контейнеров, ни volume.
            var left = await E2eFixture.RunDockerAsync(
                ["ps", "-aq", "--filter", $"name=pgw-{Cluster}-"], ct);
            left.Trim().Should().BeEmpty("контур полностью зачищен");
            var volumes = await E2eFixture.RunDockerAsync(
                ["volume", "ls", "-q", "--filter", $"name=pgw-{Cluster}-"], ct);
            volumes.Trim().Should().BeEmpty("volume префикса удалены");
        }
    }

    // Зонд свободного хост-порта (динамические порты — канон e2e-isolation §5).
    public static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    // Стаб Patroni-проб: 200 members running — пробы надзора зелёные (окно
    // миграции без UNREACHABLE-переходов, спека §5.5).
    private sealed class AliveHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
            => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    """{"members":[{"name":"shard1a","role":"replica","state":"running"},{"name":"shard1b","role":"replica","state":"running"}]}""",
                    Encoding.UTF8, "application/json"),
            });
    }

    public static async Task<Contour> BuildAsync(Etcd.EtcdFixture fixture)
    {
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"c{Guid.NewGuid():N}"[..11];
        var containerA = $"pgw-{cluster}-shard1-shard1a";
        var volumeA = $"{containerA}-data";
        var endpoint = fixture.Endpoint;
        var gateway = fixture.Gateway;

        // Заглушка-контейнер легаси-ноды: alpine (НЕ spilo), env урезан —
        // имитация контейнера, поднятого до t22; volume с data-маркером.
        await E2eFixture.RunDockerAsync(["volume", "create", volumeA], ct);
        await E2eFixture.RunDockerAsync(["run", "-d", "--name", containerA,
            "-v", $"{volumeA}:/home/postgres/pgdata",
            "-e", "SCOPE=stub", "-e", "PGW_ETCD=http://stub",
            "-e", $"PGW_MASTER_KEY=/clusters/{cluster}/shards/shard1/master",
            "-e", "PGW_NODE_HOST=local", "-e", "PGW_NODE_NAME=shard1a",
            PythonImage, "sh", "-c",
            "mkdir -p /home/postgres/pgdata && touch /home/postgres/pgdata/MARKER && sleep 600"], ct);

        // Сид контроль-плейна (живой etcd).
        await gateway.PutAsync(endpoint, $"/clusters/{cluster}/config",
            $$"""{"buckets":2,"dbname":"{{cluster}}","created_unix":1755900000}""", null, ct);
        await gateway.PutAsync(endpoint, $"/clusters/{cluster}/shards/shard1/replicas", "2", null, ct);
        await gateway.PutAsync(endpoint, $"/clusters/{cluster}/shards/shard1/dsn",
            $"host=local,local port=15000,15001 dbname={cluster} user=bucket_admin password=x", null, ct);
        await gateway.PutAsync(endpoint, $"/clusters/{cluster}/shards/shard1/nodes/shard1a/state", "RUNNING", null, ct);
        await gateway.PutAsync(endpoint, $"/clusters/{cluster}/shards/shard1/nodes/shard1b/state", "RUNNING", null, ct);
        await gateway.PutAsync(endpoint, $"/clusters/{cluster}/shards/shard1/master", "shard1a", null, ct);
        await gateway.PutAsync(endpoint, $"/service/{cluster}-shard1/request_cpu", "2", null, ct);
        await gateway.PutAsync(endpoint, $"/service/{cluster}-shard1/request_mem", "8Gi", null, ct);

        // Portalloc: динамические порты (зонд свободных).
        var portalloc = new Dictionary<string, NodeAddress>
        {
            ["shard1/shard1a"] = new("local", new NodePorts(FreePort(), FreePort(), 0)),
            ["shard1/shard1b"] = new("local", new NodePorts(FreePort(), FreePort(), 0)),
        };
        await gateway.PutAsync(endpoint, $"/pgworker/portalloc/{cluster}",
            PgWorker.Core.Model.Portalloc.Serialize(portalloc), null, ct);

        // Реальный драйвер: unix-сокет, node-image — СТАБ-ОБРАЗ (CMD sleep
        // infinity: python:3.12-alpine с default-CMD python3 гаснет сразу, а
        // реальный образ ноды держит supervisord); кеш сертов — тестовый CA
        // (выпуск REST-сертов реальный).
        var (caPem, caKeyPem) = E2eTestPki.GenerateCa("it-rest");
        var stubImage = $"pgw-reststub:{Guid.NewGuid():N}";
        var buildContext = Directory.CreateTempSubdirectory("pgw-reststub-");
        await File.WriteAllTextAsync(Path.Combine(buildContext.FullName, "Dockerfile"),
            "FROM python:3.12-alpine\nCMD [\"sh\", \"-c\", \"sleep infinity\"]\n", ct);
        await E2eFixture.RunProcessAsync(
            "docker", ["build", "-q", "-t", stubImage, buildContext.FullName], ct,
            timeout: TimeSpan.FromSeconds(60));
        var driver = new PlainClusterDriver(
            [new HostEndpoint("local", "unix:///var/run/docker.sock")],
            new DockerEngineFactory(),
            enableDoorman: false,
            nodeImage: stubImage,
            restCertificates: new RestCertificateCache(caPem, caKeyPem));

        var claims = new ClaimStore("/pgworker", [endpoint], gateway, TimeProvider.System);
        var journal = new WorkJournal("/pgworker", gateway, [endpoint]);
        var probe = new ShardProbe(new HttpClient(new AliveHandler()) { Timeout = TimeSpan.FromSeconds(3) });
        var secrets = new InstallSecrets("su", "sb", "adm", "mov");
        var ensurer = new ClusterSecretEnsurer(gateway, [endpoint]);
        var pgtuneSettings = new PgtuneSettings(18, "oltp", "ssd", "mid_ram", 60,
            new HashSet<string>(["io_method", "io_workers"], StringComparer.Ordinal));
        var supervisor = new NodeSupervisor(
            gateway, [endpoint], driver, probe, new StubSql(), claims, journal,
            new ThresholdsOptions(90, 300, 300), TimeProvider.System, secrets,
            ensurer,
            new AppParamsEnsurer(gateway, [endpoint], "sslmode=require"),
            new PgtuneInputsFactory(pgtuneSettings, NullLogger<PgtuneInputsFactory>.Instance),
            pgtuneSettings,
            NullLogger<NodeSupervisor>.Instance);
        var rotator = new ClusterSecretRotator(
            gateway, [endpoint], new StubSql(), driver, probe, claims, journal,
            secrets, ensurer);

        (await claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue("клэйм контура");
        return new Contour
        {
            Cluster = cluster,
            Gateway = gateway,
            Endpoint = endpoint,
            Driver = driver,
            Supervisor = supervisor,
            Rotator = rotator,
            Journal = journal,
            ContainerA = containerA,
            VolumeA = volumeA,
            StubImage = stubImage,
            BuildContext = buildContext.FullName,
        };
    }

    // Снапшот кластера из живого etcd.
    public static async Task<ClusterSnapshot> SnapshotAsync(Contour c)
    {
        var range = await c.Gateway.RangeAsync(c.Endpoint, "/clusters/", CancellationToken.None);
        var parsed = ClusterSnapshotParser.ParseClusters(range.Value, out _);
        return parsed.Value.Single(x => x.Config.Cluster == c.Cluster);
    }

    // SQL-стаб (SQL-механика мокается — тестируется rest-грань).
    public sealed class StubSql : ISqlExecutor
    {
        public Task<Result> ExecuteAsync(string dsn, string sql, CancellationToken ct)
            => Task.FromResult(Result.Success());

        public Task<Result<object?>> ExecuteScalarAsync(string dsn, string sql, CancellationToken ct)
            => Task.FromResult(Result<object?>.Success(null));

        public Task<Result> EnsureDatabaseAsync(string dsn, string dbname, CancellationToken ct)
            => Task.FromResult(Result.Success());
    }
}

// REST-TLS-конвергенция живого контейнера (t22, arch/14 §5 C): легаси-нода
// пересоздаётся шагом надзора с REST-TLS env; volume/маркер на месте;
// rest_password появился ensure'м; повторный тик no-op; окно миграции без
// rebuild/UNREACHABLE (спека §5.5). Гейт PGW_TEST_DOCKER=1.
[Collection(NonE2eCollection.Name)]
public class RestTlsConvergenceTests(Etcd.EtcdFixture fixture)
{
    [Fact]
    public async Task Convergence_LegacyContainer_RecreatedWithTlsEnv()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        await using var c = await RestTlsDockerRig.BuildAsync(fixture);
        var startedAt = DateTime.UtcNow;

        // Act: цикл тиков надзора до no-op шага (поллинг ≤6 тиков — каждый
        // создаёт/сносит по альпийской заглушке за секунды).
        var migrated = false;
        var containerId = "";
        for (var i = 0; i < 6 && !migrated; i++)
        {
            var snap = await RestTlsDockerRig.SnapshotAsync(c);
            var tick = await c.Supervisor.TickAsync(snap, null, ct);
            tick.IsSuccess.Should().BeTrue(tick.Error?.ToString());

            var env = await c.Driver.InspectNodeEnvAsync(c.Cluster, "shard1", "shard1a", ct);
            env.Value.Should().NotBeNull();
            migrated = env.Value.ContainsKey(RestRotation.EnvCert);
            if (migrated)
                containerId = await ContainerIdAsync(c.ContainerA);
        }

        // Assert: контейнер пересоздан (создан после старта теста) и несёт
        // REST-TLS env: серт из CA + hash пароля из etcd (ensure-результат).
        migrated.Should().BeTrue("шаг пересоздания обязан довести легаси-ноду до TLS-env");
        containerId.Should().NotBeNullOrEmpty();
        var createdAt = await ContainerCreatedAtAsync(c.ContainerA);
        createdAt.Should().BeOnOrAfter(startedAt.AddSeconds(-5), "контейнер пересоздан шагом");
        var finalEnv = await c.Driver.InspectNodeEnvAsync(c.Cluster, "shard1", "shard1a", ct);
        finalEnv.Value.Should().ContainKey(RestRotation.EnvCert);
        finalEnv.Value[RestRotation.EnvCert].Should().StartWith("-----BEGIN CERTIFICATE-----");
        var restKey = (await c.Gateway.GetAsync(c.Endpoint, $"/clusters/{c.Cluster}/rest_password", ct)).Value;
        restKey.Should().NotBeNull("ensure положил седьмой ключ");
        finalEnv.Value[RestRotation.EnvPasswordHash]
            .Should().Be(RestRotation.PasswordHash(restKey!.Value),
                "hash — пароля из результата ensure (не снапшота)");

        // Volume и data-маркер на месте (пересоздание volume сохраняет).
        var marker = await E2eFixture.RunDockerAsync(
            ["exec", c.ContainerA, "test", "-f", "/home/postgres/pgdata/MARKER"], ct);
        marker.Should().BeEmpty("MARKER пережил пересоздание (docker exec без вывода = код 0)");

        // Окно миграции (спека §5.5): без rebuild-фаз и UNREACHABLE-переходов.
        var work = (await c.Journal.ReadAsync(c.Cluster, ct)).Value;
        work.Should().NotBeNull();
        work!.Unreachable.Should().BeNullOrEmpty("пробы зелёные — трек пуст");
        work.Phase.Should().NotMatch("*rebuild*");
        var stateA = (await c.Gateway.GetAsync(c.Endpoint,
            $"/clusters/{c.Cluster}/shards/shard1/nodes/shard1a/state", ct)).Value;
        stateA!.Value.Should().Be("RUNNING", "state не покидал RUNNING");

        // Повторный тик — no-op: ID контейнера не изменился.
        var idBefore = containerId;
        var snap2 = await RestTlsDockerRig.SnapshotAsync(c);
        (await c.Supervisor.TickAsync(snap2, null, ct)).IsSuccess.Should().BeTrue();
        (await ContainerIdAsync(c.ContainerA)).Should().Be(idBefore, "идемпотентность шага");
    }

    private static async Task<string> ContainerIdAsync(string name)
        => (await E2eFixture.RunDockerAsync(["inspect", "-f", "{{.Id}}", name])).Trim();

    private static async Task<DateTime> ContainerCreatedAtAsync(string name)
    {
        var raw = (await E2eFixture.RunDockerAsync(["inspect", "-f", "{{.Created}}", name])).Trim();
        return DateTime.Parse(raw, System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.RoundtripKind);
    }
}
