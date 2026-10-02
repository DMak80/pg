using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using PgWorker.IntegrationTests.E2e;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.Docker;

// Перебор endpoints lease-скриптом мастер-ключа (t09, spec §6): PGW_ETCD —
// СПИСОК URL; при отказе активного endpoint скрипт продолжает писать/продлевать
// мастер-ключ через следующий живой. Скрипт запускается ПРОЦЕССОМ в контейнере
// python:3.12-alpine (тот же stdlib-набор, что в образе ноды; fork-демон жив,
// пока жив PID1-сессия sleep) с bind-mount файла docker/node/master-lease.py.
// Два ОДИНОЧНЫХ etcd-контейнера (не кластер): lease-скрипту безразличен кворум,
// важен перебор transport-отказов. Гейт PGW_TEST_DOCKER=1 (DockerTrait).
public class MasterLeaseFailoverTests
{
    private const string EtcdImage = "quay.io/coreos/etcd:v3.5.21";
    private const string PythonImage = "python:3.12-alpine";
    private const int LeaseTtlSec = 5;

    [Fact]
    public async Task MasterLease_FailoverToSecondEndpoint_KeepaliveContinues()
    {
        DockerTrait.SkipIfUnavailable();
        var ct = TestContext.Current.CancellationToken;
        var tag = Guid.NewGuid().ToString("N")[..8];

        // Arrange: два живых одиночных etcd (динамические хост-порты) +
        // runner-контейнер со скриптом; скрипт-демон пишет ключ через endpoint №1.
        var etcd1 = new ContainerBuilder(EtcdImage)
            .WithName($"pgw-it-ml1-{tag}")
            .WithCommand("etcd", "--name=ml1", "--data-dir=/etcd-data",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://127.0.0.1:2379")
            .WithPortBinding(2379, assignRandomHostPort: true)
            .Build();
        var etcd2 = new ContainerBuilder(EtcdImage)
            .WithName($"pgw-it-ml2-{tag}")
            .WithCommand("etcd", "--name=ml2", "--data-dir=/etcd-data",
                "--listen-client-urls=http://0.0.0.0:2379",
                "--advertise-client-urls=http://127.0.0.1:2379")
            .WithPortBinding(2379, assignRandomHostPort: true)
            .Build();
        var root = E2eFixture.FindRoot(AppContext.BaseDirectory);
        var runner = new ContainerBuilder(PythonImage)
            .WithName($"pgw-it-mlrun-{tag}")
            .WithCommand("sleep", "600")
            .WithBindMount(Path.Combine(root, "docker", "node", "master-lease.py"),
                "/tmp/master-lease.py", AccessMode.ReadOnly)
            .WithExtraHost("host.docker.internal", "host-gateway")
            .Build();
        await etcd1.StartAsync(ct);
        await etcd2.StartAsync(ct);
        await runner.StartAsync(ct);
        var p1 = etcd1.GetMappedPublicPort(2379);
        var p2 = etcd2.GetMappedPublicPort(2379);
        var ep1 = $"http://host.docker.internal:{p1}";
        var ep2 = $"http://host.docker.internal:{p2}";
        var key = $"/pgw-it/master-lease-{tag}/master";
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        var gateway = new EtcdGateway(http);

        try
        {
            var startScript = $"PGW_ETCD='{ep1},{ep2}' PGW_MASTER_KEY='{key}' "
                + "PGW_NODE_HOST='node1a' PGW_DOORMAN_PORT='6432' "
                + "python3 -u /tmp/master-lease.py master >/tmp/lease.log 2>&1";
            await E2eFixture.RunProcessAsync("docker", ["exec", runner.Name, "sh", "-c", startScript], ct);

            // до отказа скрипт пишет на АКТИВНОМ endpoint — первом элементе
            // списка (ep1): ждём появления ключа именно там (узлы независимые,
            // не кластер — на ep2 ключа до переключения нет).
            var appeared = await E2eFixture.WaitForAsync(async () =>
                (await gateway.GetAsync(ep1, key, ct)).Value is not null,
                TimeSpan.FromSeconds(15), ct);
            appeared.Should().BeTrue("демон master-lease обязан писать мастер-ключ (логи: docker exec … cat /tmp/lease.log)");

            // Act: останавливаем ПЕРВЫЙ контейнер — активный endpoint умирает.
            await etcd1.StopAsync(ct);

            // Assert: ключ продолжает продлеваться через второй узел — непрерывно
            // жив дольше 2×TTL (умерший lease погасил бы ключ ≤ 5 c), лог сообщает
            // о переключении активного endpoint.
            await Task.Delay(TimeSpan.FromSeconds(LeaseTtlSec), ct); // гарантия: выживание не «прошлым» put
            var alive = true;
            for (var probe = 0; probe < 5 && alive; probe++)
            {
                await Task.Delay(TimeSpan.FromSeconds(2), ct);
                var kv = (await gateway.GetAsync(ep2, key, ct)).Value;
                alive = kv is { Value.Length: > 0 };
            }
            alive.Should().BeTrue($"ключ {key} не гаснет на 5 пробах × 2 c (10 c > 2×TTL) через второй endpoint после смерти первого");

            var log = await E2eFixture.RunProcessAsync(
                "docker", ["exec", runner.Name, "cat", "/tmp/lease.log"], ct);
            log.Should().Contain("endpoint switch",
                "лог скрипта обязан сообщать, через какой endpoint он пишет (диагностика)");
        }
        finally
        {
            // Teardown (любой исход): гасим демон по PID-файлу, убираем контейнеры.
            try
            {
                await E2eFixture.RunProcessAsync("docker",
                    ["exec", runner.Name, "sh", "-c", "kill $(cat /tmp/master-lease.pid) 2>/dev/null || true"], ct);
            }
            catch
            {
                // демон не успел стартовать — не ошибка teardown
            }

            await runner.DisposeAsync();
            await etcd1.DisposeAsync();
            await etcd2.DisposeAsync();
        }

        // Ассерт чистоты: своих контейнеров не осталось (guid-имена).
        var left = await E2eFixture.RunProcessAsync("docker",
            ["ps", "-a", "--format", "{{.Names}}", "--filter", "name=pgw-it-ml"], CancellationToken.None);
        left.Should().BeEmpty("teardown теста обязан убрать оба etcd и runner");
    }
}
