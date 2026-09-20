using System.Net;
using System.Net.Http.Json;
using FluentAssertions;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.IntegrationTests.Valkey;
using Xunit;

namespace ValkeyWorker.IntegrationTests.E2e;

// Docker-E2E ValkeyWorker (spec §6.3, кейс-маркер мерж-гейта): свежий Release
// valkeyworker:e2e → API-create → контейнер vwk-<C>-node1 → дискавери →
// RESP app-кред по TLS → DELETE → чистота (вкл. том vwk-<C>-tls, t06).
// Изоляция по docs/e2e-isolation.md, телеметрия по docs/e2e-launch.md.
// Гейт: PGW_TEST_DOCKER=1.
public class ValkeyE2eLifecycleTests
{
    [Fact]
    public async Task Lifecycle_ProvisionToClean()
    {
        // Arrange: гейт запуска — без docker-режима E2E скипается.
        var enabled = Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") == "1";
        if (!enabled)
        {
            Assert.Skip("PGW_TEST_DOCKER=1 не задан — docker-E2E пропущен");
            return;
        }

        // Окружение: сеть + etcd + воркер (Release-образ) + TLS-пакет.
        await using var fx = await ValkeyE2eEnvironment.StartAsync("lifecycle");
        var cluster = $"e2e{fx.ClusterTag}";
        using var api = fx.CreateApiHttpClient();

        // [PHASE] wait-worker: /healthz по mTLS готов (бюджет ≤ 30 с).
        await fx.WaitPhaseAsync("wait-worker", async () =>
        {
            try
            {
                using var health = await api.GetAsync("/healthz", TestContext.Current.CancellationToken);
                return health.StatusCode == HttpStatusCode.OK;
            }
            catch (HttpRequestException)
            {
                return false; // Kestrel ещё поднимается
            }
        }, TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

        // Act 1: создание кластера через API → 201.
        using var created = await api.PostAsJsonAsync("/api/valkey/clusters",
            new
            {
                name = cluster,
                nodes = 1,
                maxmemoryBytes = 536870912,
                maxmemoryPolicy = "allkeys-lru",
                resources = new { cpu = 1m, memGi = 1, diskGi = 10 },
            }, TestContext.Current.CancellationToken);
        var createdBody = await created.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        created.StatusCode.Should().Be(HttpStatusCode.Created, createdBody);

        // [PHASE] wait-provision: state=RUNNING в etcd + контейнер жив (≤ 100 с).
        var ct = TestContext.Current.CancellationToken;
        await fx.WaitPhaseAsync("wait-provision", async () =>
        {
            var kv = await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/valkey/clusters/{cluster}/nodes/node1/state", ct);
            return kv.Value?.Value == "RUNNING"
                && await fx.ContainerAliveAsync($"vwk-{cluster}-node1");
        }, TimeSpan.FromSeconds(100), ct);

        // Act 2: PING admin-кредом по TLS (endpoints из etcd — host-порт
        // фактический, литералов :17xxx нет; ca_pem — дискавери t06).
        var endpoints = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
            $"/valkey/clusters/{cluster}/endpoints", ct)).Value!.Value;
        endpoints.Should().StartWith("host.docker.internal:");
        // Проба — по advertised-хосту из endpoints (SAN серта = advertised).
        var probeHost = endpoints.Split(':')[0];
        var port = int.Parse(endpoints.Split(':')[1]);
        var adminPassword = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
            $"/valkey/clusters/{cluster}/admin_password", ct)).Value!.Value;
        var caPem = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
            $"/valkey/clusters/{cluster}/ca_pem", ct)).Value!.Value;
        ValkeyPki.TryParseCertificate(caPem, out _).Should().BeTrue("ca_pem — валидный PEM");
        var ping = RespProbe.ExecuteTls(probeHost, port, "admin", adminPassword, caPem, "PING");
        ping.Ok.Should().BeTrue(ping.Error);

        // Act 3: RESP app-кредом по TLS: SET/GET; CONFIG GET — отказ (ACL).
        var appPassword = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
            $"/valkey/clusters/{cluster}/app_password", ct)).Value!.Value;
        var set = RespProbe.ExecuteTls(probeHost, port, "app", appPassword, caPem, "SET", "e2e:key", "value");
        set.Ok.Should().BeTrue(set.Error);
        var get = RespProbe.ExecuteTls(probeHost, port, "app", appPassword, caPem, "GET", "e2e:key");
        get.Value.Should().Be("value");
        var configGet = RespProbe.ExecuteTls(probeHost, port, "app", appPassword, caPem, "CONFIG", "GET", "maxmemory");
        configGet.Ok.Should().BeFalse("app без админ-команд");

        // Act 4: дискавери-ключи: config без state; /valkeyworker/api/<id> — url+thumbprint.
        var config = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
            $"/valkey/clusters/{cluster}/config", ct)).Value!.Value;
        config.Should().NotContain("state");
        var apiKeys = await fx.Gateway.RangeAsync(fx.EtcdEndpoint, "/valkeyworker/api/", ct);
        apiKeys.Value.Should().ContainSingle();
        apiKeys.Value[0].Value.Should().Contain("\"url\":").And.Contain("cert_thumbprint");

        // Act 5: DELETE → 202 → демонтаж процессом B.
        using var deleted = await api.DeleteAsync($"/api/valkey/clusters/{cluster}", ct);
        deleted.StatusCode.Should().Be(HttpStatusCode.Accepted);

        // [PHASE] wait-clean: контейнера нет, тома нет, префиксы etcd чисты,
        // portalloc снят.
        await fx.WaitPhaseAsync("wait-clean", async () =>
        {
            var alive = await fx.ContainerAliveAsync($"vwk-{cluster}-node1");
            if (alive || await fx.VolumeExistsAsync($"vwk-{cluster}-tls"))
                return false;
            var domain = await fx.Gateway.RangeAsync(fx.EtcdEndpoint,
                $"/valkey/clusters/{cluster}/", ct);
            var alloc = await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/valkeyworker/portalloc/{cluster}", ct);
            return domain.Value.Count == 0 && alloc.Value is null;
        }, TimeSpan.FromSeconds(60), ct);

        // Act 6: teardown + ассерт чистоты (вкл. тома тега) — в DisposeAsync.
    }

    // t06-маркер мерж-гейта (spec §4.9): TLS-only жизненный цикл — args ноды
    // с --tls-port 6379/--port 0, ca_pem/ca_key в etcd, RESP roundtrip по TLS,
    // plain отклонён, после удаления ни тома vwk-<C>-tls, ни ключей.
    [Fact]
    public async Task Tls_ClusterLifecycleTlsOnly()
    {
        // Arrange: гейт запуска — без docker-режима E2E скипается.
        var enabled = Environment.GetEnvironmentVariable("PGW_TEST_DOCKER") == "1";
        if (!enabled)
        {
            Assert.Skip("PGW_TEST_DOCKER=1 не задан — docker-E2E пропущен");
            return;
        }

        await using var fx = await ValkeyE2eEnvironment.StartAsync("tls-lifecycle");
        var cluster = $"e2e{fx.ClusterTag}";
        using var api = fx.CreateApiHttpClient();
        var ct = TestContext.Current.CancellationToken;

        try
        {
            // [PHASE] wait-worker: /healthz по mTLS готов (≤ 30 с).
            await fx.WaitPhaseAsync("wait-worker", async () =>
            {
                try
                {
                    using var health = await api.GetAsync("/healthz", ct);
                    return health.StatusCode == HttpStatusCode.OK;
                }
                catch (HttpRequestException)
                {
                    return false;
                }
            }, TimeSpan.FromSeconds(30), ct);

            // Act 1: API-create → 201 (контур как Lifecycle_ProvisionToClean).
            using var created = await api.PostAsJsonAsync("/api/valkey/clusters",
                new
                {
                    name = cluster,
                    nodes = 1,
                    maxmemoryBytes = 536870912,
                    maxmemoryPolicy = "allkeys-lru",
                    resources = new { cpu = 1m, memGi = 1, diskGi = 10 },
                }, ct);
            var createdBody = await created.Content.ReadAsStringAsync(ct);
            created.StatusCode.Should().Be(HttpStatusCode.Created, createdBody);

            // [PHASE] wait-provision: RUNNING + контейнер жив (≤ 100 с).
            await fx.WaitPhaseAsync("wait-provision", async () =>
            {
                var kv = await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                    $"/valkey/clusters/{cluster}/nodes/node1/state", ct);
                return kv.Value?.Value == "RUNNING"
                    && await fx.ContainerAliveAsync($"vwk-{cluster}-node1");
            }, TimeSpan.FromSeconds(100), ct);

            // Assert 1: args ноды — канонический TLS-набор (docker inspect).
            var args = await fx.ContainerArgsJsonAsync($"vwk-{cluster}-node1");
            args.Should().Contain("--tls-port").And.Contain("\"6379\"")
                .And.Contain("--port").And.Contain("\"0\"")
                .And.Contain("--tls-cert-file").And.Contain("--tls-auth-clients");

            // Assert 2: дискавери-ключи — endpoints прежний формат,
            // ca_pem/ca_key существуют, PEM валиден.
            var endpoints = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/valkey/clusters/{cluster}/endpoints", ct)).Value!.Value;
            endpoints.Should().StartWith("host.docker.internal:");
            // Проба — по advertised-хосту из endpoints (SAN серта = advertised).
            var probeHost = endpoints.Split(':')[0];
            var port = int.Parse(endpoints.Split(':')[1]);
            var caPem = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/valkey/clusters/{cluster}/ca_pem", ct)).Value!.Value;
            var caKey = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/valkey/clusters/{cluster}/ca_key", ct)).Value!.Value;
            ValkeyPki.TryParseCertificate(caPem, out _).Should().BeTrue();
            ValkeyPki.TryParseRsaKey(caKey, out _).Should().BeTrue();

            // Assert 3: RESP-проба по TLS app-кредом с ca_pem из etcd: SET→GET.
            var appPassword = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/valkey/clusters/{cluster}/app_password", ct)).Value!.Value;
            var set = RespProbe.ExecuteTls(probeHost, port, "app", appPassword, caPem, "SET", "e2e:tls", "v1");
            set.Ok.Should().BeTrue(set.Error);
            var get = RespProbe.ExecuteTls(probeHost, port, "app", appPassword, caPem, "GET", "e2e:tls");
            get.Value.Should().Be("v1");

            // Assert 4: plain отклонён (сырой PING без TLS — сброс/отказ).
            var plainRejected = false;
            try
            {
                var plain = RespProbe.Execute(probeHost, port, "app", appPassword, "PING");
                plainRejected = !plain.Ok;
            }
            catch (ApplicationException)
            {
                plainRejected = true; // сброс соединения на хендшейке
            }

            plainRejected.Should().BeTrue("plain-порт закрыт (--port 0)");

            // Act 2: DELETE → 202 → демонтаж.
            using var deleted = await api.DeleteAsync($"/api/valkey/clusters/{cluster}", ct);
            deleted.StatusCode.Should().Be(HttpStatusCode.Accepted);

            // [PHASE] wait-clean: ни контейнера, ни тома, ни ключей префикса.
            await fx.WaitPhaseAsync("wait-clean", async () =>
            {
                var alive = await fx.ContainerAliveAsync($"vwk-{cluster}-node1");
                if (alive || await fx.VolumeExistsAsync($"vwk-{cluster}-tls"))
                    return false;
                var domain = await fx.Gateway.RangeAsync(fx.EtcdEndpoint,
                    $"/valkey/clusters/{cluster}/", ct);
                return domain.Value.Count == 0;
            }, TimeSpan.FromSeconds(60), ct);
        }
        catch
        {
            // docs/e2e-launch.md §3: упавший сценарий — teardown остановит
            // контейнеры, но не удалит (разбор по артефактам).
            fx.MarkFailed();
            throw;
        }
    }
}
