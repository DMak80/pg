using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using FluentAssertions;
using Xunit;

namespace ValkeyWorker.IntegrationTests.E2e;

// Docker-E2E ValkeyWorker (spec §6.3, кейс-маркер мерж-гейта): свежий Release
// valkeyworker:e2e → API-create → контейнер vwk-<C>-node1 → дискавери →
// RESP app-кред → DELETE → чистота. Изоляция по docs/e2e-isolation.md,
// телеметрия по docs/e2e-launch.md. Гейт: PGW_TEST_DOCKER=1.
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

        // Act 2: PING admin-кредом по endpoints из etcd (host-порт фактический —
        // литералов :17xxx нет: окно воркера 21xxx+ из окружения).
        var endpoints = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
            $"/valkey/clusters/{cluster}/endpoints", ct)).Value!.Value;
        endpoints.Should().StartWith("host.docker.internal:");
        var port = int.Parse(endpoints.Split(':')[1]);
        var adminPassword = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
            $"/valkey/clusters/{cluster}/admin_password", ct)).Value!.Value;
        var ping = Probe.Execute("localhost", port, "admin", adminPassword, "PING");
        ping.Ok.Should().BeTrue(ping.Error);

        // Act 3: RESP app-кредом: SET/GET; CONFIG GET — отказ (ACL).
        var appPassword = (await fx.Gateway.GetAsync(fx.EtcdEndpoint,
            $"/valkey/clusters/{cluster}/app_password", ct)).Value!.Value;
        var set = Probe.Execute("localhost", port, "app", appPassword, "SET", "e2e:key", "value");
        set.Ok.Should().BeTrue(set.Error);
        var get = Probe.Execute("localhost", port, "app", appPassword, "GET", "e2e:key");
        get.Value.Should().Be("value");
        var configGet = Probe.Execute("localhost", port, "app", appPassword, "CONFIG", "GET", "maxmemory");
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

        // [PHASE] wait-clean: контейнера нет, префиксы etcd чисты, portalloc снят.
        await fx.WaitPhaseAsync("wait-clean", async () =>
        {
            var alive = await fx.ContainerAliveAsync($"vwk-{cluster}-node1");
            if (alive)
                return false;
            var domain = await fx.Gateway.RangeAsync(fx.EtcdEndpoint,
                $"/valkey/clusters/{cluster}/", ct);
            var alloc = await fx.Gateway.GetAsync(fx.EtcdEndpoint,
                $"/valkeyworker/portalloc/{cluster}", ct);
            return domain.Value.Count == 0 && alloc.Value is null;
        }, TimeSpan.FromSeconds(60), ct);

        // Act 6: teardown + ассерт чистоты — в DisposeAsync окружения.
    }

    // RESP-проба (SET/GET/CONFIG) с хоста — копия RespProbe без internal-зависимостей.
    private static class Probe
    {
        public static (bool Ok, string Error, string? Value) Execute(
            string host, int port, string user, string password, params string[] command)
        {
            try
            {
                using var client = new System.Net.Sockets.TcpClient();
                client.ConnectAsync(host, port).GetAwaiter().GetResult();
                using var stream = new System.IO.BufferedStream(client.GetStream(), 8192);

                var auth = WriteAndRead(stream, ["AUTH", user, password]);
                if (auth is not string authOk || authOk != "OK")
                    return (false, $"AUTH fail", null);

                var reply = WriteAndRead(stream, command);
                return reply switch
                {
                    string s => (true, "", s),
                    long n => (true, "", n.ToString()),
                    List<object?> list => (true, "", string.Join("|", list.Select(i => i?.ToString() ?? ""))),
                    _ => (false, "неизвестный кадр", null),
                };
            }
            catch (Exception ex)
            {
                return (false, ex.Message, null);
            }
        }

        private static object? WriteAndRead(
            System.IO.Stream stream, IReadOnlyList<string> args)
        {
            var sb = new System.Text.StringBuilder();
            sb.Append('*').Append(args.Count).Append("\r\n");
            foreach (var arg in args)
            {
                var size = System.Text.Encoding.UTF8.GetByteCount(arg);
                sb.Append('$').Append(size).Append("\r\n").Append(arg).Append("\r\n");
            }

            var bytes = System.Text.Encoding.UTF8.GetBytes(sb.ToString());
            stream.Write(bytes);
            stream.Flush();
            var buffer = new List<byte>();
            return ParseReply(stream, buffer, ct: TestContext.Current.CancellationToken)
                .GetAwaiter().GetResult();
        }

        // Минимальный парсер кадров (simple/error/bulk/array) — дочитывает до полного кадра.
        private static async Task<object?> ParseReply(
            System.IO.Stream stream, List<byte> _, CancellationToken ct)
        {
            static async Task<int> ReadByte(System.IO.Stream s, CancellationToken token)
            {
                var one = new byte[1];
                if (await s.ReadAsync(one, token) == 0)
                    throw new EndOfStreamException();
                return one[0];
            }

            static async Task<string> ReadLine(System.IO.Stream s, CancellationToken token)
            {
                var sb = new System.Text.StringBuilder();
                while (true)
                {
                    var b = await ReadByte(s, token);
                    if (b == (byte)'\r')
                    {
                        await ReadByte(s, token);
                        return sb.ToString();
                    }

                    sb.Append((char)b);
                }
            }

            var type = (char)await ReadByte(stream, ct);
            switch (type)
            {
                case '+':
                case '$':
                    if (type == '$')
                    {
                        var size = int.Parse(await ReadLine(stream, ct));
                        if (size < 0)
                            return null;
                        var payload = new byte[size];
                        var read = 0;
                        while (read < size)
                            read += await stream.ReadAsync(payload.AsMemory(read..), ct);
                        await ReadByte(stream, ct); // \r
                        await ReadByte(stream, ct); // \n
                        return System.Text.Encoding.UTF8.GetString(payload);
                    }

                    return await ReadLine(stream, ct);
                case '-':
                    return new InvalidOperationException(await ReadLine(stream, ct));
                case '*':
                {
                    var count = int.Parse(await ReadLine(stream, ct));
                    var items = new List<object?>(count);
                    for (var i = 0; i < count; i++)
                        items.Add(await ParseReply(stream, _, ct));
                    return items;
                }
                default:
                    return new InvalidOperationException($"RESP type {type}");
            }
        }
    }
}
