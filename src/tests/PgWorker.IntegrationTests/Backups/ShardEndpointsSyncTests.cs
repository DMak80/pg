using System.Net;
using FluentAssertions;
using PgWorker.Core.Model;
using PgWorker.IntegrationTests.Etcd;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Probes;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Резолв sync-standby шарда (t27, arch/19 §3.3 п.2): Patroni GET /cluster на
// динамическом порту (TcpListener(0)-зонд + HttpListener — fейк Patroni),
// portalloc-сид с patroni=<порт>. Не найден/недоступен → null БЕЗ fallback
// (второй агент поднимется тиком при появлении sync).
[Collection(EtcdCollection.Name)]
public class ShardEndpointsSyncTests(EtcdFixture fixture)
{
    // Ответ фейка Patroni: тест меняет (sync есть / нет).
    private static string SyncClusterJson(bool withSync) => withSync
        ? """{"members":[{"name":"s1a","role":"master","state":"running","timeline":1},{"name":"s1b","role":"replica","state":"running","timeline":1,"lag":0,"sync":true}]}"""
        : """{"members":[{"name":"s1a","role":"master","state":"running","timeline":1},{"name":"s1b","role":"replica","state":"running","timeline":1,"lag":0}]}""";

    /// <summary>Свободный порт: зонд TcpListener(0) (никаких литералов-портов).</summary>
    private static int FreePort()
    {
        var probe = new TcpListenerAdapter(IPAddress.Loopback, 0);
        probe.Start();
        try
        {
            return ((IPEndPoint)probe.LocalEndpoint).Port;
        }
        finally
        {
            probe.Stop();
        }
    }

    // TcpListener-обёртка для зонда (System.Net.Sockets.TcpListener).
    private sealed class TcpListenerAdapter(IPAddress address, int port)
        : System.Net.Sockets.TcpListener(address, port);

    /// <summary>Фейк Patroni: HttpListener на свободном порту, отдаёт заданный JSON
    /// на GET /cluster; порт закрывается DisposeAsync (тестовый ассет).</summary>
    private sealed class FakePatroni : IAsyncDisposable
    {
        private readonly HttpListener _listener = new();

        private FakePatroni(int port)
        {
            Port = port;
            _listener.Prefixes.Add($"http://127.0.0.1:{port}/");
        } // сид тоже на 127.0.0.1 — без IPv6-неоднозначности localhost

        public int Port { get; private set; }

        private string Body { get; set; } = "";

        public static async Task<FakePatroni> StartAsync(string body, CancellationToken ct)
        {
            var port = FreePort();
            var fake = new FakePatroni(port) { Body = body };
            fake._listener.Start();
            _ = fake.ServeLoopAsync(ct);
            await Task.Delay(50, ct); // листенер начал принимать
            return fake;
        }

        private async Task ServeLoopAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var context = await _listener.GetContextAsync();
                    var buffer = System.Text.Encoding.UTF8.GetBytes(Body);
                    context.Response.ContentType = "application/json";
                    context.Response.ContentLength64 = buffer.Length;
                    await context.Response.OutputStream.WriteAsync(buffer, ct);
                    context.Response.Close();
                }
                catch (Exception) when (ct.IsCancellationRequested)
                {
                    break; // штатная остановка теста
                }
                catch (HttpListenerException)
                {
                    break;
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            _listener.Close();
            await Task.CompletedTask;
        }
    }

    private async Task<ShardEndpoints> BuildEndpointsAsync(string cluster, int patroniPort, CancellationToken ct)
    {
        // portalloc-сид: две ноды шарда, patroni-порт обеих — фейк (один Patroni
        // отвечает за контур: перебор нод упрётся в первый же ответ).
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/pgworker/portalloc/{cluster}",
            Portalloc.Serialize(new Dictionary<string, NodeAddress>
            {
                ["shard1/s1a"] = new("127.0.0.1", new NodePorts(16001, patroniPort, 17001)),
                ["shard1/s1b"] = new("127.0.0.1", new NodePorts(16002, patroniPort, 17002)),
            }), null, ct);
        return new ShardEndpoints(
            fixture.Gateway, [fixture.Endpoint], new ShardProbe(new HttpClient()));
    }

    private static ShardSpec Shard() => new("shard1", 2, "host=s1a dbname=c1", "s1a:17001",
        [new NodeSpec("shard1", "s1a", NodeState.Running), new NodeSpec("shard1", "s1b", NodeState.Running)]);

    [Fact]
    public async Task Sync_член_резолвится_в_s1b()
    {
        // Arrange — Patroni отдаёт sync-реплику s1b
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"sync{Guid.NewGuid().ToString("N")[..8]}";
        await using var patroni = await FakePatroni.StartAsync(SyncClusterJson(withSync: true), ct);
        var endpoints = await BuildEndpointsAsync(cluster, patroni.Port, ct);

        // Act
        var result = await endpoints.ResolveSyncStandbyAsync(
            cluster, Shard(), (await endpoints.ReadPortAllocAsync(cluster, ct)).Value, ct);

        // Assert
        result.IsSuccess.Should().BeTrue(result.Error?.Message);
        result.Value.Should().NotBeNull();
        result.Value!.Ports.Pg.Should().Be(16002, "адрес sync-ноды s1b из portalloc");
    }

    [Fact]
    public async Task Без_sync_члена_null_без_fallback()
    {
        // Arrange — Patroni: мастер + реплика без sync-флага
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"nosync{Guid.NewGuid().ToString("N")[..8]}";
        await using var patroni = await FakePatroni.StartAsync(SyncClusterJson(withSync: false), ct);
        var endpoints = await BuildEndpointsAsync(cluster, patroni.Port, ct);

        // Act
        var result = await endpoints.ResolveSyncStandbyAsync(
            cluster, Shard(), (await endpoints.ReadPortAllocAsync(cluster, ct)).Value, ct);

        // Assert — null — УСПЕХ (не Failed): второй агент поднимется тиком
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeNull("fallback на мастера для sync-источника запрещён");
    }

    [Fact]
    public async Task Патрони_недоступен_null_не_Failed()
    {
        // Arrange — порт-заглушка, никто не слушает
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"down{Guid.NewGuid().ToString("N")[..8]}";
        var endpoints = await BuildEndpointsAsync(cluster, FreePort(), ct);

        // Act
        var result = await endpoints.ResolveSyncStandbyAsync(
            cluster, Shard(), (await endpoints.ReadPortAllocAsync(cluster, ct)).Value, ct);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeNull();
    }

    // DRY: ResolveBackupSourceAsync — sync ?? мастер (поведение t02 не меняется)
    [Fact]
    public async Task ResolveBackupSource_sync_приоритет_и_fallback_мастер()
    {
        // Arrange
        var ct = TestContext.Current.CancellationToken;
        var cluster = $"src{Guid.NewGuid().ToString("N")[..8]}";
        await using var patroni = await FakePatroni.StartAsync(SyncClusterJson(withSync: true), ct);
        var endpoints = await BuildEndpointsAsync(cluster, patroni.Port, ct);
        var addresses = (await endpoints.ReadPortAllocAsync(cluster, ct)).Value;

        // Act 1 — sync есть → адрес s1b
        var withSync = await endpoints.ResolveBackupSourceAsync(cluster, Shard(), addresses, ct);

        // Act 2 — sync исчез → fallback мастер (s1a по master-ключу шарда)
        await patroni.DisposeAsync();
        var withoutSync = await endpoints.ResolveBackupSourceAsync(cluster, Shard(), addresses, ct);

        // Assert
        withSync.IsSuccess.Should().BeTrue();
        withSync.Value!.Ports.Pg.Should().Be(16002);
        withoutSync.IsSuccess.Should().BeTrue();
        withoutSync.Value!.Ports.Pg.Should().Be(16001, "fallback на мастера сохранён");
    }
}
