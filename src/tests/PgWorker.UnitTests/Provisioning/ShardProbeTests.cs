using System.Net;
using System.Text;
using PgWorker.Core.Model;
using PgWorker.Provisioning.Probes;

namespace PgWorker.UnitTests.Provisioning;

// ShardProbe — Patroni REST-пробы (задача 17): GET /cluster по patroni-порту
// ноды, живость = 200. Фикстура — реальный ответ Patroni из AdminPanel.
public class ShardProbeTests
{
    // Управляемый транспорт (как EtcdGatewayTests): перехват запроса → заготовка.
    private sealed class FakeHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
            => Task.FromResult(responder(request));
    }

    private static HttpResponseMessage Json(int status, string body) => new()
    {
        StatusCode = (HttpStatusCode)status,
        Content = new StringContent(body, Encoding.UTF8, "application/json"),
    };

    private static readonly NodeAddress Node = new("10.0.0.11", new NodePorts(15432, 18008, 16432));

    private static string PatroniClusterJson()
        => File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "ProbesFixtures", "patroni-cluster.json"));

    [Fact]
    public async Task GetCluster_PatroniFixture_ParsesMembers()
    {
        // Arrange — фикстура реального GET /cluster: мастер + 2 реплики
        var probe = new ShardProbe(new HttpClient(new FakeHandler(_ => Json(200, PatroniClusterJson()))));

        // Act
        var result = await probe.GetClusterAsync(Node, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeEquivalentTo(
        [
            new PatroniMember("s1a", "master", "running"),
            new PatroniMember("s1b", "replica", "streaming"),
            new PatroniMember("s1c", "replica", "stopped"),
        ]);
    }

    [Fact]
    public async Task GetCluster_ServerErrorOrTimeout_Failed()
    {
        // Arrange — 500 и timeout (TaskCanceledException): проба транзиент-толерантна
        var errProbe = new ShardProbe(new HttpClient(new FakeHandler(_ => Json(500, "boom"))));
        var timeoutProbe = new ShardProbe(new HttpClient(
            new FakeHandler(_ => throw new TaskCanceledException("timeout"))));

        // Act
        var err = await errProbe.GetClusterAsync(Node, CancellationToken.None);
        var timedOut = await timeoutProbe.GetClusterAsync(Node, CancellationToken.None);

        // Assert
        err.IsSuccess.Should().BeFalse();
        timedOut.IsSuccess.Should().BeFalse();
    }

    [Fact]
    public async Task IsAlive_200True_500False()
    {
        // Arrange — три состояния транспорта: живой, ошибка сервера, отказ соединения
        var alive = new ShardProbe(new HttpClient(new FakeHandler(_ => Json(200, """{"members":[]}"""))));
        var dead = new ShardProbe(new HttpClient(new FakeHandler(_ => Json(500, "boom"))));
        var refused = new ShardProbe(new HttpClient(
            new FakeHandler(_ => throw new HttpRequestException("connection refused"))));

        // Act
        var ok = await alive.IsAliveAsync(Node, CancellationToken.None);
        var notOk = await dead.IsAliveAsync(Node, CancellationToken.None);
        var unreachable = await refused.IsAliveAsync(Node, CancellationToken.None);

        // Assert
        ok.Should().BeTrue();
        notOk.Should().BeFalse();
        unreachable.Should().BeFalse();
    }

    // AAA: Д1б — GET /patroni несёт scope+name: идентичность ноды (глобально
    // уникальна — scope <C>-<X>) для вывода «наша/чужая»
    [Fact]
    public async Task IdentifyAsync_PatroniJson_ParsesNameAndScope()
    {
        // Arrange: /patroni отвечает scope+name (Patroni 3.x REST).
        var probe = new ShardProbe(new HttpClient(new FakeHandler(_ => Json(200,
            """{"state":"running","role":"replica","scope":"shop-shard1","name":"shard1a"}"""))));

        // Act
        var identity = await probe.IdentifyAsync(Node, CancellationToken.None);

        // Assert: пара (name, scope) — глобально уникальна (scope <C>-<X>).
        identity.IsSuccess.Should().BeTrue();
        identity.Value.Should().Be(new NodeIdentity("shard1a", "shop-shard1"));
    }

    // AAA: Д1б — битый JSON/не-2xx/без полей → null «не опознана»: чужой ответ
    // по коллизионному порту ≠ наша нода (не ошибка — не успех)
    [Fact]
    public async Task IdentifyAsync_BrokenOrForeignOrMissing_Null()
    {
        // Arrange: битый JSON, не-2xx и JSON без полей — «не опознана» (не ошибка).
        var broken = new ShardProbe(new HttpClient(new FakeHandler(_ => Json(200, "not-json"))));
        var notFound = new ShardProbe(new HttpClient(new FakeHandler(_ => Json(404, ""))));
        var noFields = new ShardProbe(new HttpClient(new FakeHandler(_ => Json(200, """{"members":[]}"""))));

        // Act
        var a = await broken.IdentifyAsync(Node, CancellationToken.None);
        var b = await notFound.IdentifyAsync(Node, CancellationToken.None);
        var c = await noFields.IdentifyAsync(Node, CancellationToken.None);

        // Assert: null — чужой ответ по коллизионному порту ≠ наша нода (фальш-RUNNING исключён).
        a.Value.Should().BeNull();
        b.Value.Should().BeNull();
        c.Value.Should().BeNull();
    }
}
