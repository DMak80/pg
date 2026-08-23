using System.Net;
using System.Text;
using System.Text.Json;
using AdminPanel.Core;
using AdminPanel.Etcd.Client;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Транспорт etcd /v3/*: base64, range_end, фактические имена полей gateway, ошибки (spec §10.6).
public class EtcdGatewayTests
{
    // Управляемый транспорт: перехватывает запросы и отвечает заготовленным JSON.
    private sealed class FakeHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        public readonly List<(string Url, string Body)> Requests = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Requests.Add((request.RequestUri!.ToString(), await request.Content!.ReadAsStringAsync(ct)));
            return responder(request);
        }
    }

    private static HttpResponseMessage Json(string body) => new()
    {
        StatusCode = HttpStatusCode.OK,
        Content = new StringContent(body, Encoding.UTF8, "application/json"),
    };

    private static EtcdGateway NewGateway(FakeHandler handler) => new(new HttpClient(handler));

    [Fact]
    public async Task Range_Prefix_RequestHasBase64KeyAndRangeEnd()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json("""{"kvs":[]}"""));
        var gateway = NewGateway(handler);

        // Act
        await gateway.RangeAsync("http://etcd:2379", "/clusters/", CancellationToken.None);

        // Assert
        var request = handler.Requests.Should().ContainSingle().Subject;
        request.Url.Should().Be("http://etcd:2379/v3/kv/range");
        var body = JsonDocument.Parse(request.Body);
        // base64("/clusters/") и range_end = префикс с инкрементированным последним байтом: "/clusters0"
        // (константы выверены: printf '%s' "/clusters/" | base64 → L2NsdXN0ZXJzLw==)
        body.RootElement.GetProperty("key").GetString().Should().Be("L2NsdXN0ZXJzLw==");
        body.RootElement.GetProperty("range_end").GetString().Should().Be("L2NsdXN0ZXJzMA==");
    }

    [Fact]
    public async Task Range_DecodesBase64Kvs()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json(EtcdFixtures.LoadText("gateway-range.json")));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.RangeAsync("http://etcd:2379", "/a/", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var kv = result.Value.Should().ContainSingle().Subject;
        kv.Key.Should().Be("/a/b");
        kv.Value.Should().Be("v");
        kv.ModRevision.Should().Be(42); // mod_revision приходит строкой
    }

    [Fact]
    public async Task Range_MissingKvs_EmptyList()
    {
        // Arrange — пустой префикс: gateway не отдаёт kvs вовсе
        var handler = new FakeHandler(_ => Json("""{"header":{}}"""));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.RangeAsync("http://etcd:2379", "/nope/", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeEmpty();
    }

    [Fact]
    public async Task Status_ParsesFields()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json(EtcdFixtures.LoadText("gateway-status.json")));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.StatusAsync("http://etcd:2379", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Version.Should().Be("3.5.21");
        result.Value.DbSizeBytes.Should().Be(20480);
        result.Value.LeaderMemberId.Should().Be(13820473277879079085UL);
        result.Value.RaftIndex.Should().Be(17);
        result.Value.RaftTerm.Should().Be(3);
        handler.Requests.Single().Url.Should().Be("http://etcd:2379/v3/maintenance/status");
    }

    [Fact]
    public async Task MemberList_ParsesUrls()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json(EtcdFixtures.LoadText("gateway-member-list.json")));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.MemberListAsync("http://etcd:2379", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var member = result.Value.Should().ContainSingle().Subject;
        member.Id.Should().Be(13820473277879079085UL);
        member.Name.Should().Be("test");
        member.PeerUrls.Should().Contain("http://localhost:2380");
        member.ClientUrls.Should().Contain("http://localhost:2379");
    }

    [Fact]
    public async Task Alarm_MapsAlarmType()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json(EtcdFixtures.LoadText("gateway-alarm.json")));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.AlarmAsync("http://etcd:2379", CancellationToken.None);

        // Assert
        var alarm = result.Value.Should().ContainSingle().Subject;
        alarm.MemberId.Should().Be(13820473277879079085UL);
        alarm.Type.Should().Be(EtcdAlarmType.NoSpace); // "alarm": 1
    }

    [Fact]
    public async Task HttpError_ReturnsFailed()
    {
        // Arrange — Content задан явно: ответ без тела дал бы null-Content и NRE вместо EtcdHttpException
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
        {
            Content = new StringContent(string.Empty),
        });
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.StatusAsync("http://etcd:2379", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<EtcdHttpException>();
    }

    [Fact]
    public async Task NetworkError_ReturnsFailed()
    {
        // Arrange — HttpClient с недостижимым портом: connection refused мгновенен
        var gateway = new EtcdGateway(new HttpClient { Timeout = TimeSpan.FromSeconds(2) });

        // Act
        var result = await gateway.StatusAsync("http://localhost:1", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
    }

    [Fact]
    public async Task Txn_CompareAndPuts_RequestHasBase64Bodies()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json("""{"succeeded":true}"""));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.TxnAsync(
            "http://etcd:2379",
            [new TxnCompare("/clusters/shop/config", 0)],
            [new KvPut("/clusters/shop/config", "{}")],
            CancellationToken.None);

        // Assert: compare version=0 + request_put; base64("/clusters/shop/config") = L2NsdXN0ZXJzL3Nob3AvY29uZmln
        result.IsSuccess.Should().BeTrue();
        result.Value.Succeeded.Should().BeTrue();
        var request = handler.Requests.Should().ContainSingle().Subject;
        request.Url.Should().Be("http://etcd:2379/v3/kv/txn");
        var body = JsonDocument.Parse(request.Body).RootElement;
        body.GetProperty("compare")[0].GetProperty("key").GetString().Should().Be("L2NsdXN0ZXJzL3Nob3AvY29uZmln");
        body.GetProperty("compare")[0].GetProperty("version").GetInt32().Should().Be(0);
        body.GetProperty("success")[0].GetProperty("request_put").GetProperty("key").GetString().Should().Be("L2NsdXN0ZXJzL3Nob3AvY29uZmln");
    }

    [Fact]
    public async Task Txn_CompareFailed_MapsSucceededFalse()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json("""{"succeeded":false,"responses":[]}"""));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.TxnAsync(
            "http://etcd:2379", [new TxnCompare("/k", 0)], [new KvPut("/k", "v")], CancellationToken.None);

        // Assert: отказ compare — не исключение, а Succeeded=false (клэйм имени занят, arch/02 §9.2).
        result.IsSuccess.Should().BeTrue();
        result.Value.Succeeded.Should().BeFalse();
    }

    [Fact]
    public async Task Put_RequestHasBase64KeyValue()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json("""{"header":{}}"""));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.PutAsync("http://etcd:2379", "/a/b", "v", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var request = handler.Requests.Should().ContainSingle().Subject;
        request.Url.Should().Be("http://etcd:2379/v3/kv/put");
        var body = JsonDocument.Parse(request.Body).RootElement;
        body.GetProperty("key").GetString().Should().Be("L2EvYg==");
        body.GetProperty("value").GetString().Should().Be("dg==");
    }

    [Fact]
    public async Task Delete_Prefix_RequestHasKeyAndRangeEnd()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json("""{"deleted":3}"""));
        var gateway = NewGateway(handler);

        // Act
        await gateway.DeleteAsync("http://etcd:2379", "/clusters/shop/", prefix: true, CancellationToken.None);
        await gateway.DeleteAsync("http://etcd:2379", "/service/shop-shard1/request_cpu", prefix: false, CancellationToken.None);

        // Assert: prefix=true → key+range_end (префиксный deleterange); точечный — только key.
        var bodies = handler.Requests.Select(r => JsonDocument.Parse(r.Body).RootElement).ToList();
        bodies[0].TryGetProperty("range_end", out _).Should().BeTrue();
        bodies[1].TryGetProperty("range_end", out _).Should().BeFalse();
    }
}
