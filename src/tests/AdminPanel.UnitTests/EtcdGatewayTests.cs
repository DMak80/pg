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
}
