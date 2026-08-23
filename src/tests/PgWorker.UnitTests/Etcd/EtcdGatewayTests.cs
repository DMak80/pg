using System.Net;
using System.Text;
using System.Text.Json;
using PgWorker.Core;
using PgWorker.Etcd.Client;
using Xunit;

namespace PgWorker.UnitTests.Etcd;

// Транспорт etcd /v3/*: base64, range_end, txn-compare, lease (строковые int64), snapshot (задача 10).
public class EtcdGatewayTests
{
    // Управляемый транспорт: перехватывает запросы и отвечает заготовленным ответом.
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

        // Assert: base64("/clusters/") и range_end с инкрементированным последним байтом ("/clusters0")
        var request = handler.Requests.Should().ContainSingle().Subject;
        request.Url.Should().Be("http://etcd:2379/v3/kv/range");
        var body = JsonDocument.Parse(request.Body).RootElement;
        body.GetProperty("key").GetString().Should().Be("L2NsdXN0ZXJzLw==");
        body.GetProperty("range_end").GetString().Should().Be("L2NsdXN0ZXJzMA==");
    }

    [Fact]
    public async Task Get_SingleKey_DecodesKvOrNull()
    {
        // Arrange — точечный ключ: base64("/a/b") и base64("/a/c") (последний байт +1)
        var handler = new FakeHandler(_ => Json(
            """{"kvs":[{"key":"L2EvYg==","value":"dg==","mod_revision":"42"}]}"""));
        var gateway = NewGateway(handler);

        // Act
        var present = await gateway.GetAsync("http://etcd:2379", "/a/b", CancellationToken.None);
        var empty = await NewGateway(new FakeHandler(_ => Json("""{"header":{}}""")))
            .GetAsync("http://etcd:2379", "/a/b", CancellationToken.None);

        // Assert
        present.IsSuccess.Should().BeTrue();
        present.Value.Should().NotBeNull();
        present.Value!.Key.Should().Be("/a/b");
        present.Value.Value.Should().Be("v");
        present.Value.ModRevision.Should().Be(42); // mod_revision приходит строкой
        empty.Value.Should().BeNull();

        var body = JsonDocument.Parse(handler.Requests.Single().Body).RootElement;
        body.GetProperty("key").GetString().Should().Be("L2EvYg==");
        body.GetProperty("range_end").GetString().Should().Be("L2EvYw==");
    }

    [Fact]
    public async Task Txn_CompareVersionZeroAndPutWithLease_RequestBody()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json("""{"succeeded":true}"""));
        var gateway = NewGateway(handler);

        // Act — захват клэйма: compare version==0 + put с lease (spec §4.3)
        var result = await gateway.TxnAsync(
            "http://etcd:2379",
            TxnRequest.Of(
                [TxnCompare.NotExists("/pgworker/claims/shop")],
                [new TxnOp.Put("/pgworker/claims/shop", """{"instance":"abc"}""", 777)]),
            CancellationToken.None);

        // Assert: base64("/pgworker/claims/shop") = L3Bnd29ya2VyL2NsYWltcy9zaG9w
        result.IsSuccess.Should().BeTrue();
        result.Value.Succeeded.Should().BeTrue();
        var request = handler.Requests.Should().ContainSingle().Subject;
        request.Url.Should().Be("http://etcd:2379/v3/kv/txn");
        var body = JsonDocument.Parse(request.Body).RootElement;
        var compare = body.GetProperty("compare")[0];
        compare.GetProperty("key").GetString().Should().Be("L3Bnd29ya2VyL2NsYWltcy9zaG9w");
        compare.GetProperty("version").GetInt32().Should().Be(0);
        compare.GetProperty("result").GetInt32().Should().Be(0); // EQUAL
        var put = body.GetProperty("success")[0].GetProperty("request_put");
        put.GetProperty("key").GetString().Should().Be("L3Bnd29ya2VyL2NsYWltcy9zaG9w");
        put.GetProperty("value").GetString().Should().Be("eyJpbnN0YW5jZSI6ImFiYyJ9");
        put.GetProperty("lease").GetInt64().Should().Be(777);
    }

    [Fact]
    public async Task Txn_CompareValueAndDeleteInSuccess_RequestBody()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json("""{"succeeded":false}"""));
        var gateway = NewGateway(handler);

        // Act — flip routing: compare value + delete в success-ветке
        var result = await gateway.TxnAsync(
            "http://etcd:2379",
            new TxnRequest(
                [TxnCompare.ValueEqual("/clusters/shop/buckets/routing/bucket_1", "shard2")],
                [new TxnOp.Delete("/clusters/shop/buckets/status/bucket_1", Prefix: false)],
                []),
            CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Succeeded.Should().BeFalse(); // отказ compare — не исключение
        var body = JsonDocument.Parse(handler.Requests.Single().Body).RootElement;
        var compare = body.GetProperty("compare")[0];
        compare.GetProperty("value").GetString().Should().Be("c2hhcmQy"); // base64("shard2")
        var del = body.GetProperty("success")[0].GetProperty("request_delete_range");
        del.GetProperty("key").GetString().Should().Be(
            Convert.ToBase64String(Encoding.UTF8.GetBytes("/clusters/shop/buckets/status/bucket_1")));
        del.TryGetProperty("range_end", out _).Should().BeFalse(); // точечный delete
    }

    [Fact]
    public async Task Txn_CompareModRevision_UsesModRevisionField()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json("""{"succeeded":true}"""));
        var gateway = NewGateway(handler);

        // Act — перезапись config по mod_revision (spec §4.2)
        await gateway.TxnAsync(
            "http://etcd:2379",
            TxnRequest.Of(
                [TxnCompare.ModRevisionEqual("/clusters/shop/config", 15)],
                [new TxnOp.Put("/clusters/shop/config", "{}", null)]),
            CancellationToken.None);

        // Assert
        var body = JsonDocument.Parse(handler.Requests.Single().Body).RootElement;
        var compare = body.GetProperty("compare")[0];
        compare.GetProperty("mod_revision").GetInt64().Should().Be(15);
        body.GetProperty("success")[0].GetProperty("request_put").TryGetProperty("lease", out _).Should().BeFalse();
    }

    [Fact]
    public async Task LeaseGrant_ParsesStringId()
    {
        // Arrange — etcd отдаёт int64 decimal-строкой
        var handler = new FakeHandler(_ => Json("""{"ID":"123","TTL":"15"}"""));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.LeaseGrantAsync("http://etcd:2379", 15, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be(123);
        var request = handler.Requests.Should().ContainSingle().Subject;
        request.Url.Should().Be("http://etcd:2379/v3/lease/grant");
        JsonDocument.Parse(request.Body).RootElement.GetProperty("TTL").GetInt32().Should().Be(15);
    }

    [Fact]
    public async Task LeaseKeepalive_SendsIdAndChecksTtl()
    {
        // Arrange — успешный keepalive возвращает TTL > 0
        var alive = new FakeHandler(_ => Json("""{"result":{"TTL":"15"}}"""));
        var dead = new FakeHandler(_ => Json("""{"result":{"TTL":"0"}}"""));

        // Act
        var ok = await NewGateway(alive).LeaseKeepaliveAsync("http://etcd:2379", 123, CancellationToken.None);
        var lost = await NewGateway(dead).LeaseKeepaliveAsync("http://etcd:2379", 123, CancellationToken.None);

        // Assert
        ok.IsSuccess.Should().BeTrue();
        lost.IsSuccess.Should().BeFalse(); // TTL=0 — lease истёк, клэйм потерян
        var request = alive.Requests.Should().ContainSingle().Subject;
        request.Url.Should().Be("http://etcd:2379/v3/lease/keepalive");
        JsonDocument.Parse(request.Body).RootElement.GetProperty("ID").GetInt64().Should().Be(123);
    }

    [Fact]
    public async Task Snapshot_ReadsBytes()
    {
        // Arrange — snapshot/save отвечает бинарным blob
        var bytes = new byte[] { 0x1a, 0x2b, 0x3c, 0x00, 0xff };
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new ByteArrayContent(bytes),
        });
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.SnapshotSaveAsync("http://etcd:2379", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Equal(bytes);
        handler.Requests.Single().Url.Should().Be("http://etcd:2379/v3/maintenance/snapshot");
    }

    [Fact]
    public async Task Snapshot_HttpError_ReturnsFailed()
    {
        // Arrange — 500 от etcd
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError)
        {
            Content = new StringContent("boom"),
        });
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.SnapshotSaveAsync("http://etcd:2379", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error.Should().BeOfType<EtcdHttpException>();
    }

    [Fact]
    public async Task Put_WithLease_RequestHasLeaseField()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json("""{"header":{}}"""));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.PutAsync("http://etcd:2379", "/pgworker/leader", "v", 555, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var body = JsonDocument.Parse(handler.Requests.Single().Body).RootElement;
        body.GetProperty("key").GetString().Should().Be("L3Bnd29ya2VyL2xlYWRlcg==");
        body.GetProperty("lease").GetInt64().Should().Be(555);
    }
}
