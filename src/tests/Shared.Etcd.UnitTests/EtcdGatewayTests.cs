using System.Net;
using System.Text;
using System.Text.Json;

namespace Shared.Etcd.UnitTests;

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

    // --- Панельные кейсы (t08, объединение gateway-тестов; ассерты прежние) ---

    [Fact]
    public async Task Range_DecodesBase64Kvs()
    {
        // Arrange — mod_revision приходит строкой
        var handler = new FakeHandler(_ => Json(
            """{ "kvs": [ { "key": "L2EvYg==", "value": "dg==", "mod_revision": "42" } ] }"""));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.RangeAsync("http://etcd:2379", "/a/", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var kv = result.Value.Should().ContainSingle().Subject;
        kv.Key.Should().Be("/a/b");
        kv.Value.Should().Be("v");
        kv.ModRevision.Should().Be(42);
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
        // Arrange — payload 6-арный (t08: + Revision из header.revision)
        var handler = new FakeHandler(_ => Json(
            """{ "header": { "member_id": "13820473277879079085", "raft_term": "3" }, "version": "3.5.21", "dbSize": "20480", "leader": "13820473277879079085", "raftIndex": "17", "raftTerm": "3" }"""));
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
        // Arrange — имена полей по proto: ID/peerURLs/clientURLs
        var handler = new FakeHandler(_ => Json(
            """{ "members": [ { "ID": "13820473277879079085", "name": "test", "peerURLs": [ "http://localhost:2380" ], "clientURLs": [ "http://localhost:2379" ] } ] }"""));
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
        // Arrange — "alarm": 1 → NoSpace
        var handler = new FakeHandler(_ => Json(
            """{ "alarms": [ { "memberID": "13820473277879079085", "alarm": 1 } ] }"""));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.AlarmAsync("http://etcd:2379", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        var alarm = result.Value.Should().ContainSingle().Subject;
        alarm.MemberId.Should().Be(13820473277879079085UL);
        alarm.Type.Should().Be(EtcdAlarmType.NoSpace);
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
    public async Task Txn_CompareFailed_MapsSucceededFalse()
    {
        // Arrange
        var handler = new FakeHandler(_ => Json("""{"succeeded":false,"responses":[]}"""));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.TxnAsync("http://etcd:2379",
            TxnRequest.Of([TxnCompare.NotExists("/k")], [new TxnOp.Put("/k", "v", null)]),
            CancellationToken.None);

        // Assert: отказ compare — не исключение, а Succeeded=false (клэйм имени занят, arch/02 §9.2).
        result.IsSuccess.Should().BeTrue();
        result.Value.Succeeded.Should().BeFalse();
    }

    [Fact]
    public async Task Put_RequestHasBase64KeyValue()
    {
        // Arrange — одиночная запись без lease (панельный вызов)
        var handler = new FakeHandler(_ => Json("""{"header":{}}"""));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.PutAsync("http://etcd:2379", "/a/b", "v", lease: null, CancellationToken.None);

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

    [Fact]
    public async Task Txn_RequestHasBase64Bodies()
    {
        // Arrange — панельный txn §9.9 через фабрики (compare version==0 + put без lease)
        var handler = new FakeHandler(_ => Json("""{"succeeded":true}"""));
        var gateway = NewGateway(handler);

        // Act
        var result = await gateway.TxnAsync("http://etcd:2379",
            TxnRequest.Of(
                [TxnCompare.NotExists("/clusters/shop/config")],
                [new TxnOp.Put("/clusters/shop/config", "{}", null)]),
            CancellationToken.None);

        // Assert: base64("/clusters/shop/config") = L2NsdXN0ZXJzL3Nob3AvY29uZmln
        result.IsSuccess.Should().BeTrue();
        result.Value.Succeeded.Should().BeTrue();
        var request = handler.Requests.Should().ContainSingle().Subject;
        request.Url.Should().Be("http://etcd:2379/v3/kv/txn");
        var body = JsonDocument.Parse(request.Body).RootElement;
        body.GetProperty("compare")[0].GetProperty("key").GetString().Should().Be("L2NsdXN0ZXJzL3Nob3AvY29uZmln");
        body.GetProperty("compare")[0].GetProperty("version").GetInt32().Should().Be(0);
        body.GetProperty("success")[0].GetProperty("request_put").GetProperty("key").GetString().Should().Be("L2NsdXN0ZXJzL3Nob3AvY29uZmln");
    }

    // --- Новые кейсы t08 ---

    [Fact]
    public async Task Status_ParsesRevision_FromHeader()
    {
        // Arrange — header.revision приходит int64-decimal-строкой (protojson).
        var handler = new FakeHandler(_ => Json(
            """{"header":{"revision":"42"},"version":"3.5.21","leader":"1"}"""));
        var gateway = NewGateway(handler);

        // Act
        var status = await gateway.StatusAsync("http://etcd:2379", CancellationToken.None);

        // Assert
        status.IsSuccess.Should().BeTrue();
        status.Value.Revision.Should().Be(42);
        status.Value.Version.Should().Be("3.5.21");
    }

    [Fact]
    public async Task Txn_FactoryNotExists_SerializesVersionZeroCompare()
    {
        // Arrange — фабрика NotExists(key) эквивалентна прямой форме
        // new TxnCompare(key, TxnTarget.Version, TxnPredicate.Equal, "", 0) (spec §4.2).
        var handler = new FakeHandler(_ => Json("""{"succeeded":true}"""));
        var gateway = NewGateway(handler);

        // Act
        await gateway.TxnAsync("http://etcd:2379",
            TxnRequest.Of([TxnCompare.NotExists("/k")], [new TxnOp.Put("/k", "v", null)]),
            CancellationToken.None);

        // Assert: target=VERSION(0), result=EQUAL(0), version=0; success-put без lease.
        var body = JsonDocument.Parse(handler.Requests.Should().ContainSingle().Subject.Body).RootElement;
        var compare = body.GetProperty("compare")[0];
        compare.GetProperty("target").GetInt32().Should().Be(0);
        compare.GetProperty("result").GetInt32().Should().Be(0);
        compare.GetProperty("version").GetInt32().Should().Be(0);
        var put = body.GetProperty("success")[0].GetProperty("request_put");
        put.GetProperty("key").GetString().Should().Be("L2s=");
        put.GetProperty("value").GetString().Should().Be("dg==");
    }
}
