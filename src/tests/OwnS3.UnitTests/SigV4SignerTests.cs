using System.Net;
using System.Text;
using System.Text.Json;
using OwnS3.Protocol.Auth;
using OwnS3.Protocol.Requests;
using OwnS3.Replication;
using OwnS3.Replication.Transport;

namespace OwnS3.UnitTests;

// Task 7 (t35, глава 07 §6): исходящая SigV4 — roundtrip подлинка через СОБСТВЕННЫЙ
// SigV4HeaderVerifier (взаимная совместимость граней); парсинг фетч-ответов.
public sealed class SigV4SignerFixture
{
    public const string AccessKey = "peer-ak";
    public const string SecretKey = "peer-secret-0123456789";
    public static readonly DateTimeOffset At = new(2026, 10, 11, 12, 0, 0, TimeSpan.Zero);
}

public class SigV4SignerTests
{
    private const string AccessKey = SigV4SignerFixture.AccessKey;
    private const string SecretKey = SigV4SignerFixture.SecretKey;
    private static readonly DateTimeOffset At = SigV4SignerFixture.At;

    [Fact]
    public void OutgoingSignature_PassesOwnVerifier()
    {
        // Arrange — подписываем исходящий POST /replication/notifications с JSON-телом
        var payload = SigV4Core.HexSha256("{\"originHostId\":\"mesh-a\",\"events\":[]}"u8.ToArray());
        var amzDate = At.ToString("yyyyMMdd'T'HHmmss'Z'");

        // Act
        var (auth, _, _) = SigV4Signer.Sign(SecretKey, AccessKey, "POST", "/replication/notifications", "",
            [("host", "neighbor:9000"), ("x-amz-date", amzDate), ("x-amz-content-sha256", payload)],
            payload, At);

        // Assert — СОБСТВЕННЫЙ SigV4HeaderVerifier подтверждает подпись (грани совместимы);
        // время верификатора фиксировано (skew ±15 мин от At)
        var model = BuildRequestModel("/replication/notifications", "", auth, amzDate, payload);
        var result = new SigV4HeaderVerifier(new FixedTimeProvider(At)).Verify(model, _ => SecretKey);
        result.Should().BeOfType<SigV4Result.Ok>();
    }

    [Fact]
    public void OutgoingSignature_WithQuery_PassesOwnVerifier()
    {
        // Arrange — GET с query (листинг: маркер/максимум)
        var payload = SigV4Core.EmptySha256;
        var amzDate = At.ToString("yyyyMMdd'T'HHmmss'Z'");

        // Act
        var (auth, _, _) = SigV4Signer.Sign(SecretKey, AccessKey, "GET", "/replication/listing",
            "bucket=b&marker=m2&max-keys=100",
            [("host", "neighbor:9000"), ("x-amz-date", amzDate), ("x-amz-content-sha256", payload)],
            payload, At);

        // Assert — canonical query верификатора (сортировка/кодирование) согласован
        var model = BuildRequestModel("/replication/listing", "bucket=b&marker=m2&max-keys=100", auth,
            amzDate, payload);
        var result = new SigV4HeaderVerifier(new FixedTimeProvider(At)).Verify(model, _ => SecretKey);
        result.Should().BeOfType<SigV4Result.Ok>();
    }

    [Fact]
    public async Task FetchObject_ParsesHeadersBodyUserMetadata()
    {
        // Arrange — транспорт с фейк-хендлером: 200 + заголовки x-owns3-repl-*/x-amz-meta-*
        using var body = new MemoryStream("replica-bytes"u8.ToArray());
        var handler = new FakeHandler(request =>
        {
            request.RequestUri!.PathAndQuery.Should().Be("/replication/object/bucket/key");
            var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(body) };
            response.Content.Headers.TryAddWithoutValidation("Content-Type", "application/x-test");
            response.Headers.TryAddWithoutValidation("x-owns3-repl-modtime", "1760000000000");
            response.Headers.TryAddWithoutValidation("x-owns3-repl-sha256", "abc123");
            response.Headers.TryAddWithoutValidation("x-owns3-repl-etag", "\"etag1\"");
            response.Headers.TryAddWithoutValidation("x-owns3-repl-origin", "mesh-1");
            response.Headers.TryAddWithoutValidation("x-amz-meta-origin", "mesh-1");
            return response;
        });
        var transport = new ReplicationHttpClient(new HttpClient(handler) { BaseAddress = new Uri("http://neighbor:9000") });

        // Act
        var result = await transport.FetchObjectAsync(Target("peer-a"), "bucket", "key",
            TestContext.Current.CancellationToken);

        // Assert — все заголовки спарсены, тело потоковое (вариант Fetched)
        result.IsSuccess.Should().BeTrue();
        var fetched = result.Value.Should().BeOfType<FetchOutcome.Fetched>().Which;
        fetched.Body.Should().NotBeNull();
        fetched.ModTimeUnixMs.Should().Be(1760000000000);
        fetched.ContentSha256.Should().Be("abc123");
        fetched.ETag.Should().Be("\"etag1\"");
        fetched.ContentType.Should().Be("application/x-test");
        fetched.UserMetadata.Should().ContainKey("origin").WhoseValue.Should().Be("mesh-1");
        fetched.Body.Dispose();
    }

    [Fact]
    public async Task FetchObject_TombstoneHeader_ReturnsTombstoneNoBody()
    {
        // Arrange — 200 + x-owns3-repl-tombstone (ключ удалён)
        var handler = new FakeHandler(_ =>
        {
            var response = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new ByteArrayContent([]),
            };
            response.Headers.TryAddWithoutValidation("x-owns3-repl-tombstone", "1760001111000");
            return response;
        });
        var transport = new ReplicationHttpClient(new HttpClient(handler) { BaseAddress = new Uri("http://neighbor:9000") });

        // Act
        var result = await transport.FetchObjectAsync(Target("peer-a"), "b", "gone",
            TestContext.Current.CancellationToken);

        // Assert — tombstone-время, тела нет (вариант Tombstone)
        result.IsSuccess.Should().BeTrue();
        var tombstone = result.Value.Should().BeOfType<FetchOutcome.Tombstone>().Which;
        tombstone.DeleteTimeUnixMs.Should().Be(1760001111000);
    }

    [Fact]
    public async Task FetchObject_404_ReturnsUnknownKey()
    {
        // Arrange — 404 (ключ неизвестен, tombstone-знание истекло)
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.NotFound));
        var transport = new ReplicationHttpClient(new HttpClient(handler) { BaseAddress = new Uri("http://neighbor:9000") });

        // Act
        var result = await transport.FetchObjectAsync(Target("peer-a"), "b", "none",
            TestContext.Current.CancellationToken);

        // Assert — терминальный Success KeyNotFound (состояние приёмника не меняется)
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeOfType<FetchOutcome.KeyNotFound>();
    }

    [Fact]
    public async Task SendNotifications_ParsesAcceptedToSeqNo()
    {
        // Arrange — 200 {"acceptedToSeqNo": 42}
        var handler = new FakeHandler(_ =>
        {
            var response = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new ByteArrayContent("{\"acceptedToSeqNo\":42}"u8.ToArray()),
            };
            return response;
        });
        var transport = new ReplicationHttpClient(new HttpClient(handler) { BaseAddress = new Uri("http://neighbor:9000") });
        var batch = new NotificationBatchRequest("mesh-a",
            [new ReplicationEvent(1, ReplicationEventType.BucketCreate, "b", "", 1, "", 0, "", "mesh-a")]);

        // Act
        var accepted = (await transport.SendNotificationsAsync(Target("peer-a"), batch,
            TestContext.Current.CancellationToken)).Throw().Value;

        // Assert
        accepted.Should().Be(42);
    }

    [Fact]
    public async Task SendNotifications_429_ReturnsFailed()
    {
        // Arrange — приёмный буфер переполнен
        var handler = new FakeHandler(_ => new HttpResponseMessage(HttpStatusCode.TooManyRequests));
        var transport = new ReplicationHttpClient(new HttpClient(handler) { BaseAddress = new Uri("http://neighbor:9000") });
        var batch = new NotificationBatchRequest("mesh-a", []);

        // Act
        var accepted = await transport.SendNotificationsAsync(Target("peer-a"), batch,
            TestContext.Current.CancellationToken);

        // Assert — 429 → Failed(PeerRejectedException): вызывающий бэкоффит (глава 07 §8;
        // семантика критерия 5 — «не приняли» = отказ)
        (!accepted.IsSuccess).Should().BeTrue();
        accepted.Error.Should().BeOfType<PeerRejectedException>();
    }

    [Fact]
    public async Task ListingBuckets_ParsesComposition()
    {
        // Arrange — листинг без bucket: {buckets:[...]}
        var handler = new FakeHandler(request =>
        {
            request.RequestUri!.PathAndQuery.Should().Be("/replication/listing");
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new ByteArrayContent(
                    "{\"buckets\":[{\"name\":\"b1\",\"creationDateUnixMs\":1760000000000}]}"u8.ToArray()),
            };
        });
        var transport = new ReplicationHttpClient(new HttpClient(handler) { BaseAddress = new Uri("http://neighbor:9000") });

        // Act
        var buckets = (await transport.ListingBucketsAsync(Target("peer-a"),
            TestContext.Current.CancellationToken)).Throw().Value;

        // Assert — состав бакетов соседа (backfill, глава 07 §9)
        buckets.Should().ContainSingle().Which.Name.Should().Be("b1");
        buckets[0].CreationUnixMs.Should().Be(1760000000000);
    }

    [Fact]
    public void EventJson_TypeRoundtrip_KebabCase()
    {
        // Arrange — событие с типом object-put
        var e = new ReplicationEvent(7, ReplicationEventType.ObjectPut, "b", "k", 1760000000000,
            "\"etag\"", 10, "sha256hex", "mesh-a");

        // Act — сериализация/десериализация транспортом
        var json = JsonSerializer.Serialize(e, ReplicationTransportJson.Options);
        var parsed = JsonSerializer.Deserialize<ReplicationEvent>(json, ReplicationTransportJson.Options);

        // Assert — type в JSON "object-put", круговой разбор без потерь
        json.Should().Contain("\"type\":\"object-put\"");
        parsed.Should().Be(e);
    }

    private static TargetRecord Target(string name) =>
        new(name, "http://neighbor:9000", AccessKey, SecretKey);

    private static S3RequestModel BuildRequestModel(string path, string query, string authorization,
        string amzDate, string payloadHash) =>
        new()
        {
            Method = path.Contains("listing") ? "GET" : "POST",
            RawPath = path,
            RawQuery = query,
            Headers = S3HeaderCollection.FromPairs(
                ("Authorization", authorization),
                ("x-amz-date", amzDate),
                ("x-amz-content-sha256", payloadHash),
                ("host", "neighbor:9000")),
            Host = "neighbor:9000",
            OpenBody = () => Stream.Null,
        };

    // Фейк-хендлер транспорта: подмена ответа без сети.
    private sealed class FakeHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
            CancellationToken cancellationToken) =>
            Task.FromResult(responder(request));
    }
}
