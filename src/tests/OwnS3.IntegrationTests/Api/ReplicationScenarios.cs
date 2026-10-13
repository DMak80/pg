using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using OwnS3.Replication;
using OwnS3.Replication.Transport;

namespace OwnS3.IntegrationTests.Api;

// Фабрика с фиктивным соседом mesh (t35): seed targets из in-memory конфига
// (accessKey соседа = replication-ключ фабрики — sourceName резолвится); плюс
// ВТОРОЙ replication-ключ вне targets (A13-кейс). Notifier в WAF не поднимается.
public class ReplicationAppFactory : OwnS3AppFactory
{
    // Соседние ключи mesh: свой источник на группу кейсов (sourceName по accessKey —
    // независимый per-источниковый дедуп, модель канона глава 07 §6).
    public const string PeerBAccessKey = "testpeer3";
    public const string PeerBSecretKey = "testpeer3secret";
    public const string PeerCAccessKey = "testpeer4";
    public const string PeerCSecretKey = "testpeer4secret";
    // Второй replication-ключ — НЕ сосед (валидный ключ вне targets → 403).
    public const string OrphanAccessKey = "testpeer2";
    public const string OrphanSecretKey = "testpeer2secret";

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        base.ConfigureWebHost(builder);
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["OwnS3:Replication:Targets:0:Name"] = "peer-a",
            ["OwnS3:Replication:Targets:0:Endpoint"] = "http://127.0.0.1:9",
            ["OwnS3:Replication:Targets:0:AccessKey"] = ReplicationAccessKey,
            ["OwnS3:Replication:Targets:0:SecretKey"] = ReplicationSecretKey,
            ["OwnS3:Replication:Targets:1:Name"] = "peer-b",
            ["OwnS3:Replication:Targets:1:Endpoint"] = "http://127.0.0.1:9",
            ["OwnS3:Replication:Targets:1:AccessKey"] = PeerBAccessKey,
            ["OwnS3:Replication:Targets:1:SecretKey"] = PeerBSecretKey,
            ["OwnS3:Replication:Targets:2:Name"] = "peer-c",
            ["OwnS3:Replication:Targets:2:Endpoint"] = "http://127.0.0.1:9",
            ["OwnS3:Replication:Targets:2:AccessKey"] = PeerCAccessKey,
            ["OwnS3:Replication:Targets:2:SecretKey"] = PeerCSecretKey,
            ["OwnS3:AccessKeys:4:AccessKey"] = OrphanAccessKey,
            ["OwnS3:AccessKeys:4:SecretKey"] = OrphanSecretKey,
            ["OwnS3:AccessKeys:4:Policy"] = "replication",
            ["OwnS3:AccessKeys:5:AccessKey"] = PeerBAccessKey,
            ["OwnS3:AccessKeys:5:SecretKey"] = PeerBSecretKey,
            ["OwnS3:AccessKeys:5:Policy"] = "replication",
            ["OwnS3:AccessKeys:6:AccessKey"] = PeerCAccessKey,
            ["OwnS3:AccessKeys:6:SecretKey"] = PeerCSecretKey,
            ["OwnS3:AccessKeys:6:Policy"] = "replication",
        }));
    }
}

// Фабрика с крошечной fetch-очередью (кейс 429-backpressure).
public sealed class ReplicationSmallQueueAppFactory : ReplicationAppFactory
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        base.ConfigureWebHost(builder);
        builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["OwnS3:Replication:FetchQueueMax"] = "1",
        }));
    }
}

// Replication-грань (t35, глава 07 §6): приём уведомлений (дедуп/LWW-предфильтр/429),
// фетч-отдача (заголовки/tombstone/404), листинг (пагинация), изоляция политик (A13).
public sealed class ReplicationScenarios(ReplicationAppFactory factory) : IClassFixture<ReplicationAppFactory>
{
    private OwnS3TestClient Client => new(factory.CreateClient());
    private OwnS3TestClient ReplicationClient => new(factory.CreateClient());

    private OwnS3TestClient.Credentials ReplicationCreds => new(OwnS3AppFactory.ReplicationAccessKey,
        OwnS3AppFactory.ReplicationSecretKey);
    private OwnS3TestClient.Credentials OrphanReplicationCreds => new(ReplicationAppFactory.OrphanAccessKey,
        ReplicationAppFactory.OrphanSecretKey);
    private OwnS3TestClient.Credentials PeerBCreds => new(ReplicationAppFactory.PeerBAccessKey,
        ReplicationAppFactory.PeerBSecretKey);
    private OwnS3TestClient.Credentials PeerCCreds => new(ReplicationAppFactory.PeerCAccessKey,
        ReplicationAppFactory.PeerCSecretKey);

    private static string Batch(params ReplicationEvent[] events) =>
        JsonSerializer.Serialize(new NotificationBatchRequest("mesh-a", events), ReplicationTransportJson.Options);

    private static ReplicationEvent BucketCreate(long seqNo, string bucket, long modMs) =>
        new(seqNo, ReplicationEventType.BucketCreate, bucket, "", modMs, "", 0, "", "mesh-a");

    private static ReplicationEvent ObjectDelete(long seqNo, string bucket, string key, long modMs) =>
        new(seqNo, ReplicationEventType.ObjectDelete, bucket, key, modMs, "", 0, "", "mesh-a");

    private static ReplicationEvent ObjectPut(long seqNo, string bucket, string key, long modMs) =>
        new(seqNo, ReplicationEventType.ObjectPut, bucket, key, modMs, "\"e\"", 3, "abc", "mesh-a");

    [Fact]
    public async Task Notifications_SelfSufficientEvents_AppliedFromNotification()
    {
        // Arrange — батч: bucket-create + object-delete (нет данных локально);
        // seqNo-пространство кейса — 200+ (у источника сквозной seqNo, кейсы не пересекаются)
        var client = Client;
        var batch = Batch(BucketCreate(300, "repl-self", 100), ObjectDelete(301, "repl-self", "gone", 200));

        // Act — свой источник peer-c (независимый дедуп per-источник, глава 07 §6)
        var response = await ReplicationClient.SendSignedAsync("POST", "/replication/notifications",
            payloadString: PayloadHash(batch), body: Encoding.UTF8.GetBytes(batch),
            credentials: PeerCCreds);

        // Assert — 200 + acceptedToSeqNo=301; бакет создан (через S3-грань writer'а ListBuckets)
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        JsonDocument.Parse(body).RootElement.GetProperty("acceptedToSeqNo").GetInt64().Should().Be(301);
        var buckets = await client.SendSignedAsync("GET", "/", credentials: OwnS3TestClient.Writer());
        buckets.StatusCode.Should().Be(HttpStatusCode.OK);
        (await buckets.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("repl-self");
    }

    [Fact]
    public async Task Notifications_ObjectPut_LwwPrefilter_QueuesOrSkips()
    {
        // Arrange — локальный ключ с modTime новее уведомления (CreateBucket — admin, матрица 05 §3)
        var client = Client;
        await client.SendSignedAsync("PUT", "/repl-pre", credentials: OwnS3TestClient.Admin());
        const string body = "local-actual";
        await client.SendSignedAsync("PUT", "/repl-pre/key", payloadString: PayloadHash(body),
            body: Encoding.UTF8.GetBytes(body));
        var batch = Batch(ObjectPut(400, "repl-pre", "key", modMs: 100));

        // Act — батч object-put со старым modTime (свой источник peer-b)
        var response = await ReplicationClient.SendSignedAsync("POST", "/replication/notifications",
            payloadString: PayloadHash(batch), body: Encoding.UTF8.GetBytes(batch),
            credentials: PeerBCreds);

        // Assert — 200 + acceptedToSeqNo; ключ НЕ перезаписан (skip без фетча)
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var get = await client.SendSignedAsync("GET", "/repl-pre/key");
        get.StatusCode.Should().Be(HttpStatusCode.OK);
        (await get.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).Should().Be(body);
    }

    [Fact]
    public async Task Notifications_FetchQueueFull_Returns429()
    {
        // Arrange — capacity=1 (фабрика с малой очередью), батч из 2 новых put-ключей
        var client = new OwnS3TestClient(new ReplicationSmallQueueAppFactory().CreateClient());
        var batch = Batch(ObjectPut(200, "repl-full", "k1", modMs: 100), ObjectPut(201, "repl-full", "k2", modMs: 200));

        // Act
        var response = await client.SendSignedAsync("POST", "/replication/notifications",
            payloadString: PayloadHash(batch), body: Encoding.UTF8.GetBytes(batch),
            credentials: ReplicationCreds);

        // Assert — приёмный буфер переполнен → 429 (источник бэкоффит)
        response.StatusCode.Should().Be(HttpStatusCode.TooManyRequests);
    }

    [Fact]
    public async Task Notifications_DuplicateBatch_IdempotentAccept()
    {
        // Arrange — тот же батч дважды
        var client = Client;
        var batch = Batch(BucketCreate(100, "repl-dup", 100), ObjectDelete(101, "repl-dup", "gone", 200));
        var first = await ReplicationClient.SendSignedAsync("POST", "/replication/notifications",
            payloadString: PayloadHash(batch), body: Encoding.UTF8.GetBytes(batch),
            credentials: ReplicationCreds);
        first.StatusCode.Should().Be(HttpStatusCode.OK);

        // Act — повторный POST того же батча
        var second = await ReplicationClient.SendSignedAsync("POST", "/replication/notifications",
            payloadString: PayloadHash(batch), body: Encoding.UTF8.GetBytes(batch),
            credentials: ReplicationCreds);

        // Assert — 200 с тем же acceptedToSeqNo, применение не продублировано
        // (бакет жив один, состояние неизменно)
        second.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await second.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        JsonDocument.Parse(body).RootElement.GetProperty("acceptedToSeqNo").GetInt64().Should().Be(101);
        var buckets = await client.SendSignedAsync("GET", "/", credentials: OwnS3TestClient.Writer());
        (await buckets.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("repl-dup");
    }

    [Fact]
    public async Task ObjectFetch_ReturnsHeadersBody_Tombstone_And404()
    {
        // Arrange — объект записан writer'ом; второй ключ удалён tombstone-уведомлением
        // (client-DELETE → tombstone подключается задачей 10; здесь — путь реплики §6)
        var client = Client;
        await client.SendSignedAsync("PUT", "/repl-fetch", credentials: OwnS3TestClient.Admin());
        const string content = "replica-body";
        await client.SendSignedAsync("PUT", "/repl-fetch/obj", payloadString: PayloadHash(content),
            body: Encoding.UTF8.GetBytes(content));
        var deleteBatch = Batch(ObjectDelete(700, "repl-fetch", "gone", modMs: 500));
        var deleteResponse = await ReplicationClient.SendSignedAsync("POST", "/replication/notifications",
            payloadString: PayloadHash(deleteBatch), body: Encoding.UTF8.GetBytes(deleteBatch),
            credentials: PeerBCreds);
        deleteResponse.StatusCode.Should().Be(HttpStatusCode.OK);

        // Act — три ветки фетча replication-ключом (пути replication-грани, глава 07 §6)
        var live = await ReplicationClient.SendSignedAsync("GET", "/replication/object/repl-fetch/obj",
            credentials: ReplicationCreds);
        var deleted = await ReplicationClient.SendSignedAsync("GET", "/replication/object/repl-fetch/gone",
            credentials: ReplicationCreds);
        var unknown = await ReplicationClient.SendSignedAsync("GET", "/replication/object/repl-fetch/none",
            credentials: ReplicationCreds);

        // Assert — объект: 200 + тело + заголовки (sha тела совпадает)
        live.StatusCode.Should().Be(HttpStatusCode.OK);
        (await live.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).Should().Be(content);
        live.Headers.Should().ContainKey("x-owns3-repl-modtime");
        live.Headers.Should().ContainKey("x-owns3-repl-sha256");
        live.Headers.Should().ContainKey("x-owns3-repl-etag");
        live.Headers.Should().ContainKey("x-owns3-repl-origin");

        // Assert — tombstone: 200 + x-owns3-repl-tombstone без тела
        deleted.StatusCode.Should().Be(HttpStatusCode.OK);
        deleted.Headers.Should().ContainKey("x-owns3-repl-tombstone");

        // Assert — неизвестный ключ: 404 (состояние приёмника не меняется)
        unknown.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Listing_PagesByMarker()
    {
        // Arrange — 3 ключа в бакете (бакет — writer'ом)
        var client = Client;
        await client.SendSignedAsync("PUT", "/repl-list", credentials: OwnS3TestClient.Admin());
        foreach (var key in new[] { "a", "m", "z" })
        {
            var put = await client.SendSignedAsync("PUT", $"/repl-list/{key}",
                payloadString: PayloadHash("v"), body: "v"u8.ToArray());
            put.StatusCode.Should().Be(HttpStatusCode.OK);
        }

        // Act — страница 1 (max-keys=2), затем страница 2 от маркера
        var page1 = await ReplicationClient.SendSignedAsync("GET", "/replication/listing?bucket=repl-list&max-keys=2",
            credentials: ReplicationCreds);
        var page1Body = JsonDocument.Parse(
            await page1.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
        var nextMarker = page1Body.GetProperty("nextMarker").GetString();

        // Assert — первая страница + nextMarker
        page1.StatusCode.Should().Be(HttpStatusCode.OK);
        page1Body.GetProperty("entries").GetArrayLength().Should().Be(2);
        nextMarker.Should().NotBeNullOrEmpty();

        // Act — вторая страница от маркера
        var page2 = await ReplicationClient.SendSignedAsync(
            $"GET", $"/replication/listing?bucket=repl-list&marker={Uri.EscapeDataString(nextMarker!)}&max-keys=2",
            credentials: ReplicationCreds);
        var page2Body = JsonDocument.Parse(
            await page2.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;

        // Assert — остаток без повторов; конец листинга — null-маркер
        page2Body.GetProperty("entries").GetArrayLength().Should().Be(1);
        page2Body.GetProperty("entries")[0].GetProperty("key").GetString().Should().NotBe(nextMarker);
        page2Body.GetProperty("nextMarker").ValueKind.Should().Be(JsonValueKind.Null);
    }

    [Fact]
    public async Task ReplicationEdge_PolicyIsolation()
    {
        // Arrange — ключи всех ролей; seqNo вне других кейсов (дедуп не влияет на 403)
        var batch = Batch(BucketCreate(600, "repl-iso", 100));

        // Act / Assert — read-only/read-write/admin на replication-грани → 403 AccessDenied
        foreach (var creds in new[] { OwnS3TestClient.Reader(), OwnS3TestClient.Writer(), OwnS3TestClient.Admin() })
        {
            var response = await Client.SendSignedAsync("POST", "/replication/notifications",
                payloadString: PayloadHash(batch), body: Encoding.UTF8.GetBytes(batch), credentials: creds);
            response.StatusCode.Should().Be(HttpStatusCode.Forbidden,
                because: "грань доступна только политике replication (A13)");
        }

        // Act / Assert — replication-ключ, НЕ числящийся соседом → 403
        var orphan = await ReplicationClient.SendSignedAsync("POST", "/replication/notifications",
            payloadString: PayloadHash(batch), body: Encoding.UTF8.GetBytes(batch),
            credentials: OrphanReplicationCreds);
        orphan.StatusCode.Should().Be(HttpStatusCode.Forbidden);

        // Act / Assert — replication-ключ на 21 операцию (матрица A13) → 403
        var put = await ReplicationClient.SendSignedAsync("PUT", "/repl-iso/obj",
            payloadString: PayloadHash("x"), body: "x"u8.ToArray(), credentials: ReplicationCreds);
        put.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        var list = await ReplicationClient.SendSignedAsync("GET", "/repl-iso",
            credentials: ReplicationCreds);
        list.StatusCode.Should().Be(HttpStatusCode.Forbidden);
    }

    [Fact]
    public async Task Ping_ValidNeighborSignature_200EmptyBody()
    {
        // Act — GET /replication/ping подписью соседа (accessKey peer-a = ReplicationCreds)
        var response = await ReplicationClient.SendSignedAsync("GET", "/replication/ping",
            credentials: ReplicationCreds);

        // Assert — 200, пустое тело (глава 07 §6)
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        (await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().BeEmpty();
    }

    [Fact]
    public async Task Ping_PolicyIsolation_403()
    {
        // Act / Assert — read-only/read-write/admin на ping-грани → 403 (A13)
        foreach (var creds in new[] { OwnS3TestClient.Reader(), OwnS3TestClient.Writer(), OwnS3TestClient.Admin() })
            (await Client.SendSignedAsync("GET", "/replication/ping", credentials: creds))
                .StatusCode.Should().Be(HttpStatusCode.Forbidden);

        // Act / Assert — replication-ключ, НЕ числящийся соседом → 403 (глава 07 §6)
        (await ReplicationClient.SendSignedAsync("GET", "/replication/ping",
            credentials: OrphanReplicationCreds)).StatusCode.Should().Be(HttpStatusCode.Forbidden);
    }

    [Fact]
    public async Task Ping_NonGetMethods_400_UnknownPath_400()
    {
        // Act — POST/PUT ping + GET неизвестного пути грани (регресс маршрутизации)
        var post = await ReplicationClient.SendSignedAsync("POST", "/replication/ping",
            payloadString: PayloadHash(string.Empty), credentials: ReplicationCreds);
        var put = await ReplicationClient.SendSignedAsync("PUT", "/replication/ping",
            payloadString: PayloadHash(string.Empty), credentials: ReplicationCreds);
        var unknown = await ReplicationClient.SendSignedAsync("GET", "/replication/no-such",
            credentials: ReplicationCreds);

        // Assert — 400 InvalidArgument по существующему правилу грани (глава 07 §6)
        post.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        put.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        unknown.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Ping_ReceiverPurity_StatusCountersUnchanged()
    {
        // Arrange — стабильный срез статуса: счётчики/курсоры/fetch-очереди
        // (время-зависимые deliveryLagSeconds/fetchLagSeconds исключены)
        async Task<string> StableNeighborsAsync()
        {
            var response = await Client.SendSignedAsync("GET", "/admin/replication/status",
                credentials: OwnS3TestClient.Admin());
            var json = JsonDocument.Parse(
                await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
            return System.Text.Json.JsonSerializer.Serialize(json.GetProperty("neighbors")
                .EnumerateArray().Select(n => new
                {
                    name = n.GetProperty("name").GetString(),
                    acceptedSeqNo = n.GetProperty("acceptedSeqNo").GetInt64(),
                    queueDepth = n.GetProperty("queueDepth").GetInt64(),
                    fetchQueueDepth = n.GetProperty("fetchQueueDepth").GetInt64(),
                    fetchPaused = n.GetProperty("fetchPaused").GetBoolean(),
                    reconcileRunning = n.GetProperty("reconcileRunning").GetBoolean(),
                    counters = n.GetProperty("counters").EnumerateObject()
                        .ToDictionary(p => p.Name, p => p.Value.GetRawText()),
                }));
        }

        // Act — серия ping-запросов к узлу-приёмнику (принцип 3: ping бесплатен)
        var before = await StableNeighborsAsync();
        for (var i = 0; i < 10; i++)
        {
            var ping = await ReplicationClient.SendSignedAsync("GET", "/replication/ping",
                credentials: ReplicationCreds);
            ping.StatusCode.Should().Be(HttpStatusCode.OK);
        }

        // Assert — состояние приёмника байт-в-байт то же: срезы до/после идентичны
        var after = await StableNeighborsAsync();
        after.Should().Be(before);
    }

    private static string PayloadHash(string body) =>
        Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes(body)))
            .ToLowerInvariant();
}
