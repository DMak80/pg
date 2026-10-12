using System.Net;
using System.Text;
using System.Text.Json;

namespace OwnS3.IntegrationTests.Api;

// Админ-грань mesh-репликации (t35, глава 07 §2/§10): PUT targets (202, hot-apply,
// персист), status (стейт + счётчики исходов), reconcile (202), sync-report
// (XOR-hash, порядко-независимость), права (replication/writer → 403).
public sealed class ReplicationAdminScenarios(ReplicationAppFactory factory) :
    IClassFixture<ReplicationAppFactory>
{
    private OwnS3TestClient Client => new(factory.CreateClient());

    private static string PayloadHash(string body) =>
        Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes(body)))
            .ToLowerInvariant();

    [Fact]
    public async Task PutTargets_202_Persisted_Idempotent()
    {
        // Arrange — валидный список соседей
        var body = """{"targets":[{"name":"peer-x","endpoint":"http://peer-x:9000","accessKey":"akx","secretKey":"skx"}]}""";

        // Act — замена + повтор того же списка
        var first = await Client.SendSignedAsync("PUT", "/admin/replication/targets",
            payloadString: PayloadHash(body), body: Encoding.UTF8.GetBytes(body),
            credentials: OwnS3TestClient.Admin());
        var second = await Client.SendSignedAsync("PUT", "/admin/replication/targets",
            payloadString: PayloadHash(body), body: Encoding.UTF8.GetBytes(body),
            credentials: OwnS3TestClient.Admin());

        // Assert — оба 202 (идемпотентно); персист виден статусом
        first.StatusCode.Should().Be(HttpStatusCode.Accepted);
        second.StatusCode.Should().Be(HttpStatusCode.Accepted);
        var status = await Client.SendSignedAsync("GET", "/admin/replication/status",
            credentials: OwnS3TestClient.Admin());
        status.StatusCode.Should().Be(HttpStatusCode.OK);
        (await status.Content.ReadAsStringAsync(TestContext.Current.CancellationToken))
            .Should().Contain("peer-x");
    }

    [Fact]
    public async Task PutTargets_DuplicateName_400()
    {
        // Arrange — дубль имени соседа
        var body = """{"targets":[{"name":"dup","endpoint":"http://a:9000","accessKey":"a","secretKey":"s"},{"name":"dup","endpoint":"http://b:9000","accessKey":"b","secretKey":"t"}]}""";

        // Act
        var response = await Client.SendSignedAsync("PUT", "/admin/replication/targets",
            payloadString: PayloadHash(body), body: Encoding.UTF8.GetBytes(body),
            credentials: OwnS3TestClient.Admin());

        // Assert — валидация имён → 400 (глава 07 §2)
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task PutTargets_DuplicateAccessKey_400()
    {
        // Arrange — дубль accessKey (ревью t35 finding 5: accessKey — идентификатор
        // источника на приёмнике, общие креды ломают дедуп (источник, seqNo))
        var body = """{"targets":[{"name":"dup-a","endpoint":"http://a:9000","accessKey":"same-ak","secretKey":"s1"},{"name":"dup-b","endpoint":"http://b:9000","accessKey":"same-ak","secretKey":"s2"}]}""";

        // Act
        var response = await Client.SendSignedAsync("PUT", "/admin/replication/targets",
            payloadString: PayloadHash(body), body: Encoding.UTF8.GetBytes(body),
            credentials: OwnS3TestClient.Admin());

        // Assert — валидация accessKey → 400 (глава 07 §2)
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task PutTargets_InvalidEndpoint_400()
    {
        // Arrange — endpoint вне scheme://host:port (ревью t35 finding 13)
        var body = """{"targets":[{"name":"bad-ep","endpoint":"peer-a:9000","accessKey":"ak","secretKey":"sk"}]}""";

        // Act
        var response = await Client.SendSignedAsync("PUT", "/admin/replication/targets",
            payloadString: PayloadHash(body), body: Encoding.UTF8.GetBytes(body),
            credentials: OwnS3TestClient.Admin());

        // Assert — валидация endpoint → 400 (глава 07 §2)
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Status_StructureAndOutcomeCounters()
    {
        // Arrange — восстановить peer-a (кейс PutTargets выше заменил состав на peer-x):
        // accessKey соседа = replication-ключ фабрики (identity батчей)
        var restore = "{\"targets\":[{\"name\":\"peer-a\",\"endpoint\":\"http://127.0.0.1:9\",\"accessKey\":\"" + OwnS3AppFactory.ReplicationAccessKey + "\",\"secretKey\":\"" + OwnS3AppFactory.ReplicationSecretKey + "\"}]}";
        await Client.SendSignedAsync("PUT", "/admin/replication/targets",
            payloadString: PayloadHash(restore), body: Encoding.UTF8.GetBytes(restore),
            credentials: OwnS3TestClient.Admin());

        // Принятый батч от peer-a (counters.applied через inbox-точку)
        var batch = JsonSerializer.Serialize(new OwnS3.Replication.Transport.NotificationBatchRequest("mesh-a",
        [
            new OwnS3.Replication.ReplicationEvent(1100, OwnS3.Replication.ReplicationEventType.BucketCreate,
                "adm-bucket", "", 100, "", 0, "", "peer"),
        ]), OwnS3.Replication.Transport.ReplicationTransportJson.Options);
        await Client.SendSignedAsync("POST", "/replication/notifications",
            payloadString: PayloadHash(batch), body: Encoding.UTF8.GetBytes(batch),
            credentials: new OwnS3TestClient.Credentials(OwnS3AppFactory.ReplicationAccessKey,
                OwnS3AppFactory.ReplicationSecretKey));

        // Act
        var response = await Client.SendSignedAsync("GET", "/admin/replication/status",
            credentials: OwnS3TestClient.Admin());

        // Assert — структура полей, включая счётчики исходов (applied ≥ 1 от peer-a)
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var json = JsonDocument.Parse(
            await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
        json.GetProperty("active").GetBoolean().Should().BeTrue();
        json.GetProperty("outbox").GetProperty("budgetBytes").GetInt64().Should().BeGreaterThan(0);
        var peers = json.GetProperty("neighbors").EnumerateArray().ToList();
        peers.Should().Contain(p => p.GetProperty("name").GetString() == "peer-a");
        var peerA = peers.Single(p => p.GetProperty("name").GetString() == "peer-a");
        peerA.GetProperty("counters").GetProperty("applied").GetInt64().Should().BeGreaterThanOrEqualTo(1);
    }

    [Fact]
    public async Task Reconcile_202_Idempotent()
    {
        // Act — два прогона подряд
        var first = await Client.SendSignedAsync("POST", "/admin/replication/reconcile",
            payloadString: PayloadHash(string.Empty), credentials: OwnS3TestClient.Admin());
        var second = await Client.SendSignedAsync("POST", "/admin/replication/reconcile",
            payloadString: PayloadHash(string.Empty), credentials: OwnS3TestClient.Admin());

        // Assert — оба 202 (идущий продолжается, параллельный не создаётся)
        first.StatusCode.Should().Be(HttpStatusCode.Accepted);
        second.StatusCode.Should().Be(HttpStatusCode.Accepted);
    }

    [Fact]
    public async Task SyncReport_CountTotalSizeHash_MarkerPaging()
    {
        // Arrange — 3 ключа в бакете (CreateBucket admin)
        await Client.SendSignedAsync("PUT", "/adm-rep", credentials: OwnS3TestClient.Admin());
        foreach (var key in new[] { "a", "m", "z" })
        {
            var put = await Client.SendSignedAsync("PUT", $"/adm-rep/{key}",
                payloadString: PayloadHash("vv"), body: "vv"u8.ToArray());
            put.StatusCode.Should().Be(HttpStatusCode.OK);
        }

        // Act — полный отчёт и страница от маркера
        var full = JsonDocument.Parse(await (await Client.SendSignedAsync(
            "GET", "/admin/replication/sync-report?bucket=adm-rep",
            credentials: OwnS3TestClient.Admin())).Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
        var paged = JsonDocument.Parse(await (await Client.SendSignedAsync(
            "GET", "/admin/replication/sync-report?bucket=adm-rep&marker=a",
            credentials: OwnS3TestClient.Admin())).Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;

        // Assert — count/totalSize/first/last; страница от маркера — остаток
        full.GetProperty("count").GetInt32().Should().Be(3);
        full.GetProperty("totalSize").GetInt64().Should().Be(6);
        full.GetProperty("firstKey").GetString().Should().Be("a");
        full.GetProperty("lastKey").GetString().Should().Be("z");
        full.GetProperty("aggregateHash").GetString().Should().HaveLength(16);
        paged.GetProperty("count").GetInt32().Should().Be(2);
    }

    [Fact]
    public async Task SyncReport_AggregateHash_OrderIndependent()
    {
        // Arrange — ДВА набора одинаковых (key, modTime) в разном порядке вставки:
        // в один бакет ключи вставлены a,b; в другой — b,a
        await Client.SendSignedAsync("PUT", "/hash-1", credentials: OwnS3TestClient.Admin());
        await Client.SendSignedAsync("PUT", "/hash-2", credentials: OwnS3TestClient.Admin());
        foreach (var key in new[] { "a", "b" })
        {
            var put1 = await Client.SendSignedAsync("PUT", $"/hash-1/{key}",
                payloadString: PayloadHash("same"), body: "same"u8.ToArray());
            put1.StatusCode.Should().Be(HttpStatusCode.OK);
        }
        foreach (var key in new[] { "b", "a" })
        {
            var put2 = await Client.SendSignedAsync("PUT", $"/hash-2/{key}",
                payloadString: PayloadHash("same"), body: "same"u8.ToArray());
            put2.StatusCode.Should().Be(HttpStatusCode.OK);
        }

        // Act
        var report1 = JsonDocument.Parse(await (await Client.SendSignedAsync(
            "GET", "/admin/replication/sync-report?bucket=hash-1",
            credentials: OwnS3TestClient.Admin())).Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;
        var report2 = JsonDocument.Parse(await (await Client.SendSignedAsync(
            "GET", "/admin/replication/sync-report?bucket=hash-2",
            credentials: OwnS3TestClient.Admin())).Content.ReadAsStringAsync(TestContext.Current.CancellationToken)).RootElement;

        // Assert — aggregateHash равен (порядко-независимость XOR-свёртки, глава 07 §10)
        report1.GetProperty("aggregateHash").GetString()
            .Should().Be(report2.GetProperty("aggregateHash").GetString());
    }

    [Fact]
    public async Task AccessControl_ReplicationAndWriter_403()
    {
        // Act / Assert — replication-ключ и writer на все 4 эндпоинта → 403 (A13-зеркало)
        foreach (var creds in new[]
                 {
                     new OwnS3TestClient.Credentials(OwnS3AppFactory.ReplicationAccessKey,
                         OwnS3AppFactory.ReplicationSecretKey),
                     OwnS3TestClient.Writer(),
                 })
        {
            (await Client.SendSignedAsync("PUT", "/admin/replication/targets",
                payloadString: PayloadHash("{}"), body: "{}"u8.ToArray(), credentials: creds))
                .StatusCode.Should().Be(HttpStatusCode.Forbidden);
            (await Client.SendSignedAsync("GET", "/admin/replication/status", credentials: creds))
                .StatusCode.Should().Be(HttpStatusCode.Forbidden);
            (await Client.SendSignedAsync("POST", "/admin/replication/reconcile",
                payloadString: PayloadHash(string.Empty), credentials: creds))
                .StatusCode.Should().Be(HttpStatusCode.Forbidden);
            (await Client.SendSignedAsync("GET", "/admin/replication/sync-report?bucket=x", credentials: creds))
                .StatusCode.Should().Be(HttpStatusCode.Forbidden);
        }
    }
}
