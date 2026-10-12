using System.Text;
using System.Text.Json;
using OwnS3.Replication;
using OwnS3.Replication.Outbox;
using OwnS3.Replication.Transport;

namespace OwnS3.IntegrationTests.Api;

// События репликации в 7 хендлерах (t35, глава 07 §3): клиентские операции после
// коммит-поинта кладут события в outbox; приём реплики событий НЕ ставит (анти-петля);
// без соседей журнал не ведётся (Existing-регресс — каталога нет).
public sealed class ReplicationEventsScenarios(ReplicationAppFactory factory) :
    IClassFixture<ReplicationAppFactory>
{
    private OwnS3TestClient Client => new(factory.CreateClient());

    private OwnS3TestClient.Credentials ReplicationCreds => new(OwnS3AppFactory.ReplicationAccessKey,
        OwnS3AppFactory.ReplicationSecretKey);

    private string OutboxDir => Path.Combine(factory.TempVolumeDir, ".owns3.sys", "replication", "outbox");

    // Декод всех событий журнала (бинарный формат v1, глава 07 §4).
    private static List<ReplicationEvent> ReadOutbox(string outboxDir)
    {
        var events = new List<ReplicationEvent>();
        if (!Directory.Exists(outboxDir))
            return events;
        foreach (var segment in Directory.GetFiles(outboxDir, "seg-*.log").OrderBy(f => f))
        {
            var bytes = File.ReadAllBytes(segment);
            var offset = 0;
            while (offset < bytes.Length)
            {
                if (!OutboxEntryCodec.TryDecode(bytes, offset, out var e, out offset))
                    break;
                events.Add(e);
            }
        }
        return events;
    }

    private static string PayloadHash(string body) =>
        Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes(body)))
            .ToLowerInvariant();

    [Fact]
    public async Task NoNeighbors_NoOutboxDirectory()
    {
        // Arrange — базовая фабрика БЕЗ соседей (свой том)
        var factory = new OwnS3AppFactory();
        try
        {
            var client = new OwnS3TestClient(factory.CreateClient());
            await client.SendSignedAsync("PUT", "/no-neigh", credentials: OwnS3TestClient.Admin());
            await client.SendSignedAsync("PUT", "/no-neigh/k", payloadString: PayloadHash("v"), body: "v"u8.ToArray());
            await client.SendSignedAsync("DELETE", "/no-neigh/k");

            // Act / Assert — в .owns3.sys/replication НЕТ каталога outbox (события не ставятся)
            Directory.Exists(Path.Combine(factory.TempVolumeDir, ".owns3.sys", "replication", "outbox"))
                .Should().BeFalse();
        }
        finally
        {
            // Teardown: том сценария удаляется при любом исходе
            factory.Dispose();
            if (Directory.Exists(factory.TempVolumeDir))
                Directory.Delete(factory.TempVolumeDir, recursive: true);
        }
    }

    [Fact]
    public async Task PutObject_EventCarriesPutMetadata()
    {
        // Arrange — сосед задан; PUT writer'ом
        var client = Client;
        await client.SendSignedAsync("PUT", "/ev-put", credentials: OwnS3TestClient.Admin());
        const string content = "event-body";
        var put = await client.SendSignedAsync("PUT", "/ev-put/k", payloadString: PayloadHash(content),
            body: Encoding.UTF8.GetBytes(content));
        put.StatusCode.Should().Be(System.Net.HttpStatusCode.OK);

        // Act — чтение журнала бинарным кодеком
        var events = ReadOutbox(OutboxDir);

        // Assert — событие object-put кейса (outbox общий на фабрику-класс — фильтруем по ключу)
        var putEvent = events.Single(e => e.Type == ReplicationEventType.ObjectPut
            && e.Bucket == "ev-put" && e.Key == "k");
        putEvent.Bucket.Should().Be("ev-put");
        putEvent.Key.Should().Be("k");
        putEvent.Size.Should().Be(content.Length);
        putEvent.ContentSha256.Should().Be(PayloadHash(content));
        putEvent.ETag.Should().NotBeNullOrEmpty();
        putEvent.ModTimeUnixMs.Should().BeGreaterThan(0);
        putEvent.OriginHostId.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task MultipartCompleteAndCopy_PutEvents()
    {
        // Arrange — multipart из одной малой части (единственная = последняя)
        var client = Client;
        await client.SendSignedAsync("PUT", "/ev-mp", credentials: OwnS3TestClient.Admin());
        var initiate = await client.SendSignedAsync("POST", "/ev-mp/obj?uploads",
            payloadString: PayloadHash(string.Empty), credentials: OwnS3TestClient.Writer());
        initiate.StatusCode.Should().Be(System.Net.HttpStatusCode.OK);
        var initiateBody = await initiate.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        initiate.StatusCode.Should().Be(System.Net.HttpStatusCode.OK, initiateBody);
        var uploadId = System.Text.RegularExpressions.Regex.Match(initiateBody,
            "<UploadId>([^<]+)</UploadId>").Groups[1].Value;
        const string part = "mp-part-body";
        var partEtag = await client.SendSignedAsync("PUT", $"/ev-mp/obj?partNumber=1&uploadId={uploadId}",
            payloadString: PayloadHash(part), body: Encoding.UTF8.GetBytes(part));
        partEtag.StatusCode.Should().Be(System.Net.HttpStatusCode.OK);
        // ETag части — с кавычками (формат PutResult), манифест Complete — нормализация сверки
        var etag = partEtag.Headers.ETag?.ToString() ?? string.Empty;
        var complete = await client.SendSignedAsync("POST", $"/ev-mp/obj?uploadId={uploadId}",
            payloadString: PayloadHash(string.Empty),
            body: Encoding.UTF8.GetBytes(
                $"<?xml version=\"1.0\" encoding=\"utf-8\"?><CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"));
        var completeBody = await complete.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        complete.StatusCode.Should().Be(System.Net.HttpStatusCode.OK, completeBody);

        // Act — CopyObject (нужен CreateBucket admin + источник)
        await client.SendSignedAsync("PUT", "/ev-copy", credentials: OwnS3TestClient.Admin());
        await client.SendSignedAsync("PUT", "/ev-copy/src", payloadString: PayloadHash("copy"), body: "copy"u8.ToArray());
        var copy = await client.SendSignedAsync("PUT", "/ev-copy/dst",
            headers: new Dictionary<string, string> { ["x-amz-copy-source"] = "/ev-copy/src" },
            credentials: OwnS3TestClient.Writer());
        var copyBody = await copy.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        copy.StatusCode.Should().Be(System.Net.HttpStatusCode.OK, copyBody);

        // Assert — multipart-complete и copy кладут object-put (разные ключи)
        var events = ReadOutbox(OutboxDir);
        events.Where(e => e.Type == ReplicationEventType.ObjectPut).Select(e => (e.Bucket, e.Key))
            .Should().Contain(("ev-mp", "obj"));
        events.Where(e => e.Type == ReplicationEventType.ObjectPut).Select(e => (e.Bucket, e.Key))
            .Should().Contain(("ev-copy", "dst"));
    }

    [Fact]
    public async Task DeletesAndBuckets_EventsPerKeyAndBucket()
    {
        // Arrange — бакет + два ключа; DeleteObjects пачкой
        var client = Client;
        await client.SendSignedAsync("PUT", "/ev-del", credentials: OwnS3TestClient.Admin());
        foreach (var key in new[] { "k1", "k2" })
            await client.SendSignedAsync("PUT", $"/ev-del/{key}", payloadString: PayloadHash("v"),
                body: "v"u8.ToArray());
        const string manifest = "<Delete><Object><Key>k1</Key></Object><Object><Key>k2</Key></Object></Delete>";
        var multi = await client.SendSignedAsync("POST", "/ev-del?delete=",
            payloadString: PayloadHash(manifest), body: Encoding.UTF8.GetBytes(manifest));
        multi.StatusCode.Should().Be(System.Net.HttpStatusCode.OK);

        // Act — CreateBucket и DeleteBucket
        await client.SendSignedAsync("PUT", "/ev-bucket", credentials: OwnS3TestClient.Admin());
        await client.SendSignedAsync("DELETE", "/ev-bucket", credentials: OwnS3TestClient.Admin());

        // Assert — DeleteObjects: по событию на ключ; бакеты: create + delete
        var events = ReadOutbox(OutboxDir);
        events.Count(e => e.Type == ReplicationEventType.ObjectDelete && e.Bucket == "ev-del").Should().Be(2);
        events.Should().Contain(e => e.Type == ReplicationEventType.BucketCreate && e.Bucket == "ev-bucket");
        events.Should().Contain(e => e.Type == ReplicationEventType.BucketDelete && e.Bucket == "ev-bucket");
    }

    [Fact]
    public async Task ReplicaApply_DoesNotAppendEvents_AntiLoop()
    {
        // Arrange — активная репликация (журнал уже содержит клиентские события)
        var client = Client;
        await client.SendSignedAsync("PUT", "/ev-loop", credentials: OwnS3TestClient.Admin());
        await client.SendSignedAsync("PUT", "/ev-loop/local", payloadString: PayloadHash("v"), body: "v"u8.ToArray());
        var before = ReadOutbox(OutboxDir).Count;
        before.Should().BeGreaterThan(0);

        // Act — приём реплики: самодостаточные события от соседа
        const long modMs = 1000;
        var batch = JsonSerializer.Serialize(new NotificationBatchRequest("mesh-a",
        [
            new ReplicationEvent(900, ReplicationEventType.BucketCreate, "ev-loop2", "", modMs, "", 0, "", "peer"),
            new ReplicationEvent(901, ReplicationEventType.ObjectDelete, "ev-loop2", "gone", modMs, "", 0, "", "peer"),
        ]), ReplicationTransportJson.Options);
        var response = await Client.SendSignedAsync("POST", "/replication/notifications",
            payloadString: PayloadHash(batch), body: Encoding.UTF8.GetBytes(batch),
            credentials: ReplicationCreds);
        response.StatusCode.Should().Be(System.Net.HttpStatusCode.OK);

        // Assert — приём реплики события не ставит (анти-петля, принцип 5): событий не прибавилось
        ReadOutbox(OutboxDir).Count.Should().Be(before);
    }
}
