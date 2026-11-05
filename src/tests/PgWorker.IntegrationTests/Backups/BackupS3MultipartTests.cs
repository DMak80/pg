using FluentAssertions;
using PgWorker.Backups;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Multipart-обёртка BackupS3 против живого ownS3 (#22): Content-MD5 частей,
// контигуальность partNumber при Complete, Abort (сессия уходит из листинга),
// ListMultipartUploads по префиксу, Head/Range/Stream/DeleteObject границы.
// У КАЖДОГО Fact своё окружение OwnS3 (guid-имя, динамический порт, свой
// bucket, own-only teardown с ассертом чистоты — arch/testing/02-e2e-isolation.md §1/§3).
[Collection(NonE2eCollection.Name)]
public class BackupS3MultipartTests
{
    // Минимальный размер НЕпоследней части (лимит S3/OwnS3 — 5 МиБ).
    private const int MinPartBytes = 5 * 1024 * 1024;

    private static byte[] Bytes(int size, byte seed)
    {
        var data = new byte[size];
        for (var i = 0; i < size; i++)
            data[i] = (byte)(seed + i % 251);
        return data;
    }

    // AAA: части 1..2 + Complete → объект появляется атомарно; Head даёт полный
    // размер, Range — точный срез, Stream — те же байты; одиночное удаление
    // идемпотентно (повторный delete — успех, HEAD — «нет объекта»).
    [Fact]
    public async Task MultipartParts_CompleteAndReadBoundaries_DeleteIdempotent()
    {
        // Arrange — своё окружение + сессия одного объекта.
        var ct = TestContext.Current.CancellationToken;
        await using var s3Node = await OwnS3.StartAsync("s3mp", ct);
        await using var s3 = new BackupS3(s3Node.S3());
        const string key = "c1/s1/full/20261105120000Z.tar";
        var first = Bytes(MinPartBytes, 1);
        var tail = Bytes(1024, 7);

        // Act 1 — заливка частей и Complete по фактическим ETag.
        var created = await s3.CreateMultipartUploadAsync(key, ct);
        created.IsSuccess.Should().BeTrue(created.Error?.Message);
        var session = created.Value;
        var part1 = await session.UploadPartAsync(1, first, ct);
        part1.IsSuccess.Should().BeTrue(part1.Error?.Message);
        part1.Value.Should().NotBeNullOrEmpty("UploadPart returns the part ETag");
        var part2 = await session.UploadPartAsync(2, tail, ct);
        part2.IsSuccess.Should().BeTrue(part2.Error?.Message);
        var completed = await session.CompleteAsync(
            new Dictionary<int, string> { [1] = part1.Value, [2] = part2.Value }, ct);
        completed.IsSuccess.Should().BeTrue(completed.Error?.Message);
        completed.Value.Should().NotBeNullOrEmpty("Complete returns the object ETag");

        // Assert 1 — объект есть, размер = сумма частей.
        var total = first.Length + tail.Length;
        var head = await s3.HeadObjectAsync("c1", "s1", "full/20261105120000Z.tar", ct);
        head.IsSuccess.Should().BeTrue();
        head.Value.Exists.Should().BeTrue();
        head.Value.SizeBytes.Should().Be(total);

        // Assert 2 — Range отдаёт точный срез, Stream — те же байты.
        var range = await s3.GetObjectRangeAsync("c1", "s1", "full/20261105120000Z.tar", 0, 9, ct);
        range.IsSuccess.Should().BeTrue();
        range.Value.Should().Equal(first[..10]);
        var stream = await s3.GetObjectStreamAsync("c1", "s1", "full/20261105120000Z.tar", ct);
        stream.IsSuccess.Should().BeTrue();
        await using var body = stream.Value.Body;
        using var buffer = new MemoryStream();
        await body.CopyToAsync(buffer, ct);
        buffer.ToArray().Should().Equal([.. first, .. tail]);

        // Assert 3 — удаление идемпотентно.
        (await s3.DeleteObjectAsync("c1", "s1", "full/20261105120000Z.tar", ct))
            .IsSuccess.Should().BeTrue();
        (await s3.HeadObjectAsync("c1", "s1", "full/20261105120000Z.tar", ct))
            .Value.Exists.Should().BeFalse();
        (await s3.DeleteObjectAsync("c1", "s1", "full/20261105120000Z.tar", ct))
            .IsSuccess.Should().BeTrue("single-object delete is idempotent");
    }

    // AAA: неконтигуальные partNumber (1,3) → Complete отклоняется локальной
    // валидацией (без обращения к серверу); объект не создаётся.
    [Fact]
    public async Task Complete_NonContiguousParts_Failed()
    {
        // Arrange — сессия без заливки.
        var ct = TestContext.Current.CancellationToken;
        await using var s3Node = await OwnS3.StartAsync("s3mp", ct);
        await using var s3 = new BackupS3(s3Node.S3());
        const string key = "c2/s1/full/20261105120000Z.tar";
        var created = await s3.CreateMultipartUploadAsync(key, ct);
        created.IsSuccess.Should().BeTrue(created.Error?.Message);

        // Act — partNumber 1 и 3.
        var completed = await created.Value.CompleteAsync(
            new Dictionary<int, string> { [1] = "etag1", [3] = "etag3" }, ct);

        // Assert — отказ; объекта нет.
        completed.IsSuccess.Should().BeFalse();
        completed.Error!.Message.Should().Contain("not contiguous");
        (await s3.HeadObjectAsync("c2", "s1", "full/20261105120000Z.tar", ct))
            .Value.Exists.Should().BeFalse();
    }

    // AAA: Abort убирает сессию из ListMultipartUploads; листинг фильтрует
    // по префиксу шарда (сессия другого шарда не попадает).
    [Fact]
    public async Task Abort_RemovesSession_ListFiltersByPrefix()
    {
        // Arrange — две сессии в разных префиксах.
        var ct = TestContext.Current.CancellationToken;
        await using var s3Node = await OwnS3.StartAsync("s3mp", ct);
        await using var s3 = new BackupS3(s3Node.S3());
        const string key = "c3/shard1/full/20261105120000Z.tar";
        var own = await s3.CreateMultipartUploadAsync(key, ct);
        own.IsSuccess.Should().BeTrue(own.Error?.Message);
        var foreign = await s3.CreateMultipartUploadAsync(
            "c3/shard2/full/20261105120000Z.tar", ct);
        foreign.IsSuccess.Should().BeTrue(foreign.Error?.Message);

        // Act 1 — листинг по префиксу shard1: только своя сессия.
        var listed = await s3.ListMultipartUploadsAsync("c3/shard1/", ct);
        listed.IsSuccess.Should().BeTrue(listed.Error?.Message);
        listed.Value.Select(u => u.UploadId).Should().Contain(own.Value.UploadId);
        listed.Value.Select(u => u.UploadId).Should().NotContain(foreign.Value.UploadId);

        // Act 2 — Abort своей сессии.
        (await own.Value.AbortAsync(ct)).IsSuccess.Should().BeTrue();

        // Assert — сессии больше нет в листинге.
        var after = await s3.ListMultipartUploadsAsync("c3/shard1/", ct);
        after.IsSuccess.Should().BeTrue();
        after.Value.Select(u => u.UploadId).Should().NotContain(own.Value.UploadId);
    }
}
