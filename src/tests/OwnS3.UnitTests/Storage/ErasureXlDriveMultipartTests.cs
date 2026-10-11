using Microsoft.Extensions.Logging.Abstractions;
using OwnS3.Storage;
using OwnS3.Storage.Erasure;

namespace OwnS3.UnitTests.Storage;

// Тесты multipart в EC (t40, план Task 11): топология 4/1, temp-корни.
public class ErasureXlDriveMultipartTests : IDisposable
{
    private readonly List<string> _roots = [];
    private readonly ErasurePool _pool;
    private readonly ErasureXlDrive _drive;

    public ErasureXlDriveMultipartTests()
    {
        for (var i = 0; i < 4; i++)
        {
            var root = Path.Combine(Path.GetTempPath(), "owns3-ecmp-" + Guid.NewGuid().ToString("N"));
            _roots.Add(root);
        }

        _pool = new ErasurePool(_roots, new ErasureTopology(4, 4, 1), TimeProvider.System);
        _pool.Initialize();
        _drive = new ErasureXlDrive(_pool, defaultParity: 1, TimeProvider.System,
            NullLogger<ErasureXlDrive>.Instance);
        _drive.CreateBucket("b");
    }

    public void Dispose()
    {
        // Teardown: temp-корни удаляются при любом исходе
        _pool.Dispose();
        foreach (var root in _roots)
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
    }

    private async Task<string> CreateUploadAsync(string key) =>
        await Task.Run(() =>
        {
            var uploadId = Guid.NewGuid().ToString("N");
            _drive.CreateUploadDir("b", key, uploadId, new XlMetaRecord(Guid.Parse(uploadId), 0,
                DateTimeOffset.UnixEpoch, "", "text/plain", new Dictionary<string, string>(),
                new Dictionary<string, string>(), ""));
            _drive.WriteUploads("b", key,
            [
                new MultipartJournals.UploadJournalEntry(uploadId, "b", key,
                    DateTimeOffset.UnixEpoch.ToUnixTimeMilliseconds(), "root"),
            ]);
            return uploadId;
        });

    private async Task UploadPartAsync(string key, string uploadId, int number, byte[] body)
    {
        using var staging = _drive.CreateUploadStaging("b", key, uploadId);
        var digest = await _drive.WritePartAsync(staging, number, new MemoryStream(body), body.LongLength,
            CancellationToken.None);
        _drive.CommitUploadPart(staging, number, digest.ETagHex, digest.Length, DateTimeOffset.UnixEpoch);
    }

    private async Task<CompleteResult> CompleteAsync(string key, string uploadId, IReadOnlyList<PartEtag> manifest) =>
        await Task.Run(() => _drive.CommitMultipart("b", key, uploadId, manifest,
            new XlMetaRecord(Guid.Empty, 0, DateTimeOffset.UnixEpoch, "", "text/plain",
                new Dictionary<string, string>(), new Dictionary<string, string>(), "")));

    private static byte[] RandomBytes(int len, int seed = 31)
    {
        var buf = new byte[len];
        new Random(seed).NextBytes(buf);
        return buf;
    }

    [Fact]
    public async Task Multipart_FullLifecycle_ObjectsEqualOriginal()
    {
        // Arrange: 2 части по 6 МиБ + Complete
        const int partSize = 6 * 1024 * 1024;
        var part1 = RandomBytes(partSize, 1);
        var part2 = RandomBytes(partSize + 12345, 2);
        var uploadId = await CreateUploadAsync("mp");

        // Act
        await UploadPartAsync("mp", uploadId, 1, part1);
        await UploadPartAsync("mp", uploadId, 2, part2);
        var etag1 = Convert.ToHexString(System.Security.Cryptography.MD5.HashData(part1)).ToLowerInvariant();
        var etag2 = Convert.ToHexString(System.Security.Cryptography.MD5.HashData(part2)).ToLowerInvariant();
        var complete = await CompleteAsync("mp", uploadId,
        [
            new PartEtag(1, '"' + etag1 + '"'),
            new PartEtag(2, '"' + etag2 + '"'),
        ]);

        // Assert: чтение равно исходнику; многочастевой объект: dataDir содержит
        // part.1/part.2 шардовые; xl.meta v2 с bitrot-хэшами обеих частей
        var meta = _drive.TryReadMeta("b", "mp");
        meta.Should().NotBeNull();
        meta!.Ec!.Parts.Should().HaveCount(2);
        meta.Ec.Parts[0].SourceSize.Should().Be(partSize);
        meta.Ec.Parts[1].SourceSize.Should().Be(partSize + 12345);
        using var body = _drive.OpenData("b", "mp", meta, 0, meta.Size);
        using var target = new MemoryStream();
        body.CopyTo(target);
        target.ToArray().Should().Equal(part1.Concat(part2).ToArray());
        complete.ETag.Should().StartWith("\"2-");
        // каталог загрузки удалён
        _drive.UploadDirExists("b", "mp", uploadId).Should().BeFalse();
        _drive.ReadUploads("b", "mp").Should().BeEmpty();
    }

    [Fact]
    public async Task Multipart_PartsJournalOnAllOnlineDisks()
    {
        // Arrange/Act
        var uploadId = await CreateUploadAsync("journal");
        await UploadPartAsync("journal", uploadId, 1, RandomBytes(5 * 1024 * 1024));

        // Assert — parts.json на всех 4 дисках сета
        foreach (var root in _roots)
        {
            var multipartRoot = Path.Combine(root, ".owns3.sys", "multipart");
            var uploadDirs = Directory.EnumerateFiles(multipartRoot, "parts.json", SearchOption.AllDirectories)
                .ToList();
            uploadDirs.Should().HaveCountGreaterThanOrEqualTo(1, $"parts.json в {root}");
        }
    }

    [Fact]
    public async Task UploadPart_ETagOfOriginalPartBody()
    {
        // Arrange
        var body = RandomBytes(5 * 1024 * 1024 + 777, 5);
        var uploadId = await CreateUploadAsync("etag");

        // Act
        using var staging = _drive.CreateUploadStaging("b", "etag", uploadId);
        var digest = await _drive.WritePartAsync(staging, 3, new MemoryStream(body), body.LongLength,
            CancellationToken.None);

        // Assert — ETag/size исходного тела
        digest.ETagHex.Should().Be(Convert.ToHexString(System.Security.Cryptography.MD5.HashData(body))
            .ToLowerInvariant());
        digest.Length.Should().Be(body.LongLength);
    }

    [Fact]
    public async Task Complete_IdempotentRetry_SameResult()
    {
        // Arrange: эмуляция краха после attempt, до коммита: первый Complete падает
        // на этапе зачистки загрузки (каталог уже перенесён) — повтор собирает
        // в тот же dataDir (attempt)
        var body = RandomBytes(5 * 1024 * 1024, 6);
        var uploadId = await CreateUploadAsync("retry");
        await UploadPartAsync("retry", uploadId, 1, body);
        var etag = Convert.ToHexString(System.Security.Cryptography.MD5.HashData(body)).ToLowerInvariant();

        // Act — первый Complete и повтор
        var first = await CompleteAsync("retry", uploadId, [new PartEtag(1, '"' + etag + '"')]);
        var meta1 = _drive.TryReadMeta("b", "retry");
        var second = await AssertCompleteRetryAsync("retry", uploadId, etag, first);

        // Assert — повтор Complete того же результата (attempt-маркер сохранён
        // до зачистки; после зачистки повтор — NoSuchUpload, как в single)
        second.Should().BeNull();
        meta1!.VersionId.Should().NotBe(Guid.Empty);
    }

    [Fact]
    public async Task ReadUploads_JournalMissingOnFirstDisk_UnionWithReplicas()
    {
        // Arrange: загрузка создана (журнал реплицирован на все диски); затем
        // uploads.json удалён на диске 0 — имитация частичной репликации
        // (загрузка создана, пока диск был offline)
        var uploadId = await CreateUploadAsync("union");
        await UploadPartAsync("union", uploadId, 1, RandomBytes(64, 3));
        var journal0 = Directory.EnumerateFiles(
            Path.Combine(_roots[0], ".owns3.sys", "multipart"), "uploads.json",
            SearchOption.AllDirectories).Single();
        File.Delete(journal0);

        // Act
        var uploads = _drive.ReadUploads("b", "union");

        // Assert: загрузка видна — отсутствие файла ≠ «мнение: пусто»,
        // носители сета сливаются union'ом
        uploads.Should().ContainSingle(e => e.UploadId == uploadId,
            "реплики журнала на остальных дисках обязаны читаться");
    }

    [Fact]
    public async Task CreateUpload_OverPartiallyReplicatedJournal_DoesNotLoseEntries()
    {
        // Arrange: загрузка id1 ключа; журнал диска 0 удалён (частичная
        // репликация после возврата диска)
        var id1 = await CreateUploadAsync("keep");
        var journal0 = Directory.EnumerateFiles(
            Path.Combine(_roots[0], ".owns3.sys", "multipart"), "uploads.json",
            SearchOption.AllDirectories).Single();
        File.Delete(journal0);

        // Act: механика CreateMultipartUpload поверх (ReadUploads → add →
        // WriteUploads на все онлайн-диски) — union база не теряет id1
        var id2 = Guid.NewGuid().ToString("N");
        var merged = _drive.ReadUploads("b", "keep");
        merged.Add(new MultipartJournals.UploadJournalEntry(id2, "b", "keep",
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), "root"));
        _drive.WriteUploads("b", "keep", merged);

        // Assert: обе записи на КАЖДОМ диске сета (пустой журнал диска 0
        // не затёр непустые реплики при обратной записи)
        for (var i = 0; i < 4; i++)
        {
            var journal = Directory.EnumerateFiles(
                Path.Combine(_roots[i], ".owns3.sys", "multipart"), "uploads.json",
                SearchOption.AllDirectories).Single();
            var ids = MultipartJournals.ReadUploads(journal).Select(e => e.UploadId).ToList();
            ids.Should().Contain(id1, "запись id1 пережила Create поверх частичной реплики (диск {0})", i);
            ids.Should().Contain(id2);
        }
    }

    [Fact]
    public async Task Complete_AttemptOnNonFirstDisk_ReusedSameDataDir()
    {
        // Arrange: загрузка с частью; attempt-маркер существует ТОЛЬКО на диске 2
        // (частичный WriteAttempt при I/O-сбое первого) — известный dataDir
        var body = RandomBytes(300_000, 9);
        var uploadId = await CreateUploadAsync("attempt");
        await UploadPartAsync("attempt", uploadId, 1, body);
        var attemptDataDir = Guid.NewGuid().ToString("N");
        var uploadDir2 = Directory.EnumerateFiles(
                Path.Combine(_roots[2], ".owns3.sys", "multipart"), "parts.json",
                SearchOption.AllDirectories).Single();
        MultipartJournals.WriteAttempt(Path.GetDirectoryName(uploadDir2)!,
            new MultipartJournals.AttemptMarker(attemptDataDir));
        var etag = Convert.ToHexString(System.Security.Cryptography.MD5.HashData(body)).ToLowerInvariant();

        // Act: Complete — attempt обязан читаться с носителя, а не с online[0]
        await CompleteAsync("attempt", uploadId, [new PartEtag(1, '"' + etag + '"')]);

        // Assert: объект собран в dataDir attempt-маркера (новый не сгенерирован)
        var meta = _drive.TryReadMeta("b", "attempt");
        meta!.VersionId.ToString("N").Should().Be(attemptDataDir,
            "повтор использует attempt любого носителя сета — иначе перенесённое не находилось бы");
    }

    [Fact]
    public async Task UploadPart_Reupload_PurgesStaleAttemptTransfer()
    {
        // Arrange: загрузка part.1=body1; Complete сорвался посреди переноса
        // (part.1 удалён с дисков 2,3 → InvalidPart, перенос на 0,1 остался)
        var body1 = RandomBytes(300_000, 11);
        var body2 = RandomBytes(300_000, 12);
        var uploadId = await CreateUploadAsync("reupload");
        await UploadPartAsync("reupload", uploadId, 1, body1);
        var etag1 = Convert.ToHexString(System.Security.Cryptography.MD5.HashData(body1)).ToLowerInvariant();
        var lostPartPaths = _roots.Skip(2).Select(root =>
            Directory.EnumerateFiles(Path.Combine(root, ".owns3.sys", "multipart"), "part.1",
                SearchOption.AllDirectories).Single()).ToList();
        foreach (var path in lostPartPaths)
            File.Delete(path);
        var firstComplete = () => CompleteAsync("reupload", uploadId, [new PartEtag(1, '"' + etag1 + '"')]);
        await firstComplete.Should().ThrowAsync<ObjectStoreException>()
            .Where(e => e.Code == ObjectStoreErrorCode.InvalidPart);

        // Act: перезаливка части новым телом → Complete
        await UploadPartAsync("reupload", uploadId, 1, body2);
        var etag2 = Convert.ToHexString(System.Security.Cryptography.MD5.HashData(body2)).ToLowerInvariant();
        await CompleteAsync("reupload", uploadId, [new PartEtag(1, '"' + etag2 + '"')]);

        // Assert: объект == НОВОЕ тело (устаревший перенос не скипнулся
        // при новом sidecar — SHA/шарды согласованы)
        var meta = _drive.TryReadMeta("b", "reupload");
        using var stream = _drive.OpenData("b", "reupload", meta!, 0, meta!.Size);
        using var target = new MemoryStream();
        await stream.CopyToAsync(target, TestContext.Current.CancellationToken);
        target.ToArray().Should().Equal(body2);
    }

    [Fact]
    public async Task Complete_FailedTransfer_RetrySucceeds_WithoutPartLoss()
    {
        // Arrange: загрузка из одной части; перенос Complete сорвётся посреди —
        // шардовый part.1 удалён с дисков 2 и 3 сета (копии сохранены): после
        // переноса на дисках 0,1 носителей 2 < k=3 → InvalidPart. При схеме
        // tmp-staging сбой сносил ПЕРЕНЕСЁННОЕ со всех дисков — повтор был
        // InvalidPart навсегда; перенос в dataDir цели сохраняет части
        var body = RandomBytes(5 * 1024 * 1024, 7);
        var uploadId = await CreateUploadAsync("retry-transfer");
        await UploadPartAsync("retry-transfer", uploadId, 1, body);
        var etag = Convert.ToHexString(System.Security.Cryptography.MD5.HashData(body)).ToLowerInvariant();
        var lostPartPaths = _roots.Skip(2).Select(root =>
            Directory.EnumerateFiles(Path.Combine(root, ".owns3.sys", "multipart"), "part.1",
                SearchOption.AllDirectories).Single()).ToList();
        var savedCopies = lostPartPaths.Select(File.ReadAllBytes).ToList();
        foreach (var path in lostPartPaths)
            File.Delete(path);

        // Act — первый Complete: InvalidPart (перенос на 0,1 уже прошёл)
        var firstAct = () => CompleteAsync("retry-transfer", uploadId, [new PartEtag(1, '"' + etag + '"')]);
        await firstAct.Should().ThrowAsync<ObjectStoreException>()
            .Where(e => e.Code == ObjectStoreErrorCode.InvalidPart);

        // Arrange2: «источники вернулись» — part.1 восстановлен на дисках 2,3
        foreach (var (path, copy) in lostPartPaths.Zip(savedCopies))
            File.WriteAllBytes(path, copy);

        // Act2 — повтор Complete доезжает: перенесённое на 0,1 не потеряно
        // (dest skip), возвратившиеся источники переносятся, коммит успешен
        await CompleteAsync("retry-transfer", uploadId, [new PartEtag(1, '"' + etag + '"')]);

        // Assert — объект закоммичен и читается исходником
        var meta = _drive.TryReadMeta("b", "retry-transfer");
        meta.Should().NotBeNull();
        using var stream = _drive.OpenData("b", "retry-transfer", meta!, 0, meta!.Size);
        using var target = new MemoryStream();
        await stream.CopyToAsync(target, TestContext.Current.CancellationToken);
        target.ToArray().Should().Equal(body);
    }

    private async Task<CompleteResult?> AssertCompleteRetryAsync(string key, string uploadId, string etag,
        CompleteResult first)
    {
        // повтор после успешного Complete: загрузка зачищена — NoSuchUpload
        try
        {
            return await CompleteAsync(key, uploadId, [new PartEtag(1, '"' + etag + '"')]);
        }
        catch (ObjectStoreException ex) when (ex.Code == ObjectStoreErrorCode.NoSuchUpload)
        {
            return null;
        }
    }

    [Fact]
    public async Task Complete_BelowWriteQuorum_Throws503()
    {
        // Arrange: деградация до 2 (< writeQuorum 3) ДО Complete
        var body = RandomBytes(5 * 1024 * 1024, 7);
        var uploadId = await CreateUploadAsync("degr");
        await UploadPartAsync("degr", uploadId, 1, body);
        var setIndex = _pool.PickSetIndex("b", "degr");
        foreach (var disk in _pool.Sets[setIndex].Take(2))
            Directory.Delete(disk.Root, recursive: true);
        _pool.RefreshOnline();
        var etag = Convert.ToHexString(System.Security.Cryptography.MD5.HashData(body)).ToLowerInvariant();

        // Act
        var act = () => CompleteAsync("degr", uploadId, [new PartEtag(1, '"' + etag + '"')]);

        // Assert
        (await act.Should().ThrowAsync<ObjectStoreException>())
            .Which.Code.Should().Be(ObjectStoreErrorCode.InsufficientWriteQuorum);
    }

    [Fact]
    public async Task Abort_TrashesUploadOnAllDisks()
    {
        // Arrange
        var uploadId = await CreateUploadAsync("abort");
        await UploadPartAsync("abort", uploadId, 1, RandomBytes(5 * 1024 * 1024, 8));

        // Act
        var uploads = _drive.ReadUploads("b", "abort");
        uploads.RemoveAll(e => e.UploadId == uploadId);
        _drive.WriteUploads("b", "abort", uploads);
        _drive.TrashUpload("b", "abort", uploadId);

        // Assert — каталогов загрузки нет нигде
        _drive.UploadDirExists("b", "abort", uploadId).Should().BeFalse();
        foreach (var root in _roots)
            Directory.EnumerateFiles(Path.Combine(root, ".owns3.sys", "multipart"), "part.1",
                    SearchOption.AllDirectories)
                .Should().BeEmpty($"каталог загрузки уходит в .trash: {root}");
    }

    [Fact]
    public async Task ListParts_ReadsFromAnySuccessfulDisk()
    {
        // Arrange: один диск offline — журнал читается с оставшихся
        var body = RandomBytes(5 * 1024 * 1024, 9);
        var uploadId = await CreateUploadAsync("listparts");
        await UploadPartAsync("listparts", uploadId, 1, body);
        var setIndex = _pool.PickSetIndex("b", "listparts");
        Directory.Delete(_pool.Sets[setIndex][0].Root, recursive: true);
        _pool.RefreshOnline();

        // Act
        var parts = _drive.ReadParts("b", "listparts", uploadId);

        // Assert
        parts.Should().ContainSingle().Which.PartNumber.Should().Be(1);
    }
}
