using PgWorker.Backups;
using Shared.Core;

namespace PgWorker.UnitTests.Backups;

// PoolBackupS3 (arch/19 §5): перебор write/read-целей на фейках
// FakeTargetS3 с управляемым отказом (инъекция targetFactory).
public class PoolBackupS3Tests
{
    // Фейк цели: сценарий управляется делегатами/флагом отказа.
    private sealed class FakeTargetS3(
        Func<Task<Result<bool>>>? bucketExists = null,
        Func<string, Task<Result>>? put = null,
        Func<IReadOnlyList<string>, Task<Result>>? delete = null,
        Func<string, Task<Result<string>>>? get = null,
        Func<string, Task<Result<IReadOnlyList<WalObject>>>>? list = null,
        Func<string, Task<Result<IReadOnlyList<S3ObjectInfo>>>>? listPrefix = null,
        Func<string, Task<Result<(byte[] Data, string? ETag)>>>? getBytes = null) : IBackupS3
    {
        public int Calls;

        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct)
            => (bucketExists ?? (() => Task.FromResult(Result<bool>.Success(true))))();

        public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
            => list?.Invoke("wal/") ?? Task.FromResult(Result<IReadOnlyList<WalObject>>.Success([]));

        public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(
            string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
            => listPrefix?.Invoke(prefix) ?? Task.FromResult(Result<IReadOnlyList<S3ObjectInfo>>.Success([]));

        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default)
            => (delete ?? (_ => Task.FromResult(Result.Success())))(keys);

        public async Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default)
        {
            Calls++;
            return put is null ? Result.Success() : await put(key);
        }

        public Task<Result<IReadOnlyList<WalObject>>> ListAsync(
            string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
            => list?.Invoke(prefix) ?? Task.FromResult(Result<IReadOnlyList<WalObject>>.Success([]));

        public Task<Result<string>> GetObjectAsync(
            string cluster, string shard, string key, CancellationToken ct = default)
        {
            Calls++;
            return (get ?? ((_ => Task.FromResult(Result<string>.Failed(
                new ApplicationException("S3 get: not found"))))))(key);
        }

        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
            => Task.FromResult(Result<IReadOnlyList<string>>.Success(["id1"]));

        // файловые методы интерфейса в этом тесте не используются —
        // заглушка (фейк нужен только перечисленным выше операциям).
        public Task<Result> PutFileAsync(string key, string filePath, CancellationToken ct = default)
            => Task.FromResult(Result.Failed(new ApplicationException("put-file: not supported by this fake")));

        // t15: методы кусков в этом тесте не используются — заглушки.
        public Task<Result> PutObjectMd5Async(string key, byte[] data, string md5Hex, CancellationToken ct = default)
            => Task.FromResult(Result.Failed(new ApplicationException("put-md5: not supported by this fake")));

        public Task<Result<(byte[] Data, string? ETag)>> GetObjectBytesAsync(
            string cluster, string shard, string key, CancellationToken ct = default)
        {
            Calls++;
            return (getBytes ?? (_ => Task.FromResult(
                Result<(byte[] Data, string? ETag)>.Failed(
                    new ApplicationException("S3 get: not found")))))(key);
        }

        public Task<Result> GetObjectToFileAsync(string cluster, string shard, string key, string destFilePath, CancellationToken ct = default)
            => Task.FromResult(Result.Failed(new ApplicationException("get-to-file: not supported by this fake")));
        public Task<Result<string>> DownloadTextAsync(
            string cluster, string shard, string objectKey, CancellationToken ct = default)
            => get?.Invoke(objectKey) ?? Task.FromResult(Result<string>.Success("text"));

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private static PoolBackupS3 Pool(FakeTargetS3 t1, FakeTargetS3 t2, FakeTargetS3? t3 = null)
    {
        var map = new Dictionary<string, IBackupS3>
        {
            ["http://n1:9000"] = t1,
            ["http://n2:9000"] = t2,
        };
        if (t3 is not null)
            map["http://n3:9000"] = t3;
        return new PoolBackupS3(
            ["http://n1:9000", "http://n2:9000"], ["http://n1:9000", "http://n2:9000"],
            "ak", "sk", "b", null,
            targetFactory: uri => map[uri]);
    }

    // Управляемое время breaker-окна (arch/19 §5): сдвиг часов имитирует
    // истечение окна исключения отказавшей цели.
    private sealed class FakeClock : TimeProvider
    {
        public DateTimeOffset Now = DateTimeOffset.UnixEpoch;

        public override DateTimeOffset GetUtcNow() => Now;
    }

    private static PoolBackupS3 Pool(FakeTargetS3 t1, FakeTargetS3 t2, FakeClock clock)
    {
        var map = new Dictionary<string, IBackupS3>
        {
            ["http://n1:9000"] = t1,
            ["http://n2:9000"] = t2,
        };
        return new PoolBackupS3(
            ["http://n1:9000", "http://n2:9000"], ["http://n1:9000", "http://n2:9000"],
            "ak", "sk", "b", null,
            clock: clock,
            targetFactory: uri => map[uri]);
    }

    // AAA: put — target1 всегда Failed, target2 успех → успех; порядок n1 → n2.
    [Fact]
    public async Task Put_FailsOverToSecondTarget()
    {
        // Arrange — первая цель отказывает, вторая жива.
        var t1 = new FakeTargetS3(put: _ => Task.FromResult(Result.Failed(
            new ApplicationException("S3 put: conn refused"))));
        var t2 = new FakeTargetS3();
        var pool = Pool(t1, t2);

        // Act
        var result = await pool.PutObjectAsync("key1", [1], null, CancellationToken.None);

        // Assert — попытка шла на обе цели по порядку.
        result.IsSuccess.Should().BeTrue();
        t1.Calls.Should().Be(1);
        t2.Calls.Should().Be(1);
    }

    // AAA: обе цели отказали → Failed, Error содержит маркер s3-pool-exhausted.
    [Fact]
    public async Task Put_AllTargetsFail_ExhaustedMarker()
    {
        // Arrange
        Result Failed() => Result.Failed(new ApplicationException("S3 put: down"));
        var t1 = new FakeTargetS3(put: _ => Task.FromResult(Failed()));
        var t2 = new FakeTargetS3(put: _ => Task.FromResult(Failed()));
        var pool = Pool(t1, t2);

        // Act
        var result = await pool.PutObjectAsync("key1", [1], null, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain(S3PoolMarkers.Exhausted);
    }

    // AAA: точечное чтение (GetObjectBytesAsync — путь fetch-wal) — NotFound
    // на первой цели, значение со второй.
    [Fact]
    public async Task GetBytes_NotFoundOnFirst_FoundOnSecond()
    {
        // Arrange — первая отдаёт «not found», вторая содержимое.
        var t1 = new FakeTargetS3(getBytes: _ => Task.FromResult(
            Result<(byte[] Data, string? ETag)>.Failed(
                new ApplicationException("S3 get k: not found"))));
        var t2 = new FakeTargetS3(getBytes: _ => Task.FromResult(
            Result<(byte[] Data, string? ETag)>.Success(([1, 2, 3], "etag"))));
        var pool = Pool(t1, t2);

        // Act
        var result = await pool.GetObjectBytesAsync(
            "c", "s", "wal/00000001.history", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Data.Should().Equal(1, 2, 3);
        result.Value.ETag.Should().Be("etag");
    }

    // AAA: листинг — первая живая цель выигрывает, третья не трогалась.
    [Fact]
    public async Task List_FirstAliveWins()
    {
        // Arrange — n1 отказ, n2 отдаёт сегмент, n3 жив (не должен вызываться).
        var t1 = new FakeTargetS3(list: _ => Task.FromResult(Result<IReadOnlyList<WalObject>>.Failed(
            new ApplicationException("S3 list: down"))));
        var expected = new List<WalObject> { new("000000010000000000000001", DateTimeOffset.UnixEpoch) };
        var t2 = new FakeTargetS3(list: _ => Task.FromResult(Result<IReadOnlyList<WalObject>>.Success(expected)));
        var t3 = new FakeTargetS3();
        var map = new Dictionary<string, IBackupS3>
        {
            ["http://n1:9000"] = t1, ["http://n2:9000"] = t2, ["http://n3:9000"] = t3,
        };
        var pool = new PoolBackupS3(
            ["http://n1:9000"], ["http://n1:9000", "http://n2:9000", "http://n3:9000"],
            "ak", "sk", "b", null, targetFactory: uri => map[uri]);

        // Act
        var result = await pool.ListWalAsync("c", "s", ct: CancellationToken.None);

        // Assert — список c n2, n3 не вызывался.
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Equal(expected);
        t3.Calls.Should().Be(0);
    }

    // AAA: BucketExists — любая живая цель → успех.
    [Fact]
    public async Task BucketExists_AnyAlive_True()
    {
        // Arrange — write-цель первая мертва, вторая жива.
        var t1 = new FakeTargetS3(bucketExists: () => Task.FromResult(Result<bool>.Failed(
            new ApplicationException("down"))));
        var t2 = new FakeTargetS3();
        var pool = Pool(t1, t2);

        // Act
        var result = await pool.BucketExistsAsync(CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().BeTrue();
    }

    // AAA: delete — перебор write-целей симметрично put.
    [Fact]
    public async Task Delete_Failover()
    {
        // Arrange — первая цель отказывает, вторая жива.
        var t1 = new FakeTargetS3(delete: _ => Task.FromResult(Result.Failed(
            new ApplicationException("S3 batch-delete: down"))));
        var t2 = new FakeTargetS3();
        var pool = Pool(t1, t2);

        // Act
        var result = await pool.DeleteKeysAsync(["k1"], CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
    }

    // ===== Circuit-breaker цели (arch/19 §5 «Исключение отказавшей цели») =====

    // AAA: транспортный отказ первой цели исключает её из перебора на окно
    // breaker'а — повторный вызов идёт сразу на вторую цель (без пер-попыточного
    // таймаута мёртвой), результат успех.
    [Fact]
    public async Task Put_TargetFails_ExcludedForBreakerWindow()
    {
        // Arrange — n1 всегда отказывает (транспорт), n2 жива; часы стоят.
        var t1 = new FakeTargetS3(put: _ => Task.FromResult(Result.Failed(
            new ApplicationException("S3 put: conn refused"))));
        var t2 = new FakeTargetS3();
        var pool = Pool(t1, t2, new FakeClock());

        // Act — два вызова подряд в пределах окна.
        var first = await pool.PutObjectAsync("k", [1], null, CancellationToken.None);
        var second = await pool.PutObjectAsync("k", [1], null, CancellationToken.None);

        // Assert — первый прошёл через отказ n1 (1 вызов), второй миновал n1
        // (breaker) и ушёл на n2.
        first.IsSuccess.Should().BeTrue();
        second.IsSuccess.Should().BeTrue();
        t1.Calls.Should().Be(1, "after a failure n1 is excluded from the rotation for the window");
        t2.Calls.Should().Be(2);
    }

    // AAA: истечение окна breaker'а возвращает цель в перебор (периодический
    // зонд мёртвой ноды) — после сдвига часов цель снова вызывается, снова
    // отказывает и снова исключается.
    [Fact]
    public async Task Put_BreakerWindowExpired_TargetReturnsToRotation()
    {
        // Arrange — n1 всегда отказывает, n2 жива; часы управляемые.
        var t1 = new FakeTargetS3(put: _ => Task.FromResult(Result.Failed(
            new ApplicationException("S3 put: conn refused"))));
        var t2 = new FakeTargetS3();
        var clock = new FakeClock();
        var pool = Pool(t1, t2, clock);
        await pool.PutObjectAsync("k", [1], null, CancellationToken.None);
        await pool.PutObjectAsync("k", [1], null, CancellationToken.None);

        // Act — окно истекло (сдвиг часов за 45 с), третий вызов.
        clock.Now += TimeSpan.FromSeconds(46);
        var third = await pool.PutObjectAsync("k", [1], null, CancellationToken.None);

        // Assert — n1 вернулась в перебор (зонд), результат — успех от n2.
        third.IsSuccess.Should().BeTrue();
        t1.Calls.Should().Be(2, "window expiry returned the target to the rotation");
        t2.Calls.Should().Be(3);
    }

    // AAA: успешная цель НЕ исключается из перебора — приоритет первой цели
    // сохраняется на каждом вызове.
    [Fact]
    public async Task Put_TargetAlive_NeverExcluded()
    {
        // Arrange — обе цели живы.
        var t1 = new FakeTargetS3();
        var t2 = new FakeTargetS3();
        var pool = Pool(t1, t2, new FakeClock());

        // Act — два вызова.
        await pool.PutObjectAsync("k", [1], null, CancellationToken.None);
        await pool.PutObjectAsync("k", [1], null, CancellationToken.None);

        // Assert — всё ушло в первую цель, вторая не тронута.
        t1.Calls.Should().Be(2);
        t2.Calls.Should().Be(0);
    }

    // AAA: NotFound — НЕ отказ цели (сервер жив и отвечает): breaker не
    // открывается, цель остаётся в переборе чтения (отстающая mesh-нода
    // не выпадает).
    [Fact]
    public async Task GetBytes_NotFoundDoesNotOpenBreaker()
    {
        // Arrange — n1 отдаёт NotFound (объекта нет), n2 — тело.
        var t1 = new FakeTargetS3(getBytes: _ => Task.FromResult(
            Result<(byte[] Data, string? ETag)>.Failed(
                new ApplicationException("S3 get k: not found"))));
        var t2 = new FakeTargetS3(getBytes: _ => Task.FromResult(
            Result<(byte[] Data, string? ETag)>.Success(([9], null))));
        var pool = Pool(t1, t2, new FakeClock());

        // Act — два вызова в пределах окна.
        var first = await pool.GetObjectBytesAsync("c", "s", "wal/x", CancellationToken.None);
        var second = await pool.GetObjectBytesAsync("c", "s", "wal/x", CancellationToken.None);

        // Assert — оба раза n1 опрошена (окно не открылось), значение — со второй.
        first.Value.Data.Should().Equal(9);
        second.Value.Data.Should().Equal(9);
        t1.Calls.Should().Be(2, "NotFound does not open the breaker — the target is alive");
        t2.Calls.Should().Be(2);
    }

    // AAA (arch/19 §5): отмена/исчерпание бюджета ИСТОЧНИКА — не транспортный
    // отказ цели: breaker цели НЕ открывается, цель остаётся в переборе
    // (иначе истёкший общий бюджет одного вызова исключал бы на 45 с все
    // неуспешные цели, включая возможно здоровую, и доводка тормозила бы
    // дольше модели «transient → доводка тиком»).
    [Fact]
    public async Task Put_CancelledBySourceBudget_TargetStaysInRotation()
    {
        // Arrange — токен источника уже отменён (бюджет истёк); обе цели
        // отвечают отказом (текст без «not found» — обычный транспортный).
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var t1 = new FakeTargetS3(put: _ => Task.FromResult(Result.Failed(
            new ApplicationException("S3 put: down"))));
        var t2 = new FakeTargetS3(put: _ => Task.FromResult(Result.Failed(
            new ApplicationException("S3 put: down"))));
        var pool = Pool(t1, t2, new FakeClock());

        // Act — два вызова подряд с отменённым токеном.
        var first = await pool.PutObjectAsync("k", [1], null, cts.Token);
        var second = await pool.PutObjectAsync("k", [1], null, cts.Token);

        // Assert — оба вызова опросили ЦЕЛИ по порядку (не отказ по breaker'у):
        // n1 вызвана дважды — окна нет, цель в переборе.
        first.IsSuccess.Should().BeFalse();
        second.IsSuccess.Should().BeFalse();
        t1.Calls.Should().Be(2, "source-budget cancellation does not open the breaker — the target stays in the rotation");
        t2.Calls.Should().Be(2);
    }

    // AAA: единственная write-цель в окне breaker'а — маркер исчерпания несёт
    // ИСХОДНУЮ причину отказа (сохранённую при открытии окна), а не пустую.
    [Fact]
    public async Task Put_BrokenTargetInWindow_MarkerCarriesOriginalError()
    {
        // Arrange — write-список только из n1 (read — обе).
        var t1 = new FakeTargetS3(put: _ => Task.FromResult(Result.Failed(
            new ApplicationException("S3 put: conn refused"))));
        var t2 = new FakeTargetS3();
        var map = new Dictionary<string, IBackupS3>
        {
            ["http://n1:9000"] = t1, ["http://n2:9000"] = t2,
        };
        var pool = new PoolBackupS3(
            ["http://n1:9000"], ["http://n1:9000", "http://n2:9000"],
            "ak", "sk", "b", null, clock: new FakeClock(),
            targetFactory: uri => map[uri]);

        // Act — первый вызов открывает окно отказом, второй идёт мимо n1.
        await pool.PutObjectAsync("k", [1], null, CancellationToken.None);
        var second = await pool.PutObjectAsync("k", [1], null, CancellationToken.None);

        // Assert — исчерпание с исходной ошибкой первой попытки.
        second.IsSuccess.Should().BeFalse();
        second.Error!.Message.Should().Contain(S3PoolMarkers.Exhausted);
        second.Error!.Message.Should().Contain("conn refused");
    }


    // AAA: детальный union — одна цель не ответила: успех, объекты живой цели
    // ДОСТАВЛЕНЫ, FailedTargets несёт отказавший uri (факт неполноты для
    // контроля дыры, arch/19 §5).
    [Fact]
    public async Task ListWalUnionDetailed_PartialFailure_ReportsFailedTargets()
    {
        // Arrange — n1 мертва, n2 отдаёт сегменты.
        var dead = new FakeTargetS3(list: _ => Task.FromResult(
            Result<IReadOnlyList<WalObject>>.Failed(new ApplicationException("S3 list: down"))));
        var alive = new FakeTargetS3(list: _ => Task.FromResult(
            Result<IReadOnlyList<WalObject>>.Success(
            [
                new WalObject("000000010000000000000002", DateTimeOffset.UnixEpoch),
                new WalObject("000000010000000000000003", DateTimeOffset.UnixEpoch),
            ])));
        var pool = Pool(dead, alive);

        // Act
        var result = await pool.ListWalUnionDetailedAsync("c", "s", ct: CancellationToken.None);

        // Assert — набор от живой цели + перечень не ответивших.
        result.IsSuccess.Should().BeTrue();
        result.Value.Objects.Select(o => o.Name).Should().BeEquivalentTo(
            ["000000010000000000000002", "000000010000000000000003"]);
        result.Value.FailedTargets.Should().BeEquivalentTo(["http://n1:9000"]);
    }

    // AAA: детальный union — все цели живы: FailedTargets пуст (наблюдение
    // полно: дыра в таком наборе — настоящий разрыв → WAL_CHAIN_ERROR-путь).
    [Fact]
    public async Task ListWalUnionDetailed_AllAlive_NoFailedTargets()
    {
        // Arrange
        var pool = Pool(
            new FakeTargetS3(list: _ => Task.FromResult(
                Result<IReadOnlyList<WalObject>>.Success(
                    [new WalObject("000000010000000000000002", DateTimeOffset.UnixEpoch)]))),
            new FakeTargetS3(list: _ => Task.FromResult(
                Result<IReadOnlyList<WalObject>>.Success(
                    [new WalObject("000000010000000000000003", DateTimeOffset.UnixEpoch)]))));

        // Act
        var result = await pool.ListWalUnionDetailedAsync("c", "s", ct: CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Objects.Should().HaveCount(2);
        result.Value.FailedTargets.Should().BeEmpty();
    }

    // AAA: детальный union — все цели отказали: Failed с маркером (исчерпание).
    [Fact]
    public async Task ListWalUnionDetailed_AllFail_ExhaustedMarker()
    {
        // Arrange
        var pool = Pool(
            new FakeTargetS3(list: _ => Task.FromResult(
                Result<IReadOnlyList<WalObject>>.Failed(new ApplicationException("down")))),
            new FakeTargetS3(list: _ => Task.FromResult(
                Result<IReadOnlyList<WalObject>>.Failed(new ApplicationException("down")))));

        // Act
        var result = await pool.ListWalUnionDetailedAsync("c", "s", ct: CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain(S3PoolMarkers.Exhausted);
    }

    // AAA (spec t01 §6.10): удаление полного — ОДИН объект full/<id>.tar;
    // write-перебор: первая цель отказала — вторая удалила → успех.
    [Fact]
    public async Task DeleteObject_FailsOverToSecondTarget()
    {
        // Arrange — n1 отказывает, n2 удаляет.
        var t1 = new FakeDeleteTargetS3(fail: true);
        var t2 = new FakeDeleteTargetS3();
        var pool = PoolOf([("http://n1:9000", t1), ("http://n2:9000", t2)]);

        // Act
        var result = await pool.DeleteObjectAsync("c", "s", "full/id.tar", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        t1.Deleted.Should().BeEmpty();
        t2.Deleted.Should().Equal("c/s/full/id.tar");
    }

    // AAA: все цели отказали — маркер исчерпания пула в ошибке.
    [Fact]
    public async Task DeleteObject_AllTargetsFail_ExhaustedMarker()
    {
        // Arrange
        var pool = PoolOf(
        [
            ("http://n1:9000", new FakeDeleteTargetS3(fail: true)),
            ("http://n2:9000", new FakeDeleteTargetS3(fail: true)),
        ]);

        // Act
        var result = await pool.DeleteObjectAsync("c", "s", "full/id.tar", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain(S3PoolMarkers.Exhausted);
    }

    // Пул на произвольных целях (write=read=список endpoint'ов).
    private static PoolBackupS3 PoolOf(
        IReadOnlyList<(string Uri, IBackupS3 Target)> targets)
    {
        var map = targets.ToDictionary(t => t.Uri, t => t.Target, StringComparer.Ordinal);
        var uris = targets.Select(t => t.Uri).ToList();
        return new PoolBackupS3(uris, uris, "ak", "sk", "b", null,
            targetFactory: uri => map[uri]);
    }

    /// <summary>Фейк-цель одиночного удаления: копит композированные ключи,
    /// управляемо отказывает; IBackupS3-методы не используются.</summary>
    private sealed class FakeDeleteTargetS3(bool fail = false) : IBackupS3, IDeleteObjectS3
    {
        public List<string> Deleted { get; } = [];

        public Task<Result> DeleteObjectAsync(
            string cluster, string shard, string key, CancellationToken ct)
        {
            if (fail)
                return Task.FromResult(Result.Failed(new ApplicationException("S3 delete: conn refused")));
            Deleted.Add($"{cluster}/{shard}/{key}");
            return Task.FromResult(Result.Success());
        }

        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListAsync(string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> GetObjectAsync(string cluster, string shard, string key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> PutObjectMd5Async(string key, byte[] data, string md5Hex, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<(byte[] Data, string? ETag)>> GetObjectBytesAsync(string cluster, string shard, string key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> DownloadTextAsync(string cluster, string shard, string objectKey, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> PutFileAsync(string key, string filePath, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> GetObjectToFileAsync(string cluster, string shard, string key, string destFilePath, CancellationToken ct = default) => throw new NotSupportedException();
    }

    // AAA (spec t01 §6.23): HEAD по read-целям — первая найденная цель даёт
    // размер; все NotFound → (false, 0).
    [Fact]
    public async Task HeadObject_FirstExistingTargetGivesSize_AllMissFalse()
    {
        // Arrange — n1 «нет объекта», n2 отдаёт размер.
        var pool = PoolOf(
        [
            ("http://n1:9000", new FakeStreamTargetS3 { Exists = false }),
            ("http://n2:9000", new FakeStreamTargetS3 { Size = 4096 }),
        ]);

        // Act
        var found = await pool.HeadObjectAsync("c", "s", "full/id.tar", CancellationToken.None);
        var missPool = PoolOf(
        [
            ("http://n1:9000", new FakeStreamTargetS3 { Exists = false }),
            ("http://n2:9000", new FakeStreamTargetS3 { Exists = false }),
        ]);
        var missing = await missPool.HeadObjectAsync("c", "s", "full/id.tar", CancellationToken.None);

        // Assert
        found.IsSuccess.Should().BeTrue();
        found.Value.Exists.Should().BeTrue();
        found.Value.SizeBytes.Should().Be(4096);
        missing.IsSuccess.Should().BeTrue();
        missing.Value.Exists.Should().BeFalse();
        missing.Value.SizeBytes.Should().Be(0);
    }

    // AAA: Range-GET по read-целям: NotFound на первой — срез со второй.
    [Fact]
    public async Task GetObjectRange_NotFoundOnFirst_FoundOnSecond()
    {
        // Arrange
        var pool = PoolOf(
        [
            ("http://n1:9000", new FakeStreamTargetS3 { RangeNotFound = true }),
            ("http://n2:9000", new FakeStreamTargetS3 { RangeBytes = [7, 8, 9] }),
        ]);

        // Act
        var result = await pool.GetObjectRangeAsync("c", "s", "full/id.tar", 0, 2, CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Equal(7, 8, 9);
    }

    // AAA: стрим тела пулом не реализуется — операции приёмника фиксируют endpoint.
    [Fact]
    public async Task GetObjectStream_NotSupported_PoolIsEndpointPinned()
    {
        var pool = PoolOf([("http://n1:9000", new FakeStreamTargetS3())]);

        var act = async () => await pool.GetObjectStreamAsync("c", "s", "full/id.tar", CancellationToken.None);

        await act.Should().ThrowAsync<NotSupportedException>();
    }

    /// <summary>Фейк-цель потокового чтения (HEAD/Range); IBackupS3 — заглушки.</summary>
    private sealed class FakeStreamTargetS3 : IBackupS3, IStreamObjectS3
    {
        public bool Exists { get; init; } = true;

        public long Size { get; init; } = 1024;

        public bool RangeNotFound { get; init; }

        public byte[] RangeBytes { get; init; } = [1, 2, 3];

        public Task<Result<(bool Exists, long SizeBytes)>> HeadObjectAsync(
            string cluster, string shard, string key, CancellationToken ct)
            => Task.FromResult(Result<(bool Exists, long SizeBytes)>.Success(
                (Exists, Exists ? Size : 0L)));

        public Task<Result<byte[]>> GetObjectRangeAsync(
            string cluster, string shard, string key, long start, long end, CancellationToken ct)
            => Task.FromResult(RangeNotFound
                ? Result<byte[]>.Failed(new ApplicationException(
                    $"S3 get-range {cluster}/{shard}/{key}: not found"))
                : Result<byte[]>.Success(RangeBytes));

        public Task<Result<(Stream Body, long Size)>> GetObjectStreamAsync(
            string cluster, string shard, string key, CancellationToken ct)
            => throw new NotSupportedException();

        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<WalObject>>> ListAsync(string cluster, string shard, string prefix, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> GetObjectAsync(string cluster, string shard, string key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> PutObjectMd5Async(string key, byte[] data, string md5Hex, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<(byte[] Data, string? ETag)>> GetObjectBytesAsync(string cluster, string shard, string key, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result<string>> DownloadTextAsync(string cluster, string shard, string objectKey, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> PutFileAsync(string key, string filePath, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<Result> GetObjectToFileAsync(string cluster, string shard, string key, string destFilePath, CancellationToken ct = default) => throw new NotSupportedException();
    }
}
