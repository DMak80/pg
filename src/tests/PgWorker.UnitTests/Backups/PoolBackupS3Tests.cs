using PgWorker.Backups;
using Shared.Core;

namespace PgWorker.UnitTests.Backups;

// PoolBackupS3 (t36, arch/19 §5): перебор write/read-целей на фейках
// FakeTargetS3 с управляемым отказом (инъекция targetFactory).
public class PoolBackupS3Tests
{
    // Фейк цели: сценарий управляется делегатами/флагом отказа.
    private sealed class FakeTargetS3(
        Func<Task<Result<bool>>>? bucketExists = null,
        Func<string, Task<Result>>? put = null,
        Func<IReadOnlyList<string>, Task<Result>>? delete = null,
        Func<string, Task<Result<string>>>? get = null,
        Func<string, Task<Result<IReadOnlyList<WalObject>>>>? list = null) : IBackupS3
    {
        public int Calls;

        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct)
            => (bucketExists ?? (() => Task.FromResult(Result<bool>.Success(true))))();

        public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(
            string cluster, string shard, int? maxKeysPerTest = null, CancellationToken ct = default)
            => list?.Invoke("wal/") ?? Task.FromResult(Result<IReadOnlyList<WalObject>>.Success([]));

        public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(
            string prefix, int? maxKeysPerTest = null, CancellationToken ct = default)
            => Task.FromResult(Result<IReadOnlyList<S3ObjectInfo>>.Success([]));

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

    // AAA: get — NotFound на первой, значение со второй.
    [Fact]
    public async Task Get_NotFoundOnFirst_FoundOnSecond()
    {
        // Arrange — первая отдаёт «not found», вторая содержимое.
        var t1 = new FakeTargetS3(get: _ => Task.FromResult(Result<string>.Failed(
            new ApplicationException("S3 get k: not found"))));
        var t2 = new FakeTargetS3(get: _ => Task.FromResult(Result<string>.Success("body")));
        var pool = Pool(t1, t2);

        // Act
        var result = await pool.GetObjectAsync("c", "s", "wal/00000001.history", CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be("body");
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
    // открывается, цель остаётся в переборе чтения (union-семантика
    // «где данные есть» — отстающая mesh-нода не выпадает).
    [Fact]
    public async Task Get_NotFoundDoesNotOpenBreaker()
    {
        // Arrange — n1 отдаёт NotFound (объекта нет), n2 — тело.
        var t1 = new FakeTargetS3(get: _ => Task.FromResult(Result<string>.Failed(
            new ApplicationException("S3 get k: not found"))));
        var t2 = new FakeTargetS3(get: _ => Task.FromResult(Result<string>.Success("body")));
        var pool = Pool(t1, t2, new FakeClock());

        // Act — два вызова get в пределах окна.
        var first = await pool.GetObjectAsync("c", "s", "wal/x", CancellationToken.None);
        var second = await pool.GetObjectAsync("c", "s", "wal/x", CancellationToken.None);

        // Assert — оба раза n1 опрошена (окно не открылось), значение — со второй.
        first.Value.Should().Be("body");
        second.Value.Should().Be("body");
        t1.Calls.Should().Be(2, "NotFound does not open the breaker — the target is alive");
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
}
