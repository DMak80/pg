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
        Func<string, Task<Result<IReadOnlyList<S3ObjectInfo>>>>? listPrefix = null) : IBackupS3
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

    // AAA (arch/19 §5 «где данные есть»): union-листинг ПРОИЗВОЛЬНОГО
    // префикса объединяет ответы ВСЕХ read-целей с дедупом по имени и живёт,
    // пока отвечает хотя бы одна (первая живая может не иметь сегмента,
    // залитого на вторую — прецедент AC4: полный заливается на первую живую
    // ноду, а лист набора verify с FirstAlive видел пусто).
    [Fact]
    public async Task ListUnion_MergesAllTargets_DedupByName()
    {
        // Arrange — n1: сегмент 02; n2: 02 (дубль) и 03; n3 — мёртвая.
        WalObject W(string s) => new(s, DateTimeOffset.UnixEpoch);
        var t1 = new FakeTargetS3(list: _ => Task.FromResult(
            Result<IReadOnlyList<WalObject>>.Success([W("000000010000000000000002")])));
        var t2 = new FakeTargetS3(list: _ => Task.FromResult(
            Result<IReadOnlyList<WalObject>>.Success(
            [W("000000010000000000000002"), W("000000010000000000000003")])));
        var t3 = new FakeTargetS3(list: _ => Task.FromResult(
            Result<IReadOnlyList<WalObject>>.Failed(new ApplicationException("n3 down"))));
        var map = new Dictionary<string, IBackupS3>
        {
            ["http://n1:9000"] = t1, ["http://n2:9000"] = t2, ["http://n3:9000"] = t3,
        };
        var pool = new PoolBackupS3(
            ["http://n1:9000"], ["http://n1:9000", "http://n2:9000", "http://n3:9000"],
            "ak", "sk", "b", null,
            targetFactory: uri => map[uri]);

        // Act — union префикса набора полного (как verify-валидация).
        var result = await pool.ListUnionAsync("c", "s", "full/id1/pg_wal/", ct: CancellationToken.None);

        // Assert — успех при мёртвой n3; дедуп 02; 03 из n2 присутствует.
        result.IsSuccess.Should().BeTrue();
        var names = result.Value.Select(o => o.Name).OrderBy(n => n).ToList();
        names.Should().BeEquivalentTo(
            ["000000010000000000000002", "000000010000000000000003"]);
    }

    // AAA: ВСЕ цели мертвы → Failed с маркером исчерпания (не пустой успех).
    [Fact]
    public async Task ListUnion_AllTargetsFail_ExhaustedMarker()
    {
        // Arrange
        var t1 = new FakeTargetS3(list: _ => Task.FromResult(
            Result<IReadOnlyList<WalObject>>.Failed(new ApplicationException("S3 list: down"))));
        var t2 = new FakeTargetS3(list: _ => Task.FromResult(
            Result<IReadOnlyList<WalObject>>.Failed(new ApplicationException("S3 list: down"))));
        var pool = Pool(t1, t2);

        // Act
        var result = await pool.ListUnionAsync("c", "s", "wal/", ct: CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain(S3PoolMarkers.Exhausted);
    }

    // AAA: union list-prefix по всем нодам пула (скачивание набора в
    // verify/restore): пересечение ответов дедупится по ПОЛНОМУ ключу —
    // полный заливается на одну ноду момента заливки, лаг mesh даёт копии.
    [Fact]
    public async Task ListPrefixUnion_MergesAllTargets_DedupByKey()
    {
        // Arrange — n1: объект A; n2: A (копия/лаг) и B.
        S3ObjectInfo O(string key) => new(key, 16, DateTimeOffset.UnixEpoch);
        var t1 = new FakeTargetS3(listPrefix: _ => Task.FromResult(
            Result<IReadOnlyList<S3ObjectInfo>>.Success([O("c/s/full/id1/backup_label")])));
        var t2 = new FakeTargetS3(listPrefix: _ => Task.FromResult(
            Result<IReadOnlyList<S3ObjectInfo>>.Success(
            [O("c/s/full/id1/backup_label"), O("c/s/full/id1/backup_manifest")])));
        var pool = Pool(t1, t2);

        // Act — union префикса набора (как скачивание verify/restore).
        var result = await pool.ListPrefixUnionAsync("c/s/full/id1/", ct: CancellationToken.None);

        // Assert — дедуп по ключу: A один раз, B присутствует.
        result.IsSuccess.Should().BeTrue();
        result.Value.Select(o => o.Key).OrderBy(k => k).Should().BeEquivalentTo(
            ["c/s/full/id1/backup_label", "c/s/full/id1/backup_manifest"]);
    }

    // AAA: частичный отказ пула — успех при живой хотя бы одной ноде
    // (симметрия ListUnionAsync: соло-лист живой ноды достоверен);
    // ВСЕ мертвы → Failed с маркером исчерпания.
    [Fact]
    public async Task ListPrefixUnion_PartialFailure_SucceedsAllFail_Exhausted()
    {
        // Arrange 1 — n1 мертва, n2 отвечает: union успешен.
        var dead = new FakeTargetS3(listPrefix: _ => Task.FromResult(
            Result<IReadOnlyList<S3ObjectInfo>>.Failed(new ApplicationException("S3 list: down"))));
        var alive = new FakeTargetS3(listPrefix: _ => Task.FromResult(
            Result<IReadOnlyList<S3ObjectInfo>>.Success(
                [new S3ObjectInfo("c/s/full/id1/backup_label", 16, DateTimeOffset.UnixEpoch)])));
        var pool = Pool(dead, alive);

        // Act 1 / Assert 1
        var result = await pool.ListPrefixUnionAsync("c/s/full/id1/", ct: CancellationToken.None);
        result.IsSuccess.Should().BeTrue("one live node's listing is sufficient");
        result.Value.Should().ContainSingle().Which.Key.Should().Be("c/s/full/id1/backup_label");

        // Arrange 2 — обе мертвы.
        var pool2 = Pool(
            new FakeTargetS3(listPrefix: _ => Task.FromResult(
                Result<IReadOnlyList<S3ObjectInfo>>.Failed(new ApplicationException("S3 list: down")))),
            new FakeTargetS3(listPrefix: _ => Task.FromResult(
                Result<IReadOnlyList<S3ObjectInfo>>.Failed(new ApplicationException("S3 list: down")))));

        // Act 2 / Assert 2 — маркер исчерпания (не пустой успех).
        var result2 = await pool2.ListPrefixUnionAsync("c/s/full/id1/", ct: CancellationToken.None);
        result2.IsSuccess.Should().BeFalse();
        result2.Error!.Message.Should().Contain(S3PoolMarkers.Exhausted);
    }
}
