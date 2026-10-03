using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using FluentAssertions;
using PgWorker.App;
using PgWorker.App.Loops;
using PgWorker.Backups;
using PgWorker.Backups.EtcdExport;
using PgWorker.Core;
using Shared.Etcd.Client;
using Xunit;

namespace PgWorker.UnitTests.App;

// Доводка выгрузки в SnapshotLoop (t08, spec §3.5): тик лидера перед снятием
// догоняет отстающую выгрузку (статус FAILED/ключа нет/локальный новее) —
// re-export новейшего локального слепка; сон при отставании RetryIntervalSec
// (наблюдаемо: второй слепок в пределах тестового бюджета, не 360 мин).
public class SnapshotLoopExportTests
{
    // S3 в памяти: put/list/delete по словарю (для CatchUpAsync-конвейера sink'а);
    // PutObjectFails — инъекция отказа put (транзиент S3: статус FAILED держится).
    private sealed class MemoryS3 : IBackupS3
    {
        public Dictionary<string, byte[]> Objects { get; } = [];
        public bool PutObjectFails { get; set; }
        public Task<Result> PutObjectAsync(string key, byte[] data, string? sha256, CancellationToken ct = default)
        {
            if (PutObjectFails)
                return Task.FromResult(Result.Failed(new ApplicationException("S3 недоступен (инъекция)")));
            Objects[key] = data;
            return Task.FromResult(Result.Success());
        }
        public Task<Result<IReadOnlyList<S3ObjectInfo>>> ListPrefixAsync(string prefix, int? m = null, CancellationToken ct = default)
            => Task.FromResult(Result<IReadOnlyList<S3ObjectInfo>>.Success(
                Objects.Keys.Where(k => k.StartsWith(prefix, StringComparison.Ordinal))
                    .Select(k => new S3ObjectInfo(k, Objects[k].Length, DateTimeOffset.UnixEpoch)).ToList()));
        public Task<Result> DeleteKeysAsync(IReadOnlyList<string> keys, CancellationToken ct = default)
        { foreach (var k in keys) Objects.Remove(k); return Task.FromResult(Result.Success()); }
        // не нужны сценарию
        public Task<Result<bool>> BucketExistsAsync(CancellationToken ct = default) => Task.FromResult(Result<bool>.Success(true));
        public Task<Result<IReadOnlyList<WalObject>>> ListWalAsync(string c, string s, int? m = null, CancellationToken ct = default) => Task.FromResult(Result<IReadOnlyList<WalObject>>.Success([]));
        public Task<Result<IReadOnlyList<WalObject>>> ListAsync(string c, string s, string p, int? m = null, CancellationToken ct = default) => Task.FromResult(Result<IReadOnlyList<WalObject>>.Success([]));
        public Task<Result<string>> GetObjectAsync(string c, string s, string k, CancellationToken ct = default) => Task.FromResult(Result<string>.Success(""));
        public Task<Result<IReadOnlyList<string>>> ListFullsAsync(string c, string s, int? m = null, CancellationToken ct = default) => Task.FromResult(Result<IReadOnlyList<string>>.Success([]));
        public Task<Result<string>> DownloadTextAsync(string c, string s, string k, CancellationToken ct = default) => Task.FromResult(Result<string>.Success(""));
    }

    private sealed class FixedMonitor(PgWorkerOptions value) : IOptionsMonitor<PgWorkerOptions>
    {
        public PgWorkerOptions CurrentValue => value;
        public IDisposable? OnChange(Action<PgWorkerOptions, string?> listener) => null;
        public PgWorkerOptions Get(string? name) => value;
    }

    private const string Ep = "http://etcd:2379";

    // AAA: доводка перед снятием — локальный слепок с FAILED-статусом доезжает в S3
    [Fact]
    public async Task Тик_доводит_отстающую_выгрузку()
    {
        // Arrange — etcd-фейк (снимает [1,2,3]), каталог с уже снятым слепком,
        // статус-ключ FAILED (прошлый транзиент), sink на памяти
        var etcd = new Provisioning.Fakes.FakeEtcd();
        var dir = Directory.CreateTempSubdirectory("loop-export-").FullName;
        var fileName = $"snapshot-{DateTimeOffset.UtcNow:yyyyMMdd-HHmmss}.db";
        await File.WriteAllBytesAsync(Path.Combine(dir, fileName), [1, 2, 3], TestContext.Current.CancellationToken);
        await etcd.PutAsync(Ep, EtcdSnapshotStatusJson.Key,
            EtcdSnapshotStatusJson.Failed(null, "S3 был недоступен", 360), null, TestContext.Current.CancellationToken);
        var s3 = new MemoryS3();
        var sink = new EtcdSnapshotSink(s3, etcd, [Ep], 28, 5, "inst-A", 360);

        var loop = BuildLoop(etcd, dir, sink, retrySec: 1, snapshotMin: 360);

        // Act — пара тиков лидера (доводка п.1 до TakeAsync)
        using var cts = new CancellationTokenSource();
        await loop.StartAsync(cts.Token);
        var caught = await WaitUntilAsync(() => s3.Objects.ContainsKey($"etcd/{fileName}"));

        // Assert — новейший локальный файл доехал ДО/вместе со снятием; статус OK
        caught.Should().BeTrue("доводка обязана выгрузить новейший локальный слепок (spec §3.5 п.1)");
        await loop.StopAsync(CancellationToken.None);
    }

    // AAA: сон при отставании — RetryIntervalSec (второй слепок за секунды, не 360 мин):
    // статус FAILED держится отказом S3-put — доводка не чинит, каждый тик отстаёт.
    // Тик 1: CatchUpAsync(true: FAILED, том пуст — догонять нечего) → TakeAsync
    // слепок 1 (встроенный export падает, статус FAILED) → короткий сон 1 c →
    // тик 2: слепок 2. Второй файл = строгое доказательство короткого межтика.
    [Fact]
    public async Task Сон_при_отставании_RetryIntervalSec()
    {
        // Arrange — FAILED-статус, том пуст, S3-фейк с отказом put (статус не чинится)
        var etcd = new Provisioning.Fakes.FakeEtcd();
        var dir = Directory.CreateTempSubdirectory("loop-retry-").FullName;
        await etcd.PutAsync(Ep, EtcdSnapshotStatusJson.Key,
            EtcdSnapshotStatusJson.Failed(null, "транзиент", 360), null, TestContext.Current.CancellationToken);
        var s3 = new MemoryS3 { PutObjectFails = true };
        var sink = new EtcdSnapshotSink(s3, etcd, [Ep], 28, 5, "inst-A", 360);

        var loop = BuildLoop(etcd, dir, sink, retrySec: 1, snapshotMin: 360);
        using var cts = new CancellationTokenSource();
        await loop.StartAsync(cts.Token);

        // Act — ждём ВТОРОЙ локальный слепок: первый тик снимает свой слепок ещё
        // до сна, второй возможен только при RetryIntervalSec=1 c (не 360 мин)
        var second = await WaitUntilAsync(() =>
            Directory.GetFiles(dir, "snapshot-*.db").Length >= 2, budgetMs: 15_000);

        // Assert
        second.Should().BeTrue("сон при отставании — RetryIntervalSec=1 c, не SnapshotIntervalMin=360");
        await loop.StopAsync(CancellationToken.None);
    }

    // AAA (крайний случай, §3.5 п.3): FAILED при ПУСТОМ томе — выгрузка отстаёт
    // (CatchUpAsync → true без re-export): сон короткий, не 360-минутный
    [Fact]
    public async Task CatchUp_пустой_том_FAILED_отстаёт()
    {
        // Arrange — FAILED-статус, локальных слепков нет (догонять нечего)
        var etcd = new Provisioning.Fakes.FakeEtcd();
        var dir = Directory.CreateTempSubdirectory("loop-empty-").FullName;
        await etcd.PutAsync(Ep, EtcdSnapshotStatusJson.Key,
            EtcdSnapshotStatusJson.Failed(null, "транзиент", 360), null, TestContext.Current.CancellationToken);
        var sink = new EtcdSnapshotSink(new MemoryS3(), etcd, [Ep], 28, 5, "inst-A", 360);

        // Act
        var catchUp = await sink.CatchUpAsync(dir, TestContext.Current.CancellationToken);

        // Assert — отстаёт (true, без ошибки): SnapshotLoop уйдёт в RetryIntervalSec
        catchUp.IsSuccess.Should().BeTrue();
        catchUp.Value.Should().BeTrue("FAILED держит короткий сон даже при пустом томе");
    }

    private SnapshotLoop BuildLoop(Provisioning.Fakes.FakeEtcd etcd, string dir, EtcdSnapshotSink sink,
        int retrySec, int snapshotMin)
    {
        var options = new FixedMonitor(new PgWorkerOptions
        {
            Etcd = new EtcdOptions { Endpoints = [Ep] },
            Loops = new LoopsOptions { ScanIntervalSec = 1, SnapshotIntervalMin = snapshotMin },
            Snapshots = new SnapshotOptions
            {
                Dir = dir,
                Export = new SnapshotExportOptions { Enabled = true, RetryIntervalSec = retrySec },
            },
        });
        var job = new Shared.Etcd.Maintenance.SnapshotJob(etcd, [Ep], dir, 10, 60, sink);
        return new SnapshotLoop(
            options,
            new ClaimStore("/pgworker", [Ep], etcd, TimeProvider.System),
            job, NullLogger<SnapshotLoop>.Instance, new HealthState(TimeProvider.System), TimeProvider.System,
            new Shared.Metrics.Worker.WorkerMetricsInstrumentation(
                new System.Diagnostics.Metrics.Meter("TestLoops"), TimeProvider.System),
            sink);
    }

    private static async Task<bool> WaitUntilAsync(Func<bool> done, int budgetMs = 10_000)
    {
        for (var i = 0; i < budgetMs / 50 && !done(); i++)
            await Task.Delay(50, TestContext.Current.CancellationToken);
        return done();
    }
}
