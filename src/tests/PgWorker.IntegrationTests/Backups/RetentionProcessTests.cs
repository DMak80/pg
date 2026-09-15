using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using PgWorker.Backups;
using PgWorker.Backups.Job;
using PgWorker.Backups.Sql;
using PgWorker.Core.Model;
using Shared.Etcd.Client;
using PgWorker.Etcd.Parsing;
using PgWorker.IntegrationTests.Etcd;
using PgWorker.Core.Templates;
using PgWorker.Provisioning.Endpoints;
using PgWorker.Provisioning.Processes;
using PgWorker.Provisioning.Probes;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Интеграции RetentionProcess (t06 spec Ф3): реальный etcd (статусы/журнал/ключ
// storage) + FakeBackupS3. Часы по умолчанию заморожены (детерминированные
// now/started_unix); MutableClock — для семантики put ключа storage «при
// изменении» (наблюдаемые поля сравниваются БЕЗ updated_unix — идущие часы
// дискриминируют лишний put). RetentionIntervalSec=0 — ретенция выполняется
// КАЖДЫМ тиком (валидация >=60 — только на старте App, runtime-опции тест
// строит напрямую).
[Collection(EtcdCollection.Name)]
public class RetentionProcessTests(EtcdFixture fixture)
{
    private readonly ClaimStore _claims = new("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);

    // Замороженный момент: среда 2026-09-09 12:00:00 UTC (все started_unix
    // считаются от него; updated_unix ключа storage детерминирован).
    private static readonly DateTimeOffset Now = new(2026, 9, 9, 12, 0, 0, TimeSpan.Zero);

    private sealed class FrozenClock : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => Now;
    }

    // Идущие часы (двигаются только вручную — детерминированно).
    private sealed class MutableClock(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _now = start;

        public void AdvanceMinutes(int minutes) => _now = _now.AddMinutes(minutes);

        public override DateTimeOffset GetUtcNow() => _now;
    }

    // ── Хелперы Arrange ──

    // Активный кластер c1/shard1 с нодой shard1a (master-ключ → shard1a:17001).
    private static ClusterSnapshot BuildSnap(string cluster = "c1", ClusterState state = ClusterState.Active) => new(
        new ClusterConfig(cluster, 2, cluster, null, state),
        [new ShardSpec("shard1", 1, $"host=shard1a dbname={cluster}", "shard1a:17001",
            [new NodeSpec("shard1", "shard1a", NodeState.Running)])],
        []);

    // Сид etcd под конкретный тест-кластер: чистка префикса бэкапов и клэйма
    // прошлых тестов + глобального ключа storage (детерминизм ассертов).
    private async Task SeedAsync(string cluster)
    {
        var ct = TestContext.Current.CancellationToken;
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/backups/{cluster}/", prefix: true, ct);
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/pgworker/claims/{cluster}", prefix: false, ct);
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, "/pgworker/backups/storage", prefix: false, ct);
    }

    private RetentionProcess BuildProcess(
        BackupsRuntimeOptions options, FakeBackupS3 s3, TimeProvider? clock = null) => new(
        fixture.Gateway, [fixture.Endpoint], s3,
        _claims, new WorkJournal("/pgworker", fixture.Gateway, [fixture.Endpoint]),
        options, clock ?? new FrozenClock(),
        NullLogger<RetentionProcess>.Instance);

    // Тест-опции: ретенция каждым тиком; дефолт-политика — параметризуется.
    private static BackupsRuntimeOptions Options(
        int intervalSec = 0, int keepFailed = 20,
        int policyDays = 7, int policyWeeks = 4, int policyMonths = 6,
        long quotaBytes = 0, int quotaWarn = 80, int quotaCrit = 90)
        => new(
            Enabled: true,
            S3Endpoint: "http://minio",
            S3Bucket: "bkt",
            S3AccessKey: "ak",
            S3SecretKey: "sk",
            PolicyRetentionDays: policyDays,
            PolicyRetentionWeeks: policyWeeks,
            PolicyRetentionMonths: policyMonths,
            RetentionIntervalSec: intervalSec,
            RetentionKeepFailed: keepFailed,
            QuotaBytes: quotaBytes,
            QuotaWarnPercent: quotaWarn,
            QuotaCritPercent: quotaCrit);

    private static long Unix(DateTimeOffset t) => t.ToUnixTimeSeconds();

    private static FullBackupState Full(
        string id, DateTimeOffset started, FullBackupStatus state = FullBackupStatus.Completed,
        string? walStart = "000000010000000000000001", BackupVerify? verify = null)
        => new(id, state, "shard1a", BackupSourceRole.Replica,
            Unix(started), Unix(started) + 60, walStart, 1024, null, verify);

    // Сид полного в etcd каноническим JSON (читается парсером t01).
    private async Task SeedFullAsync(string cluster, FullBackupState full)
    {
        await fixture.Gateway.PutAsync(fixture.Endpoint,
            BackupNames.FullKey(cluster, "shard1", full.Id),
            BackupStatusJson.Serialize(full), null, TestContext.Current.CancellationToken);
    }

    private async Task<Kv?> GetKvAsync(string key)
        => (await fixture.Gateway.GetAsync(fixture.Endpoint, key, TestContext.Current.CancellationToken)).Value;

    // ── Сценарии ──

    // AC3: transient-сбой S3 на первом тике — статус DELETING не трогается,
    // объекты/ключ живы; второй тик доводит: объекты удалены, ключ удалён.
    [Fact]
    public async Task Доводка_DELETING_при_сбое_S3()
    {
        // Arrange — свежий COMPLETED (guard/окно) + просроченный COMPLETED
        // с объектами full/<old>/; клэйм наш
        var ct = TestContext.Current.CancellationToken;
        const string cluster = "rt1";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var fresh = Full("20260909000000Z", Now.AddDays(-1));
        var old = Full("20260731000000Z", Now.AddDays(-40));
        await SeedFullAsync(cluster, fresh);
        await SeedFullAsync(cluster, old);
        var objectKeys = new[]
        {
            $"{cluster}/shard1/full/{old.Id}/base.tar",
            $"{cluster}/shard1/full/{old.Id}/.tables",
        };
        var s3 = new FakeBackupS3();
        s3.PrefixObjects.AddRange(objectKeys.Select(k => (k, 5L)));
        var process = BuildProcess(Options(policyDays: 1, policyWeeks: 0, policyMonths: 0), s3);

        // Act — первый тик: put DELETING прошёл, batch-delete сорвался (fake-сбой)
        s3.FailNextDelete = true;
        (await process.TickAsync(BuildSnap(cluster), backups(cluster, fresh, old), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — статус DELETING, объекты живы, ключ жив
        var deleting = await GetKvAsync(BackupNames.FullKey(cluster, "shard1", old.Id));
        deleting.Should().NotBeNull();
        deleting!.Value.Should().Contain("\"DELETING\"");
        s3.PrefixObjects.Should().HaveCount(2, "сбой S3 — ничего не удалено (transient)");
        s3.DeletedKeys.Should().BeEmpty();

        // Act — второй тик без сбоя
        (await process.TickAsync(BuildSnap(cluster), backups(cluster, fresh, old), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — объекты удалены, ключ удалён (доводка завершена)
        s3.DeletedKeys.Should().BeEquivalentTo(objectKeys);
        (await GetKvAsync(BackupNames.FullKey(cluster, "shard1", old.Id))).Should().BeNull();
        (await GetKvAsync(BackupNames.FullKey(cluster, "shard1", fresh.Id))).Should().NotBeNull();
    }

    // AC7-хвост: per-cluster политика из ключа замещает дефолт конфига —
    // при дефолте (месячная точка) старый удерживался бы, policy 1/0/0 удаляет.
    [Fact]
    public async Task Policy_из_ключа_кластера_действует()
    {
        // Arrange — свежий (вчера) + 40-дневный; policy кластера 1/0/0
        var ct = TestContext.Current.CancellationToken;
        const string cluster = "rt2";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var fresh = Full("20260909000000Z", Now.AddDays(-1));
        var old = Full("20260731000000Z", Now.AddDays(-40));
        await SeedFullAsync(cluster, fresh);
        await SeedFullAsync(cluster, old);
        var backups = new ClusterBackups(cluster,
            new BackupPolicy(1, 0, 0, 86400, false),
            new Dictionary<string, ShardBackups>
            {
                ["shard1"] = new([fresh, old], null),
            });
        var s3 = new FakeBackupS3();
        var process = BuildProcess(Options(), s3); // дефолт конфига 7/4/6

        // Act — тик ретенции
        (await process.TickAsync(BuildSnap(cluster), backups, ct)).IsSuccess.Should().BeTrue();

        // Assert — применена policy 1/0/0: старый удалён, свежий жив
        (await GetKvAsync(BackupNames.FullKey(cluster, "shard1", old.Id))).Should().BeNull();
        (await GetKvAsync(BackupNames.FullKey(cluster, "shard1", fresh.Id))).Should().NotBeNull();
    }

    // AC3: два кандидата — за один тик удаляется РОВНО ОДИН (старейший).
    [Fact]
    public async Task Один_кандидат_за_проход()
    {
        // Arrange — свежий + два просроченных; policy 1/0/0
        var ct = TestContext.Current.CancellationToken;
        const string cluster = "rt3";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var fresh = Full("20260909000000Z", Now.AddDays(-1));
        var old1 = Full("20260730000000Z", Now.AddDays(-41));
        var old2 = Full("20260731000000Z", Now.AddDays(-40));
        await SeedFullAsync(cluster, fresh);
        await SeedFullAsync(cluster, old1);
        await SeedFullAsync(cluster, old2);
        var s3 = new FakeBackupS3();
        var process = BuildProcess(Options(policyDays: 1, policyWeeks: 0, policyMonths: 0), s3);

        // Act — один тик
        (await process.TickAsync(BuildSnap(cluster), backups(cluster, fresh, old1, old2), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — удалён только старейший (порядок Delete), второй жив
        (await GetKvAsync(BackupNames.FullKey(cluster, "shard1", old1.Id))).Should().BeNull();
        (await GetKvAsync(BackupNames.FullKey(cluster, "shard1", old2.Id))).Should().NotBeNull();
        (await GetKvAsync(BackupNames.FullKey(cluster, "shard1", fresh.Id))).Should().NotBeNull();
    }

    // AC2: единственный просроченный COMPLETED не удаляется никогда.
    [Fact]
    public async Task Guard_единственный_COMPLETED_не_удаляется()
    {
        // Arrange — один COMPLETED 40 дней назад + его S3-объекты
        var ct = TestContext.Current.CancellationToken;
        const string cluster = "rt4";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var lone = Full("20260731000000Z", Now.AddDays(-40));
        await SeedFullAsync(cluster, lone);
        var s3 = new FakeBackupS3();
        s3.PrefixObjects.Add(($"{cluster}/shard1/full/{lone.Id}/base.tar", 7L));
        var process = BuildProcess(Options(policyDays: 1, policyWeeks: 0, policyMonths: 0), s3);

        // Act — тик ретенции
        (await process.TickAsync(BuildSnap(cluster), backups(cluster, lone), ct)).IsSuccess.Should().BeTrue();

        // Assert — ключ и объекты нетронуты
        (await GetKvAsync(BackupNames.FullKey(cluster, "shard1", lone.Id))).Should().NotBeNull();
        s3.DeletedKeys.Should().BeEmpty();
    }

    // AC4: WAL строго ниже cutoff удалён; cutoff/выше/.history того же TLI живы.
    [Fact]
    public async Task WAL_чистка_ниже_cutoff()
    {
        // Arrange — COMPLETED с wal_start=..05; wal/ содержит 3,4,5,6 + history
        var ct = TestContext.Current.CancellationToken;
        const string cluster = "rt5";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var full = Full("20260908000000Z", Now.AddDays(-1), walStart: "000000010000000000000005");
        await SeedFullAsync(cluster, full);
        var s3 = new FakeBackupS3();
        s3.PrefixObjects.AddRange(new[]
        {
            ($"{cluster}/shard1/wal/000000010000000000000003", 16L),
            ($"{cluster}/shard1/wal/000000010000000000000004", 16L),
            ($"{cluster}/shard1/wal/000000010000000000000005", 16L),
            ($"{cluster}/shard1/wal/000000010000000000000006", 16L),
            ($"{cluster}/shard1/wal/00000001.history", 32L),
        });
        var process = BuildProcess(Options(), s3);

        // Act — тик (просроченных полных нет — только WAL-чистка)
        (await process.TickAsync(BuildSnap(cluster), backups(cluster, full), ct)).IsSuccess.Should().BeTrue();

        // Assert — 3 и 4 удалены; cutoff 5, 6 и history живы
        s3.DeletedKeys.Should().BeEquivalentTo([
            $"{cluster}/shard1/wal/000000010000000000000003",
            $"{cluster}/shard1/wal/000000010000000000000004",
        ]);
        s3.PrefixObjects.Select(o => o.Key).Should().BeEquivalentTo([
            $"{cluster}/shard1/wal/000000010000000000000005",
            $"{cluster}/shard1/wal/000000010000000000000006",
            $"{cluster}/shard1/wal/00000001.history",
        ]);
    }

    // AC4-хвост: после чистки контроль t03 поднимает chain_start до cutoff —
    // цепочка от оставшихся непрерывна (ACTIVE, без error).
    [Fact]
    public async Task Чистка_WAL_поднимает_chain_start_контроля_t03()
    {
        // Arrange — full wal_start=..05; wal-ключ chain_start=..03; S3: сегменты 3..7
        var ct = TestContext.Current.CancellationToken;
        const string cluster = "rt6";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/pgworker/portalloc/{cluster}",
            Portalloc.Serialize(new Dictionary<string, NodeAddress>
            {
                ["shard1/shard1a"] = new("localhost", new NodePorts(16001, 18001, 17001)),
            }), null, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, $"/clusters/{cluster}/backup_password",
            "pw", null, ct);
        var full = Full("20260908000000Z", Now.AddDays(-1), walStart: "000000010000000000000005");
        await SeedFullAsync(cluster, full);
        var s3 = new FakeBackupS3();
        for (var i = 3; i <= 7; i++)
            s3.Objects.Add((cluster, "shard1", $"0000000100000000000000{i:x2}"));
        var writer = new WalStatusWriter(fixture.Gateway, [fixture.Endpoint]);
        await writer.WriteIfChangedAsync(cluster, "shard1", new WalStreamState(
            WalStreamStatus.Active, BackupNames.Slot(cluster, "shard1"), "shard1a",
            "000000010000000000000003", "000000010000000000000007",
            "000000010000000000000007", Unix(Now), 0, null), ct);
        var retention = BuildProcess(Options(), s3);
        var sql = new FakeWalSqlExecutor { Current = ("0/7000000", 1) };
        var walProcess = new WalStreamProcess(
            fixture.Gateway, [fixture.Endpoint], new StubScaleDriver(),
            new ShardEndpoints(fixture.Gateway, [fixture.Endpoint], new ShardProbe(new HttpClient())),
            sql, s3, writer, _claims, new WorkJournal("/pgworker", fixture.Gateway, [fixture.Endpoint]),
            () => (BackupsRuntimeOptions?)Options(), new InstallSecrets("su", "sb", "adm", "mv"),
            new FrozenClock());

        // Act — тик ретенции (сегменты 3,4 удалены), затем тик контроля t03
        (await retention.TickAsync(BuildSnap(cluster), backups(cluster, full), ct))
            .IsSuccess.Should().BeTrue();
        s3.DeletedKeys.Should().HaveCount(2, "предусловие: чистка срезала 3 и 4");
        (await walProcess.TickAsync(BuildSnap(cluster), backups(cluster, full), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — chain_start поднялся до cutoff, цепочка непрерывна
        var wal = await writer.ReadAsync(cluster, "shard1", ct);
        wal.Value.Should().NotBeNull();
        wal.Value!.ChainStartSegment.Should().Be("000000010000000000000005");
        wal.Value.State.Should().Be(WalStreamStatus.Active);
        wal.Value.Error.Should().BeNull();
    }

    // Консервативность: полных нет (cutoff не определён) — WAL не чистится.
    [Fact]
    public async Task WAL_без_полных_не_чистится()
    {
        // Arrange — только wal-объекты, полных/FAILED нет
        var ct = TestContext.Current.CancellationToken;
        const string cluster = "rt7";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var s3 = new FakeBackupS3();
        s3.PrefixObjects.Add(($"{cluster}/shard1/wal/000000010000000000000001", 16L));
        var process = BuildProcess(Options(), s3);
        var empty = new ClusterBackups(cluster, null,
            new Dictionary<string, ShardBackups> { ["shard1"] = new([], null) });

        // Act — тик ретенции
        (await process.TickAsync(BuildSnap(cluster), empty, ct)).IsSuccess.Should().BeTrue();

        // Assert — ничего не удалено
        s3.DeletedKeys.Should().BeEmpty();
    }

    // AC5: при >KeepFailed FAILED старейшие удалены; бэкофф t02 не изменился.
    [Fact]
    public async Task FAILED_гигиена()
    {
        // Arrange — COMPLETED + 7 FAILED после него; KeepFailed=5
        var ct = TestContext.Current.CancellationToken;
        const string cluster = "rt8";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var done = Full("20260907000000Z", Now.AddDays(-2));
        var failed = Enumerable.Range(0, 7)
            .Select(i => Full($"f{i}", Now.AddHours(-20 + i), FullBackupStatus.Failed, walStart: null))
            .ToList();
        await SeedFullAsync(cluster, done);
        foreach (var f in failed)
            await SeedFullAsync(cluster, f);
        var s3 = new FakeBackupS3();
        var process = BuildProcess(Options(keepFailed: 5), s3);
        var before = failed.ToList();

        // Act — тик ретенции
        (await process.TickAsync(BuildSnap(cluster), backups(cluster, [done, .. failed]), ct))
            .IsSuccess.Should().BeTrue();

        // Assert — удалены 2 старейших ключа, 5 свежих живы; бэкофф не сброшен
        (await GetKvAsync(BackupNames.FullKey(cluster, "shard1", "f0"))).Should().BeNull();
        (await GetKvAsync(BackupNames.FullKey(cluster, "shard1", "f1"))).Should().BeNull();
        foreach (var f in failed.Skip(2))
            (await GetKvAsync(BackupNames.FullKey(cluster, "shard1", f.Id))).Should().NotBeNull();
        var remaining = failed.Skip(2).ToList();
        var nowUnix = Unix(Now.AddMinutes(10));
        BackupPlanner.BackoffPassed(before, 300, 3600, nowUnix)
            .Should().Be(BackupPlanner.BackoffPassed(remaining, 300, 3600, nowUnix),
                "чистка до KeepFailed не двигает retry_not_before (n упирается в MaxSec)");
    }

    // AC8: Enabled=false — полный no-op, ключ storage не пишется.
    [Fact]
    public async Task Enabled_false_no_op()
    {
        // Arrange — живые ключи/объекты; выключенная подсистема
        var ct = TestContext.Current.CancellationToken;
        const string cluster = "rt9";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var full = Full("20260731000000Z", Now.AddDays(-40));
        await SeedFullAsync(cluster, full);
        var s3 = new FakeBackupS3();
        s3.PrefixObjects.Add(($"{cluster}/shard1/full/{full.Id}/base.tar", 7L));
        var process = BuildProcess(Options() with { }, s3);
        var disabled = new RetentionProcess(
            fixture.Gateway, [fixture.Endpoint], s3,
            _claims, new WorkJournal("/pgworker", fixture.Gateway, [fixture.Endpoint]),
            Options() with { Enabled = false }, new FrozenClock(),
            NullLogger<RetentionProcess>.Instance);

        // Act — тик выключенной подсистемы
        (await disabled.TickAsync(BuildSnap(cluster), backups(cluster, full), ct)).IsSuccess.Should().BeTrue();

        // Assert — ничего не удалено, ключ storage не писался
        (await GetKvAsync(BackupNames.FullKey(cluster, "shard1", full.Id))).Should().NotBeNull();
        s3.DeletedKeys.Should().BeEmpty();
        (await GetKvAsync("/pgworker/backups/storage")).Should().BeNull();
    }

    // Клэйм-гвард: без клэйма тик отказывает (мутации запрещены).
    [Fact]
    public async Task Клэйм_не_наш_отказ()
    {
        // Arrange — клэйм НЕ брали
        const string cluster = "rt10";
        await SeedAsync(cluster);
        var process = BuildProcess(Options(), new FakeBackupS3());

        // Act — тик без клэйма
        var result = await process.TickAsync(BuildSnap(cluster), null, TestContext.Current.CancellationToken);

        // Assert — Failed с сообщением про клэйм
        result.IsSuccess.Should().BeFalse();
        result.Error!.Message.Should().Contain("клэйм");
    }

    // AC6: ключ storage пишется с суммой размеров и вердиктом; повторный тик
    // без изменений НЕ переписывает значение (сравнение наблюдаемых полей).
    [Fact]
    public async Task Ключ_storage_пишется_при_изменении()
    {
        // Arrange — 2 объекта (10+20), квота 100 (30% — OK)
        var ct = TestContext.Current.CancellationToken;
        const string cluster = "rt11";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var s3 = new FakeBackupS3();
        s3.PrefixObjects.AddRange(new[] { ("a/obj1", 10L), ("b/obj2", 20L) });
        var process = BuildProcess(Options(quotaBytes: 100, quotaWarn: 80, quotaCrit: 90), s3);

        // Act — два тика подряд (данные не менялись → наблюдаемые поля те же)
        (await process.TickAsync(BuildSnap(cluster), null, ct)).IsSuccess.Should().BeTrue();
        var first = await GetKvAsync("/pgworker/backups/storage");
        (await process.TickAsync(BuildSnap(cluster), null, ct)).IsSuccess.Should().BeTrue();
        var second = await GetKvAsync("/pgworker/backups/storage");

        // Assert — поля канона; повторный тик значение не изменил
        first.Should().NotBeNull();
        first!.Value.Should().Contain("\"used_bytes\":30")
            .And.Contain("\"quota_bytes\":100")
            .And.Contain("\"used_percent\":30")
            .And.Contain("\"state\":\"OK\"");
        second!.Value.Should().Be(first.Value, "put при неизменных наблюдаемых полях не выполняется");
    }

    // AC6-семантика «при изменении» (arch/19 §4): два прохода с неизменными
    // данными при ИДУЩИХ часах — второй put НЕ выполняется: updated_unix не
    // участвует в сравнении, свежесть прохода видна по возрасту updated_unix.
    [Fact]
    public async Task Storage_без_изменений_второй_put_не_выполняется()
    {
        // Arrange — 2 объекта (10+20); часы идут вперёд между тиками
        var ct = TestContext.Current.CancellationToken;
        const string cluster = "rt14";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var clock = new MutableClock(Now);
        var s3 = new FakeBackupS3();
        s3.PrefixObjects.AddRange(new[] { ("a/obj1", 10L), ("b/obj2", 20L) });
        var process = BuildProcess(Options(quotaBytes: 100), s3, clock);

        // Act — тик, минута, второй тик (данные не менялись)
        (await process.TickAsync(BuildSnap(cluster), null, ct)).IsSuccess.Should().BeTrue();
        var first = await GetKvAsync("/pgworker/backups/storage");
        clock.AdvanceMinutes(1);
        (await process.TickAsync(BuildSnap(cluster), null, ct)).IsSuccess.Should().BeTrue();
        var second = await GetKvAsync("/pgworker/backups/storage");

        // Assert — ключ не переписан: updated_unix остался от первого прохода
        first.Should().NotBeNull();
        first!.Value.Should().Contain($"\"updated_unix\":{Unix(Now)}");
        second!.Value.Should().Be(first.Value,
            "меняющийся updated_unix не должен превращать каждый проход в put");
    }

    // AC6: изменился used_bytes — put выполняется с новым значением и свежим
    // updated_unix (наблюдаемые поля разошлись → запись обязательна).
    [Fact]
    public async Task Storage_при_изменении_put_с_новым_значением_и_свежим_updated()
    {
        // Arrange — 1 объект (10); часы идут
        var ct = TestContext.Current.CancellationToken;
        const string cluster = "rt15";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var clock = new MutableClock(Now);
        var s3 = new FakeBackupS3();
        s3.PrefixObjects.Add(("a/obj1", 10L));
        var process = BuildProcess(Options(quotaBytes: 100), s3, clock);

        // Act — тик, рост занятости (+20), минута, второй тик
        (await process.TickAsync(BuildSnap(cluster), null, ct)).IsSuccess.Should().BeTrue();
        var first = await GetKvAsync("/pgworker/backups/storage");
        s3.PrefixObjects.Add(("a/obj2", 20L));
        clock.AdvanceMinutes(1);
        (await process.TickAsync(BuildSnap(cluster), null, ct)).IsSuccess.Should().BeTrue();
        var second = await GetKvAsync("/pgworker/backups/storage");

        // Assert — новое used_bytes и updated_unix второго момента
        first.Should().NotBeNull();
        first!.Value.Should().Contain("\"used_bytes\":10");
        second!.Value.Should().Contain("\"used_bytes\":30")
            .And.Contain($"\"updated_unix\":{Unix(Now.AddMinutes(1))}",
                "изменение наблюдаемых полей → put со свежим updated_unix");
    }

    // AC6: квота 0 → только used/state/updated, полей квоты нет.
    [Fact]
    public async Task Квота_0_без_полей_квоты()
    {
        // Arrange — объекты есть, квота не задана (Bytes=0)
        var ct = TestContext.Current.CancellationToken;
        const string cluster = "rt12";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var s3 = new FakeBackupS3();
        s3.PrefixObjects.Add(("a/obj1", 42L));
        var process = BuildProcess(Options(quotaBytes: 0), s3);

        // Act — тик
        (await process.TickAsync(BuildSnap(cluster), null, ct)).IsSuccess.Should().BeTrue();

        // Assert — used_bytes есть, квота-полей нет, state OK
        var kv = await GetKvAsync("/pgworker/backups/storage");
        kv.Should().NotBeNull();
        kv!.Value.Should().Contain("\"used_bytes\":42")
            .And.Contain("\"state\":\"OK\"")
            .And.NotContain("quota_bytes")
            .And.NotContain("used_percent");
    }

    // G0: кластер не Active — no-op.
    [Fact]
    public async Task Не_Active_кластер()
    {
        // Arrange — ToRemove-кластер с живым ключом
        var ct = TestContext.Current.CancellationToken;
        const string cluster = "rt13";
        await SeedAsync(cluster);
        (await _claims.TryClaimClusterAsync(cluster, ct)).Value.Should().BeTrue();
        var full = Full("20260731000000Z", Now.AddDays(-40));
        await SeedFullAsync(cluster, full);
        var s3 = new FakeBackupS3();
        s3.PrefixObjects.Add(($"{cluster}/shard1/full/{full.Id}/base.tar", 7L));
        var process = BuildProcess(Options(policyDays: 1, policyWeeks: 0, policyMonths: 0), s3);

        // Act — тик по не-Active снапшоту
        var result = await process.TickAsync(
            BuildSnap(cluster, ClusterState.ToRemove), backups(cluster, full),
            TestContext.Current.CancellationToken);

        // Assert — Done, ничего не удалено
        result.IsSuccess.Should().BeTrue();
        s3.DeletedKeys.Should().BeEmpty();
        (await GetKvAsync(BackupNames.FullKey(cluster, "shard1", full.Id))).Should().NotBeNull();
    }

    // Вспомогательное: ClusterBackups c одним шардовым набором полных.
    private static ClusterBackups backups(string cluster, params FullBackupState[] fulls)
        => new(cluster, null, new Dictionary<string, ShardBackups>
        {
            ["shard1"] = new(fulls, null),
        });
}
