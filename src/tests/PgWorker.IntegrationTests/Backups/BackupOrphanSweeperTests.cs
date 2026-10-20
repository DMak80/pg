using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Supervisor;
using Shared.Core;
using Shared.Etcd.Client;
using PgWorker.IntegrationTests.Etcd;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Интеграции BackupOrphanSweeper (t07, spec §3.4 глобальная часть): реальный
// etcd (реестр/лидерство/кластеры) + FakeBackupS3 + MutableClock (сжатое TTL).
// Целевая архитектура E2E (docs/e2e-isolation.md §1): единица изоляции —
// класс; окружение — СВОЙ etcd в СВОЙ docker-сети (OwnedEtcdFixture) —
// чужие записи в общий etcd коллекции на класс не влияют.
[Collection(NonE2eCollection.Name)]
public class BackupOrphanSweeperTests(OwnedEtcdFixture fixture) : IClassFixture<OwnedEtcdFixture>
{
    // Фиксированные часы: сжатое время TTL/first_seen (AAA).
    private sealed class MutableClock : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = new(2026, 9, 13, 12, 0, 0, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow() => Now;
    }

    private static BackupsRuntimeOptions Options(long ttl = 604800) => new(
        Enabled: true,
        S3Bucket: "bkt",
        SupervisorOrphanTtlSec: ttl);

    // Сид: чистка лидерства/реестра/чужих кластеров + объекты сироты ghost<тег>/s1.
    private async Task<MutableClock> SeedAsync(FakeBackupS3 s3, string cluster = "ghost")
    {
        var ct = TestContext.Current.CancellationToken;
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, "/pgworker/leader", prefix: false, ct);
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, OrphanRegistry.Key, prefix: false, ct);
        // hold/заявки кейсов t04 — чистка от предыдущих тестов (изоляция сида).
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, OrphanRegistry.HoldsPrefix, prefix: true, ct);
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, OrphanRegistry.DeletesPrefix, prefix: true, ct);
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/clusters/{cluster}/", prefix: true, ct);
        var clock = new MutableClock();
        s3.PrefixObjects.Add(($"{cluster}/s1/full/20260911110000Z/base.tar", 100));
        return clock;
    }

    private BackupOrphanSweeper BuildSweeper(
        FakeBackupS3 s3, ClaimStore claims, MutableClock? clock = null, BackupsRuntimeOptions? options = null,
        IEtcdGateway? gateway = null)
        => new(gateway ?? fixture.Gateway, [fixture.Endpoint], FakePoolFactory.Single(s3),
            FakePoolFactory.Store(), claims,
            () => options ?? Options(), clock ?? TimeProvider.System);

    private async Task<OrphanRegistry.Registry?> ReadRegistryAsync()
    {
        var kv = await fixture.Gateway.GetAsync(
            fixture.Endpoint, OrphanRegistry.Key, TestContext.Current.CancellationToken);
        return kv.Value is null ? null : OrphanRegistry.Parse(kv.Value.Value);
    }

    // AAA (AC7): префикс исчезнувшего шарда → OBSERVED в реестре; повторный проход
    // переносит first_seen (не сбрасывает)
    [Fact]
    public async Task SweepOrphanInRegistryFirstSeenCarriedOver()
    {
        // Arrange — лидер взят; сирота ghost/s1; объектов владельца в /clusters/ нет
        var ct = TestContext.Current.CancellationToken;
        var s3 = new FakeBackupS3();
        var clock = await SeedAsync(s3);
        var claims = new ClaimStore("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        (await claims.TryBecomeLeaderAsync(ct)).Value.Should().BeTrue("leadership is a sweep precondition");
        var sweeper = BuildSweeper(s3, claims, clock);

        // Act 1 — первый проход
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();

        // Assert 1 — запись OBSERVED с first_seen = now
        var registry = await ReadRegistryAsync();
        registry.Should().NotBeNull();
        var entry = registry!.Orphans.Should().ContainSingle().Subject;
        entry.Prefix.Should().Be("ghost/s1");
        entry.State.Should().Be(OrphanState.Observed);
        entry.Kind.Should().Be("cluster");
        entry.FirstSeenUnix.Should().Be(clock.Now.ToUnixTimeSeconds());

        // Act 2 — второй проход через минуту (сжатое время)
        clock.Now = clock.Now.AddMinutes(1);
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();

        // Assert 2 — first_seen ПЕРЕнесён (не сброшен на now)
        var after = (await ReadRegistryAsync())!.Orphans.Should().ContainSingle().Subject;
        after.FirstSeenUnix.Should().Be(entry.FirstSeenUnix, "TTL from the first observation");
        after.SizeBytes.Should().Be(entry.SizeBytes);
    }

    // AAA (AC7): TTL истёк (сжатое время) → DELETING → объекты удалены → запись удалена
    [Fact]
    public async Task SweepTtlExpiredRemovesPrefixAndRecord()
    {
        // Arrange — сирота в реестре с first_seen 2 минуты назад (ttl 60 c)
        var ct = TestContext.Current.CancellationToken;
        var s3 = new FakeBackupS3();
        var clock = await SeedAsync(s3);
        var claims = new ClaimStore("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        (await claims.TryBecomeLeaderAsync(ct)).Value.Should().BeTrue();
        // сжатый TTL 60 c — как в E2E-сценарии (сжатое время)
        var sweeper = BuildSweeper(s3, claims, clock, Options(ttl: 60));
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue(); // сирота в реестре

        // Act — время вперёд за TTL; проход
        clock.Now = clock.Now.AddMinutes(2);
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();

        // Assert — объекты префикса удалены, записи в реестре нет
        s3.DeletedKeys.Should().Contain("ghost/s1/full/20260911110000Z/base.tar");
        s3.PrefixObjects.Should().BeEmpty();
        var registry = await ReadRegistryAsync();
        registry!.Orphans.Should().BeEmpty("the removal is confirmed — the record is retired");
    }

    // AAA (AC7): OrphanTtlSec=0 — реестр живёт, удаления нет
    [Fact]
    public async Task SweepTtlZeroRegistryOnly()
    {
        // Arrange — ttl=0 (только алерт); сирота давно
        var ct = TestContext.Current.CancellationToken;
        var s3 = new FakeBackupS3();
        var clock = await SeedAsync(s3);
        var claims = new ClaimStore("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        (await claims.TryBecomeLeaderAsync(ct)).Value.Should().BeTrue();
        var sweeper = BuildSweeper(s3, claims, clock, Options(ttl: 0));
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();
        clock.Now = clock.Now.AddDays(30);

        // Act
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();

        // Assert — объекты живы; запись осталась OBSERVED (авто-удаление выключено)
        s3.DeletedKeys.Should().BeEmpty("OrphanTtlSec=0 — only the panel alert");
        (await ReadRegistryAsync())!.Orphans.Should().ContainSingle();
    }

    // AAA (AC7): воскресение владельца (шард снова заявлен в /clusters/) — запись гаснет
    [Fact]
    public async Task SweepOwnerResurrectedRecordRetired()
    {
        // Arrange — сирота ghost/s1 в реестре; затем кластер ghost/shard s1 заявлен
        var ct = TestContext.Current.CancellationToken;
        var s3 = new FakeBackupS3();
        var clock = await SeedAsync(s3);
        var claims = new ClaimStore("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        (await claims.TryBecomeLeaderAsync(ct)).Value.Should().BeTrue();
        var sweeper = BuildSweeper(s3, claims, clock);
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();
        (await ReadRegistryAsync())!.Orphans.Should().ContainSingle();
        // владелец воскрес: /clusters/ghost/... с шардом s1 (декларация — ключи etcd)
        await fixture.Gateway.PutAsync(fixture.Endpoint, "/clusters/ghost/config",
            """{"buckets":1,"dbname":"ghost","created_unix":1757100000}""", null, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, "/clusters/ghost/shards/s1/replicas",
            "2", null, ct);

        // Act
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();

        // Assert — запись гаснет (объекты НЕ удаляются — владелец жив)
        (await ReadRegistryAsync())!.Orphans.Should().BeEmpty("the owner is resurrected — the record is retired");
        s3.DeletedKeys.Should().BeEmpty("the DELETING finalization is cancelled");
    }

    // AAA: не-лидер ничего не пишет (гвард IsLeader в SweepAsync — проверка
    // вызовом без лидерства: ключ реестра не появился)
    [Fact]
    public async Task SweepWithoutLeadershipWritesNothing()
    {
        // Arrange — лидерство НЕ брали (TryBecomeLeaderAsync не звали)
        var ct = TestContext.Current.CancellationToken;
        var s3 = new FakeBackupS3();
        await SeedAsync(s3);
        var claims = new ClaimStore("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        var sweeper = BuildSweeper(s3, claims);

        // Act
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue("non-leader — a silent no-op");

        // Assert — ключа реестра нет, объекты живы
        var kv = await fixture.Gateway.GetAsync(
            fixture.Endpoint, OrphanRegistry.Key, ct);
        kv.Value.Should().BeNull("only the global leader writes the registry");
        s3.DeletedKeys.Should().BeEmpty();
    }

    // ── DR-hold (reliability t04, arch/19 §4) ──

    // Хелпер сида: put hold-ключа/заявки канона (AAA-Act общий).
    private async Task PutHoldAsync(string prefix, long unix = 1757764800)
        => await fixture.Gateway.PutAsync(fixture.Endpoint, OrphanRegistry.HoldKey(prefix),
            OrphanRegistry.HoldToJson(unix, "operator"), null, TestContext.Current.CancellationToken);

    private async Task PutDeleteRequestAsync(string prefix, long unix = 1757764800)
        => await fixture.Gateway.PutAsync(fixture.Endpoint, OrphanRegistry.DeleteKey(prefix),
            OrphanRegistry.DeleteToJson(unix, "operator"), null, TestContext.Current.CancellationToken);

    private async Task<bool> KeyExistsAsync(string key)
        => (await fixture.Gateway.GetAsync(
            fixture.Endpoint, key, TestContext.Current.CancellationToken)).Value is not null;

    // AAA (AC1): сирота с валидным полным (backup_manifest) переживает истёкший
    // TTL — автоправило «последнего полного»: автоматика не удаляет НИКОГДА
    [Fact]
    public async Task SweepFullInPrefixKeepsOrphanAfterTtl()
    {
        // Arrange — сирота с manifest-объектом (критерий валидного полного)
        var ct = TestContext.Current.CancellationToken;
        var s3 = new FakeBackupS3();
        var clock = await SeedAsync(s3);
        s3.PrefixObjects.Add(("ghost/s1/full/20260911110000Z/backup_manifest", 1));
        var claims = new ClaimStore("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        (await claims.TryBecomeLeaderAsync(ct)).Value.Should().BeTrue();
        var sweeper = BuildSweeper(s3, claims, clock, Options(ttl: 60));

        // Act 1 — первый проход
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();

        // Assert 1 — запись OBSERVED с has_valid_full=true
        var entry = (await ReadRegistryAsync())!.Orphans.Should().ContainSingle().Subject;
        entry.Prefix.Should().Be("ghost/s1");
        entry.State.Should().Be(OrphanState.Observed);
        entry.HasValidFull.Should().BeTrue("a manifest in the prefix — a valid full");

        // Act 2 — время вперёд за TTL; проход
        clock.Now = clock.Now.AddMinutes(2);
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();

        // Assert 2 — объекты живы (автоправило), запись осталась OBSERVED с защитой
        s3.DeletedKeys.Should().BeEmpty("an orphan with a valid full is not removed by the automation");
        var after = (await ReadRegistryAsync())!.Orphans.Should().ContainSingle().Subject;
        after.State.Should().Be(OrphanState.Observed);
        after.HasValidFull.Should().BeTrue();
    }

    // AAA (AC2): hold-флаг оператора держит незащищённую сироту от TTL-удаления
    [Fact]
    public async Task SweepHoldKeepsUnprotectedOrphan()
    {
        // Arrange — сирота без полных в реестре; затем hold-ключ
        var ct = TestContext.Current.CancellationToken;
        var s3 = new FakeBackupS3();
        var clock = await SeedAsync(s3);
        var claims = new ClaimStore("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        (await claims.TryBecomeLeaderAsync(ct)).Value.Should().BeTrue();
        var sweeper = BuildSweeper(s3, claims, clock, Options(ttl: 60));
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();
        await PutHoldAsync("ghost/s1");

        // Act — время вперёд за TTL; проход (hold жив)
        clock.Now = clock.Now.AddMinutes(2);
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();

        // Assert — объекты живы, запись жива (hold исключил из TTL-отбора)
        s3.DeletedKeys.Should().BeEmpty("hold keeps the orphan");
        (await ReadRegistryAsync())!.Orphans.Should().ContainSingle(e => e.Prefix == "ghost/s1");
        (await KeyExistsAsync(OrphanRegistry.HoldKey("ghost/s1"))).Should().BeTrue();
    }

    // AAA (AC3): снятие hold (unhold) возвращает сироту под общие правила —
    // истёкший TTL без иных защит → удаление ближайшим проходом
    [Fact]
    public async Task SweepUnholdReturnsUnderTtl()
    {
        // Arrange — сирота без полных; hold; время за TTL — проход держит
        var ct = TestContext.Current.CancellationToken;
        var s3 = new FakeBackupS3();
        var clock = await SeedAsync(s3);
        var claims = new ClaimStore("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        (await claims.TryBecomeLeaderAsync(ct)).Value.Should().BeTrue();
        var sweeper = BuildSweeper(s3, claims, clock, Options(ttl: 60));
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();
        await PutHoldAsync("ghost/s1");
        clock.Now = clock.Now.AddMinutes(2);
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();
        s3.DeletedKeys.Should().BeEmpty("under hold there is no removal");

        // Act — unhold (del hold-ключа) и ещё проход (время не двигаем — TTL истёк)
        await fixture.Gateway.DeleteAsync(
            fixture.Endpoint, OrphanRegistry.HoldKey("ghost/s1"), prefix: false, ct);
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();

        // Assert — объекты удалены, запись погашена
        s3.DeletedKeys.Should().Contain("ghost/s1/full/20260911110000Z/base.tar");
        (await ReadRegistryAsync())!.Orphans.Should().BeEmpty("unhold — removal by the nearest sweep");
    }

    // AAA (AC4): заявка явного удаления минует hold и автоправило; после
    // доводки запись и ключ заявки гасятся вместе
    [Fact]
    public async Task SweepRequestRemovesProtectedAndRetires()
    {
        // Arrange — сирота с manifest И hold; затем заявка delete
        var ct = TestContext.Current.CancellationToken;
        var s3 = new FakeBackupS3();
        var clock = await SeedAsync(s3);
        s3.PrefixObjects.Add(("ghost/s1/full/20260911110000Z/backup_manifest", 1));
        var claims = new ClaimStore("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        (await claims.TryBecomeLeaderAsync(ct)).Value.Should().BeTrue();
        var sweeper = BuildSweeper(s3, claims, clock, Options(ttl: 60));
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();
        await PutHoldAsync("ghost/s1");
        await PutDeleteRequestAsync("ghost/s1");

        // Act — время вперёд; проход (заявка приоритетнее защит)
        clock.Now = clock.Now.AddMinutes(2);
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();

        // Assert — объекты удалены, записи нет, ключа заявки нет
        s3.DeletedKeys.Should().Contain("ghost/s1/full/20260911110000Z/base.tar")
            .And.Contain("ghost/s1/full/20260911110000Z/backup_manifest");
        (await ReadRegistryAsync())!.Orphans.Should().BeEmpty("the request removes the protected orphan");
        (await KeyExistsAsync(OrphanRegistry.DeleteKey("ghost/s1"))).Should()
            .BeFalse("the request is retired together with the record");
    }

    // AAA (AC5): воскресший владелец гасит и запись, и hold, и заявку —
    // удаление живого префикса не происходит
    [Fact]
    public async Task SweepResurrectedOwnerRetiresHoldAndRequest()
    {
        // Arrange — сирота без полных в реестре; hold + заявка; затем воскресение
        var ct = TestContext.Current.CancellationToken;
        var s3 = new FakeBackupS3();
        var clock = await SeedAsync(s3);
        var claims = new ClaimStore("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        (await claims.TryBecomeLeaderAsync(ct)).Value.Should().BeTrue();
        var sweeper = BuildSweeper(s3, claims, clock, Options(ttl: 60));
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();
        await PutHoldAsync("ghost/s1");
        await PutDeleteRequestAsync("ghost/s1");
        // владелец воскрес: /clusters/ghost/... с шардом s1 (декларация — ключи etcd)
        await fixture.Gateway.PutAsync(fixture.Endpoint, "/clusters/ghost/config",
            """{"buckets":1,"dbname":"ghost","created_unix":1757100000}""", null, ct);
        await fixture.Gateway.PutAsync(fixture.Endpoint, "/clusters/ghost/shards/s1/replicas",
            "2", null, ct);

        // Act
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();

        // Assert — записи/hold/заявки нет, объекты живы (удаление живого исключено)
        (await ReadRegistryAsync())!.Orphans.Should().BeEmpty("the owner is resurrected — the record is retired");
        (await KeyExistsAsync(OrphanRegistry.HoldKey("ghost/s1"))).Should().BeFalse("hold is retired by the sweeper");
        (await KeyExistsAsync(OrphanRegistry.DeleteKey("ghost/s1"))).Should().BeFalse("the request is retired by the sweeper");
        s3.DeletedKeys.Should().BeEmpty("a live prefix is not removed");
    }

    // AAA (AC5/спека §2 п.6): санитар — hold/заявки с префиксом вне merged-реестра
    // (несирота) гасятся, «висящих» флагов не копится
    [Fact]
    public async Task SweepSweeperCleansForeignKeys()
    {
        // Arrange — S3 пуст (реестр после merge пуст); hold+заявка на чужой other/s9
        var ct = TestContext.Current.CancellationToken;
        var s3 = new FakeBackupS3();
        var clock = await SeedAsync(s3);
        s3.PrefixObjects.Clear(); // ghost/s1 пуст в S3 — в реестр не попадёт
        var claims = new ClaimStore("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        (await claims.TryBecomeLeaderAsync(ct)).Value.Should().BeTrue();
        var sweeper = BuildSweeper(s3, claims, clock, Options(ttl: 60));
        await PutHoldAsync("other/s9");
        await PutDeleteRequestAsync("other/s9");

        // Act — один проход
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();

        // Assert — оба ключа удалены (санитар)
        (await KeyExistsAsync(OrphanRegistry.HoldKey("other/s9"))).Should().BeFalse();
        (await KeyExistsAsync(OrphanRegistry.DeleteKey("other/s9"))).Should().BeFalse();
        s3.DeletedKeys.Should().BeEmpty();
    }

    // AAA (AC1, transient list): ненаблюдаемый сирота сохраняет has_valid_full —
    // защита переживает моргание list bucket
    [Fact]
    public async Task SweepUnobservedOrphanKeepsHasValidFull()
    {
        // Arrange — сирота с manifest; первый проход фиксирует защиту
        var ct = TestContext.Current.CancellationToken;
        var s3 = new FakeBackupS3();
        var clock = await SeedAsync(s3);
        s3.PrefixObjects.Add(("ghost/s1/full/20260911110000Z/backup_manifest", 1));
        var claims = new ClaimStore("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        (await claims.TryBecomeLeaderAsync(ct)).Value.Should().BeTrue();
        var sweeper = BuildSweeper(s3, claims, clock, Options(ttl: 60));
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();
        (await ReadRegistryAsync())!.Orphans.Single().HasValidFull.Should().BeTrue();

        // Act — list пуст (transient); второй проход
        s3.PrefixObjects.Clear();
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();

        // Assert — запись жива, HasValidFull остался true, удаления нет
        var entry = (await ReadRegistryAsync())!.Orphans.Should().ContainSingle().Subject;
        entry.HasValidFull.Should().BeTrue("the protection survives a transient list");
        entry.State.Should().Be(OrphanState.Observed);
        s3.DeletedKeys.Should().BeEmpty("no candidate — no removal");
    }

    // AAA (t36, arch/19 §5): дедуп union-листинга — по (пул, ключ): одинаковый
    // ключ в двух пулах учитывается КАЖДОЙ пул-копией (размер реестра = сумма
    // по пулам); дедуп по голому ключу схлопывал бы межпулевые дубли
    [Fact]
    public async Task SweepKeyInTwoPoolsCountedTwice()
    {
        // Arrange — сирота ghost/s1 (объект 100 байт); оба пула листят ОДИН фейк
        var ct = TestContext.Current.CancellationToken;
        var s3 = new FakeBackupS3();
        var clock = await SeedAsync(s3);
        var claims = new ClaimStore("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        (await claims.TryBecomeLeaderAsync(ct)).Value.Should().BeTrue();
        var (factory, store) = FakePoolFactory.TwoPoolsSameTarget(s3);
        var sweeper = new BackupOrphanSweeper(fixture.Gateway, [fixture.Endpoint],
            factory, store, claims,
            () => Options(), clock);

        // Act — один проход
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();

        // Assert — запись одна (префикс общий), размер = 100+100: обе пул-копии в union
        var entry = (await ReadRegistryAsync())!.Orphans.Should().ContainSingle().Subject;
        entry.Prefix.Should().Be("ghost/s1");
        entry.SizeBytes.Should().Be(200, "dedup by (pool, key) — a cross-pool duplicate is not collapsed");
    }

    // ── Перепроверка перед удалением (arch/19 §4): окно гонки снимок→удаление ──

    // Декоратор gateway — инъекция события в окно гонки свипера: подмешивает
    // ключи в результат range-вызова с заданным префиксом после skip совпавших
    // вызовов (снимок /clusters/ шага (2) остаётся чистым — воскресение/hold
    // видит только перепроверка перед удалением (4.5)). Реальный etcd не трогает.
    private sealed class InjectingGateway(IEtcdGateway inner) : IEtcdGateway
    {
        private sealed class Injection(int skip, List<Kv> kvs)
        {
            public int Skip { get; } = skip;
            public List<Kv> Kvs { get; } = kvs;
            public int Seen { get; set; }
            public bool Fired { get; set; }
        }

        private readonly Dictionary<string, Injection> pending = new(StringComparer.Ordinal);

        // Первые skip совпавших range-вызовов — чистые, следующий — с ключами.
        public void InjectAfterRanges(string prefix, int skip, params (string Key, string Value)[] kvs)
            => pending[prefix] = new Injection(skip,
                kvs.Select(kv => new Kv(kv.Key, kv.Value, 1)).ToList());

        public async Task<Result<IReadOnlyList<Kv>>> RangeAsync(
            string endpoint, string prefix, CancellationToken ct)
        {
            var result = await inner.RangeAsync(endpoint, prefix, ct);
            if (!pending.TryGetValue(prefix, out var injection) || !result.IsSuccess)
                return result;
            injection.Seen++;
            if (injection.Fired || injection.Seen <= injection.Skip)
                return result;
            injection.Fired = true;
            return Result<IReadOnlyList<Kv>>.Success(
                result.Value.Concat(injection.Kvs).ToList());
        }

        public Task<Result<Kv?>> GetAsync(string endpoint, string key, CancellationToken ct)
            => inner.GetAsync(endpoint, key, ct);

        public Task<Result> PutAsync(
            string endpoint, string key, string value, long? lease, CancellationToken ct)
            => inner.PutAsync(endpoint, key, value, lease, ct);

        public Task<Result> DeleteAsync(
            string endpoint, string keyOrPrefix, bool prefix, CancellationToken ct)
            => inner.DeleteAsync(endpoint, keyOrPrefix, prefix, ct);

        public Task<Result<TxnResult>> TxnAsync(string endpoint, TxnRequest req, CancellationToken ct)
            => inner.TxnAsync(endpoint, req, ct);

        public Task<Result<long>> LeaseGrantAsync(string endpoint, int ttlSec, CancellationToken ct)
            => inner.LeaseGrantAsync(endpoint, ttlSec, ct);

        public Task<Result> LeaseRevokeAsync(string endpoint, long lease, CancellationToken ct)
            => inner.LeaseRevokeAsync(endpoint, lease, ct);

        public Task<Result> LeaseKeepaliveAsync(string endpoint, long lease, CancellationToken ct)
            => inner.LeaseKeepaliveAsync(endpoint, lease, ct);

        public Task<Result<byte[]>> SnapshotSaveAsync(string endpoint, CancellationToken ct)
            => inner.SnapshotSaveAsync(endpoint, ct);

        public Task<Result<EtcdStatusPayload>> StatusAsync(string endpoint, CancellationToken ct)
            => inner.StatusAsync(endpoint, ct);

        public Task<Result<IReadOnlyList<EtcdMember>>> MemberListAsync(string endpoint, CancellationToken ct)
            => inner.MemberListAsync(endpoint, ct);

        public Task<Result<IReadOnlyList<EtcdAlarm>>> AlarmAsync(string endpoint, CancellationToken ct)
            => inner.AlarmAsync(endpoint, ct);

        public Task<Result> CompactAsync(string endpoint, long revision, CancellationToken ct)
            => inner.CompactAsync(endpoint, revision, ct);

        public Task<Result> DefragmentAsync(string endpoint, CancellationToken ct)
            => inner.DefragmentAsync(endpoint, ct);
    }

    // AAA (arch/19 §4): воскресение владельца, видимое ТОЛЬКО перепроверкой
    // (снимок /clusters/ шага (2) чист), отменяет и DELETING-доводку: объекты
    // живого владельца не трогаются, гасятся запись реестра, hold и заявка
    [Fact]
    public async Task SweepOwnerResurrectedAfterSnapshotCancelsDeleting()
    {
        // Arrange — DELETING-запись в реестре (идущая доводка); hold + заявка;
        // воскресение инъецируется на перепроверке /clusters/ (2-й range-вызов)
        var ct = TestContext.Current.CancellationToken;
        var s3 = new FakeBackupS3();
        var clock = await SeedAsync(s3);
        var claims = new ClaimStore("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        (await claims.TryBecomeLeaderAsync(ct)).Value.Should().BeTrue();
        var gateway = new InjectingGateway(fixture.Gateway);
        var sweeper = BuildSweeper(s3, claims, clock, gateway: gateway);
        var deleting = new OrphanRegistry.Registry(
        [
            new OrphanEntry("ghost/s1", "cluster", 100,
                clock.Now.AddMinutes(-2).ToUnixTimeSeconds(), OrphanState.Deleting),
        ], clock.Now.ToUnixTimeSeconds());
        await fixture.Gateway.PutAsync(fixture.Endpoint, OrphanRegistry.Key,
            OrphanRegistry.ToJson(deleting), null, ct);
        await PutHoldAsync("ghost/s1");
        await PutDeleteRequestAsync("ghost/s1");
        gateway.InjectAfterRanges("/clusters/", skip: 1,
            ("/clusters/ghost/config", """{"buckets":1,"dbname":"ghost","created_unix":1757100000}"""),
            ("/clusters/ghost/shards/s1/replicas", "2"));

        // Act — снимок чист (владелец «мёртв») → доводка кандидата; перепроверка
        // видит воскресшего — удаление объектов отменяется
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();

        // Assert — объекты живы; запись/hold/заявка погашены
        s3.DeletedKeys.Should().BeEmpty("a resurrected owner cancels the deletion");
        (await ReadRegistryAsync())!.Orphans.Should()
            .BeEmpty("the record is retired, not the objects");
        (await KeyExistsAsync(OrphanRegistry.HoldKey("ghost/s1"))).Should()
            .BeFalse("the hold is retired with the cancelled deletion");
        (await KeyExistsAsync(OrphanRegistry.DeleteKey("ghost/s1"))).Should()
            .BeFalse("the request is retired with the cancelled deletion");
    }

    // AAA (arch/19 §4): hold, появившийся ПОСЛЕ TTL-отбора (видим только
    // перепроверкой), откладывает удаление: объекты живы, запись остаётся OBSERVED
    [Fact]
    public async Task SweepHoldAppearedAfterSelectionDelaysRemoval()
    {
        // Arrange — сирота OBSERVED; инъекция hold-ключа на перепроверке (1-й
        // range по точному hold-ключу прохода — она и есть)
        var ct = TestContext.Current.CancellationToken;
        var s3 = new FakeBackupS3();
        var clock = await SeedAsync(s3);
        var claims = new ClaimStore("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        (await claims.TryBecomeLeaderAsync(ct)).Value.Should().BeTrue();
        var gateway = new InjectingGateway(fixture.Gateway);
        var sweeper = BuildSweeper(s3, claims, clock, Options(ttl: 60), gateway);
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();
        (await ReadRegistryAsync())!.Orphans.Should()
            .ContainSingle(e => e.State == OrphanState.Observed);
        gateway.InjectAfterRanges(OrphanRegistry.HoldKey("ghost/s1"), skip: 0,
            (OrphanRegistry.HoldKey("ghost/s1"), OrphanRegistry.HoldToJson(1757764800, "operator")));
        clock.Now = clock.Now.AddMinutes(2);

        // Act — TTL истёк, отбор выбирает кандидата; перепроверка видит hold
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();

        // Assert — объекты живы, запись осталась OBSERVED (удаление отложено)
        s3.DeletedKeys.Should().BeEmpty("a hold appeared after selection — removal is delayed");
        (await ReadRegistryAsync())!.Orphans.Should()
            .ContainSingle(e => e.Prefix == "ghost/s1" && e.State == OrphanState.Observed);
    }
}
