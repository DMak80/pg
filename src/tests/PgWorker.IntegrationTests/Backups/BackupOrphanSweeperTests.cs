using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Supervisor;
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
        S3Endpoint: "http://minio",
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
        FakeBackupS3 s3, ClaimStore claims, MutableClock? clock = null, BackupsRuntimeOptions? options = null)
        => new(fixture.Gateway, [fixture.Endpoint], s3, claims,
            new WorkJournal("/pgworker", fixture.Gateway, [fixture.Endpoint]),
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
    public async Task Проход_сирота_в_реестре_first_seen_переносится()
    {
        // Arrange — лидер взят; сирота ghost/s1; объектов владельца в /clusters/ нет
        var ct = TestContext.Current.CancellationToken;
        var s3 = new FakeBackupS3();
        var clock = await SeedAsync(s3);
        var claims = new ClaimStore("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        (await claims.TryBecomeLeaderAsync(ct)).Value.Should().BeTrue("лидерство — предусловие прохода");
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
        after.FirstSeenUnix.Should().Be(entry.FirstSeenUnix, "TTL от первого наблюдения");
        after.SizeBytes.Should().Be(entry.SizeBytes);
    }

    // AAA (AC7): TTL истёк (сжатое время) → DELETING → объекты удалены → запись удалена
    [Fact]
    public async Task Проход_TTL_истёк_удаляет_префикс_и_запись()
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
        registry!.Orphans.Should().BeEmpty("удаление подтверждено — запись погашена");
    }

    // AAA (AC7): OrphanTtlSec=0 — реестр живёт, удаления нет
    [Fact]
    public async Task Проход_TtlZero_только_реестр()
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
        s3.DeletedKeys.Should().BeEmpty("OrphanTtlSec=0 — только алерт панели");
        (await ReadRegistryAsync())!.Orphans.Should().ContainSingle();
    }

    // AAA (AC7): воскресение владельца (шард снова заявлен в /clusters/) — запись гаснет
    [Fact]
    public async Task Проход_владелец_воскрес_запись_гаснет()
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
        (await ReadRegistryAsync())!.Orphans.Should().BeEmpty("владелец воскрес — запись гаснет");
        s3.DeletedKeys.Should().BeEmpty("доводка DELETING отменяется");
    }

    // AAA: не-лидер ничего не пишет (гвард IsLeader в SweepAsync — проверка
    // вызовом без лидерства: ключ реестра не появился)
    [Fact]
    public async Task Проход_без_лидерства_не_пишет()
    {
        // Arrange — лидерство НЕ брали (TryBecomeLeaderAsync не звали)
        var ct = TestContext.Current.CancellationToken;
        var s3 = new FakeBackupS3();
        await SeedAsync(s3);
        var claims = new ClaimStore("/pgworker", [fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        var sweeper = BuildSweeper(s3, claims);

        // Act
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue("не-лидер — тихий no-op");

        // Assert — ключа реестра нет, объекты живы
        var kv = await fixture.Gateway.GetAsync(
            fixture.Endpoint, OrphanRegistry.Key, ct);
        kv.Value.Should().BeNull("реестр пишет только глобальный лидер");
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
    public async Task Проход_полный_в_префиксе_держит_сироту_после_TTL()
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
        entry.HasValidFull.Should().BeTrue("manifest в префиксе — валидный полный");

        // Act 2 — время вперёд за TTL; проход
        clock.Now = clock.Now.AddMinutes(2);
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();

        // Assert 2 — объекты живы (автоправило), запись осталась OBSERVED с защитой
        s3.DeletedKeys.Should().BeEmpty("сирота с валидным полным автоматикой не удаляется");
        var after = (await ReadRegistryAsync())!.Orphans.Should().ContainSingle().Subject;
        after.State.Should().Be(OrphanState.Observed);
        after.HasValidFull.Should().BeTrue();
    }

    // AAA (AC2): hold-флаг оператора держит незащищённую сироту от TTL-удаления
    [Fact]
    public async Task Проход_hold_держит_незащищённую_сироту()
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
        s3.DeletedKeys.Should().BeEmpty("hold держит сироту");
        (await ReadRegistryAsync())!.Orphans.Should().ContainSingle(e => e.Prefix == "ghost/s1");
        (await KeyExistsAsync(OrphanRegistry.HoldKey("ghost/s1"))).Should().BeTrue();
    }

    // AAA (AC3): снятие hold (unhold) возвращает сироту под общие правила —
    // истёкший TTL без иных защит → удаление ближайшим проходом
    [Fact]
    public async Task Проход_unhold_возвращает_под_TTL()
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
        s3.DeletedKeys.Should().BeEmpty("под hold удаления нет");

        // Act — unhold (del hold-ключа) и ещё проход (время не двигаем — TTL истёк)
        await fixture.Gateway.DeleteAsync(
            fixture.Endpoint, OrphanRegistry.HoldKey("ghost/s1"), prefix: false, ct);
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue();

        // Assert — объекты удалены, запись погашена
        s3.DeletedKeys.Should().Contain("ghost/s1/full/20260911110000Z/base.tar");
        (await ReadRegistryAsync())!.Orphans.Should().BeEmpty("unhold — удаление ближайшим проходом");
    }

    // AAA (AC4): заявка явного удаления минует hold и автоправило; после
    // доводки запись и ключ заявки гасятся вместе
    [Fact]
    public async Task Проход_заявка_удаляет_защищённую_и_гасится()
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
        (await ReadRegistryAsync())!.Orphans.Should().BeEmpty("заявка удаляет защищённую сироту");
        (await KeyExistsAsync(OrphanRegistry.DeleteKey("ghost/s1"))).Should()
            .BeFalse("заявка гасится вместе с записью");
    }

    // AAA (AC5): воскресший владелец гасит и запись, и hold, и заявку —
    // удаление живого префикса не происходит
    [Fact]
    public async Task Проход_воскресший_владелец_гасит_hold_и_заявку()
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
        (await ReadRegistryAsync())!.Orphans.Should().BeEmpty("владелец воскрес — запись гаснет");
        (await KeyExistsAsync(OrphanRegistry.HoldKey("ghost/s1"))).Should().BeFalse("hold гасит санитар");
        (await KeyExistsAsync(OrphanRegistry.DeleteKey("ghost/s1"))).Should().BeFalse("заявку гасит санитар");
        s3.DeletedKeys.Should().BeEmpty("живой префикс не удаляется");
    }

    // AAA (AC5/спека §2 п.6): санитар — hold/заявки с префиксом вне merged-реестра
    // (несирота) гасятся, «висящих» флагов не копится
    [Fact]
    public async Task Проход_санитар_чистит_чужие_ключи()
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
    public async Task Проход_ненаблюдаемый_сирота_сохраняет_has_valid_full()
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
        entry.HasValidFull.Should().BeTrue("защита переживает transient list");
        entry.State.Should().Be(OrphanState.Observed);
        s3.DeletedKeys.Should().BeEmpty("кандидата нет — удаления нет");
    }
}
