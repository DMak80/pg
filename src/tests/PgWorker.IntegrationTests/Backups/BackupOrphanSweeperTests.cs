using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Supervisor;
using Shared.Etcd.Client;
using PgWorker.Etcd.Coordination;
using PgWorker.IntegrationTests.Etcd;
using Xunit;

namespace PgWorker.IntegrationTests.Backups;

// Интеграции BackupOrphanSweeper (t07, spec §3.4 глобальная часть): реальный
// etcd (реестр/лидерство/кластеры) + FakeBackupS3 + MutableClock (сжатое TTL).
[Collection(EtcdCollection.Name)]
public class BackupOrphanSweeperTests(EtcdFixture fixture)
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
        await fixture.Gateway.DeleteAsync(fixture.Endpoint, $"/clusters/{cluster}/", prefix: true, ct);
        var clock = new MutableClock();
        s3.PrefixObjects.Add(($"{cluster}/s1/full/20260911110000Z/base.tar", 100));
        return clock;
    }

    private BackupOrphanSweeper BuildSweeper(
        FakeBackupS3 s3, ClaimStore claims, MutableClock? clock = null, BackupsRuntimeOptions? options = null)
        => new(fixture.Gateway, [fixture.Endpoint], s3, claims,
            new WorkJournal(fixture.Gateway, [fixture.Endpoint]),
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
        var claims = new ClaimStore([fixture.Endpoint], fixture.Gateway, TimeProvider.System);
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
        var claims = new ClaimStore([fixture.Endpoint], fixture.Gateway, TimeProvider.System);
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
        var claims = new ClaimStore([fixture.Endpoint], fixture.Gateway, TimeProvider.System);
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
        var claims = new ClaimStore([fixture.Endpoint], fixture.Gateway, TimeProvider.System);
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
        var claims = new ClaimStore([fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        var sweeper = BuildSweeper(s3, claims);

        // Act
        (await sweeper.SweepAsync(ct)).IsSuccess.Should().BeTrue("не-лидер — тихий no-op");

        // Assert — ключа реестра нет, объекты живы
        var kv = await fixture.Gateway.GetAsync(
            fixture.Endpoint, OrphanRegistry.Key, ct);
        kv.Value.Should().BeNull("реестр пишет только глобальный лидер");
        s3.DeletedKeys.Should().BeEmpty();
    }
}
