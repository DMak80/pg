using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Supervisor;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Чистые функции реестра сирот (t07, arch/19 §4): группировка по префиксам
// нашей формы, merge с переносом first_seen, гвард воскресения, TTL-отбор,
// JSON roundtrip формата ключа /pgworker/backups/orphans.
public class OrphanRegistryTests
{
    private static readonly DateTimeOffset NowT = new(2026, 9, 13, 12, 0, 0, TimeSpan.Zero);

    private static long Unix(DateTimeOffset t) => t.ToUnixTimeSeconds();

    private static S3ObjectInfo Obj(string key, long size = 10) => new(key, size, NowT);

    private static IReadOnlySet<string> Clusters(params string[] names)
        => new HashSet<string>(names, StringComparer.Ordinal);

    private static IReadOnlySet<(string, string)> Shards(params (string, string)[] pairs)
        => new HashSet<(string, string)>(pairs);

    private const long Now = 1757764800; // 2026-09-13 12:00:00 UTC

    // AAA (AC7): группировка — посторонние корневые объекты вне реестра
    [Fact]
    public void GroupShardPrefixes_только_нашей_формы()
    {
        // Arrange — объекты c1/s1/full/A/x, c1/s1/wal/seg, чужой root.txt
        var objects = new List<S3ObjectInfo>
        {
            Obj("c1/s1/full/20260911110000Z/base.tar", 30),
            Obj("c1/s1/wal/000000010000000000000001", 16),
            Obj("root.txt", 5),
            Obj("c1/UPPER/wal/x", 7), // имя не нашей формы
        };

        // Act
        var grouped = OrphanRegistry.GroupShardPrefixes(objects);

        // Assert — словарь {["c1/s1"]=сумма}, чужие ключи не сгруппированы
        grouped.Should().HaveCount(1);
        grouped["c1/s1"].Should().Be(46);
    }

    // AAA (AC7): merge переносит first_seen (TTL от первого наблюдения)
    [Fact]
    public void Merge_ПереноситFirstSeen()
    {
        // Arrange — current: c2/s3 OBSERVED first_seen=T0; observed: c2/s3 новый size
        var current = new OrphanRegistry.Registry(
            [new OrphanEntry("c2/s3", "cluster", 100, Unix(NowT.AddDays(-2)), OrphanState.Observed)],
            Unix(NowT.AddDays(-1)));
        var observed = new Dictionary<string, long> { ["c2/s3"] = 150 };

        // Act
        var merged = OrphanRegistry.Merge(current, observed, Shards(), Clusters(), Now);

        // Assert — first_seen=T0, size обновлён, state OBSERVED
        merged.Orphans.Should().ContainSingle();
        var e = merged.Orphans[0];
        e.FirstSeenUnix.Should().Be(Unix(NowT.AddDays(-2)), "TTL от первого наблюдения");
        e.SizeBytes.Should().Be(150);
        e.State.Should().Be(OrphanState.Observed);
        e.Kind.Should().Be("cluster", "кластера c2 нет в live → сирота уровня кластера");
    }

    // AAA (AC7): воскресение владельца гасит запись (и отменяет DELETING)
    [Fact]
    public void Merge_ВоскресшийВладелец_ЗаписьГаснет()
    {
        // Arrange — DELETING-запись c2/s3; владелец воскрес (шард жив)
        var current = new OrphanRegistry.Registry(
            [new OrphanEntry("c2/s3", "cluster", 100, Unix(NowT.AddDays(-9)), OrphanState.Deleting)],
            Now);
        var observed = new Dictionary<string, long> { ["c2/s3"] = 100 };

        // Act
        var merged = OrphanRegistry.Merge(
            current, observed, Shards(("c2", "s3")), Clusters("c2"), Now);

        // Assert — записи нет (доводка отменена)
        merged.Orphans.Should().BeEmpty("владелец появился в etcd — запись гаснет");
    }

    // AAA (AC7): TTL-кандидат — DELETING-доводка приоритетна; без неё —
    // просроченный OBSERVED; ttl=0 → null при отсутствии DELETING
    [Fact]
    public void SelectTtlCandidate_ДоводкаПриоритетна_ПотомTtl()
    {
        // Arrange — OBSERVED просрочен (8 сут при ttl 7 сут) + DELETING-запись
        var registry = new OrphanRegistry.Registry(
        [
            new OrphanEntry("c1/s1", "cluster", 10, Now - 8 * 86400, OrphanState.Observed),
            new OrphanEntry("c2/s2", "cluster", 20, Now - 9 * 86400, OrphanState.Deleting),
        ], Now);

        // Act / Assert — DELETING приоритетна
        OrphanRegistry.SelectTtlCandidate(registry, 7 * 86400, Now).Should().Be("c2/s2");

        // только OBSERVED: старейший просроченный (один)
        var observedOnly = new OrphanRegistry.Registry([registry.Orphans[0]], Now);
        OrphanRegistry.SelectTtlCandidate(observedOnly, 7 * 86400, Now).Should().Be("c1/s1");

        // ttl=0 → авто-удаление выключено: без DELETING кандидата нет
        OrphanRegistry.SelectTtlCandidate(observedOnly, 0, Now).Should().BeNull();

        // не истёкший OBSERVED — кандидата нет
        var fresh = new OrphanRegistry.Registry(
            [new OrphanEntry("c1/s1", "cluster", 10, Now - 3600, OrphanState.Observed)], Now);
        OrphanRegistry.SelectTtlCandidate(fresh, 7 * 86400, Now).Should().BeNull();
    }

    // AAA (AC8): JSON roundtrip симметричен (формат arch/19 §4); битый JSON → null
    [Fact]
    public void ToJsonParse_Roundtrip()
    {
        // Arrange
        var registry = new OrphanRegistry.Registry(
        [
            new OrphanEntry("ghost1/s1", "cluster", 123, 1757100000, OrphanState.Observed),
            new OrphanEntry("ghost2/s2", "shard", 456, 1757000000, OrphanState.Deleting),
        ], Now);

        // Act
        var json = OrphanRegistry.ToJson(registry);
        var parsed = OrphanRegistry.Parse(json);

        // Assert — записи равны; ключ формата канона
        parsed.Should().NotBeNull();
        OrphanRegistry.SameOrphans(registry, parsed!).Should().BeTrue();
        parsed.UpdatedUnix.Should().Be(Now);
        json.Should().StartWith("{\"orphans\":[{").And.Contain("\"prefix\":\"ghost1/s1\"")
            .And.Contain("\"first_seen_unix\":1757100000").And.Contain("\"updated_unix\":")
            .And.Contain("\"state\":\"DELETING\"");
        json.Should().NotContain("\"UpdatedUnix\"");

        // битый JSON / неизвестный state → null
        OrphanRegistry.Parse("{не json").Should().BeNull();
        OrphanRegistry.Parse("""{"orphans":[{"prefix":"a/b","kind":"shard","size_bytes":1,"first_seen_unix":1,"state":"???"}],"updated_unix":1}""")
            .Should().BeNull("неизвестный state — битый ключ");
    }

    // AAA: SameOrphans — updated_unix не входит в сравнение (put только при
    // изменении записей)
    [Fact]
    public void SameOrphans_UpdatedUnix_Не_Входит()
    {
        // Arrange
        var orphans = new List<OrphanEntry>
        {
            new("g1/s1", "cluster", 10, Now, OrphanState.Observed),
        };
        var a = new OrphanRegistry.Registry(orphans, Now);
        var b = new OrphanRegistry.Registry(orphans, Now + 600);

        // Act / Assert
        OrphanRegistry.SameOrphans(a, b).Should().BeTrue("различие только в updated_unix");
        var changed = new OrphanRegistry.Registry(
        [new OrphanEntry("g1/s1", "cluster", 11, Now, OrphanState.Observed)], Now);
        OrphanRegistry.SameOrphans(a, changed).Should().BeFalse("size изменился — нужен put");
    }
}
