using FluentAssertions;
using PgWorker.Backups;
using PgWorker.Backups.Supervisor;
using Xunit;

namespace PgWorker.UnitTests.Backups;

// Чистые функции реестра сирот (t07, arch/19 §4): группировка по префиксам
// нашей формы (с детектом валидных полных — DR-hold, reliability t04), merge
// с переносом first_seen/has_valid_full, гвард воскресения, TTL-отбор с
// hold-множеством, JSON roundtrip формата ключа /pgworker/backups/orphans,
// ключи/JSON hold-заявок канона.
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

    // AAA (AC7): группировка — посторонние корневые объекты вне реестра;
    // размеры по всем префиксам нашей формы
    [Fact]
    public void GroupPrefixes_только_нашей_формы_и_детект_полных()
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
        var grouped = OrphanRegistry.GroupPrefixes(objects);

        // Assert — размеры {["c1/s1"]=сумма}, чужие ключи не сгруппированы
        grouped.Sizes.Should().HaveCount(1);
        grouped.Sizes["c1/s1"].Should().Be(46);
        grouped.FullPrefixes.Should().BeEmpty("manifest-критерия здесь нет");
    }

    // AAA (AC1/AC9): детект валидных полных из list-ключей — только
    // full/<id>/backup_manifest (5 сегментов, 3-й full, последний backup_manifest)
    [Fact]
    public void GroupPrefixes_детектирует_полные_по_manifest()
    {
        // Arrange — в префиксе c1/s1 есть manifest, в c2/s2 — только base.tar
        var objects = new List<S3ObjectInfo>
        {
            Obj("c1/s1/full/20260911110000Z/backup_manifest"),
            Obj("c1/s1/full/20260911110000Z/base.tar", 30),
            Obj("c2/s2/full/20260911110000Z/base.tar", 20),
            Obj("c3/s3/full/20260911110000Z/backup_manifest.extra"), // не manifest
            Obj("c3/s3/fullx/20260911110000Z/backup_manifest"),      // 3-й сегмент не full
            Obj("root/full/20260911110000Z/backup_manifest"),        // имя не нашей формы
        };

        // Act
        var grouped = OrphanRegistry.GroupPrefixes(objects);

        // Assert — полные только в c1/s1; размеры по всем префиксам нашей формы
        grouped.FullPrefixes.Should().BeEquivalentTo(["c1/s1"]);
        grouped.Sizes["c1/s1"].Should().Be(40);
        grouped.Sizes.Should().ContainKey("c2/s2");
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
        var merged = OrphanRegistry.Merge(
            current, observed, new HashSet<string>(), Shards(), Clusters(), Now);

        // Assert — first_seen=T0, size обновлён, state OBSERVED
        merged.Orphans.Should().ContainSingle();
        var e = merged.Orphans[0];
        e.FirstSeenUnix.Should().Be(Unix(NowT.AddDays(-2)), "TTL от первого наблюдения");
        e.SizeBytes.Should().Be(150);
        e.State.Should().Be(OrphanState.Observed);
        e.Kind.Should().Be("cluster", "кластера c2 нет в live → сирота уровня кластера");
    }

    // AAA (AC1): merge — has_valid_full из свежего наблюдения; ненаблюдаемый
    // сирота сохраняет прежнее значение (transient list не роняет защиту)
    [Fact]
    public void Merge_HasValidFull_свежее_наблюдение_и_перенос()
    {
        // Arrange — current: две записи, обе с has_valid_full=true
        var current = new OrphanRegistry.Registry(
        [
            new OrphanEntry("c1/s1", "cluster", 10, Now - 86400, OrphanState.Observed, HasValidFull: true),
            new OrphanEntry("c2/s2", "cluster", 20, Now - 86400, OrphanState.Observed, HasValidFull: true),
        ], Now);
        // Act — c1/s1 наблюдается БЕЗ полных (полный удалён), c2/s2 не наблюдается
        var merged = OrphanRegistry.Merge(
            current,
            new Dictionary<string, long> { ["c1/s1"] = 10 },
            new HashSet<string>(), // observedFulls пуст
            Shards(), Clusters(), Now);

        // Assert — наблюдаемый пересчитан в false, ненаблюдаемый перенёс true
        merged.Orphans.Single(e => e.Prefix == "c1/s1").HasValidFull.Should().BeFalse();
        merged.Orphans.Single(e => e.Prefix == "c2/s2").HasValidFull.Should().BeTrue("защита переживает transient list");
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
            current, observed, new HashSet<string>(), Shards(("c2", "s3")), Clusters("c2"), Now);

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
        OrphanRegistry.SelectTtlCandidate(registry, 7 * 86400, Now, new HashSet<string>()).Should().Be("c2/s2");

        // только OBSERVED: старейший просроченный (один)
        var observedOnly = new OrphanRegistry.Registry([registry.Orphans[0]], Now);
        OrphanRegistry.SelectTtlCandidate(observedOnly, 7 * 86400, Now, new HashSet<string>()).Should().Be("c1/s1");

        // ttl=0 → авто-удаление выключено: без DELETING кандидата нет
        OrphanRegistry.SelectTtlCandidate(observedOnly, 0, Now, new HashSet<string>()).Should().BeNull();

        // не истёкший OBSERVED — кандидата нет
        var fresh = new OrphanRegistry.Registry(
            [new OrphanEntry("c1/s1", "cluster", 10, Now - 3600, OrphanState.Observed)], Now);
        OrphanRegistry.SelectTtlCandidate(fresh, 7 * 86400, Now, new HashSet<string>()).Should().BeNull();
    }

    // AAA (AC1/AC2/AC3): TTL-отбор исключает защищённых; ttl=0 — только доводка
    [Fact]
    public void SelectTtlCandidate_защищённые_не_кандидаты()
    {
        // Arrange — три просроченных OBSERVED: с полным, с hold, чистый
        var registry = new OrphanRegistry.Registry(
        [
            new OrphanEntry("c1/s1", "cluster", 10, Now - 8 * 86400, OrphanState.Observed, HasValidFull: true),
            new OrphanEntry("c2/s2", "cluster", 20, Now - 8 * 86400, OrphanState.Observed),
            new OrphanEntry("c3/s3", "cluster", 30, Now - 8 * 86400, OrphanState.Observed),
        ], Now);
        var held = new HashSet<string> { "c2/s2" };

        // Act / Assert — кандидат только незащищённый c3/s3
        OrphanRegistry.SelectTtlCandidate(registry, 7 * 86400, Now, held).Should().Be("c3/s3");
        // все защищены → кандидата нет
        OrphanRegistry.SelectTtlCandidate(registry, 7 * 86400, Now,
            new HashSet<string> { "c2/s2", "c3/s3" }).Should().BeNull();
        // ttl=0 → TTL-кандидатов нет (только доводка DELETING — прежняя семантика)
        OrphanRegistry.SelectTtlCandidate(registry, 0, Now, held).Should().BeNull();
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

    // AAA (AC9): старый JSON без has_valid_full парсится (поле → false); roundtrip
    // с полем симметричен; SameOrphans чувствителен к изменению has_valid_full
    [Fact]
    public void Parse_без_поля_HasValidFull_false_и_SameOrphans_чувствителен()
    {
        // Arrange — реестр, записанный старым кодом (без has_valid_full)
        var legacy = """{"orphans":[{"prefix":"a/b","kind":"shard","size_bytes":1,"first_seen_unix":1,"state":"OBSERVED"}],"updated_unix":1}""";

        // Act / Assert — парсится, поле false
        var parsed = OrphanRegistry.Parse(legacy);
        parsed.Should().NotBeNull();
        parsed!.Orphans[0].HasValidFull.Should().BeFalse("старый ключ — ближайший проход пересчитает");

        // roundtrip нового формата: has_valid_full сериализуется и читается
        var registry = new OrphanRegistry.Registry(
            [new OrphanEntry("a/b", "shard", 1, 1, OrphanState.Observed, HasValidFull: true)], 2);
        var json = OrphanRegistry.ToJson(registry);
        json.Should().Contain("\"has_valid_full\":true");
        OrphanRegistry.SameOrphans(registry, OrphanRegistry.Parse(json)!).Should().BeTrue();
        OrphanRegistry.SameOrphans(registry, new OrphanRegistry.Registry(
            [new OrphanEntry("a/b", "shard", 1, 1, OrphanState.Observed)], 2)).Should()
            .BeFalse("изменение has_valid_full — повод для put");
    }

    // AAA (AC6): ключи/JSON hold-заявки канона arch/19 §4
    [Fact]
    public void HoldDelete_ключи_и_JSON_канона()
    {
        // Act
        var holdKey = OrphanRegistry.HoldKey("c1/s1");
        var deleteKey = OrphanRegistry.DeleteKey("c2/s2");

        // Assert — формат канона
        holdKey.Should().Be("/pgworker/backups/orphan-holds/c1/s1");
        deleteKey.Should().Be("/pgworker/backups/orphan-deletes/c2/s2");
        OrphanRegistry.HoldToJson(1757764800, "operator").Should()
            .Be("""{"set_unix":1757764800,"set_by":"operator"}""");
        OrphanRegistry.DeleteToJson(1757764800, "panel").Should()
            .Be("""{"requested_unix":1757764800,"requested_by":"panel"}""");
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
