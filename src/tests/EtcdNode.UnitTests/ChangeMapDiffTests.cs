using EtcdNode.Core;

namespace EtcdNode.UnitTests;

// Дифф полной карты key→mod_revision с эталоном последней выгрузки
// (arch/etcdnode/02-snapshots.md §3): пульс-перезапись не важна,
// изменения вне пульсов — важны.

public class ChangeMapDiffTests
{
    [Fact]
    public void PulseRewrite_NotImportant()
    {
        // Arrange: эталон с пульсами revision 1/1
        var baseline = new Dictionary<string, long>
        {
            ["/service/x/leader"] = 1,
            ["/clusters/c/shards/s/master"] = 1,
        };
        // Act: те же ключи, revision вырос (перезапись пульсов)
        var current = new Dictionary<string, long>
        {
            ["/service/x/leader"] = 2,
            ["/clusters/c/shards/s/master"] = 5,
        };

        // Assert: рост revision только у пульсов — не важно
        ChangeMapDiff.IsImportant(baseline, current).Should().BeFalse();
    }

    [Fact]
    public void NewKeyOutsidePulses_Important()
    {
        // Arrange: пустой эталон
        var baseline = new Dictionary<string, long>();
        // Act: появился незнакомый ключ вне пульсов
        var current = new Dictionary<string, long> { ["/pgworker/work"] = 3 };

        // Assert
        ChangeMapDiff.IsImportant(baseline, current).Should().BeTrue();
    }

    [Fact]
    public void ChangedValueOutsidePulses_Important()
    {
        // Arrange: ключ вне пульсов в эталоне
        var baseline = new Dictionary<string, long> { ["/clusters/c/shards/s/routing"] = 1 };
        // Act: тот же ключ, revision вырос (смена значения)
        var current = new Dictionary<string, long> { ["/clusters/c/shards/s/routing"] = 2 };

        // Assert
        ChangeMapDiff.IsImportant(baseline, current).Should().BeTrue();
    }

    [Fact]
    public void KeyOutsidePulsesDisappeared_Important()
    {
        // Arrange: эталон содержит ключ вне пульсов
        var baseline = new Dictionary<string, long> { ["/etcd/config"] = 4 };
        // Act: в текущей карте ключ исчез
        var current = new Dictionary<string, long>();

        // Assert
        ChangeMapDiff.IsImportant(baseline, current).Should().BeTrue();
    }

    [Fact]
    public void MasterKeyChangedValue_NotImportant()
    {
        // Arrange: единственный ключ — мастер-ключ HA (пульс)
        var baseline = new Dictionary<string, long> { ["/clusters/c/shards/s/master"] = 1 };
        // Act: смена значения мастер-ключа (перезапись пульса)
        var current = new Dictionary<string, long> { ["/clusters/c/shards/s/master"] = 2 };

        // Assert: смена значения пульса — не важно
        ChangeMapDiff.IsImportant(baseline, current).Should().BeFalse();
    }

    [Fact]
    public void EmptyMaps_NotImportant()
    {
        // Arrange: обе карты пусты
        var baseline = new Dictionary<string, long>();
        var current = new Dictionary<string, long>();

        // Act + Assert: изменений нет
        ChangeMapDiff.IsImportant(baseline, current).Should().BeFalse();
    }
}
