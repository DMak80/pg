using System.Text.Json;
using OwnS3.Storage.Erasure;

namespace OwnS3.UnitTests.Erasure;

// Тесты пула сетов (t40, план Task 6): старт-правила канона 06 §2 + правка Task 0
// (отсутствующий путь живого тома — offline), SIPMOD, distribution.
public class ErasurePoolTests : IDisposable
{
    private readonly List<string> _roots = [];

    private string NewRoot()
    {
        var root = Path.Combine(Path.GetTempPath(), "owns3-ecpool-" + Guid.NewGuid().ToString("N"));
        _roots.Add(root);
        return root;
    }

    public void Dispose()
    {
        // Teardown: temp-корни удаляются при любом исходе
        foreach (var root in _roots)
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
    }

    private ErasurePool NewPool(int drives, int setSize = 4, int parity = 1)
    {
        var roots = Enumerable.Range(0, drives).Select(_ => NewRoot()).ToList();
        return new ErasurePool(roots, new ErasureTopology(drives, setSize, parity), TimeProvider.System);
    }

    [Fact]
    public void Initialize_AllEmpty_CreatesFormatOnEveryDisk()
    {
        // Arrange: 4 temp-пути (каталоги не существуют — создание при старте)
        using var pool = NewPool(4);

        // Act
        pool.Initialize();

        // Assert: на каждом диске валидный volume.json v2, один deploymentId, разные
        // diskId, sets == diskId по позициям, distribution — перестановка 0..3;
        // структура .owns3.sys/{tmp,multipart,.trash,buckets,config}
        var infos = pool.Disks.Select(d => ErasureVolumeInfo.Read(d.Root)).ToList();
        infos.Should().OnlyContain(i => i.Magic == "OWNS3-VOL" && i.FormatVersion == 2 && i.Mode == "XL Erasure");
        infos.Select(i => i.DeploymentId).Distinct().Should().HaveCount(1);
        infos.Select(i => i.DiskId).Distinct().Should().HaveCount(4);
        infos[0].Sets[0].Should().Equal(infos.Select(i => i.DiskId));
        infos[0].Distribution.OrderBy(v => v).Should().Equal(0, 1, 2, 3);
        foreach (var disk in pool.Disks)
            foreach (var sub in new[] { "tmp", "multipart", ".trash", "buckets", "config" })
                Directory.Exists(Path.Combine(disk.Root, ".owns3.sys", sub)).Should().BeTrue(
                    "служебная структура на каждом диске: {0}", sub);
    }

    [Fact]
    public void Initialize_ExistingThenRestartOnePathMissing_DiskOfflineNoReplace()
    {
        // Arrange: созданный пул; путь диска 3 удалён
        var roots = Enumerable.Range(0, 4).Select(_ => NewRoot()).ToList();
        var first = new ErasurePool(roots, new ErasureTopology(4, 4, 1), TimeProvider.System);
        first.Initialize();
        var diskId3 = ErasureVolumeInfo.Read(roots[3]).DiskId;
        Directory.Delete(roots[3], recursive: true);

        // Act: новый pool.Initialize() на тех же путях
        using var second = new ErasurePool(roots, new ErasureTopology(4, 4, 1), TimeProvider.System);
        second.Initialize();

        // Assert: диск 3 offline, diskId НЕ выдан новый (замены нет — правка Task 0)
        second.Disks[3].Online.Should().BeFalse();
        second.Disks[3].DiskId.Should().Be(diskId3, "отсутствующий путь — offline без замены identity");
        ErasureVolumeInfo.Read(roots[0]).Sets[0][3].Should().Be(diskId3, "sets не менялись");
    }

    [Fact]
    public void Initialize_ForeignContentWithoutFormat_FailsFast()
    {
        // Arrange: позиция 2 с посторонним содержимым без volume.json
        using var pool = NewPool(4);
        Directory.CreateDirectory(pool.Disks[2].Root);
        File.WriteAllText(Path.Combine(pool.Disks[2].Root, "foreign.txt"), "x");

        // Act
        var act = () => pool.Initialize();

        // Assert
        act.Should().Throw<InvalidOperationException>().WithMessage("*посторонние данные*");
    }

    [Fact]
    public void Initialize_ForeignerMagic_Fails()
    {
        // Arrange: чужой magic на одной позиции живого-кандидата
        using var pool = NewPool(4);
        Directory.CreateDirectory(pool.Disks[1].Root);
        ErasureVolumeInfo.Write(pool.Disks[1].Root, new ErasureVolumeInfo("FOREIGN", 2, "v", "XL Erasure",
            "d", "x", 4, [new[] { "a", "b", "c", "d" }], [0, 1, 2, 3], "SIPMOD"));

        // Act
        var act = () => pool.Initialize();

        // Assert
        act.Should().Throw<InvalidOperationException>().WithMessage("*чужой magic*");
    }

    [Fact]
    public void Initialize_NewerFormatVersion_Fails()
    {
        // Arrange
        using var pool = NewPool(4);
        Directory.CreateDirectory(pool.Disks[0].Root);
        ErasureVolumeInfo.Write(pool.Disks[0].Root, new ErasureVolumeInfo("OWNS3-VOL", 3, "v", "XL Erasure",
            "d", "x", 4, [new[] { "a", "b", "c", "d" }], [0, 1, 2, 3], "SIPMOD"));

        // Act
        var act = () => pool.Initialize();

        // Assert
        act.Should().Throw<InvalidOperationException>().WithMessage("*новее поддерживаемой*");
    }

    [Fact]
    public void Initialize_DiskIdOutsideSets_Fails()
    {
        // Arrange: валидный формат, но diskId не из sets своей позиции
        using var pool = NewPool(4);
        var seeds = Enumerable.Range(0, 4).Select(_ => Guid.NewGuid().ToString("N")).ToArray();
        for (var i = 0; i < 4; i++)
        {
            Directory.CreateDirectory(pool.Disks[i].Root);
            var diskId = i == 0 ? Guid.NewGuid().ToString("N") : seeds[i]; // позиция 0 — чужой diskId
            ErasureVolumeInfo.Write(pool.Disks[i].Root, new ErasureVolumeInfo("OWNS3-VOL", 2, "v",
                "XL Erasure", "dep", diskId, 4, [seeds], [0, 1, 2, 3], "SIPMOD"));
        }

        // Act
        var act = () => pool.Initialize();

        // Assert
        act.Should().Throw<InvalidOperationException>().WithMessage("*diskId вне sets*");
    }

    [Fact]
    public void Initialize_PathCountChanged_Fails()
    {
        // Arrange: живой том 4 дисков; старт с 8 путями (изменение N запрещено)
        var roots = Enumerable.Range(0, 4).Select(_ => NewRoot()).ToList();
        var first = new ErasurePool(roots, new ErasureTopology(4, 4, 1), TimeProvider.System);
        first.Initialize();
        first.Dispose();
        var eightRoots = roots.Concat(Enumerable.Range(0, 4).Select(_ => NewRoot())).ToList();
        using var second = new ErasurePool(eightRoots, new ErasureTopology(8, 4, 1), TimeProvider.System);

        // Act
        var act = () => second.Initialize();

        // Assert
        act.Should().Throw<InvalidOperationException>().WithMessage("*ожидает 4 дисков, конфигурация даёт 8*");
    }

    [Fact]
    public void Initialize_EmptyDirCandidate_ReplacesDiskIdWhenAliveGeK()
    {
        // Arrange: живой том 4/1 (k=3); диск 3 стёрт содержимым (пустой каталог)
        var roots = Enumerable.Range(0, 4).Select(_ => NewRoot()).ToList();
        var first = new ErasurePool(roots, new ErasureTopology(4, 4, 1), TimeProvider.System);
        first.Initialize();
        var oldDiskId3 = ErasureVolumeInfo.Read(roots[3]).DiskId;
        first.Dispose();
        foreach (var file in Directory.EnumerateFiles(roots[3], "*", SearchOption.AllDirectories))
            File.Delete(file);
        DeleteSubDirs(roots[3]);

        // Act: живых 3 >= k=3 — замена: новый diskId, sets обновлён на онлайн-дисках
        using var second = new ErasurePool(roots, new ErasureTopology(4, 4, 1), TimeProvider.System);
        second.Initialize();

        // Assert
        second.Disks[3].DiskId.Should().NotBe(oldDiskId3, "кандидат получил новый diskId");
        second.Disks[3].Online.Should().BeTrue();
        foreach (var disk in second.Disks.Take(3))
            ErasureVolumeInfo.Read(disk.Root).Sets[0][3].Should().Be(second.Disks[3].DiskId,
                "sets обновлён на онлайн-дисках");
    }

    [Fact]
    public void Initialize_EmptyCandidatesGeK_Fails()
    {
        // Arrange: setSize=4, k=2 (parity 2): 2 пустых кандидата >= k — отказ старта
        var roots = Enumerable.Range(0, 4).Select(_ => NewRoot()).ToList();
        var first = new ErasurePool(roots, new ErasureTopology(4, 4, 2), TimeProvider.System);
        first.Initialize();
        first.Dispose();
        for (var i = 2; i < 4; i++)
        {
            foreach (var file in Directory.EnumerateFiles(roots[i], "*", SearchOption.AllDirectories))
                File.Delete(file);
            DeleteSubDirs(roots[i]);
        }

        using var pool = new ErasurePool(roots, new ErasureTopology(4, 4, 2), TimeProvider.System);

        // Act
        var act = () => pool.Initialize();

        // Assert
        act.Should().Throw<InvalidOperationException>().WithMessage("*заменяемых одновременно*");
    }

    [Fact]
    public void Initialize_AliveBelowK_CandidatePresent_Fails()
    {
        // Arrange: setSize=4, k=3, m=1: 2 живых валидных + 2 пустых кандидата —
        // заменяемых 2 < k, но живых 2 < k → отказ старта
        var roots = Enumerable.Range(0, 4).Select(_ => NewRoot()).ToList();
        var first = new ErasurePool(roots, new ErasureTopology(4, 4, 1), TimeProvider.System);
        first.Initialize();
        first.Dispose();
        for (var i = 1; i < 3; i++)
        {
            foreach (var file in Directory.EnumerateFiles(roots[i], "*", SearchOption.AllDirectories))
                File.Delete(file);
            DeleteSubDirs(roots[i]);
        }

        using var pool = new ErasurePool(roots, new ErasureTopology(4, 4, 1), TimeProvider.System);

        // Act
        var act = () => pool.Initialize();

        // Assert
        act.Should().Throw<InvalidOperationException>().WithMessage("*живых старых дисков 2 < k=3*");
    }

    [Fact]
    public void PickSetIndex_StableForSameKeyAndDeployment()
    {
        // Arrange
        using var pool = NewPool(8, setSize: 4, parity: 1);
        pool.Initialize();

        // Act/Assert
        var first = pool.PickSetIndex("b", "k1");
        for (var t = 0; t < 100; t++)
            pool.PickSetIndex("b", "k1").Should().Be(first, "SIPMOD детерминирован");

        pool.PickSetIndex("b", "k1").Should().BeInRange(0, 1, "2 сета");
    }

    [Fact]
    public void PickSetIndex_UniformAcrossRandomKeys()
    {
        // Arrange: 1000 ключей, 2 сета
        using var pool = NewPool(8, setSize: 4, parity: 1);
        pool.Initialize();
        var counts = new int[2];

        // Act
        var rng = new Random(9);
        for (var t = 0; t < 1000; t++)
        {
            var key = "obj-" + rng.NextInt64();
            counts[pool.PickSetIndex("b", key)]++;
        }

        // Assert — отклонение долей < 0.1
        Math.Abs(counts[0] - counts[1]).Should().BeLessThan(100, "равномерность SIPMOD");
    }

    [Fact]
    public void PickSetIndex_DependsOnDeploymentId()
    {
        // Arrange: два пула (разные deploymentId) на одинаковых путях-состояниях
        var rootsA = Enumerable.Range(0, 8).Select(_ => NewRoot()).ToList();
        using var poolA = new ErasurePool(rootsA, new ErasureTopology(8, 4, 1), TimeProvider.System);
        poolA.Initialize();
        var rootsB = Enumerable.Range(0, 8).Select(_ => NewRoot()).ToList();
        using var poolB = new ErasurePool(rootsB, new ErasureTopology(8, 4, 1), TimeProvider.System);
        poolB.Initialize();

        // Act/Assert: хотя бы один ключ из набора уходит в разные сеты
        var differs = Enumerable.Range(0, 50)
            .Any(t => poolA.PickSetIndex("b", "k" + t) != poolB.PickSetIndex("b", "k" + t));
        differs.Should().BeTrue("распределение зависит от deploymentId");
    }

    [Fact]
    public void Distribution_IsPermutationOfRange()
    {
        // Arrange/Act/Assert — перестановка для разных setSize
        foreach (var setSize in new[] { 4, 8, 16 })
        {
            var dist = ErasurePool.BuildDistribution(Guid.NewGuid(), setSize);
            dist.OrderBy(v => v).Should().Equal(Enumerable.Range(0, setSize));
        }
    }

    [Theory]
    [InlineData(4, 4)]
    [InlineData(8, 8)]
    [InlineData(16, 16)]
    [InlineData(32, 16)]
    [InlineData(12, 4)]
    [InlineData(5, 0)]
    public void Topology_DefaultSetSize(int drives, int expected)
    {
        // Arrange/Act/Assert
        ErasureTopology.DefaultSetSize(drives).Should().Be(expected);
    }

    private static void DeleteSubDirs(string root)
    {
        foreach (var dir in Directory.EnumerateDirectories(root))
            Directory.Delete(dir, recursive: true);
    }
}
