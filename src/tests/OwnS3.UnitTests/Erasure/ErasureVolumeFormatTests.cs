using OwnS3.Storage.Erasure;

namespace OwnS3.UnitTests.Erasure;

// Тесты модели/IO volume.json v2 (t40, план Task 6).
public class ErasureVolumeFormatTests
{
    private string TempRoot() =>
        Path.Combine(Path.GetTempPath(), "owns3-ecfmt-" + Guid.NewGuid().ToString("N"));

    [Fact]
    public void RoundTrip_WritesAndReadsV2()
    {
        // Arrange
        var root = TempRoot();
        var sets = new[] { new[] { "d1", "d2", "d3", "d4" } };
        var info = new ErasureVolumeInfo("OWNS3-VOL", 2, "vol-1", "XL Erasure", "dep-1", "d1",
            4, sets, [2, 0, 3, 1], "SIPMOD");
        try
        {
            // Act
            ErasureVolumeInfo.Write(root, info);
            var read = ErasureVolumeInfo.Read(root);

            // Assert — camelCase-поля полностью сходятся
            read.Magic.Should().Be("OWNS3-VOL");
            read.FormatVersion.Should().Be(2);
            read.VolumeId.Should().Be("vol-1");
            read.Mode.Should().Be("XL Erasure");
            read.DeploymentId.Should().Be("dep-1");
            read.DiskId.Should().Be("d1");
            read.SetSize.Should().Be(4);
            read.Sets.Should().BeEquivalentTo(sets);
            read.Distribution.Should().Equal(2, 0, 3, 1);
            read.DistributionAlgo.Should().Be("SIPMOD");
        }
        finally
        {
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void Read_BrokenJson_ThrowsClearError()
    {
        // Arrange
        var root = TempRoot();
        Directory.CreateDirectory(Path.Combine(root, ".owns3.sys"));
        File.WriteAllText(ErasureVolumeInfo.VolumeJsonPath(root), "{ not-json");
        try
        {
            // Act
            var act = () => ErasureVolumeInfo.Read(root);

            // Assert
            act.Should().Throw<InvalidOperationException>().WithMessage("*не читается*");
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void Read_ForeignMagic_PoolStartFails()
    {
        // Arrange — чужой magic: отказ даёт валидация старта пула (ValidateReference)
        var root = TempRoot();
        try
        {
            Directory.CreateDirectory(root);
            ErasureVolumeInfo.Write(root, new ErasureVolumeInfo("FOREIGN", 2, "v", "XL Erasure", "d", "x",
                4, [new[] { "a", "b", "c", "d" }], [0, 1, 2, 3], "SIPMOD"));
            using var pool = NewPool([root, TempRoot(), TempRoot(), TempRoot()], createOthers: true);

            // Act
            var act = () => pool.Initialize();

            // Assert
            act.Should().Throw<InvalidOperationException>().WithMessage("*чужой magic*");
        }
        finally
        {
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void Read_NewerFormatVersion_PoolStartFails()
    {
        // Arrange — версия 3 новее поддерживаемой
        var root = TempRoot();
        try
        {
            Directory.CreateDirectory(root);
            ErasureVolumeInfo.Write(root, new ErasureVolumeInfo("OWNS3-VOL", 3, "v", "XL Erasure", "d", "x",
                4, [new[] { "a", "b", "c", "d" }], [0, 1, 2, 3], "SIPMOD"));
            using var pool = NewPool([root, TempRoot(), TempRoot(), TempRoot()], createOthers: true);

            // Act
            var act = () => pool.Initialize();

            // Assert
            act.Should().Throw<InvalidOperationException>().WithMessage("*новее поддерживаемой*");
        }
        finally
        {
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
        }
    }

    // Пул поверх путей; createOthers — создать каталоги остальных позиций (пустые).
    private static ErasurePool NewPool(IReadOnlyList<string> roots, bool createOthers)
    {
        if (createOthers)
            foreach (var root in roots.Skip(1))
                Directory.CreateDirectory(root);
        return new ErasurePool(roots, new ErasureTopology(4, 4, 1), TimeProvider.System);
    }
}
