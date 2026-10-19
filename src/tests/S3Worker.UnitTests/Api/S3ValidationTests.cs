using FluentAssertions;
using S3Worker.App.Api.Operations;
using Xunit;

namespace S3Worker.UnitTests.Api;

// Валидаторы API S3-домена (arch/24 §1.1, spec §8.1.2): создание с instances[]
// (имена/резерв/дубли/placement), add-node, links (существование, a≠b,
// уникальность пар с точностью до зеркальности).
public class S3ValidationTests
{
    private static S3InstanceDto Instance(string name, S3ResourcesDto? resources = null)
        => new(name, new S3PlacementDto("h1", null, null), resources);

    private static CreateS3ClusterRequest Valid(
        string? name = "demo", string? platform = "owns3",
        IReadOnlyList<S3InstanceDto>? instances = null, decimal? cpu = 1m, int? memGi = 1, int? diskGi = 10,
        bool? etcdBackup = true) => new(
        name, platform, etcdBackup, instances, new S3ResourcesDto(cpu, memGi, diskGi));

    [Fact]
    public void ValidRequest_NoErrors()
    {
        // Arrange + Act: 0 инстансов и 2 именованных — обе декларации валидны.
        var empty = S3ClusterValidator.Validate(Valid(instances: []));
        var two = S3ClusterValidator.Validate(Valid(instances: [Instance("east1"), Instance("east2")]));

        // Assert: 0..9 — валидно (arch/23 §2).
        empty.Should().BeEmpty();
        two.Should().BeEmpty();
    }

    [Fact]
    public void Name_LengthBounds()
    {
        // Arrange: 63 символа ок / 64 нет; дефис запрещён.
        var ok = S3ClusterValidator.Validate(Valid(name: new string('a', 63)));
        var tooLong = S3ClusterValidator.Validate(Valid(name: new string('a', 64)));
        var dash = S3ClusterValidator.Validate(Valid(name: "a-b"));

        // Assert
        ok.Should().BeEmpty();
        tooLong.Should().ContainSingle().Which.Field.Should().Be("name");
        dash.Should().Contain(e => e.Field == "name");
    }

    [Fact]
    public void Platform_Minio_Rejected()
    {
        // Arrange + Act
        var errors = S3ClusterValidator.Validate(Valid(platform: "minio"));

        // Assert: только owns3 (arch/23 §2).
        errors.Should().Contain(e => e.Field == "platform");
    }

    [Fact]
    public void Instances_MoreThan9_Rejected()
    {
        // Arrange: 10 инстансов — структурный предел 0..9.
        var instances = Enumerable.Range(0, 10).Select(i => Instance($"n{i}")).ToList();

        // Act
        var errors = S3ClusterValidator.Validate(Valid(instances: instances));

        // Assert
        errors.Should().Contain(e => e.Field == "instances");
    }

    [Fact]
    public void InstanceName_PatternReservedDuplicate()
    {
        // Arrange: паттерн-нарушение; резерв orchestrator; дубль имени в теле.
        var badPattern = S3ClusterValidator.Validate(Valid(instances: [Instance("9bad")]));
        var reserved = S3ClusterValidator.Validate(Valid(instances: [Instance("orchestrator")]));
        var duplicate = S3ClusterValidator.Validate(Valid(instances: [Instance("east1"), Instance("east1")]));

        // Assert: все — 400 (коллизия с существующим в etcd — 409, интеграционно).
        badPattern.Should().Contain(e => e.Field == "instances[0].name");
        reserved.Should().Contain(e => e.Field == "instances[0].name");
        duplicate.Should().Contain(e => e.Field == "instances[1].name");
    }

    [Fact]
    public void InstancePlacement_RequiredForEach()
    {
        // Arrange: у второго инстанса пустой placement.
        var instances = new[] { Instance("east1"), new S3InstanceDto("east2", new S3PlacementDto(null, null, null), null) };

        // Act
        var errors = S3ClusterValidator.Validate(Valid(instances: instances));

        // Assert
        errors.Should().Contain(e => e.Field == "instances[1].placement");
    }

    [Fact]
    public void InstanceResources_PerInstance_Bounds()
    {
        // Arrange: per-instance cpu вне диапазона (fallback общий валиден).
        var instances = new[] { Instance("east1", new S3ResourcesDto(100m, 1, 10)) };

        // Act
        var errors = S3ClusterValidator.Validate(Valid(instances: instances));

        // Assert: поле с префиксом инстанса.
        errors.Should().Contain(e => e.Field == "instances[0].cpu");
    }

    [Fact]
    public void Cpu_Bounds()
    {
        // Arrange: 0.01/64 ок, 0.005/65 нет.
        S3ClusterValidator.Validate(Valid(cpu: 0.01m)).Should().BeEmpty();
        S3ClusterValidator.Validate(Valid(cpu: 64m)).Should().BeEmpty();

        // Act + Assert
        S3ClusterValidator.Validate(Valid(cpu: 0.005m)).Should().Contain(e => e.Field == "cpu");
        S3ClusterValidator.Validate(Valid(cpu: 65m)).Should().Contain(e => e.Field == "cpu");
    }

    [Fact]
    public void MemDisk_Bounds()
    {
        // Arrange: 1/65536 ок, 0/65537 нет.
        S3ClusterValidator.Validate(Valid(memGi: 1, diskGi: 65536)).Should().BeEmpty();

        // Act + Assert
        S3ClusterValidator.Validate(Valid(memGi: 0)).Should().Contain(e => e.Field == "memGi");
        S3ClusterValidator.Validate(Valid(diskGi: 65537)).Should().Contain(e => e.Field == "diskGi");
    }

    [Fact]
    public void EtcdBackup_True_NoErrors()
    {
        // Arrange: EtcdBackup=true, остальные поля валидны.
        var request = Valid(etcdBackup: true);

        // Act
        var errors = S3ClusterValidator.Validate(request);

        // Assert: 0 ошибок.
        errors.Should().BeEmpty();
    }

    [Fact]
    public void EtcdBackup_Null_NoErrors()
    {
        // Arrange: EtcdBackup null — поле опционально.
        var request = Valid(etcdBackup: null);

        // Act
        var errors = S3ClusterValidator.Validate(request);

        // Assert: 0 ошибок.
        errors.Should().BeEmpty();
    }

    // ============ links (чистый валидатор, §8.1.2) ============

    private static readonly string[] Names = ["east1", "east2", "west1"];

    [Fact]
    public void Links_ValidGraph_NoErrors()
    {
        // Arrange + Act: частичный граф — полнота/симметричность не контролируются.
        var errors = S3LinksValidator.Validate(
            [new S3LinkDto("east1", "east2"), new S3LinkDto("east1", "west1")], Names);

        // Assert
        errors.Should().BeEmpty();
    }

    [Fact]
    public void Links_UnknownName_400()
    {
        // Arrange + Act
        var errors = S3LinksValidator.Validate([new S3LinkDto("east1", "ghost")], Names);

        // Assert
        errors.Should().Contain(e => e.Field == "links[0].b");
    }

    [Fact]
    public void Links_AEqualsB_400()
    {
        // Arrange + Act
        var errors = S3LinksValidator.Validate([new S3LinkDto("east1", "east1")], Names);

        // Assert
        errors.Should().Contain(e => e.Field == "links[0]");
    }

    [Fact]
    public void Links_DuplicatePair_400()
    {
        // Arrange + Act: прямой и зеркальный дубли — обе 400.
        var straight = S3LinksValidator.Validate(
            [new S3LinkDto("east1", "east2"), new S3LinkDto("east1", "east2")], Names);
        var mirrored = S3LinksValidator.Validate(
            [new S3LinkDto("east1", "east2"), new S3LinkDto("east2", "east1")], Names);

        // Assert: {a,b} ≡ {b,a} — одна связь (arch/23 §2).
        straight.Should().Contain(e => e.Field == "links[1]");
        mirrored.Should().Contain(e => e.Field == "links[1]");
    }
}
