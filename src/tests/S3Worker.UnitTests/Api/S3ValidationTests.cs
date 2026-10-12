using FluentAssertions;
using S3Worker.App.Api.Operations;
using Xunit;

namespace S3Worker.UnitTests.Api;

// Валидатор создания S3-кластера (spec §6): граничные кейсы лимитов.
public class S3ValidationTests
{
    private static CreateS3ClusterRequest Valid(
        string? name = "demo", string? platform = "owns3", int? nodes = 3,
        int? placements = null, decimal? cpu = 1m, int? memGi = 1, int? diskGi = 10,
        string? secretProfile = "backups-main") => new(
        name, platform, nodes, secretProfile,
        Enumerable.Range(0, placements ?? nodes ?? 3)
            .Select(_ => new S3PlacementDto("h1", null, null)).ToList(),
        new S3ResourcesDto(cpu, memGi, diskGi));

    [Fact]
    public void ВалиднаяЗаявка_БезОшибок()
    {
        // Arrange + Act
        var errors = S3ClusterValidator.Validate(Valid());

        // Assert
        errors.Should().BeEmpty();
    }

    [Fact]
    public void Name_ГраницыДлины()
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
    public void Platform_Minio_Отклонён()
    {
        // Arrange + Act
        var errors = S3ClusterValidator.Validate(Valid(platform: "minio"));

        // Assert: только owns3 (arch/23 §2).
        errors.Should().Contain(e => e.Field == "platform");
    }

    [Fact]
    public void Nodes_Границы()
    {
        // Arrange: 1/9 ок, 0/10 нет.
        S3ClusterValidator.Validate(Valid(nodes: 1)).Should().BeEmpty();
        S3ClusterValidator.Validate(Valid(nodes: 9)).Should().BeEmpty();

        // Act + Assert
        S3ClusterValidator.Validate(Valid(nodes: 0)).Should().Contain(e => e.Field == "nodes");
        S3ClusterValidator.Validate(Valid(nodes: 10)).Should().Contain(e => e.Field == "nodes");
    }

    [Fact]
    public void Cpu_Границы()
    {
        // Arrange: 0.01/64 ок, 0.005/65 нет.
        S3ClusterValidator.Validate(Valid(cpu: 0.01m)).Should().BeEmpty();
        S3ClusterValidator.Validate(Valid(cpu: 64m)).Should().BeEmpty();

        // Act + Assert
        S3ClusterValidator.Validate(Valid(cpu: 0.005m)).Should().Contain(e => e.Field == "cpu");
        S3ClusterValidator.Validate(Valid(cpu: 65m)).Should().Contain(e => e.Field == "cpu");
    }

    [Fact]
    public void MemDisk_Границы()
    {
        // Arrange: 1/65536 ок, 0/65537 нет.
        S3ClusterValidator.Validate(Valid(memGi: 1, diskGi: 65536)).Should().BeEmpty();

        // Act + Assert
        S3ClusterValidator.Validate(Valid(memGi: 0)).Should().Contain(e => e.Field == "memGi");
        S3ClusterValidator.Validate(Valid(diskGi: 65537)).Should().Contain(e => e.Field == "diskGi");
    }

    [Fact]
    public void Placements_ЧислоИНепустота()
    {
        // Arrange: 2 из nodes=3 → 400; пустой placement → 400.
        var few = S3ClusterValidator.Validate(Valid(placements: 2));
        var empty = S3ClusterValidator.Validate(new CreateS3ClusterRequest(
            "demo", "owns3", 1, "backups-main",
            [new S3PlacementDto(null, null, null)],
            new S3ResourcesDto(1m, 1, 10)));

        // Assert
        few.Should().Contain(e => e.Field == "placements");
        empty.Should().Contain(e => e.Field == "placements");
    }

    [Fact]
    public void ПрофильНеЗадан_Ошибка()
    {
        // Arrange + Act
        var errors = S3ClusterValidator.Validate(Valid(secretProfile: " "));

        // Assert: 400 здесь; отсутствующий профиль — 422 в хендлере.
        errors.Should().Contain(e => e.Field == "secretProfile");
    }
}
