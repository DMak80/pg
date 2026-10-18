using FluentAssertions;
using ValkeyWorker.App;
using Xunit;

namespace ValkeyWorker.UnitTests.App;

// Fail-fast старта (arch/21 §2): ContourNetwork не поддерживается в Mode=Swarm —
// сеть overlay назначает сам Swarm (канон PgWorker).
public class OptionsContourNetworkTests
{
    [Theory]
    [InlineData("Swarm")]
    [InlineData("swarm")]
    public void Swarm_WithContourNetwork_Throws(string mode)
    {
        // Arrange — swarm (в любом регистре) + контурная сеть: ошибка конфигурации.
        var docker = new DockerOptions { Mode = mode, ContourNetwork = "adminpanel-stand-net" };

        // Act
        var act = () => docker.EnsureContourNetworkMode();

        // Assert
        act.Should().Throw<ApplicationException>().WithMessage(
            "ValkeyWorker:Docker:ContourNetwork is not supported in Mode=Swarm — contour network mode is implemented for plain containers");
    }

    [Theory]
    [InlineData("Swarm", null)]
    [InlineData("Swarm", "")]
    [InlineData("Swarm", "   ")]
    [InlineData("Plain", "adminpanel-stand-net")]
    public void OtherCombinations_DoNotThrow(string mode, string? contourNetwork)
    {
        // Arrange — пустая сеть в Swarm и любая в Plain — валидные комбинации.
        var docker = new DockerOptions { Mode = mode, ContourNetwork = contourNetwork };

        // Act/Assert
        docker.EnsureContourNetworkMode();
    }
}
