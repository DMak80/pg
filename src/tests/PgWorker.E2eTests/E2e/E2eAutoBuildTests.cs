using FluentAssertions;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// spec «фаза Г»: правило пересборки Release в статических ассетах
// (E2eEnvironment.EnsureStaticAssetsAsync → E2eFixture.PublishProjectAsync).
// Юнит-слой — fail-fast NOBUILD без publish-артефактов (EnsurePublishedDir);
// автосборка проверяется живым прогоном.
public class E2eAutoBuildTests
{
    [Fact]
    public void EnsurePublishedDir_NoBuild_WithoutArtifacts_FailsFastWithHint()
    {
        // Arrange: пустой временный каталог — артефактов publish нет.
        var root = Path.Combine(Path.GetTempPath(), $"pgw-e2e-nobuild-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        try
        {
            // Act: NOBUILD + отсутствие артефактов.
            var ex = Assert.Throws<ApplicationException>(
                () => E2eFixture.EnsurePublishedDir(root, "hint-test"));

            // Assert: fail-fast с подсказкой про автосборку.
            ex.Message.Should().Contain("PGW_TEST_E2E_NOBUILD").And.Contain("dotnet publish");
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }
}
