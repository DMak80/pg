using FluentAssertions;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// t09, spec «фаза Г»: правило пересборки Release в E2eFixture. Юнит-слой —
// детерминированная ветка PGW_TEST_E2E_NOBUILD без бинаря (fail-fast);
// автосборка проверяется живым прогоном (Task 2).
public class E2eAutoBuildTests
{
    [Fact]
    public async Task EnsureAppDll_NoBuild_WithoutDll_FailsFastWithBuildHint()
    {
        // Arrange: пустой временный каталог — бинаря нет, автосборка выключена.
        var root = Path.Combine(Path.GetTempPath(), $"pgw-e2e-nobuild-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        try
        {
            // Act: NOBUILD + отсутствие бинаря.
            var ex = await Assert.ThrowsAsync<ApplicationException>(
                () => E2eFixture.EnsureAppDllAsync(root, noBuild: true));

            // Assert: fail-fast с командой сборки в сообщении.
            ex.Message.Should().Contain("dotnet build src/PgWorker.slnx -c Release");
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }
}
