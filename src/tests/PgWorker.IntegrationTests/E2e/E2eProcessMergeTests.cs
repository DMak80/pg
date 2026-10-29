using FluentAssertions;
using Xunit;

namespace PgWorker.IntegrationTests.E2e;

// Механика объединённого съёма stdout+stderr (канон docs/e2e-launch.md §1):
// docker-CLI разводит потоки контейнера по своим потокам — телеметрия обязана
// видеть оба; контракт RunProcessAsync (только stdout при exit=0) не меняется.
// Не-E2E факт: локальный /bin/sh, без docker.
[Collection(NonE2eCollection.Name)]
public sealed class E2eProcessMergeTests
{
    [Fact]
    public async Task RunProcessMergedAsync_ReturnsStdoutThenStderr()
    {
        // Arrange: процесс пишет маркер в stdout и другой — в stderr.
        var args = new[] { "-c", "echo stdout-marker; echo stderr-marker >&2" };

        // Act: объединённый запуск.
        var merged = await E2eFixture.RunProcessMergedAsync(
            "/bin/sh", args, CancellationToken.None, TimeSpan.FromSeconds(30));

        // Assert: ОБА маркера; stdout-блок раньше stderr-блока (§3.1).
        merged.Should().Contain("stdout-marker");
        merged.Should().Contain("stderr-marker");
        merged.IndexOf("stdout-marker").Should().BeLessThan(
            merged.IndexOf("stderr-marker"));
    }

    [Fact]
    public async Task RunProcessAsync_ReturnsOnlyStdout()
    {
        // Arrange: тот же двупоточный процесс.
        var args = new[] { "-c", "echo stdout-marker; echo stderr-marker >&2" };

        // Act: обычный (не-merge) запуск.
        var output = await E2eFixture.RunProcessAsync(
            "/bin/sh", args, CancellationToken.None);

        // Assert: регресс-охрана контракта — в возврате ТОЛЬКО stdout.
        output.Should().Contain("stdout-marker");
        output.Should().NotContain("stderr-marker");
    }
}
