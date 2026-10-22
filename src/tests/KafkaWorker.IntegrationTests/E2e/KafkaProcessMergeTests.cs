using FluentAssertions;
using Xunit;

namespace KafkaWorker.IntegrationTests.E2e;

// Механика объединённого съёма stdout+stderr (канон docs/e2e-launch.md §1):
// лёгкий факт без docker — /bin/sh пишет в оба потока, merge-хелпер обязан
// вернуть оба блока (stdout раньше stderr).
public sealed class KafkaProcessMergeTests
{
    [Fact]
    public async Task RunProcessMergedAsync_ReturnsStdoutThenStderr()
    {
        // Arrange: процесс пишет маркер в stdout и другой — в stderr.
        var args = new[] { "-c", "echo stdout-marker; echo stderr-marker >&2" };

        // Act: объединённый запуск.
        var merged = await KafkaE2eEnvironment.RunProcessMergedAsync(
            "/bin/sh", args, CancellationToken.None, TimeSpan.FromSeconds(30));

        // Assert: оба маркера; stdout-блок раньше stderr-блока.
        merged.Should().Contain("stdout-marker");
        merged.Should().Contain("stderr-marker");
        merged.IndexOf("stdout-marker").Should().BeLessThan(
            merged.IndexOf("stderr-marker"));
    }
}
