using FluentAssertions;
using Xunit;

namespace ValkeyWorker.IntegrationTests.E2e;

// ShellToFileAsync телеметрии обязан писать ОБА потока процесса (stdout+
// stderr, канон docs/e2e-launch.md §1): лёгкий факт без docker — /bin/sh
// пишет в оба потока, файл содержит оба блока.
public sealed class ValkeyShellToFileTests
{
    [Fact]
    public async Task ShellToFileAsync_WritesBothStreams()
    {
        // Arrange: temp-файл артефакта; процесс пишет в оба потока.
        var path = Path.Combine(Path.GetTempPath(), $"vwk-shelltofile-{Guid.NewGuid():N}.txt");

        try
        {
            // Act: съём вывода в файл (скрипт — константа, аргументы списком).
            await ValkeyE2eEnvironment.ShellToFileAsync(
                "/bin/sh", ["-c", "echo stdout-marker; echo stderr-marker >&2"], path);
            var content = await File.ReadAllTextAsync(path, TestContext.Current.CancellationToken);

            // Assert: файл содержит оба маркера.
            content.Should().Contain("stdout-marker");
            content.Should().Contain("stderr-marker");
        }
        finally
        {
            File.Delete(path);
        }
    }
}
