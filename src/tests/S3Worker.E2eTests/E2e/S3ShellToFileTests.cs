using FluentAssertions;
using Xunit;

namespace S3Worker.IntegrationTests.E2e;

// ShellToFileAsync телеметрии обязан писать ОБА потока процесса (stdout+
// stderr, канон docs/e2e-launch.md §1): лёгкий факт без docker — /bin/sh
// пишет в оба потока, файл содержит оба блока.
public sealed class S3ShellToFileTests
{
    [Fact]
    public async Task ShellToFileAsync_WritesBothStreams()
    {
        // Arrange: temp-файл артефакта; процесс пишет в оба потока.
        var path = Path.Combine(Path.GetTempPath(), $"s3w-shelltofile-{Guid.NewGuid():N}.txt");

        try
        {
            // Act: съём вывода в файл (скрипт — константа, аргументы списком).
            await S3E2eEnvironment.ShellToFileAsync(
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
