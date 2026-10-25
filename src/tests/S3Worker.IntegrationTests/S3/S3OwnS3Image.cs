using System.Diagnostics;

namespace S3Worker.IntegrationTests.S3;

// Внешний образ owns3:dev (репозиторий ../OwnS3, сборка — deploy/build-image.sh
// там): pg его не собирает — проверка наличия раз на процесс тест-хоста,
// fail-fast с командой сборки. Freshness контролирует разработчик
// пересборкой в ../OwnS3 (устаревший образ маскирует регрессии).
internal static class S3OwnS3Image
{
    public const string Tag = "owns3:dev";

    private static readonly SemaphoreSlim Gate = new(1, 1);
    private static bool _ready;

    public static async Task EnsureOnceAsync(string artifactsDir, CancellationToken ct)
    {
        if (_ready)
            return;
        await Gate.WaitAsync(ct);
        try
        {
            if (_ready)
                return;
            // Список аргументов без shell: имя образа не попадает в строку команды.
            var inspect = new ProcessStartInfo("docker")
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
            };
            inspect.ArgumentList.Add("image");
            inspect.ArgumentList.Add("inspect");
            inspect.ArgumentList.Add(Tag);
            using var process = new Process { StartInfo = inspect };
            process.Start();
            await process.WaitForExitAsync(ct);
            if (process.ExitCode != 0)
                throw new InvalidOperationException(
                    $"image {Tag} is missing — build it in ../OwnS3: bash ../OwnS3/deploy/build-image.sh");
            Phase(artifactsDir, $"external image {Tag} found (repository ../OwnS3)");
            _ready = true;
        }
        finally
        {
            Gate.Release();
        }
    }

    private static void Phase(string artifactsDir, string message)
    {
        Console.WriteLine($"[PHASE] {message}");
        File.AppendAllText(Path.Combine(artifactsDir, "phases.log"), $"{DateTime.UtcNow:O} {message}\n");
    }
}
