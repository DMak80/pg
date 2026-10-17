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
            var inspect = new ProcessStartInfo("docker", $"image inspect {Tag}")
            {
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
            };
            using var process = Process.Start(inspect)!;
            await process.WaitForExitAsync(ct);
            if (process.ExitCode != 0)
                throw new InvalidOperationException(
                    $"образ {Tag} отсутствует — собери в ../OwnS3: bash ../OwnS3/deploy/build-image.sh");
            Phase(artifactsDir, $"внешний образ {Tag} найден (репозиторий ../OwnS3)");
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
