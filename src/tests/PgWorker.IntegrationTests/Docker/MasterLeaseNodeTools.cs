using PgWorker.IntegrationTests.E2e;

namespace PgWorker.IntegrationTests.Docker;

// Тестовый бинарь pgwnodetool для docker-серий: publish на хосте для
// linux-gnu (Debian .NET-раннер), self-contained без trimming. Файлы — в /tmp.
public static class MasterLeaseNodeTools
{
    private static readonly SemaphoreSlim Gate = new(1, 1);
    private static string? _dir;
    private static bool _ready;

    public static async Task<string> EnsureAsync(CancellationToken ct)
    {
        await Gate.WaitAsync(ct);
        try
        {
            if (_ready && _dir is { } path && File.Exists(Path.Combine(path, "pgwnodetool")))
                return path;

            var root = E2eFixture.FindRoot(AppContext.BaseDirectory);
            var rid = System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture
                == System.Runtime.InteropServices.Architecture.Arm64 ? "linux-arm64" : "linux-x64";
            var dir = Path.Combine(root, "artifacts", "node-tools-glibc");
            await E2eFixture.RunProcessAsync("dotnet",
            [
                "publish", $"{root}/src/PgWorker.NodeTools/PgWorker.NodeTools.csproj",
                "-c", "Release", "-r", rid, "--self-contained", "-o", dir, "--nologo",
            ], ct, timeout: TimeSpan.FromMinutes(5), env: E2eFixture.NoMsBuildReuseEnv,
                logFile: "/tmp/pgw-node-tools-musl-publish.log");
            _dir = dir;
            _ready = true;
            return dir;
        }
        finally
        {
            Gate.Release();
        }
    }
}
