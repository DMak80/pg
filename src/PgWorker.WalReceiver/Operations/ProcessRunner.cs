using System.Diagnostics;

namespace PgWorker.WalReceiver.Operations;

/// <summary>Прод-реализация IProcessRunner (arch/19 §3.4 «все child-процессы —
/// Process.Start»): полный слив stdout/stderr в память; при отмене — kill
/// дерева процессов (pg_basebackup/recovery не оставляют сирот).</summary>
public sealed class ProcessRunner : IProcessRunner
{
    public async Task<(int ExitCode, string StdOut, string StdErr)> RunAsync(
        ProcessStartInfo psi, CancellationToken ct)
    {
        psi.RedirectStandardOutput = true;
        psi.RedirectStandardError = true;
        psi.UseShellExecute = false;
        using var process = Process.Start(psi)!;
        using var reg = ct.Register(() =>
        {
            try { process.Kill(entireProcessTree: true); } catch { /* уже вышел */ }
        });
        var stdout = await process.StandardOutput.ReadToEndAsync(ct);
        var stderr = await process.StandardError.ReadToEndAsync(ct);
        await process.WaitForExitAsync(ct);
        return (process.ExitCode, stdout, stderr);
    }
}
