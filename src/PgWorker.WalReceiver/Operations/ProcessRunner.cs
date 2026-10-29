using System.Diagnostics;

namespace PgWorker.WalReceiver.Operations;

/// <summary>Прод-реализация IProcessRunner (arch/19 §3.4 «все child-процессы —
/// Process.Start»): полный слив stdout/stderr в память — ОБА потока читаются
/// параллельно (дочерний пишет stderr одновременно с stdout: последовательное
/// чтение при многострочном stderr упирается в заполненный пайп-буфер и
/// deadlock'ает child); при отмене — kill дерева процессов
/// (pg_basebackup/recovery не оставляют сирот).</summary>
public sealed class ProcessRunner : IProcessRunner
{
    public async Task<(int ExitCode, string StdOut, string StdErr)> RunAsync(
        ProcessStartInfo psi, CancellationToken ct)
    {
        psi.RedirectStandardOutput = true;
        psi.RedirectStandardError = true;
        psi.UseShellExecute = false;
        if (psi.UseShellExecute || psi.Arguments.Length > 0)
        {
            // Инвариант безопасности: только ArgumentList (никакой строки Arguments
            // и никакого shell) — аргументы не интерпретируются интерпретатором.
            throw new InvalidOperationException(
                "ProcessRunner требует ArgumentList и UseShellExecute=false (без shell).");
        }
        using var process = Process.Start(psi)!;
        using var reg = ct.Register(() =>
        {
            try { process.Kill(entireProcessTree: true); } catch { /* уже вышел */ }
        });
        var stdoutTask = process.StandardOutput.ReadToEndAsync(ct);
        var stderrTask = process.StandardError.ReadToEndAsync(ct);
        await Task.WhenAll(stdoutTask, stderrTask);
        await process.WaitForExitAsync(ct);
        return (process.ExitCode, stdoutTask.Result, stderrTask.Result);
    }
}
