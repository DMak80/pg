using System.Diagnostics;

namespace PgWorker.WalReceiver.Operations;

/// <summary>Абстракция запуска child-процессов операций бэкапов (PG-утилиты):
/// полный слив stdout/stderr, kill дерева процессов при отмене. Тесты — фейки.</summary>
public interface IProcessRunner
{
    Task<(int ExitCode, string StdOut, string StdErr)> RunAsync(
        ProcessStartInfo psi, CancellationToken ct);
}
