using System.Text;

namespace PgWorker.IntegrationTests.E2e;

/// <summary>Файловый лок подготовки статики `artifacts/e2e/.prepare.lock`:
/// взаимное исключение ЗАПИСИ в publish-каталоги одного worktree (НЕ глобальный
/// лок запуска серий; тестируемые процессы лок не берут). Занят — поллинг
/// шагом 5 с, общий бюджет 10 минут; pid/started владельца пишется В ОТКРЫТЫЙ
/// лок-файл — через тот же удерживаемый FileStream, что держит лок (отдельное
/// открытие пути при удерживаемом FileShare.None конфликтует само с собой);
/// освобождение — Dispose потока-владельца (finally).</summary>
internal static class E2ePrepareLock
{
    public static readonly TimeSpan PollStep = TimeSpan.FromSeconds(5);
    public static readonly TimeSpan TotalBudget = TimeSpan.FromMinutes(10);

    /// <summary>Захват лока. Занят (IOException от FileShare.None; .NET
    /// enforce'ит его на Unix через flock — конфликт действует и между
    /// потоками одного процесса) — ждёт до бюджета; истёкший бюджет —
    /// исходный IOException наружу (вызывающий оборачивает в
    /// ApplicationException; при живом держателе лок-файл нечитаем —
    /// FileShare.None — и сообщение деградирует до holder: &lt;unreadable&gt;
    /// (оговорка ниже, после PrepareStaticAssetsAsync).</summary>
    public static async Task<FileStream> AcquireAsync(string lockPath, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + TotalBudget;
        while (true)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(lockPath)!);
            FileStream? stream = null;
            try
            {
                stream = new FileStream(
                    lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
                stream.SetLength(0);
                var payload = Encoding.UTF8.GetBytes(
                    $"pid={Environment.ProcessId} started={DateTime.UtcNow:O}");
                await stream.WriteAsync(payload, ct);
                await stream.FlushAsync(ct);
                return stream;
            }
            catch (IOException) when (DateTime.UtcNow < deadline)
            {
                // Хэндл освобождаем ДО retry: «утёкший» stream продолжал бы
                // держать лок до конца процесса.
                if (stream is not null)
                    await stream.DisposeAsync();
                await Task.Delay(PollStep, ct);
            }
            catch
            {
                if (stream is not null)
                    await stream.DisposeAsync();
                throw;
            }
        }
    }
}
