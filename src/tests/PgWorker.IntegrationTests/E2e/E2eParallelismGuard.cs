namespace PgWorker.IntegrationTests.E2e;

/// <summary>Рельса канона параллелизма E2E (t24, docs/e2e-launch.md §5):
/// потолок — 5 одновременно живых контуров (выбран приёмкой N=3/5/6 —
/// решение пользователя). Считает ФАКТИЧЕСКИ живые окружения процесса:
/// при превышении канона один раз на процесс пишет громкое предупреждение
/// в stderr и /tmp/pgw-e2e-static-phase.log. Жёсткий запрет не вводится —
/// диагностические прогоны с переопределённым потолком N&gt;5 легальны,
/// но обязаны быть громкими.</summary>
internal static class E2eParallelismGuard
{
    internal const int CanonMaxLiveEnvironments = 5;

    private static int _live;
    private static int _warned;

    /// <summary>Вызывается E2eEnvironment после успешного подъёма окружения.
    /// Возвращает текущее число живых окружений (диагностика стартовых строк).</summary>
    public static int OnEnvironmentStarted(string slug)
    {
        var live = Interlocked.Increment(ref _live);
        if (live > CanonMaxLiveEnvironments && Interlocked.Exchange(ref _warned, 1) == 0)
        {
            var line = $"{DateTime.UtcNow:HH:mm:ss} [E2E-PARALLELISM] e2e[{slug}]: живых E2E-контуров {live} > канона"
                + $" {CanonMaxLiveEnvironments} (xunit.runner.json maxParallelThreads / docs/e2e-launch.md «Параллелизм и нагрузка»)"
                + " — прогон ВНЕ канона: вероятна деградация docker-хоста (DNS/сети); результаты требуют этой пометки.";
            Console.Error.WriteLine(line);
            try
            {
                File.AppendAllText("/tmp/pgw-e2e-static-phase.log", line + Environment.NewLine);
            }
            catch
            {
                // файловая телеметрия — «лучшими усилиями», не роняет серию
            }
        }

        return live;
    }

    /// <summary>Вызывается E2eEnvironment.DisposeAsync при любом исходе.</summary>
    public static void OnEnvironmentDisposed()
        => Interlocked.Decrement(ref _live);
}
