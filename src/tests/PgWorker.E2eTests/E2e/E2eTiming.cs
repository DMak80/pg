namespace PgWorker.IntegrationTests.E2e;

/// <summary>[TIMING]-журнал производительности контура A (машинно-читаемый:
/// одна строка = один шаг). Формат строки:
/// [TIMING] &lt;utc&gt; &lt;scope&gt; step=&lt;step&gt; elapsed=&lt;s.s&gt;s — в Console (журнал
/// факта/раннера) и в timings.log артефактов; классовые шаги — дополнительно
/// в глобальный /tmp/pgw-e2e-timings.log (агрегация по прогону).</summary>
public static class E2eTiming
{
    /// <summary>Начало класса-контура: возвращает секундомер сождласса.</summary>
    public static System.Diagnostics.Stopwatch StartClass(string slug, string fqn)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        Emit(slug, "class-start", 0, $"fqn={fqn}");
        return sw;
    }

    /// <summary>Закрытие класса: итоговая длительность.</summary>
    public static void FinishClass(System.Diagnostics.Stopwatch sw, string fqn)
        => Emit("class", "class-finish", sw.Elapsed.TotalSeconds, $"fqn={fqn}");

    /// <summary>Шаг: одна строка [TIMING] с elapsed от старта секундомера
    /// (последовательные шаги — elapsed от начала; для длительности самого
    /// шага вызывающий передаёт собственный св).</summary>
    public static void Step(System.Diagnostics.Stopwatch sw, string step)
        => Emit("class", step, sw.Elapsed.TotalSeconds);

    /// <summary>Шаг произвольного scope с собственной длительностью (сек).</summary>
    public static void Emit(string scope, string step, double seconds, string? extra = null)
    {
        var line = $"[TIMING] {DateTime.UtcNow:HH:mm:ss.fff} {scope} step={step}"
                   + $" elapsed={seconds.ToString("F1", System.Globalization.CultureInfo.InvariantCulture)}s"
                   + (string.IsNullOrEmpty(extra) ? "" : $" {extra}");
        Console.WriteLine(line);
        try
        {
            File.AppendAllText("/tmp/pgw-e2e-timings.log", line + Environment.NewLine);
        }
        catch
        {
            // файловая телеметрия — «лучшими усилиями»
        }
    }
}
