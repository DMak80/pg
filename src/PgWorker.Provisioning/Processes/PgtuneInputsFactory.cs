using Microsoft.Extensions.Logging;
using PgWorker.Core.Model;
using PgWorker.Core.Tuning;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// Фабрика входов PGTune (spec.md §4.3): расчёт тюнинга шарда от ОБЯЗАТЕЛЬНЫХ
/// заявок ресурсов (/service/&lt;scope&gt;/request_{cpu,mem} на ноду — arch/14
/// §2.1 п.4, панель пишет при создании шарда) и констант PgWorker:Pgtune.
/// Пересчёт на каждый EnsureNode-путь (решение пользователя: параметры НЕ
/// фиксируются в etcd — никакого состояния). Отсутствие/нечитаемость заявки —
/// исключение = фейл фазы тика (транзиент-ретрай с бэкоффом): воркер НЕ
/// выдумывает размер ноды, никаких дефолтов. Вызывается только держателем
/// клэйма &lt;C&gt; (контекст процессов).
/// </summary>
public sealed class PgtuneInputsFactory(PgtuneSettings settings, ILogger<PgtuneInputsFactory> log)
{
    /// <summary>Расчёт тюнинга шарда от обязательной заявки ресурсов;
    /// заявки нет — InvalidOperationException (фейл фазы, не тихий дефолт).</summary>
    public PgTuneResult Create(NodeResources? resources)
    {
        // Заявки ресурсов ОБЯЗАТЕЛЬНЫ (arch/14 §2.1 п.4): размер ноды знает
        // только etcd-заявка — выдумывать его нельзя ни при каких условиях.
        if (resources?.MemoryBytes is not { } memoryBytes)
            throw new InvalidOperationException(
                "pgtune: обязательная заявка request_mem отсутствует/нечитаема — расчёт PGTune невозможен " +
                "(заявку пишет панель при создании шарда; arch/14 §2.1)");
        if (resources.CpuCores is null)
            throw new InvalidOperationException(
                "pgtune: обязательная заявка request_cpu отсутствует/нечитаема — расчёт PGTune невозможен " +
                "(заявку пишет панель при создании шарда; arch/14 §2.1)");

        // Память: floor(MemoryBytes/1024) от etcd-заявки request_mem.
        var totalMemoryKb = memoryBytes / 1024;

        // CPU: floor(CpuCores); floor < 1 (субъядерная заявка "0.5") → cpuNum
        // не задан — алгоритм честно пропускает параллельные/autovacuum/io_workers
        // (§4.0/§4.12 спецификации алгоритма).
        int? cpuNum = (int)Math.Floor(resources.CpuCores.Value);
        if (cpuNum is < 1)
            cpuNum = null;

        var input = new PgTuneInput(
            settings.DbVersion,
            PgTuneOsType.Linux, // контейнеры pgworker-node — всегда Linux
            MapDomain<PgTuneDbType>(settings.DbType),
            totalMemoryKb,
            PgTuneMemoryUnit.KB,
            cpuNum,
            settings.Connections,
            MapDomain<PgTuneHdType>(settings.HdType),
            MapDomain<PgTuneDbSize>(settings.DbSize));

        // Исключение НЕ глотается — уходит вверх (фейл фазы тика, транзиент-ретрай).
        var result = PgTune.Calculate(input);

        // Предупреждения PGTune — в warning-лог воркера, расчёт не блокируют
        // (spec.md §6: память > 100GB / < 256MB — только предупреждение).
        foreach (var warning in result.Warnings.Where(w => w.Length > 0 && w != "WARNING"))
            log.LogWarning("pgtune: {Warning}", warning);

        return result;
    }

    /// <summary>Маппинг строки настроек в enum ядра (case-insensitive; имена с
    /// подчёркиванием — less_ram/mid_ram/greater_ram — нормализуются). Незнакомая
    /// строка → InvalidOperationException: fail-fast расчёта (валидация старта
    /// уже отсекла мусор — это защита от обхода IsValid).</summary>
    private static T MapDomain<T>(string value) where T : struct, Enum
    {
        var normalized = value.Trim().Replace("_", "").Replace("-", "");
        foreach (var name in Enum.GetNames<T>())
        {
            if (string.Equals(name, normalized, StringComparison.OrdinalIgnoreCase))
                return Enum.Parse<T>(name);
        }

        throw new InvalidOperationException(
            $"pgtune: значение '{value}' не в домене {typeof(T).Name} (PgWorker:Pgtune)");
    }
}
