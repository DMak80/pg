using Microsoft.Extensions.Logging;
using PgWorker.Core.Model;
using PgWorker.Core.Tuning;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// Фабрика входов PGTune (spec.md §4.3): расчёт тюнинга шарда от ОБЯЗАТЕЛЬНЫХ
/// заявок ресурсов (/service/&lt;scope&gt;/request_{cpu,mem} на ноду — arch/14
/// §2.1 п.4, панель пишет при создании шарда) и констант PgWorker:Pgtune.
/// Пересчёт на каждый EnsureNode-путь и каждый тик конвергенции DCS (arch/14
/// §5 C: параметры НЕ фиксируются в etcd — никакого состояния; повторный
/// вызов БЕЗ изменения входа возвращает кэшированный результат — тот же
/// расчёт, без повторного warning-лога: тик надзора ~1/с не должен штормить
/// журнал одинаковыми предупреждениями). Отсутствие/нечитаемость заявки —
/// исключение = фейл фазы тика (транзиент-ретрай с бэкоффом): воркер НЕ
/// выдумывает размер ноды, никаких дефолтов. Вызывается только держателем
/// клэйма &lt;C&gt; (контекст процессов); синглтон DI — тики кластеров идут
/// параллельно, кэш под блокировкой.
/// </summary>
public sealed class PgtuneInputsFactory(PgtuneSettings settings, ILogger<PgtuneInputsFactory> log)
{
    // Кэш «вход → результат»: вход — record со значимым равенством (заявки +
    // константы настроек). Различных входов в живой системе — считанные
    // единицы (по одному на набор заявок шардов); переполнение (патология
    // «заявка меняется каждый тик») — простая очистка: расчёт дёшев, корректность
    // не зависит от кэша.
    private readonly Dictionary<PgTuneInput, PgTuneResult> cache = new();
    private readonly object gate = new();

    /// <summary>Расчёт тюнинга шарда от обязательной заявки ресурсов;
    /// заявка нет — InvalidOperationException (фейл фазы, не тихий дефолт).</summary>
    public PgTuneResult Create(NodeResources? resources)
    {
        // Заявки ресурсов ОБЯЗАТЕЛЬНЫ (arch/14 §2.1 п.4): размер ноды знает
        // только etcd-заявка — выдумывать его нельзя ни при каких условиях.
        if (resources?.MemoryBytes is not { } memoryBytes)
            throw new InvalidOperationException(
                "pgtune: mandatory request_mem request is missing/unreadable — PGTune calculation is impossible " +
                "(the request is written by the panel when creating a shard; arch/14 §2.1)");
        if (resources.CpuCores is null)
            throw new InvalidOperationException(
                "pgtune: mandatory request_cpu request is missing/unreadable — PGTune calculation is impossible " +
                "(the request is written by the panel when creating a shard; arch/14 §2.1)");

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

        // Вход без изменений → кэшированный результат: повторного расчёта и
        // повторного warning-лога нет (шторм «одинаковые предупреждения каждый
        // тик надзора» исключён); вход изменился — расчёт свежий.
        lock (gate)
        {
            if (cache.TryGetValue(input, out var cached))
                return cached;
        }

        // Исключение НЕ глотается — уходит вверх (фейл фазы тика, транзиент-ретрай).
        var result = PgTune.Calculate(input);

        // Предупреждения PGTune — в warning-лог воркера, расчёт не блокируют
        // (spec.md §6: память > 100GB / < 256MB — только предупреждение).
        // Однократно на вход: повторные тики с теми же заявками молчаливы.
        foreach (var warning in result.Warnings.Where(w => w.Length > 0 && w != "WARNING"))
            log.LogWarning("pgtune: {Warning}", warning);

        lock (gate)
        {
            cache[input] = result;
            if (cache.Count > 64)
                cache.Clear();
        }

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
            $"pgtune: value '{value}' is outside the domain of {typeof(T).Name} (PgWorker:Pgtune)");
    }
}
