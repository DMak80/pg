using KafkaWorker.Core.Model;

namespace KafkaWorker.Core.Planning;

/// <summary>
/// Фактические лимиты контейнера/сервиса брокера из docker inspect
/// (t06, spec §5.3): 0 = лимит не задан.
/// </summary>
public sealed record NodeLimits(long NanoCpus, long MemoryBytes);

/// <summary>
/// Сверка лимитов контейнера с декларацией brokers/&lt;b&gt;/resources
/// (t06, spec §5.2 J2 / §5.3). Формула cpu ПОКАЗАТЕЛЬНО повторяет
/// арифметику ЗАПИСИ DockerEngine.BuildContainerBody: spec.CpuCores
/// (decimal) → KafkaNodeSpec.CpuCores (double) →
/// (long)(cores * 1_000_000_000). Каст в double ДО умножения обязателен:
/// decimal-арифметика (long)(cpu * 1e9m) для значений, непредставимых
/// точно в double (0.01, 1.15), расходится с фактом инспекта → вечный
/// цикл регенерации (ревью Фазы 4, замечание 4). mem — целые GiB, сдвиг
/// точен в обеих арифметиках. disk не сверяется (инфо-поле, квот нет).
/// </summary>
public static class NodeRegenPlanner
{
    public static long ExpectedNanoCpus(decimal cpu)
        => (long)((double)cpu * 1_000_000_000);

    public static long ExpectedMemoryBytes(int memGi)
        => (long)memGi * 1024 * 1024 * 1024;

    public static bool NeedsRegen(BrokerResources decl, NodeLimits actual)
        => actual.NanoCpus != ExpectedNanoCpus(decl.Cpu)
           || actual.MemoryBytes != ExpectedMemoryBytes(decl.MemGi);
}
