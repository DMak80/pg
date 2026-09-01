using PgWorker.Core;
using PgWorker.Core.Model;

namespace PgWorker.Provisioning.Processes;

/// <summary>Исход такта процесса (arch/14 §5): ожидание / цель достигнута / недостижимо без вмешательства.</summary>
public enum ProcessOutcome
{
    /// <summary>Продолжить следующими тиками (ждём Patroni, ключей панели, мастера).</summary>
    InProgress,

    /// <summary>Цель процесса достигнута (кластер в целевом состоянии).</summary>
    Done,

    /// <summary>Бюджет исчерпан / guard отказал — внимание оператора (journal.last_error).</summary>
    Failed,
}

/// <summary>
/// Исход тика надзора: обычный outcome + полностью мёртвые шарды (событие
/// эвакуации для цикла). Мёртвые шарды — ЗНАЧЕНИЕМ тика, не свойством
/// синглтона-процесса: кластеры обрабатываются параллельно и общее
/// mutable-состояние тиков перезаписывалось бы чужими кластерами (rework №1).
/// </summary>
public sealed record SuperviseOutcome(ProcessOutcome Outcome, IReadOnlyList<string> DeadShards);

/// <summary>
/// Один такт процесса-машины состояний: доводит кластер насколько возможно за
/// вызов (все фазы идемпотентны — повтор безопасен). Прогресс — в
/// /pgworker/work/&lt;C&gt; + nodes state; вход — снапшот кластера от цикла
/// (задача 23 ReconcileLoop), etcd/docker — через внедрённые зависимости.
/// </summary>
public interface IClusterProcess
{
    Task<Result<ProcessOutcome>> TickAsync(ClusterSnapshot snap, CancellationToken ct);
}

/// <summary>
/// Параметры размещения/бюджетов процессов (appsettings → задача 23):
/// диапазон портов нод (arch/14 §2.4), бюджет ожидания Patroni (P2.2, сек) и
/// бэкофф ретраев provision (arch/14 §5 A: Base·2^(n−1) с капом Max).
/// </summary>
public sealed record PlacementOptions(
    int PortFrom, int PortTo, int PatroniBootSec,
    int ProvisionRetryBaseSec = 5, int ProvisionRetryMaxSec = 60);

/// <summary>
/// Пороги надзора (spec §10 Thresholds → задача 23): нода мертва дольше
/// NodeDeadSec → rebuild (при кворуме и не-лидере); шард целиком мертв дольше
/// ShardDeadSec → эвакуация (arch/14 §5 C).
/// </summary>
public sealed record ThresholdsOptions(int NodeDeadSec, int ShardDeadSec);
