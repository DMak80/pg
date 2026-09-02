namespace AdminPanel.Core;

// Статус health-пробы инстанса воркера (контракт arch/adminpanel/02 §3):
// панель опрашивает /healthz живых lease-ключей.
public enum WorkerHealthStatus
{
    /// <summary>/healthz отвечает 200 — процесс здоров.</summary>
    Healthy,

    /// <summary>/healthz ≠ 200 (503 degraded): процесс жив, но секции нездоровы.</summary>
    Degraded,

    /// <summary>Сетевой сбой/таймаут при живом lease-ключе.</summary>
    Unreachable,
}

/// <summary>
/// Результат одной health-пробы инстанса PgWorker (spec §3.4 D4): Url — адрес
/// lease-ключа, Detail — причина (HTTP-код или сетевая ошибка), null у Healthy.
/// </summary>
public sealed record WorkerHealth(
    string InstanceId,
    string Url,
    WorkerHealthStatus Status,
    DateTimeOffset CheckedAtUtc,
    string? Detail);

/// <summary>
/// Стор результатов опроса /healthz: poller пишет, refresher вносит готовым в
/// снапшот — KV-тик не блокируется (arch/adminpanel/02 §4; паттерн IProbeStateStore).
/// </summary>
public interface IWorkerHealthStore
{
    IReadOnlyList<WorkerHealth>? Current { get; }

    void Replace(IReadOnlyList<WorkerHealth> health);
}

/// <summary>
/// Стор результатов опроса /healthz инстансов KafkaWorker (t09; arch/adminpanel/02
/// §2.3.2): poller пишет, kafka-refresher вносит готовым в снапшот — KV-тик
/// не блокируется (симметрия IWorkerHealthStore).
/// </summary>
public interface IKafkaWorkerHealthStore
{
    IReadOnlyList<WorkerHealth>? Current { get; }

    void Replace(IReadOnlyList<WorkerHealth> health);
}
