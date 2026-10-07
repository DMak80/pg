namespace Shared.Core.Hosting;

/// <summary>Узкий интерфейс доставки прогресс-отметок долгих фаз reconcile в
/// глубину процессов (Provisioning/Moves ниже App и HealthState не видят):
/// реализация — HealthState воркера (Mark() = MarkReconcileActivity),
/// регистрируется app; вызовы — микросекунды, без исключений.</summary>
public interface ILoopProgress
{
    /// <summary>Прогресс-отметка: итерация жива (активность без тика).</summary>
    void Mark();
}
