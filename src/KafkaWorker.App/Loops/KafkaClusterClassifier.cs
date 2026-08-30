using KafkaWorker.Core.Model;

namespace KafkaWorker.App.Loops;

// Тип работы над кластером (arch/16 §5): классификация по config.state.

/// <summary>NOT_INITIALIZED → Provision; TO_REMOVE → Deprovision; иначе → Active.</summary>
public enum KafkaClusterWork
{
    /// <summary>config.state=NOT_INITIALIZED — ProvisioningProcess (K0–K6).</summary>
    Provision,

    /// <summary>config.state=TO_REMOVE — DeprovisioningProcess (X0–X3).</summary>
    Deprovision,

    /// <summary>Инициализированный кластер — надзор + converge (+ scale/ротация/TopicSync).</summary>
    Active,
}

/// <summary>
/// Чистая функция классификации кластера по config.state (arch/16 §5) +
/// кандидаты scale-прохода волны B: брокеры NOT_INITIALIZED (add) и
/// TO_REMOVE (remove) у Active-кластера.
/// </summary>
public static class KafkaClusterClassifier
{
    public static KafkaClusterWork Classify(KafkaClusterConfig config) => config.State switch
    {
        "NOT_INITIALIZED" => KafkaClusterWork.Provision,
        "TO_REMOVE" => KafkaClusterWork.Deprovision,
        _ => KafkaClusterWork.Active, // отсутствие state = Active (arch/15 §2.1)
    };

    /// <summary>Add-кандидаты Active-ветки: brokers/&lt;b&gt;/state=NOT_INITIALIZED (волна B).</summary>
    public static IReadOnlyList<string> AddCandidates(KafkaClusterSnapshot snap)
        => snap.Brokers.Where(b => b.State == "NOT_INITIALIZED").Select(b => b.Name).ToList();

    /// <summary>Remove-кандидаты Active-ветки: brokers/&lt;b&gt;/state=TO_REMOVE (волна B).</summary>
    public static IReadOnlyList<string> RemoveCandidates(KafkaClusterSnapshot snap)
        => snap.Brokers.Where(b => b.State == "TO_REMOVE").Select(b => b.Name).ToList();
}
