using PgWorker.Core.Model;

namespace PgWorker.App.Loops;

// Тип работы над кластером (spec §6.2, arch/14 §4): классификация по config.state.

/// <summary>NOT_INITIALIZED → Provision; TO_REMOVE → Deprovision; иначе → Supervise.</summary>
public enum ClusterWork
{
    /// <summary>config.state=NOT_INITIALIZED — ProvisioningProcess (P0–P5).</summary>
    Provision,

    /// <summary>config.state=TO_REMOVE — DeprovisioningProcess (D0–D3).</summary>
    Deprovision,

    /// <summary>Инициализированный кластер (state отсутствует/иное) — NodeSupervisor.</summary>
    Supervise,
}

/// <summary>Чистая функция классификации кластера по config.state (таблица spec §6.2).</summary>
public static class ClusterClassifier
{
    public static ClusterWork Classify(ClusterConfig config) => config.State switch
    {
        ClusterState.NotInitialized => ClusterWork.Provision,
        ClusterState.ToRemove => ClusterWork.Deprovision,
        _ => ClusterWork.Supervise,
    };
}
