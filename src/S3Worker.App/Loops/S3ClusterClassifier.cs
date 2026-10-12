using S3Worker.Core.Model;

namespace S3Worker.App.Loops;

// Классификация тика (arch/24 §5): тип работы над кластером по config.state.

/// <summary>NOT_INITIALIZED → Provision; TO_REMOVE → Deprovision; отсутствие state → Active;
/// битый/отсутствующий config → Skip; незнакомое state → Active (raw, arch/23 §2).</summary>
public enum S3ClusterKind
{
    /// <summary>config.state=NOT_INITIALIZED — ProvisioningProcess (P0–P5).</summary>
    Provision,

    /// <summary>config.state=TO_REMOVE — DeprovisioningProcess (X0–X3).</summary>
    Deprovision,

    /// <summary>Инициализированный кластер — Active-ветка: надзор (C) → add-node (D) →
    /// remove-node (E) → ротация.</summary>
    Active,

    /// <summary>Битый/отсутствующий config — пропуск с логом.</summary>
    Skip,
}

/// <summary>
/// Чистая функция классификации кластера по config.state (arch/24 §5):
/// NOT_INITIALIZED → Provision; TO_REMOVE → Deprovision; отсутствие state →
/// Active; Config == null (нет ключа/битый) → Skip; незнакомое state →
/// Active с raw-строкой (толерантность arch/23 §2 — система развивается).
/// </summary>
public static class S3ClusterClassifier
{
    public static S3ClusterKind Classify(S3ClusterSnapshot snapshot)
        => snapshot.Config?.State switch
        {
            null when snapshot.Config is null => S3ClusterKind.Skip,
            null => S3ClusterKind.Active,
            S3NodeStates.NotInitialized => S3ClusterKind.Provision,
            S3NodeStates.ToRemove => S3ClusterKind.Deprovision,
            _ => S3ClusterKind.Active, // незнакомое state — Active (raw)
        };
}
