using ValkeyWorker.Core.Model;

namespace ValkeyWorker.App.Loops;

// Классификация тика (arch/21 §5): тип работы над кластером по config.state.

/// <summary>NOT_INITIALIZED → Provision; TO_REMOVE → Deprovision; отсутствие state → Active;
/// битый config → Skip; незнакомое state → Active (raw, arch/20 §5).</summary>
public enum ValkeyClusterKind
{
    /// <summary>config.state=NOT_INITIALIZED — ProvisioningProcess (V0–V5).</summary>
    Provision,

    /// <summary>config.state=TO_REMOVE — DeprovisioningProcess (X0–X3).</summary>
    Deprovision,

    /// <summary>Инициализированный кластер — надзор (C) → converge (D) → ротация (E).</summary>
    Active,

    /// <summary>Битый/отсутствующий config — пропуск с логом.</summary>
    Skip,
}

/// <summary>
/// Чистая функция классификации кластера по config.state (arch/21 §5):
/// NOT_INITIALIZED → Provision; TO_REMOVE → Deprovision; отсутствие state →
/// Active; Config == null (нет ключа/битый) → Skip; незнакомое state →
/// Active с raw-строкой (толерантность arch/20 §5 — система развивается).
/// </summary>
public static class ValkeyClusterClassifier
{
    public static ValkeyClusterKind Classify(ValkeyClusterSnapshot snapshot)
        => snapshot.Config?.State switch
        {
            null when snapshot.Config is null => ValkeyClusterKind.Skip,
            null => ValkeyClusterKind.Active,
            "NOT_INITIALIZED" => ValkeyClusterKind.Provision,
            "TO_REMOVE" => ValkeyClusterKind.Deprovision,
            _ => ValkeyClusterKind.Active, // незнакомое state — Active (raw)
        };
}
