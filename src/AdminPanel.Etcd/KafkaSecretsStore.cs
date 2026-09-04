namespace AdminPanel.Etcd;

// Per-cluster admin-креды + CA (arch/02 §10.1, t03): панель читает
// admin_user/admin_password/ca_pem ТОЛЬКО для SASL_SSL-проб; в модель
// KafkaClusterInfo/UI/API не выносит никогда. ca_key и app-креды панель
// не читает (app — роль приложений).
public sealed record KafkaClusterSecrets(string Cluster, string AdminUser, string AdminPassword, string CaPem);

// Внутренний стор кредов: заполняет KafkaSnapshotRefresher при тике, читает
// kafka-проба (B6). Значение пароля не покидает этот контур.
public interface IKafkaSecretsStore
{
    IReadOnlyDictionary<string, KafkaClusterSecrets> Current { get; }

    void Replace(IReadOnlyDictionary<string, KafkaClusterSecrets> secrets);
}

public sealed class KafkaSecretsStore : IKafkaSecretsStore
{
    private volatile IReadOnlyDictionary<string, KafkaClusterSecrets> _current =
        new Dictionary<string, KafkaClusterSecrets>();

    public IReadOnlyDictionary<string, KafkaClusterSecrets> Current => _current;

    public void Replace(IReadOnlyDictionary<string, KafkaClusterSecrets> secrets) => _current = secrets;
}
