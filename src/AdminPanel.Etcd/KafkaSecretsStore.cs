namespace AdminPanel.Etcd;

// Per-cluster SASL-креды (arch/02 §10.1): панель читает app_user/app_password
// ТОЛЬКО для проб; в модель KafkaClusterInfo/UI/API не выносит никогда.
public sealed record KafkaClusterSecrets(string Cluster, string User, string Password);

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
