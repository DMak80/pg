namespace AdminPanel.Etcd;

// Per-cluster admin-креды (arch/02 §11.1): панель читает admin_user/admin_password
// ТОЛЬКО для live-проб PING; в модель ValkeyClusterInfo/UI/API не выносит никогда.
// app-креды панель не читает вовсе (роль приложений).
public sealed record ValkeyClusterSecrets(string Cluster, string AdminUser, string AdminPassword);

// Внутренний стор кредов: заполняет ValkeySnapshotRefresher при тике, читает
// valkey-проба (spec §4.6). Значение пароля не покидает этот контур.
public interface IValkeySecretsStore
{
    IReadOnlyDictionary<string, ValkeyClusterSecrets> Current { get; }

    void Replace(IReadOnlyDictionary<string, ValkeyClusterSecrets> secrets);
}

public sealed class ValkeySecretsStore : IValkeySecretsStore
{
    private volatile IReadOnlyDictionary<string, ValkeyClusterSecrets> _current =
        new Dictionary<string, ValkeyClusterSecrets>();

    public IReadOnlyDictionary<string, ValkeyClusterSecrets> Current => _current;

    public void Replace(IReadOnlyDictionary<string, ValkeyClusterSecrets> secrets) => _current = secrets;
}
