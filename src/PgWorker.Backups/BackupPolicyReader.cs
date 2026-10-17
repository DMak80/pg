using PgWorker.Etcd.Parsing;
using Shared.Core;
using Shared.Etcd.Client;

namespace PgWorker.Backups;

/// <summary>Читатель policy-ключа кластера (t36, arch/19 §4): живое чтение
/// /pgworker/backups/&lt;C&gt;/policy (failover по endpoints — паттерн WalStreamProcess
/// GetAsync), разбор BackupsParser'ом. Потребители — BackupPoolResolver/фабрика;
/// ключа нет → null (дефолт-семантика у вызывающего).</summary>
public sealed class BackupPolicyReader(IEtcdGateway etcd, string[] endpoints)
{
    public async Task<BackupPolicy?> ReadAsync(string cluster, CancellationToken ct)
    {
        Result<Kv?> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, $"/pgworker/backups/{cluster}/policy", ct);
            if (!result.IsSuccess)
            {
                last = result;
                continue;
            }

            if (result.Value is null)
                return null; // ключа нет — policy нет (не ошибка)
            var parsed = BackupsParser.Parse([result.Value], out _);
            return parsed.Value.FirstOrDefault(c => c.Cluster == cluster)?.Policy;
        }

        throw last.Error ?? new ApplicationException($"policy {cluster}: etcd endpoints не заданы");
    }
}
