using PgWorker.Backups;
using PgWorker.Etcd.Parsing;
using PgWorker.Etcd.S3;
using Shared.Core;
using Shared.Etcd.Client;

namespace PgWorker.WalReceiver;

/// <summary>Результат резолва «куда бэкапиться» (arch/19 §3, живой из etcd на
/// каждом цикле приёмника): Password — /clusters/&lt;C&gt;/backup_password;
/// Pool — контекст пула (write-списки по роли, ReadAll, креды); Slot — имя
/// слота шарда (BackupNames.Slot). WaitingPassword — пароля нет/пуст;
/// WaitingPolicy — policy без s3/Failed-резолв (текст — в Error); оба —
/// transient-ожидания, ретрай следующим циклом.</summary>
public sealed record WalReceiverTargetsResult(
    string? Password, BackupPoolContext? Pool, string? Slot, string? Error)
{
    public bool WaitingPassword => Password is null;
    public bool WaitingPolicy => Pool is null;

    public static WalReceiverTargetsResult PasswordMissing() => new(null, null, null, null);

    public static WalReceiverTargetsResult PolicyMissing(string? error) => new("", null, null, error);

    public static WalReceiverTargetsResult Ok(string password, BackupPoolContext pool, string slot)
        => new(password, pool, slot, null);
}

/// <summary>Резолв кредов/целей приёмника из etcd (arch/19 §3, решение spec §7
/// №10): пароль роли backup_exec, policy → секция s3 → пул — тот же резолв,
/// что у воркера (BackupPoolResolver + S3PoolStore + BackupPolicyReader из
/// PgWorker.Backups). Транспортный отказ etcd — WaitingPolicy с текстом
/// (ретрай-ожидание: из отличить «нет policy» не нужно — обе ветки ждут).</summary>
public sealed class WalReceiverTargets(IEtcdGateway etcd, string[] endpoints, IS3PoolStore pools)
{
    public async Task<WalReceiverTargetsResult> ResolveAsync(
        string cluster, string shard, CancellationToken ct)
    {
        // (1) Пароль роли backup_exec (arch/19 §7): ключ есть и непуст → ок;
        // отсутствует/пуст → waiting_password (ensure делает воркер — ожидание).
        var password = await ReadPasswordAsync(cluster, ct);
        if (!password.IsSuccess)
            return WalReceiverTargetsResult.PolicyMissing(password.Error!.Message);
        if (string.IsNullOrWhiteSpace(password.Value))
            return WalReceiverTargetsResult.PasswordMissing();

        // (2) Policy → пул: тот же резолв, что у воркера (BackupPoolResolver).
        // Success(null) — пул не выбран (нет policy/секции s3 — подсистема
        // кластера молчит); Failed — transient-диагностика (пул не найден,
        // TO_REMOVE, нет secrets, списки пусты/битые) — текст в Error.
        try
        {
            var policyReader = new BackupPolicyReader(etcd, endpoints);
            var resolver = new BackupPoolResolver(pools, policyReader.ReadAsync);
            var resolved = await resolver.ResolveAsync(cluster, ct);
            if (!resolved.IsSuccess)
                return WalReceiverTargetsResult.PolicyMissing(resolved.Error!.Message);
            if (resolved.Value is not { } pool)
                return WalReceiverTargetsResult.PolicyMissing(null);

            return WalReceiverTargetsResult.Ok(
                password.Value!, pool, BackupNames.Slot(cluster, shard));
        }
        catch (Exception e)
        {
            // BackupPolicyReader бросает при транспортном отказе etcd —
            // transient-ожидание с диагностикой (ретрай следующим циклом).
            return WalReceiverTargetsResult.PolicyMissing(
                $"targets resolve failed: {e.Message.Split('\n')[0]}");
        }
    }

    // GET /clusters/<C>/backup_password — failover-перебор endpoints
    // (образец WalStatusWriter.ReadAsync); Failed — транспорт etcd.
    private async Task<Result<string?>> ReadPasswordAsync(string cluster, CancellationToken ct)
    {
        Result<Kv?> last = default;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.GetAsync(endpoint, $"/clusters/{cluster}/backup_password", ct);
            if (!result.IsSuccess)
            {
                last = result; // отказ — пробуем следующий endpoint (failover)
                continue;
            }
            return Result<string?>.Success(result.Value?.Value);
        }

        return Result<string?>.Failed(last.Error
            ?? new EtcdUnreachableException("etcd endpoints are not set"));
    }
}
