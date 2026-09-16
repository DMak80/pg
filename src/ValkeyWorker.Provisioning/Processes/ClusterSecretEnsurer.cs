using Shared.Core;
using Shared.Etcd.Client;
using ValkeyWorker.Core.Model;

namespace ValkeyWorker.Provisioning.Processes;

/// <summary>Per-cluster ACL-креды (arch/21 §4): пароли ролей admin/app.</summary>
public sealed record ValkeyCredentials(string AdminPassword, string AppPassword);

/// <summary>
/// Ensure per-cluster кредов (arch/21 §4): чтение
/// /valkey/clusters/&lt;C&gt;/{app_user,app_password,admin_user,admin_password};
/// отсутствующие ключи генерируются и кладутся ОДНОЙ txn put-if-absent
/// (compare NotExists только на отсутствующие). Проигрыш txn (гонка/re-run)
/// корректно разрешается re-read. Вызывается только держателем клэйма &lt;C&gt;.
/// </summary>
public interface IClusterSecretEnsurer
{
    Task<Result<ValkeyCredentials>> EnsureAsync(string cluster, CancellationToken ct);
}

public sealed class ClusterSecretEnsurer(IEtcdGateway etcd, string[] endpoints) : IClusterSecretEnsurer
{
    private const string DefaultAppUser = "app";
    private const string DefaultAdminUser = "admin";

    // Сырое чтение: null — ключ отсутствует (добирается txn-ом).
    private sealed record RawSecrets(
        string? AppUser, string? AppPassword, string? AdminUser, string? AdminPassword);

    public async Task<Result<ValkeyCredentials>> EnsureAsync(string cluster, CancellationToken ct)
    {
        var read = await ReadAsync(cluster, ct);
        if (!read.IsSuccess)
            return Result<ValkeyCredentials>.Failed(read.Error!);

        var current = read.Value;
        if (IsComplete(current))
            return Result<ValkeyCredentials>.Success(ToCredentials(current!));

        // Отсутствующие добираем txn NotExists: существующие не переписываем
        // (идемпотентность re-run).
        var missing = MissingKeys(cluster, current);
        var compare = missing.Select(k => TxnCompare.NotExists(k.Key)).ToList();
        var put = missing.Select(k => new TxnOp.Put(k.Key, k.Value, null)).ToList();

        // Txn с failover по endpoints. txn.IsSuccess=false — транспортный сбой;
        // проигрыш compare — законный исход put-if-absent, обрабатывается re-read.
        var txn = await TxnAsync(TxnRequest.Of(compare, put), ct);
        if (!txn.IsSuccess)
            return Result<ValkeyCredentials>.Failed(txn.Error!);

        // Re-read: txn мог проиграть (гонка) — актуальны существующие значения.
        var final = await ReadAsync(cluster, ct);
        if (!final.IsSuccess)
            return Result<ValkeyCredentials>.Failed(final.Error!);

        if (IsComplete(final.Value))
            return Result<ValkeyCredentials>.Success(ToCredentials(final.Value!));

        return Result<ValkeyCredentials>.Failed(new ApplicationException(
            $"ensure секретов кластера {cluster}: после txn ключи неполны " +
            $"(app_user: {final.Value?.AppUser is not null}, app_password: {final.Value?.AppPassword is not null}, " +
            $"admin_user: {final.Value?.AdminUser is not null}, admin_password: {final.Value?.AdminPassword is not null})"));
    }

    private static ValkeyCredentials ToCredentials(RawSecrets v)
        => new(v.AdminPassword!, v.AppPassword!);

    private static bool IsComplete(RawSecrets? s)
        => s is { } v
            && v.AppUser is { Length: > 0 }
            && v.AppPassword is { Length: > 0 }
            && v.AdminUser is { Length: > 0 }
            && v.AdminPassword is { Length: > 0 };

    // Ключи/значения отсутствующих: пароли — генератор 32 симв [A-Za-z0-9],
    // пользователи — канон arch/20 §2 ("app"/"admin").
    private static IReadOnlyList<(string Key, string Value)> MissingKeys(string cluster, RawSecrets? current)
    {
        var missing = new List<(string Key, string Value)>(4);
        if (current?.AppUser is null)
            missing.Add(($"/valkey/clusters/{cluster}/app_user", DefaultAppUser));
        if (current?.AppPassword is null)
            missing.Add(($"/valkey/clusters/{cluster}/app_password", ValkeyPasswordGenerator.Generate()));
        if (current?.AdminUser is null)
            missing.Add(($"/valkey/clusters/{cluster}/admin_user", DefaultAdminUser));
        if (current?.AdminPassword is null)
            missing.Add(($"/valkey/clusters/{cluster}/admin_password", ValkeyPasswordGenerator.Generate()));
        return missing;
    }

    // Чтение четырёх ключей с failover по endpoints.
    private async Task<Result<RawSecrets?>> ReadAsync(string cluster, CancellationToken ct)
    {
        Result? lastError = null;
        foreach (var endpoint in endpoints)
        {
            var read = await ReadFourAsync(endpoint, cluster, ct);
            if (!read.IsSuccess)
            {
                lastError = read;
                continue;
            }

            var v = read.Value!;
            return Result<RawSecrets?>.Success(new RawSecrets(
                TrimOrNull(v[0]), TrimOrNull(v[1]), TrimOrNull(v[2]), TrimOrNull(v[3])));
        }

        return Result<RawSecrets?>.Failed(lastError!.Error!);
    }

    // Чтение четырёх ключей одного endpoint'а: null-значения — ключ отсутствует.
    private async Task<Result<string?[]>> ReadFourAsync(string endpoint, string cluster, CancellationToken ct)
    {
        var keys = new[]
        {
            $"/valkey/clusters/{cluster}/app_user",
            $"/valkey/clusters/{cluster}/app_password",
            $"/valkey/clusters/{cluster}/admin_user",
            $"/valkey/clusters/{cluster}/admin_password",
        };
        var values = new string?[keys.Length];
        for (var i = 0; i < keys.Length; i++)
        {
            var kv = await etcd.GetAsync(endpoint, keys[i], ct);
            if (!kv.IsSuccess)
                return Result<string?[]>.Failed(kv.Error!);
            values[i] = kv.Value?.Value;
        }

        return Result<string?[]>.Success(values);
    }

    private async Task<Result<TxnResult>> TxnAsync(TxnRequest req, CancellationToken ct)
    {
        Result<TxnResult>? last = null;
        foreach (var endpoint in endpoints)
        {
            var result = await etcd.TxnAsync(endpoint, req, ct);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last!;
    }

    private static string? TrimOrNull(string? raw)
        => string.IsNullOrWhiteSpace(raw) ? null : raw.Trim();
}
