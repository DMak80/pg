using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Core.Templates;
using KafkaWorker.Etcd.Client;

namespace KafkaWorker.Provisioning.Processes;

/// <summary>
/// Per-cluster секреты безопасности (arch/15 §2, t03): креды ролей
/// app (приложения, ACL) и admin (воркер/панель/CLI) + per-cluster PKI
/// (публичный серт CA и приватный ключ CA, PEM одной строкой).
/// </summary>
public sealed record ClusterSecrets(
    string AppUser,
    string AppPassword,
    string AdminUser,
    string AdminPassword,
    string CaPem,
    string CaKey);

/// <summary>
/// Ensure per-cluster секретов кластера (arch/16 §4, t03): чтение
/// /kafka/clusters/&lt;C&gt;/{app_user,app_password,admin_user,admin_password,ca_pem,ca_key};
/// отсутствующие ключи генерируются и кладутся ОДНОЙ txn put-if-absent
/// (compare NotExists только на отсутствующие). Проигрыш txn (гонка/re-run)
/// корректно разрешается re-read. CA генерируется ClusterPki (случайно, не из
/// сида — компрометация одного кластера не открывает остальные). Вызывается
/// только держателем клэйма &lt;C&gt;.
/// </summary>
public interface IClusterSecretEnsurer
{
    Task<Result<ClusterSecrets>> EnsureAsync(string cluster, CancellationToken ct);
}

public sealed class ClusterSecretEnsurer(IEtcdGateway etcd, string[] endpoints) : IClusterSecretEnsurer
{
    private const string DefaultAppUser = "app";
    private const string DefaultAdminUser = "admin";

    // Сырое чтение: null — ключ отсутствует (добирается txn-ом).
    private sealed record RawSecrets(
        string? AppUser, string? AppPassword, string? AdminUser,
        string? AdminPassword, string? CaPem, string? CaKey);

    public async Task<Result<ClusterSecrets>> EnsureAsync(string cluster, CancellationToken ct)
    {
        var read = await ReadAsync(cluster, ct);
        if (!read.IsSuccess)
            return Result<ClusterSecrets>.Failed(read.Error!);

        var current = read.Value;
        if (IsComplete(current))
            return Result<ClusterSecrets>.Success(ToSecrets(current!));

        // Отсутствующие добираем txn NotExists: существующие не переписываем
        // (идемпотентность re-run); CA — один вызов даёт и серт, и ключ.
        var missing = MissingKeys(cluster, current);
        var compare = missing.Select(k => TxnCompare.NotExists(k.Key)).ToList();
        var put = missing.Select(k => new TxnOp.Put(k.Key, k.Value, null)).ToList();

        // Txn с failover по endpoints: упавший endpoint → следующий.
        // txn.IsSuccess=false — транспортный сбой; проигрыш compare — законный
        // исход put-if-absent, обрабатывается re-read ниже.
        var txn = await TxnAsync(TxnRequest.Of(compare, put), ct);
        if (!txn.IsSuccess)
            return Result<ClusterSecrets>.Failed(txn.Error!);

        // Re-read: txn мог проиграть (гонка) — актуальны существующие значения.
        var final = await ReadAsync(cluster, ct);
        if (!final.IsSuccess)
            return Result<ClusterSecrets>.Failed(final.Error!);

        if (IsComplete(final.Value))
            return Result<ClusterSecrets>.Success(ToSecrets(final.Value!));

        return Result<ClusterSecrets>.Failed(new ApplicationException(
            $"ensure секретов кластера {cluster}: после txn ключи неполны " +
            $"(app_user: {final.Value?.AppUser is not null}, app_password: {final.Value?.AppPassword is not null}, " +
            $"admin_user: {final.Value?.AdminUser is not null}, admin_password: {final.Value?.AdminPassword is not null}, " +
            $"ca_pem: {final.Value?.CaPem is not null}, ca_key: {final.Value?.CaKey is not null})"));
    }

    private static ClusterSecrets ToSecrets(RawSecrets v)
        => new(v.AppUser!, v.AppPassword!, v.AdminUser!, v.AdminPassword!, v.CaPem!, v.CaKey!);

    private static bool IsComplete(RawSecrets? s)
        => s is { } v
            && v.AppUser is { Length: > 0 }
            && v.AppPassword is { Length: > 0 }
            && v.AdminUser is { Length: > 0 }
            && v.AdminPassword is { Length: > 0 }
            && v.CaPem is { Length: > 0 }
            && v.CaKey is { Length: > 0 };

    // Ключи/значения отсутствующих: пароли — генератор 32 симв [A-Za-z0-9],
    // CA — per-cluster self-signed (архив/16 §2.3), пользователи — канон
    // arch/15 §2 ("app"/"admin").
    private static IReadOnlyList<(string Key, string Value)> MissingKeys(string cluster, RawSecrets? current)
    {
        var missing = new List<(string Key, string Value)>(6);
        if (current?.AppUser is null)
            missing.Add(($"/kafka/clusters/{cluster}/app_user", DefaultAppUser));
        if (current?.AppPassword is null)
            missing.Add(($"/kafka/clusters/{cluster}/app_password", KafkaPasswordGenerator.Generate()));
        if (current?.AdminUser is null)
            missing.Add(($"/kafka/clusters/{cluster}/admin_user", DefaultAdminUser));
        if (current?.AdminPassword is null)
            missing.Add(($"/kafka/clusters/{cluster}/admin_password", KafkaPasswordGenerator.Generate()));
        if (current?.CaPem is null || current?.CaKey is null)
        {
            var (caPem, caKeyPem) = ClusterPki.GenerateCa(cluster);
            if (current?.CaPem is null)
                missing.Add(($"/kafka/clusters/{cluster}/ca_pem", caPem));
            if (current?.CaKey is null)
                missing.Add(($"/kafka/clusters/{cluster}/ca_key", caKeyPem));
        }

        return missing;
    }

    // Чтение шести ключей с failover по endpoints.
    private async Task<Result<RawSecrets?>> ReadAsync(string cluster, CancellationToken ct)
    {
        Result? lastError = null;
        foreach (var endpoint in endpoints)
        {
            var read = await ReadSixAsync(endpoint, cluster, ct);
            if (!read.IsSuccess)
            {
                lastError = read;
                continue;
            }

            var v = read.Value!;
            return Result<RawSecrets?>.Success(new RawSecrets(
                TrimOrNull(v[0]), TrimOrNull(v[1]), TrimOrNull(v[2]),
                TrimOrNull(v[3]), TrimOrNull(v[4]), TrimOrNull(v[5])));
        }

        return Result<RawSecrets?>.Failed(lastError!.Error!);
    }

    // Чтение шести ключей одного endpoint'а: null-значения — ключ отсутствует.
    private async Task<Result<string?[]>> ReadSixAsync(string endpoint, string cluster, CancellationToken ct)
    {
        var keys = new[]
        {
            $"/kafka/clusters/{cluster}/app_user",
            $"/kafka/clusters/{cluster}/app_password",
            $"/kafka/clusters/{cluster}/admin_user",
            $"/kafka/clusters/{cluster}/admin_password",
            $"/kafka/clusters/{cluster}/ca_pem",
            $"/kafka/clusters/{cluster}/ca_key",
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
