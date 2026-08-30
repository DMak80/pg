using KafkaWorker.Core;
using KafkaWorker.Core.Model;
using KafkaWorker.Etcd.Client;

namespace KafkaWorker.Provisioning.Processes;

/// <summary>Per-cluster SASL-креды (arch/15 §2: app_user="app" + app_password).</summary>
public sealed record KafkaSecrets(string User, string Password);

/// <summary>
/// Ensure per-cluster SASL-секрета (arch/16 §4, порт P1.5 PgWorker): чтение
/// /kafka/clusters/&lt;C&gt;/{app_user,app_password}; отсутствующие ключи генерируются
/// и кладутся ОДНОЙ txn put-if-absent (compare NotExists только на отсутствующие).
/// Проигрыш txn (гонка/re-run) корректно разрешается re-read. Вызывается только
/// держателем клэйма &lt;C&gt;.
/// </summary>
public interface IAppSecretEnsurer
{
    Task<Result<KafkaSecrets>> EnsureAsync(string cluster, CancellationToken ct);
}

public sealed class AppSecretEnsurer(IEtcdGateway etcd, string[] endpoints) : IAppSecretEnsurer
{
    private const string DefaultAppUser = "app";

    public async Task<Result<KafkaSecrets>> EnsureAsync(string cluster, CancellationToken ct)
    {
        var read = await ReadAsync(cluster, ct);
        if (!read.IsSuccess)
            return Result<KafkaSecrets>.Failed(read.Error!);

        var (user, password) = read.Value;
        if (user is { Length: > 0 } && password is { Length: > 0 })
            return Result<KafkaSecrets>.Success(new KafkaSecrets(user, password));

        // Отсутствующие добираем txn NotExists: существующие не переписываем
        // (идемпотентность re-run).
        var newUser = user ?? DefaultAppUser;
        var newPassword = password ?? KafkaPasswordGenerator.Generate();
        var compare = new List<TxnCompare>();
        var put = new List<TxnOp>();
        if (user is null)
        {
            compare.Add(TxnCompare.NotExists(UserKey(cluster)));
            put.Add(new TxnOp.Put(UserKey(cluster), newUser, null));
        }

        if (password is null)
        {
            compare.Add(TxnCompare.NotExists(PasswordKey(cluster)));
            put.Add(new TxnOp.Put(PasswordKey(cluster), newPassword, null));
        }

        // Txn с failover по endpoints: упавший endpoint → следующий.
        // txn.IsSuccess=false — транспортный сбой; проигрыш compare — законный
        // исход put-if-absent, обрабатывается re-read ниже.
        var txn = await TxnAsync(TxnRequest.Of(compare, put), ct);
        if (!txn.IsSuccess)
            return Result<KafkaSecrets>.Failed(txn.Error!);

        // Re-read: txn мог проиграть (гонка) — актуальны существующие значения.
        var final = await ReadAsync(cluster, ct);
        if (!final.IsSuccess)
            return Result<KafkaSecrets>.Failed(final.Error!);

        var (finalUser, finalPassword) = final.Value;
        if (finalUser is { Length: > 0 } && finalPassword is { Length: > 0 })
            return Result<KafkaSecrets>.Success(new KafkaSecrets(finalUser, finalPassword));

        return Result<KafkaSecrets>.Failed(new ApplicationException(
            $"ensure app-секрета {cluster}: после txn ключи неполны " +
            $"(app_user присутствует: {finalUser is not null}, app_password присутствует: {finalPassword is not null})"));
    }

    private static string UserKey(string cluster) => $"/kafka/clusters/{cluster}/app_user";

    private static string PasswordKey(string cluster) => $"/kafka/clusters/{cluster}/app_password";

    // Чтение обоих ключей с failover по endpoints.
    private async Task<Result<(string?, string?)>> ReadAsync(string cluster, CancellationToken ct)
    {
        Result<Kv?>? lastError = null;
        foreach (var endpoint in endpoints)
        {
            var user = await etcd.GetAsync(endpoint, UserKey(cluster), ct);
            if (!user.IsSuccess)
            {
                lastError = user;
                continue;
            }

            var password = await etcd.GetAsync(endpoint, PasswordKey(cluster), ct);
            if (!password.IsSuccess)
            {
                lastError = password;
                continue;
            }

            return Result<(string?, string?)>.Success((
                TrimOrNull(user.Value?.Value),
                TrimOrNull(password.Value?.Value)));
        }

        return Result<(string?, string?)>.Failed(lastError!.Error!);
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
