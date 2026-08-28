using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Etcd.Client;

namespace PgWorker.Provisioning.Processes;

/// <summary>
/// Ensure per-cluster app-секрета (spec §3.1/§4.1, arch/14 §5 P1.5): чтение
/// /clusters/&lt;C&gt;/{app_user,app_password}; отсутствующие ключи генерируются
/// и кладутся ОДНОЙ txn put-if-absent (compare NotExists только на отсутствующие).
/// Проигрыш txn (гонка/re-run) → re-read и использование существующих значений.
/// Вызывается только держателем клэйма &lt;C&gt; (инвариант мутаций /clusters/).
/// </summary>
public interface IAppSecretEnsurer
{
    Task<Result<AppCredentials>> EnsureAsync(string cluster, CancellationToken ct);
}

public sealed class AppSecretEnsurer(IEtcdGateway etcd, string[] endpoints) : IAppSecretEnsurer
{
    private const string DefaultAppUser = "app";

    public async Task<Result<AppCredentials>> EnsureAsync(string cluster, CancellationToken ct)
    {
        var read = await ReadAsync(cluster, ct);
        if (!read.IsSuccess)
            return Result<AppCredentials>.Failed(read.Error!);

        var (user, password) = read.Value;
        if (user is { Length: > 0 } && password is { Length: > 0 })
            return Result<AppCredentials>.Success(new AppCredentials(user, password));

        // Отсутствующие добираем txn NotExists: существующие не переписываем
        // (идемпотентность re-run — spec §2.5).
        var newUser = user ?? DefaultAppUser;
        var newPassword = password ?? AppSecretGenerator.Generate();
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

        foreach (var endpoint in endpoints)
        {
            var txn = await etcd.TxnAsync(endpoint, TxnRequest.Of(compare, put), ct);
            if (!txn.IsSuccess)
                return Result<AppCredentials>.Failed(txn.Error!);
            break; // первый живой endpoint (паттерн ReadPortAllocAsync)
        }

        // Re-read: txn мог проиграть (гонка) — актуальны существующие значения.
        var final = await ReadAsync(cluster, ct);
        if (!final.IsSuccess)
            return Result<AppCredentials>.Failed(final.Error!);

        var (finalUser, finalPassword) = final.Value;
        if (finalUser is { Length: > 0 } && finalPassword is { Length: > 0 })
            return Result<AppCredentials>.Success(new AppCredentials(finalUser, finalPassword));

        return Result<AppCredentials>.Failed(new ApplicationException(
            $"ensure app-секрета {cluster}: после txn ключи неполны " +
            $"(app_user присутствует: {finalUser is not null}, app_password присутствует: {finalPassword is not null})"));
    }

    private static string UserKey(string cluster) => $"/clusters/{cluster}/app_user";

    private static string PasswordKey(string cluster) => $"/clusters/{cluster}/app_password";

    // Чтение обоих ключей с failover по endpoints (паттерн ReadPortAllocAsync).
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

    private static string? TrimOrNull(string? raw)
        => string.IsNullOrWhiteSpace(raw) ? null : raw.Trim();
}
