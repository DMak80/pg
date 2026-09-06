using PgWorker.Core;
using PgWorker.Core.Model;
using PgWorker.Etcd.Client;

namespace PgWorker.Provisioning.Processes;

/// <summary>Итог ensure тройки per-cluster кредов (t02, arch/14 §4): app
/// (приложения), mover (роль bucket_mover переездов), bucket_admin (DSN-точка
/// входа). Все значения гарантированно существуют в etcd после EnsureAsync.</summary>
public sealed record ClusterCredentials(
    AppCredentials App, string MoverPassword, AppCredentials BucketAdmin);

/// <summary>
/// Ensure per-cluster тройки кредов (t02, arch/14 §4): чтение
/// /clusters/&lt;C&gt;/{app_user,app_password,mover_password,bucket_admin_user,bucket_admin_password};
/// отсутствующие ключи генерируются (bucket_admin — из config кластера или
/// генерация) и кладутся ОДНОЙ txn put-if-absent (compare NotExists только на
/// отсутствующие). Имя роли mover фиксировано (bucket_mover) — отдельного
/// user-ключа нет. И чтение, и txn — с failover по endpoints до первого живого
/// (паттерн ReadPortAllocAsync); повтор txn на другом endpoint безопасен для
/// put-if-absent: проигрыш compare корректно разрешается re-read. Вызывается
/// только держателем клэйма &lt;C&gt; (инвариант мутаций /clusters/).
/// </summary>
public interface IClusterSecretEnsurer
{
    Task<Result<ClusterCredentials>> EnsureAsync(
        string cluster, ClusterConfig config, CancellationToken ct);
}

public sealed class ClusterSecretEnsurer(IEtcdGateway etcd, string[] endpoints) : IClusterSecretEnsurer
{
    private const string DefaultAppUser = "app";
    private const string DefaultBucketAdminUser = "bucket_admin";

    // Сырое чтение пяти ключей: null — ключ отсутствует (добирается txn-ом).
    private sealed record RawSecrets(
        string? AppUser, string? AppPassword, string? MoverPassword,
        string? BucketAdminUser, string? BucketAdminPassword);

    public async Task<Result<ClusterCredentials>> EnsureAsync(
        string cluster, ClusterConfig config, CancellationToken ct)
    {
        var read = await ReadAsync(cluster, ct);
        if (!read.IsSuccess)
            return Result<ClusterCredentials>.Failed(read.Error!);

        var current = read.Value;
        if (IsComplete(current))
            return Result<ClusterCredentials>.Success(ToCredentials(current));

        // Отсутствующие добираем txn NotExists: существующие не переписываем
        // (идемпотентность re-run — spec §2.5); bucket_admin-вход — config.
        var desiredAppUser = current.AppUser ?? DefaultAppUser;
        var desiredAppPassword = current.AppPassword ?? AppSecretGenerator.Generate();
        var desiredMover = current.MoverPassword ?? AppSecretGenerator.Generate();
        var desiredAdminUser = current.BucketAdminUser ?? config.BucketAdminUser ?? DefaultBucketAdminUser;
        var desiredAdminPassword = current.BucketAdminPassword
            ?? config.BucketAdminPassword ?? AppSecretGenerator.Generate();

        var compare = new List<TxnCompare>();
        var put = new List<TxnOp>();
        void AddIfAbsent(string key, string value, bool exists)
        {
            if (exists)
                return;
            compare.Add(TxnCompare.NotExists(key));
            put.Add(new TxnOp.Put(key, value, null));
        }

        AddIfAbsent(UserKey(cluster), desiredAppUser, current.AppUser is not null);
        AddIfAbsent(PasswordKey(cluster), desiredAppPassword, current.AppPassword is not null);
        AddIfAbsent(MoverKey(cluster), desiredMover, current.MoverPassword is not null);
        AddIfAbsent(BucketAdminUserKey(cluster), desiredAdminUser, current.BucketAdminUser is not null);
        AddIfAbsent(BucketAdminPasswordKey(cluster), desiredAdminPassword, current.BucketAdminPassword is not null);

        // Txn с failover по endpoints (образец ReadAsync ниже): упавший
        // endpoint → следующий; ни один не ответил — Failed(lastError).
        // Замечание: txn.IsSuccess=false — транспортный сбой вызова; проигрыш
        // compare (txn.Value.Succeeded=false) — НЕ сбой: законный исход
        // put-if-absent, обрабатывается re-read ниже.
        Result<TxnResult>? lastTxnError = null;
        var txnDone = false;
        foreach (var endpoint in endpoints)
        {
            var txn = await etcd.TxnAsync(endpoint, TxnRequest.Of(compare, put), ct);
            if (!txn.IsSuccess)
            {
                lastTxnError = txn;
                continue;
            }

            txnDone = true;
            break;
        }

        if (!txnDone)
            return Result<ClusterCredentials>.Failed(lastTxnError!.Error!);

        // Re-read: txn мог проиграть (гонка) — актуальны существующие значения.
        var final = await ReadAsync(cluster, ct);
        if (!final.IsSuccess)
            return Result<ClusterCredentials>.Failed(final.Error!);

        if (IsComplete(final.Value))
            return Result<ClusterCredentials>.Success(ToCredentials(final.Value));

        return Result<ClusterCredentials>.Failed(new ApplicationException(
            $"ensure per-cluster кредов {cluster}: после txn ключи неполны " +
            $"(app_user: {final.Value.AppUser is not null}, app_password: {final.Value.AppPassword is not null}, " +
            $"mover_password: {final.Value.MoverPassword is not null}, " +
            $"bucket_admin_user: {final.Value.BucketAdminUser is not null}, " +
            $"bucket_admin_password: {final.Value.BucketAdminPassword is not null})"));
    }

    private static bool IsComplete(RawSecrets s)
        => s.AppUser is { Length: > 0 } && s.AppPassword is { Length: > 0 }
           && s.MoverPassword is { Length: > 0 }
           && s.BucketAdminUser is { Length: > 0 } && s.BucketAdminPassword is { Length: > 0 };

    private static ClusterCredentials ToCredentials(RawSecrets s)
        => new(new AppCredentials(s.AppUser!, s.AppPassword!), s.MoverPassword!,
            new AppCredentials(s.BucketAdminUser!, s.BucketAdminPassword!));

    private static string UserKey(string cluster) => $"/clusters/{cluster}/app_user";

    private static string PasswordKey(string cluster) => $"/clusters/{cluster}/app_password";

    private static string MoverKey(string cluster) => $"/clusters/{cluster}/mover_password";

    private static string BucketAdminUserKey(string cluster) => $"/clusters/{cluster}/bucket_admin_user";

    private static string BucketAdminPasswordKey(string cluster) => $"/clusters/{cluster}/bucket_admin_password";

    // Чтение пяти ключей с failover по endpoints (паттерн ReadPortAllocAsync):
    // упавший endpoint → следующий; на живом — все пять Get подряд.
    private async Task<Result<RawSecrets>> ReadAsync(string cluster, CancellationToken ct)
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

            var mover = await etcd.GetAsync(endpoint, MoverKey(cluster), ct);
            if (!mover.IsSuccess)
            {
                lastError = mover;
                continue;
            }

            var adminUser = await etcd.GetAsync(endpoint, BucketAdminUserKey(cluster), ct);
            if (!adminUser.IsSuccess)
            {
                lastError = adminUser;
                continue;
            }

            var adminPassword = await etcd.GetAsync(endpoint, BucketAdminPasswordKey(cluster), ct);
            if (!adminPassword.IsSuccess)
            {
                lastError = adminPassword;
                continue;
            }

            return Result<RawSecrets>.Success(new RawSecrets(
                TrimOrNull(user.Value?.Value),
                TrimOrNull(password.Value?.Value),
                TrimOrNull(mover.Value?.Value),
                TrimOrNull(adminUser.Value?.Value),
                TrimOrNull(adminPassword.Value?.Value)));
        }

        return Result<RawSecrets>.Failed(lastError!.Error!);
    }

    private static string? TrimOrNull(string? raw)
        => string.IsNullOrWhiteSpace(raw) ? null : raw.Trim();
}
