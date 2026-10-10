using PgWorker.Core;
using PgWorker.Core.Model;
using Shared.Etcd.Client;

namespace PgWorker.Provisioning.Processes;

/// <summary>Итог ensure кластерных кредов (t02, arch/14 §4 / arch/19 §7): app
/// (приложения), mover (роль bucket_mover переездов), bucket_admin (DSN-точка
/// входа), backup (роль backup_exec полных бэкапов t02-backups), rest
/// (basic-auth Patroni REST :8008, t22; username — константа patroni).
/// Все значения гарантированно существуют в etcd после EnsureAsync.</summary>
public sealed record ClusterCredentials(
    AppCredentials App, string MoverPassword, AppCredentials BucketAdmin, string BackupPassword,
    string RestPassword);

/// <summary>
/// Ensure per-cluster кредов (t02, arch/14 §4 / arch/19 §7): чтение
/// /clusters/&lt;C&gt;/{app_user,app_password,mover_password,bucket_admin_user,bucket_admin_password,backup_password};
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

    // Сырое чтение семи ключей: null — ключ отсутствует (добирается txn-ом).
    private sealed record RawSecrets(
        string? AppUser, string? AppPassword, string? MoverPassword,
        string? BucketAdminUser, string? BucketAdminPassword, string? BackupPassword,
        string? RestPassword);

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
        var desiredAppUser = NonEmptyOrDefault(current.AppUser, DefaultAppUser);
        var desiredAppPassword = NonEmptyOrDefault(current.AppPassword, AppSecretGenerator.Generate());
        var desiredMover = NonEmptyOrDefault(current.MoverPassword, AppSecretGenerator.Generate());
        var desiredAdminUser = !string.IsNullOrWhiteSpace(current.BucketAdminUser) ? current.BucketAdminUser.Trim()
            : config.BucketAdminUser ?? DefaultBucketAdminUser;
        var desiredAdminPassword = !string.IsNullOrWhiteSpace(current.BucketAdminPassword) ? current.BucketAdminPassword.Trim()
            : config.BucketAdminPassword ?? AppSecretGenerator.Generate();
        var desiredBackupPassword = NonEmptyOrDefault(current.BackupPassword, AppSecretGenerator.Generate());
        var desiredRestPassword = NonEmptyOrDefault(current.RestPassword, AppSecretGenerator.Generate());

        // Put-if-absent: отсутствующий ключ — compare NotExists; ключ с ПУСТЫМ
        // значением (битое состояние) — compare ValueEqual по фактическому raw —
        // txn добирает пустое (канон «ensure гарантирует значения после вызова»).
        var compare = new List<TxnCompare>();
        var put = new List<TxnOp>();
        void AddIfAbsent(string key, string value, string? current)
        {
            if (!string.IsNullOrWhiteSpace(current))
                return; // существующее непустое не переписываем
            compare.Add(current is null
                ? TxnCompare.NotExists(key)
                : TxnCompare.ValueEqual(key, current));
            put.Add(new TxnOp.Put(key, value, null));
        }

        AddIfAbsent(UserKey(cluster), desiredAppUser, current.AppUser);
        AddIfAbsent(PasswordKey(cluster), desiredAppPassword, current.AppPassword);
        AddIfAbsent(MoverKey(cluster), desiredMover, current.MoverPassword);
        AddIfAbsent(BucketAdminUserKey(cluster), desiredAdminUser, current.BucketAdminUser);
        AddIfAbsent(BucketAdminPasswordKey(cluster), desiredAdminPassword, current.BucketAdminPassword);
        AddIfAbsent(BackupKey(cluster), desiredBackupPassword, current.BackupPassword);
        AddIfAbsent(RestKey(cluster), desiredRestPassword, current.RestPassword);

        // Txn с failover по endpoints (образец ReadAsync ниже): упавший
        // endpoint → следующий; ни один не ответил — Failed(lastError).
        // Замечание: txn.IsSuccess=false — транспортный сбой вызова; проигрыш
        // compare (txn.Value.Succeeded=false) — НЕ сбой: законный исход
        // put-if-absent, обрабатывается re-read ниже.
        Result<TxnResult> lastTxnError = default;
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
            return Result<ClusterCredentials>.Failed(lastTxnError.Error ?? new EtcdUnreachableException("etcd endpoints не заданы"));

        // Re-read: txn мог проиграть (гонка) — актуальны существующие значения.
        var final = await ReadAsync(cluster, ct);
        if (!final.IsSuccess)
            return Result<ClusterCredentials>.Failed(final.Error!);

        if (IsComplete(final.Value))
            return Result<ClusterCredentials>.Success(ToCredentials(final.Value));

        return Result<ClusterCredentials>.Failed(new ApplicationException(
            $"ensure per-cluster кредов {cluster}: после txn ключи неполны " +
            $"(app_user: {Filled(final.Value.AppUser)}, app_password: {Filled(final.Value.AppPassword)}, " +
            $"mover_password: {Filled(final.Value.MoverPassword)}, " +
            $"bucket_admin_user: {Filled(final.Value.BucketAdminUser)}, " +
            $"bucket_admin_password: {Filled(final.Value.BucketAdminPassword)}, " +
            $"backup_password: {Filled(final.Value.BackupPassword)}, " +
            $"rest_password: {Filled(final.Value.RestPassword)})"));
    }

    // Полнота — ЗНАЧИМЫЕ значения: пробельно-пустой ключ (битое состояние)
    // НЕ полон — ensure добирает его txn-ом (put-if-absent на пустое).
    private static bool IsComplete(RawSecrets s)
        => Filled(s.AppUser) && Filled(s.AppPassword)
           && Filled(s.MoverPassword)
           && Filled(s.BucketAdminUser) && Filled(s.BucketAdminPassword)
           && Filled(s.BackupPassword)
           && Filled(s.RestPassword);

    private static bool Filled(string? value)
        => !string.IsNullOrWhiteSpace(value);

    private static ClusterCredentials ToCredentials(RawSecrets s)
        => new(new AppCredentials(s.AppUser!, s.AppPassword!), s.MoverPassword!,
            new AppCredentials(s.BucketAdminUser!, s.BucketAdminPassword!), s.BackupPassword!,
            s.RestPassword!);

    private static string UserKey(string cluster) => $"/clusters/{cluster}/app_user";

    private static string PasswordKey(string cluster) => $"/clusters/{cluster}/app_password";

    private static string MoverKey(string cluster) => $"/clusters/{cluster}/mover_password";

    private static string BucketAdminUserKey(string cluster) => $"/clusters/{cluster}/bucket_admin_user";

    private static string BucketAdminPasswordKey(string cluster) => $"/clusters/{cluster}/bucket_admin_password";

    private static string BackupKey(string cluster) => $"/clusters/{cluster}/backup_password";

    private static string RestKey(string cluster) => $"/clusters/{cluster}/rest_password";

    // Чтение семи ключей с failover по endpoints (паттерн ReadPortAllocAsync):
    // упавший endpoint → следующий; на живом — все шесть Get подряд.
    private async Task<Result<RawSecrets>> ReadAsync(string cluster, CancellationToken ct)
    {
        Result<Kv?> lastError = default;
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

            var backupPassword = await etcd.GetAsync(endpoint, BackupKey(cluster), ct);
            if (!backupPassword.IsSuccess)
            {
                lastError = backupPassword;
                continue;
            }

            var restPassword = await etcd.GetAsync(endpoint, RestKey(cluster), ct);
            if (!restPassword.IsSuccess)
            {
                lastError = restPassword;
                continue;
            }

            return Result<RawSecrets>.Success(new RawSecrets(
                RawOrNull(user.Value?.Value),
                RawOrNull(password.Value?.Value),
                RawOrNull(mover.Value?.Value),
                RawOrNull(adminUser.Value?.Value),
                RawOrNull(adminPassword.Value?.Value),
                RawOrNull(backupPassword.Value?.Value),
                RawOrNull(restPassword.Value?.Value)));
        }

        return Result<RawSecrets>.Failed(lastError.Error ?? new EtcdUnreachableException("etcd endpoints не заданы"));
    }

    // null — ключ отсутствует; пробельно-пустой raw — как есть (compare по нему);
    // иначе trimmed значение.
    private static string? RawOrNull(string? raw)
        => raw is null || string.IsNullOrWhiteSpace(raw) ? raw : raw.Trim();

    private static string NonEmptyOrDefault(string? current, string fallback)
        => !string.IsNullOrWhiteSpace(current) ? current.Trim() : fallback;
}
