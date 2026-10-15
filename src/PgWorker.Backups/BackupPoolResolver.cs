using PgWorker.Etcd.Parsing;
using PgWorker.Etcd.S3;
using Shared.Core;

namespace PgWorker.Backups;

/// <summary>Резолв per-cluster (t36, arch/19 §4/§2.6-2.7): policy.s3 + пул +
/// env-профили → контекст списков. null = пул не выбран (policy без s3 —
/// подсистема кластера молчит); Failed — диагностическая ошибка (пул не
/// найден / TO_REMOVE / профиль без кредов / списки пусты / имя ноды не в
/// пуле / нода без адреса) — ВСЕ Failed-исходы transient для процессов:
/// ретраи тиком + алертная проекция (маркер s3-pool-exhausted в статусах),
/// НЕ краш тика и НЕ 400 на приёме policy (валидация приёма — по
/// nodes/&lt;I&gt;/placement, а не по адресам).</summary>
public sealed record BackupPoolContext(
    string Pool, string AccessKey, string SecretKey,
    IReadOnlyList<string> WriteMaster, IReadOnlyList<string> WriteReplica,
    IReadOnlyList<string> ReadAll);

/// <summary>
/// Единая точка «какой кластер куда пишет/читает» (t36): имена нод списков —
/// ссылки (nodes/&lt;I&gt;/placement), адреса — portalloc/Endpoints пула;
/// креды — env-профиль config.secret_profile (арх/19 §7). Схема URI
/// фиксирована http — owns3-контур; https появится вместе с external-целями
/// (точка расширения arch/19 §7).
/// </summary>
public sealed class BackupPoolResolver(
    IS3PoolStore pools,
    BackupSecretProfileStore secrets,
    Func<string, CancellationToken, Task<BackupPolicy?>>? policyReader = null)
{
    public async Task<Result<BackupPoolContext?>> ResolveAsync(string cluster, CancellationToken ct)
    {
        // Policy: ключа нет ИЛИ секции s3 нет → Success(null) — «пул не выбран»
        // (не ошибка; алерт backup-pool-missing — проекция панели).
        var policy = policyReader is null ? null : await policyReader(cluster, ct);
        var s3 = policy?.S3;
        if (s3 is null)
            return Result<BackupPoolContext?>.Success(null);

        // Пул: живое чтение /s3/clusters/<C> (S3PoolStore).
        var poolResult = await pools.ReadAsync(s3.Pool, ct);
        if (!poolResult.IsSuccess)
            return Result<BackupPoolContext?>.Failed(poolResult.Error!);
        var pool = poolResult.Value;
        if (pool is null)
            return Result<BackupPoolContext?>.Failed(
                new ApplicationException($"s3-pool: пул {s3.Pool} не найден в /s3/clusters/"));
        if (pool.ToRemove)
            return Result<BackupPoolContext?>.Failed(
                new ApplicationException($"s3-pool: пул {s3.Pool} в состоянии TO_REMOVE"));

        // Креды — env-профиль пула; диагностика без значений (spec §8).
        string? accessKey = null;
        string? secretKey = null;
        string? diag = null;
        if (pool.SecretProfile is null
            || !secrets.TryGet(pool.SecretProfile, out accessKey, out secretKey, out diag))
            return Result<BackupPoolContext?>.Failed(
                new ApplicationException($"s3-pool: {diag ?? $"пул {pool.Name} без secret_profile"}"));

        // Списки записи — по роли источника; имена обязаны быть инстансами пула.
        var writeMasterResult = ResolveWriteList(pool, s3.Pool, "write_master", s3.WriteMaster);
        if (!writeMasterResult.IsSuccess)
            return Result<BackupPoolContext?>.Failed(writeMasterResult.Error!);
        var writeReplicaResult = ResolveWriteList(pool, s3.Pool, "write_replica", s3.WriteReplica);
        if (!writeReplicaResult.IsSuccess)
            return Result<BackupPoolContext?>.Failed(writeReplicaResult.Error!);

        // ReadAll — все ноды пула без приоритетов, порядок endpoints-ключа:
        // адреса — portalloc-пары нод (http-URI, как write-списки; endpoints-ключ
        // — host:port без схемы, arch/23 §2). Нода без адреса пропускается —
        // пул поднимается (arch/19 §5).
        var readAll = ReadAllUris(pool);
        return Result<BackupPoolContext?>.Success(new BackupPoolContext(
            pool.Name, accessKey!, secretKey!,
            writeMasterResult.Value, writeReplicaResult.Value, readAll));
    }

    /// <summary>Read-адреса пула: portalloc-пары в порядке endpoints-ключа,
    /// http-URI (тот же резолв, что у write-списков; env-джобы чтения и клиенты
    /// требуют абсолютный URI). Записи endpoints без portalloc-ноды пропускаются.</summary>
    public static IReadOnlyList<string> ReadAllUris(S3Pool pool)
        => pool.Endpoints
            .Select(ep => pool.Nodes.FirstOrDefault(n =>
                n.Host is { } host && n.S3Port is { } port && $"{host}:{port}" == ep))
            .Where(n => n is not null)
            .Select(n => $"http://{n!.Host}:{n.S3Port}")
            .ToList();

    // Резолв одного write-списка: имена → "http://host:port"; пустой список =
    // записанная битая policy (transient-диагностика; «не выбрано» — это
    // отсутствие секции s3 целиком, arch/19 §4).
    private static Result<IReadOnlyList<string>> ResolveWriteList(
        S3Pool pool, string poolName, string fieldName, IReadOnlyList<string> names)
    {
        if (names.Count == 0)
            return Result<IReadOnlyList<string>>.Failed(
                new ApplicationException($"s3-pool: s3.{fieldName} пуст (пул {poolName}) — секция задана, список не заполнен"));

        var uris = new List<string>(names.Count);
        foreach (var name in names)
        {
            var node = pool.Nodes.FirstOrDefault(n => n.Name == name);
            if (node is null)
                return Result<IReadOnlyList<string>>.Failed(
                    new ApplicationException($"s3-pool: s3.{fieldName}: нода {name} не в пуле {poolName} (нет nodes/{name}/placement)"));
            if (node.Host is null || node.S3Port is null)
                return Result<IReadOnlyList<string>>.Failed(
                    new ApplicationException($"s3-pool: s3.{fieldName}: нода {name} без адреса (portalloc) — пул поднимается"));
            uris.Add($"http://{node.Host}:{node.S3Port}");
        }

        return Result<IReadOnlyList<string>>.Success(uris);
    }
}
