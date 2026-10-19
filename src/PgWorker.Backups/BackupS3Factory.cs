using PgWorker.Etcd.S3;
using Shared.Core;

namespace PgWorker.Backups;

/// <summary>
/// Фабрика per-cluster пула-клиента (t36): резолв policy+пула (резолвер) + кэш
/// контекста на тик НЕ нужен — создание per-вызов, dispose вызывающим (кэш
/// клиентов per-цели внутри PoolBackupS3). restore/глобальные проходы —
/// по имени пула напрямую (CreateForPoolAsync: write=read=все ноды).
/// </summary>
public sealed class BackupS3Factory(
    BackupPoolResolver resolver,
    IS3PoolStore pools,
    Func<string> bucket,
    Func<string?> caBundlePath,
    TimeSpan requestTimeout,
    Func<string, IBackupS3>? targetFactory = null)
{
    /// <summary>Пасс-through резолв для процессов: контекст списков до создания
    /// клиента (гварды policy.s3 / waiting-backup-pool — у вызывающего).</summary>
    public Task<Result<BackupPoolContext?>> ResolveAsync(string cluster, CancellationToken ct)
        => resolver.ResolveAsync(cluster, ct);

    /// <summary>Пул-клиент из резолвнутого контекста (создание per-вызов,
    /// dispose вызывающим; клиенты per-цели кэшируются внутри PoolBackupS3).
    /// Чтение — ВСЕГДА ReadAll (все ноды пула, arch/19 §5 «где данные есть»):
    /// write-реплика — не read-список (per-cluster клиент обслуживает контроль/
    /// verify/restore — перебор чтения обязан доходить до живой ноды пула).</summary>
    public PoolBackupS3 Create(BackupPoolContext ctx)
        => new(
            ctx.WriteMaster.ToList(), ctx.ReadAll.ToList(),
            ctx.AccessKey, ctx.SecretKey, bucket(), caBundlePath(),
            requestTimeout,
            targetFactory: targetFactory ?? (uri => new BackupS3(
                new S3ClientOptions(uri, bucket(), ctx.AccessKey, ctx.SecretKey,
                    CaBundlePath: caBundlePath()),
                requestTimeout)));

    /// <summary>Пул-клиент кластера: write — списки роли источника из policy,
    /// read — все ноды; policy без s3 → Success(null)-стайл ошибки — нет:
    /// фабрика вызывается уже ПОСЛЕ гварда policy.s3 у процесса (null сюда не
    /// доходит), Failed — резолв (transient-путь процесса).</summary>
    public async Task<Result<PoolBackupS3>> CreateForClusterAsync(string cluster, CancellationToken ct)
    {
        var resolved = await resolver.ResolveAsync(cluster, ct);
        if (!resolved.IsSuccess)
            return Result<PoolBackupS3>.Failed(resolved.Error!);
        var ctx = resolved.Value;
        if (ctx is null)
            return Result<PoolBackupS3>.Failed(new ApplicationException(
                $"s3-pool: cluster {cluster} policy has no s3 section — pool not selected (arch/19 §4)"));
        return Result<PoolBackupS3>.Success(Create(ctx));
    }

    /// <summary>Пул-клиент по имени (restore source_pool, глобальные сверки):
    /// write=read=все ноды пула (порядок endpoints); контекст — списки env
    /// джобов чтения.</summary>
    public async Task<Result<(PoolBackupS3 Client, BackupPoolContext Context)>> CreateForPoolAsync(
        string pool, CancellationToken ct)
    {
        var read = await pools.ReadAsync(pool, ct);
        if (!read.IsSuccess)
            return Result<(PoolBackupS3, BackupPoolContext)>.Failed(read.Error!);
        var s3pool = read.Value;
        if (s3pool is null)
            return Result<(PoolBackupS3, BackupPoolContext)>.Failed(new ApplicationException(
                $"s3-pool: pool {pool} not found in /s3/clusters/"));
        if (s3pool.ToRemove)
            return Result<(PoolBackupS3, BackupPoolContext)>.Failed(new ApplicationException(
                $"s3-pool: pool {pool} in state TO_REMOVE"));
        // Креды — per-cluster secrets из etcd (/s3/clusters/<pool>/secrets,
        // ключ backups), без секрет-профилей.
        if (s3pool.Secrets is not { } poolSecrets)
            return Result<(PoolBackupS3, BackupPoolContext)>.Failed(new ApplicationException(
                $"s3-pool: pool {s3pool} has no readable secrets in etcd (/s3/clusters/{s3pool}/secrets, key backups)"));

        // Адреса чтения — portalloc-пары в порядке endpoints, http-URI (тот же
        // резолв, что в резолвере; endpoints-ключ схему не несёт — arch/23 §2).
        var endpoints = BackupPoolResolver.ReadAllUris(s3pool);
        return Result<(PoolBackupS3, BackupPoolContext)>.Success((
            new PoolBackupS3(endpoints, endpoints, poolSecrets.AccessKey, poolSecrets.SecretKey, bucket(),
                caBundlePath(), requestTimeout, targetFactory: targetFactory),
            new BackupPoolContext(pool, poolSecrets.AccessKey, poolSecrets.SecretKey, endpoints, endpoints,
                endpoints)));
    }

    // Сборка клиента из резолвнутого контекста вынесена в public Create(ctx).
}
