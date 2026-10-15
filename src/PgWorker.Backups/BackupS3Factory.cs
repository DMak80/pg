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
    BackupSecretProfileStore secrets,
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
    /// dispose вызывающим; клиенты per-цели кэшируются внутри PoolBackupS3).</summary>
    public PoolBackupS3 Create(BackupPoolContext ctx)
        => new(
            ctx.WriteMaster.ToList(), ctx.WriteReplica.ToList(),
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
                $"s3-pool: у кластера {cluster} policy без секции s3 — пул не выбран (arch/19 §4)"));
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
                $"s3-pool: пул {pool} не найден в /s3/clusters/"));
        if (s3pool.ToRemove)
            return Result<(PoolBackupS3, BackupPoolContext)>.Failed(new ApplicationException(
                $"s3-pool: пул {pool} в состоянии TO_REMOVE"));
        string? accessKey = null;
        string? secretKey = null;
        string? diag = null;
        if (s3pool.SecretProfile is null
            || !secrets.TryGet(s3pool.SecretProfile, out accessKey, out secretKey, out diag))
            return Result<(PoolBackupS3, BackupPoolContext)>.Failed(new ApplicationException(
                $"s3-pool: {diag ?? $"пул {pool} без secret_profile"}"));

        var endpoints = (IReadOnlyList<string>)s3pool.Endpoints;
        return Result<(PoolBackupS3, BackupPoolContext)>.Success((
            new PoolBackupS3(endpoints, endpoints, accessKey!, secretKey!, bucket(), caBundlePath(),
                requestTimeout, targetFactory),
            new BackupPoolContext(pool, accessKey!, secretKey!, endpoints, endpoints, endpoints)));
    }

    // Сборка клиента из резолвнутого контекста вынесена в public Create(ctx).
}
