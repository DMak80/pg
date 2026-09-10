using PgWorker.Core.Model;
using PgWorker.Docker.Engine;

namespace PgWorker.Backups.Job;

// Спецификация ephemeral-джоба полного бэкапа (arch/19 §2, t02): env-контракт
// образа pgworker-backup; пароль per-cluster — ТОЛЬКО env контейнера (не в
// статусы/логи). Квота staging задана → tmpfs size= (жёсткий ENOSPC); null →
// named volume (docker создаст при первом mount).
public static class BackupJobSpec
{
    public static ContainerSpec Build(
        BackupsRuntimeOptions opts, string cluster, string shard, string id,
        NodeAddress source, string backupPassword)
    {
        var env = new Dictionary<string, string>
        {
            ["PGW_BK_DSN"] =
                $"host={source.Host} port={source.Ports.Pg} user=backup_exec password={backupPassword} sslmode=require",
            ["PGW_BK_ID"] = id,
            ["PGW_BK_S3_ENDPOINT"] = opts.S3Endpoint,
            ["PGW_BK_S3_REGION"] = opts.S3Region ?? string.Empty,
            ["PGW_BK_S3_BUCKET"] = opts.S3Bucket,
            ["PGW_BK_S3_ACCESS_KEY"] = opts.S3AccessKey,
            ["PGW_BK_S3_SECRET_KEY"] = opts.S3SecretKey,
            ["PGW_BK_PREFIX"] = $"{cluster}/{shard}",
            ["PGW_BK_STAGING_DIR"] = opts.StagingDir,
        };
        return new ContainerSpec(
            Image: opts.JobImage,
            Env: env,
            VolumeName: opts.StagingQuotaBytes is null ? BackupNames.VolumeName(cluster, shard, id) : "",
            VolumeDest: opts.StagingDir,
            Ports: [],
            Hostname: BackupNames.ContainerName(cluster, shard, id),
            CpuCores: opts.AgentCpu,
            MemoryBytes: opts.AgentMem,
            Label: cluster,
            Cmd: null,
            Network: null,
            NetworkAliases: null,
            Tmpfs: opts.StagingQuotaBytes is { } quota
                ? new Dictionary<string, string> { [opts.StagingDir] = $"size={quota}" }
                : null,
            // advertised-хосты источника (arch/19 §2/§6): host.docker.internal —
            // general-правило; "local" — зарезервированное имя docker-хоста стенда
            // (deploy-compose extra_hosts воркера; portalloc усвоенного demo).
            ExtraHosts: ["host.docker.internal:host-gateway", "local:host-gateway"],
            RestartPolicy: "no");
    }
}
