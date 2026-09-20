
namespace PgWorker.Backups.Job;

// Спецификация ephemeral verify-джоба (arch/19 §5, t04; образец BackupJobSpec):
// отличия от джоба t02 — без DSN/пароля PG (verify чисто по S3), Cmd —
// VerifyJobCommand (полная замена ENTRYPOINT). Квота staging задана → tmpfs
// size= (жёсткий ENOSPC); null → named volume. Имя контейнера == имени volume
// (ephemeral, чистится после итога).
public static class VerifyJobSpec
{
    public static ContainerSpec Build(BackupsRuntimeOptions opts, string cluster, string shard, string id)
    {
        var env = new Dictionary<string, string>
        {
            [VerifyJobCommand.EnvS3Endpoint] = opts.S3Endpoint,
            [VerifyJobCommand.EnvS3Region] = opts.S3Region ?? string.Empty,
            [VerifyJobCommand.EnvS3Bucket] = opts.S3Bucket,
            [VerifyJobCommand.EnvS3AccessKey] = opts.S3AccessKey,
            [VerifyJobCommand.EnvS3SecretKey] = opts.S3SecretKey,
            [VerifyJobCommand.EnvPrefix] = $"{cluster}/{shard}",
            [VerifyJobCommand.EnvId] = id,
            [VerifyJobCommand.EnvStagingDir] = opts.StagingDir,
            // S3-креды — только env (arch/19 §5): mc-alias собирается скриптом из кредов
            [VerifyJobCommand.EnvMcHostVariable] =
                VerifyJobCommand.McHost(opts.AgentS3Endpoint, opts.S3AccessKey, opts.S3SecretKey),
        };
        return new ContainerSpec(
            Image: opts.JobImage,
            Env: env,
            VolumeName: opts.StagingQuotaBytes is null ? BackupNames.VerifyVolumeName(cluster, shard, id) : "",
            VolumeDest: opts.StagingDir,
            Ports: [],
            Hostname: BackupNames.VerifyContainerName(cluster, shard, id),
            CpuCores: opts.AgentCpu,
            MemoryBytes: opts.AgentMem,
            LabelKey: "pgworker",
            Label: cluster,
            ResetEntrypoint: true, // inline-команда джоба — ENTRYPOINT образа сброшен (t02)
            Cmd: VerifyJobCommand.Build(),
            Network: null,
            NetworkAliases: null,
            Tmpfs: opts.StagingQuotaBytes is { } quota
                ? new Dictionary<string, string> { [opts.StagingDir] = $"size={quota}" }
                : null,
            // advertised-хосты источника (как у джоба t02): host.docker.internal —
            // general-правило; "local" — зарезервированное имя docker-хоста стенда
            ExtraHosts: ["host.docker.internal:host-gateway", "local:host-gateway"],
            RestartPolicy: "no");
    }
}
