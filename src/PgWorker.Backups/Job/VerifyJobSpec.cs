
namespace PgWorker.Backups.Job;

// Спецификация ephemeral verify-джоба (arch/19 §5, t04; образец BackupJobSpec):
// отличия от джоба t02 — без DSN/пароля PG (verify чисто по S3), Cmd —
// VerifyJobCommand (полная замена ENTRYPOINT). Квота staging задана → tmpfs
// size= (жёсткий ENOSPC); null → named volume. Имя контейнера == имени volume
// (ephemeral, чистится после итога).
public static class VerifyJobSpec
{
    // t36: s3Endpoints — read-список (все ноды пула, порядок endpoints); креды —
    // env-профиля пула (arch/19 §7); MC_HOST-алиасы строит скрипт из S3_ENDPOINTS.
    public static ContainerSpec Build(BackupsRuntimeOptions opts, string cluster, string shard, string id,
        IReadOnlyList<string> s3Endpoints, string s3AccessKey, string s3SecretKey)
    {
        var env = new Dictionary<string, string>
        {
            [VerifyJobCommand.EnvS3Endpoints] = string.Join(",", s3Endpoints),
            [VerifyJobCommand.EnvS3Bucket] = opts.S3Bucket,
            [VerifyJobCommand.EnvS3AccessKey] = s3AccessKey,
            [VerifyJobCommand.EnvS3Credentials] = s3SecretKey,
            [VerifyJobCommand.EnvPrefix] = $"{cluster}/{shard}",
            [VerifyJobCommand.EnvId] = id,
            [VerifyJobCommand.EnvStagingDir] = opts.StagingDir,
        };
        // t42 (arch/19 §7): CA https-endpoint — PEM значением (multi-host), материализация внутри контейнера
        var caPem = BackupCaPem.ReadIfConfigured(opts);
        if (caPem is not null)
            env[BackupCaPem.EnvName] = caPem;
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
            // Режим сети контура (t41, arch/14 §2.1): джоб в сети контура,
            // extra_hosts не применяются (адресация по именам сети; S3
            // резолвится compose-DNS/alias). Прод-режим — default bridge +
            // фиксированные host-переходы advertised-хостов (arch/19 §2/§6).
            Network: opts.ContourNetwork,
            NetworkAliases: null,
            Tmpfs: opts.StagingQuotaBytes is { } quota
                ? new Dictionary<string, string> { [opts.StagingDir] = $"size={quota}" }
                : null,
            // advertised-хосты источника (как у джоба t02): host.docker.internal —
            // general-правило; "local" — зарезервированное имя docker-хоста стенда
            ExtraHosts: opts.ContourNetwork is null
                ? ["host.docker.internal:host-gateway", "local:host-gateway"]
                : null,
            RestartPolicy: "no");
    }
}
