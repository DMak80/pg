using PgWorker.Core.Model;

namespace PgWorker.Backups.Restore;

// Спецификация ephemeral restore-джоба (t05, arch/19 §3.5): монтирует
// data-volume ПЕРВОЙ ноды шарда в точку dataDir (docker создаст named volume
// при create контейнера) и пишет в него восстановленный PGDATA. Env —
// S3-комплект одной MC_HOST-строкой (RestoreJobCommand.McHost, §7),
// source-префикс, backup_id, цель и пути PGDATA (env-контракт §3.3); никаких
// паролей PG (джоб слушает только unix-socket). Порты/сеть/рестарт — как у
// джоба t02: none.
public static class RestoreJobSpec
{
    public static ContainerSpec Build(
        BackupsRuntimeOptions opts, string cluster, string shard, string id,
        string nodeVolumeName, string targetTime, string srcCluster, string srcShard,
        string dataDir = "/restore")
    {
        var env = new Dictionary<string, string>
        {
            [RestoreJobCommand.EnvMcHost] = RestoreJobCommand.McHost(
                opts.AgentS3Endpoint, opts.S3AccessKey, opts.S3SecretKey),
            [RestoreJobCommand.EnvBucket] = opts.S3Bucket,
            [RestoreJobCommand.EnvSrcPrefix] = $"{srcCluster}/{srcShard}",
            [RestoreJobCommand.EnvBackupId] = id,
            // "" = latest (скрипт не пишет recovery_target_time)
            [RestoreJobCommand.EnvTargetTime] = targetTime,
            [RestoreJobCommand.EnvRecoveryTimeoutSec] = opts.RestoreRecoveryTimeoutSec.ToString(),
            [RestoreJobCommand.EnvDataDir] = dataDir,
            // Внутри тома: pgroot/data — volume-корень узла /home/postgres/pgdata,
            // данные узла pgroot/data (arch/14 §2.1; инцидент E2E-гейта t05:
            // лишний уровень pgdata делал PGDATA пустым для Patroni → reinit).
            [RestoreJobCommand.EnvPgdata] = $"{dataDir}/pgroot/data",
        };
        // t42 (arch/19 §7): CA https-endpoint — PEM значением (multi-host), материализация внутри контейнера
        var caPem = BackupCaPem.ReadIfConfigured(opts);
        if (caPem is not null)
            env[BackupCaPem.EnvName] = caPem;
        return new ContainerSpec(
            Image: opts.JobImage,
            Env: env,
            VolumeName: nodeVolumeName,
            VolumeDest: dataDir,
            Ports: [],
            Hostname: BackupNames.RestoreContainerName(cluster, shard, id),
            CpuCores: opts.AgentCpu,
            MemoryBytes: opts.AgentMem,
            LabelKey: "pgworker",
            Label: cluster,
            ResetEntrypoint: true, // inline-команда джоба — ENTRYPOINT образа сброшен (t02)
            Cmd: RestoreJobCommand.Build(),
            // Режим сети контура (t41, arch/14 §2.1): джоб в сети контура,
            // extra_hosts не применяются (S3 резолвится compose-DNS/alias).
            // Прод-режим — default bridge + фиксированные host-переходы.
            Network: opts.ContourNetwork,
            NetworkAliases: null,
            Tmpfs: null,
            // advertised-хосты S3 (как у джоба t02, arch/19 §2/§7).
            ExtraHosts: opts.ContourNetwork is null
                ? ["host.docker.internal:host-gateway", "local:host-gateway"]
                : null,
            RestartPolicy: "no");
    }
}
