using PgWorker.Core.Model;
using PgWorker.Docker.Engine;

namespace PgWorker.Backups.Restore;

// Спецификация ephemeral restore-джоба (t05, arch/19 §3.5): монтирует
// data-volume ПЕРВОЙ ноды шарда в точку dataDir (docker создаст named volume
// при create контейнера) и пишет в него восстановленный PGDATA. Env —
// S3-комплект одной MC_HOST-строкой (паттерн WalAgentCommand.McHost, §7),
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
            [RestoreJobCommand.EnvMcHost] = WalAgentCommand.McHost(
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
        return new ContainerSpec(
            Image: opts.JobImage,
            Env: env,
            VolumeName: nodeVolumeName,
            VolumeDest: dataDir,
            Ports: [],
            Hostname: BackupNames.RestoreContainerName(cluster, shard, id),
            CpuCores: opts.AgentCpu,
            MemoryBytes: opts.AgentMem,
            Label: cluster,
            Cmd: RestoreJobCommand.Build(),
            Network: null,
            NetworkAliases: null,
            Tmpfs: null,
            // advertised-хосты S3 (как у джоба t02, arch/19 §2/§7).
            ExtraHosts: ["host.docker.internal:host-gateway", "local:host-gateway"],
            RestartPolicy: "no");
    }
}
