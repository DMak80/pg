using PgWorker.Core.Model;

namespace PgWorker.Backups.Drill;

// Спецификация ephemeral drill-джоба (t02-restore-drill, arch/19 §3.6) —
// по образцу RestoreJobSpec (t05) с отличиями изоляции: volume СВОЙ (имя =
// имя контейнера pgw-backup-drill-<C>-<X>-<id>, docker создаёт при create),
// dataDir "/drill" (PGDATA /drill/pgroot/data), цель latest (TARGET_TIME=""),
// source — всегда собственный <C>/<X>. Всё прочее (env S3-комплект MC_HOST,
// Ports: [], Network: null, RestartPolicy: "no", лимиты Agent, ExtraHosts
// advertised-S3, ResetEntrypoint) — как у restore-джоба: джоб слушает только
// unix-socket, никаких паролей PG.
public static class DrillJobSpec
{
    // id — идентификатор дрилла (имя контейнера/volume); backupId — РЕЗОЛВНУТЫЙ
    // валидацией полный (BACKUP_ID env: джоб качает full/<backup_id>/ — инцидент
    // E2E t05: в спеку уходил id заявки, mc «Object does not exist»).
    public static ContainerSpec Build(
        BackupsRuntimeOptions opts, string cluster, string shard, string id,
        string backupId, string dataDir = "/drill")
    {
        var env = new Dictionary<string, string>
        {
            [Restore.RestoreJobCommand.EnvMcHost] = WalAgentCommand.McHost(
                opts.AgentS3Endpoint, opts.S3AccessKey, opts.S3SecretKey),
            [Restore.RestoreJobCommand.EnvBucket] = opts.S3Bucket,
            [Restore.RestoreJobCommand.EnvSrcPrefix] = $"{cluster}/{shard}",
            [Restore.RestoreJobCommand.EnvBackupId] = backupId,
            // "" = latest (скрипт не пишет recovery_target_* — конец WAL = promote)
            [Restore.RestoreJobCommand.EnvTargetTime] = "",
            [Restore.RestoreJobCommand.EnvRecoveryTimeoutSec] = opts.RestoreRecoveryTimeoutSec.ToString(),
            [Restore.RestoreJobCommand.EnvDataDir] = dataDir,
            // Внутри тома: pgroot/data — как у restore-джоба (объём ноды,
            // arch/14 §2.1; инцидент E2E-гейта t05).
            [Restore.RestoreJobCommand.EnvPgdata] = $"{dataDir}/pgroot/data",
        };
        return new ContainerSpec(
            Image: opts.JobImage,
            Env: env,
            VolumeName: BackupNames.DrillVolumeName(cluster, shard, id),
            VolumeDest: dataDir,
            Ports: [],
            Hostname: BackupNames.DrillContainerName(cluster, shard, id),
            CpuCores: opts.AgentCpu,
            MemoryBytes: opts.AgentMem,
            LabelKey: "pgworker",
            Label: cluster,
            ResetEntrypoint: true, // inline-команда джоба — ENTRYPOINT образа сброшен (t02)
            Cmd: DrillJobCommand.Build(),
            Network: null,
            NetworkAliases: null,
            Tmpfs: null,
            // advertised-хосты S3 (как у джоба t02, arch/19 §2/§7).
            ExtraHosts: ["host.docker.internal:host-gateway", "local:host-gateway"],
            RestartPolicy: "no");
    }
}
