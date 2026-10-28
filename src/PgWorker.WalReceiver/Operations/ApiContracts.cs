using System.Text.Json.Serialization;

namespace PgWorker.WalReceiver.Operations;

/// <summary>Тело POST /api/backup/full и POST /api/ops/* (запуск операции).</summary>
public sealed record StartFullRequest([property: JsonPropertyName("id")] string Id);

/// <summary>Статус операции полного бэкапа (GET /api/backup/full; 409 на POST —
/// снимок активной операции): id несёт активная операция — воркер сверяет его
/// в 409-ветке запуска (своя/чужая: фантомный COMPLETED чужого снимка исключён).</summary>
public sealed record FullStatusResponse(
    [property: JsonPropertyName("state")] string State,
    [property: JsonPropertyName("phase")] string Phase,
    [property: JsonPropertyName("id")] string? Id = null,
    [property: JsonPropertyName("wal_start_segment")] string? WalStartSegment = null,
    [property: JsonPropertyName("size_bytes")] long? SizeBytes = null,
    [property: JsonPropertyName("error")] string? Error = null);

/// <summary>S3-контекст ops-операций (verify/restore/drill; секреты — ТОЛЬКО
/// в POST-теле, spec §3.2).</summary>
public sealed record S3Context(
    [property: JsonPropertyName("endpoints")] string[] Endpoints,
    [property: JsonPropertyName("access_key")] string AccessKey,
    [property: JsonPropertyName("secret_key")] string SecretKey,
    [property: JsonPropertyName("bucket")] string Bucket,
    [property: JsonPropertyName("prefix")] string Prefix,
    [property: JsonPropertyName("ca_pem")] string? CaPem = null);

public sealed record StartVerifyRequest(
    [property: JsonPropertyName("backup_id")] string BackupId,
    [property: JsonPropertyName("s3")] S3Context S3);

public sealed record StartRestoreRequest(
    [property: JsonPropertyName("backup_id")] string BackupId,
    // id ЛИШЬ у drill-запуска (arch/19 §3.6): scratch-каталог
    // backup-drill/<id>/pgdata; restore (null) пишет в PGDATA ноды.
    [property: JsonPropertyName("id")] string? Id,
    [property: JsonPropertyName("target_time")] string? TargetTime, // PG-совместимый yyyy-MM-dd HH:mm:sszzz (воркер нормализует RFC3339); null = latest
    [property: JsonPropertyName("recovery_timeout_sec")] int RecoveryTimeoutSec,
    [property: JsonPropertyName("s3")] S3Context S3);

/// <summary>Статус ops-операции (GET /api/ops/verify|restore); started_unix —
/// время TryBegin (возрастной бюджет verify воркера, arch/19 §6).</summary>
public sealed record OpsStatusResponse(
    [property: JsonPropertyName("state")] string State,
    [property: JsonPropertyName("phase")] string Phase,
    [property: JsonPropertyName("restored_to_lsn")] string? RestoredToLsn = null,
    [property: JsonPropertyName("system_id")] string? SystemId = null,
    [property: JsonPropertyName("started_unix")] long? StartedUnix = null,
    [property: JsonPropertyName("error")] string? Error = null);

public sealed record ErrorResponse([property: JsonPropertyName("error")] string Error);
