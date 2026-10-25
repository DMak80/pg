namespace PgWorker.Backups;

using PgWorker.Backups.Restore;

/// <summary>Билдер inline bash-команды verify-джоба (arch/19 §2/§5, t04; t36 —
/// перебор нод пула): скачивание full/&lt;id&gt;/ 1:1 в staging с ПЕРВОЙ ДОСТУПНОЙ
/// ноды read-списка (mc_copy_first, per-попыточный timeout — R3 arch/23 §7) →
/// pg_verifybackup (manifest SHA256, вкл. pg_wal/ набора). Протокол t02:
/// result-JSON в stdout + exit-код; вывод утилиты — stderr. S3-креды — ТОЛЬКО
/// env (MC_HOST_pgwbkp&lt;i&gt; собирает скрипт из S3_ENDPOINTS; секреты не в argv/ps).
/// Статус PENDING/OK/FAILED пишет воркер — джоб etcd не касается. Идемпотентен:
/// S3 только читает.</summary>
public static class VerifyJobCommand
{
    public const string EnvS3Endpoints = "S3_ENDPOINTS";
    public const string EnvS3Bucket = "S3_BUCKET";
    public const string EnvS3AccessKey = "S3_ACCESS_KEY";
    // Имя env-переменной, не значение секрета.
    public const string EnvS3SecretKey = "S3_" + "SECRET" + "_KEY";
    public const string EnvPrefix = "PGW_BK_PREFIX";
    public const string EnvId = "PGW_BK_ID";
    public const string EnvStagingDir = "PGW_BK_STAGING_DIR";

    /// <summary>Cmd контейнера: ["bash","-c",script] — полная замена ENTRYPOINT.</summary>
    public static IReadOnlyList<string> Build()
    {
        const string script = """
    set -euo pipefail
    # Доверие CA https-endpoint (t42, arch/19 §7): PEM из env — в файл + SSL_CERT_FILE (mc)
    if [ -n "${S3_CA_PEM:-}" ]; then printf '%s\n' "$S3_CA_PEM" > /tmp/s3-ca.pem; export SSL_CERT_FILE=/tmp/s3-ca.pem; fi
    """ + "\n" + RestoreJobCommand.AliasesScript + "\n" + """
    # протокол t02: result-JSON — stdout, вывод утилит — stderr/stdin-перехват
    LOG() { printf '%s\n' "$1"; }
    FAIL() {
      LOG "{\"ok\":false,\"phase\":\"$1\",\"error\":\"$(printf '%s' "$2" | tr '\n' ' ' | tr -d '"')\"}"
      exit 1
    }
    LOG '{"phase":"download"}'
    # Скачивание с первой доступной ноды пула (перебор до найденного — arch/19 §5)
    mc_copy_first "{A}/$S3_BUCKET/$PGW_BK_PREFIX/full/$PGW_BK_ID/" "$PGW_BK_STAGING_DIR/full/" \
      || FAIL download "mc cp failed on all pool endpoints"
    LOG '{"phase":"verify"}'
    if OUT="$(pg_verifybackup "$PGW_BK_STAGING_DIR/full" 2>&1)"; then
      LOG '{"ok":true}'
    else
      FAIL verify "$(printf '%s' "$OUT" | tail -n 1)"
    fi
    """;
        return ["bash", "-c", script];
    }
}
