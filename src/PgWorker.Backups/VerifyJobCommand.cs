namespace PgWorker.Backups;

/// <summary>Билдер inline bash-команды verify-джоба (arch/19 §2/§5, t04; паттерн
/// WalAgentCommand): скачивание full/&lt;id&gt;/ 1:1 в staging (mc) → pg_verifybackup
/// (manifest SHA256, вкл. pg_wal/ набора). Протокол t02: result-JSON в stdout +
/// exit-код; вывод утилиты — stderr. S3-креды — ТОЛЬКО env (MC_HOST_pgw собирает
/// воркер; секреты не в argv/ps). Статус PENDING/OK/FAILED пишет воркер — джоб
/// etcd не касается. Идемпотентен: S3 только читает.</summary>
public static class VerifyJobCommand
{
    public const string EnvMcHostVariable = "MC_HOST_pgw";
    public const string EnvS3Endpoint = "PGW_BK_S3_ENDPOINT";
    public const string EnvS3Region = "PGW_BK_S3_REGION";
    public const string EnvS3Bucket = "PGW_BK_S3_BUCKET";
    public const string EnvS3AccessKey = "PGW_BK_S3_ACCESS_KEY";
    public const string EnvS3SecretKey = "PGW_BK_S3_SECRET_KEY";
    public const string EnvPrefix = "PGW_BK_PREFIX";
    public const string EnvId = "PGW_BK_ID";
    public const string EnvStagingDir = "PGW_BK_STAGING_DIR";

    /// <summary>Значение env MC_HOST_pgw: scheme://access:secret@authority
    /// (URL-escape кредов — per-install спецсимволы). Невалидный/пустой endpoint
    /// не бросает (fail-fast конфига — не забота спеки): scheme опускается.</summary>
    public static string McHost(string endpoint, string accessKey, string secretKey)
    {
        var scheme = string.Empty;
        var authority = endpoint;
        try
        {
            var uri = new Uri(endpoint);
            scheme = $"{uri.Scheme}://";
            authority = uri.Authority;
        }
        catch (UriFormatException)
        {
            // пустой/кривой endpoint — джоб упадёт на подключении (transient), не спека
        }

        return $"{scheme}{Uri.EscapeDataString(accessKey)}:{Uri.EscapeDataString(secretKey)}@{authority}";
    }

    /// <summary>Cmd контейнера: ["bash","-c",script] — полная замена ENTRYPOINT.</summary>
    public static IReadOnlyList<string> Build()
    {
        const string script = """
    set -euo pipefail
    # протокол t02: result-JSON — stdout, вывод утилит — stderr/stdin-перехват
    LOG() { printf '%s\n' "$1"; }
    FAIL() {
      LOG "{\"ok\":false,\"phase\":\"$1\",\"error\":\"$(printf '%s' "$2" | tr '\n' ' ' | tr -d '"')\"}"
      exit 1
    }
    LOG '{"phase":"download"}'
    mc cp --recursive "pgw/$PGW_BK_S3_BUCKET/$PGW_BK_PREFIX/full/$PGW_BK_ID/" "$PGW_BK_STAGING_DIR/full/" \
      || FAIL download "mc cp failed"
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
