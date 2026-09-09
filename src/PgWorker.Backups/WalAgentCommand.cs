namespace PgWorker.Backups;

/// <summary>Билдер inline bash-команды контейнера WAL-агента (arch/19 §2/§3) —
/// ЕДИНСТВЕННОЕ место механики шиппера: запуск pg_receivewal в staging + цикл
/// доставки закрытых сегментов/history в S3 через mc с удалением после успешного
/// cp; `.partial` не грузится; смерть pg_receivewal гасит контейнер (restart-луп
/// подхватывает unless-stopped, воркер пересоздаёт со свежими env). Секреты —
/// ТОЛЬКО env контейнера (§7): S3 — одной строкой MC_HOST_pgwbkp (mc резолвит
/// alias из env: секреты не попадают в argv процессов и в ps), PG — по-переменно;
/// скрипт оперирует именами переменных.</summary>
public static class WalAgentCommand
{
    /// <summary>Имя env-переменной mc-alias (формат mc: MC_HOST_&lt;alias&gt;).</summary>
    public const string EnvMcHostVariable = "MC_HOST_pgwbkp";

    public const string EnvS3Bucket = "S3_BUCKET";
    public const string EnvCluster = "CLUSTER";
    public const string EnvShard = "SHARD";
    public const string EnvSlot = "SLOT";
    public const string EnvPgHost = "PG_HOST";
    public const string EnvPgPort = "PG_PORT";
    public const string EnvPgUser = "PG_USER";
    public const string EnvPgPassword = "PG_PASSWORD";
    public const string EnvPgDbName = "PG_DBNAME";
    public const string EnvStagingDir = "STAGING_DIR";
    public const string EnvStagingQuotaBytes = "STAGING_QUOTA_BYTES";
    public const string EnvPollSec = "POLL_SEC";

    /// <summary>Значение env MC_HOST_pgwbkp: scheme://access:secret@authority.
    /// URL-escape кредов — секреты per-install могут содержать спецсимволы URL.</summary>
    public static string McHost(string endpoint, string accessKey, string secretKey)
    {
        var uri = new Uri(endpoint);
        return $"{uri.Scheme}://{Uri.EscapeDataString(accessKey)}:{Uri.EscapeDataString(secretKey)}@{uri.Authority}";
    }

    /// <summary>Cmd контейнера: ["bash","-c",script]. S3-креды скрипту не нужны —
    /// alias pgwbkp приходит env-строкой MC_HOST_pgwbkp.</summary>
    public static IReadOnlyList<string> Build()
    {
        const string script = """
set -euo pipefail
# S3: alias pgwbkp резолвится mc из env MC_HOST_pgwbkp — креды не в argv (не видны в ps)
pg_receivewal --slot="$SLOT" -D "$STAGING_DIR" \
  -d "host=$PG_HOST port=$PG_PORT user=$PG_USER password=$PG_PASSWORD dbname=$PG_DBNAME sslmode=require" &
RECEIVE_PID=$!
while kill -0 "$RECEIVE_PID" 2>/dev/null; do
  if [ -n "${STAGING_QUOTA_BYTES:-}" ]; then
    USED=$(du -sb "$STAGING_DIR" | cut -f1)
    [ "$USED" -gt "$STAGING_QUOTA_BYTES" ] && exit 4
  fi
  find "$STAGING_DIR" -maxdepth 1 -type f ! -name '*.partial' -print0 |
    while IFS= read -r -d '' f; do
      if mc cp -- "$f" "pgwbkp/$S3_BUCKET/$CLUSTER/$SHARD/wal/$(basename -- "$f")"; then
        rm -f -- "$f"
      fi
    done
  sleep "$POLL_SEC"
done
wait "$RECEIVE_PID"
""";
        return ["bash", "-c", script];
    }
}
