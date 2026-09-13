namespace PgWorker.Backups;

/// <summary>Билдер inline bash-команды контейнера WAL-агента (arch/19 §2/§3) —
/// ЕДИНСТВЕННОЕ место механики шиппера: pg_receivewal в staging + цикл доставки
/// закрытых сегментов/history в S3 через mc с удалением после успешного cp;
/// `.partial` не грузится. Обрыв приёмника (рестарт PG мастера при смене
/// sync-standby, сеть) — штатное событие: скрипт ПЕРЕПОДКЛЮЧАЕТ pg_receivewal
/// — слот продолжает с restart_lsn, текущий `.partial` доигрывается до закрытия
/// и доставляется; гашение контейнера — только переполнение staging (exit 4,
/// супервиз пересоздаёт агента чистым томом). Секреты —
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
deliver() {
  find "$STAGING_DIR" -maxdepth 1 -type f ! -name '*.partial' -print0 |
    while IFS= read -r -d '' f; do
      if mc cp -- "$f" "pgwbkp/$S3_BUCKET/$CLUSTER/$SHARD/wal/$(basename -- "$f")"; then
        rm -f -- "$f"
      fi
    done
}
while :; do
  pg_receivewal --slot="$SLOT" -D "$STAGING_DIR" \
    -d "host=$PG_HOST port=$PG_PORT user=$PG_USER password=$PG_PASSWORD dbname=$PG_DBNAME sslmode=require" &
  RECEIVE_PID=$!
  while kill -0 "$RECEIVE_PID" 2>/dev/null; do
    if [ -n "${STAGING_QUOTA_BYTES:-}" ]; then
      USED=$(du -sb "$STAGING_DIR" | cut -f1)
      if [ "$USED" -gt "$STAGING_QUOTA_BYTES" ]; then
        kill "$RECEIVE_PID" 2>/dev/null || true
        wait "$RECEIVE_PID" 2>/dev/null || true
        exit 4
      fi
    fi
    deliver
    sleep "$POLL_SEC"
  done
  RC=0
  wait "$RECEIVE_PID" || RC=$?
  echo "pgw-wal-agent: pg_receivewal exited (rc=$RC) — переподключение" >&2
  deliver
  sleep "$POLL_SEC"
done
""";
        return ["bash", "-c", script];
    }
}
