namespace PgWorker.WalReceiver;

/// <summary>Параметры WAL-приёмника (t27, arch/19 §3.1; t36 — списки пула):
/// DSN источника репликации, слот, координаты шарда и S3-цели пула —
/// write-список роли источника (порядок = приоритет) + read-список (все ноды).
/// Секреты — только env контейнера, в argv/логи/маркеры не попадают.</summary>
public sealed record WalReceiverOptions(
    string PgHost,
    int PgPort,
    string PgUser,
    string PgPassword,
    string PgDbname,
    string Slot,
    string Cluster,
    string Shard,
    // t36: S3_ENDPOINTS (запятая, порядок списка роли источника) +
    // S3_READ_ENDPOINTS (все ноды пула, порядок endpoints); URI http.
    IReadOnlyList<string> S3Endpoints,
    IReadOnlyList<string> S3ReadEndpoints,
    string S3Bucket,
    string S3AccessKey,
    string S3SecretKey,
    // t42 (arch/19 §7): PEM CA https S3-endpoint (материализуется Program приёмника
    // в файл → S3CaBundlePath: SDK-клиент — фабрика HttpClient с валидацией корня
    // цепочки по отпечатку CA); null — http-endpoint.
    string? S3CaPem = null);

/// <summary>Парсер env-контракта приёмника (arch/19 §3.1): невалидный env → null +
/// список ошибок. Значения СЕКРЕТНЫХ ключей (PG_PASSWORD, S3_SECRET_KEY) в тексты
/// ошибок не включаются никогда — только имена ключей.</summary>
public static class WalReceiverEnv
{
    // Значения env НИКОГДА не включаются в тексты ошибок (секреты: PG_PASSWORD,
    // S3_SECRET_KEY) — только имена ключей.

    public static WalReceiverOptions? Parse(Func<string, string?> env, out List<string> errors)
    {
        var failures = new List<string>();
        string Require(string key)
        {
            var value = env(key);
            if (string.IsNullOrWhiteSpace(value))
            {
                failures.Add($"required env {key} is missing or empty");
                return "";
            }

            return value;
        }

        var pgHost = Require("PG_HOST");
        var pgPortText = Require("PG_PORT");
        int pgPort = 0;
        if (failures.Count == 0 && (!int.TryParse(pgPortText, out pgPort) || pgPort is < 1 or > 65535))
            failures.Add("env PG_PORT must be an integer 1..65535");
        var pgUser = Require("PG_USER");
        var pgPassword = Require("PG_PASSWORD");
        var pgDbname = Require("PG_DBNAME");
        var slot = Require("SLOT");
        var cluster = Require("CLUSTER");
        var shard = Require("SHARD");
        // t36: список endpoints пула (роль источника) ОБЯЗАТЕЛЕН непустым —
        // перебор без целей бессмыслен (permanent, код 5).
        var s3EndpointsText = Require("S3_ENDPOINTS");
        var s3Endpoints = SplitEndpoints(s3EndpointsText);
        if (failures.Count == 0 && s3Endpoints.Count == 0)
            failures.Add("env S3_ENDPOINTS must be a non-empty comma-separated list of uri");
        // Read-список опционален: отсутствие = читать с write-целей (однонодовый пул).
        var s3ReadEndpoints = SplitEndpoints(env("S3_READ_ENDPOINTS") ?? "");
        var s3Bucket = Require("S3_BUCKET");
        var s3AccessKey = Require("S3_ACCESS_KEY");
        var s3SecretKey = Require("S3_SECRET_KEY");
        var s3CaPem = env("S3_CA_PEM"); // опциональный (t42): отсутствие — http-endpoint

        errors = failures;
        if (failures.Count > 0)
            return null;

        return new WalReceiverOptions(
            pgHost, pgPort, pgUser, pgPassword, pgDbname,
            slot, cluster, shard,
            s3Endpoints, s3ReadEndpoints.Count > 0 ? s3ReadEndpoints : s3Endpoints,
            s3Bucket, s3AccessKey, s3SecretKey,
            string.IsNullOrWhiteSpace(s3CaPem) ? null : s3CaPem);
    }

    // "uri1,uri2" → список непустых trimmed-элементов (мусорные пустые — долой).
    private static IReadOnlyList<string> SplitEndpoints(string raw)
        => raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
}
