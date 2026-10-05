namespace PgWorker.WalReceiver;

/// <summary>Параметры WAL-приёмника (t27, arch/19 §3.1): DSN источника репликации,
/// слот, координаты шарда и S3-приёмника. Секреты — только env контейнера, в
/// argv/логи/маркеры не попадают.</summary>
public sealed record WalReceiverOptions(
    string PgHost,
    int PgPort,
    string PgUser,
    string PgPassword,
    string PgDbname,
    string Slot,
    string Cluster,
    string Shard,
    string S3Endpoint,
    string? S3Region,
    string S3Bucket,
    string S3AccessKey,
    string S3SecretKey,
    bool S3PathStyle);

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
                failures.Add($"обязательный env {key} отсутствует или пуст");
                return "";
            }

            return value;
        }

        var pgHost = Require("PG_HOST");
        var pgPortText = Require("PG_PORT");
        int pgPort = 0;
        if (failures.Count == 0 && (!int.TryParse(pgPortText, out pgPort) || pgPort is < 1 or > 65535))
            failures.Add("env PG_PORT должен быть целым числом 1..65535");
        var pgUser = Require("PG_USER");
        var pgPassword = Require("PG_PASSWORD");
        var pgDbname = Require("PG_DBNAME");
        var slot = Require("SLOT");
        var cluster = Require("CLUSTER");
        var shard = Require("SHARD");
        var s3Endpoint = Require("S3_ENDPOINT");
        var s3Region = env("S3_REGION"); // опциональный: отсутствие — валидно
        var s3Bucket = Require("S3_BUCKET");
        var s3AccessKey = Require("S3_ACCESS_KEY");
        var s3SecretKey = Require("S3_SECRET_KEY");

        var s3PathStyle = true;
        var pathStyleText = env("S3_PATHSTYLE");
        switch (pathStyleText)
        {
            case null:
            case "":
                break; // default true
            case "true":
            case "1":
                s3PathStyle = true;
                break;
            case "false":
            case "0":
                s3PathStyle = false;
                break;
            default:
                failures.Add("env S3_PATHSTYLE должен быть true/false/1/0");
                break;
        }

        errors = failures;
        if (failures.Count > 0)
            return null;

        return new WalReceiverOptions(
            pgHost, pgPort, pgUser, pgPassword, pgDbname,
            slot, cluster, shard,
            s3Endpoint, string.IsNullOrWhiteSpace(s3Region) ? null : s3Region,
            s3Bucket, s3AccessKey, s3SecretKey, s3PathStyle);
    }
}
