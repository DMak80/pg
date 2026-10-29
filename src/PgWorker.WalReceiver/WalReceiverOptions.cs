namespace PgWorker.WalReceiver;

/// <summary>Параметры одной стрим-сессии приёмника (arch/19 §3): DSN
/// источника репликации (фиксирует цикл приёмника: host 127.0.0.1:5432 ноды,
/// user backup_exec, dbname postgres — НЕ env), слот, координаты шарда и S3-цели
/// пула — write-список роли источника (порядок = приоритет) + read-список (все
/// ноды). Секреты — из etcd-резолва цикла (WalReceiverTargets), в логи не
/// попадают. CA https S3 — вне сессии: цикл материализует PEM в temp-файл и
/// передаёт S3-клиенту путём (CaBundlePath), PEM в options не дублируется.</summary>
public sealed record WalReceiverOptions(
    string PgHost,
    int PgPort,
    string PgUser,
    string PgPassword,
    string PgDbname,
    string Slot,
    string Cluster,
    string Shard,
    // write-список роли источника (порядок = приоритет) + read-список (все ноды
    // пула, порядок endpoints); URI http.
    IReadOnlyList<string> S3Endpoints,
    IReadOnlyList<string> S3ReadEndpoints,
    string S3Bucket,
    string S3AccessKey,
    string S3SecretKey);
