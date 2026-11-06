namespace EtcdNode.App;

// Конфигурация агента EtcdNode (arch/etcdnode/01-agent.md §3): доставляется
// env контейнера узла воркером EtcdNodeSpecBuilder. Чтение env — ручное
// (ключи с одиночными подчёркиваниями не биндятся конфиг-билдером).
// Отсутствующая переменная — дефолт; заданная и нечитаемая — fail-fast.

/// <summary>Корневые настройки агента.</summary>
public sealed class EtcdNodeOptions
{
    /// <summary>Имя своего узла etcdw-node-&lt;k&gt; (env ETCDNODE_NODE_NAME);
    /// уходит в .meta.json.instance и логи.</summary>
    public string NodeName { get; set; } = "etcdw-node-?";

    /// <summary>Путь к материализованному CA-PEM (env ETCDNODE_S3_CA_PEM
    /// записывается в temp-файл на старте Program; пусто — null, http-цель).</summary>
    public string? S3CaPemPath { get; set; }

    public SnapshotOptions Snapshots { get; set; } = new();
}

/// <summary>Параметры DR-слепков (arch/etcdnode/02-snapshots.md): значения
/// прежние (бюджеты не меняются).</summary>
public sealed class SnapshotOptions
{
    public int IntervalMin { get; set; } = 360;
    public int MinIntervalSec { get; set; } = 300;
    public int RetryIntervalSec { get; set; } = 300;
    public int TimeoutSec { get; set; } = 30;
    public int RetentionFiles { get; set; } = 10;
    public int RetentionObjects { get; set; } = 28;
    public int MaintenanceIntervalMin { get; set; } = 60;
}

/// <summary>Ручное чтение env ETCDNODE_*: Int(...)-хелпер с дефолтом,
/// битое значение — fallback-дефолт (fail-fast валидация собранного объекта —
/// в Load).</summary>
public static class EtcdNodeEnv
{
    /// <summary>Сборка опций из env; s3CaPemPath — уже материализованный
    /// Program'ом temp-файл (env несёт содержимое, не путь). Невалидный
    /// набор интервалов/ретенций — InvalidOperationException (fail-fast старта).</summary>
    public static EtcdNodeOptions Load(string? s3CaPemPath = null)
    {
        var name = Environment.GetEnvironmentVariable("ETCDNODE_NODE_NAME");
        var o = new EtcdNodeOptions
        {
            NodeName = name is { Length: > 0 } nodeName ? nodeName : "etcdw-node-?",
            S3CaPemPath = s3CaPemPath,
            Snapshots = new SnapshotOptions
            {
                IntervalMin = Int("ETCDNODE_SNAPSHOTS__INTERVAL_MIN", 360),
                MinIntervalSec = Int("ETCDNODE_SNAPSHOTS__MIN_INTERVAL_SEC", 300),
                RetryIntervalSec = Int("ETCDNODE_SNAPSHOTS__RETRY_INTERVAL_SEC", 300),
                TimeoutSec = Int("ETCDNODE_SNAPSHOTS__TIMEOUT_SEC", 30),
                RetentionFiles = Int("ETCDNODE_SNAPSHOTS__RETENTION_FILES", 10),
                RetentionObjects = Int("ETCDNODE_SNAPSHOTS__RETENTION_OBJECTS", 28),
                MaintenanceIntervalMin = Int("ETCDNODE_SNAPSHOTS__MAINTENANCE_INTERVAL_MIN", 60),
            },
        };

        // Fail-fast (arch/etcdnode/01-agent.md §3): интервалы > 0, ретенции ≥ 1.
        if (o.Snapshots.IntervalMin <= 0 || o.Snapshots.MinIntervalSec <= 0
            || o.Snapshots.RetryIntervalSec <= 0 || o.Snapshots.TimeoutSec <= 0
            || o.Snapshots.RetentionFiles < 1 || o.Snapshots.RetentionObjects < 1
            || o.Snapshots.MaintenanceIntervalMin <= 0)
            throw new InvalidOperationException(
                $"ETCDNODE_SNAPSHOTS__*: invalid values (intervalMin={o.Snapshots.IntervalMin},"
                + $" minIntervalSec={o.Snapshots.MinIntervalSec}, retryIntervalSec={o.Snapshots.RetryIntervalSec},"
                + $" timeoutSec={o.Snapshots.TimeoutSec}, retentionFiles={o.Snapshots.RetentionFiles},"
                + $" retentionObjects={o.Snapshots.RetentionObjects},"
                + $" maintenanceIntervalMin={o.Snapshots.MaintenanceIntervalMin})");
        return o;
    }

    public static int Int(string key, int fallback)
        => Environment.GetEnvironmentVariable(key) is { } raw && int.TryParse(raw, out var v)
            ? v
            : fallback;
}
