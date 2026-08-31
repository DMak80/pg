namespace PgWorker.Moves;

/// <summary>
/// Runtime-опции процессов переезда (t01 задача 11; appsettings-секции
/// PgWorker:Moves + PgWorker:Thresholds, arch/14 §8; дефолты — из скриптов):
/// поллинг ожиданий, параметры заморозки, защита abort, failover-слоты и пороги
/// cutover/недоступности + пороги репарации брошенных статусов (adopt-repair §3.5).
/// Маппер из конфига — интеграция (задача 17).
/// </summary>
public sealed record MovesRuntimeOptions(
    int PollIntervalSec = 2,
    int FreezeWaitSec = 5,
    int FreezeLockTimeoutSec = 5,
    int FreezeLockTries = 3,
    int AbortMinAgeSec = 120,
    bool FailoverSlots = true,
    int CutoverTimeoutSec = 90,
    int ConnFailBudgetSec = 120,
    string? AdvertisedPublisherHost = null,
    int RepairStaleSec = 600,
    int RepairFrozenSec = 120);
