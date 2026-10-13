namespace OwnS3.Replication.Lww;

// Исход LWW-применения реплики (глава 07 §7/§10) — терминальные Success-варианты;
// двухбулевый результат (Applied, Conflict) демонтируется. Четыре значения
// кодируют ровно то, что сегодня несут два bool (Applied, Conflict): пара
// ConflictApplied/ConflictSkipped выражает флаг конфликта канона 07 §10 ПОВЕРХ
// исхода применения — у фетчика конфликт сосуществует с applied/skipped,
// инкременты байт-в-байт как сегодня (см. Task 6).
public enum ReplicaApplyOutcome
{
    /// <summary>IncomingWins без конфликта — реплика применена.</summary>
    Applied,

    /// <summary>Identical/LocalWins без конфликта — пропуск без применения.</summary>
    Skipped,

    /// <summary>Равные modTime, разный sha — тай-брейк по sha, входящая победила
    /// и применена: фетчер инкрементирует conflict И applied (канон 07 §10).</summary>
    ConflictApplied,

    /// <summary>Равные modTime, разный sha — тай-брейк по sha в пользу локальной,
    /// входящая не применена: фетчер инкрементирует conflict И skipped.</summary>
    ConflictSkipped,
}
