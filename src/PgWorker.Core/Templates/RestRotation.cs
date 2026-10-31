namespace PgWorker.Core.Templates;

/// <summary>
/// Общий хелпер REST-грани Patroni (arch/14 §2.1/§5): константа REST-пары
/// и резолвор эффективной пары окна ротации. env-хеш механики упразднён t05
/// (rest-пара доставляется bootstrap-каналом, не env — arch/14 §4).
/// </summary>
public static class RestRotation
{
    /// <summary>Username basic-auth Patroni REST — константа (Patroni требует
    /// пару username+password в конфиге каждой ноды; отдельного user-ключа нет).</summary>
    public const string RestUsername = "patroni";

    /// <summary>
    /// Эффективная REST-пара сборки env (арх/14 §5 I): пока журнал несёт
    /// rest_pending (окно rolling-ротации) — bootstrap собирается из pending,
    /// после txn-коммита — из ключа кластера. Все EnsureNode-пути окна не
    /// расширяют её: пересоздание любым путём ставит NEW-пароль.
    /// </summary>
    public static string? EffectivePassword(string? snapshotRestPassword, string? pendingRestPassword)
        => pendingRestPassword ?? snapshotRestPassword;
}
