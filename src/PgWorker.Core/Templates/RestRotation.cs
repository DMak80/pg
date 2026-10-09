using System.Security.Cryptography;
using System.Text;

namespace PgWorker.Core.Templates;

/// <summary>
/// Общий хелпер REST-грани Patroni (t22, arch/14 §2.1/§5): константы env,
/// sha256-хеш эффективной REST-пары (факт «контейнер несёт эту пару» —
/// инспекция env без раскрытия секрета, сверка прогресса rolling-ротации)
/// и резолвор эффективной пары окна ротации.
/// </summary>
public static class RestRotation
{
    /// <summary>Username basic-auth Patroni REST — константа (Patroni требует
    /// пару username+password в конфиге каждой ноды; отдельного user-ключа нет).</summary>
    public const string RestUsername = "patroni";

    /// <summary>PEM серта REST-эндпоинта ноды (материализует Spilo в certfile).</summary>
    public const string EnvCert = "SSL_RESTAPI_CERTIFICATE";

    /// <summary>sha256-hex эффективной REST-пары контейнера (инспекция env).</summary>
    public const string EnvPasswordHash = "PGW_REST_PASSWORD_HASH";

    /// <summary>Короткий sha256-hex пары (hex-lower).</summary>
    public static string PasswordHash(string restPassword)
        => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(restPassword))).ToLowerInvariant();

    /// <summary>
    /// Эффективная REST-пара сборки env (арх/14 §5 I): пока журнал несёт
    /// rest_pending (окно rolling-ротации) — env собирается из pending, после
    /// txn-коммита — из ключа кластера. Все EnsureNode-пути окна не расширяют
    /// её: пересоздание любым путём ставит NEW-пароль.
    /// </summary>
    public static string? EffectivePassword(string? snapshotRestPassword, string? pendingRestPassword)
        => pendingRestPassword ?? snapshotRestPassword;
}
