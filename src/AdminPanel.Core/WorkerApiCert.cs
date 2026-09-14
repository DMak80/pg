namespace AdminPanel.Core;

/// <summary>
/// Метаданные целевого серверного серта API воркера — ключ
/// /workers/api_tls/&lt;worker&gt; (arch/adminpanel/02 §9.9): панель — единственный
/// писатель, воркеры читают при старте. PEM-материалы (cert/key) в модель НЕ
/// попадают — наружу только метаданные (прецедент kafka-паролей §10.1).
/// </summary>
public sealed record WorkerApiCert(
    string Thumbprint,            // sha256-hex (lowercase) серта
    string Subject,
    string Issuer,
    IReadOnlyList<string> San,    // DNS-имена + IP (строками)
    DateTimeOffset NotBefore,
    DateTimeOffset NotAfter,
    long UpdatedUnix,
    string? UpdatedBy);
