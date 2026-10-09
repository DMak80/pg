using System.Security.Cryptography.X509Certificates;

namespace PgWorker.App;

/// <summary>
/// Материал REST-TLS нод (arch/14 §8): per-install CA PEM + ключ выпуска
/// серверных сертов REST-эндпоинтов нод + распарсенный сертификат CA (для
/// TLS-верификации клиентом «patroni»). Разбирается на старте fail-fast;
/// живёт всё приложение.
/// </summary>
public sealed record RestTlsMaterial(string CaPem, string CaKeyPem, X509Certificate2 Ca);
