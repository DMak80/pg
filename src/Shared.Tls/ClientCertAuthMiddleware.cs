using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Http;

namespace Shared.Tls;

// 401-политика клиентского серта на HTTPS-грани воркеров (arch/14 §1.1 —
// канон всех четырёх воркеров: PgWorker/KafkaWorker/ValkeyWorker/S3Worker):
// хендшейк серт не требует (ClientCertificateMode.AllowCertificate), здесь —
// точечная авторизация. Валидный клиентский серт per-install API-CA обязателен
// ТОЛЬКО на префиксе /api: без серта или с невалидным сертом — HTTP 401 (НЕ
// разрыв TLS). /healthz и /metrics — открытая грань доверенной docker-сети:
// присланный серт игнорируется. Enabled=false — pass-through (режим
// AllowInsecureHttp, только in-memory WAF-тесты).
public sealed record ClientCertAuthOptions(bool Enabled, X509Certificate2? ClientCa);

public sealed class ClientCertAuthMiddleware(RequestDelegate next, ClientCertAuthOptions options)
{
    public async Task InvokeAsync(HttpContext context)
    {
        // 1) WAF-режим (AllowInsecureHttp): без TLS-грани — pass-through.
        if (!options.Enabled)
        {
            await next(context);
            return;
        }

        // 2) Открытая грань: всё, кроме точного сегмент-префикса /api
        // ("/api" или "/api/…", case-insensitive; "/apifoo" — не /api).
        var path = context.Request.Path.Value ?? "/";
        if (!path.Equals("/api", StringComparison.OrdinalIgnoreCase)
            && !path.StartsWith("/api/", StringComparison.OrdinalIgnoreCase))
        {
            await next(context);
            return;
        }

        // 3) Нет клиентского серта — 401 (без тела, без WWW-Authenticate).
        if (context.Connection.ClientCertificate is null)
        {
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            return;
        }

        // 4) Серт не проходит цепочку к per-install API-CA — 401.
        if (!TlsChain.ValidateChain(context.Connection.ClientCertificate, options.ClientCa!))
        {
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            return;
        }

        // 5) Валидный серт — пропустить дальше.
        await next(context);
    }
}
