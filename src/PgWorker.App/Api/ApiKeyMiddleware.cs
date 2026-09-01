using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;
using PgWorker.App;

namespace PgWorker.App.Api;

// arch/14 §1.1: X-Api-Key против env-секрета PGW_API_KEY (конфиг PgWorker:Api:ApiKey).
// Пусто — проверка отключена (доверенная docker-сеть). /healthz не трогаем.
public sealed class ApiKeyMiddleware(RequestDelegate next)
{
    public async Task InvokeAsync(HttpContext ctx, IOptions<PgWorkerOptions> options)
    {
        var key = options.Value.Api.ApiKey;
        if (!string.IsNullOrEmpty(key)
            && ctx.Request.Path.StartsWithSegments("/api")
            && !string.Equals(ctx.Request.Headers["X-Api-Key"], key))
        {
            ctx.Response.StatusCode = StatusCodes.Status401Unauthorized;
            await ctx.Response.WriteAsJsonAsync(new { title = "Unauthorized", status = 401 });
            return;
        }
        await next(ctx);
    }
}
