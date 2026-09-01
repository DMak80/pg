using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;
using KafkaWorker.App;

namespace KafkaWorker.App.Api;

// arch/16 §1.1: X-Api-Key против env-секрета KFW_API_KEY (конфиг
// KafkaWorker:Api:ApiKey). Пусто — проверка отключена (доверленная
// docker-сеть). /healthz не трогаем. Порт PgWorker.App/Api/ApiKeyMiddleware.
public sealed class ApiKeyMiddleware(RequestDelegate next)
{
    public async Task InvokeAsync(HttpContext ctx, IOptions<KafkaWorkerOptions> options)
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
