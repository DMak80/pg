using System.Security.Claims;
using System.Text.Json;
using AdminPanel.Infrastructure.CQRS;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Api.Auth;

// Тело POST /api/auth/login.
public sealed record LoginRequest(string? Username, string? Password);

// Композиция auth-модуля: cookie-схема, guard /api/*, эндпоинты (spec t02 §7.5).
public static class AuthModule
{
    public const string CookieName = "adminpanel_session";
    public const string ApiPrefix = "/api";
    public const string LoginPath = "/api/auth/login";
    public const string HealthzPath = "/api/healthz";

    // Cookie-схема аутентификации; значения — из [Config]-POCO AdminPanel:Auth.
    public static IServiceCollection AddCookieAuth(this IServiceCollection services)
    {
        services
           .AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
           .AddCookie(CookieAuthenticationDefaults.AuthenticationScheme, o =>
            {
                o.Cookie.Name = CookieName;
                o.Cookie.HttpOnly = true;
                o.Cookie.SameSite = SameSiteMode.Lax;
                o.Cookie.SecurePolicy = CookieSecurePolicy.Always;
                o.SlidingExpiration = true;
                // API не редиректит на логин-страницу: чистые 401/403 (spec t02 §3.7).
                o.Events.OnRedirectToLogin = ctx =>
                {
                    ctx.Response.StatusCode = StatusCodes.Status401Unauthorized;
                    return Task.CompletedTask;
                };
                o.Events.OnRedirectToAccessDenied = ctx =>
                {
                    ctx.Response.StatusCode = StatusCodes.Status403Forbidden;
                    return Task.CompletedTask;
                };
            });

        // Срок сессии и Secure-политика — из AuthOptions (spec t02 §3.7).
        // ILogger<Program>: маркер-тип не-static (AuthModule — static, иначе CS0718).
        services
           .AddOptions<CookieAuthenticationOptions>(CookieAuthenticationDefaults.AuthenticationScheme)
           .Configure<IOptions<AuthOptions>, ILogger<Program>>((o, auth, logger) =>
            {
                if (auth.Value.SessionHours <= 0)
                {
                    // Опечатка в конфиге — не роняем хост, откатываемся к 8 часам.
                    logger.LogWarning("AdminPanel:Auth:SessionHours <= 0 — использую 8 часов");
                    o.ExpireTimeSpan = TimeSpan.FromHours(8);
                }
                else
                    o.ExpireTimeSpan = TimeSpan.FromHours(auth.Value.SessionHours);

                o.Cookie.SecurePolicy = auth.Value.AllowHttp
                    ? CookieSecurePolicy.SameAsRequest
                    : CookieSecurePolicy.Always;
            });

        return services;
    }

    // Default-deny guard: всё /api/*, кроме login и healthz, требует cookie (spec t02 §3.8).
    public static IApplicationBuilder UseApiAuthorization(this IApplicationBuilder app)
        => app.Use(ApiGuard);

    private static async Task ApiGuard(HttpContext context, Func<Task> next)
    {
        var path = context.Request.Path;
        var isApi = path.StartsWithSegments(ApiPrefix);
        var isException = PathEquals(path, LoginPath) || PathEquals(path, HealthzPath);
        if (isApi && !isException && context.User.Identity?.IsAuthenticated != true)
        {
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            context.Response.ContentType = "application/problem+json";
            await context.Response.WriteAsync(JsonSerializer.Serialize(new
            {
                title = "Unauthorized",
                status = 401,
            }));
            return;
        }

        await next();
    }

    // Сравнение пути без учёта регистра (spec t02 §3.8).
    private static bool PathEquals(PathString path, string value)
        => path.Equals((PathString)value, StringComparison.OrdinalIgnoreCase);

    // Эндпоинты логина/логаута/сессии (arch/03 §1, spec t02 §4).
    public static IEndpointRouteBuilder MapAuthApi(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapPost(
            LoginPath,
            async (LoginRequest request, IAdminLoginService service, IOptions<AuthOptions> authOptions, HttpContext context) =>
            {
                var clientKey = context.Connection.RemoteIpAddress?.ToString() ?? "unknown";
                var result = service.Login(request.Username, request.Password, clientKey);
                if (result.Status == LoginStatus.Ok)
                {
                    // Имя в сессии — каноническое из настроек, не из запроса.
                    var principal = MakePrincipal(authOptions.Value.Username ?? request.Username ?? string.Empty);
                    await context.SignInAsync(CookieAuthenticationDefaults.AuthenticationScheme, principal);
                    return Results.NoContent();
                }

                if (result.Status == LoginStatus.RateLimited)
                {
                    context.Response.Headers["Retry-After"] = result.RetryAfterSeconds.ToString();
                    return Results.Problem(statusCode: StatusCodes.Status429TooManyRequests);
                }

                // Generic-ответ: не раскрываем, какое поле неверно.
                return Results.Problem(statusCode: StatusCodes.Status401Unauthorized, detail: "Invalid credentials");
            });

        endpoints.MapPost(
            "/api/auth/logout",
            async (HttpContext context) =>
            {
                await context.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
                return Results.NoContent();
            });

        endpoints.MapGet(
            "/api/auth/me",
            async (ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
            {
                var result = await handler.HandleQuery<MeQuery, MeDto>(new MeQuery(user.Identity!.Name!), ct);
                return result.IsSuccess
                    ? Results.Ok(new { username = result.Value.Username })
                    : Results.Problem(statusCode: StatusCodes.Status500InternalServerError);
            });

        return endpoints;
    }

    // Principal сессии: единственный claim — имя админа из настроек.
    private static ClaimsPrincipal MakePrincipal(string username)
        => new(new ClaimsIdentity(
            [new Claim(ClaimTypes.Name, username)],
            CookieAuthenticationDefaults.AuthenticationScheme));
}
