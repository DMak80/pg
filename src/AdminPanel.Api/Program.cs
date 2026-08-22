using AdminPanel.Api;
using AdminPanel.Api.Auth;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.DI;
using AdminPanel.Infrastructure.Traces;
using AdminPanel.Probes;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

// Точка входа панели: сборка хоста и модульная композиция сервисов.
var builder = WebApplication.CreateBuilder(args);

// Инициализация ActivitySource каркаса до первого HandleQuery (по образцу референса).
Tracing.Init(builder.Environment.ApplicationName);

builder
   .Services.UseDiBehaviours(builder.Configuration)
   .AddInfrastructure()
   .AddApi() // t02: auth-сервисы и [Config]-POCO Api-сборки
   .AddCore()
   .AddEtcd()
   .AddProbes()
   .AddOpenApi()
   .AddHealthChecks()
   .AddCheck("self", () => HealthCheckResult.Healthy(), ["live"]);

// t02: cookie-схема аутентификации (настройки — AdminPanel:Auth).
builder.Services.AddCookieAuth();

var app = builder.Build();

// t02: fail-closed — без пароля в конфиге логин невозможен, предупреждаем на старте.
var auth = app.Services.GetRequiredService<IOptions<AuthOptions>>().Value;
if (string.IsNullOrEmpty(auth.Password) && string.IsNullOrEmpty(auth.PasswordHash))
    app.Logger.LogWarning("AdminPanel:Auth: не задан ни Password, ни PasswordHash — логин отключён");

// OpenAPI-схема — только в dev-окружении.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

// t02: аутентификация + default-deny guard — всё /api/*, кроме login и healthz, → 401.
app.UseAuthentication();
app.UseApiAuthorization();
app.MapAuthApi();

// Живость самой панели; без авторизации.
app.MapHealthChecks(
    "/api/healthz",
    new HealthCheckOptions { ResponseWriter = HealthzWriter.WriteStatus });

app.Run();

// Экспозиция точки входа для WebApplicationFactory в интеграционных тестах.
public partial class Program;
