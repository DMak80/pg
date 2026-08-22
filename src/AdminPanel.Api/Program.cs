using AdminPanel.Api;
using AdminPanel.Api.Auth;
using AdminPanel.Api.Inspection;
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
   .AddCheck("self", () => HealthCheckResult.Healthy(), ["live"])
   .AddCheck<EtcdHealthCheck>("etcd"); // [t03] чек refresher'а; без тега live — healthz не роняет (arch/03 §1)

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
app.MapInspectionApi(); // [t04] эндпоинты инспекции etcd из снапшота (arch/03 §1)

// Живость самой панели (liveness, arch/03 §1): только чеки с тегом live.
// Чек etcd (readiness-семантика) не роняет /api/healthz — его статус отдают t04+ эндпоинты.
app.MapHealthChecks(
    "/api/healthz",
    new HealthCheckOptions
    {
        Predicate = registration => registration.Tags.Contains("live"),
        ResponseWriter = HealthzWriter.WriteStatus,
    });

app.Run();

// Экспозиция точки входа для WebApplicationFactory в интеграционных тестах.
public partial class Program;
