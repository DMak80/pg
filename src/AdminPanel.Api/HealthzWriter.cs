using System.Text.Json;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace AdminPanel.Api;

// Пишет компактный JSON-ответ контракта /api/healthz: {"status":"ok"} и производные статусы.
public static class HealthzWriter
{
    public static async Task WriteStatus(HttpContext context, HealthReport report)
    {
        var status = report.Status switch
        {
            HealthStatus.Healthy => "ok",
            HealthStatus.Degraded => "degraded",
            _ => "unhealthy",
        };
        context.Response.ContentType = "application/json";
        await context.Response.WriteAsync(JsonSerializer.Serialize(new { status }));
    }
}
