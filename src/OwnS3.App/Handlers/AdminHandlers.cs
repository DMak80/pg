using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Http;
using OwnS3.Storage.Erasure;

namespace OwnS3.App.Handlers;

// Админ-грань (канон 05 §7): GET /admin/storage — JSON-статус режим/сеты/диски/
// деградация/heal; POST /admin/heal — 202 (внеплановый проход; single — no-op).
// Доступ — SigV4 admin/root (проверяет S3Middleware ДО роутинга 21 операций).
// EC-сервисы резолвятся только в EC-режиме: их ленивые регистрации в single
// отказывают намеренно — развилка по StorageModeInfo.
public static class AdminHandlers
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };

    public static async Task HandleStorageAsync(HttpContext context)
    {
        var mode = context.RequestServices.GetRequiredService<StorageModeInfo>();
        object status;
        if (mode.IsErasure)
        {
            // EC-режим: полная картина сетов/деградации/heal
            var s = context.RequestServices.GetRequiredService<ErasureStatusProvider>().GetStatus();
            status = new
            {
                mode = s.Mode,
                sets = s.Sets.Select(set => new
                {
                    index = set.Index,
                    readable = set.Readable,
                    writable = set.Writable,
                    drives = set.Drives.Select(d => new
                    {
                        position = d.Position,
                        path = d.Path,
                        diskId = d.DiskId,
                        online = d.Online,
                    }),
                }),
                heal = new
                {
                    running = s.Heal.Running,
                    lastPassAt = s.Heal.LastPassAt,
                    healedObjects = s.Heal.HealedObjects,
                    healedShards = s.Heal.HealedShards,
                },
            };
        }
        else
        {
            // single-режим: статус single-тома без сетов (грань доступна всегда — 05 §7)
            status = new
            {
                mode = "XL Single",
                sets = Array.Empty<object>(),
                heal = new { running = false, lastPassAt = (DateTimeOffset?)null, healedObjects = 0L, healedShards = 0L },
            };
        }

        context.Response.ContentType = "application/json";
        await context.Response.WriteAsync(JsonSerializer.Serialize(status, JsonOptions));
    }

    public static Task HandleHealAsync(HttpContext context)
    {
        // heal существует только в EC-режиме: single — 202 no-op (канон 05 §7);
        // EC — внеплановый запуск (идущий проход продолжается, параллельный не создаётся)
        var mode = context.RequestServices.GetRequiredService<StorageModeInfo>();
        if (mode.IsErasure)
            context.RequestServices.GetRequiredService<ErasureHealService>().RequestPass();
        context.Response.StatusCode = StatusCodes.Status202Accepted;
        return Task.CompletedTask;
    }
}
