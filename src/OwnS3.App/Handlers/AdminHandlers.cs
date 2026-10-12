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

    // Дисковые поля корня/диска (канон 05 §7): used = total − free по DriveInfo;
    // корень недоступен (IOException/UnauthorizedAccess и пр.) — поля null,
    // грань не падает (наблюдаемость деградации — метрики §5).
    private static (long? Used, long? Total) DiskOf(string root)
    {
        try
        {
            var d = new DriveInfo(root);
            return (d.TotalSize - d.TotalFreeSpace, d.TotalSize);
        }
        catch (Exception)
        {
            return (null, null); // корень недоступен — поля null
        }
    }

    // Агрегат дисковых полей: сумма не-null значений; все null → null (канон 05 §7).
    private static (long? Used, long? Total) SumDisks(IEnumerable<(long? Used, long? Total)> disks)
    {
        long? used = null, total = null;
        foreach (var disk in disks)
        {
            if (disk.Used is { } u)
                used = (used ?? 0) + u;
            if (disk.Total is { } t)
                total = (total ?? 0) + t;
        }

        return (used, total);
    }

    public static async Task HandleStorageAsync(HttpContext context)
    {
        var mode = context.RequestServices.GetRequiredService<StorageModeInfo>();
        object status;
        if (mode.IsErasure)
        {
            // EC-режим: полная картина сетов/деградации/heal
            var s = context.RequestServices.GetRequiredService<ErasureStatusProvider>().GetStatus();
            var top = SumDisks(s.Sets.SelectMany(set => set.Drives.Select(d => DiskOf(d.Path))));
            status = new
            {
                mode = s.Mode,
                usedBytes = top.Used,
                totalBytes = top.Total,
                sets = s.Sets.Select(set => new
                {
                    index = set.Index,
                    readable = set.Readable,
                    writable = set.Writable,
                    drives = set.Drives.Select(d =>
                    {
                        var disk = DiskOf(d.Path);
                        return new
                        {
                            position = d.Position,
                            path = d.Path,
                            diskId = d.DiskId,
                            online = d.Online,
                            usedBytes = disk.Used,
                            totalBytes = disk.Total,
                        };
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
            // single-режим: статус single-тома без сетов (грань доступна всегда — 05 §7);
            // дисковые поля — по корню данных (несколько корней — агрегат)
            var disks = mode.DataRoots.Select(DiskOf).ToList();
            var top = SumDisks(disks);
            status = new
            {
                mode = "XL Single",
                usedBytes = top.Used,
                totalBytes = top.Total,
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
