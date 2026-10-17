using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Http;
using OwnS3.Storage;
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
            // Один снапшот дисков на запрос: верхняя сумма и per-drive поля из одного
            // чтения DriveInfo — JSON внутренне консистентен (двойное чтение даёт
            // расхождение сумм при фоновой записи тома между чтениями)
            var setSnapshots = s.Sets
                .Select(set => (Set: set, Drives: set.Drives
                    .Select(d => new { d.Position, d.Path, d.DiskId, d.Online, Disk = DiskOf(d.Path) })
                    .ToList()))
                .ToList();
            var top = SumDisks(setSnapshots.SelectMany(x => x.Drives).Select(d => d.Disk));
            status = new
            {
                mode = s.Mode,
                usedBytes = top.Used,
                totalBytes = top.Total,
                sets = setSnapshots.Select(x => new
                {
                    index = x.Set.Index,
                    readable = x.Set.Readable,
                    writable = x.Set.Writable,
                    drives = x.Drives.Select(d => new
                    {
                        position = d.Position,
                        path = d.Path,
                        diskId = d.DiskId,
                        online = d.Online,
                        usedBytes = d.Disk.Used,
                        totalBytes = d.Disk.Total,
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

    // GET /s3/health/cluster (канон 05 §5): готовность хранилища обслуживать
    // запись — single: touch-проба тома (условия /healthz); EC: все сеты
    // writable (write-кворум, глава 06). Публичная проба наблюдения — без
    // подписи (прецедент /healthz; потребитель — ClusterOk-проба панели,
    // adminpanel/02 §2.5). Тело — JSON drives-счётчики camelCase (long);
    // healingDrives не отдаётся (heal-проход не per-drive — /admin/storage).
    public static async Task HandleClusterHealthAsync(HttpContext context)
    {
        var mode = context.RequestServices.GetRequiredService<StorageModeInfo>();
        long healthy, offline, total;
        bool writable;
        if (mode.IsErasure)
        {
            var status = context.RequestServices.GetRequiredService<ErasureStatusProvider>().GetStatus();
            var drives = status.Sets.SelectMany(set => set.Drives).ToList();
            total = drives.Count;
            healthy = drives.Count(d => d.Online);
            offline = total - healthy;
            writable = status.Sets.All(set => set.Writable);
        }
        else
        {
            var drive = context.RequestServices.GetRequiredService<IXlDrive>();
            writable = drive.CheckHealth();
            total = 1;
            healthy = writable ? 1 : 0;
            offline = 1 - healthy;
        }

        context.Response.StatusCode = writable
            ? StatusCodes.Status200OK
            : StatusCodes.Status503ServiceUnavailable;
        context.Response.ContentType = "application/json";
        await context.Response.WriteAsync(JsonSerializer.Serialize(new
        {
            healthyDrives = healthy,
            offlineDrives = offline,
            totalDrives = total,
        }, JsonOptions));
    }
}
