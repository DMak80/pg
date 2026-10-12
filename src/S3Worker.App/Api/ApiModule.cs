using Microsoft.AspNetCore.Builder;

namespace S3Worker.App.Api;

// Маршрутизация HTTP API S3Worker (arch/24 §1.1): каркас t32 Task 3 —
// эндпоинты добавляются в Task 14 (7 операций мутаций домена + seed).
public static class ApiModule
{
    // Пустой каркас: эндпоинты Task 14.
    public static void MapWorkerApi(this WebApplication app)
    {
    }
}
