using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using PgWorker.WalReceiver;
using Shared.Metrics;

namespace PgWorker.WalReceiver;

// Точка входа приёмника (t25/t26, arch/19 §2/§3/§3.3): WebApplication — база
// образа требует aspnet-рантайм; Kestrel 0.0.0.0:8080 несёт /metrics
// (Shared.Metrics.AddAppMetrics/MapAppMetrics, Meter PgWorker.WalReceiver,
// словарь arch/18 §2.8) и HTTP API операций бэкапов (arch/19 §2/§3.5/§3.6/§5;
// маршруты — шаги 5–7). Режимы (spec t26 §3.3): нодовый — identity из env
// контейнера ноды + hosted-цикл стрима WalReceiverLoop; tool-режим
// (PGW_NODE_TOOL=1) — WalReceiver главный процесс tool-контейнера БЕЗ
// identity: hosted-цикл не регистрируется (нет identity — нет цикла стрима),
// etcd-статусов агент не пишет, единственный канал команд/статуса — API.
// Метрики — одинаковые в обоих режимах (новых серий нет, arch/18 §2.8).
// Реестр операций — singleton в обоих режимах (нодовый: полный; tool: одна
// операция на контейнер).
// Явный internal-класс (не top-level statements): сгенерированный top-level
// Program публичен в глобальном namespace и конфликтует (CS0433) с
// PgWorker.App.Program в сборках, ссылающихся на оба проекта (тесты).
internal static class Program
{
    private static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        builder.WebHost.UseUrls("http://0.0.0.0:8080");
        builder.Services.AddAppMetrics(WalReceiverMetrics.MeterName, builder.Configuration.GetSection("Metrics"));
        builder.Services.AddSingleton<Operations.OperationRegistry>();

        var toolMode = Environment.GetEnvironmentVariable("PGW_NODE_TOOL") == "1";
        if (!toolMode)
        {
            // Нодовый режим: стрим WAL + API полного бэкапа (маршруты — шаг 5).
            // Identity парсит сам WalReceiverLoop: невалидный env — exit 1
            // (supervisord с exitcodes=0 не перезапускает).
            builder.Services.AddHostedService<WalReceiverLoop>();
        }

        var app = builder.Build();
        app.MapAppMetrics();
        await app.RunAsync();
    }
}
