using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using PgWorker.Etcd.S3;
using PgWorker.WalReceiver;
using PgWorker.WalReceiver.Operations;
using Shared.Etcd.Client;
using Shared.Metrics;

namespace PgWorker.WalReceiver;

// Точка входа приёмника (t25/t26, arch/19 §2/§3/§3.3): WebApplication — база
// образа требует aspnet-рантайм; Kestrel 0.0.0.0:8080 несёт /metrics
// (Shared.Metrics, Meter PgWorker.WalReceiver, словарь arch/18 §2.8) и HTTP
// API операций бэкапов (arch/19 §2/§3.5/§3.6/§5; канал arch/25 №22).
// Режимы (spec t26 §3.3): нодовый — identity из env контейнера ноды +
// hosted-цикл стрима WalReceiverLoop + маршруты /api/backup/full*; tool-режим
// (PGW_NODE_TOOL=1) — WalReceiver главный процесс tool-контейнера БЕЗ
// identity: hosted-цикл не регистрируется (нет identity — нет цикла стрима),
// etcd-статусов агент не пишет, единственный канал команд/статуса — API
// (маршруты — шаг 6). Метрики — одинаковые в обоих режимах (новых серий нет).
// Реестр операций — singleton в обоих режимах (нодовый: полный; tool: одна
// операция на контейнер). PGW_WALRECEIVER_NO_STREAM=1 — тестовый выключатель
// hosted-цикла (API-тесты маршрутов поднимают Program без identity/etcd).
// Явный internal-класс (не top-level statements): сгенерированный top-level
// Program публичен в глобальном namespace и конфликтует (CS0433) с
// PgWorker.App.Program в сборках, ссылающихся на оба проекта (тесты).
internal static class Program
{
    // Дефолт каталога staging полного в data-volume ноды (arch/19 §9);
    // перекрывается env PGW_BACKUP_STAGING_DIR (ставит EnsureNode — шаг 11).
    private const string DefaultStagingDir = "/home/postgres/pgdata/backup-staging";

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        builder.WebHost.UseUrls("http://0.0.0.0:8080");
        builder.Services.AddAppMetrics(WalReceiverMetrics.MeterName, builder.Configuration.GetSection("Metrics"));
        builder.Services.AddSingleton<OperationRegistry>();

        var toolMode = Environment.GetEnvironmentVariable("PGW_NODE_TOOL") == "1";
        WalReceiverIdentity? identity = null;
        string[] endpoints = [];
        if (toolMode)
        {
            // Tool-режим: identity нет — hosted-цикл стрима не стартует;
            // маршруты /api/ops/* — шаг 6. Инъекция фабрик — там же.
        }
        else
        {
            // Нодовый режим: identity обязана быть валидной (env ноды —
            // невалидный набор: лог в stdout + exit 1; supervisord с
            // exitcodes=0 не перезапускает — лупинга env-дефекта нет).
            identity = WalReceiverIdentity.Parse(Environment.GetEnvironmentVariable, out var errors);
            if (identity is null)
            {
                Console.Out.WriteLine(
                    $"walreceiver: invalid identity env: {string.Join("; ", errors)} — exit 1");
                Environment.Exit(1);
            }
            endpoints = identity.EtcdEndpoints.ToArray();

            var etcd = new EtcdGateway(new HttpClient { Timeout = TimeSpan.FromSeconds(10) });
            var pools = new S3PoolStore(
                new EtcdGateway(new HttpClient { Timeout = TimeSpan.FromSeconds(10) }), endpoints);
            Func<CancellationToken, Task<WalReceiverRole>> roleProbe = ct =>
                PatroniRoleProbe.ProbeAsync(
                    Environment.GetEnvironmentVariable("SSL_RESTAPI_CA") ?? "", ct);

            builder.Services.AddSingleton<IEtcdGateway>(etcd);
            builder.Services.AddSingleton(endpoints);
            builder.Services.AddSingleton<IS3PoolStore>(pools);

            // Стрим WAL — тот же процесс: hosted-цикл с общими deps
            // (PGW_WALRECEIVER_NO_STREAM=1 — тестовый выключатель).
            if (Environment.GetEnvironmentVariable("PGW_WALRECEIVER_NO_STREAM") != "1")
            {
                builder.Services.AddSingleton(sp => new WalReceiverLoop(
                    Environment.GetEnvironmentVariable, etcd, endpoints, pools, roleProbe,
                    o => new NpgsqlXLogSource(o), o => new NpgsqlSlotPositionReader(o)));
                builder.Services.AddHostedService(sp => sp.GetRequiredService<WalReceiverLoop>());
            }

            // Staging полного (Р5): env ноды → дефолт arch/19 §9.
            var stagingDir = Environment.GetEnvironmentVariable("PGW_BACKUP_STAGING_DIR")
                is { Length: > 0 } dir ? dir : DefaultStagingDir;
            builder.Services.AddSingleton<IFullBackupOperationFactory>(sp =>
                new FullBackupOperationFactory(
                    identity, etcd, endpoints, pools, roleProbe,
                    new ProcessRunner(), stagingDir,
                    sp.GetRequiredService<OperationRegistry>()));
        }

        var app = builder.Build();
        app.MapAppMetrics();
        if (!toolMode)
            MapFullBackupRoutes(app, identity!, endpoints);
        await app.RunAsync();
    }

    // Маршруты операции полного (arch/19 §2, spec §3.2): POST — 503 стоп-гейтов
    // (операция не начинается; Unknown — транспортный отказ etcd, fail-closed),
    // 409 (вторая активная), 202 (принята, фоновая Task); GET — 200 снимок /
    // 404 (нет активной — vanished-семантика); cancel — 202 best-effort / 404.
    // Гейты читаются только при СТАРТЕ: идущая операция стоп-гейтами не рвётся
    // (отменяют только cancel и бюджет воркера).
    private static void MapFullBackupRoutes(
        WebApplication app, WalReceiverIdentity identity, string[] endpoints)
    {
        // Активная операция для cancel (снимок — в реестре, экземпляр — здесь).
        IReceiverOperation? active = null;

        app.MapPost("/api/backup/full", async (
            StartFullRequest body,
            OperationRegistry registry,
            IFullBackupOperationFactory factory,
            IEtcdGateway etcd,
            CancellationToken ct) =>
        {
            var gates = await WalReceiverGates.ReadAsync(
                etcd, endpoints, identity.Cluster, identity.Shard, ct);
            if (gates.Disabled || gates.Paused || gates.Unknown)
                return Results.Json(
                    new ErrorResponse("backups disabled, paused or gates unknown"),
                    JsonOpts, statusCode: 503);
            if (!registry.TryBegin(new OperationSnapshot(
                    body.Id, OperationStates.Running, OperationPhases.Basebackup)))
                return Results.Json(
                    new ErrorResponse("operation already active"), JsonOpts, statusCode: 409);

            var operation = factory.Create(body.Id);
            active = operation;
            // Фоновая Task вне запроса (ответ 202 — операция живёт дальше);
            // RunAsync сам гасит все исключения в failed-снимок.
            _ = Task.Run(() => operation.RunAsync(CancellationToken.None));
            return Results.Json(
                new FullStatusResponse(OperationStates.Running, OperationPhases.Basebackup),
                JsonOpts, statusCode: 202);
        });

        app.MapGet("/api/backup/full", (OperationRegistry registry) =>
            registry.Current is { } snapshot
                ? Results.Json(new FullStatusResponse(
                        snapshot.State, snapshot.Phase,
                        snapshot.WalStartSegment, snapshot.SizeBytes,
                        snapshot.Error),
                    JsonOpts)
                : Results.Json(
                    new ErrorResponse("no active operation"), JsonOpts, statusCode: 404));

        app.MapPost("/api/backup/full/cancel", async (OperationRegistry registry) =>
        {
            if (registry.Current is null || active is null)
                return Results.Json(
                    new ErrorResponse("no active operation"), JsonOpts, statusCode: 404);
            await active.CancelAsync();
            return Results.Json(new { state = "cancelling" }, JsonOpts, statusCode: 202);
        });
    }
}
