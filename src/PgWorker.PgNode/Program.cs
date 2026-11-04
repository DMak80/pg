using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PgWorker.Etcd.S3;
using PgWorker.PgNode;
using PgWorker.PgNode.Operations;
using Shared.Etcd.Client;
using Shared.Metrics;

namespace PgWorker.PgNode;

// Точка входа приёмника (/, arch/19 §2/§3/§3.3): WebApplication — база
// образа требует aspnet-рантайм; Kestrel 0.0.0.0:8080 несёт /metrics
// (Shared.Metrics, Meter PgWorker.PgNode, словарь arch/18 §2.8) и HTTP
// API операций бэкапов (arch/19 §2/§3.5/§3.6/§5; канал arch/25 №22).
// Режим ОДИН: identity из env контейнера ноды + hosted-цикл стрима
// WalReceiverLoop + маршруты /api/backup/full* и ops-маршруты /api/ops/*
// (verify/restore/cancel) — ВСЕ операции бэкапов исполняет этот единственный
// процесс: полный — full-реестр, verify/restore/drill — общий ops-реестр
// (полный ops-операцию не блокирует — разные реестры, arch/19 §5).
// PGW_WALRECEIVER_NO_STREAM=1 — тестовый выключатель hosted-цикла (API-тесты
// маршрутов поднимают Program без identity/etcd).
// Явный internal-класс (не top-level statements): сгенерированный top-level
// Program публичен в глобальном namespace и конфликтует (CS0433) с
// PgWorker.App.Program в сборках, ссылающихся на оба проекта (тесты).
// Не static: WebApplicationFactory<T> требует не-static тип (API-тесты,
// шаг 9); точка входа — static Main.
internal partial class Program
{
    // Дефолт каталога staging полного в data-volume ноды (arch/19 §9);
    // перекрывается env PGW_BACKUP_STAGING_DIR (ставит EnsureNode — шаг 11).
    private const string DefaultStagingDir = NodePaths.Staging;

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private static async Task Main(string[] args)
    {
        // Sub-режим restore_command (spec §3.4): `walreceiver fetch-wal
        // <seg> <dest>` — ДО создания builder; итог — exit-код процесса
        // (0 — объект записан; 1 — объекта нет/отказ — конец WAL для postgres).
        if (args is [{ } sub, ..] && sub == "fetch-wal")
            Environment.Exit(await Operations.FetchWalCommand.RunAsync(args[1..]));

        // Identity обязана быть валидной (env ноды — невалидный набор: лог в
        // stdout + exit 1; supervisord с exitcodes=0 не перезапускает —
        // лупинга env-дефекта нет).
        var identity = PgNodeIdentity.Parse(Environment.GetEnvironmentVariable, out var errors);
        if (identity is null)
        {
            Console.Out.WriteLine(
                $"walreceiver: invalid identity env: {string.Join("; ", errors)} — exit 1");
            Environment.Exit(1);
        }
        var endpoints = identity.EtcdEndpoints.ToArray();

        var etcd = new EtcdGateway(new HttpClient { Timeout = TimeSpan.FromSeconds(10) });

        // Статусная машина агента (arch/14 §3.3/§4, spec §3.6): mTLS-канал
        // :8081 и хендшейк живут этим же процессом; стрим/операции бэкапов от
        // машины не зависят (spec §3.6-6). Применение ротаций — ПО МАТЕРИАЛУ
        // (arch/14 §5 I, spec §8-29, П11.9): пароль — reload Patroni, серт —
        // рестарт Patroni-программы (реплика), пулер — supervisorctl.
        // Машина строится ПОСЛЕ Build: её диагностика идёт через ILogger
        // хоста (строки гарантированно попадают в docker-лог контейнера;
        // прецедент «молчащего stderr» красного прогона окна ротации:
        // Console.Error-строки применителя в лог не попали при
        // redirect_stderr=true). Kestrel-лямбды захватывают машину ссылкой —
        // исполняются на запросах, когда машина уже построена.
        var supervisor = new SupervisorCommands(new ProcessRunner());
        PgNodeMachine? machineRef = null;

        var builder = WebApplication.CreateBuilder(args);
        builder.WebHost.ConfigureKestrel(o =>
        {
            o.ListenAnyIP(8080); // metrics + HTTP API операций (arch/25 №22)
            o.ListenAnyIP(8081, lo => lo.UseHttps(https =>
            {
                // Канал доставки секретов (arch/13 §2, arch/25 №23): серверный
                // серт — из bootstrap-ключа (null до чтения → handshake
                // отклоняется: канала нет, воркер повторит тиком); клиент
                // обязан предъявить валидный серт воркера (публичный CA из env).
                // Валидация клиента — ClientCertificateValidation (фактический
                // колбэк-хук HttpsConnectionAdapterOptions текущего TFM).
                https.ClientCertificateMode = ClientCertificateMode.RequireCertificate;
                https.ServerCertificateSelector = (_, _) => machineRef?.ServerCertificate;
                https.ClientCertificateValidation = (certificate, _, _) =>
                    NodeClientCa() is { } ca
                    && ClientCertValidator.Validate(certificate, ca);
            }));
        });
        builder.Services.AddAppMetrics(WalReceiverMetrics.MeterName, builder.Configuration.GetSection("Metrics"));
        builder.Services.AddSingleton<OperationRegistry>();
        builder.Services.AddSingleton<OpsOperationRegistry>();
        builder.Services.AddSingleton<IProcessRunner, ProcessRunner>();

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
                o => new NpgsqlXLogSource(o), o => new NpgsqlSlotPositionReader(o),
                hbaEnsure: ct => new HbaEnsureOperation(new ProcessRunner()).RunAsync(ct)));
            builder.Services.AddHostedService(sp => sp.GetRequiredService<WalReceiverLoop>());
        }

        // Staging остаётся только у ops-операций (drill scratch); полный на
        // диск не пишет (spec t01 §6.17) — env удаляется шагом 10.
        var stagingDir = Environment.GetEnvironmentVariable("PGW_BACKUP_STAGING_DIR")
            is { Length: > 0 } dir ? dir : DefaultStagingDir;
        builder.Services.AddSingleton<IFullBackupOperationFactory>(sp =>
            new FullBackupOperationFactory(
                identity, etcd, endpoints, pools, roleProbe,
                sp.GetRequiredService<OperationRegistry>()));
        // Ops-операции (verify/restore/drill) — тот же процесс: общий
        // ops-реестр (одна активная; полный — свой реестр, arch/19 §5).
        builder.Services.AddSingleton<IOpsOperationFactory>(sp =>
            new NodeOperationFactory(
                stagingDir, sp.GetRequiredService<OpsOperationRegistry>()));

        var app = builder.Build();

        // Логгеры готового хоста: машина + apply-команды печатают в
        // консольный провайдер хоста (docker-лог контейнера).
        var loggerFactory = app.Services.GetRequiredService<ILoggerFactory>();
        var machine = new PgNodeMachine(
            identity, etcd, endpoints,
            supervisor,
            Environment.GetEnvironmentVariable("DOORMAN_CONFIG_TEMPLATE") ?? "",
            patroniApply: new PatroniApplyCommands(
                supervisor, loggerFactory.CreateLogger("pgnode.patroni-apply")),
            logger: loggerFactory.CreateLogger<PgNodeMachine>());
        machineRef = machine;
        _ = machine.RunAsync(app.Lifetime.ApplicationStopping);
        app.MapAppMetrics();
        MapNodeRoutes(app);
        MapBootstrapRoutes(app, machine);
        MapFullBackupRoutes(app, identity, endpoints);
        MapOpsRoutes(app);
        await app.RunAsync();
    }

    // Публичный CA верификации клиентских сертов воркера (env
    // PGW_AGENT_CLIENT_CA, t05): X509Certificate2 с кешем по содержимому PEM
    // (temp-файл по образцу PatroniRoleProbe.GetCaAsync). null — env пуст
    // (листенер поднимется, но handshake отклонится: клиента не верифицировать).
    private static System.Security.Cryptography.X509Certificates.X509Certificate2? _nodeClientCa;
    private static string? _nodeClientCaPem;

    private static System.Security.Cryptography.X509Certificates.X509Certificate2? NodeClientCa()
    {
        var caPem = Environment.GetEnvironmentVariable("PGW_AGENT_CLIENT_CA");
        if (string.IsNullOrWhiteSpace(caPem))
            return null;
        if (_nodeClientCa is not null && _nodeClientCaPem == caPem)
            return _nodeClientCa;

        // Temp-файл по образцу PatroniRoleProbe.GetCaAsync: CreateFromPemFile
        // требует пару cert+key — CA-PEM пишет в temp и грузит LoadCertificate.
        var caPath = Path.Combine(Path.GetTempPath(), $"pgw-agent-ca-{Guid.NewGuid():N}.pem");
        try
        {
            File.WriteAllText(caPath, caPem + "\n");
            _nodeClientCa = System.Security.Cryptography.X509Certificates.X509CertificateLoader
                .LoadCertificateFromFile(caPath);
            _nodeClientCaPem = caPem;
            return _nodeClientCa;
        }
        catch (Exception e) when (
            e is ArgumentException
                or System.Security.Cryptography.CryptographicException
                or System.IO.IOException)
        {
            return null; // PEM битый/ФС недоступна — клиента не верифицировать
        }
        finally
        {
            try { File.Delete(caPath); } catch { /* temp — не критично */ }
        }
    }

    // Команды канала доставки (spec §3.4): статус воркер читает из etcd —
    // здесь только команды. POST /api/bootstrap — хендшейк старта (валидация
    // аргументов 400 / атомарный txn-переход / ок-пинок 200); POST
    // /api/bootstrap/apply — применение на ready-ноде (ротации/замены).
    // Секреты в логи/ошибки не попадают.
    private static void MapBootstrapRoutes(WebApplication app, PgNodeMachine machine)
    {
        // Граница порта канала (spec §8-23): канальные роуты живут ТОЛЬКО на
        // mTLS-листенере :8081; тот же путь на :8080 (открытая грань
        // метрик/ops) — 404. LocalPort == 0 (in-memory TestServer) — пропуск
        // (контракт-тесты канала; прод-сокет всегда даёт реальный порт).
        static IResult PortGate(HttpContext ctx) =>
            ctx.Connection.LocalPort is 0 or 8081
                ? null!
                : Results.NotFound();

        app.MapPost("/api/bootstrap", async (HttpContext ctx, WorkerPushRequest body, CancellationToken ct) =>
        {
            if (PortGate(ctx) is { } gate) return gate;
            Console.Out.WriteLine("pgnode: POST /api/bootstrap received");
            var result = await machine.HandshakeAsync(
                body.SuPassword ?? "", body.StandbyPassword ?? "", body.IsToolStart ?? false, ct);
            return result switch
            {
                HandshakeResult.Invalid => Results.Json(
                    new ErrorResponse("invalid arguments or bootstrap key"), JsonOpts, statusCode: 400),
                HandshakeResult.EtcdError => Results.Json(
                    new ErrorResponse("etcd is unavailable"), JsonOpts, statusCode: 503),
                _ => Results.Json(new OkResponse(), JsonOpts), // Started | Nop («пинок состоялся»)
            };
        });

        app.MapPost("/api/bootstrap/apply", async (HttpContext ctx, CancellationToken ct) =>
        {
            if (PortGate(ctx) is { } gate) return gate;
            var result = await machine.ApplyAsync(ct);
            if (result.EtcdError)
                return Results.Json(new ErrorResponse("etcd is unavailable"), JsonOpts, statusCode: 503);
            if (!result.Ok)
                return Results.Json(new ErrorResponse("node is not ready"), JsonOpts, statusCode: 409);
            return Results.Json(new AppliedResponse(result.AppliedRevision), JsonOpts);
        });
    }

    private sealed record OkResponse([property: JsonPropertyName("ok")] bool Ok = true);
    private sealed record AppliedResponse([property: JsonPropertyName("applied_revision")] long? AppliedRevision);
    private sealed record WorkerPushRequest(
        [property: JsonPropertyName("su_password")] string? SuPassword,
        [property: JsonPropertyName("standby_password")] string? StandbyPassword,
        [property: JsonPropertyName("is_tool_start")] bool? IsToolStart);

    // Node-операции приёмника (arch/25 №22): обеспечение pg_hba (arch/19 §7),
    // pg_dump схемы (arch/14 §Move M1), проба данных ноды (arch/14 Д3) —
    // синхронные короткие операции: ответ 200 — итог, 500 — отказ (transient
    // для тиков воркера). Исполняются локально в контейнере ноды.
    private static void MapNodeRoutes(WebApplication app)
    {
        app.MapPost("/api/node/hba-ensure", async (
            IProcessRunner runner, CancellationToken ct) =>
        {
            try
            {
                var response = await new HbaEnsureOperation(runner).RunAsync(ct);
                return Results.Json(response, JsonOpts);
            }
            catch (Exception e)
            {
                return Results.Json(
                    new ErrorResponse(e.Message.Split('\n')[0]), JsonOpts, statusCode: 500);
            }
        });

        app.MapPost("/api/node/pgdump", async (
            StartPgDumpRequest body, IProcessRunner runner, CancellationToken ct) =>
        {
            try
            {
                var response = await new PgDumpOperation(runner)
                    .RunAsync(body.Schema, body.DbName, ct);
                return Results.Json(response, JsonOpts);
            }
            catch (Exception e)
            {
                return Results.Json(
                    new ErrorResponse(e.Message.Split('\n')[0]), JsonOpts, statusCode: 500);
            }
        });

        app.MapGet("/api/node/pgdata", () =>
        {
            // Проба данных (arch/14 Д3): факт PG_VERSION файловой системой
            // контейнера (user=postgres — доступ к PGDATA ноды).
            var present = File.Exists(Path.Combine(NodePaths.Pgdata, "PG_VERSION"));
            return Results.Json(
                new PgDataResponse(present ? "present" : "absent"), JsonOpts);
        });
    }

    // Маршруты ops-операций (arch/19 §5/§3.5/§3.6): POST /api/ops/verify —
    // 409 (вторая активная ops-операция) / 202; GET — 200 снимок (в т.ч.
    // started_unix — возрастной бюджет verify воркера §6) / 404; POST
    // /api/ops/restore — 409 / 202 (drill использует тот же маршрут —
    // механика одна, id в теле — признак drill, §3.6); POST /api/ops/cancel
    // — общий для ops-операций: 404 (нет активной) / 202 best-effort (итог —
    // снимок state=cancelled: НЕ failed — отмена не суждение о данных;
    // воркер-супервиз отличает её от vanished). Одна активная ops-операция
    // на процесс — общий OpsOperationRegistry (TryBegin — гвард); полный
    // живёт в СВОЁМ реестре и ops-операцию не блокирует.
    private static void MapOpsRoutes(WebApplication app)
    {
        // Активная операция для cancel (снимок — в реестре, экземпляр — здесь).
        IReceiverOperation? active = null;

        app.MapPost("/api/ops/verify", (StartVerifyRequest body,
            OpsOperationRegistry registry, IOpsOperationFactory factory) =>
        {
            if (!registry.TryBegin(new OperationSnapshot(
                    body.BackupId, OperationStates.Running, OperationPhases.Downloading,
                    StartedUnix: DateTimeOffset.UtcNow.ToUnixTimeSeconds())))
                return Results.Json(
                    new ErrorResponse("operation already active"), JsonOpts, statusCode: 409);

            var operation = factory.CreateVerify(body);
            active = operation;
            _ = Task.Run(() => operation.RunAsync(CancellationToken.None));
            return Results.Json(
                new OpsStatusResponse(OperationStates.Running, OperationPhases.Downloading),
                JsonOpts, statusCode: 202);
        });

        app.MapGet("/api/ops/verify", (OpsOperationRegistry registry) => OpsStatus(registry));

        // POST /api/ops/restore — тело StartRestoreRequest (backup_id /
        // id? — drill-признак / target_time RFC3339, null = latest /
        // recovery_timeout_sec / S3-контекст): 409 / 202 (drill использует
        // те же маршруты — механика одна, arch/19 §3.6). GET —
        // OpsStatusResponse (downloading|recovering + restored_to_lsn/
        // system_id/started_unix) / 404.
        app.MapPost("/api/ops/restore", (StartRestoreRequest body,
            OpsOperationRegistry registry, IOpsOperationFactory factory) =>
        {
            if (!registry.TryBegin(new OperationSnapshot(
                    body.Id ?? body.BackupId, OperationStates.Running, OperationPhases.Downloading,
                    StartedUnix: DateTimeOffset.UtcNow.ToUnixTimeSeconds())))
                return Results.Json(
                    new ErrorResponse("operation already active"), JsonOpts, statusCode: 409);

            var operation = factory.CreateRestore(body);
            active = operation;
            _ = Task.Run(() => operation.RunAsync(CancellationToken.None));
            return Results.Json(
                new OpsStatusResponse(OperationStates.Running, OperationPhases.Downloading),
                JsonOpts, statusCode: 202);
        });

        app.MapGet("/api/ops/restore", (OpsOperationRegistry registry) => OpsStatus(registry));

        app.MapPost("/api/ops/cancel", async (OpsOperationRegistry registry) =>
        {
            if (registry.Current is null || active is null)
                return Results.Json(
                    new ErrorResponse("no active operation"), JsonOpts, statusCode: 404);
            await active.CancelAsync();
            return Results.Json(new { state = "cancelling" }, JsonOpts, statusCode: 202);
        });
    }

    // Общий статус ops-операции: 200 снимок (в т.ч. терминальный — снимок
    // персистентен, воркер-супервиз отличает cancelled от vanished-404) /
    // 404 (операций в реестре нет).
    private static IResult OpsStatus(OpsOperationRegistry registry)
        => registry.Current is { } snapshot
            ? Results.Json(new OpsStatusResponse(
                    snapshot.State, snapshot.Phase,
                    snapshot.RestoredToLsn, snapshot.SystemId,
                    snapshot.StartedUnix, snapshot.Error),
                JsonOpts)
            : Results.Json(
                new ErrorResponse("no active operation"), JsonOpts, statusCode: 404);

    // Маршруты операции полного (arch/19 §2, spec §3.2): POST — 503 гейта
    // ОПЕРАЦИИ (STOPPED/disabled/Unknown — операция не начинается; Unknown —
    // транспортный отказ etcd, fail-closed); WAL_CHAIN_ERROR — НЕ гейт
    // операции: 202, операция начинается (пересъём — единственное лечение
    // разрыва цепочки, гейтом не блокируется; стрим при этом стоит по гейту
    // СТРИМА — IsStopGatedAsync, без изменений), 409 (вторая активная), 202
    // (принята, фоновая Task); GET — 200 снимок / 404 (нет активной —
    // vanished-семантика); cancel — 202 best-effort / 404.
    // Гейты читаются только при СТАРТЕ: идущая операция стоп-гейтами не рвётся
    // (отменяют только cancel и бюджет воркера).
    private static void MapFullBackupRoutes(
        WebApplication app, PgNodeIdentity identity, string[] endpoints)
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
            if (gates.Disabled || (gates.Paused && !gates.ChainError) || gates.Unknown)
                return Results.Json(
                    new ErrorResponse("backups disabled, stopped or gates unknown"),
                    JsonOpts, statusCode: 503);
            if (!registry.TryBegin(new OperationSnapshot(
                    body.Id, OperationStates.Running, OperationPhases.Basebackup)))
                // 409-ответ несёт снимок активной операции (id в т.ч.): воркер
                // сверяет id в 409-ветке — своя операция (пере-старт прошлого
                // тика) или чужая (phantom-COMPLETED гвард, arch/19 §2).
                return Results.Json(
                    ActiveFullStatus(registry, "operation already active"),
                    JsonOpts, statusCode: 409);

            var operation = factory.Create(body.Id);
            active = operation;
            // Фоновая Task вне запроса (ответ 202 — операция живёт дальше);
            // RunAsync сам гасит все исключения в failed-снимок.
            _ = Task.Run(() => operation.RunAsync(CancellationToken.None));
            return Results.Json(
                new FullStatusResponse(OperationStates.Running, OperationPhases.Basebackup, Id: body.Id),
                JsonOpts, statusCode: 202);
        });

        app.MapGet("/api/backup/full", (OperationRegistry registry) =>
            registry.Current is { } snapshot
                ? Results.Json(new FullStatusResponse(
                        snapshot.State, snapshot.Phase,
                        Id: snapshot.Id,
                        WalStartSegment: snapshot.WalStartSegment, SizeBytes: snapshot.SizeBytes,
                        Error: snapshot.Error,
                        BytesDone: snapshot.BytesDone, PartsDone: snapshot.PartsDone,
                        Sha256: snapshot.Sha256),
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

    // Снимок активной операции полным контрактом (с id и причиной — для 409):
    // активной нет (гонка TryBegin/Clear) — running/basebackup-заглушка.
    private static FullStatusResponse ActiveFullStatus(OperationRegistry registry, string error)
    {
        var snapshot = registry.Current;
        return new FullStatusResponse(
            snapshot?.State ?? OperationStates.Running,
            snapshot?.Phase ?? OperationPhases.Basebackup,
            Id: snapshot?.Id,
            WalStartSegment: snapshot?.WalStartSegment,
            SizeBytes: snapshot?.SizeBytes,
            Error: error,
            BytesDone: snapshot?.BytesDone, PartsDone: snapshot?.PartsDone,
            Sha256: snapshot?.Sha256);
    }
}
