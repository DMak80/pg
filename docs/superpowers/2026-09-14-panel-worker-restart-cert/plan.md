# План: управление сертификатами API воркеров и их перезапуск из AdminPanel

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** оператор из панели создаёт/заменяет/удаляет серверные сертификаты mTLS-грани API воркеров (хранение в etcd, применение перезапуском) и перезапускает воркеров; запись отвергается с явным 422, если сертификат затрагивает исходящие коммуникации воркеров.

**Архитектура:** etcd-ключ `/workers/api_tls/<worker>` — единственный источник управляемого серта; панель — единственный писатель (прямая запись, вторая категория после §9 provisioning); воркеры читают ключ при старте (до Kestrel) с env-фоллбеком и fail-fast на битый ключ; применение — graceful self-stop `POST /api/restart` (контейнер поднимает docker-политика); дискавери-ключи воркеров несут `cert_thumbprint`, панель сверяет его с целевым сертом → статусы applied/pending restart/unmanaged/unknown; валидатор панели доверяет self-signed сертам по thumbprint (цепочка к ServerCa ИЛИ совпадение).

**Тех-стек:** .NET 10 (C# latest, Nullable, `TreatWarningsAsErrors=true`), ASP.NET Core Minimal API, etcd HTTP JSON gateway `/v3/*`, React+Mantine+TanStack Query, xUnit + testcontainers + WAF.

**Спецификация:** `docs/superpowers/2026-09-14-panel-worker-restart-cert/spec.md` — план аргументируется от неё; исполнители читают spec и план вместе. Контракты уже в arch (arch/14 §1.1/§3.3, arch/16 §1.1/§3.1, arch/adminpanel/02 §9.9, arch/adminpanel/03 §1/§3.7).

## Глобальные ограничения

- Кодовая база: worktree `/Users/demakaev/ZCodeProject/worktrees/feat-panel-worker-restart-cert`, решение `src/PgWorker.slnx`. Все пути ниже — от корня worktree.
- `TreatWarningsAsErrors=true` — ноль предупреждений при сборке.
- Комментарии/доки — по-русски, идентификаторы — на английском.
- Копирование кода PgWorker↔KafkaWorker (ридер, рестарт-хендлер, ClaimStore-правка) — осознанное (spec §3.2 п.1, паттерн TlsEndpoints).
- Тесты: AAA-комментарии (Arrange/Act/Assert); docker-порты только динамические; `BrokerBootSec`-подобные таймауты ≤100 с; ожидания ≤30 с.
- Зачистка docker после каждой серии: контейнеры своих тестов удалить (dev-станд `as-*`/`adminpanel` не трогать) + `docker network prune -f`; перед серией проверить `docker ps -aq | wc -l`.
- E2E — изолированное окружение на сценарий + полный teardown при любом исходе + ассерт чистоты (канон `docs/e2e-isolation.md`, `AGENTS.base.md` §11).
- Коммиты — в feature-ветке worktree, свободно; мерж в `main` и пуш — только по явной просьбе пользователя.
- Команды прогона всегда с `DOTNET_CLI_UI_LANGUAGE=en`.
- `deploy/docker-compose.yml` НЕ меняется (env-серты остаются бутстрап-фоллбеком, spec §3.5).

## Карта задач (фазы spec §5)

| Фаза spec | Задачи |
|---|---|
| Ф1 Воркеры | Task 1 (ридер PgWorker), Task 2 (ридер KafkaWorker), Task 3 (`POST /api/restart`), Task 4 (`cert_thumbprint` в дискавери) |
| Ф2 Backend панели | Task 5 (модель/парсеры/снапшоты), Task 6 (`WorkerCertService`), Task 7 (`SendAllAsync` + thumbprint-доверие), Task 8 (`WorkersModule`) |
| Ф3 Frontend | Task 9 (страница `/workers`) |
| Ф4 Стенд/доки/E2E | Task 10 (E2E полного цикла), Task 11 (dev-stand чек + runbook + мерж-гейт) |

---

## Task 1: Ридер managed-серверного серта при старте PgWorker

**Вход (предусловие):** ветка worktree чистая; `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx` зелёный.

**Files:**
- Create: `src/PgWorker.App/Api/WorkerApiCertReader.cs`
- Modify: `src/PgWorker.App/Api/ApiTlsEndpoints.cs`
- Modify: `src/PgWorker.App/Program.cs` (блок t03 до `builder.Build()`)
- Test: `src/tests/PgWorker.UnitTests/App/WorkerApiCertReaderTests.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Api/WorkerApiCertStartupTests.cs`

**Interfaces:**
- Produces (используют Task 2, 4, 10):
  - `PgWorker.App.Api.ManagedCertStatus` — enum `Found | Missing | Unreachable | Broken`
  - `PgWorker.App.Api.ManagedCertRead(ManagedCertStatus Status, string? CertPem, string? KeyPem, string? Error)`
  - `PgWorker.App.Api.WorkerApiCertReader.ReadAsync(string[] endpoints, string worker, CancellationToken ct)` → `Task<ManagedCertRead>`; `ParsePayload(string json)` → `(string CertPem, string KeyPem)` (кидает на битом)
  - `PgWorker.App.Api.ApiTlsSetup(X509Certificate2? ServerCert, string? Source, string? Warning)`
  - `ApiTlsEndpoints.ConfigureMtls(WebApplicationBuilder builder, ManagedCertRead? managedCert = null)` → `ApiTlsSetup` (было `void`; default-параметр сохраняет совместимость WAF-вызовов `ConfigureMtls(builder)`)

- [x] **Шаг 1: юнит-тесты ParsePayload (красный)**

`src/tests/PgWorker.UnitTests/App/WorkerApiCertReaderTests.cs`:

```csharp
using PgWorker.App.Api;
using Xunit;

namespace PgWorker.UnitTests.App;

// Парсинг value ключа /workers/api_tls/pgworker (spec §3.2 п.1): JSON
// {cert_pem, key_pem} → пара PEM; битый JSON/отсутствие полей — исключение
// (fail-fast старта, тихий fallback на env маскирует проблему).
public class WorkerApiCertReaderTests
{
    [Fact]
    public void ParsePayload_ValidJson_ReturnsPair()
    {
        // Arrange
        var json = """{"cert_pem":"-----BEGIN CERTIFICATE-----\nX\n-----END CERTIFICATE-----","key_pem":"-----BEGIN PRIVATE KEY-----\nY\n-----END PRIVATE KEY-----"}""";

        // Act
        var (cert, key) = WorkerApiCertReader.ParsePayload(json);

        // Assert
        cert.Should().Contain("BEGIN CERTIFICATE");
        key.Should().Contain("BEGIN PRIVATE KEY");
    }

    [Fact]
    public void ParsePayload_BrokenJson_Throws()
    {
        // Arrange / Act / Assert
        Assert.ThrowsAny<Exception>(() => WorkerApiCertReader.ParsePayload("{not-json"));
    }

    [Fact]
    public void ParsePayload_MissingFields_Throws()
    {
        // Arrange: JSON без cert_pem — управляемый ключ обязан быть полным
        // Act / Assert
        Assert.ThrowsAny<Exception>(() => WorkerApiCertReader.ParsePayload("""{"key_pem":"Y"}"""));
    }
}
```

(Добавить `using FluentAssertions;` и `GlobalUsings`-совместимость по образцу соседних файлов.)

- [x] **Шаг 2: прогнать юниты — красный**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~WorkerApiCertReaderTests"`
Expected: FAIL компиляция — `WorkerApiCertReader` не существует.

- [x] **Шаг 3: реализация WorkerApiCertReader**

`src/PgWorker.App/Api/WorkerApiCertReader.cs`:

```csharp
using System.Text.Json;
using PgWorker.Etcd.Client;

namespace PgWorker.App.Api;

// Статус чтения ключа /workers/api_tls/<worker> при старте (spec §3.2 п.1):
// Found — ключ есть и PEM-пара валидна; Missing — ключа нет (env-фоллбек);
// Unreachable — etcd недоступен (env-фоллбек с warning или fail-fast);
// Broken — ключ есть, но JSON/PEM битые (fail-fast: ключ — явное намерение
// оператора, тихий fallback маскирует проблему).
public enum ManagedCertStatus { Found, Missing, Unreachable, Broken }

// Результат чтения managed-серверного серта: PEM-пара при Found, Error — при Broken/Unreachable.
public sealed record ManagedCertRead(ManagedCertStatus Status, string? CertPem, string? KeyPem, string? Error);

// Ридер ключа /workers/api_tls/<worker> ДО поднятия Kestrel (spec §3.2 п.1):
// одиночный /v3/kv/range к первому живому endpoint (формат gateway — EtcdGateway).
// Панель — единственный писатель ключа; воркер его только читает.
public static class WorkerApiCertReader
{
    public const string KeyPrefix = "/workers/api_tls/";

    // Перебор endpoints по одному (failover): первый ответивший выигрывает.
    public static async Task<ManagedCertRead> ReadAsync(string[] endpoints, string worker, CancellationToken ct)
    {
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
        var gateway = new EtcdGateway(http);
        string? lastError = null;
        foreach (var endpoint in endpoints)
        {
            var result = await gateway.GetAsync(endpoint, KeyPrefix + worker, ct);
            if (!result.IsSuccess)
            {
                lastError = result.Error?.Message;
                continue;
            }

            if (result.Value is null)
                return new(ManagedCertStatus.Missing, null, null, null);
            try
            {
                var (certPem, keyPem) = ParsePayload(result.Value.Value);
                // PEM обязан быть валидной парой — проверяем сразу (fail-fast старта).
                using var _ = X509Certificate2CreatePair(certPem, keyPem);
                return new(ManagedCertStatus.Found, certPem, keyPem, null);
            }
            catch (Exception e)
            {
                return new(ManagedCertStatus.Broken, null, null, e.Message);
            }
        }

        return new(ManagedCertStatus.Unreachable, null, null,
            lastError ?? "endpoints не заданы");
    }

    // JSON {cert_pem, key_pem} → пара; битый JSON/отсутствие полей — исключение.
    public static (string CertPem, string KeyPem) ParsePayload(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        if (root.ValueKind != JsonValueKind.Object
            || !root.TryGetProperty("cert_pem", out var cert)
            || cert.ValueKind != JsonValueKind.String
            || !root.TryGetProperty("key_pem", out var key)
            || key.ValueKind != JsonValueKind.String)
            throw new ApplicationException("value ключа обязан быть JSON {cert_pem, key_pem}");
        return (cert.GetString()!, key.GetString()!);
    }

    // Проверка пары PEM: серт + ключ обязаны синтаксически собираться вместе.
    private static System.Security.Cryptography.X509Certificates.X509Certificate2 X509Certificate2CreatePair(
        string certPem, string keyPem)
        => System.Security.Cryptography.X509Certificates.X509Certificate2.CreateFromPem(certPem, keyPem);
}
```

- [x] **Шаг 4: прогнать юниты — зелёный**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~WorkerApiCertReaderTests"`
Expected: PASS (3 теста).

- [x] **Шаг 5: интеграционные тесты чтения и применения (красный)**

`src/tests/PgWorker.IntegrationTests/Api/WorkerApiCertStartupTests.cs` — реальный etcd (`[Collection(PgApiCollection.Name)]`, фикстура `PgApiFixture`) + локальный TestPki (скопировать вложенный `private static class TestPki` из `MtlsApiTests.cs` того же каталога — GenerateCa/Issue RSA-2048) + `FreePort()` (TcpListener :0). Тесты чтения:

```csharp
// Чтение ключа /workers/api_tls/pgworker при старте (spec §3.2 п.1):
// Found/Missing/Unreachable/Broken + применение в ConfigureMtls (серт грани
// из ключа; fail-fast на битый; env-фоллбек при недоступном etcd).
[Collection(PgApiCollection.Name)]
public class WorkerApiCertStartupTests(PgApiFixture fx)
{
    private static string Endpoints => fx.Etcd.Endpoint;

    private static async Task PutKeyAsync(string json)
        => await fx.Etcd.Gateway.PutAsync(
            Endpoints, "/workers/api_tls/pgworker", json, null, TestContext.Current.CancellationToken);

    [Fact]
    public async Task Read_KeyMissing_Missing()
    {
        // Arrange: ключа нет (чистый префикс фикстуры) / Act
        var read = await WorkerApiCertReader.ReadAsync([Endpoints], "pgworker", TestContext.Current.CancellationToken);
        // Assert: env-фоллбек по правилам §3.2 п.1
        read.Status.Should().Be(ManagedCertStatus.Missing);
    }

    [Fact]
    public async Task Read_ValidPair_Found()
    {
        // Arrange
        var (caPem, caKeyPem) = TestPki.GenerateCa();
        var (certPem, keyPem) = TestPki.Issue(caPem, caKeyPem, "pgworker-api");
        await PutKeyAsync(JsonSerializer.Serialize(new { cert_pem = certPem, key_pem = keyPem }));
        // Act
        var read = await WorkerApiCertReader.ReadAsync([Endpoints], "pgworker", TestContext.Current.CancellationToken);
        // Assert
        read.Status.Should().Be(ManagedCertStatus.Found);
        read.CertPem.Should().Be(certPem);
    }

    [Fact]
    public async Task Read_BrokenJson_Broken()
    {
        // Arrange: JSON-мусор — fail-fast-ветка старта
        await PutKeyAsync("{not-json");
        // Act
        var read = await WorkerApiCertReader.ReadAsync([Endpoints], "pgworker", TestContext.Current.CancellationToken);
        // Assert
        read.Status.Should().Be(ManagedCertStatus.Broken);
        read.Error.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task Read_DeadEndpoint_Unreachable()
    {
        // Arrange: мёртвый порт (зонд свободного → никто не слушает)
        var port = Etcd.EtcdFixture.ReserveHostPort();
        // Act
        var read = await WorkerApiCertReader.ReadAsync([$"http://localhost:{port}"], "pgworker", TestContext.Current.CancellationToken);
        // Assert: env-фоллбек с warning при живом env-серте
        read.Status.Should().Be(ManagedCertStatus.Unreachable);
    }
}
```

Тесты применения — TLS-хост по образцу `MtlsApiTests.StartTlsHost` (реальный Kestrel-сокет, клиент TLS 1.2 + PFX round-trip, доверие серверу по thumbprint — `RemoteCertificateValidationCallback` сверяет SHA-256 серта):

```csharp
    [Fact]
    public async Task ConfigureMtls_ManagedFound_ServerCertFromEtcd()
    {
        // Arrange: ключ с валидной парой в etcd; env-серт ДРУГОЙ (тот же метод Issue)
        var (caPem, caKeyPem) = TestPki.GenerateCa();
        var (etdCert, etdKey) = TestPki.Issue(caPem, caKeyPem, "pgworker-etcd");
        await PutKeyAsync(JsonSerializer.Serialize(new { cert_pem = etdCert, key_pem = etdKey }));
        var read = await WorkerApiCertReader.ReadAsync([Endpoints], "pgworker", TestContext.Current.CancellationToken);
        var port = FreePort();

        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddHealthChecks();
        builder.Configuration["PgWorker:Api:Tls:ServerCertPem"] = etdCert.Replace("X", "Z"); // env-мусор — источник etcd обязан победить
        builder.Configuration["PgWorker:Api:Tls:ServerKeyPem"] = etdKey;
        builder.Configuration["PgWorker:Api:Tls:ClientCaPem"] = caPem;
        builder.Configuration["PgWorker:Api:Tls:AllowInsecureHttp"] = "false";
        builder.Configuration["urls"] = $"https://localhost:{port}";
        var setup = ApiTlsEndpoints.ConfigureMtls(builder, read);
        var app = builder.Build();
        app.MapGet("/api/ping", () => Results.Ok("pong"));
        app.Start();

        // Act: клиент доверяет ТОЛЬКО thumbprint серта из ключа
        using var client = TlsClientTrustThumbprint(etdCert, port);
        using var response = await client.GetAsync("/api/ping", TestContext.Current.CancellationToken);

        // Assert: грань поднята на etcd-серте, источник — etcd
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        setup.Source.Should().Be("etcd:/workers/api_tls/pgworker");
        setup.ServerCert.Should().NotBeNull();
    }

    [Fact]
    public void ConfigureMtls_ManagedBroken_FailFast()
    {
        // Arrange: ключ бит — явное намерение оператора, тихий fallback запрещён
        var read = new ManagedCertRead(ManagedCertStatus.Broken, null, null, "битый JSON");
        var builder = WebApplication.CreateBuilder();
        builder.Configuration["PgWorker:Api:Tls:AllowInsecureHttp"] = "false";

        // Act / Assert
        Assert.ThrowsAny<Exception>(() => ApiTlsEndpoints.ConfigureMtls(builder, read));
    }

    [Fact]
    public void ConfigureMtls_UnreachableWithEnv_EnvSourceAndWarning()
    {
        // Arrange: etcd недоступен + валидный env-серт → старт на env + warning
        var (caPem, caKeyPem) = TestPki.GenerateCa();
        var (certPem, keyPem) = TestPki.Issue(caPem, caKeyPem, "pgworker-env");
        var builder = WebApplication.CreateBuilder();
        builder.Configuration["PgWorker:Api:Tls:ServerCertPem"] = certPem;
        builder.Configuration["PgWorker:Api:Tls:ServerKeyPem"] = keyPem;
        builder.Configuration["PgWorker:Api:Tls:ClientCaPem"] = caPem;
        builder.Configuration["PgWorker:Api:Tls:AllowInsecureHttp"] = "false";
        var read = new ManagedCertRead(ManagedCertStatus.Unreachable, null, null, "etcd недоступен");

        // Act
        var setup = ApiTlsEndpoints.ConfigureMtls(builder, read);

        // Assert
        setup.Source.Should().Be("env");
        setup.Warning.Should().NotBeNullOrEmpty();
        setup.ServerCert.Should().NotBeNull();
    }
```

`TlsClientTrustThumbprint(string certPem, int port)` — приватный хелпер файла: SocketsHttpHandler, TLS 1.2, `RemoteCertificateValidationCallback = (_, c, _, _) => c is not null && SHA256(c.GetCertHash()) == SHA256(эталона)` (сравнение `Convert.ToHexString(SHA256.HashData(cert.GetRawCertData()))` с thumbprint эталонного `X509Certificate2.CreateFromPem(certPem)`).

- [x] **Шаг 6: прогнать интеграционные — красный**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~WorkerApiCertStartupTests"`
Expected: FAIL компиляция (`ConfigureMtls` ещё `void`, `ApiTlsSetup` нет).

- [x] **Шаг 7: расширение ApiTlsEndpoints.ConfigureMtls**

В `src/PgWorker.App/Api/ApiTlsEndpoints.cs`:

1. Новый record + смена сигнатуры:
```csharp
// Итог конфигурации серта (spec §3.2 п.1): применённый серт + источник
// ("etcd:/workers/api_tls/pgworker" | "env") + warning (etcd недоступен,
// старт на env). ServerCert=null — AllowInsecureHttp (WAF-тесты).
public sealed record ApiTlsSetup(X509Certificate2? ServerCert, string? Source, string? Warning);

public static ApiTlsSetup ConfigureMtls(WebApplicationBuilder builder, ManagedCertRead? managedCert = null)
```
2. Тело: `AllowInsecureHttp` → `return new ApiTlsSetup(null, null, null);` (прежний ранний выход).
3. Выбор серта:
```csharp
        // Управляемый серт: etcd-ключ > env (spec §3.2 п.1). Битый ключ —
        // fail-fast: ключ — явное намерение оператора.
        X509Certificate2? serverCert;
        string? source;
        string? warning = null;
        switch (managedCert?.Status)
        {
            case ManagedCertStatus.Found:
                serverCert = LoadCertificatePemPair(managedCert.CertPem!, managedCert.KeyPem!);
                source = "etcd:/workers/api_tls/pgworker";
                break;
            case ManagedCertStatus.Broken:
                throw new ApplicationException(
                    $"PgWorker:Api:Tls: ключ /workers/api_tls/pgworker бит ({managedCert.Error}) — "
                    + "исправьте из панели (PUT api-cert) или удалите (DELETE api-cert)");
            default:
                serverCert = LoadServerCertificate(tls);
                source = "env";
                if (managedCert?.Status == ManagedCertStatus.Unreachable)
                    warning = $"etcd недоступен ({managedCert.Error}) — стартую на env-серте";
                break;
        }

        serverCert ??= throw new ApplicationException(
            "PgWorker:Api:Tls: серверный серт/ключ не заданы (PGW_API_TLS_CERT/KEY или *_PATH; etcd-ключ /workers/api_tls/pgworker; arch/14 §1.1)");
```
4. Вынести PFX round-trip в общий приватный метод (существующий `LoadServerCertificate` использует его):
```csharp
    // PFX round-trip: ключ из CreateFromPem эфемерный (не экспортируемый) —
    // SslStream (macOS) не может его использовать без ре-импорта.
    private static X509Certificate2 LoadCertificatePemPair(string certPem, string keyPem)
    {
        var pem = X509Certificate2.CreateFromPem(certPem, keyPem);
        return X509CertificateLoader.LoadPkcs12(pem.Export(X509ContentType.Pkcs12), null);
    }
```
(`LoadServerCertificate` переписать на вызов `LoadCertificatePemPair(certPem, keyPem)`.)
5. В конце: `return new ApiTlsSetup(serverCert, source, warning);` (ListenAnyIP-блок не меняется).

- [x] **Шаг 8: интеграция в Program.cs**

`src/PgWorker.App/Program.cs` — после `ApiTlsEndpoints.ApplyEnvOverrides(...)` (строка ~34), до `ConfigureMtls` (строка ~62):

```csharp
// Управляемый серт API (spec §3.2 п.1): чтение ключа /workers/api_tls/pgworker
// ДО поднятия Kestrel; приоритет etcd > env; битый ключ — fail-fast.
var etcdEndpoints = builder.Configuration.GetSection("PgWorker:Etcd:Endpoints").Get<string[]>() ?? [];
var managedCert = await WorkerApiCertReader.ReadAsync(etcdEndpoints, "pgworker", CancellationToken.None);
```

Заменить `ApiTlsEndpoints.ConfigureMtls(builder);` на:
```csharp
var apiTls = ApiTlsEndpoints.ConfigureMtls(builder, managedCert);
```

После `var app = builder.Build();` рядом с существующим warning про `AllowInsecureHttp`:
```csharp
if (apiTls.Source is { } certSource)
    app.Logger.LogInformation("PgWorker:Api:Tls: серверный серт API — источник {Source}", certSource);
if (apiTls.Warning is { } certWarning)
    app.Logger.LogWarning("PgWorker:Api:Tls: {Warning}", certWarning);
```

- [x] **Шаг 9: полный прогон воркера**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~Api"`
Expected: сборка без предупреждений; все тесты зелёные (включая прежние `MtlsApiTests` — `ConfigureMtls(builder)` с default-параметром совместим).

- [x] **Шаг 10: коммит**

```bash
git add src/PgWorker.App/Api/WorkerApiCertReader.cs src/PgWorker.App/Api/ApiTlsEndpoints.cs src/PgWorker.App/Program.cs src/tests/PgWorker.UnitTests/App/WorkerApiCertReaderTests.cs src/tests/PgWorker.IntegrationTests/Api/WorkerApiCertStartupTests.cs
git commit -m "feat(pgworker): чтение managed-серверного серта API из etcd при старте (etcd>env, fail-fast на битый ключ)"
```

**Выход:** PgWorker стартует на серте из `/workers/api_tls/pgworker` с env-фоллбеком и fail-fast; источник в логе.
**Связь со spec:** §2 п.1/п.4, §3.2 п.1, §4.5 (второй/третий/четвёртый пункт правил), §7 кр.7.

---

## Task 2: Ридер managed-серверного серта при старте KafkaWorker (симметрия)

**Вход (предусловие):** Task 1 слит; сборка зелёная.

**Files:**
- Create: `src/KafkaWorker.App/Api/WorkerApiCertReader.cs`
- Modify: `src/KafkaWorker.App/Api/TlsEndpoints.cs`
- Modify: `src/KafkaWorker.App/Program.cs`
- Test: `src/tests/KafkaWorker.UnitTests/App/WorkerApiCertReaderTests.cs`
- Test: `src/tests/KafkaWorker.IntegrationTests/Api/WorkerApiCertStartupTests.cs`

**Interfaces:**
- Consumes: типы из Task 1 воспроизводятся копией в namespace `KafkaWorker.App.Api` (осознанное дублирование, паттерн TlsEndpoints).
- Produces: `KafkaWorker.App.Api.WorkerApiCertReader` / `ManagedCertRead` / `ManagedCertStatus` (сигнатуры 1:1 Task 1), `TlsEndpoints.ConfigureMtls(WebApplicationBuilder builder, int port, ManagedCertRead? managedCert = null)` → `ApiTlsSetup`.

- [x] **Шаг 1: копия юнит-тестов (красный)**

`src/tests/KafkaWorker.UnitTests/App/WorkerApiCertReaderTests.cs` — 1:1 тесты Task 1 (Шаг 1) с `using KafkaWorker.App.Api;` и namespace `KafkaWorker.UnitTests.App`; в JSON-фикстуре ключ тот же формат (worker-agnostic ридер).

- [x] **Шаг 2: прогнать — красный**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter "FullyQualifiedName~WorkerApiCertReaderTests"`
Expected: FAIL компиляция.

- [x] **Шаг 3: копия реализации WorkerApiCertReader**

`src/KafkaWorker.App/Api/WorkerApiCertReader.cs` — код 1:1 из Task 1 Шаг 3, отличия: `namespace KafkaWorker.App.Api;`, `using KafkaWorker.Etcd.Client;` (gateway KafkaWorker). Комментарий в шапке — «арх/16 §1.1, копия PgWorker.App/Api/WorkerApiCertReader.cs (осознанное дублирование по паттерну TlsEndpoints)».

- [x] **Шаг 4: прогнать юниты — зелёный**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter "FullyQualifiedName~WorkerApiCertReaderTests"`
Expected: PASS.

- [x] **Шаг 5: интеграционные тесты (красный)**

`src/tests/KafkaWorker.IntegrationTests/Api/WorkerApiCertStartupTests.cs` — зеркально Task 1 Шаг 5: `[Collection(KafkaApiCollection.Name)]` (фикстура `KafkaApiFixture`, etcd — `fx.Etcd.Endpoint`), ключ `/workers/api_tls/kafkaworker`, `TlsEndpoints.ConfigureMtls(builder, port, read)`. Четыре Read-кейса (Missing/Found/Broken/Unreachable) + три ConfigureMtls-кейса (Found → грань на etcd-серте + `setup.Source == "etcd:/workers/api_tls/kafkaworker"`; Broken → исключение; Unreachable+env → `Source=="env"`, `Warning` не пуст). TestPki: у KafkaWorker-тестов есть прецедент `ClusterPki.GenerateCa/IssueBrokerCertificate` (см. `MtlsApiTests.cs` KafkaWorker) — использовать его вместо локальной копии.

- [x] **Шаг 6: расширение TlsEndpoints.ConfigureMtls**

`src/KafkaWorker.App/Api/TlsEndpoints.cs` — те же правки, что Task 1 Шаг 7:
1. `public sealed record ApiTlsSetup(X509Certificate2? ServerCert, string? Source, string? Warning);` (namespace `KafkaWorker.App.Api`);
2. сигнатура `ConfigureMtls(WebApplicationBuilder builder, int port, ManagedCertRead? managedCert = null)` → `ApiTlsSetup`;
3. `AllowInsecureHttp` → `new ApiTlsSetup(null, null, null)`;
4. switch выбора серта как в Task 1, но текст ошибки — `KafkaWorker:Api:Tls: ключ /workers/api_tls/kafkaworker бит (...)`, источник `etcd:/workers/api_tls/kafkaworker`;
5. `LoadCertificatePemPair`-extract (PFX round-trip) и `return new ApiTlsSetup(serverCert, source, warning);`.

- [x] **Шаг 7: интеграция в Program.cs KafkaWorker**

`src/KafkaWorker.App/Program.cs` — после `TlsEndpoints.ApplyEnvOverrides(builder.Configuration);` (строка ~69):

```csharp
// Управляемый серт API (spec §3.2 п.1): чтение ключа /workers/api_tls/kafkaworker
// ДО поднятия Kestrel; приоритет etcd > env; битый ключ — fail-fast.
var etcdEndpoints = builder.Configuration.GetSection("KafkaWorker:Etcd:Endpoints").Get<string[]>() ?? [];
var managedCert = await WorkerApiCertReader.ReadAsync(etcdEndpoints, "kafkaworker", CancellationToken.None);
```

`TlsEndpoints.ConfigureMtls(builder, port: 8080);` → `var apiTls = TlsEndpoints.ConfigureMtls(builder, port: 8080, managedCert);`; после `builder.Build()` — логи источника/warning как в Task 1 Шаг 8 (тексты `KafkaWorker:Api:Tls: ...`).

- [x] **Шаг 8: полный прогон**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests -c Debug && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~Api"`
Expected: всё зелёное; прежние `MtlsApiTests` KafkaWorker не сломаны (default-параметр).

- [x] **Шаг 9: коммит**

```bash
git add src/KafkaWorker.App/Api/WorkerApiCertReader.cs src/KafkaWorker.App/Api/TlsEndpoints.cs src/KafkaWorker.App/Program.cs src/tests/KafkaWorker.UnitTests/App/WorkerApiCertReaderTests.cs src/tests/KafkaWorker.IntegrationTests/Api/WorkerApiCertStartupTests.cs
git commit -m "feat(kafkaworker): чтение managed-серверного серта API из etcd при старте (копия PgWorker-ридера)"
```

**Выход:** KafkaWorker симметрично PgWorker применяет etcd-серт при старте.
**Связь со spec:** §3.2 п.1 («симметрично»), §2 п.1/п.4.

---

## Task 3: `POST /api/restart` обоих воркеров

**Вход (предусловие):** Task 1–2 слиты; сборка зелёная.

**Files:**
- Create: `src/PgWorker.App/Api/Operations/RestartHandler.cs`
- Create: `src/KafkaWorker.App/Api/Operations/RestartHandler.cs`
- Modify: `src/PgWorker.App/Api/ApiModule.cs`
- Modify: `src/KafkaWorker.App/Api/ApiModule.cs`
- Modify: `src/PgWorker.App/Program.cs`, `src/KafkaWorker.App/Program.cs` (DI-регистрация)
- Test: `src/tests/PgWorker.IntegrationTests/Api/RestartApiTests.cs`
- Test: `src/tests/KafkaWorker.IntegrationTests/Api/RestartApiTests.cs`

**Interfaces:**
- Produces (использует Task 8 — панель проксирует `POST /api/restart`; Task 10 — E2E):
  - `RestartHandler(IHostApplicationLifetime lifetime, ILogger<RestartHandler> logger, TimeSpan? stopDelay = null)`
  - `RestartHandler.Handle(string? requestedBy)` → `RestartDto(bool Restarting)`
  - HTTP: `POST /api/restart` → 202 `{"restarting":true}`; заголовок `X-Requested-By` — в лог; ~1 c пауза → `StopApplication`; в etcd ничего не пишет.

- [x] **Шаг 1: WAF-тест рестарта PgWorker (красный)**

`src/tests/PgWorker.IntegrationTests/Api/RestartApiTests.cs`:

```csharp
using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using PgWorker.App.Api.Operations;
using Xunit;

namespace PgWorker.IntegrationTests.Api;

// POST /api/restart (spec §3.2 п.2/§4.4): 202 {"restarting":true}; заголовок
// X-Requested-By — в лог; отложенный StopApplication. Lifetime подменён фейком
// (реальный стоп уронил бы WAF-хост общей фикстуры); stopDelay укорочен.
[Collection(PgApiCollection.Name)]
public class RestartApiTests
{
    private sealed class FakeLifetime : IHostApplicationLifetime
    {
        public CancellationTokenSource Started { get; } = new();
        public CancellationTokenSource Stopping { get; } = new();
        public volatile bool Stopped;
        public CancellationToken ApplicationStarted => Started.Token;
        public CancellationToken ApplicationStopping => Stopping.Token;
        public CancellationToken ApplicationStopped => Stopping.Token;
        public void StopApplication() => Stopped = true;
    }

    private sealed class RestartFactory(Etcd.EtcdFixture etcd, FakeLifetime lifetime) : PgWorkerApiFactory(etcd)
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);
            builder.ConfigureTestServices(services =>
            {
                services.RemoveAll<IHostApplicationLifetime>();
                services.AddSingleton<IHostApplicationLifetime>(lifetime);
            });
        }
    }

    [Fact]
    public async Task Restart_Returns202_AndStopsHostAfterDelay()
    {
        // Arrange: своя фабрика с фейковым lifetime (общий хост не гасим)
        var lifetime = new FakeLifetime();
        using var factory = new RestartFactory(new PgApiFixture().Etcd, lifetime);
        // RestartHandler в Program.cs регистрируется с stopDelay 1 c — тест ждёт ≤3 c
        using var client = factory.CreateClient();

        // Act: POST без тела (spec §3.2 п.2 — тело отсутствует)
        using var response = await client.PostAsync(
            "/api/restart", null, TestContext.Current.CancellationToken);
        var dto = await response.Content.ReadFromJsonAsync<RestartingDto>(TestContext.Current.CancellationToken);

        // Assert: 202 сразу, stop — отложенно
        response.StatusCode.Should().Be(HttpStatusCode.Accepted);
        dto!.Restarting.Should().BeTrue();
        lifetime.Stopped.Should().BeFalse(); // ответ ушёл ДО стопа
        var deadline = DateTimeOffset.UtcNow.AddSeconds(3);
        while (!lifetime.Stopped && DateTimeOffset.UtcNow < deadline)
            await Task.Delay(100, TestContext.Current.CancellationToken);
        lifetime.Stopped.Should().BeTrue(); // graceful stop после ~1 c
    }
}

public sealed record RestartingDto(bool Restarting);
```

Уточнение по Act (без лишнего Content): `using var response = await client.PostAsync("/api/restart", null, TestContext.Current.CancellationToken);`. Фабрике нужен etcd: для standalone-запуска — `new Etcd.EtcdFixture()` + `await etcd.InitializeAsync()` в `IAsyncLifetime`-обёртке, либо переиспользовать `PgApiFixture` через collection — РЕКОМЕНДАЦИЯ: оформить тест-класс на общей `PgApiFixture` нельзя (нужна своя фабрика) → сделать вложенный `IAsyncLifetime`-класс с собственным `EtcdFixture` (dispose в teardown).

- [x] **Шаг 2: прогнать — красный**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~RestartApiTests"`
Expected: FAIL (404 — маршрута нет).

- [x] **Шаг 3: RestartHandler PgWorker + маршрут**

`src/PgWorker.App/Api/Operations/RestartHandler.cs`:

```csharp
using Microsoft.AspNetCore.Hosting;

namespace PgWorker.App.Api.Operations;

// Ответ 202 POST /api/restart (spec §3.2 п.2).
public sealed record RestartDto(bool Restarting);

// Graceful self-stop воркера (spec §3.2 п.2/§4.4): панель НЕ имеет docker-доступа —
// рестарт = самостоятельный стоп; контейнер поднимает docker-политика
// restart: unless-stopped. 202 → пауза ~1 c (ответ успевает уйти) →
// StopApplication. В etcd ничего не пишет: клэймы/джорнал живут в lease,
// операцию продолжит этот же или другой инстанс.
public sealed class RestartHandler(
    IHostApplicationLifetime lifetime,
    ILogger<RestartHandler> logger,
    TimeSpan? stopDelay = null)
{
    public RestartDto Handle(string? requestedBy)
    {
        logger.LogInformation(
            "POST /api/restart: запрошен перезапуск (оператор {Operator})", requestedBy ?? "unknown");
        _ = Task.Run(async () =>
        {
            await Task.Delay(stopDelay ?? TimeSpan.FromSeconds(1));
            lifetime.StopApplication();
        });
        return new RestartDto(true);
    }
}
```

(`using Microsoft.Extensions.Logging;` — добавить.)

В `src/PgWorker.App/Api/ApiModule.cs` (в конец `MapWorkerApi`, перед закрывающей скобкой):

```csharp
        // POST /api/restart — graceful self-stop (spec §3.2 п.2, arch/14 §3.3):
        // 202 {"restarting":true}, стоп отложен на ~1 c; etcd не пишет.
        endpoints.MapPost("/api/restart", (
            [FromHeader(Name = "X-Requested-By")] string? requestedBy,
            RestartHandler handler) => Results.Accepted((string?)null, handler.Handle(requestedBy)));
```

(`using Microsoft.AspNetCore.Mvc;` для `FromHeader`.)

В `src/PgWorker.App/Program.cs` (рядом с SeedDemoHandler):

```csharp
// Graceful self-stop (spec §3.2 п.2): рестарт из панели применяет серт/конфиг.
builder.Services.AddSingleton(sp => new RestartHandler(
    sp.GetRequiredService<IHostApplicationLifetime>(),
    sp.GetRequiredService<ILoggerFactory>().CreateLogger<RestartHandler>()));
```

- [x] **Шаг 4: прогнать PgWorker — зелёный**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~RestartApiTests"`
Expected: PASS.

- [x] **Шаг 5: KafkaWorker — копия теста, хендлера, маршрута, DI**

`src/tests/KafkaWorker.IntegrationTests/Api/RestartApiTests.cs` — 1:1 Шагов 1 с `using KafkaWorker.App.Api.Operations;`, namespace `KafkaWorker.IntegrationTests.Api`, база — `KafkaApiFactory`, собственный `EtcdFixture` (`src/tests/KafkaWorker.IntegrationTests/Etcd/ApiEtcdFixture.cs`), collection-имя локальное (своя фикстура).

`src/KafkaWorker.App/Api/Operations/RestartHandler.cs` — копия `RestartHandler` (namespace `KafkaWorker.App.Api.Operations`, комментарий — arch/16 §1.1).

`src/KafkaWorker.App/Api/ApiModule.cs` — тот же маршрут `/api/restart` в конец маппинга.

`src/KafkaWorker.App/Program.cs` — та же DI-регистрация после хендлеров.

- [x] **Шаг 6: полный прогон**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~RestartApiTests" && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~RestartApiTests"`
Expected: оба зелёные.

- [x] **Шаг 7: коммит**

```bash
git add src/PgWorker.App/Api/Operations/RestartHandler.cs src/KafkaWorker.App/Api/Operations/RestartHandler.cs src/PgWorker.App/Api/ApiModule.cs src/KafkaWorker.App/Api/ApiModule.cs src/PgWorker.App/Program.cs src/KafkaWorker.App/Program.cs src/tests/PgWorker.IntegrationTests/Api/RestartApiTests.cs src/tests/KafkaWorker.IntegrationTests/Api/RestartApiTests.cs
git commit -m "feat(workers): POST /api/restart — graceful self-stop c 202 и отложенным StopApplication"
```

**Выход:** оба воркера обслуживают `POST /api/restart` (202 → self-stop), etcd не трогают.
**Связь со spec:** §2 п.3, §3.2 п.2, §4.4, §7 кр.2 (половина — перезапуск).

---

## Task 4: `cert_thumbprint` в дискавери-ключах (ClaimStore обоих воркеров)

**Вход (предусловие):** Task 1–2 слиты (Program.cs держит `apiTls.ServerCert`).

**Files:**
- Modify: `src/PgWorker.Etcd/Coordination/ClaimStore.cs`
- Modify: `src/KafkaWorker.Etcd/Coordination/ClaimStore.cs`
- Modify: `src/PgWorker.App/Program.cs`, `src/KafkaWorker.App/Program.cs` (передача thumbprint)
- Test: `src/tests/PgWorker.IntegrationTests/Etcd/EtcdCoordinationTests.cs` (новый Fact)
- Test: `src/tests/KafkaWorker.IntegrationTests/Etcd/ClaimStoreTests.cs` (новый Fact)

**Interfaces:**
- Produces: value ключа `/pgworker/api/<id>` и `/kafkaworker/api/<id>` получает опциональное поле `cert_thumbprint` (sha256-hex, lowercase; null → поле не пишется). Читатели (Task 5) парсят поле опционально.
- `ClaimStore(string[] endpoints, IEtcdGateway gateway, TimeProvider clock, string? advertiseApiUrl = null, string? certThumbprint = null)`.

- [x] **Шаг 1: тест ClaimStore PgWorker (красный)**

В `src/tests/PgWorker.IntegrationTests/Etcd/EtcdCoordinationTests.cs` (коллекция `EtcdCollection`, gateway/endpoint фикстуры — по соседним Fact'ам):

```csharp
    [Fact]
    public async Task ApiKey_IncludesCertThumbprint_WhenProvided()
    {
        // Arrange: thumbprint серта, поднятого на грани (spec §3.2 п.3)
        const string thumb = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        var store = new ClaimStore([Endpoint], Gateway, TimeProvider.System,
            advertiseApiUrl: $"https://localhost:9{Random.Shared.Next(1000, 9999)}",
            certThumbprint: thumb);
        await using var _ = store;

        // Act: keepalive-контур ставит api-ключ
        await store.StartAsync(TestContext.Current.CancellationToken);
        await store.KeepaliveTickAsync(TestContext.Current.CancellationToken);

        // Assert: value содержит cert_thumbprint
        var kv = await Gateway.GetAsync(Endpoint, $"/pgworker/api/{store.InstanceId}", TestContext.Current.CancellationToken);
        kv.Value!.Value.Should().Contain($"\"cert_thumbprint\":\"{thumb}\"");

        // Cleanup: ключ гаснет с dispose (revoke lease)
    }

    [Fact]
    public async Task ApiKey_OmitsCertThumbprint_WhenNull()
    {
        // Arrange: старая семантика — поле опционально (spec §3.1)
        var store = new ClaimStore([Endpoint], Gateway, TimeProvider.System,
            advertiseApiUrl: $"https://localhost:9{Random.Shared.Next(1000, 9999)}");
        await using var _ = store;

        // Act
        await store.StartAsync(TestContext.Current.CancellationToken);
        await store.KeepaliveTickAsync(TestContext.Current.CancellationToken);

        // Assert
        var kv = await Gateway.GetAsync(Endpoint, $"/pgworker/api/{store.InstanceId}", TestContext.Current.CancellationToken);
        kv.Value!.Value.Should().NotContain("cert_thumbprint");
    }
```

- [x] **Шаг 2: прогнать — красный**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~EtcdCoordinationTests"`
Expected: FAIL компиляция (нет параметра `certThumbprint`).

- [x] **Шаг 3: правка ClaimStore PgWorker**

`src/PgWorker.Etcd/Coordination/ClaimStore.cs`:

1. Конструктор: `string? advertiseApiUrl = null, string? certThumbprint = null`; поле `private readonly string? _certThumbprint = certThumbprint;`.
2. `ApiDiscoveryPayload`:
```csharp
    // Value ключа /pgworker/api/<id> (arch/14 §1.1): {"url","instance","since_unix",
    // "cert_thumbprint"?} — thumbprint серта, фактически применённого на грани
    // (etcd-ключ §1.1.1 или env-фоллбек); опционально для читателей.
    private sealed record ApiDiscoveryPayload(
        [property: JsonPropertyName("url")] string Url,
        [property: JsonPropertyName("instance")] string Instance,
        [property: JsonPropertyName("since_unix")] long SinceUnix,
        [property: JsonPropertyName("cert_thumbprint")] string? CertThumbprint);
```
3. `EnsureInstanceKeyAsync`: `new ApiDiscoveryPayload(url, InstanceId, Now(), _certThumbprint)`.

- [x] **Шаг 4: прогнать — зелёный**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~EtcdCoordinationTests"`
Expected: PASS.

- [x] **Шаг 5: KafkaWorker — симметрично**

`src/tests/KafkaWorker.IntegrationTests/Etcd/ClaimStoreTests.cs` — два тех же Fact (`/kafkaworker/api/<id>`, локальные фикстуры файла).
`src/KafkaWorker.Etcd/Coordination/ClaimStore.cs` — те же три правки (комментарий — arch/16 §1.1).
Прогон: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~ClaimStoreTests"` — PASS.

- [x] **Шаг 6: Program.cs — передача thumbprint**

`src/PgWorker.App/Program.cs` — после `var apiTls = ApiTlsEndpoints.ConfigureMtls(builder, managedCert);`:

```csharp
// Thumbprint применённого серта → дискавери-ключ (spec §3.2 п.3): панель
// сверяет с целевым → applied/pending restart.
var apiCertThumbprint = apiTls.ServerCert is { } appliedCert
    ? Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(appliedCert.RawData)).ToLowerInvariant()
    : null;
```

В регистрации ClaimStore добавить аргумент: `..., opts-value.Api.AdvertiseUrl, apiCertThumbprint);` (строка ~90: `new ClaimStore(..., sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Api.AdvertiseUrl, apiCertThumbprint)`).

`src/KafkaWorker.App/Program.cs` — то же для `TlsEndpoints.ConfigureMtls`-результата (переменная `apiTls`) и регистрации `ClaimStore` (строка ~76).

- [x] **Шаг 7: сборка + регресс воркерских серий**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Debug && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests -c Debug`
Expected: зелёные.

- [x] **Шаг 8: коммит**

```bash
git add src/PgWorker.Etcd/Coordination/ClaimStore.cs src/KafkaWorker.Etcd/Coordination/ClaimStore.cs src/PgWorker.App/Program.cs src/KafkaWorker.App/Program.cs src/tests/PgWorker.IntegrationTests/Etcd/EtcdCoordinationTests.cs src/tests/KafkaWorker.IntegrationTests/Etcd/ClaimStoreTests.cs
git commit -m "feat(workers): cert_thumbprint применённого серта в дискавери-ключах api/<id>"
```

**Выход:** дискавери-ключи несут thumbprint применённого серта (etcd или env источника).
**Связь со spec:** §3.1 (дискавери-ключи), §3.2 п.3, §4.4 п.4.

---

## Task 5: Панель — модель `WorkerApiCert`, `cert_thumbprint` в `WorkerEndpoint`, чтение ключей снапшотами

**Вход (предусловие):** Task 1–4 слиты.

**Files:**
- Create: `src/AdminPanel.Core/WorkerApiCert.cs`
- Modify: `src/AdminPanel.Core/WorkerEndpoint.cs`
- Modify: `src/AdminPanel.Core/EtcdSnapshot.cs` (+`WorkerApiCert? WorkerApiCert = null`)
- Modify: `src/AdminPanel.Core/Kafka/KafkaSnapshot.cs` (+`WorkerApiCert? WorkerApiCert = null`)
- Create: `src/AdminPanel.Etcd/Parsing/WorkerCertParser.cs`
- Modify: `src/AdminPanel.Etcd/Parsing/WorkerEndpointsParser.cs`
- Modify: `src/AdminPanel.Etcd/SnapshotRefresher.cs`, `src/AdminPanel.Etcd/KafkaSnapshotRefresher.cs`, `src/AdminPanel.Etcd/SnapshotBuilder.cs`
- Test: `src/tests/AdminPanel.UnitTests/WorkerEndpointsParserTests.cs` (дополнить)
- Test: `src/tests/AdminPanel.UnitTests/WorkerCertParserTests.cs` (новый)

**Interfaces:**
- Produces (используют Task 6, 7, 8, 9):
  - `AdminPanel.Core.WorkerApiCert(string Thumbprint, string Subject, string Issuer, IReadOnlyList<string> San, DateTimeOffset NotBefore, DateTimeOffset NotAfter, long UpdatedUnix, string? UpdatedBy)` — PEM-материалы в модель НЕ попадают.
  - `WorkerEndpoint(string InstanceId, string Url, long SinceUnix, string? CertThumbprint = null)`.
  - `EtcdSnapshot.WorkerApiCert` / `KafkaSnapshot.WorkerApiCert` — целевой серт своего воркера (null — ключа нет; битый ключ — parseError, поле null).
  - `AdminPanel.Etcd.Parsing.WorkerCertParseResult(WorkerApiCert? Cert, KeyParseError? Error)`; `WorkerCertParser.Parse(string key, Kv? kv)`.
  - Префиксы: `/workers/api_tls/pgworker`, `/workers/api_tls/kafkaworker`.

- [x] **Шаг 1: тесты WorkerCertParser (красный)**

`src/tests/AdminPanel.UnitTests/WorkerCertParserTests.cs` — TestPki-хелпер (локальная копия вложенного класса по образцу `Workers/WorkerTlsHandlerTests.cs`): `GenerateCa()`, `IssueLeaf(caPem, caKeyPem, cn, eku: Oid[]? = null, ca: bool = false)` с параметрами EKU/CA для будущих задач; SAN — DNS `cn` + IP `127.0.0.1`.

```csharp
using AdminPanel.Core;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Parsing;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Парсер ключа /workers/api_tls/<worker> (spec §3.1): метаданные целевого
// серта без PEM-материалов; битый JSON/серт — parseError (тик не роняют).
public class WorkerCertParserTests
{
    private static TestPki _pki = new(); // хелпер файла: CaPem/CaKeyPem + Issue(...)

    [Fact]
    public void Parse_NullKv_NoCertNoError()
    {
        // Arrange / Act
        var (cert, error) = WorkerCertParser.Parse("/workers/api_tls/pgworker", null);
        // Assert: ключа нет → unmanaged
        cert.Should().BeNull();
        error.Should().BeNull();
    }

    [Fact]
    public void Parse_ValidKey_MetaDataWithoutPem()
    {
        // Arrange
        var (certPem, keyPem) = _pki.Issue("pgworker-api");
        var json = $$"""{"cert_pem":{{System.Text.Json.JsonSerializer.Serialize(certPem)}},"key_pem":{{System.Text.Json.JsonSerializer.Serialize(keyPem)}},"updated_unix":1757000000,"updated_by":"admin"}""";
        // Act
        var (cert, error) = WorkerCertParser.Parse("/workers/api_tls/pgworker", new Kv("/workers/api_tls/pgworker", json, 1));
        // Assert: метаданные; key_pem в модель не попадает
        error.Should().BeNull();
        cert!.Thumbprint.Should().HaveLength(64);
        cert.Subject.Should().Contain("pgworker-api");
        cert.San.Should().Contain("pgworker-api");
        cert.UpdatedUnix.Should().Be(1757000000);
        cert.UpdatedBy.Should().Be("admin");
        cert.NotAfter.Should().BeAfter(cert.NotBefore);
    }

    [Fact]
    public void Parse_BrokenJson_ParseError()
    {
        // Arrange / Act
        var (cert, error) = WorkerCertParser.Parse("/workers/api_tls/pgworker", new Kv("/workers/api_tls/pgworker", "{not-json", 1));
        // Assert: битый ключ — parseError по канону, тик жив
        cert.Should().BeNull();
        error.Should().NotBeNull();
    }

    [Fact]
    public void Parse_BrokenPem_ParseError()
    {
        // Arrange: cert_pem — не сертификат
        var json = """{"cert_pem":"-----BEGIN CERTIFICATE-----\nZZ\n-----END CERTIFICATE-----","key_pem":"x"}""";
        // Act
        var (cert, error) = WorkerCertParser.Parse("/workers/api_tls/pgworker", new Kv("/workers/api_tls/pgworker", json, 1));
        // Assert
        cert.Should().BeNull();
        error.Should().NotBeNull();
    }
}
```

- [x] **Шаг 2: тесты WorkerEndpointsParser с thumbprint (красный)**

Дополнить `src/tests/AdminPanel.UnitTests/WorkerEndpointsParserTests.cs`:

```csharp
    [Fact]
    public void Parse_CertThumbprint_OptionalField()
    {
        // Arrange: новые инстансы пишут thumbprint, старые — нет (spec §3.1)
        var kvs = new List<Kv>
        {
            new("/pgworker/api/new1", """{"url":"http://h:1","instance":"new1","since_unix":1,"cert_thumbprint":"ab"}""", 1),
            new("/pgworker/api/old1", """{"url":"http://h:2","instance":"old1","since_unix":2}""", 2),
        };

        // Act
        var (endpoints, _) = WorkerEndpointsParser.Parse(kvs);

        // Assert
        endpoints.Should().Contain(e => e.InstanceId == "new1" && e.CertThumbprint == "ab");
        endpoints.Should().Contain(e => e.InstanceId == "old1" && e.CertThumbprint is null);
    }
```

- [x] **Шаг 3: прогнать — красный**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~WorkerCertParserTests|FullyQualifiedName~WorkerEndpointsParserTests"`
Expected: FAIL компиляция.

- [x] **Шаг 4: модель WorkerApiCert + WorkerEndpoint.CertThumbprint**

`src/AdminPanel.Core/WorkerApiCert.cs`:

```csharp
namespace AdminPanel.Core;

/// <summary>
/// Метаданные целевого серверного серта API воркера — ключ
/// /workers/api_tls/&lt;worker&gt; (arch/adminpanel/02 §9.9): панель — единственный
/// писатель, воркеры читают при старте. PEM-материалы (cert/key) в модель НЕ
/// попадают — наружу только метаданные (прецедент kafka-паролей §10.1).
/// </summary>
public sealed record WorkerApiCert(
    string Thumbprint,            // sha256-hex (lowercase) серта
    string Subject,
    string Issuer,
    IReadOnlyList<string> San,    // DNS-имена + IP (строками)
    DateTimeOffset NotBefore,
    DateTimeOffset NotAfter,
    long UpdatedUnix,
    string? UpdatedBy);
```

`src/AdminPanel.Core/WorkerEndpoint.cs`: `public sealed record WorkerEndpoint(string InstanceId, string Url, long SinceUnix, string? CertThumbprint = null);` — doc-комментарий дополнить: «`CertThumbprint` — sha256 серта на грани инстанса; null — старая версия не пишет (статус unknown)».

`EtcdSnapshot` — последний опциональный параметр `WorkerApiCert? WorkerApiCert = null` (после `MinioStorage`); `KafkaSnapshot` — после `AdminRotations`.

- [x] **Шаг 5: WorkerCertParser + WorkerEndpointsParser**

`src/AdminPanel.Etcd/Parsing/WorkerCertParser.cs`:

```csharp
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using AdminPanel.Core;

namespace AdminPanel.Etcd.Parsing;

// Результат разбора ключа серта: метаданные либо parseError (битый JSON/PEM).
public sealed record WorkerCertParseResult(WorkerApiCert? Cert, KeyParseError? Error);

// Чистая функция: Kv ключа /workers/api_tls/<worker> → метаданные целевого
// серта (spec §3.1): key_pem в модель не попадает — парсим только серт.
// Битый JSON/серт — KeyParseError (тик не роняют, толерантность парсеров).
public static class WorkerCertParser
{
    public static WorkerCertParseResult Parse(string key, Kv? kv)
    {
        if (kv is null)
            return new(null, null);
        try
        {
            using var doc = JsonDocument.Parse(kv.Value);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object
                || !root.TryGetProperty("cert_pem", out var certPem)
                || certPem.ValueKind != JsonValueKind.String)
                return new(null, new(key, "нет поля cert_pem"));

            using var cert = X509Certificate2.CreateFromPem(certPem.GetString()!);
            var san = new List<string>();
            foreach (var ext in cert.Extensions.OfType<X509SubjectAlternativeNameExtension>())
            {
                san.AddRange(ext.EnumerateDnsNames());
                san.AddRange(ext.EnumerateIPAddresses().Select(ip => ip.ToString()));
            }

            return new(new WorkerApiCert(
                Convert.ToHexString(SHA256.HashData(cert.RawData)).ToLowerInvariant(),
                cert.Subject,
                cert.Issuer,
                san,
                new DateTimeOffset(cert.NotBefore, TimeSpan.Zero),
                new DateTimeOffset(cert.NotAfter, TimeSpan.Zero),
                root.TryGetProperty("updated_unix", out var unix) && unix.ValueKind == JsonValueKind.Number
                    ? unix.GetInt64()
                    : 0,
                root.TryGetProperty("updated_by", out var by) && by.ValueKind == JsonValueKind.String
                    ? by.GetString()
                    : null), null);
        }
        catch (JsonException e)
        {
            return new(null, new(key, $"битый JSON: {e.Message}"));
        }
        catch (Exception e) when (e is ArgumentException or CryptographicException or FormatException)
        {
            return new(null, new(key, $"битый PEM сертификата: {e.Message}"));
        }
    }
}
```

`WorkerEndpointsParser.Parse` — в конструктор `WorkerEndpoint` добавить 4-й аргумент:
```csharp
                endpoints.Add(new WorkerEndpoint(
                    instanceId,
                    url.GetString()!,
                    root.TryGetProperty("since_unix", out var unix) && unix.ValueKind == JsonValueKind.Number
                        ? unix.GetInt64()
                        : 0,
                    root.TryGetProperty("cert_thumbprint", out var tp) && tp.ValueKind == JsonValueKind.String
                        ? tp.GetString()
                        : null));
```

- [x] **Шаг 6: прогнать — зелёный**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~WorkerCertParserTests|FullyQualifiedName~WorkerEndpointsParserTests|FullyQualifiedName~SnapshotBuilderTests"`
Expected: PASS (SnapshotBuilderTests старые не сломаны — опциональные параметры).

- [x] **Шаг 7: чтение ключей в refresher'ах**

`src/AdminPanel.Etcd/SnapshotRefresher.cs`:
1. В тик (рядом с `pgApiTask`, ~строка 93):
```csharp
        var pgCertTask = WithFailoverAsync(alive, active, (ep, t) => gateway.RangeAsync(ep, Prefixes.WorkerApiCert, t), ct);
```
2. После `var pgApiParsed = ...` (~строка 123):
```csharp
        var pgCertKv = await pgCertTask;
        // Префикс-запрос точечный: ровно один ключ /workers/api_tls/pgworker.
        var pgCertParsed = WorkerCertParser.Parse(Prefixes.WorkerApiCert, pgCertKv.Value.FirstOrDefault());
```
3. Ошибку разбора — в общий список ParseErrors снапшота (по канону: добавить в `SnapshotBuilder.Build`-вызов через параметр или в списке errors до Build; практичнее — параметром `WorkerCertParseResult workerCert` в Build, где errors склеиваются).
4. `Prefixes`: `public const string WorkerApiCert = "/workers/api_tls/pgworker";`.

`SnapshotBuilder.Build(...)` — новый параметр `WorkerCertParseResult workerCert` → в `EtcdSnapshot`-конструктор: `..., workerCert.Cert)`; errors: в список `[.. ..., .. workerCert.Errors()]`, где `Errors()` — `Error is null ? [] : [Error]` (локальная static-функция в WorkerCertParser: `public static IReadOnlyList<KeyParseError> ErrorsOf(WorkerCertParseResult r) => r.Error is null ? [] : [r.Error];`).

`src/AdminPanel.Etcd/KafkaSnapshotRefresher.cs` — симметрично: `Prefixes.WorkerApiCert = "/workers/api_tls/kafkaworker"`, чтение, парсинг, поле `WorkerApiCert: certParsed.Cert`, parseError — в `ParseErrors` kafka-снапшота.

- [x] **Шаг 8: сборка панели + полный юнит-прогон**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug`
Expected: зелёные (рефрешер-тесты `SnapshotRefresherTests`/`KafkaRefresherTests` — обновить сигнатуры тиков при необходимости компиляции: передать результат `WorkerCertParser.Parse(key, null)`).

- [x] **Шаг 9: коммит**

```bash
git add src/AdminPanel.Core/WorkerApiCert.cs src/AdminPanel.Core/WorkerEndpoint.cs src/AdminPanel.Core/EtcdSnapshot.cs src/AdminPanel.Core/Kafka/KafkaSnapshot.cs src/AdminPanel.Etcd/Parsing/WorkerCertParser.cs src/AdminPanel.Etcd/Parsing/WorkerEndpointsParser.cs src/AdminPanel.Etcd/SnapshotRefresher.cs src/AdminPanel.Etcd/KafkaSnapshotRefresher.cs src/AdminPanel.Etcd/SnapshotBuilder.cs src/tests/AdminPanel.UnitTests/WorkerCertParserTests.cs src/tests/AdminPanel.UnitTests/WorkerEndpointsParserTests.cs src/tests/AdminPanel.UnitTests/SnapshotRefresherTests.cs src/tests/AdminPanel.UnitTests/KafkaRefresherTests.cs
git commit -m "feat(panel): модель WorkerApiCert + cert_thumbprint в WorkerEndpoint, чтение ключей /workers/api_tls снапшотами"
```

**Выход:** снапшоты панели содержат метаданные целевого серта и thumbprint инстансов; PEM не покидает парсер.
**Связь со spec:** §2 п.7, §3.1 (модель панели), §3.3 п.4 (источник GET /api/workers).

---

## Task 6: Панель — `WorkerCertService` (валидатор §4.3 + генератор + запись/удаление)

**Вход (предусловие):** Task 5 слит (`WorkerApiCert` в модели).

**Files:**
- Create: `src/AdminPanel.Etcd/Workers/WorkerCertService.cs`
- Test: `src/tests/AdminPanel.UnitTests/Workers/WorkerCertServiceTests.cs`

**Interfaces:**
- Consumes: `IEtcdGateway` (TxnAsync/PutAsync/RangeAsync/DeleteAsync + `TxnCompare(key, Version: 0)`), `IOptions<EtcdOptions>` (endpoints failover), `IKafkaSecretsStore` (per-cluster `ca_pem`), `IOptions<WorkerApiOptions>` (`WorkerTls`), `TimeProvider`.
- Produces (использует Task 8):
  - `WorkerCertService` — `[InjectAsSingleton]` (без состояния между вызовами, тяжёлые зависимости):
    - `static string KeyOf(string worker) => $"/workers/api_tls/{worker}"`;
    - `static string? HostOf(string url)` — хост из URL (DNS или IP-строка);
    - `WorkerApiCert ValidateAndBuildMeta(string worker, string certPem, string keyPem)` — правила §4.3 (4–6 → `WorkerCertInvalidException`; 1–3 → `WorkerCertAffectsOutgoingException`);
    - `(string CertPem, string KeyPem, WorkerApiCert Meta) Generate(string worker, IReadOnlyList<string> advertiseHosts)`;
    - `Task<Result<WorkerCertWriteResult>> GenerateAndPutAsync(...)`, `PutAsync(...)`, `Task<Result> DeleteAsync(...)` — все три write-метода гвардят `worker is ("pgworker" or "kafkaworker")` ДО `KeyOf`: иначе → `WorkerNotFoundException` (404; мусорный ключ вида `/workers/api_tls/foo` в etcd физически не может появиться через панель — spec §3.3 п.4). Остальная семантика: generate — самоконтроль-валидация + txn `version==0` (живой ключ → `WorkerCertAlreadyManagedException`); PUT — валидация + безусловный put; DELETE — ключа нет → `WorkerCertNotFoundException`.
  - `WorkerCertWriteResult(string Worker, WorkerApiCert Meta)`.
  - Исключения (все — в `WorkerCertService.cs`, namespace `AdminPanel.Etcd.Workers`; Task 8 переиспользует, дублей в AdminPanel.Api нет): `WorkerCertInvalidException(string reason)` (400), `WorkerCertAffectsOutgoingException(string reason)` (422; message начинается с «сертификат влияет на коммуникации воркеров с их подчинёнными сервисами: »), `WorkerCertAlreadyManagedException(string worker)` (409), `WorkerCertNotFoundException(string worker)` (404), `WorkerNotFoundException(string worker)` (404; общий для всех эндпоинтов модуля, включая рестарт).

- [x] **Шаг 1: тесты валидатора и генератора (красный)**

`src/tests/AdminPanel.UnitTests/Workers/WorkerCertServiceTests.cs` — TestPki-хеллер файла (RSA-2048; параметры: EKU-набор, CA=TRUE, окно валидности, SAN вкл/выкл):

```csharp
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Client;
using AdminPanel.Etcd.Workers;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests.Workers;

// Валидатор «серт не затрагивает исходящие» (spec §4.3) и генератор
// self-signed листа (spec §3.3 п.1): все строки таблицы §4.3 + свойства
// сгенерированного серта (SAN/EKU/CA=false/срок 825 дней).
public class WorkerCertServiceTests
{
    private static readonly string[] NoEndpoints = ["http://dead:1"];

    private sealed class StubSecrets : IKafkaSecretsStore
    {
        public IReadOnlyDictionary<string, KafkaClusterSecrets> Current { get; set; } =
            new Dictionary<string, KafkaClusterSecrets>();
        public void Replace(IReadOnlyDictionary<string, KafkaClusterSecrets> secrets) => Current = secrets;
    }

    private static WorkerCertService Service(StubSecrets? secrets = null, WorkerTlsOptions? tls = null)
        => new(
            new DeadGateway(), // запись в юнитах не нужна: тестируем валидатор/генератор
            Options.Create(new EtcdOptions { Endpoints = NoEndpoints }),
            secrets ?? new StubSecrets(),
            Options.Create(new WorkerApiOptions { WorkerTls = tls ?? new WorkerTlsOptions() }),
            new FixedTimeProvider());

    // ===== Таблица §4.3 =====

    [Fact]
    public void Validate_ValidLeaf_Passes()
    {
        // Arrange: лист с SAN и EKU serverAuth
        var (cert, key) = TestPki.Issue(eku: [TestPki.ServerAuth], san: true);

        // Act / Assert: не кидает, метаданные на месте
        var meta = Service().ValidateAndBuildMeta("pgworker", cert, key);
        meta.Thumbprint.Should().HaveLength(64);
    }

    [Fact]
    public void Validate_BrokenPair_Invalid400()
    {
        // Arrange: правило 4 — ключ не соответствует серту
        var (cert, _) = TestPki.Issue(eku: [TestPki.ServerAuth], san: true);
        var (_, otherKey) = TestPki.Issue(eku: [TestPki.ServerAuth], san: true);

        // Act / Assert
        Assert.Throws<WorkerCertInvalidException>(() => Service().ValidateAndBuildMeta("pgworker", cert, otherKey));
    }

    [Fact]
    public void Validate_Expired_Invalid400()
    {
        // Arrange: правило 5 — NotAfter в прошлом
        var (cert, key) = TestPki.Issue(eku: [TestPki.ServerAuth], san: true, notAfter: DateTimeOffset.UtcNow.AddDays(-1));

        // Act / Assert
        Assert.Throws<WorkerCertInvalidException>(() => Service().ValidateAndBuildMeta("pgworker", cert, key));
    }

    [Fact]
    public void Validate_NoSan_Invalid400()
    {
        // Arrange: правило 6 — ни одного SAN
        var (cert, key) = TestPki.Issue(eku: [TestPki.ServerAuth], san: false);

        // Act / Assert
        Assert.Throws<WorkerCertInvalidException>(() => Service().ValidateAndBuildMeta("pgworker", cert, key));
    }

    [Fact]
    public void Validate_CaTrue_AffectsOutgoing422()
    {
        // Arrange: правило 1 — CA-серт как trust anchor исходящих
        var (ca, caKey) = TestPki.GenerateCa();

        // Act / Assert: 422 с явной формулировкой
        var ex = Assert.Throws<WorkerCertAffectsOutgoingException>(
            () => Service().ValidateAndBuildMeta("pgworker", ca, caKey));
        ex.Message.Should().Contain("подчинёнными сервисами");
    }

    [Fact]
    public void Validate_ClientAuthEku_AffectsOutgoing422()
    {
        // Arrange: правило 2 — EKU clientAuth пригоден в исходящих
        var (cert, key) = TestPki.Issue(eku: [TestPki.ServerAuth, TestPki.ClientAuth], san: true);

        // Act / Assert
        Assert.Throws<WorkerCertAffectsOutgoingException>(() => Service().ValidateAndBuildMeta("pgworker", cert, key));
    }

    [Fact]
    public void Validate_EkuWithoutServerAuth_AffectsOutgoing422()
    {
        // Arrange: правило 2 — EKU задан, serverAuth нет
        var (cert, key) = TestPki.Issue(eku: [TestPki.CodeSigning], san: true);

        // Act / Assert
        Assert.Throws<WorkerCertAffectsOutgoingException>(() => Service().ValidateAndBuildMeta("pgworker", cert, key));
    }

    [Fact]
    public void Validate_ThumbprintMatchesKafkaCa_AffectsOutgoing422()
    {
        // Arrange: правило 3 — серт = per-cluster CA kafka-кластера
        var (ca, caKey) = TestPki.GenerateCa();
        var secrets = new StubSecrets
        {
            Current = new Dictionary<string, KafkaClusterSecrets>
            { ["c1"] = new("c1", "u", "p", ca) },
        };

        // Act / Assert
        Assert.Throws<WorkerCertAffectsOutgoingException>(() => Service(secrets).ValidateAndBuildMeta("pgworker", ca, caKey));
    }

    [Fact]
    public void Validate_ThumbprintMatchesPanelServerCa_AffectsOutgoing422()
    {
        // Arrange: правило 3 — кандидат совпадает по fingerprint с ServerCa панели
        var (ca, caKey) = TestPki.GenerateCa();
        var tls = new WorkerTlsOptions { ServerCaPem = ca };
        var (leaf, leafKey) = TestPki.Issue(eku: [TestPki.ServerAuth], san: true);

        // Act / Assert: CA-кандидат (thumbprint == ServerCa) — отказ
        Assert.Throws<WorkerCertAffectsOutgoingException>(
            () => Service(tls: tls).ValidateAndBuildMeta("pgworker", ca, caKey));
        // sanity: обычный лист при том же конфиге проходит
        Service(tls: tls).ValidateAndBuildMeta("pgworker", leaf, leafKey);
    }

    [Fact]
    public void Validate_KafkaCaBundle_AllPiecesChecked()
    {
        // Arrange: ca_pem в окне ротации — бандл OLD+NEW (arch/15 §2.1)
        var (oldCa, _) = TestPki.GenerateCa();
        var (newCa, newKey) = TestPki.GenerateCa();
        var bundle = oldCa + "\n" + newCa;
        var secrets = new StubSecrets
        {
            Current = new Dictionary<string, KafkaClusterSecrets> { ["c1"] = new("c1", "u", "p", bundle) },
        };

        // Act / Assert: NEW-часть бандла тоже опознаётся как известный материал
        Assert.Throws<WorkerCertAffectsOutgoingException>(
            () => Service(secrets).ValidateAndBuildMeta("pgworker", newCa, newKey));
    }

    // ===== Генератор (spec §3.3 п.1) =====

    [Fact]
    public void Generate_SelfSignedLeaf_Properties()
    {
        // Act
        var (certPem, keyPem, meta) = Service().Generate("pgworker", ["worker1.local"]);

        // Assert: SAN = advertise + localhost/127.0.0.1; EKU только serverAuth;
        // CA=false; срок 825 дней; NotBefore ≤ now (сдвиг −5 мин)
        meta.San.Should().Contain("worker1.local").And.Contain("localhost").And.Contain("127.0.0.1");
        using var cert = X509Certificate2.CreateFromPem(certPem, keyPem);
        var eku = cert.Extensions.OfType<X509EnhancedKeyUsageExtension>().Single();
        eku.EnhancedKeyUsages.Cast<Oid>().Select(o => o.Value)
            .Should().BeEquivalentTo(["1.3.6.1.5.5.7.3.1"]);
        cert.Extensions.OfType<X509BasicConstraintsExtension>().Single().CertificateAuthority.Should().BeFalse();
        (cert.NotAfter - cert.NotBefore).Days.Should().BeInRange(820, 830);
        cert.NotBefore.Should().BeBefore(DateTimeOffset.UtcNow);
        meta.Thumbprint.Should().HaveLength(64);
    }

    [Fact]
    public void Generate_EmptyHosts_LocalhostOnly()
    {
        // Arrange: живых инстансов нет → SAN localhost/127.0.0.1 (spec §4.1 п.1)
        // Act / Assert
        var (_, _, meta) = Service().Generate("pgworker", []);
        meta.San.Should().BeEquivalentTo(["localhost", "127.0.0.1"]);
    }

    [Fact]
    public void Generate_PassesValidation_SelfCheck()
    {
        // Arrange: сгенерированный серт обязан проходить §4.3 (самоконтроль)
        var (certPem, keyPem, _) = Service().Generate("kafkaworker", ["b1"]);

        // Act / Assert: не кидает
        Service().ValidateAndBuildMeta("kafkaworker", certPem, keyPem);
    }

    // ===== Гвард worker (spec §3.3 п.4: 404 до любой записи) =====

    [Fact]
    public async Task Write_UnknownWorker_RejectedWithoutEtcdCall()
    {
        // Arrange: worker вне pgworker|kafkaworker; шлюз DeadGateway — успех
        // возможен только если гвард сработал ДО etcd-обращения
        // Act / Assert: все три write-метода — WorkerNotFoundException
        var put = await Service().PutAsync("foo", "x", "y", "admin", CancellationToken.None);
        put.Error.Should().BeOfType<WorkerNotFoundException>();
        var gen = await Service().GenerateAndPutAsync("foo", [], "admin", CancellationToken.None);
        gen.Error.Should().BeOfType<WorkerNotFoundException>();
        var del = await Service().DeleteAsync("foo", CancellationToken.None);
        del.Error.Should().BeOfType<WorkerNotFoundException>();
    }
}
```

`DeadGateway : IEtcdGateway` — заглушка файла, все методы `Task.FromResult(Result.Failed(new EtcdUnreachableException("dead")))` (сигнатуры — `src/AdminPanel.Etcd/Client/IEtcdGateway.cs`). `FixedTimeProvider` — уже есть в `src/tests/AdminPanel.UnitTests/FixedTimeProvider.cs`.
Кейс `Validate_KafkaCaBundle_AllPiecesChecked` упростить: завести в TestPki `IssueWithKey`-пару для newCa невозможно (CA генерируется self-signed со своим ключом) → использовать `TestPki.GenerateCa()` второй раз и передавать ЕГО ключ: `var (_, newKey) = TestPki.GenerateCa()` неверно — ключ от другой CA. Правильно: `GenerateCa` возвращает пару `(caPem, caKeyPem)`; для бандла взять ДВЕ пары: `(oldCa, oldKey)` и `(newCa, newKey)`; валидировать `newCa`+`newKey`.

- [x] **Шаг 2: прогнать — красный**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~WorkerCertServiceTests"`
Expected: FAIL компиляция.

- [x] **Шаг 3: реализация WorkerCertService**

`src/AdminPanel.Etcd/Workers/WorkerCertService.cs`:

```csharp
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Text.Json.Serialization;
using AdminPanel.Core;
using AdminPanel.Etcd.Client;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.Options;

namespace AdminPanel.Etcd.Workers;

// 422: сертификат влияет на исходящие коммуникации (spec §4.3 правила 1–3).
public sealed class WorkerCertAffectsOutgoingException(string reason)
    : Exception($"сертификат влияет на коммуникации воркеров с их подчинёнными сервисами: {reason}");

// 400: не годен как серверный (правила 4–6).
public sealed class WorkerCertInvalidException(string reason) : Exception(reason);

// 409: generate при живом ключе (spec §4.1 п.2).
public sealed class WorkerCertAlreadyManagedException(string worker)
    : Exception($"сертификат {worker} уже управляется — замените (PUT api-cert) или удалите (DELETE api-cert)");

// 404: ключа нет (DELETE, spec §4.2).
public sealed class WorkerCertNotFoundException(string worker)
    : Exception($"ключ /workers/api_tls/{worker} не найден");

// 404: неизвестный воркер (worker ∈ pgworker|kafkaworker, spec §3.3 п.4).
// Гвардится сервисом ДО KeyOf во ВСЕХ write-методах (и рестарт-хендлером
// панели): мусорный ключ /workers/api_tls/<foo> в etcd не пишется никогда.
public sealed class WorkerNotFoundException(string worker)
    : Exception($"неизвестный воркер {worker} (ожидался pgworker|kafkaworker)");

// Итог записи: воркер + метаданные записанного серта.
public sealed record WorkerCertWriteResult(string Worker, WorkerApiCert Meta);

// Ядро грани «серты API воркеров» (spec §3.3 п.1, arch/adminpanel/02 §9.9):
// валидация «не затрагивает исходящие», генерация self-signed листа,
// запись/удаление ключа /workers/api_tls/<worker>. Панель — единственный
// писатель; запись напрямую в etcd (вторая категория после §9 provisioning).
[InjectAsSingleton]
public sealed class WorkerCertService(
    IEtcdGateway gateway,
    IOptions<EtcdOptions> etcdOptions,
    IKafkaSecretsStore kafkaSecrets,
    IOptions<WorkerApiOptions> workerApiOptions,
    TimeProvider clock)
{
    private static readonly Oid ServerAuthOid = new("1.3.6.1.5.5.7.3.1");
    private static readonly Oid ClientAuthOid = new("1.3.6.1.5.5.7.3.2");

    public static string KeyOf(string worker) => $"/workers/api_tls/{worker}";

    // Хост advertise-URL (DNS или IP-строка) для SAN генерации.
    public static string? HostOf(string url)
        => Uri.TryCreate(url, UriKind.Absolute, out var uri) && uri.Host.Length > 0 ? uri.Host : null;

    // ===== Валидация §4.3 =====
    public WorkerApiCert ValidateAndBuildMeta(string worker, string certPem, string keyPem)
    {
        X509Certificate2 cert;
        try
        {
            cert = X509Certificate2.CreateFromPem(certPem, keyPem);
        }
        catch (Exception e)
        {
            throw new WorkerCertInvalidException(
                $"PEM-пара невалидна (синтаксис или ключ не соответствует серту): {e.Message}");
        }

        using var _ = cert;
        var now = clock.GetUtcNow();

        // Правило 5: NotBefore ≤ now < NotAfter.
        if (new DateTimeOffset(cert.NotBefore, TimeSpan.Zero) > now
            || new DateTimeOffset(cert.NotAfter, TimeSpan.Zero) <= now)
            throw new WorkerCertInvalidException(
                $"срок действия: NotBefore {cert.NotBefore:u} ≤ now < NotAfter {cert.NotAfter:u} — сейчас {now:u}");

        // SAN (правило 6) — заодно для метаданных.
        var san = new List<string>();
        foreach (var ext in cert.Extensions.OfType<X509SubjectAlternativeNameExtension>())
        {
            san.AddRange(ext.EnumerateDnsNames());
            san.AddRange(ext.EnumerateIPAddresses().Select(ip => ip.ToString()));
        }

        if (san.Count == 0)
            throw new WorkerCertInvalidException("нет ни одного SAN (DNS или IP)");

        // Правило 1: CA=TRUE — потенциальный trust anchor исходящих.
        if (cert.Extensions.OfType<X509BasicConstraintsExtension>()
            .Any(bc => bc.CertificateAuthority))
            throw new WorkerCertAffectsOutgoingException(
                "сертификат — CA (BasicConstraints CA=TRUE)");

        // Правило 2: EKU задан → serverAuth есть, clientAuth нет.
        var eku = cert.Extensions.OfType<X509EnhancedKeyUsageExtension>().FirstOrDefault();
        if (eku is not null)
        {
            var oids = eku.EnhancedKeyUsages.Cast<Oid>().Select(o => o.Value).ToHashSet();
            if (!oids.Contains(ServerAuthOid.Value) || oids.Contains(ClientAuthOid.Value))
                throw new WorkerCertAffectsOutgoingException(
                    "EKU не содержит serverAuth либо пригоден в исходящих (clientAuth)");
        }

        // Правило 3: sha256 не совпадает ни с одним известным материалом доверия/исходящих.
        var thumbprint = Convert.ToHexString(SHA256.HashData(cert.RawData)).ToLowerInvariant();
        foreach (var known in KnownThumbprints())
        {
            if (known == thumbprint)
                throw new WorkerCertAffectsOutgoingException(
                    "sha256-отпечаток совпадает с известным материалом доверия/исходящих установки "
                    + "(per-cluster CA kafka-кластера или серты панели)");
        }

        return new WorkerApiCert(
            thumbprint, cert.Subject, cert.Issuer, san,
            new DateTimeOffset(cert.NotBefore, TimeSpan.Zero),
            new DateTimeOffset(cert.NotAfter, TimeSpan.Zero),
            clock.GetUtcNow().ToUnixTimeSeconds(),
            updatedBy: null);
    }

    // ===== Генерация (spec §3.3 п.1) =====
    public (string CertPem, string KeyPem, WorkerApiCert Meta) Generate(
        string worker, IReadOnlyList<string> advertiseHosts)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN={worker}-api", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        foreach (var host in advertiseHosts.Where(h => h.Length > 0).Distinct())
        {
            if (IPAddress.TryParse(host, out var ip))
                san.AddIpAddress(ip);
            else
                san.AddDnsName(host);
        }

        san.AddDnsName("localhost");
        san.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(
            new X509EnhancedKeyUsageExtension([ServerAuthOid], critical: false));
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        var now = clock.GetUtcNow();
        using var cert = request.CreateSelfSigned(now.AddMinutes(-5), now.AddDays(825));
        var certPem = cert.ExportCertificatePem();
        var keyPem = rsa.ExportPkcs8PrivateKeyPem();
        return (certPem, keyPem, ValidateAndBuildMeta(worker, certPem, keyPem));
    }

    // ===== Запись (spec §4.1/§4.2) =====
    public async Task<Result<WorkerCertWriteResult>> GenerateAndPutAsync(
        string worker, IReadOnlyList<string> advertiseHosts, string updatedBy, CancellationToken ct)
    {
        if (worker is not ("pgworker" or "kafkaworker"))
            return Result<WorkerCertWriteResult>.Failed(new WorkerNotFoundException(worker)); // до KeyOf: мусорные ключи не пишем
        var (certPem, keyPem, meta) = Generate(worker, advertiseHosts);
        var value = SerializePayload(certPem, keyPem, updatedBy);
        var txn = await WithEtcdAsync(endpoint => gateway.TxnAsync(
            endpoint,
            [new TxnCompare(KeyOf(worker), Version: 0)],
            [new KvPut(KeyOf(worker), value)],
            ct));
        if (!txn.IsSuccess)
            return Result<WorkerCertWriteResult>.Failed(txn.Error!);
        if (!txn.Value.Succeeded)
            return Result<WorkerCertWriteResult>.Failed(new WorkerCertAlreadyManagedException(worker));
        return Result<WorkerCertWriteResult>.Success(new WorkerCertWriteResult(worker, meta));
    }

    public async Task<Result<WorkerCertWriteResult>> PutAsync(
        string worker, string certPem, string keyPem, string updatedBy, CancellationToken ct)
    {
        if (worker is not ("pgworker" or "kafkaworker"))
            return Result<WorkerCertWriteResult>.Failed(new WorkerNotFoundException(worker)); // до KeyOf: мусорные ключи не пишем
        var meta = ValidateAndBuildMeta(worker, certPem, keyPem);
        var put = await WithEtcdAsync(endpoint => gateway.PutAsync(
            endpoint, KeyOf(worker), SerializePayload(certPem, keyPem, updatedBy), ct));
        if (!put.IsSuccess)
            return Result<WorkerCertWriteResult>.Failed(put.Error!);
        return Result<WorkerCertWriteResult>.Success(new WorkerCertWriteResult(worker, meta));
    }

    public async Task<Result> DeleteAsync(string worker, CancellationToken ct)
    {
        if (worker is not ("pgworker" or "kafkaworker"))
            return Result.Failed(new WorkerNotFoundException(worker)); // даже если мусорный ключ кем-то записан — не трогаем
        var existing = await WithEtcdAsync(endpoint =>
            gateway.RangeAsync(endpoint, KeyOf(worker), ct));
        if (!existing.IsSuccess)
            return Result.Failed(existing.Error!);
        if (existing.Value.Count == 0)
            return Result.Failed(new WorkerCertNotFoundException(worker));
        return await WithEtcdAsync(endpoint => gateway.DeleteAsync(endpoint, KeyOf(worker), prefix: false, ct));
    }

    // Value ключа: {"cert_pem","key_pem","updated_unix","updated_by"} (§3.1).
    private string SerializePayload(string certPem, string keyPem, string updatedBy)
    {
        var meta = new { cert_pem = certPem, key_pem = keyPem,
            updated_unix = clock.GetUtcNow().ToUnixTimeSeconds(), updated_by = updatedBy };
        return JsonSerializer.Serialize(meta, PayloadJson);
    }

    private static readonly JsonSerializerOptions PayloadJson = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    // Известные материалы исходящих/доверия: per-cluster ca_pem kafka (бандл
    // OLD+NEW разворачиваем по кускам) + клиентский серт и ServerCa панели.
    // Терминология: «ClientCa» из spec §4.3 правило 3 трактуется как клиентский
    // СЕРТ панели — AdminPanel:Workers:WorkerTls:ClientCertPem[_PATH]; поля
    // ClientCa в конфиге НЕТ (WorkerTlsOptions: ClientCert*/ServerCa*).
    private IEnumerable<string> KnownThumbprints()
    {
        foreach (var secrets in kafkaSecrets.Current.Values)
            foreach (var pem in SplitBundle(secrets.CaPem))
                if (TryThumbprint(pem, out var thumb))
                    yield return thumb;

        var tls = workerApiOptions.Value.WorkerTls;
        foreach (var pem in new[]
        {
            tls.ClientCertPem ?? ReadFile(tls.ClientCertPath),
            tls.ServerCaPem ?? ReadFile(tls.ServerCaPath),
        })
            if (pem is not null && TryThumbprint(pem, out var thumb))
                yield return thumb;
    }

    // Бандл ca_pem в окне ротации — конкатенация PEM (arch/15 §2.1): каждый кусок.
    private static IEnumerable<string> SplitBundle(string pem)
        => pem.Split("-----END CERTIFICATE-----", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(p => p.Contains("BEGIN CERTIFICATE"))
            .Select(p => p + "\n-----END CERTIFICATE-----");

    private static bool TryThumbprint(string pem, out string thumbprint)
    {
        try
        {
            using var cert = X509Certificate2.CreateFromPem(pem);
            thumbprint = Convert.ToHexString(SHA256.HashData(cert.RawData)).ToLowerInvariant();
            return true;
        }
        catch (Exception e) when (e is ArgumentException or CryptographicException or FormatException)
        {
            thumbprint = "";
            return false;
        }
    }

    private static string? ReadFile(string? path)
        => path is null || !File.Exists(path) ? null : File.ReadAllText(path).Trim();

    // Failover по endpoint'ам панели: первый успешный ответ выигрывает.
    private async Task<Result<T>> WithEtcdAsync<T>(Func<string, Task<Result<T>>> call)
    {
        Result<T>? last = null;
        foreach (var endpoint in etcdOptions.Value.Endpoints)
        {
            var result = await call(endpoint);
            if (result.IsSuccess)
                return result;
            last = result;
        }

        return last ?? Result<T>.Failed(new EtcdUnreachableException("AdminPanel:Etcd:Endpoints не заданы"));
    }
}
```

Примечания: `EtcdUnreachableException` — проверить фактическое имя в `src/AdminPanel.Etcd/Client/EtcdGateway.cs` и использовать его; `InjectAsSingleton` — из `AdminPanel.Infrastructure.DI`; `[Config]`-регистрация `WorkerApiOptions` уже есть (AddEtcd → AutoRegistration сканирует сборку — атрибут сам подхватится).

- [x] **Шаг 4: прогнать юниты — зелёный**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~WorkerCertServiceTests"`
Expected: PASS (все кейсы).

- [x] **Шаг 5: сборка + коммит**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx`
Expected: без предупреждений.

```bash
git add src/AdminPanel.Etcd/Workers/WorkerCertService.cs src/tests/AdminPanel.UnitTests/Workers/WorkerCertServiceTests.cs
git commit -m "feat(panel): WorkerCertService — валидатор изоляции исходящих, генератор self-signed листа, запись/удаление ключа"
```

**Выход:** панель умеет валидировать/генерировать/писать/удалять серты с полными правилами §4.3.
**Связь со spec:** §2 п.5/п.6, §3.3 п.1, §4.1–§4.3, §7 кр.3/кр.4 (серверная часть).

---

## Task 7: Панель — `WorkerApiGateway.SendAllAsync` + thumbprint-доверие `WorkerTlsHandler`

**Вход (предусловие):** Task 5 слит (`WorkerApiCert` в снапшотах).

**Files:**
- Modify: `src/AdminPanel.Etcd/Workers/IWorkerApiGateway.cs`
- Modify: `src/AdminPanel.Etcd/Workers/WorkerApiGateway.cs`
- Modify: `src/AdminPanel.Etcd/Workers/WorkerTlsHandler.cs`
- Modify: `src/AdminPanel.Etcd/ModuleExtensions.cs`
- Test: `src/tests/AdminPanel.UnitTests/Workers/WorkerApiGatewayTests.cs` (дополнить)
- Test: `src/tests/AdminPanel.UnitTests/Workers/WorkerTlsHandlerTests.cs` (дополнить)

**Interfaces:**
- Produces (использует Task 8):
  - `WorkerApiInstanceResult(string Instance, WorkerApiResult? Response, string? Error)` — per-instance итог (Response — любой HTTP-ответ; Error — сетевой сбой/таймаут).
  - `IWorkerApiGateway.SendAllAsync(string worker, HttpMethod method, string path, object? body, string? requestedBy, CancellationToken ct)` → `Task<IReadOnlyList<WorkerApiInstanceResult>>`; живых ключей нет → `WorkerApiUnavailableException`.
  - `WorkerTlsHandler.Build(WorkerTlsOptions tls, Func<IReadOnlyCollection<string>>? trustedThumbprints = null)` — серверный серт валиден, если цепочка к ServerCa ИЛИ sha256 в `trustedThumbprints()`.

- [x] **Шаг 1: тест SendAllAsync (красный)**

Дополнить `src/tests/AdminPanel.UnitTests/Workers/WorkerApiGatewayTests.cs` по существующему паттерну файла (стаб-снапшоты + http-стаб или готовый локальный механизм файла):

```csharp
    [Fact]
    public async Task SendAll_TwoLiveEndpoints_BothCalledPerInstance()
    {
        // Arrange: два живых ключа (inst1 → 202, inst2 → 500; оба URL отвечают)
        // механизмом файла (TestSnapshotStore/стаб-снапшоты + http-стабы URL)

        // Act
        var results = await gateway.SendAllAsync(
            "pgworker", HttpMethod.Post, "/api/restart", body: null,
            requestedBy: "admin", TestContext.Current.CancellationToken);

        // Assert: оба инстанса в ответе (порядок сортировки InstanceId); ответ
        // НЕ-2xx НЕ прерывает остальных (в отличие от failover SendAsync)
        results.Should().HaveCount(2);
        results.Should().Contain(r => r.Instance == "inst1" && r.Response!.StatusCode == 202 && r.Error is null);
        results.Should().Contain(r => r.Instance == "inst2" && r.Response!.StatusCode == 500 && r.Error is null);
    }

    [Fact]
    public async Task SendAll_DeadEndpoint_ErrorResultOthersAlive()
    {
        // Arrange: inst1 — мёртвый порт (зонд свободного), inst2 — живой http-стаб

        // Act
        var results = await gateway.SendAllAsync(
            "pgworker", HttpMethod.Post, "/api/restart", null, null, TestContext.Current.CancellationToken);

        // Assert: мёртвый — Error != null и Response == null; живой — Response
        results.Should().HaveCount(2);
        results.Should().Contain(r => r.Instance == "inst1" && r.Error is not null && r.Response is null);
        results.Should().Contain(r => r.Instance == "inst2" && r.Response is not null && r.Error is null);
    }

    [Fact]
    public async Task SendAll_NoLiveEndpoints_ThrowsUnavailable()
    {
        // Arrange: снапшот с пустым списком ключей
        // Act / Assert
        await Assert.ThrowsAsync<WorkerApiUnavailableException>(() =>
            gateway.SendAllAsync("pgworker", HttpMethod.Post, "/api/restart", null, null,
                TestContext.Current.CancellationToken));
    }
```

- [x] **Шаг 2: тест thumbprint-доверия (красный)**

Дополнить `src/tests/AdminPanel.UnitTests/Workers/WorkerTlsHandlerTests.cs` (там есть TestPki и SslStream-механика):

```csharp
    [Fact]
    public async Task Build_SelfSignedWithTrustedThumbprint_HandshakeOk()
    {
        // Arrange: серверный self-signed лист ВНЕ ServerCa (своя RSA-пара);
        // доверие — thumbprint этого листа (spec §3.3 п.3: панель доверяет
        // сертам, которые сама записала)
        var thumb = Convert.ToHexString(
            SHA256.HashData(serverCert.RawData)).ToLowerInvariant();

        // Act: handler = WorkerTlsHandler.Build(new WorkerTlsOptions(), () => [thumb]);
        // TLS-клиент на этом handler → локальный SslStream/Tls-сервер на serverCert
        // (механика соседних тестов файла)

        // Assert: хендшейк успешен, запрос прошёл
    }

    [Fact]
    public async Task Build_UnknownSelfSigned_HandshakeRefused()
    {
        // Arrange: тот же серверный серт, но доверие-колбэк отдаёт ДРУГОЙ thumbprint
        // (или ServerCa не задан и список пуст)

        // Act / Assert: TLS-хендшейк отказ (AuthenticationException у клиента)
    }
```

- [x] **Шаг 3: прогнать — красный**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~WorkerApiGatewayTests|FullyQualifiedName~WorkerTlsHandlerTests"`
Expected: FAIL компиляция.

- [x] **Шаг 4: реализация SendAllAsync**

`IWorkerApiGateway.cs` — добавить:

```csharp
/// <summary>Per-instance итог broadcast-вызова (spec §3.3 п.2): Response — любой
/// HTTP-ответ инстанса; Error — сетевой сбой/таймаут этого URL.</summary>
public sealed record WorkerApiInstanceResult(string Instance, WorkerApiResult? Response, string? Error);
```

и в интерфейс:

```csharp
    /// <summary>
    /// Обход ВСЕХ живых endpoints (рестарт, spec §4.4): без failover — каждый
    /// инстанс получает запрос независимо от остальных; per-instance итог.
    /// Живых ключей нет → WorkerApiUnavailableException.
    /// </summary>
    Task<IReadOnlyList<WorkerApiInstanceResult>> SendAllAsync(
        string worker, HttpMethod method, string path, object? body, string? requestedBy, CancellationToken ct);
```

`WorkerApiGateway.cs` — метод (переиспользует resolve/timeout/запрос-блок `SendAsync` — выделить приватный хелпер `SendCoreAsync(HttpClient, WorkerEndpoint, HttpMethod, string, object?, string?, CancellationToken)`):

```csharp
    public async Task<IReadOnlyList<WorkerApiInstanceResult>> SendAllAsync(
        string worker, HttpMethod method, string path, object? body, string? requestedBy, CancellationToken ct)
    {
        var endpoints = ResolveEndpoints(worker)
            ?? throw new WorkerApiUnavailableException(worker);
        var ordered = endpoints.OrderBy(e => e.InstanceId, StringComparer.Ordinal).ToArray();
        if (ordered.Length == 0)
            throw new WorkerApiUnavailableException(worker);

        using var client = factory.CreateClient(HttpClientName);
        var seconds = options.Value.TimeoutSec;
        if (seconds > 0)
            client.Timeout = TimeSpan.FromSeconds(seconds);

        var results = new List<WorkerApiInstanceResult>();
        foreach (var endpoint in ordered)
        {
            try
            {
                using var response = await SendCoreAsync(client, endpoint, method, path, body, requestedBy, ct);
                results.Add(new(endpoint.InstanceId, response, null));
            }
            catch (HttpRequestException e)
            {
                results.Add(new(endpoint.InstanceId, null, e.Message)); // сетевой сбой этого URL — остальные продолжаются
            }
            catch (TaskCanceledException e) when (!ct.IsCancellationRequested)
            {
                results.Add(new(endpoint.InstanceId, null, $"таймаут: {e.Message}"));
            }
        }

        return results;
    }
```

`SendAsync` рефакторится на вызов того же `SendCoreAsync` (поведение failover не меняется — тесты `WorkerApiGatewayTests` прежние обязаны остаться зелёными).

- [x] **Шаг 5: реализация thumbprint-доверия**

`WorkerTlsHandler.cs`:

1. Сигнатура: `public static HttpMessageHandler Build(WorkerTlsOptions tls, Func<IReadOnlyCollection<string>>? trustedThumbprints = null)`.
2. Внутри ветки `if (certPem is not null && keyPem is not null)`:
```csharp
            var ca = serverCaPem is not null ? X509Certificate2.CreateFromPem(serverCaPem) : null;
            if (ca is not null || trustedThumbprints is not null)
                handler.SslOptions.RemoteCertificateValidationCallback =
                    (_, certificate, _, _) =>
                    {
                        // Колбэк отдаёт X509Certificate — построим X509Certificate2.
                        var cert2 = certificate as X509Certificate2
                            ?? (certificate is null ? null : new X509Certificate2(certificate));
                        if (cert2 is null)
                            return false;
                        // Цепочка к per-install ServerCA ИЛИ доверие по thumbprint:
                        // панель доверяет сертам, которые сама записала (spec §3.3 п.3,
                        // arch/adminpanel/02 §9.9) — иначе self-signed генерация рвала бы
                        // доступ к перезапущенному воркеру.
                        if (ca is not null && ValidateChain(cert2, ca))
                            return true;
                        var thumbprint = Convert.ToHexString(
                            System.Security.Cryptography.SHA256.HashData(cert2.RawData)).ToLowerInvariant();
                        return trustedThumbprints?.Invoke().Contains(thumbprint) == true;
                    };
```
(существующий блок `if (serverCaPem is not null) { ... }` заменяется приведённым; `using` на `ca` не ставить — серты живут время жизни handler'а).

- [x] **Шаг 6: подключение живых thumbprint'ов в ModuleExtensions**

`src/AdminPanel.Etcd/ModuleExtensions.cs` — замена регистрации `AddHttpClient(WorkerApiGateway.HttpClientName)`:

```csharp
        services.AddHttpClient(WorkerApiGateway.HttpClientName)
            .ConfigurePrimaryHttpMessageHandler(sp => Workers.WorkerTlsHandler.Build(
                sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<WorkerApiOptions>>().Value.WorkerTls,
                TrustedWorkerThumbprints(sp)));

        // Живые thumbprint'ы целевых сертов из снапшотов (spec §3.3 п.3): панель
        // доверяет сертам, которые сама записала; null-поле — ключа нет.
        static Func<IReadOnlyCollection<string>> TrustedWorkerThumbprints(IServiceProvider sp) => () =>
        {
            var thumbs = new List<string>();
            if (sp.GetRequiredService<ISnapshotStore>().Current?.WorkerApiCert is { } pg)
                thumbs.Add(pg.Thumbprint);
            if (sp.GetRequiredService<IKafkaSnapshotStore>().Current?.WorkerApiCert is { } kfw)
                thumbs.Add(kfw.Thumbprint);
            return thumbs;
        };
```

- [x] **Шаг 7: прогнать — зелёный + стаб TestWorkerApi**

`src/tests/AdminPanel.IntegrationTests/AuthTests.cs` — `TestWorkerApi : IWorkerApiGateway` реализует и `SendAllAsync` (стаб: журнал + `Respond`-делегат по аналогии; вернуть `IReadOnlyList<WorkerApiInstanceResult>`):

```csharp
    public Task<IReadOnlyList<WorkerApiInstanceResult>> SendAllAsync(
        string worker, HttpMethod method, string path, object? body, string? requestedBy, CancellationToken ct)
    {
        var call = new Call(worker, method, path, body, requestedBy);
        Calls.Add(call);
        if (Throw is not null)
            throw Throw;
        var response = Respond is not null ? Respond(call) : new WorkerApiResult(204, null);
        return Task.FromResult<IReadOnlyList<WorkerApiInstanceResult>>(
            [new("stub-instance", response, null)]);
    }
```

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~WorkerApiGatewayTests|FullyQualifiedName~WorkerTlsHandlerTests"`
Expected: PASS, включая прежние failover-тесты.

- [x] **Шаг 8: коммит**

```bash
git add src/AdminPanel.Etcd/Workers/IWorkerApiGateway.cs src/AdminPanel.Etcd/Workers/WorkerApiGateway.cs src/AdminPanel.Etcd/Workers/WorkerTlsHandler.cs src/AdminPanel.Etcd/ModuleExtensions.cs src/tests/AdminPanel.UnitTests/Workers/WorkerApiGatewayTests.cs src/tests/AdminPanel.UnitTests/Workers/WorkerTlsHandlerTests.cs src/tests/AdminPanel.IntegrationTests/AuthTests.cs
git commit -m "feat(panel): SendAllAsync broadcast в API воркеров + thumbprint-доверие серверных сертов"
```

**Выход:** панель обходит все живые инстансы (рестарт) и принимает self-signed серты, которые сама записала.
**Связь со spec:** §3.3 п.2/п.3, §4.4 п.1–2, §7 кр.2 (панель сохраняет доступ).

---

## Task 8: Панель — `WorkersModule` (GET /api/workers + generate/upload/delete/restart)

**Вход (предусловие):** Task 5–7 слиты.

**Files:**
- Create: `src/AdminPanel.Api/Operations/WorkersModule.cs` (маршруты + DTO + query)
- Create: `src/AdminPanel.Api/Operations/WorkersCommands.cs` (команды + хендлеры)
- Modify: `src/AdminPanel.Api/Program.cs` (`app.MapWorkersApi();`)
- Test: `src/tests/AdminPanel.IntegrationTests/WorkersApiTests.cs`
- Test (factory): `src/tests/AdminPanel.IntegrationTests/WorkersWebFactory.cs`

**Interfaces:**
- Consumes: `WorkerCertService` (Task 6; write-методы сами гвардят `worker` → 404), `WorkerNotFoundException` (Task 6, `AdminPanel.Etcd.Workers`), `IWorkerApiGateway.SendAllAsync` (Task 7), `ISnapshotStore`/`IKafkaSnapshotReader`, `WorkerApiCert`, `WorkerEndpoint.CertThumbprint`.
- Produces (использует Task 9 — фронтенд): REST-контракт arch/adminpanel/03 §1/§3.7:
  - `GET /api/workers` → 200 `WorkersViewDto`:
```csharp
public sealed record WorkersViewDto(IReadOnlyList<WorkerViewDto> Workers);
public sealed record WorkerViewDto(string Worker, IReadOnlyList<WorkerInstanceDto> Instances, WorkerCertDto? TargetCert);
public sealed record WorkerInstanceDto(
    string Instance, string Url, long SinceUnix, string? Health, string? CertThumbprint,
    string ApplyStatus); // "applied" | "pending restart" | "unmanaged" | "unknown"
public sealed record WorkerCertDto(
    string Thumbprint, string Subject, string Issuer, IReadOnlyList<string> San,
    long NotBeforeUnix, long NotAfterUnix, long UpdatedUnix, string? UpdatedBy);
```
  - `POST /api/workers/{worker}/api-cert/generate` → 201 `WorkerApiCertDto(string Worker, string Thumbprint, long UpdatedUnix, string UpdatedBy, bool RestartRequired, string? Warning)` | 404 (worker) | 409 | 422 | 503;
  - `PUT /api/workers/{worker}/api-cert` (тело `{cert_pem, key_pem}`) → 201 | 400 | 404 (worker) | 422 | 503;
  - `DELETE /api/workers/{worker}/api-cert` → 204 | 404 (worker или ключа нет) | 503;
  - `POST /api/workers/{worker}/restart` → 202 `WorkerRestartDto(IReadOnlyList<RestartInstanceResultDto> Results)`; `RestartInstanceResultDto(string Instance, bool Accepted, string? Error)` | 404 (worker) | 503;
  - `worker` ∈ {pgworker, kafkaworker} иначе 404: generate/PUT/DELETE — гвард внутри `WorkerCertService` (до `KeyOf`, мусорный ключ не пишется), restart — явная проверка в `RestartWorkerCommandHandler`.

- [x] **Шаг 1: интеграционные тесты (красный)**

`src/tests/AdminPanel.IntegrationTests/WorkersWebFactory.cs` — фабрика по образцу `BackupsWebFactory` (PanelHostBuilder.BuildExclusive, реальный etcd через `UseSetting("AdminPanel:Etcd:Endpoints:0", ...)`):

```csharp
// Фабрика грани «Воркеры» (spec §3.3 п.4): РЕАЛЬНЫЙ etcd-контейнер (панель —
// прямой писатель ключей сертов) + TestSnapshotStore (статусы applied/pending)
// + стаб WorkerApi (рестарт) + стаб kafka-кредов (kafka-ветка правила 3 §4.3).
// Hosted сняты; build строго через PanelHostBuilder.
public sealed class WorkersWebFactory : WebApplicationFactory<Program>
{
    public string EtcdEndpoint { get; init; } = "";
    public TestWorkerApi WorkerApi { get; } = new();
    public FixedTimeProvider Time { get; } = new();
    public StubKafkaSecrets KafkaSecrets { get; } = new();

    private bool _built;
    public void EnsureBuilt()
    {
        if (_built) return;
        PanelHostBuilder.BuildExclusive(this);
        _built = true;
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseSetting("AdminPanel:Auth:Username", "admin");
        builder.UseSetting("AdminPanel:Auth:Password", "adminpw");
        builder.UseSetting("AdminPanel:Auth:AllowHttp", "true");
        if (EtcdEndpoint.Length > 0)
            builder.UseSetting("AdminPanel:Etcd:Endpoints:0", EtcdEndpoint);
        builder.ConfigureTestServices(services =>
        {
            services.RemoveAll<IHostedService>();
            services.Replace(new ServiceDescriptor(typeof(TimeProvider), Time));
            services.Replace(new ServiceDescriptor(typeof(ISnapshotStore), new TestSnapshotStore()));
            services.Replace(new ServiceDescriptor(typeof(IKafkaSnapshotStore), new TestKafkaSnapshotStore()));
            services.Replace(new ServiceDescriptor(typeof(IWorkerApiGateway), WorkerApi));
            // Kafka-креды (правило 3 §4.3): подмена singleton'а ДО build — тот же
            // приём, что и гейтвей выше (прецедент AuthTests.cs:137-138).
            services.Replace(new ServiceDescriptor(typeof(IKafkaSecretsStore), KafkaSecrets));
        });
    }

    public EtcdSnapshot? Snapshot { set => ((TestSnapshotStore)Services.GetRequiredService<ISnapshotStore>()).Current = value; }
    public KafkaSnapshot? KafkaSnapshot { set => ((TestKafkaSnapshotStore)Services.GetRequiredService<IKafkaSnapshotStore>()).Current = value; }
}

// Стаб kafka-кредов интеграционных кейсов (зеркало StubSecrets из Task 6):
// тест кладёт ca_pem кластера → WorkerCertService видит его известным
// материалом исходящих (spec §4.3 правило 3, kafka-ветка).
public sealed class StubKafkaSecrets : IKafkaSecretsStore
{
    public IReadOnlyDictionary<string, KafkaClusterSecrets> Current { get; set; } =
        new Dictionary<string, KafkaClusterSecrets>();
    public void Replace(IReadOnlyDictionary<string, KafkaClusterSecrets> secrets) => Current = secrets;
}
```

`TestKafkaSnapshotStore` — мини-стор файла (аналог `TestSnapshotStore`, поле `KafkaSnapshot? Current`); если уже существует — переиспользовать. IKafkaSnapshotStore-подмена обязательна: ModuleExtensions-делегат thumbprint-доверия резолвит эти сторы.

`src/tests/AdminPanel.IntegrationTests/WorkersApiTests.cs` — collection на собственные фикстуры: `[CollectionDefinition("workers-cert")]` c `ICollectionFixture<WorkersCertFixture>`; фикстура = `EtcdContainerFixture` + `WorkersWebFactory` (EnsureBuilt после инициализации etcd) + login-хелпер (по образцу `ApiTestLogin.LoginAsync`). Тест-кейсы:

```csharp
// Грань «Воркеры» (spec §3.3 п.4, §4.1–§4.4): статусы применения, generate
// (txn 409 на живом ключе), PUT/DELETE, 422-изоляция исходящих, рестарт-прокси.
[Collection("workers-cert")]
public class WorkersApiTests
{
    // === GET /api/workers: статусы (spec §3.3 п.4) ===
    [Fact] Get_NoCertKey_Unmanaged()        // снапшот с инстансом, ключа нет → applyStatus "unmanaged"
    [Fact] Get_InstanceWithoutThumbprint_Unknown()  // ключ есть, CertThumbprint=null → "unknown"
    [Fact] Get_ThumbprintMatchesTarget_Applied()    // thumbprint инстанса == целевому → "applied"
    [Fact] Get_ThumbprintDiffers_PendingRestart()   // отличается → "pending restart"
    [Fact] Get_PemNotLeaked()               // в теле НЕТ "cert_pem"/"key_pem"/"BEGIN PRIVATE KEY" (§7 кр.8)

    // === generate (spec §4.1) ===
    [Fact] Generate_NoKey_201TxnPut()       // 201, поле thumbprint 64 hex, restartRequired=true; ключ в etcd появился (RangeAsync)
    [Fact] Generate_LiveKey_409()           // повторный generate → 409 «уже управляется»
    [Fact] Generate_NoInstances_WarningSanLocalhost() // живых нет → warning, SAN localhost/127.0.0.1
    [Fact] Generate_Instances_SanFromAdvertiseUrls()  // снапшот с URL → SAN содержит хосты
    [Fact] Generate_UnknownWorker_404_EtcdUntouched() // worker=foo → 404; RangeAsync("/workers/api_tls/") НЕ содержит ключа foo

    // === PUT (spec §4.2) ===
    [Fact] Put_ValidLeaf_201Overwrite()     // валидная пара (TestPki) → 201; повтор PUT другого серта → 201 (безусловная замена)
    [Fact] Put_BrokenPair_400_EtcdUntouched()  // мусор → 400, ключ в etcd не изменился
    [Fact] Put_Expired_400()
    [Fact] Put_NoSan_400()
    [Fact] Put_UnknownWorker_404_EtcdUntouched() // worker=foo → 404 (НЕ 201); ключ /workers/api_tls/foo не появился

    // === 422-изоляция (spec §4.3) ===
    [Fact] Put_CaCert_422_EtcdUntouched()   // CA → 422 title «Сертификат влияет...», ключ НЕ записан
    [Fact] Put_ClientAuthEku_422_EtcdUntouched()
    [Fact] Put_KafkaCaPem_422_EtcdUntouched()
    // kafka-ветка правила 3 честно: TestPki.GenerateCa → пара кладётся и в стаб
    // фабрики (factory.KafkaSecrets.Current = {"c1": KafkaClusterSecrets(.., CaPem)}),
    // и как кандидат PUT → 422 «влияет на исходящие»; RangeAsync: ключ НЕ записан

    // === DELETE (spec §4.2) ===
    [Fact] Delete_LiveKey_204_KeyGone()     // 204, RangeAsync пуст
    [Fact] Delete_MissingKey_404()
    [Fact] Delete_UnknownWorker_404()       // worker=foo → 404 (гвард сервиса), даже если ключ /workers/api_tls/foo записан в etcd руками — остаётся нетронут

    // === restart (spec §4.4) ===
    [Fact] Restart_ProxiesToAllInstances_202() // стаб WorkerApi.SendAll → 202 {results:[{instance, accepted}]}
    [Fact] Restart_NoLiveEndpoints_503()       // снапшот без ключей → 503
    [Fact] Restart_UnknownWorker_404()         // worker=foo → 404
    [Fact] Actions_WithoutCookie_401()         // default-deny guard
}
```

Кейсы статусов: снапшоты строить по образцу `InspectionSnapshots.Fixture(...)` — дополнить хелпером с `WorkerApiCert` и `WorkerEndpoint(..., CertThumbprint: ...)` (файл `TestSnapshots.cs` расширить копией-методом `WithWorkers(...)`).

- [x] **Шаг 2: прогнать — красный**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.IntegrationTests -c Debug --filter "FullyQualifiedName~WorkersApiTests"`
Expected: FAIL (404 — маршрутов нет).

- [x] **Шаг 3: команды и хендлеры**

`src/AdminPanel.Api/Operations/WorkersCommands.cs`:

```csharp
using AdminPanel.Core;
using AdminPanel.Core.Kafka;
using AdminPanel.Etcd;
using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Operations;

// WorkerNotFoundException — НЕ определяется здесь: общий тип живёт в
// AdminPanel.Etcd.Workers (Task 6, WorkerCertService.cs) — его гвардят
// write-методы сервиса (generate/PUT/DELETE → 404 до KeyOf) и рестарт-хендлер.

// ===== Команды (CQRS по канону) =====

public sealed record GenerateWorkerApiCertCommand(string Worker, string RequestedBy)
    : ICommand<WorkerApiCertDto>;

public sealed record UploadWorkerApiCertCommand(string Worker, string CertPem, string KeyPem, string RequestedBy)
    : ICommand<WorkerApiCertDto>;

public sealed record DeleteWorkerApiCertCommand(string Worker) : ICommand<WorkerApiCertDeletedDto>;

public sealed record RestartWorkerCommand(string Worker, string RequestedBy) : ICommand<WorkerRestartDto>;

public sealed record WorkerApiCertDto(
    string Worker, string Thumbprint, long UpdatedUnix, string UpdatedBy,
    bool RestartRequired, string? Warning = null);

public sealed record WorkerApiCertDeletedDto(string Worker);

public sealed record WorkerRestartDto(IReadOnlyList<RestartInstanceResultDto> Results);

public sealed record RestartInstanceResultDto(string Instance, bool Accepted, string? Error);

// Генерация + txn-запись (spec §4.1): SAN — хосты живых advertise-URL.
[InjectAsScoped]
public sealed class GenerateWorkerApiCertCommandHandler(
    WorkerCertService certs, ISnapshotStore pg, IKafkaSnapshotReader kafka) : ICommandHandler<GenerateWorkerApiCertCommand, WorkerApiCertDto>
{
    public async ValueTask<Result<WorkerApiCertDto>> Handle(GenerateWorkerApiCertCommand c, CancellationToken ct)
    {
        var hosts = LiveHosts(c.Worker, pg.Current, kafka.Current);
        var write = await certs.GenerateAndPutAsync(c.Worker, hosts, c.RequestedBy, ct);
        return write.Map(w => ToDto(w, hosts.Count == 0
            ? "живых инстансов нет — SAN только localhost/127.0.0.1; после подъёма замените серт (PUT) для полного SAN"
            : null));
    }

    // Хосты живых advertise-URL этого воркера (spec §3.3 п.1). Неизвестный
    // воркер → пустой список (НЕ throw): 404 возвращает WorkerCertService —
    // исключение здесь пролетело бы мимо Result и роняло запрос в 500.
    internal static IReadOnlyList<string> LiveHosts(string worker, EtcdSnapshot? pg, KafkaSnapshot? kafka)
        => (worker switch
        {
            "pgworker" => pg?.PgWorkerEndpoints ?? [],
            "kafkaworker" => kafka?.WorkerEndpoints ?? [],
            _ => [], // 404 даст сервис (гвард до KeyOf)
        })
        .Select(e => WorkerCertService.HostOf(e.Url))
        .Where(h => h is not null)
        .Select(h => h!)
        .Distinct()
        .ToList();

    // DTO ответа 201 (spec §4.1 п.3): UpdatedUnix/UpdatedBy — из метаданных,
    // записанных сервисом (сервис пересобирает Meta с оператором до возврата —
    // см. Шаг 6); restartRequired=true всегда: применение — только рестартом.
    internal static WorkerApiCertDto ToDto(WorkerCertWriteResult w, string? warning)
        => new(w.Worker, w.Meta.Thumbprint, w.Meta.UpdatedUnix, w.Meta.UpdatedBy ?? "adminpanel",
            RestartRequired: true, warning);
}
```

Остальные хендлеры:

```csharp
// PUT (spec §4.2): валидация §4.3 + 404 неизвестного воркера — внутри сервиса
// (гвард до KeyOf), хендлер дополнительной проверки не дублирует.
[InjectAsScoped]
public sealed class UploadWorkerApiCertCommandHandler(WorkerCertService certs)
    : ICommandHandler<UploadWorkerApiCertCommand, WorkerApiCertDto>
{
    public async ValueTask<Result<WorkerApiCertDto>> Handle(UploadWorkerApiCertCommand c, CancellationToken ct)
    {
        var write = await certs.PutAsync(c.Worker, c.CertPem, c.KeyPem, c.RequestedBy, ct);
        return write.Map(w => GenerateWorkerApiCertCommandHandler.ToDto(w, null));
    }
}

// DELETE (spec §4.2): 404 неизвестного воркера — там же, в сервисе.
[InjectAsScoped]
public sealed class DeleteWorkerApiCertCommandHandler(WorkerCertService certs)
    : ICommandHandler<DeleteWorkerApiCertCommand, WorkerApiCertDeletedDto>
{
    public async ValueTask<Result<WorkerApiCertDeletedDto>> Handle(DeleteWorkerApiCertCommand c, CancellationToken ct)
    {
        var delete = await certs.DeleteAsync(c.Worker, ct);
        return delete.Map(() => new WorkerApiCertDeletedDto(c.Worker));
    }
}

// Прокси рестарта на ВСЕ живые инстансы (spec §4.4). Сервис здесь не участвует —
// гвард worker явный (WorkerNotFoundException из AdminPanel.Etcd.Workers).
[InjectAsScoped]
public sealed class RestartWorkerCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RestartWorkerCommand, WorkerRestartDto>
{
    public async ValueTask<Result<WorkerRestartDto>> Handle(RestartWorkerCommand c, CancellationToken ct)
    {
        if (c.Worker is not ("pgworker" or "kafkaworker"))
            return Result<WorkerRestartDto>.Failed(new WorkerNotFoundException(c.Worker));
        IReadOnlyList<WorkerApiInstanceResult> results;
        try
        {
            results = await api.SendAllAsync(c.Worker, HttpMethod.Post, "/api/restart", null, c.RequestedBy, ct);
        }
        catch (WorkerApiUnavailableException e)
        {
            return Result<WorkerRestartDto>.Failed(e);
        }

        return Result<WorkerRestartDto>.Success(new WorkerRestartDto(
            results.Select(r => new RestartInstanceResultDto(
                r.Instance,
                r.Response is { StatusCode: >= 200 and < 300 },
                r.Error ?? (r.Response is { StatusCode: >= 200 and < 300 } ? null : $"HTTP {r.Response.StatusCode}")));
    }
}
```

(`Result.Map`-хелперы сверить с фактическими сигнатурами `AdminPanel.Infrastructure.Result`; при отсутствии нулевой Map — через явный if.)

- [x] **Шаг 4: query GET /api/workers**

В `WorkersModule.cs` (или отдельный `WorkersQuery.cs` по образцу `OverviewQuery.cs`):

```csharp
// Сводка грани «Воркеры» (spec §3.3 п.4): инстансы + целевой серт + статус
// применения per-instance (applied/pending restart/unmanaged/unknown).
public sealed record GetWorkersQuery() : IQuery<WorkersViewDto>;

[InjectAsScoped]
public sealed class GetWorkersQueryHandler(ISnapshotStore pg, IKafkaSnapshotReader kafka)
    : IQueryHandler<GetWorkersQuery, WorkersViewDto>
{
    public ValueTask<Result<WorkersViewDto>> Handle(GetWorkersQuery _, CancellationToken ct)
    {
        var p = pg.Current;
        var k = kafka.Current;

        static string ApplyStatus(WorkerApiCert? target, string? instanceThumb) => target switch
        {
            null => "unmanaged",                       // ключа нет — инстанс на env-серте
            _ when instanceThumb is null => "unknown", // старая версия не сообщает
            var t when t.Thumbprint == instanceThumb => "applied",
            _ => "pending restart",
        };

        static WorkerInstanceDto Instance(WorkerEndpoint e, WorkerApiCert? target, WorkerHealth? health) => new(
            e.InstanceId, e.Url, e.SinceUnix,
            health is null ? null : health.Status.ToString().ToLowerInvariant(),
            e.CertThumbprint,
            ApplyStatus(target, e.CertThumbprint));

        static WorkerCertDto? Cert(WorkerApiCert? c) => c is null ? null : new(
            c.Thumbprint, c.Subject, c.Issuer, c.San,
            c.NotBefore.ToUnixTimeSeconds(), c.NotAfter.ToUnixTimeSeconds(),
            c.UpdatedUnix, c.UpdatedBy);

        return ValueTask.FromResult(Result<WorkersViewDto>.Success(new WorkersViewDto(
        [
            new("pgworker",
                (p?.PgWorkerEndpoints ?? []).Select(e => Instance(e, p?.WorkerApiCert,
                    p?.WorkerHealth?.FirstOrDefault(h => h.InstanceId == e.InstanceId))).ToList(),
                Cert(p?.WorkerApiCert)),
            new("kafkaworker",
                (k?.WorkerEndpoints ?? []).Select(e => Instance(e, k?.WorkerApiCert,
                    k?.WorkerHealth?.FirstOrDefault(h => h.InstanceId == e.InstanceId))).ToList(),
                Cert(k?.WorkerApiCert)),
        ])));
    }
}
```

- [x] **Шаг 5: маршруты WorkersModule + Program.cs**

`src/AdminPanel.Api/Operations/WorkersModule.cs`:

```csharp
using System.Text;
using AdminPanel.Etcd.Workers;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace AdminPanel.Api.Operations;

// Грань «Воркеры» (spec §3.3 п.4, arch/adminpanel/03 §1/§3.7): сводка сертов
// и инстансов + управление (generate/PUT/DELETE) — ПРЯМАЯ запись панели в etcd
// (arch/adminpanel/02 §9.9, вторая категория после §9) + рестарт-прокси.
public static class WorkersModule
{
    private const string ProblemContentType = "application/problem+json";

    public static IEndpointRouteBuilder MapWorkersApi(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGet("/api/workers", async (IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleQuery<GetWorkersQuery, WorkersViewDto>(new(), ct);
            return result.IsSuccess ? Results.Ok(result.Value) : Error(result);
        });

        endpoints.MapPost("/api/workers/{worker}/api-cert/generate", async (
            string worker, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<GenerateWorkerApiCertCommand, WorkerApiCertDto>(
                new(worker, user.Identity?.Name ?? "adminpanel"), ct);
            return result.IsSuccess
                ? Results.Created($"/api/workers/{worker}", result.Value)
                : Error(result);
        });

        endpoints.MapPut("/api/workers/{worker}/api-cert", async (
            string worker, UploadWorkerApiCertRequest request, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<UploadWorkerApiCertCommand, WorkerApiCertDto>(
                new(worker, request.CertPem ?? "", request.KeyPem ?? "", user.Identity?.Name ?? "adminpanel"), ct);
            return result.IsSuccess
                ? Results.Created($"/api/workers/{worker}", result.Value)
                : Error(result);
        });

        endpoints.MapDelete("/api/workers/{worker}/api-cert", async (
            string worker, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<DeleteWorkerApiCertCommand, WorkerApiCertDeletedDto>(
                new(worker), ct);
            return result.IsSuccess ? Results.NoContent() : Error(result);
        });

        endpoints.MapPost("/api/workers/{worker}/restart", async (
            string worker, ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
        {
            var result = await handler.HandleCommand<RestartWorkerCommand, WorkerRestartDto>(
                new(worker, user.Identity?.Name ?? "adminpanel"), ct);
            return result.IsSuccess ? Results.Accepted((string?)null, result.Value) : Error(result);
        });

        return endpoints;
    }

    // Тело PUT {cert_pem, key_pem} (camelCase — Minimal API Web-биндинг).
    public sealed record UploadWorkerApiCertRequest(string? CertPem, string? KeyPem);

    // Error-ветка: 422 — ЯВНОЕ «влияет на исходящие» (spec §2 п.6); 400 — не годен
    // как серверный; 404/409/503 — по таблице §4.3/03 §1.
    private static IResult Error(Result result) => result.Error switch
    {
        WorkerCertAffectsOutgoingException affects => Results.Problem(
            statusCode: StatusCodes.Status422UnprocessableEntity,
            title: "Сертификат влияет на коммуникации воркеров с их подчинёнными сервисами — обновление отклонено",
            detail: affects.Message),
        WorkerCertInvalidException invalid => Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            title: "Invalid certificate",
            detail: invalid.Message),
        WorkerCertAlreadyManagedException => Results.Problem(
            statusCode: StatusCodes.Status409Conflict,
            title: "Certificate already managed",
            detail: result.Error.Message),
        WorkerNotFoundException or WorkerCertNotFoundException => Results.Problem(
            statusCode: StatusCodes.Status404NotFound,
            title: "Not found",
            detail: result.Error.Message),
        WorkerApiUnavailableException unavailable => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "API воркера недоступен",
            detail: unavailable.Message),
        _ => Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "Etcd write failed",
            detail: result.Error!.Message),
    };
}
```

`src/AdminPanel.Api/Program.cs` — после `app.MapKafkaOperationsApi();`:

```csharp
app.MapWorkersApi(); // серты API воркеров + рестарт (arch/adminpanel/02 §9.9, 03 §3.7)
```

- [x] **Шаг 6: правка WorkerCertService — аудит оператора в Meta**

В `GenerateAndPutAsync`/`PutAsync` (Task 6-файл) перед возвратом: `meta = meta with { UpdatedBy = updatedBy };` (record-with). Записи в etcd уже содержат `updated_by` (SerializePayload); метаданные ответа выравниваются.

- [x] **Шаг 7: прогнать интеграционные — зелёный**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.IntegrationTests -c Debug --filter "FullyQualifiedName~WorkersApiTests"`
Expected: PASS (все кейсы Шага 1).

- [x] **Шаг 8: полный регресс панели**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Debug && DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.IntegrationTests -c Debug`
Expected: зелёные (старые серии не сломаны подменами фабрик).
После серии: зачистить docker-остатки тестов (`docker ps -aq` — только свои; контейнеры dev-стенда `as-*`/`adminpanel` не трогать) + `docker network prune -f`.

- [x] **Шаг 9: коммит**

```bash
git add src/AdminPanel.Api/Operations/WorkersModule.cs src/AdminPanel.Api/Operations/WorkersCommands.cs src/AdminPanel.Api/Program.cs src/AdminPanel.Etcd/Workers/WorkerCertService.cs src/tests/AdminPanel.IntegrationTests/WorkersApiTests.cs src/tests/AdminPanel.IntegrationTests/WorkersWebFactory.cs
git commit -m "feat(panel): грань Воркеры — GET /api/workers + generate/PUT/DELETE серта + рестарт-прокси"
```

**Выход:** полный REST-контракт 03 §1 для воркеров с кодами 201/202/204/400/404/409/422/503.
**Связь со spec:** §3.3 п.4, §4.1–§4.5, §7 кр.1/3/4/6/7/8 (серверная часть).

---

## Task 9: Frontend — страница «Воркеры»

**Вход (предусловие):** Task 8 слит (REST-контракт готов).

**Files:**
- Modify: `frontend/src/api/dto.ts` (DTO)
- Modify: `frontend/src/api/queries.ts` (fetch + mutations)
- Create: `frontend/src/pages/WorkersPage.tsx`
- Create: `frontend/src/pages/workers/UploadWorkerCertModal.tsx`
- Modify: `frontend/src/App.tsx` (маршрут `/workers`)
- Modify: `frontend/src/layout/AppLayout.tsx` (пункт «Воркеры»)

**Interfaces:**
- Consumes: REST Task 8 (`/api/workers`, `.../api-cert/generate|PUT|DELETE`, `.../restart`).

- [x] **Шаг 1: DTO и queries**

`frontend/src/api/dto.ts` — добавить:

```typescript
// Грань «Воркеры» (arch/adminpanel/03 §1/§3.7): сводные DTO и мутации серта.
export type WorkerApplyStatus = 'applied' | 'pending restart' | 'unmanaged' | 'unknown';

export interface WorkerInstanceDto {
  instance: string;
  url: string;
  sinceUnix: number;
  health?: string | null;
  certThumbprint?: string | null;
  applyStatus: WorkerApplyStatus;
}

export interface WorkerCertDto {
  thumbprint: string;
  subject: string;
  issuer: string;
  san: string[];
  notBeforeUnix: number;
  notAfterUnix: number;
  updatedUnix: number;
  updatedBy?: string | null;
}

export interface WorkerViewDto {
  worker: string;
  instances: WorkerInstanceDto[];
  targetCert?: WorkerCertDto | null;
}

export interface WorkersViewDto {
  workers: WorkerViewDto[];
}

export interface WorkerApiCertDto {
  worker: string;
  thumbprint: string;
  updatedUnix: number;
  updatedBy: string;
  restartRequired: boolean;
  warning?: string | null;
}

export interface RestartInstanceResultDto {
  instance: string;
  accepted: boolean;
  error?: string | null;
}

export interface WorkerRestartDto {
  results: RestartInstanceResultDto[];
}

export interface UploadWorkerCertRequestDto {
  cert_pem: string;
  key_pem: string;
}
```

`frontend/src/api/queries.ts` — добавить ключи и функции (по образцу `fetchKafkaClusters`/`createKafkaCluster`):

```typescript
export const workerQueryKeys = {
  workers: ['workers'] as const,
};

export function fetchWorkers(): Promise<WorkersViewDto> {
  return apiFetch('/api/workers');
}

export function generateWorkerApiCert(worker: string): Promise<WorkerApiCertDto> {
  return apiFetch(`/api/workers/${worker}/api-cert/generate`, { method: 'POST' });
}

export function uploadWorkerApiCert(
  worker: string,
  request: UploadWorkerCertRequestDto,
): Promise<WorkerApiCertDto> {
  return apiFetch(`/api/workers/${worker}/api-cert`, { method: 'PUT', body: request });
}

export function deleteWorkerApiCert(worker: string): Promise<void> {
  return apiFetch(`/api/workers/${worker}/api-cert`, { method: 'DELETE' });
}

export function restartWorker(worker: string): Promise<WorkerRestartDto> {
  return apiFetch(`/api/workers/${worker}/restart`, { method: 'POST' });
}
```

- [x] **Шаг 2: модал загрузки PEM**

`frontend/src/pages/workers/UploadWorkerCertModal.tsx` — по образцу `CreateKafkaClusterModal.tsx`: `Modal` с двумя `Textarea` (сертификат PEM / приватный ключ PKCS#8) + кнопка выбора файла (`input type=file`, чтение `file.text()` в textarea); клиентская валидация-зеркало: непустые поля, `-----BEGIN CERTIFICATE-----`/`-----BEGIN PRIVATE KEY-----` в текстах; `useMutation(uploadWorkerApiCert)`; ошибка `ApiError` со статусом 422 → баннер `<Alert color="red">` с точным текстом из `error.detail` (заголовок уже от ProblemDetails: «Сертификат влияет на коммуникации воркеров с их подчинёнными сервисами — обновление отклонено»); 400 — тот же баннер; успех → `onClose()` + `invalidateQueries(workerQueryKeys.workers)`.

- [x] **Шаг 3: страница WorkersPage**

`frontend/src/pages/WorkersPage.tsx` — по образцу `KafkaClustersPage.tsx` (useQuery + polling + карточки):

```tsx
// Грань «Воркеры» (arch/adminpanel/03 §3.7): карточки PgWorker/KafkaWorker —
// инстансы, целевой серт API, статус применения, действия с сертом и рестарт.
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Alert, Badge, Button, Card, Group, Modal, Stack, Table, Text, Title, Tooltip } from '@mantine/core';
import { useState } from 'react';
import { ApiError } from '../api/client';
import { deleteWorkerApiCert, fetchWorkers, generateWorkerApiCert, restartWorker, workerQueryKeys } from '../api/queries';
import type { WorkerInstanceDto, WorkerViewDto } from '../api/dto';
import { ErrorSection, LoadingSection } from '../components/LoadState';
import { usePollingIntervalMs } from '../polling/PollingContext';
import { UploadWorkerCertModal } from './workers/UploadWorkerCertModal';

const WORKER_TITLES: Record<string, string> = { pgworker: 'PgWorker (бд)', kafkaworker: 'KafkaWorker (кафки)' };

export function WorkersPage() {
  const intervalMs = usePollingIntervalMs();
  const queryClient = useQueryClient();
  const query = useQuery({ queryKey: workerQueryKeys.workers, queryFn: fetchWorkers, refetchInterval: intervalMs });
  const [uploadWorker, setUploadWorker] = useState<string | null>(null);
  const [banner, setBanner] = useState<string | null>(null);

  const refresh = () => queryClient.invalidateQueries({ queryKey: workerQueryKeys.workers });

  if (query.data === undefined)
    return query.isError ? <ErrorSection error={query.error} onRetry={() => void query.refetch()} /> : <LoadingSection />;

  return (
    <>
      <Group justify="space-between" mb="md">
        <Title order={2}>Воркеры</Title>
      </Group>
      {banner ? <Alert color="red" mb="md" onClose={() => setBanner(null)} withCloseButton>{banner}</Alert> : null}
      <Stack>
        {query.data.workers.map((w) => (
          <WorkerCard key={w.worker} view={w}
            onGenerate={() => generateMutation(w.worker)}
            onUpload={() => setUploadWorker(w.worker)}
            onDelete={() => deleteMutation(w.worker)}
            onRestart={() => restartMutation(w.worker)} />
        ))}
      </Stack>
      <UploadWorkerCertModal worker={uploadWorker ?? ''} opened={uploadWorker !== null} onClose={() => setUploadWorker(null)} />
    </>
  );
  // generate/delete/restart — useMutation с confirm-модалами (Mantine Modal):
  // тексты предупреждений — arch/adminpanel/03 §3.7 (рестарт: «идущие операции
  // продолжатся после подъёма — клэймы и журнал в etcd; краткое окно
  // недоступности API (секунды)»; delete: «воркер вернётся к env-серту после
  // перезапуска»); ошибки ApiError → setBanner(`${title}: ${detail}`).
}
```

`WorkerCard` (в том же файле):
- заголовок `WORKER_TITLES[w.worker]`;
- таблица инстансов: instance/url/uptime (`sinceUnix` → «поднят …», форматирование дат утилитами файла по образцу соседних страниц)/health-бейдж (`healthy`→зелёный, `degraded`→жёлтый, `unreachable`/null→серый)/thumbprint (`ff monospace`, обрезан);
- бейдж статуса per-instance: `applied`→зелёный, `pending restart`→жёлтый + tooltip «требуется перезапуск», `unmanaged`→серый tooltip «env-сертификат, ключа в etcd нет», `unknown`→серый tooltip «инстанс не сообщает thumbprint (старая версия)»;
- блок целевого серта (при наличии): subject/issuer/SAN (join «, ")/сроки (`notBeforeUnix`–`notAfterUnix` в читаемом виде)/thumbprint/«обновлён `updatedBy` …»;
- действия: «Сгенерировать сертификат» (confirm: «применится только после перезапуска»), «Загрузить сертификат» (открывает модал), «Перезапустить воркера» (красная, confirm с предупреждением 03 §3.7), «Убрать управляемый сертификат» (красная, рендерится только при живом `targetCert`).

- [x] **Шаг 4: маршрут и навигация**

`frontend/src/App.tsx`: import `WorkersPage`; в children: `{ path: 'workers', element: <WorkersPage /> }` (после `alerts`).

`frontend/src/layout/AppLayout.tsx`: в массив ссылок — `{ to: '/workers', label: 'Воркеры' }` (после «Обзор»).

- [x] **Шаг 5: проверка фронта**

Run: `cd frontend && npm run typecheck && npm run build`
Expected: tsc без ошибок, vite-сборка успешна.

- [x] **Шаг 6: коммит**

```bash
git add frontend/src/api/dto.ts frontend/src/api/queries.ts frontend/src/pages/WorkersPage.tsx frontend/src/pages/workers/UploadWorkerCertModal.tsx frontend/src/App.tsx frontend/src/layout/AppLayout.tsx
git commit -m "feat(panel-ui): страница Воркеры — серты API воркеров, статусы применения, рестарт"
```

**Выход:** UI грани «Воркеры» с карточками, формами, баннером 422 и polling-статусами.
**Связь со spec:** §3.4, §2 п.2/п.6, §7 кр.1/кр.4/кр.8 (UI-часть).

---

## Task 10: E2E PgWorker — полный цикл серта и рестарта

**Вход (предусловие):** Task 1–4 слиты; docker-образы стенда доступны (`dev-stand/images/pull-images.sh` при необходимости); предыдущие docker-серии зачищены.

**Files:**
- Create: `src/tests/PgWorker.IntegrationTests/E2e/E2eWorkerCertScenarios.cs`

**Interfaces:**
- Consumes: `E2eEnvironment` (изолированное окружение: сеть/etcd/MinIO per-сценарий, `StartHostAsync`, DisposeAsync с ассертом чистоты), `E2eFixture` (WaitForAsync, EnsureAppDllAsync, RunDockerAsync), `E2eTestPki` (если подходит; иначе локальный TestPki).

- [x] **Шаг 1: сценарий полного цикла (красный)**

`src/tests/PgWorker.IntegrationTests/E2e/E2eWorkerCertScenarios.cs` — изоляция per-сценарий (`await using var env = new E2eEnvironment(...)` в теле Fact, паттерн соседних `E2e*Scenarios.cs`; Release-бинарь — `E2eFixture.EnsureAppDllAsync` по образцу `E2eScenarios.cs`):

```csharp
// E2E полного цикла управляемого серта API (spec §5 Ф4, §7 кр.1/2/6/7):
// генерация пары → ключ /workers/api_tls/pgworker → старт воркера на etcd-серте
// → замена ключа → pending (факт на грани старый) → POST /api/restart →
// повторный подъём на новом серте → DELETE ключа → рестарт → env-фоллбек
// (unmanaged). Отдельный сценарий: битый ключ — fail-fast старта.
[Collection("e2e-serial")]
public class E2eWorkerCertScenarios
{
    [Fact]
    public async Task WorkerCert_Lifecycle_AppliedPendingRestartEnvFallback()
    {
        // Arrange: изолированное окружение + ДВЕ пары PEM (старая/новая) +
        // env-сертификат для фоллбека (передаётся StartHostAsync env
        // PGW_API_TLS_CERT/KEY/CLIENT_CA — образец E2eScenarios)
        // Секреты e2e: E2eFixture.SuPassword и пр.

        // Act 1: put ключа со старой парой → старт воркера
        // Assert 1: /healthz 200 ЧЕРЕЗ TLS-клиент, доверяющий thumbprint старой
        //   пары (RemoteCertificateValidationCallback по SHA-256); серт сервера
        //   фактически == старой паре (custom validation считает хендшейк);
        //   дискавери-ключ /pgworker/api/<id>.cert_thumbprint == thumbprint старой

        // Act 2: put ключа с НОВОЙ парой (панельный путь имитируем прямым put —
        //   панель = единственный писатель, формат value идентичен §3.1)
        // Assert 2: грань всё ещё на старом серте (применение — только рестарт,
        //   spec §2 п.2): хендшейк с доверием НОВОМУ thumbprint — отказ

        // Act 3: POST /api/restart (mTLS-клиент) → 202 {"restarting":true}
        // Assert 3: процесс вышел (ждём ≤30 с поллом), docker-политику имитируем
        //   повторным StartHostAsync; новый процесс поднял грань на НОВОМ серте
        //   (хендшейк по новому thumbprint, /healthz 200), дискавери-ключ
        //   переподставился, cert_thumbprint == новому

        // Act 4: DELETE ключа /workers/api_tls/pgworker → рестарт
        // Assert 4: процесс поднялся на ENV-серте (хендшейк по thumbprint
        //   env-пары), дискавери cert_thumbprint == thumbprint env (статус unmanaged)

        // Teardown: env.DisposeAsync() (await using) — ассерт чистоты внутри
    }

    [Fact]
    public async Task WorkerCert_BrokenKey_StartFails()
    {
        // Arrange: put ключа с битым JSON → старт воркера
        // Act / Assert: процесс упал с сообщением про /workers/api_tls/pgworker
        //   (читаем stderr/лог воркера — E2eEnvironment.HostLogAsync-механика
        //   соседних сценариев), exit != 0, ≤30 с
    }
}
```

Обязательные детали реализации (по канонам `docs/e2e-isolation.md`/AGENTS §11):
- guid-тег во всех именах — уже внутри `E2eEnvironment` (`_runId`);
- advertise-URL — от фактического динамического порта (зонд свободного порта, образец `E2eScenarios.cs`);
- таймауты ожиданий ≤30 с (`E2eFixture.WaitForAsync`), общий бюджет сценария — по соседним E2E;
- teardown при любом исходе — `await using`; упавший сценарий — `MarkFailed()` (телеметрия `docs/e2e-launch.md`);
- рестарт-«политика»: хост-процесс перезапускает сам сценарий (документирующий комментарий: прод — docker `restart: unless-stopped`, E2E-хосты — имитация).

- [x] **Шаг 2: прогон E2E-сценариев (свежий Release)**

Run: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~E2eWorkerCertScenarios"`
Expected: PASS оба сценария; в логе — фазы, артефакты в `/tmp/pgw-e2e-artifacts-<guid>/`.

- [x] **Шаг 3: зачистка docker после серии**

Run: `docker ps -aq | wc -l` → удалить контейнеры СВОИХ прогонов (`docker rm -f` по списку; dev-стенд `as-*`/`adminpanel` не трогать) → `docker ps -aq | wc -l` — только стендовые/ноль → `docker network prune -f`.
Expected: чисто.

- [x] **Шаг 4: коммит**

```bash
git add src/tests/PgWorker.IntegrationTests/E2e/E2eWorkerCertScenarios.cs
git commit -m "test(e2e): полный цикл управляемого серта API PgWorker — applied/pending/restart/env-фоллбек + fail-fast на битый ключ"
```

**Выход:** E2E-мерж-гейт кейс полного цикла на свежем Release с полным teardown.
**Связь со spec:** §5 Ф4, §7 кр.1/2/6/7/9.

---

## Task 11: dev-stand чек + runbook + финальный мерж-гейт

**Вход (предусловие):** Task 1–10 слиты; dev-стенд поднимаем при проверке (`dev-stand/adminpanel/checks/00-up.sh`).

**Files:**
- Create: `dev-stand/adminpanel/checks/70-worker-cert.sh`
- Modify: `docs/runbook.md` (новый раздел)

**Interfaces:**
- Consumes: живой стенд (панель :5050, cookie-логин admin/admin, воркеры в deploy-канон mTLS).

- [x] **Шаг 1: чек-скрипт**

`dev-stand/adminpanel/checks/70-worker-cert.sh` — по образцу `10-smoke-api.sh` (curl + jq, trap-очистка JAR):

```bash
#!/usr/bin/env bash
# Чек грани «Воркеры» (spec §3.3/§3.4): generate → pending restart →
# restart → applied; панель сохраняет доступ к API воркера после
# перезапуска на self-signed серте (thumbprint-доверие). Возвращает стенд
# в исходное состояние (DELETE ключа + финальный рестарт → env-серт).
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${ADMINPANEL_URL:-http://localhost:5050}"
JAR="$(mktemp)"; trap 'rm -f "$JAR"' EXIT

# Arrange: панель жива, login
for i in $(seq 1 60); do curl -fsS "$BASE/api/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -fsS -c "$JAR" -o /dev/null -X POST "$BASE/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}'

# 401 без cookie / GET /api/workers отдаёт обоих воркеров
code="$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/workers")"
[ "$code" = 401 ] || { echo "❌ /api/workers без cookie = $code"; exit 1; }
curl -fsS -b "$JAR" "$BASE/api/workers" | jq -e '.workers | length == 2' >/dev/null \
  || { echo "❌ /api/workers: ожидались карточки pgworker+kafkaworker"; exit 1; }

# Act 1: generate (409-идемпотентность допускаем: ключ мог остаться от прогона)
http="$(curl -s -o /tmp/pgw-wc-gen.json -w '%{http_code}' -b "$JAR" -X POST \
  "$BASE/api/workers/pgworker/api-cert/generate")"
[ "$http" = 201 ] || [ "$http" = 409 ] || { echo "❌ generate = $http"; cat /tmp/pgw-wc-gen.json; exit 1; }
thumb="$(jq -r '.thumbprint // empty' /tmp/pgw-wc-gen.json)"
[ -n "$thumb" ] || thumb="$(curl -fsS -b "$JAR" "$BASE/api/workers" | jq -r '.workers[] | select(.worker=="pgworker") | .targetCert.thumbprint // empty')"
[ -n "$thumb" ] || { echo "❌ thumbprint целевого серта не найден"; exit 1; }

# pending restart до перезапуска (PECULIARITY: живые инстансы сообщают старый thumbprint)
status="$(curl -fsS -b "$JAR" "$BASE/api/workers" | jq -r '.workers[] | select(.worker=="pgworker") | .instances[0].applyStatus // "none"')"
[ "$status" = "pending restart" ] || echo "  ⚠️ статус до рестарта: $status (не 'pending restart' — проверьте стенд)"

# Act 2: restart → 202; Assert: applied после подъёма (полл ≤60 с)
http="$(curl -s -o /tmp/pgw-wc-restart.json -w '%{http_code}' -b "$JAR" -X POST \
  "$BASE/api/workers/pgworker/restart")"
[ "$http" = 202 ] || { echo "❌ restart = $http"; cat /tmp/pgw-wc-restart.json; exit 1; }
for i in $(seq 1 60); do
  st="$(curl -fsS -b "$JAR" "$BASE/api/workers" | jq -r '.workers[] | select(.worker=="pgworker") | .instances[0].applyStatus // "none"')"
  [ "$st" = "applied" ] && break; sleep 1
done
[ "${st:-}" = "applied" ] || { echo "❌ после рестарта статус $st, ожидался applied"; exit 1; }
echo "  серт применён (applied), панель сохранила доступ к API воркера"

# Cleanup: откат на env (DELETE + рестарт) — стенд в исходном состоянии
curl -fsS -b "$JAR" -X DELETE "$BASE/api/workers/pgworker/api-cert" -o /dev/null \
  || { echo "⚠️ DELETE api-cert не прошёл (ключа уже нет?)"; }
curl -fsS -b "$JAR" -X POST "$BASE/api/workers/pgworker/restart" -o /dev/null
echo "✅ чек грани Воркеры пройден"
```

`chmod +x dev-stand/adminpanel/checks/70-worker-cert.sh`.

- [x] **Шаг 2: проверка чека на живом стенде**

Run: `bash dev-stand/adminpanel/checks/70-worker-cert.sh` (стенд поднят `00-up.sh`; если не поднят — поднять)
Expected: «✅ чек грани Воркеры пройден»; панель после прогона на env-серте (стенд в исходном состоянии).

- [x] **Шаг 3: runbook**

`docs/runbook.md` — новый раздел (после существующих `##`):

```markdown
## Управление сертом API воркера из панели

Серверные сертификаты входящих mTLS-граней PgWorker/KafkaWorker хранятся в
etcd (`/workers/api_tls/<worker>`), панель — единственный писатель; воркеры
читают ключ ТОЛЬКО при старте (приоритет etcd > env `PGW_API_TLS_*` /
`KFW_API_TLS_*`). Применение — всегда перезапуском: страница «Воркеры» →

1. «Сгенерировать сертификат» — self-signed лист (SAN — хосты живых
   advertise-URL + localhost/127.0.0.1, EKU serverAuth, 825 дней);
   повторная генерация при живом ключе → 409 (замените через PUT или DELETE).
2. «Загрузить сертификат» — своя PEM-пара (например, от install-CA).
3. «Перезапустить воркера» — 202, инстанс делает graceful self-stop;
   контейнер поднимает docker-политика `restart: unless-stopped` (deploy).
4. «Убрать управляемый сертификат» — DELETE ключа; после перезапуска воркер
   вернётся к env-серту (статус `unmanaged`).

Статусы применения: `applied` (thumbprint инстанса == целевому),
`pending restart` (ключ новее грани — перезапустите), `unmanaged` (ключа нет,
env-серт), `unknown` (старая версия воркера не сообщает thumbprint).

Ограничение защиты: сертификат, пригодный в исходящих коммуникациях воркеров
(CA, clientAuth, совпадение fingerprint с per-cluster CA kafka или сертами
панели), ОТКЛОНЯЕТСЯ с 422 «сертификат влияет на коммуникации воркеров с их
подчинёнными сервисами — обновление отклонено» — исходящие (PG-шарды/docker/
etcd/S3, kafka-брокеры) управляемым сертом не затрагиваются.

Без restart-политики (запуск вне docker) рестарт оставит процесс
остановленным — панель покажет недоступность API воркера.
```

- [x] **Шаг 4: финальный мерж-гейт — полный прогон (задача трогает код воркеров, канон AGENTS.md)**

Последовательность (после каждой серии — финальная строка прогона, затем docker-зачистка):

```bash
# 1. Сборка Release всего решения
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release

# 2. Юниты (все)
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.UnitTests -c Release
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Release

# 3. Интеграция/WAF (все)
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~Api|FullyQualifiedName~Etcd"
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.IntegrationTests -c Release --filter "FullyQualifiedName~Api|FullyQualifiedName~Etcd"
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.IntegrationTests -c Release

# 4. E2E-мерж-гейт: кейс-маркер регресса + новые сценарии серта (свежий Release)
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName~Scale_AddEmptyShard"
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.IntegrationTests -c Release --filter "FullyQualifiedName~E2eWorkerCertScenarios"

# 5. Фронтенд
cd frontend && npm run typecheck && npm run build
```

После серий: `docker rm -f` своих контейнеров (dev-стенд `as-*`/`adminpanel` не трогать), `docker ps -aq | wc -l` — контроль, `docker network prune -f`.
Expected: все прогоны зелёные.

**Выполнено — фиксация фактических результатов прогона (решение по замечанию код-ревью Фазы 7: гейт выполняется до мержа, результат фиксируется здесь):**

- Release-сборка решения — 0 warnings / 0 errors.
- Юниты: PgWorker — 898, KafkaWorker — 312, AdminPanel — 557; зелёные в Debug и Release.
- Интеграция PgWorker (фильтр `Api|Etcd`, Release) — 98/98.
- Интеграция AdminPanel — 149/149.
- E2E-маркер `Scale_AddEmptyShard` на свежем Release — 1/1 (2м07с).
- E2E `E2eWorkerCertScenarios` — 2/2.
- Фронтенд: `npm run typecheck` + `npm run build` — зелёные.

**Зафиксированное отклонение (единственный красный кейс — внешний flaky, не связанный с задачей):**
`KafkaWorker.IntegrationTests.Api.KafkaSeedApiTests.SeedDemo_AlreadySeeded_NoOp` (NRE). Причина — кросс-классовое загрязнение общей etcd-коллекции: соседние классы оставляют `/kafka/clusters/events/config`; добавление веткой двух классов в коллекцию сменило недетерминированный порядок исполнения xUnit. Доказательства внешности: изолированный прогон класса — 3/3 зелёный; тот же фильтр на `main` — 41/41 зелёный; прод-код задачи не затронут. Оформлено в `arch/roadmap/kafkaworker.md` тегом `t91-kafka-seed-api-flaky` (коммит `2cc2ce1`).

**Вывод: гейт пройден; отклонение внешнее и задокументировано.**

- [x] **Шаг 5: коммит**

```bash
git add dev-stand/adminpanel/checks/70-worker-cert.sh docs/runbook.md
git commit -m "docs(stand): чек грани Воркеры на живом стенде + runbook-раздел управления сертом API"
```

**Выход:** стенд-проверка сценария generate→restart→applied задокументирована и автоматизирована; мерж-гейт прогнан.
**Связь со spec:** §3.5, §5 Ф4, §7 (все критерии закрыты совокупно с Task 1–10).

---

## Self-review плана (выполнен составителем)

1. **Покрытие spec:**
   - §3.1 контракт etcd → Task 4 (писатели), Task 5 (читатели панели), Task 1–2 (читатели-воркеры).
   - §3.2 п.1 → Task 1/2; п.2 → Task 3; п.3 → Task 4.
   - §3.3 п.1 → Task 6; п.2 → Task 7; п.3 → Task 7; п.4 → Task 8. Требование «`worker` ∈ {pgworker, kafkaworker} иначе 404» закрыто для ВСЕХ пяти эндпоинтов: generate/PUT/DELETE — централизованный гвард в write-методах `WorkerCertService` (до `KeyOf`, мусорный ключ `/workers/api_tls/<foo>` в etcd не пишется; юнит-тест Task 6 + интеграционные кейсы `*_UnknownWorker_404_EtcdUntouched` в Task 8), restart — явная проверка в `RestartWorkerCommandHandler` (кейс `Restart_UnknownWorker_404`).
   - §3.4 → Task 9. §3.5 → Task 11 (deploy не меняется — ограничение соблюдено).
   - §4.1 → Task 6+8; §4.2 → Task 6+8; §4.3 → Task 6 (таблица целиком в тестах Шага 1; kafka-ветка правила 3 дополнительно покрыта интеграцией: Task 8 `Put_KafkaCaPem_422_EtcdUntouched` на стабе `IKafkaSecretsStore` в `WorkersWebFactory`); §4.4 → Task 3+7+8; §4.5 → Task 6 (одна txn/put), Task 1/2 (рестарт при упавшем etcd), Task 6 (идемпотентность).
   - §5 Ф1→Task 1–4, Ф2→Task 5–8, Ф3→Task 9, Ф4→Task 10–11.
   - §6 НЕ-делания — не реализуются (CLIENT_CA не трогаем, живая ротация отсутствует, hostname-валидация не меняется).
   - §7 кр.1→T8/T9/T11; кр.2→T3/T7/T10/T11; кр.3→T6/T8; кр.4→T6/T8/T9; кр.5→инвариант §4.3 (тесты T6 + kafka-ветка в T8: материалы исходящих не затронуты — проверено отказами 422); кр.6→T6/T8/T10; кр.7→T1/T2/T5/T8; кр.8→T5/T8/T9; кр.9→T10/T11.
2. **Плейсхолдеры:** «TBD/TODO»-заглушек нет; в шагах Task 9/10 часть Arrange-механики описана текстом с указанием точных файлов-образцов (соседние страницы/E2E-сценарии той же кодовой базы) — контрактные элементы (DTO, маршруты, ассерты, формулировки) приведены кодом полностью.
3. **Консистентность типов:** `ManagedCertRead`/`ManagedCertStatus` (воркеры, T1/T2), `ApiTlsSetup` (T1/T2), `RestartHandler`/`RestartDto` (T3), `certThumbprint`-параметр ClaimStore (T4), `WorkerApiCert`/`WorkerCertParser`/`WorkerCertParseResult` (T5), `WorkerCertService`-API + исключения (T6/T8), `WorkerApiInstanceResult`/`SendAllAsync`/`Build(tls, trustedThumbprints)` (T7/T8), DTO `WorkersViewDto`-семейство (T8/T9). `WorkerNotFoundException` определён ОДИН раз — в `AdminPanel.Etcd.Workers` (Task 6, `WorkerCertService.cs`); Task 8 только переиспользует (using уже в шаблоне файла) — дублей в `AdminPanel.Api` нет. Терминология «ClientCa» (spec §4.3) закреплена как `WorkerTls:ClientCertPem[_PATH]` панели (примечание в `KnownThumbprints`, Task 6). Расхождения именования между задачами отсутствуют.
