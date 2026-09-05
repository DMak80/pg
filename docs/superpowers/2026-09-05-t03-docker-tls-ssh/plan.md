# t03-docker-tls-ssh — план реализации

> **Для агентных исполнителей:** ОБЯЗАТЕЛЬНЫЙ SUB-SKILL: superpowers:subagent-driven-development (рекомендуется) или superpowers:executing-plans — исполнять план задача-за-задачей. Шаги используют синтаксис чекбоксов (`- [ ]`).

**Цель:** закрыть четыре транспортные дыры pg-домена: mTLS/SSH к Docker Engine API, mTLS-only HTTP API PgWorker (удаление `PGW_API_KEY`), переименование TLS-конфигурации панели на оба воркера, per-install TLS-пакет поставки/стенда + скрейп метрик за mTLS.

**Архитектура:** 1:1 механика t03-kafka (`TlsEndpoints`/`WorkerTlsHandler`) переносится на pg-домен; docker-транспорт — расширение существующего шва `DockerEngineFactory` (`unix://` как сейчас, `tcp://`+TLS через `SocketsHttpHandler.SslOptions`, `ssh://` через worker-managed `ForwardedPortLocal`-туннель с делегированием в штатный tcp-путь). Драйверы/HostEndpoint/portalloc не меняются. Панель и Prometheus ходят в воркеры клиентскими сертами одной per-install CA `kfw-install-ca` (расширение `deploy/tls/gen.sh`); docker-хосты — отдельная CA (`gen-docker.sh`).

**Технологии:** .NET 10 (Nullable, `TreatWarningsAsErrors=true`), SSH.NET (`Renci.SshNet`, NuGet-пакет `SSH.NET` 2026.0.0), Testcontainers 4.14, xunit.v3 + FluentAssertions, openssl (gen-скрипты).

**Spec:** `docs/superpowers/2026-09-05-t03-docker-tls-ssh/spec.md` (в этом же каталоге; план аргументируется от спеки — исполнители читают оба документа).

**Worktree:** `/Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh` (все пути ниже — относительно его корня, если не сказано «абсолютный»).

## Глобальные ограничения

- .NET 10, C# `LangVersion=latest`, `Nullable=enable`, **`TreatWarningsAsErrors=true`** — каждая сборка/тест обязаны проходить с 0 warnings (`DOTNET_CLI_UI_LANGUAGE=en`).
- Версии пакетов — централизованно в `src/Directory.Packages.props`; новый пакет: `SSH.NET` 2026.0.0; `ProjectReference`/`PackageReference` — только у `src/PgWorker.Docker/PgWorker.Docker.csproj` (+ `Microsoft.Extensions.Configuration` для env-биндингов, версия уже в props).
- Тестовые порты docker — только динамические: `WithPortBinding(<containerPort>, assignRandomHostPort: true)` + `GetMappedPublicPort(<containerPort>)`, либо зонд свободного порта на рантайме; никаких литералов портов в expects. `BrokerBootSec`-подобные таймауты интеграционных фикстур ≤ 100 с.
- Зачистка после КАЖДОЙ тестовой серии — ТОЛЬКО по name-фильтрам docker (подстрочный матч): `docker rm -f $(docker ps -aq --filter name=pgw-) 2>/dev/null; docker network prune -f`. Префикс `pgw-` покрывает и e2e-артефакты, и одноразовые тест-контейнеры этой задачи (`pgw-t-tlsproxy`, `pgw-t-sshd`). Контейнеры живого стенда (`as-*`, `deploy-*`, `adminpanel-*`) и посторонние (`cryptomgmt-*`) — НЕ трогать. ⚠️ `docker ps -aq` выводит ТОЛЬКО hex-ID без имён — строить чистку на `docker ps -aq | grep <имя>` НЕЛЬЗЯ (фильтр ничего не отсечёт и снесёт живой стенд); если нужна выборка по именам — только `docker ps -a --format '{{.Names}} {{.ID}}'` с явным исключением стендовых префиксов. Перед серией: `docker network ls | grep -c kfw-net` при нуле контейнеров = осиротевшие → `docker network prune -f`.
- Русский язык документации/комментариев; идентификаторы — английские; тесты — с AAA-комментариями (`// Arrange / // Act / // Assert`).
- Мерж-гейт (задачи трогают `PgWorker.App`/`Docker`): docker-E2E на свежем Release — минимум маркер `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard`; t03 меняет `DockerEngineFactory` (provisioning-путь) — обязателен ПОЛНЫЙ прогон `E2eFixture` (8/8).
- Коммит после каждой задачи; пуш/мерж — только по отдельному указанию пользователя.
- Прецедент для копирования: `src/KafkaWorker.App/Api/TlsEndpoints.cs`, `src/AdminPanel.Etcd/Workers/WorkerTlsHandler.cs`, `src/tests/KafkaWorker.IntegrationTests/Api/MtlsApiTests.cs`, `src/tests/AdminPanel.UnitTests/Workers/WorkerTlsHandlerTests.cs` (локальный `TestPki`-хелпер).
- «ApiKey» в именах тестов координации (`CoordinationTests`, `EtcdCoordinationTests`, `KafkaRefresherTests`, `SnapshotRefresherTests` — `Refresh_WithApiKeys_...`) — это lease-ключи `/pgworker/api/<id>`, НЕ `X-Api-Key`: их НЕ трогать.

---

### Task 1: Парсинг endpoint-схем `unix|tcp|ssh` (чистая функция)

**Files:**
- Create: `src/PgWorker.Docker/Engine/EndpointScheme.cs`
- Test: `src/tests/PgWorker.UnitTests/Docker/EndpointSchemeTests.cs`

**Interfaces:**
- Consumes: ничего (новая базовая единица).
- Produces: `PgWorker.Docker.Engine.EndpointScheme(string Scheme, string Host, int Port, string? User)`; константы `EndpointScheme.Unix/Tcp/Ssh` (`"unix"/"tcp"/"ssh"`), `DefaultTcpPort = 2375`, `DefaultSshPort = 22`; статический `EndpointScheme Parse(string endpoint)` — используют Tasks 2–3.

- [ ] **Step 1: Написать падающий юнит-тест**

`src/tests/PgWorker.UnitTests/Docker/EndpointSchemeTests.cs` (новый файл):

```csharp
using FluentAssertions;
using PgWorker.Docker.Engine;
using Xunit;

namespace PgWorker.UnitTests.Docker;

// Парсинг endpoint-схем Engine API (arch/14 §2.2, t03): unix-сокет, tcp с
// дефолтом 2375, ssh c user@host и дефолтом 22; неизвестная схема — отказ.
public class EndpointSchemeTests
{
    [Theory]
    [InlineData("unix:///var/run/docker.sock", EndpointScheme.Unix, "/var/run/docker.sock", 0, null)]
    [InlineData("tcp://host1:2376", EndpointScheme.Tcp, "host1", 2376, null)]
    [InlineData("tcp://host1", EndpointScheme.Tcp, "host1", 2375, null)]
    [InlineData("ssh://root@dock1", EndpointScheme.Ssh, "dock1", 22, "root")]
    [InlineData("ssh://ops@dock1:2222", EndpointScheme.Ssh, "dock1", 2222, "ops")]
    [InlineData("ssh://dock1:2222", EndpointScheme.Ssh, "dock1", 2222, null)]
    public void Parse_Schemes_Defaults_And_User(string endpoint, string scheme, string host, int port, string? user)
    {
        // Act
        var parsed = EndpointScheme.Parse(endpoint);

        // Assert
        parsed.Scheme.Should().Be(scheme);
        parsed.Host.Should().Be(host);
        parsed.Port.Should().Be(port);
        parsed.User.Should().Be(user);
    }

    [Theory]
    [InlineData("http://host:2375")]
    [InlineData("dock1")]
    [InlineData("ssh://")]
    public void Parse_UnknownSchemeOrEmptyHost_FailFast(string endpoint)
    {
        // Act / Assert: конфигурационная ошибка обязана падать при старте, а не в рантайме тика
        Assert.Throws<ApplicationException>(() => EndpointScheme.Parse(endpoint));
    }
}
```

- [ ] **Step 2: Прогнать тест — убедиться в падении**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter FullyQualifiedName~EndpointSchemeTests
```
Ожидание: FAIL (тип `EndpointScheme` не существует — ошибка компиляции).

- [ ] **Step 3: Реализовать минимально**

`src/PgWorker.Docker/Engine/EndpointScheme.cs`:

```csharp
namespace PgWorker.Docker.Engine;

// Разбор endpoint-схем Engine API (arch/14 §2.2, t03): unix://<path> |
// tcp://[host][:port] | ssh://[user@]host[:port]. Чистая функция — юнит-тесты
// без сети; дефолты портов: tcp — 2375 (plain Engine API), ssh — 22.
public sealed record EndpointScheme(string Scheme, string Host, int Port, string? User)
{
    public const string Unix = "unix";
    public const string Tcp = "tcp";
    public const string Ssh = "ssh";
    public const int DefaultTcpPort = 2375;
    public const int DefaultSshPort = 22;

    public static EndpointScheme Parse(string endpoint)
    {
        if (endpoint.StartsWith("unix://", StringComparison.Ordinal))
            return new EndpointScheme(Unix, endpoint["unix://".Length..], 0, null);

        foreach (var scheme in (Tcp, DefaultTcpPort), (Ssh, DefaultSshPort))
        {
            if (!endpoint.StartsWith(scheme.Item1 + "://", StringComparison.Ordinal))
                continue;
            var rest = endpoint[(scheme.Item1.Length + 3)..];
            string? user = null;
            var at = rest.LastIndexOf('@');
            if (at >= 0)
            {
                user = rest[..at];
                rest = rest[(at + 1)..];
            }

            // порт — после последнего ':' (хосты — DNS/IPv4; IPv6-литералы вне канона §2.2)
            var port = scheme.Item2;
            var colon = rest.LastIndexOf(':');
            if (colon >= 0 && int.TryParse(rest[(colon + 1)..], out var explicitPort))
            {
                port = explicitPort;
                rest = rest[..colon];
            }

            if (string.IsNullOrEmpty(rest))
                throw new ApplicationException($"endpoint без хоста: {endpoint}");
            return new EndpointScheme(scheme.Item1, rest, port, user);
        }

        throw new ApplicationException(
            $"неизвестная схема endpoint: {endpoint} (ожидался unix://|tcp://|ssh://, arch/14 §2.2)");
    }
}
```

- [ ] **Step 4: Прогнать тест — убедиться в прохождении**

Та же команда, что в Step 2. Ожидание: PASS (все кейсы).

- [ ] **Step 5: Собрать решение с 0 warnings и закоммитить**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release && \
git add src/PgWorker.Docker/Engine/EndpointScheme.cs src/tests/PgWorker.UnitTests/Docker/EndpointSchemeTests.cs && \
git commit -m "feat(docker): парсинг endpoint-схем unix|tcp|ssh для Engine API (t03 Ф1)"
```

---

### Task 2: `DockerTlsOptions` + TLS-handler в `DockerEngineFactory` (fail-fast, warning plaintext)

**Files:**
- Create: `src/PgWorker.Docker/Engine/DockerTlsOptions.cs`
- Modify: `src/PgWorker.Docker/Engine/DockerEngine.cs` (класс `DockerEngineFactory`, строки ~11–53)
- Modify: `src/PgWorker.Docker/PgWorker.Docker.csproj` (+2 PackageReference)
- Modify: `src/Directory.Packages.props` (если `Microsoft.Extensions.Configuration` ещё не подключена проектом — версия 10.0.9 уже в props, добавлять Version-атрибут НЕ нужно)
- Test: `src/tests/PgWorker.UnitTests/Docker/DockerTlsOptionsTests.cs`

**Interfaces:**
- Consumes: `EndpointScheme.Parse` (Task 1).
- Produces:
  - `DockerTlsOptions { string? CaPem; string? CaPath; string? ClientCertPem; string? ClientCertPath; string? ClientKeyPem; string? ClientKeyPath }` + `static (string Env, string Key)[] EnvBindings` (6 записей `PGW_DOCKER_TLS_{CA,CERT,KEY}`[+`_PATH`] → `PgWorker:Docker:Tls:*`) + `static void ApplyEnvOverrides(ConfigurationManager, Func<string,string?>? getenv = null)`;
  - ФИНАЛЬНАЯ сигнатура конструктора фабрики (4 optional-параметра; `ssh`/`loggerFactory` наполняются поведением в Task 3, но сигнатура фиксируется уже сейчас): `DockerEngineFactory(DockerTlsOptions? tls = null, SshTunnelOptions? ssh = null, ILogger<DockerEngineFactory>? logger = null, ILoggerFactory? loggerFactory = null)`; `Create(string endpoint, string? hostAlias = null)` без изменений контракта (совместимость с `new DockerEngineFactory()` в существующих тестах).

- [ ] **Step 1: Пакеты в проект `PgWorker.Docker`**

`src/PgWorker.Docker/PgWorker.Docker.csproj` — добавить в `<ItemGroup>`:

```xml
        <PackageReference Include="Microsoft.Extensions.Configuration" />
        <PackageReference Include="Microsoft.Extensions.Logging.Abstractions" />
```

(версии — из `src/Directory.Packages.props`: 10.0.9; `Directory.Packages.props` менять не нужно — оба `PackageVersion` уже есть.)

- [ ] **Step 2: Написать падающий юнит-тест**

`src/tests/PgWorker.UnitTests/Docker/DockerTlsOptionsTests.cs` (новый файл). Локальный PKI-хелпер — копия паттерна `TestPki` из `src/tests/AdminPanel.UnitTests/Workers/WorkerTlsHandlerTests.cs` (строки 64–100: `GenerateCa()` + `Issue(caPem, caKeyPem, cn)` на `CertificateRequest`/RSA-2048; панельные тесты не тянут зависимости воркеров — здесь свой локальный класс):

```csharp
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using PgWorker.Docker.Engine;
using Xunit;

namespace PgWorker.UnitTests.Docker;

// TLS к Engine API (arch/14 §2.2.1, t03): env-биндинги, сборка handler'а с
// клиентским сертом и доверием docker-CA, fail-fast частичной конфигурации,
// unix:// игнорирует TLS, plaintext tcp:// без TLS остаётся рабочим (R15).
public class DockerTlsOptionsTests
{
    [Fact]
    public void ApplyEnvOverrides_DockerTlsKeysMapped()
    {
        // Arrange: env-словарь (inject, без окружения).
        var env = new Dictionary<string, string>
        {
            ["PGW_DOCKER_TLS_CA_PEM"] = "ca-pem",
            ["PGW_DOCKER_TLS_CERT_PATH"] = "/tls/pgworker-docker.crt",
        };
        var config = new ConfigurationManager();

        // Act
        DockerTlsOptions.ApplyEnvOverrides(config, key => env.GetValueOrDefault(key));

        // Assert: ключи легли в PgWorker:Docker:Tls:*; таблица — 6 записей.
        config["PgWorker:Docker:Tls:CaPem"].Should().Be("ca-pem");
        config["PgWorker:Docker:Tls:ClientCertPath"].Should().Be("/tls/pgworker-docker.crt");
        DockerTlsOptions.EnvBindings.Should().HaveCount(6);
    }

    [Fact]
    public void Factory_TcpWithTls_ClientCertAndChainCallbackSet()
    {
        // Arrange: фикстурная docker-CA + клиентская пара (локальный TestPki).
        var (caPem, caKeyPem) = TestPki.GenerateCa();
        var (certPem, keyPem) = TestPki.Issue(caPem, caKeyPem, "pgworker");
        var factory = new DockerEngineFactory(new DockerTlsOptions
        {
            CaPem = caPem, ClientCertPem = certPem, ClientKeyPem = keyPem,
        });

        // Act: транспортный handler tcp-эндпоинта.
        var handler = factory.CreateHandler("tcp://host1:2376") as SocketsHttpHandler;

        // Assert: клиентский серт подан, колбэк доверия цепочки установлен
        // (паттерн WorkerTlsHandlerTests).
        handler.Should().NotBeNull();
        handler!.SslOptions.ClientCertificates.Should().NotBeNull().And.NotBeEmpty();
        handler.SslOptions.RemoteCertificateValidationCallback.Should().NotBeNull();
    }

    [Fact]
    public void Factory_PartialTlsConfig_FailFast()
    {
        // Arrange: CA задан, клиентская пара — нет (частичная конфигурация).
        var (caPem, _) = TestPki.GenerateCa();

        // Act / Assert: ошибка старта фабрики (spec §5.1), а не молчаливый plaintext.
        var ex = Assert.Throws<ApplicationException>(() =>
            new DockerEngineFactory(new DockerTlsOptions { CaPem = caPem }));
        ex.Message.Should().Contain("PgWorker:Docker:Tls");
    }

    [Fact]
    public void Factory_NoTls_PlainTcpHandlerWithoutSslOptions()
    {
        // Arrange / Act: фабрика без TLS-конфигурации (dev/тесты, R15).
        var handler = new DockerEngineFactory().CreateHandler("tcp://host1:2375") as SocketsHttpHandler;

        // Assert: plaintext-путь не сломан.
        handler!.SslOptions.ClientCertificates.Should().BeNull();
    }

    [Fact]
    public void Factory_UnixEndpoint_TlsIgnored()
    {
        // Arrange: TLS задан, но endpoint — unix-сокет.
        var (caPem, caKeyPem) = TestPki.GenerateCa();
        var (certPem, keyPem) = TestPki.Issue(caPem, caKeyPem, "pgworker");
        var factory = new DockerEngineFactory(new DockerTlsOptions
        {
            CaPem = caPem, ClientCertPem = certPem, ClientKeyPem = keyPem,
        });

        // Act
        var handler = factory.CreateHandler("unix:///var/run/docker.sock") as SocketsHttpHandler;

        // Assert: unix-транспорт — без TLS (сокет локальный, arch/14 §2.2).
        handler!.SslOptions.ClientCertificates.Should().BeNull();
    }

    // Локальный PKI-хелпер: CertificateRequest + RSA-2048 (паттерн TestPki из
    // AdminPanel.UnitTests/Workers/WorkerTlsHandlerTests.cs:64-100).
    private static class TestPki
    {
        public static (string CaPem, string CaKeyPem) GenerateCa()
        {
            using var rsa = RSA.Create(2048);
            var request = new CertificateRequest(
                "CN=test-docker-ca", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            using var ca = request.CreateSelfSigned(
                DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));
            return (ca.ExportCertificatePem(), ca.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());
        }

        public static (string CertPem, string KeyPem) Issue(string caPem, string caKeyPem, string commonName)
        {
            using var caCert = X509Certificate2.CreateFromPem(caPem);
            using var caKey = RSA.Create();
            caKey.ImportFromPem(caKeyPem);
            using var caWithKey = caCert.CopyWithPrivateKey(caKey);
            using var rsa = RSA.Create(2048);
            var request = new CertificateRequest(
                $"CN={commonName}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
                [new Oid("1.3.6.1.5.5.7.3.1"), new Oid("1.3.6.1.5.5.7.3.2")], critical: false));
            using var cert = request.Create(
                caWithKey, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1),
                RandomNumberGenerator.GetBytes(16));
            return (cert.ExportCertificatePem(), rsa.ExportPkcs8PrivateKeyPem());
        }
    }
}
```

- [ ] **Step 3: Прогнать тест — убедиться в падении (компиляция)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter FullyQualifiedName~DockerTlsOptionsTests
```
Ожидание: FAIL (нет `DockerTlsOptions`, у `DockerEngineFactory` нет конструктора с опциями/`CreateHandler(endpoint)` без TLS-ветки).

- [ ] **Step 4: Реализовать `DockerTlsOptions`**

`src/PgWorker.Docker/Engine/DockerTlsOptions.cs`:

```csharp
using Microsoft.Extensions.Configuration;

namespace PgWorker.Docker.Engine;

// TLS к Docker Engine API (arch/14 §2.2.1, t03): per-install docker-CA + клиентская
// пара воркера (deploy/tls/gen-docker.sh). PEM-дуализм env-секретов — значение или
// _PATH-файл. Частичная конфигурация — fail-fast фабрики (DockerEngineFactory).
public sealed class DockerTlsOptions
{
    // env-секреты → конфиг-дерево: PEM-значения и _PATH-файлы (паттерн WorkerTlsHandler.EnvBindings).
    public static readonly (string Env, string Key)[] EnvBindings =
    [
        ("PGW_DOCKER_TLS_CA", "PgWorker:Docker:Tls:CaPem"),
        ("PGW_DOCKER_TLS_CERT", "PgWorker:Docker:Tls:ClientCertPem"),
        ("PGW_DOCKER_TLS_KEY", "PgWorker:Docker:Tls:ClientKeyPem"),
        ("PGW_DOCKER_TLS_CA_PATH", "PgWorker:Docker:Tls:CaPath"),
        ("PGW_DOCKER_TLS_CERT_PATH", "PgWorker:Docker:Tls:ClientCertPath"),
        ("PGW_DOCKER_TLS_KEY_PATH", "PgWorker:Docker:Tls:ClientKeyPath"),
    ];

    /// <summary>PEM per-install docker-CA (или CA_PATH файл).</summary>
    public string? CaPem { get; set; }

    public string? CaPath { get; set; }

    /// <summary>PEM клиентского серта воркера (или CERT_PATH файл).</summary>
    public string? ClientCertPem { get; set; }

    public string? ClientCertPath { get; set; }

    /// <summary>PEM приватного ключа PKCS#8 (или KEY_PATH файл).</summary>
    public string? ClientKeyPem { get; set; }

    public string? ClientKeyPath { get; set; }

    // Перенос env → конфиг; getenv-инъекция — для юнит-теста (без окружения).
    public static void ApplyEnvOverrides(ConfigurationManager configuration, Func<string, string?>? getenv = null)
    {
        getenv ??= Environment.GetEnvironmentVariable;
        foreach (var (env, key) in EnvBindings)
        {
            var value = getenv(env);
            if (!string.IsNullOrWhiteSpace(value))
                configuration[key] = value;
        }
    }
}
```

- [ ] **Step 5: Расширить `DockerEngineFactory` (в `DockerEngine.cs`)**

Заменить класс `DockerEngineFactory` (строки 11–54 текущего файла) на:

```csharp
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;

// ... (существующие using файла остаются)

// Фабрика движков (arch/14 §2.2/§2.2.1, t03): endpoint "unix:///var/run/docker.sock"
// | "tcp://host[:2375]" (+TLS при заданном DockerTlsOptions) | "ssh://[user@]host[:22]"
// (туннель — SshTunnelOptions). API-версия закреплена v1.44 (docker >= 23).
public class DockerEngineFactory : IAsyncDisposable
{
    private readonly DockerTlsMaterial? _tls;
    private readonly SshTunnelOptions? _ssh;
    private readonly ILogger<DockerEngineFactory>? _logger;

    // Fail-fast здесь (а не в тике): частичная TLS-конфигурация — ошибка старта.
    public DockerEngineFactory(
        DockerTlsOptions? tls = null,
        SshTunnelOptions? ssh = null,
        ILogger<DockerEngineFactory>? logger = null)
    {
        _ssh = ssh;
        _logger = logger;
        _tls = tls is null ? null : DockerTlsMaterial.Load(tls);
    }

    // Транспортный handler: unix → ConnectCallback с UnixDomainSocketEndPoint;
    // tcp → TLS (клиентский серт + цепочка против docker-CA), если сконфигурирован.
    internal HttpMessageHandler CreateHandler(string endpoint)
    {
        var scheme = EndpointScheme.Parse(endpoint);
        var sockets = new SocketsHttpHandler
        {
            // docker-прокси держит соединения — не рвём их агрессивно
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
        };
        if (scheme.Scheme == EndpointScheme.Unix)
        {
            var socketPath = scheme.Host;
            sockets.ConnectCallback = async (context, ct) =>
            {
                var socket = new System.Net.Sockets.Socket(
                    System.Net.Sockets.AddressFamily.Unix, System.Net.Sockets.SocketType.Stream,
                    System.Net.Sockets.ProtocolType.Unspecified);
                try
                {
                    await socket.ConnectAsync(new System.Net.Sockets.UnixDomainSocketEndPoint(socketPath), ct);
                    return new System.Net.Sockets.NetworkStream(socket, ownsSocket: true);
                }
                catch
                {
                    socket.Dispose();
                    throw;
                }
            };
        }
        else if (_tls is not null)
        {
            // tcp (+TLS поверх ssh-туннеля — endpoint уже tcp://127.0.0.1:<bound>)
            sockets.SslOptions.ClientCertificates = new X509CertificateCollection { _tls.ClientCert };
            sockets.SslOptions.RemoteCertificateValidationCallback =
                (_, certificate, _, _) => DockerTlsMaterial.ValidateChain(certificate, _tls.Ca);
        }
        else if (scheme.Scheme == EndpointScheme.Tcp)
        {
            // R15: plaintext tcp — только dev/тесты/локальные стенды; канон прода —
            // 2376+mTLS или ssh (arch/14 §2.2.1).
            _logger?.LogWarning(
                "Engine API {Endpoint} без TLS (plaintext tcp; канон прода — tcp://:2376 mTLS или ssh://, arch/14 §2.2.1)",
                endpoint);
        }

        return sockets;
    }

    // hostAlias — имя docker-хоста для BusyPorts plain-режима (swarm: null).
    public virtual IDockerEngine Create(string endpoint, string? hostAlias = null)
    {
        var scheme = EndpointScheme.Parse(endpoint);
        if (scheme.Scheme == EndpointScheme.Ssh)
            endpoint = $"tcp://127.0.0.1:{TunnelFor(endpoint, scheme).BoundPort}";

        var baseAddress = scheme.Scheme == EndpointScheme.Unix
            ? "http://localhost" // фиктивный хост: соединение уходит в unix-сокет через ConnectCallback
            : (_tls is not null
                ? $"https://{EndpointScheme.Parse(endpoint).Host}:{EndpointScheme.Parse(endpoint).Port}"
                : endpoint);
        var httpClient = new HttpClient(CreateHandler(endpoint)) { BaseAddress = new Uri(baseAddress) };
        return new DockerEngine(httpClient, hostAlias);
    }

    // SSH-туннели (Task 3): кэш по endpoint + reconnect-семантика.
    private SshHostConnection TunnelFor(string endpoint, EndpointScheme scheme) => throw new NotSupportedException("реализован в Task 3");

    public ValueTask DisposeAsync() => ValueTask.CompletedTask; // наполняется в Task 3
}

// Загруженный TLS-материал фабрики: живёт время жизни фабрики (валидация цепочки
// вызывается на КАЖДОМ хендшейке — без using; паттерн WorkerTlsHandler.Build).
internal sealed class DockerTlsMaterial
{
    public required X509Certificate2 ClientCert { get; init; }

    public required X509Certificate2 Ca { get; init; }

    // PEM с файловым fallback; частичная конфигурация → ApplicationException.
    public static DockerTlsMaterial Load(DockerTlsOptions tls)
    {
        var caPem = tls.CaPem ?? ReadFile(tls.CaPath);
        var certPem = tls.ClientCertPem ?? ReadFile(tls.ClientCertPath);
        var keyPem = tls.ClientKeyPem ?? ReadFile(tls.ClientKeyPath);
        if (caPem is null || certPem is null || keyPem is null)
            throw new ApplicationException(
                "PgWorker:Docker:Tls: частичная TLS-конфигурация — нужны CA+CERT+KEY "
                + "(env PGW_DOCKER_TLS_{CA,CERT,KEY}[_PATH], arch/14 §2.2.1)");

        // PFX round-trip: ключ CreateFromPem эфемерный — macOS SslStream требует
        // ре-импорт (паттерн WorkerTlsHandler.Build).
        var pem = X509Certificate2.CreateFromPem(certPem, keyPem);
        var clientCert = X509CertificateLoader.LoadPkcs12(pem.Export(X509ContentType.Pkcs12), null);
        var ca = X509Certificate2.CreateFromPem(caPem);
        return new DockerTlsMaterial
        {
            ClientCert = clientCert,
            Ca = OperatingSystem.IsMacOS()
                ? X509CertificateLoader.LoadPkcs12(ca.Export(X509ContentType.Pkcs12), null)
                : ca,
        };
    }

    // Цепочка серверного серта демона против per-install docker-CA (паттерн
    // WorkerTlsHandler.ValidateChain: CustomRootTrust + NoCheck — приватная CA без CRL).
    public static bool ValidateChain(X509Certificate? certificate, X509Certificate2 ca)
    {
        var cert2 = certificate as X509Certificate2
            ?? (certificate is null ? null : new X509Certificate2(certificate));
        if (cert2 is null)
            return false;
        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(ca);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        return chain.Build(cert2);
    }

    private static string? ReadFile(string? path)
        => path is null || !File.Exists(path) ? null : File.ReadAllText(path).Trim();
}
```

ВНИМАНИЕ: на этом шаге код не соберётся из-за `SshTunnelOptions`/`SshHostConnection` — добавьте в `DockerEngine.cs` временные заглушки НИЖЕ фабрики и удалите их в Task 3 Step 4:

```csharp
// ВРЕМЕННАЯ заглушка до Task 3 (удалить при реализации SshTunnelOptions/SshHostConnection).
public sealed class SshTunnelOptions;
internal sealed class SshHostConnection { public int BoundPort => throw new NotSupportedException(); }
```

- [ ] **Step 6: Прогнать тесты — убедиться в прохождении**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter FullyQualifiedName~DockerTlsOptionsTests
```
Ожидание: PASS 5/5. Затем вся сборка (не сломаны существующие `DockerEngineTests`/`ClusterDriverTests`): `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release` — 0 warnings.

- [ ] **Step 7: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
git add src/PgWorker.Docker src/Directory.Packages.props src/tests/PgWorker.UnitTests/Docker/DockerTlsOptionsTests.cs && \
git commit -m "feat(docker): TLS к Engine API — DockerTlsOptions + handler фабрики, fail-fast частичной конфигурации (t03 Ф1)"
```

---

### Task 3: `SshTunnelOptions` + `SshHostConnection` (SSH.NET) + ssh://-шов фабрики

**Files:**
- Modify: `src/Directory.Packages.props` (+`<PackageVersion Include="SSH.NET" Version="2026.0.0" />`)
- Modify: `src/PgWorker.Docker/PgWorker.Docker.csproj` (+`<PackageReference Include="SSH.NET" />`)
- Create: `src/PgWorker.Docker/Engine/SshTunnelOptions.cs`
- Create: `src/PgWorker.Docker/Engine/SshHostConnection.cs`
- Modify: `src/PgWorker.Docker/Engine/DockerEngine.cs` (удалить временные заглушки Task 2; реализовать `TunnelFor`/`DisposeAsync`)
- Test: `src/tests/PgWorker.UnitTests/Docker/SshTunnelOptionsTests.cs`

**Interfaces:**
- Consumes: `EndpointScheme` (Task 1), `DockerEngineFactory`-поля из Task 2.
- Produces:
  - `SshTunnelOptions { string? KeyPem; string? KeyPath; string RemoteDaemonHost = "127.0.0.1"; int RemoteDaemonPort = 2376; string? FingerprintSha256; int KeepAliveSec = 15; int ConnectTimeoutSec = 10 }` + `EnvBindings` (3 записи: `PGW_DOCKER_SSH_KEY[_PATH]`, `PGW_DOCKER_SSH_FINGERPRINT`) + `ApplyEnvOverrides(ConfigurationManager, Func<string,string?>?)`;
  - `(string Host, int Port) TunnelTarget()` — чистая функция target-вычисления форварда (валидация host/порта, spec §5.5 «plan-функции туннеля без сети»; юнит-тесты);
  - `SshHostConnection(EndpointScheme scheme, SshTunnelOptions options, ILogger? logger = null)`: `int BoundPort { get; }`, `bool IsConnected { get; }`, `string? FingerprintSha256 { get; }` (последний увиденный fingerprint — для TOFU-диагностики/тестов), `void EnsureConnected()` (connect/reconnect, бэкофф ≥5 с между попытками), `ValueTask DisposeAsync()`;
  - `SshTunnelOptions.DecideHostKeyTrust(byte[] hostKeyData, string? expectedSha256, out bool trustByTofu)` — чистая функция семантики pin (юнит-тесты).

- [ ] **Step 1: Пин пакета**

`src/Directory.Packages.props` — в `<ItemGroup>` по алфавиту:

```xml
    <PackageVersion Include="SSH.NET" Version="2026.0.0" />
```

`src/PgWorker.Docker/PgWorker.Docker.csproj` — в `<ItemGroup>` с пакетами:

```xml
        <PackageReference Include="SSH.NET" />
```

Проверка резолва: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && DOTNET_CLI_UI_LANGUAGE=en dotnet restore src/PgWorker.slnx` — exit 0.

- [ ] **Step 2: Написать падающие юнит-тесты**

`src/tests/PgWorker.UnitTests/Docker/SshTunnelOptionsTests.cs`:

```csharp
using System.Security.Cryptography;
using System.Text;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using PgWorker.Docker.Engine;
using Xunit;

namespace PgWorker.UnitTests.Docker;

// SSH-туннель к Engine API (arch/14 §2.2.1, t03): env-биндинги, fingerprint-семантика
// (pin задан — строгое сравнение; не задан — TOFU-accept c признаком warning, R14),
// целевой адрес форварда — чистые функции без сети.
public class SshTunnelOptionsTests
{
    [Fact]
    public void ApplyEnvOverrides_SshKeysMapped()
    {
        // Arrange
        var env = new Dictionary<string, string>
        {
            ["PGW_DOCKER_SSH_KEY_PATH"] = "/secrets/id_pgworker",
            ["PGW_DOCKER_SSH_FINGERPRINT"] = "SHA256:abcdef",
        };
        var config = new ConfigurationManager();

        // Act
        SshTunnelOptions.ApplyEnvOverrides(config, key => env.GetValueOrDefault(key));

        // Assert
        config["PgWorker:Docker:Ssh:KeyPath"].Should().Be("/secrets/id_pgworker");
        config["PgWorker:Docker:Ssh:FingerprintSha256"].Should().Be("SHA256:abcdef");
        SshTunnelOptions.EnvBindings.Should().HaveCount(3);
    }

    [Fact]
    public void DecideHostKeyTrust_ExpectedPinSet_StrictComparison()
    {
        // Arrange: произвольные host-key данные + ожидаемый pin = их SHA-256.
        var hostKey = Encoding.ASCII.GetBytes("host-key-blob");
        var sha = Convert.ToBase64String(SHA256.HashData(hostKey)).TrimEnd('=');

        // Act / Assert: точное совпадение (в форматах с префиксом и без) — доверие;
        // посторонний pin — отказ; TOFU-флага нет.
        SshTunnelOptions.DecideHostKeyTrust(hostKey, "SHA256:" + sha, out var tofu1).Should().BeTrue();
        SshTunnelOptions.DecideHostKeyTrust(hostKey, sha, out _).Should().BeTrue();
        SshTunnelOptions.DecideHostKeyTrust(hostKey, "SHA256:AAAA", out var tofu2).Should().BeFalse();
        tofu1.Should().BeFalse();
        tofu2.Should().BeFalse();
    }

    [Fact]
    public void DecideHostKeyTrust_NoPin_TofuAcceptWithWarning()
    {
        // Arrange: pin не задан (PGW_DOCKER_SSH_FINGERPRINT пуст).
        // Act
        var trust = SshTunnelOptions.DecideHostKeyTrust("blob"u8.ToArray(), null, out var tofu);

        // Assert: принимаем (TOFU), но семантика требует warning-лога у вызывающего.
        trust.Should().BeTrue();
        tofu.Should().BeTrue();
    }

    [Fact]
    public void KeyMaterial_PemOrPathFallback()
    {
        // Arrange: PEM-значение приоритетнее пути (дуализм env-секретов).
        var opts = new SshTunnelOptions { KeyPem = "-----BEGIN PRIVATE KEY-----", KeyPath = "/nonexistent" };

        // Act / Assert: наличный ключ без сети — только факт выбора источника
        // (метод SshHostConnection.ReadKeyMaterial вынесен как internal static).
        SshHostConnection.ReadKeyMaterial(opts).Should().Be("-----BEGIN PRIVATE KEY-----");
        SshHostConnection.ReadKeyMaterial(new SshTunnelOptions { KeyPem = null, KeyPath = null })
            .Should().BeNull();
    }

    [Fact]
    public void TunnelTarget_DefaultsAndCustom_Validated()
    {
        // Arrange: дефолты канона (loopback демона, 2376 c --tlsverify) и кастом.
        var custom = new SshTunnelOptions { RemoteDaemonHost = "dock-internal", RemoteDaemonPort = 2375 };

        // Act / Assert: target-вычисление без сети (spec §5.5) — дефолты/кастом.
        new SshTunnelOptions().TunnelTarget().Should().Be(("127.0.0.1", 2376));
        custom.TunnelTarget().Should().Be(("dock-internal", 2375));
    }

    [Theory]
    [InlineData("", 2376)]   // пустой хост
    [InlineData("127.0.0.1", 0)]     // порт вне диапазона
    [InlineData("127.0.0.1", 65536)]  // порт вне диапазона
    public void TunnelTarget_Invalid_FailFast(string host, int port)
    {
        // Arrange: некорректная цель форварда.
        var opts = new SshTunnelOptions { RemoteDaemonHost = host, RemoteDaemonPort = port };

        // Act / Assert: конфигурационная ошибка — при создании туннеля, не в рантайме тика.
        Assert.Throws<ApplicationException>(() => opts.TunnelTarget());
    }
}
```

- [ ] **Step 3: Прогнать — убедиться в падении (компиляция)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter FullyQualifiedName~SshTunnelOptionsTests
```

- [ ] **Step 4: Реализовать**

`src/PgWorker.Docker/Engine/SshTunnelOptions.cs`:

```csharp
using System.Security.Cryptography;
using Microsoft.Extensions.Configuration;

namespace PgWorker.Docker.Engine;

// SSH-туннель к Engine API (arch/14 §2.2.1, t03): worker-managed
// ForwardedPortLocal → RemoteDaemonHost:RemoteDaemonPort. key-аутентификация
// (пароли вне канона); fingerprint-pin опционален (без него TOFU+warning, R14).
public sealed class SshTunnelOptions
{
    // env-секреты → конфиг-дерево (паттерн WorkerTlsHandler.EnvBindings).
    public static readonly (string Env, string Key)[] EnvBindings =
    [
        ("PGW_DOCKER_SSH_KEY", "PgWorker:Docker:Ssh:KeyPem"),
        ("PGW_DOCKER_SSH_KEY_PATH", "PgWorker:Docker:Ssh:KeyPath"),
        ("PGW_DOCKER_SSH_FINGERPRINT", "PgWorker:Docker:Ssh:FingerprintSha256"),
    ];

    /// <summary>PEM приватного ключа PKCS#8/OpenSSL RSA (или KEY_PATH файл).</summary>
    public string? KeyPem { get; set; }

    public string? KeyPath { get; set; }

    /// <summary>Адрес daemon-порта НА удалённом хосте (loopback демона).</summary>
    public string RemoteDaemonHost { get; set; } = "127.0.0.1";

    /// <summary>Порт daemon-порта на удалённом хосте (канон: 2376 c --tlsverify).</summary>
    public int RemoteDaemonPort { get; set; } = 2376;

    /// <summary>SHA-256 fingerprint хост-ключа (ssh-keygen-формат, с/без «SHA256:»);
    /// null — TOFU-accept + warning (R14).</summary>
    public string? FingerprintSha256 { get; set; }

    /// <summary>Keepalive SSH-сессии, сек.</summary>
    public int KeepAliveSec { get; set; } = 15;

    /// <summary>Бюджет подключения/аутентификации, сек.</summary>
    public int ConnectTimeoutSec { get; set; } = 10;

    public static void ApplyEnvOverrides(ConfigurationManager configuration, Func<string, string?>? getenv = null)
    {
        getenv ??= Environment.GetEnvironmentVariable;
        foreach (var (env, key) in EnvBindings)
        {
            var value = getenv(env);
            if (!string.IsNullOrWhiteSpace(value))
                configuration[key] = value;
        }
    }

    // Цель форварда на удалённом хосте (чистая функция — юнит-тесты без сети,
    // spec §5.5 «target-вычисление туннеля»): валидация host/порта.
    public (string Host, int Port) TunnelTarget()
    {
        if (string.IsNullOrWhiteSpace(RemoteDaemonHost) || RemoteDaemonPort is < 1 or > 65535)
            throw new ApplicationException(
                $"PgWorker:Docker:Ssh: некорректная цель туннеля {RemoteDaemonHost}:{RemoteDaemonPort} (arch/14 §2.2.1)");
        return (RemoteDaemonHost, RemoteDaemonPort);
    }

    // Семантика host-key (юнит-тестируема без сети): pin задан — строгое
    // сравнение SHA-256 (нормализация префикса/паддинга); не задан — TOFU-accept
    // (trustByTofu=true — вызывающий логирует warning единожды на хост).
    public static bool DecideHostKeyTrust(byte[] hostKeyData, string? expectedSha256, out bool trustByTofu)
    {
        trustByTofu = false;
        var actual = Convert.ToBase64String(SHA256.HashData(hostKeyData)).TrimEnd('=');
        if (expectedSha256 is not { Length: > 0 })
        {
            trustByTofu = true;
            return true;
        }

        var expected = expectedSha256.Trim().TrimStart("SHA256:").TrimEnd('=');
        return string.Equals(actual, expected, StringComparison.Ordinal);
    }
}
```

(`TrimStart("SHA256:")` — если проект не собирается из-за отсутствия string-перегрузки в используемой версии — заменить на: `var expected = expectedSha256.Trim(); if (expected.StartsWith("SHA256:", StringComparison.Ordinal)) expected = expected["SHA256:".Length..]; expected = expected.TrimEnd('=');`)

`src/PgWorker.Docker/Engine/SshHostConnection.cs`:

```csharp
using System.Text;
using Microsoft.Extensions.Logging;
using Renci.SshNet;

namespace PgWorker.Docker.Engine;

// SSH-туннель к Engine API (arch/14 §2.2.1, t03, О3): одна SshClient-сессия
// (key-аутентификация, fingerprint-pin/TOFU) + ForwardedPortLocal
// ("127.0.0.1":0 → RemoteDaemonHost:RemoteDaemonPort); фактический bound-порт
// отдаётся фабрике — Engine-клиент ходит на него как на обычный tcp:// (+TLS
// сверху при заданном Docker:Tls — канон daemon --tlsverify). Штатные вызовы
// движка о туннеле не знают. Разрыв — transient: EnsureConnected() переподключает
// с бэкоффом, тики healthz/надзора честно видят хост недоступным.
public sealed class SshHostConnection : IAsyncDisposable
{
    private const int ReconnectBackoffSec = 5;

    private readonly SshClient _client;
    private readonly ForwardedPortLocal _port;
    private readonly ILogger _logger;
    private readonly string _endpoint;
    private readonly string? _fingerprint;
    private readonly bool[] _tofuWarned = [false]; // warning единожды на хост
    private DateTimeOffset _lastAttempt = DateTimeOffset.MinValue;

    public int BoundPort { get; }

    // fingerprint последнего хендшейка (диагностика TOFU; тест pin-кейса).
    public string? FingerprintSha256 { get; private set; }

    public bool IsConnected => _client.IsConnected && _port.IsStarted;

    public SshHostConnection(EndpointScheme scheme, SshTunnelOptions options, ILogger? logger = null)
    {
        _endpoint = $"ssh://{scheme.User}@{scheme.Host}:{scheme.Port}";
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
        var keyPem = ReadKeyMaterial(options)
            ?? throw new ApplicationException(
                $"PgWorker:Docker:Ssh: ключ не задан (env PGW_DOCKER_SSH_KEY[_PATH]) для {_endpoint}, arch/14 §2.2.1");
        _fingerprint = options.FingerprintSha256;
        var keyFile = new PrivateKeyFile(new MemoryStream(Encoding.UTF8.GetBytes(keyPem)));
        var user = scheme.User ?? "root"; // дефолт docker-хостов: операторские демоны
        var connection = new ConnectionInfo(scheme.Host, scheme.Port, user, new PrivateKeyAuthenticationMethod(user, keyFile))
        {
            Timeout = TimeSpan.FromSeconds(Math.Max(1, options.ConnectTimeoutSec)),
        };
        _client = new SshClient(connection) { KeepAliveInterval = TimeSpan.FromSeconds(Math.Max(1, options.KeepAliveSec)) };
        _client.HostKeyReceived += (_, e) =>
        {
            var trust = SshTunnelOptions.DecideHostKeyTrust(e.HostKeyData, _fingerprint, out var tofu);
            FingerprintSha256 = "SHA256:" + Convert.ToBase64String(System.Security.Cryptography.SHA256.HashData(e.HostKeyData)).TrimEnd('=');
            if (trust && tofu && !_tofuWarned[0])
            {
                _tofuWarned[0] = true;
                _logger.LogWarning(
                    "SSH host-key {Endpoint} принят по TOFU (fingerprint {Fingerprint}); закрепите PGW_DOCKER_SSH_FINGERPRINT (R14)",
                    _endpoint, FingerprintSha256);
            }

            e.CanTrust = trust;
        };

        var (targetHost, targetPort) = options.TunnelTarget(); // target — валидированная чистая функция (юнит-тест)
        _port = new ForwardedPortLocal("127.0.0.1", 0, targetHost, checked((uint)targetPort));
        _client.AddForwardedPort(_port);
        _port.Start(); // bound-порт выделяется здесь (порт 0 → фактический)
        BoundPort = (int)_port.BoundPort;
        Connect();
    }

    // Connect/reconnect с бэкоффом: подряд идущие попытки не чаще
    // ReconnectBackoffSec (иначе — ApplicationException-transient на тик).
    public void EnsureConnected()
    {
        if (IsConnected)
            return;
        if (DateTimeOffset.UtcNow - _lastAttempt < TimeSpan.FromSeconds(ReconnectBackoffSec))
            throw new ApplicationException($"SSH-туннель {_endpoint} разорван — повторная попытка после бэкоффа (transient)");
        Connect();
    }

    private void Connect()
    {
        _lastAttempt = DateTimeOffset.UtcNow;
        try
        {
            _client.Connect();
        }
        catch (Exception e)
        {
            throw new ApplicationException($"SSH-подключение {_endpoint} не удалось (transient): {e.Message}", e);
        }
    }

    // PEM с файловым fallback (PKCS#8/OpenSSL — формат PrivateKeyFile SSH.NET).
    internal static string? ReadKeyMaterial(SshTunnelOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.KeyPem))
            return options.KeyPem;
        return options.KeyPath is not null && File.Exists(options.KeyPath)
            ? File.ReadAllText(options.KeyPath).Trim()
            : null;
    }

    public ValueTask DisposeAsync()
    {
        try
        {
            if (_port.IsStarted)
                _port.Stop();
            _port.Dispose();
            if (_client.IsConnected)
                _client.Disconnect();
            _client.Dispose();
        }
        catch (Exception e)
        {
            _logger.LogWarning("закрытие SSH-туннеля {Endpoint}: {Message}", _endpoint, e.Message);
        }

        return ValueTask.CompletedTask;
    }
}
```

Модификация `DockerEngine.cs` (фабрика из Task 2): удалить временные заглушки `SshTunnelOptions`/`SshHostConnection`, в `DockerEngineFactory` добавить:

```csharp
    private readonly Dictionary<string, SshHostConnection> _tunnels = new();
    private readonly object _tunnelsLock = new();
    private readonly ILoggerFactory? _loggerFactory;

    // кэш туннелей по endpoint: подключённый — переиспользуем; разорванный —
    // reconnect с бэкоффом (EnsureConnected бросает transient-ошибку на тик).
    private SshHostConnection TunnelFor(string endpoint, EndpointScheme scheme)
    {
        lock (_tunnelsLock)
        {
            if (_tunnels.TryGetValue(endpoint, out var existing))
            {
                existing.EnsureConnected();
                return existing;
            }

            var tunnel = new SshHostConnection(scheme, _ssh ?? new SshTunnelOptions(),
                _loggerFactory?.CreateLogger<SshHostConnection>());
            _tunnels[endpoint] = tunnel;
            return tunnel;
        }
    }

    public async ValueTask DisposeAsync()
    {
        lock (_tunnelsLock)
        {
            foreach (var tunnel in _tunnels.Values)
                await tunnel.DisposeAsync();
            _tunnels.Clear();
        }
    }
```

`_loggerFactory` — новый optional-параметр конструктора фабрики (`ILoggerFactory? loggerFactory = null`; из него же создать `ILogger<DockerEngineFactory>` для warning R15, если `logger` не передан явно). Существующие вызовы `new DockerEngineFactory()` (тесты) остаются совместимыми.

В `Create`: `endpoint = $"tcp://127.0.0.1:{TunnelFor(endpoint, scheme).BoundPort}"` — уже из Task 2.

- [ ] **Step 5: Прогнать юниты + сборку**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release \
  --filter "FullyQualifiedName~SshTunnelOptionsTests|FullyQualifiedName~EndpointSchemeTests|FullyQualifiedName~DockerTlsOptionsTests" && \
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
```
Ожидание: PASS; сборка 0 warnings.

- [ ] **Step 6: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
git add src/Directory.Packages.props src/PgWorker.Docker src/tests/PgWorker.UnitTests/Docker/SshTunnelOptionsTests.cs && \
git commit -m "feat(docker): SSH-туннель к Engine API — SshHostConnection (SSH.NET) + кэш в фабрике, fingerprint pin/TOFU (t03 Ф1)"
```

---

### Task 4: Интеграционные docker-тесты транспорта (TLS-proxy nginx + sshd-туннель)

**Files:**
- Create: `src/tests/PgWorker.IntegrationTests/Docker/TlsEngineProxyTests.cs`
- Create: `src/tests/PgWorker.IntegrationTests/Docker/SshTunnelEngineTests.cs`
- Create: `src/tests/PgWorker.IntegrationTests/Docker/EngineProxyTestPki.cs` (общий PKI-хелпер серий — код ниже в Step 1)

**Interfaces:**
- Consumes: `DockerEngineFactory(tls, ssh)` (Tasks 2–3), `Testcontainers` (`ContainerBuilder`), `DockerTrait.SkipIfUnavailable`.
- Produces: доказательство критериев spec §8.2 (TLS к Engine API; SSH-туннель; неверный pin → отказ). Ничего для последующих задач.

- [ ] **Step 1: TLS-proxy тест (nginx stream ssl → docker.sock)**

`src/tests/PgWorker.IntegrationTests/Docker/TlsEngineProxyTests.cs`:

```csharp
using System.Net;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using PgWorker.Core.Model;
using PgWorker.Docker.Engine;
using Xunit;
using static PgWorker.IntegrationTests.Docker.EngineProxyTestPki;

namespace PgWorker.IntegrationTests.Docker;

// TLS к Engine API (spec §5.5/§8.2, t03): nginx stream-прокси (listen … ssl →
// unix:/var/run/docker.sock) с сертами фикстуры — DockerEngineFactory на
// tcp://localhost:<mapped> + DockerTlsOptions выполняет Ping/ListContainers и
// create/start/delete одноразового контейнера. Порт — динамический.
public class TlsEngineProxyTests
{
    [Fact]
    public async Task TlsProxy_PingList_CreateDelete_ThroughTls()
    {
        // Arrange: per-install docker-CA + серверный (SAN localhost/127.0.0.1) +
        // клиентский серты; nginx.conf stream-прокси; порт — случайный хост-порт.
        DockerTrait.SkipIfUnavailable();
        var (caPem, caKeyPem) = GenerateCa();
        var (serverCertPem, serverKeyPem) = Issue(caPem, caKeyPem, "docker-host", ["localhost"], IPAddress.Loopback);
        var (clientCertPem, clientKeyPem) = Issue(caPem, caKeyPem, "pgworker", ["pgworker"], null);
        var certsDir = Path.Combine(Path.GetTempPath(), $"pgw-tls-proxy-{Guid.NewGuid():N}");
        Directory.CreateDirectory(certsDir);
        await File.WriteAllTextAsync(Path.Combine(certsDir, "server.crt"), serverCertPem);
        await File.WriteAllTextAsync(Path.Combine(certsDir, "server.key"), serverKeyPem);
        const string nginxConf = """
            worker_processes 1;
            events { worker_connections 64; }
            stream {
              server {
                listen 2376 ssl;
                ssl_certificate /etc/nginx/certs/server.crt;
                ssl_certificate_key /etc/nginx/certs/server.key;
                proxy_pass unix:/var/run/docker.sock;
              }
            }
            """;
        var confPath = Path.Combine(certsDir, "nginx.conf");
        await File.WriteAllTextAsync(confPath, nginxConf);

        var container = new ContainerBuilder("nginx:alpine")
            // Явное имя pgw-t-*: зачистка серии по --filter name=pgw- (случайные
            // имена testcontainers по фильтрам nginx/alpine не матчатся).
            .WithName("pgw-t-tlsproxy")
            .WithResourceMapping(confPath, "/etc/nginx/nginx.conf")
            .WithResourceMapping(Directory.GetFiles(certsDir, "*.crt")
                .Concat(Directory.GetFiles(certsDir, "*.key")).ToArray(), "/etc/nginx/certs/")
            .WithBindMount("/var/run/docker.sock", "/var/run/docker.sock")
            .WithPortBinding(2376, assignRandomHostPort: true)
            .Build();
        await container.StartAsync(TestContext.Current.CancellationToken);
        var port = container.GetMappedPublicPort(2376);

        var factory = new DockerEngineFactory(new DockerTlsOptions
        {
            CaPem = caPem, ClientCertPem = clientCertPem, ClientKeyPem = clientKeyPem,
        });
        await using var engine = factory.Create($"tcp://localhost:{port}", hostAlias: "local");

        // Act 1: транспорт жив — Ping + ListContainers.
        (await engine.PingAsync(TestContext.Current.CancellationToken)).IsSuccess.Should().BeTrue("TLS-прокси к Engine API жив");
        var listed = await engine.ListContainersAsync("pgw-tls-proxy-test", all: false, TestContext.Current.CancellationToken);
        listed.IsSuccess.Should().BeTrue();

        // Act 2 / Assert 2: полный цикл одноразового контейнера через TLS.
        var name = $"pgw-tls-proxy-test-{Guid.NewGuid():N[..6]}";
        var spec = new ContainerSpec(
            "alpine:3.20", [], "", "", [], name, null, null, null,
            Cmd: ["sleep", "5"]);
        (await engine.CreateContainerAsync(spec, name, TestContext.Current.CancellationToken)).IsSuccess.Should().BeTrue();
        (await engine.StartContainerAsync(name, TestContext.Current.CancellationToken)).IsSuccess.Should().BeTrue();
        (await engine.RemoveContainerAsync(name, force: true, TestContext.Current.CancellationToken)).IsSuccess.Should().BeTrue();

        // Cleanup: сертификаты фикстуры + контейнер.
        await container.DisposeAsync();
        Directory.Delete(certsDir, recursive: true);
    }
}
```

(`ContainerSpec` — позиционный рекорд из `src/PgWorker.Docker/Engine/IDockerEngine.cs:88-98`: `Image, Env, VolumeName, VolumeDest, Ports, Hostname, CpuCores, MemoryBytes, Label, Cmd=null, Network=null, NetworkAliases=null`; при компиляции сверить порядок с файлом. Testcontainers 4.14: если `WithResourceMapping(string[], string)` не подойдёт для каталога сертов — класть каждый файл отдельным `WithResourceMapping(path, "/etc/nginx/certs/<имя>")`.)

Общий PKI-хелпер `src/tests/PgWorker.IntegrationTests/Docker/EngineProxyTestPki.cs` — локальная копия логики `ClusterPki` (`src/KafkaWorker.Core/Templates/ClusterPki.cs:18-58`) в тестовой сборке (та же механика: BasicConstraints CA, SAN DNS+IP, EKU serverAuth+clientAuth, PEM PKCS#8):

```csharp
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace PgWorker.IntegrationTests.Docker;

// PKI-хелпер транспортных интеграционных тестов (t03): фикстурная CA + выпуски
// сертов с SAN — копия механики ClusterPki воркера (KafkaWorker.Core), локально
// в тестовой сборке (тесты PgWorker не тянут зависимость от KafkaWorker.Core).
public static class EngineProxyTestPki
{
    private static readonly Oid ServerAuthOid = new("1.3.6.1.5.5.7.3.1");
    private static readonly Oid ClientAuthOid = new("1.3.6.1.5.5.7.3.2");

    public static (string CaPem, string CaKeyPem) GenerateCa()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            "CN=pgw-test-docker-ca", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        using var ca = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1));
        return (ca.ExportCertificatePem(), ca.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());
    }

    public static (string CertPem, string KeyPem) Issue(
        string caCertPem, string caKeyPem, string commonName,
        IReadOnlyList<string> dnsNames, IPAddress? ip)
    {
        using var caCertificate = X509Certificate2.CreateFromPem(caCertPem);
        using var caKey = RSA.Create();
        caKey.ImportFromPem(caKeyPem);
        using var caWithKey = caCertificate.CopyWithPrivateKey(caKey);
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN={commonName}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        foreach (var dns in dnsNames)
            san.AddDnsName(dns);
        if (ip is not null)
            san.AddIpAddress(ip);
        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(
            new X509EnhancedKeyUsageExtension([ServerAuthOid, ClientAuthOid], critical: false));
        using var certificate = request.Create(
            caWithKey, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(1),
            RandomNumberGenerator.GetBytes(16));
        return (certificate.ExportCertificatePem(), rsa.ExportPkcs8PrivateKeyPem());
    }
}
```

- [ ] **Step 2: SSH-туннель тест (sshd + socat)**

`src/tests/PgWorker.IntegrationTests/Docker/SshTunnelEngineTests.cs`:

```csharp
using System.Net;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using FluentAssertions;
using PgWorker.Docker.Engine;
using Xunit;

namespace PgWorker.IntegrationTests.Docker;

// SSH-туннель к Engine API (spec §5.5/§8.2, t03): sshd-контейнер c key-аутентификацией
// и socat (TCP-LISTEN:2376 → unix:docker.sock); DockerEngineFactory на
// ssh://testuser@localhost:<mapped> — Ping/ListContainers через ForwardedPortLocal.
// Fingerprint-pin: корректный pin — подключается; неверный — отказ host-key.
public class SshTunnelEngineTests
{
    private static SshTunnelOptions Options(string keyPem, string? fingerprint = null) => new()
    {
        KeyPem = keyPem,
        RemoteDaemonHost = "127.0.0.1",
        RemoteDaemonPort = 2376,
        FingerprintSha256 = fingerprint,
    };

    private static async Task<IContainer> StartSshdAsync(string authorizedKeys)
    {
        var keysDir = Path.Combine(Path.GetTempPath(), $"pgw-sshd-{Guid.NewGuid():N}");
        Directory.CreateDirectory(keysDir);
        await File.WriteAllTextAsync(Path.Combine(keysDir, "authorized_keys"), authorizedKeys + "\n");
        return new ContainerBuilder("alpine:3.20")
            // Явное имя pgw-t-*: зачистка серии по --filter name=pgw-.
            .WithName("pgw-t-sshd")
            .WithCommand("sh", "-c", string.Join(" && ", [
                "apk add --no-cache openssh socat >/dev/null",
                "ssh-keygen -A",
                "adduser -D testuser",
                "mkdir -p /home/testuser/.ssh",
                $"cp /keys/authorized_keys /home/testuser/.ssh/authorized_keys",
                "chown -R testuser:testuser /home/testuser/.ssh",
                "chmod 600 /home/testuser/.ssh/authorized_keys",
                "/usr/sbin/sshd -e -p 2222",
                "socat TCP-LISTEN:2376,fork,reuseaddr UNIX-CONNECT:/var/run/docker.sock",
            ]))
            .WithBindMount("/var/run/docker.sock", "/var/run/docker.sock")
            .WithResourceMapping(Path.Combine(keysDir, "authorized_keys"), "/keys/authorized_keys")
            .WithPortBinding(2222, assignRandomHostPort: true)
            .WithPortBinding(2376, assignRandomHostPort: true)
            // Готовность ДО первого Connect: apk add идёт секундами — без wait
            // первый SSH-хендшейк ловит connection refused (флаки). Оба порта:
            // 2222 (sshd) и 2376 (socat — цель форварда туннеля).
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilPortIsAvailable(2222)
                .UntilPortIsAvailable(2376))
            .WithStartupTimeout(TimeSpan.FromSeconds(90)) // ≤ 100 c: упавший прогон падает быстро
            .Build();
        // NOTE: keysDir не удалять до DisposeAsync контейнера (bind source).
    }

    [Fact]
    public async Task SshTunnel_PingListContainers_ThroughForward()
    {
        // Arrange: RSA-ключпар фикстуры → authorized_keys (openssh-формат);
        // sshd+socat контейнер; endpoint ssh://testuser@localhost:<mapped-2222>.
        DockerTrait.SkipIfUnavailable();
        using var rsa = RSA.Create(2048);
        var keyPem = rsa.ExportPkcs8PrivateKeyPem();
        var authorizedKeys = OpenSshPublicKey(rsa, "pgw-test");
        await using var sshd = await StartSshdAsync(authorizedKeys);
        await sshd.StartAsync(TestContext.Current.CancellationToken);
        var sshPort = sshd.GetMappedPublicPort(2222);
        var endpoint = $"ssh://testuser@localhost:{sshPort}";

        var factory = new DockerEngineFactory(ssh: Options(keyPem));
        await using var engine = factory.Create(endpoint, hostAlias: "local");

        // Act: Ping + ListContainers через туннель (форвард → socat → docker.sock).
        var ct = TestContext.Current.CancellationToken;
        (await engine.PingAsync(ct)).IsSuccess.Should().BeTrue("SSH-туннель пробрасывает Engine API");
        (await engine.ListContainersAsync("pgw-ssh-test", all: false, ct)).IsSuccess.Should().BeTrue();
    }

    [Fact]
    public async Task SshTunnel_WrongFingerprintPin_Refused()
    {
        // Arrange: живой sshd, но pin заведомо неверный.
        DockerTrait.SkipIfUnavailable();
        using var rsa = RSA.Create(2048);
        await using var sshd = await StartSshdAsync(OpenSshPublicKey(rsa, "pgw-test"));
        await sshd.StartAsync(TestContext.Current.CancellationToken);
        var endpoint = $"ssh://testuser@localhost:{sshd.GetMappedPublicPort(2222)}";

        // Act / Assert: host-key отвергнут — создание туннеля падает (spec §8.2).
        var factory = new DockerEngineFactory(ssh: Options(rsa.ExportPkcs8PrivateKeyPem(), "SHA256:AAAAAAAAAAAAAAAAAAAAAA=="));
        Assert.ThrowsAny<Exception>(() => factory.Create(endpoint)).Message.Should().NotBeNull();
    }

    [Fact]
    public async Task SshTunnel_CorrectFingerprintPin_Connects()
    {
        // Arrange: первый туннель без pin читает фактический fingerprint (TOFU),
        // второй — с этим pin: подключение обязано пройти (строгая семантика).
        DockerTrait.SkipIfUnavailable();
        using var rsa = RSA.Create(2048);
        var keyPem = rsa.ExportPkcs8PrivateKeyPem();
        await using var sshd = await StartSshdAsync(OpenSshPublicKey(rsa, "pgw-test"));
        await sshd.StartAsync(TestContext.Current.CancellationToken);
        var endpoint = $"ssh://testuser@localhost:{sshd.GetMappedPublicPort(2222)}";

        var probe = new SshHostConnection(
            PgWorker.Docker.Engine.EndpointScheme.Parse(endpoint), Options(keyPem));
        try
        {
            var pinned = probe.FingerprintSha256;
            pinned.Should().NotBeNull("TOFU-подключение фиксирует fingerprint");

            // Act: фабрика с pin.
            var factory = new DockerEngineFactory(ssh: Options(keyPem, pinned));
            await using var engine = factory.Create(endpoint);
            (await engine.PingAsync(TestContext.Current.CancellationToken))
                .IsSuccess.Should().BeTrue("корректный pin не мешает туннелю");
        }
        finally
        {
            await probe.DisposeAsync();
        }
    }

    // ssh-rsa public-строка из RSA-параметров (blob: "ssh-rsa" + mpint e + mpint n).
    private static string OpenSshPublicKey(RSA rsa, string comment)
    {
        var p = rsa.ExportParameters(false);
        static byte[] Len(byte[] b)
            => BitConverter.GetBytes(IPAddress.HostToNetworkOrder(b.Length));
        static byte[] Mpint(byte[] v)
        {
            var padded = (v[0] & 0x80) != 0 ? new byte[] { 0 }.Concat(v).ToArray() : v;
            return Len(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(padded.Length)).ToArray().Length == 4
                ? Len(padded).Concat(padded).ToArray() : throw new InvalidOperationException();
        }
        var name = Len(Encoding.ASCII.GetBytes("ssh-rsa"));
        var blob = name
            .Concat(Mpint(p.Exponent!))
            .Concat(Mpint(p.Modulus!))
            .ToArray();
        return $"ssh-rsa {Convert.ToBase64String(blob)} {comment}";
    }
}
```

(`Mpint` упростить при реализации до читаемого вида без вложенного тернарника — план показывает intent: length-prefixed big-endian; главное — корректный openssh-blob. Если цепочка двух `UntilPortIsAvailable` в Testcontainers 4.14 поведёт себя ненадёжно — оставить в WaitStrategy только 2222 и добавить ретрай-цикл первого `PingAsync` с бюджетом 20 с: socat стартует сразу за sshd, туннель-Ping транзиентно-толерантен.)

- [ ] **Step 3: Прогнать серию с зачисткой**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Release \
  --filter "FullyQualifiedName~TlsEngineProxyTests|FullyQualifiedName~SshTunnelEngineTests" ; \
docker rm -f $(docker ps -aq --filter name=pgw-) 2>/dev/null; \
docker network prune -f
```
Ожидание: PASS 4 кейсов; после зачистки `docker ps -a --format '{{.Names}}'` больше не содержит `pgw-t-tlsproxy`/`pgw-t-sshd`, но содержит живой стенд (`as-*`, `deploy-*`). Существующий plaintext-прогон не сломан: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Release --filter FullyQualifiedName~DockerDriverTests` — PASS + та же зачистка.

- [ ] **Step 4: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
git add src/tests/PgWorker.IntegrationTests/Docker && \
git commit -m "test(docker): интеграция TLS-proxy (nginx stream ssl) и SSH-туннеля (sshd+socat) к Engine API (t03 Ф1)"
```

---

### Task 5: mTLS HTTP API PgWorker — `ApiTlsEndpoints`, Options, Program, удаление `PGW_API_KEY`

**Files:**
- Create: `src/PgWorker.App/Api/ApiTlsEndpoints.cs`
- Modify: `src/PgWorker.App/Options.cs` (`ApiOptions`: −`ApiKey`, +`Tls`; `DockerOptions`: +`Tls`/`Ssh` — типы из `PgWorker.Docker.Engine`)
- Modify: `src/PgWorker.App/Program.cs`
- Delete: `src/PgWorker.App/Api/ApiKeyMiddleware.cs`
- Modify: `src/PgWorker.App/appsettings.json` (секции `Api:Tls`, `Docker:Tls/Ssh: null`)
- Test: `src/tests/PgWorker.UnitTests/App/ApiTlsEnvBindingsTests.cs`
- Test: `src/tests/PgWorker.IntegrationTests/Api/MtlsApiTests.cs` (новый)
- Modify: `src/tests/PgWorker.IntegrationTests/Api/PgWorkerApiFactory.cs`, `Api/MetricsApiFactory.cs` (+`AllowInsecureHttp=true`), `Api/CreateClusterApiTests.cs` (−ApiKey-кейс), `Api/MetricsTests.cs` (−ApiKey-кейсы)

**Interfaces:**
- Consumes: `DockerTlsOptions.ApplyEnvOverrides`/`SshTunnelOptions.ApplyEnvOverrides` (Tasks 2–3), паттерн `KafkaWorker.App/Api/TlsEndpoints.cs`.
- Produces:
  - `ApiTlsEndpoints.EnvBindings` (6 записей `PGW_API_TLS_{CERT,KEY,CLIENT_CA}`[+`_PATH`] → `PgWorker:Api:Tls:*`); `ApplyEnvOverrides(ConfigurationManager, Func<string,string?>?)`; `static int ResolvePort(ConfigurationManager)`; `ConfigureMtls(WebApplicationBuilder)`;
  - `PgWorkerOptions.Api.Tls` (`TlsOptions { ServerCertPem|ServerCertPath, ServerKeyPem|ServerKeyPath, ClientCaPem|ClientCaPath, AllowInsecureHttp=false }`); `PgWorkerOptions.Docker.Tls` (`DockerTlsOptions?`), `.Ssh` (`SshTunnelOptions?`).

- [ ] **Step 1: Юнит-тесты env-биндингов и порт-резолва (падающие)**

`src/tests/PgWorker.UnitTests/App/ApiTlsEnvBindingsTests.cs`:

```csharp
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using PgWorker.App.Api;
using Xunit;

// Env-секреты mTLS API (arch/14 §1.1, t03): PEM и _PATH; порт — из
// ASPNETCORE_URLS/urls (E2E поднимает хост-процесс на свободном порту), иначе 8080.
public class ApiTlsEnvBindingsTests
{
    [Fact]
    public void ApplyEnvOverrides_PgApiTlsKeysMapped()
    {
        // Arrange
        var env = new Dictionary<string, string>
        {
            ["PGW_API_TLS_CERT"] = "cert-pem",
            ["PGW_API_TLS_KEY_PATH"] = "/tls/pgserver.key",
            ["PGW_API_TLS_CLIENT_CA_PATH"] = "/tls/ca.pem",
        };
        var config = new ConfigurationManager();

        // Act
        ApiTlsEndpoints.ApplyEnvOverrides(config, key => env.GetValueOrDefault(key));

        // Assert
        config["PgWorker:Api:Tls:ServerCertPem"].Should().Be("cert-pem");
        config["PgWorker:Api:Tls:ServerKeyPath"].Should().Be("/tls/pgserver.key");
        config["PgWorker:Api:Tls:ClientCaPath"].Should().Be("/tls/ca.pem");
        ApiTlsEndpoints.EnvBindings.Should().HaveCount(6);
    }

    [Theory]
    [InlineData("https://127.0.0.1:18443", 18443)]
    [InlineData("http://127.0.0.1:9000;https://127.0.0.1:19001", 19001)] // последний binding
    [InlineData("", 8080)]
    [InlineData(null, 8080)]
    public void ResolvePort_FromUrlsConfig_OrDefault(string? urls, int expected)
    {
        // Arrange
        var config = new ConfigurationManager();
        if (urls is not null) config["urls"] = urls;

        // Act / Assert
        ApiTlsEndpoints.ResolvePort(config).Should().Be(expected);
    }
}
```

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release --filter FullyQualifiedName~ApiTlsEnvBindingsTests
```
Ожидание: FAIL (типа нет).

- [ ] **Step 2: Реализовать `ApiTlsEndpoints`**

`src/PgWorker.App/Api/ApiTlsEndpoints.cs` — копия `src/KafkaWorker.App/Api/TlsEndpoints.cs` (целиком, ~103 строки) со следующими отличиями (ренейм-механика 1:1, паттерны `LoadServerCertificate`/`LoadClientCa`/`ValidateChain`/`ReadFile` — без изменений):

```csharp
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.Configuration;
using PgWorker.App;

namespace PgWorker.App.Api;

// mTLS HTTP-грани PgWorker (arch/14 §1.1, t03): вся грань (вкл. /healthz и
// /metrics) — только TLS; клиентские серты — per-install API-CA (единая пакета
// с KafkaWorker, решение О1). Вызывается на WebApplicationBuilder ДО Build().
// Сертификаты живут всё приложение: ClientCertificateValidation — на каждом
// хендшейке (без using).
public static class ApiTlsEndpoints
{
    // env-секреты → конфиг-дерево (arch/14 §4): PEM-значения и _PATH-файлы.
    public static readonly (string Env, string Key)[] EnvBindings =
    [
        ("PGW_API_TLS_CERT", "PgWorker:Api:Tls:ServerCertPem"),
        ("PGW_API_TLS_KEY", "PgWorker:Api:Tls:ServerKeyPem"),
        ("PGW_API_TLS_CLIENT_CA", "PgWorker:Api:Tls:ClientCaPem"),
        ("PGW_API_TLS_CERT_PATH", "PgWorker:Api:Tls:ServerCertPath"),
        ("PGW_API_TLS_KEY_PATH", "PgWorker:Api:Tls:ServerKeyPath"),
        ("PGW_API_TLS_CLIENT_CA_PATH", "PgWorker:Api:Tls:ClientCaPath"),
    ];

    public static void ApplyEnvOverrides(ConfigurationManager configuration, Func<string, string?>? getenv = null)
    {
        getenv ??= Environment.GetEnvironmentVariable;
        foreach (var (env, key) in EnvBindings)
        {
            var value = getenv(env);
            if (!string.IsNullOrWhiteSpace(value))
                configuration[key] = value;
        }
    }

    // Порт Kestrel: из urls/ASPNETCORE_URLS (E2E поднимает хост-процесс на
    // свободном порту; жёсткий 8080 kafka-прецедента НЕ переиспользуется),
    // иначе дефолт 8080.
    public static int ResolvePort(ConfigurationManager configuration)
    {
        var urls = configuration["urls"] ?? Environment.GetEnvironmentVariable("ASPNETCORE_URLS");
        if (string.IsNullOrWhiteSpace(urls))
            return 8080;
        foreach (var binding in urls.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Reverse())
        {
            if (Uri.TryCreate(binding, UriKind.Absolute, out var uri) && uri.Port > 0)
                return uri.Port;
        }

        return 8080;
    }

    public static void ConfigureMtls(WebApplicationBuilder builder)
    {
        var tls = builder.Configuration.GetSection("PgWorker:Api:Tls").Get<TlsOptions>() ?? new TlsOptions();
        if (tls.AllowInsecureHttp)
            return; // без TLS — только WAF-тесты; warning логирует Program.cs

        // Fail-fast при конфигурации хоста: серт/ключ/ClientCA обязаны быть заданы.
        var serverCert = LoadServerCertificate(tls) ?? throw new ApplicationException(
            "PgWorker:Api:Tls: серверный серт/ключ не заданы (PGW_API_TLS_CERT/KEY или *_PATH; arch/14 §1.1)");
        var clientCa = LoadClientCa(tls) ?? throw new ApplicationException(
            "PgWorker:Api:Tls: ClientCA не задан (PGW_API_TLS_CLIENT_CA[_PATH])");

        var port = ResolvePort(builder.Configuration);
        // Явный Listen подавляет default-URL — только mTLS-грань.
        builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port, listenOptions => listenOptions.UseHttps(
            new HttpsConnectionAdapterOptions
            {
                ServerCertificate = serverCert,
                ClientCertificateMode = ClientCertificateMode.RequireCertificate,
                ClientCertificateValidation = (certificate, _, _) => ValidateChain(certificate, clientCa),
            })));
    }

    // Валидация цепочки клиентского серта против per-install API-CA (копия
    // KafkaWorker.App/Api/TlsEndpoints.cs:63-75, тексты — PgWorker:Api:Tls).
    private static bool ValidateChain(X509Certificate2? certificate, X509Certificate2 clientCa)
    {
        if (certificate is null)
            return false;
        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(clientCa);
        // Per-install приватная CA не публикует CRL/OCSP — онлайн-проверка отзыва
        // всегда падала бы и отвергала валидные клиентские серты.
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        return chain.Build(certificate);
    }

    private static X509Certificate2? LoadServerCertificate(TlsOptions tls)
    {
        var certPem = tls.ServerCertPem ?? ReadFile(tls.ServerCertPath);
        var keyPem = tls.ServerKeyPem ?? ReadFile(tls.ServerKeyPath);
        if (certPem is null || keyPem is null)
            return null;

        // PFX round-trip: ключ из CreateFromPem эфемерный (не экспортируемый) —
        // SslStream (macOS) не может его использовать без ре-импорта.
        var pem = X509Certificate2.CreateFromPem(certPem, keyPem);
        return X509CertificateLoader.LoadPkcs12(pem.Export(X509ContentType.Pkcs12), null);
    }

    private static X509Certificate2? LoadClientCa(TlsOptions tls)
    {
        var caPem = tls.ClientCaPem ?? ReadFile(tls.ClientCaPath);
        if (caPem is null)
            return null;
        var ca = X509Certificate2.CreateFromPem(caPem);
        return OperatingSystem.IsMacOS()
            ? X509CertificateLoader.LoadPkcs12(ca.Export(X509ContentType.Pkcs12), null)
            : ca;
    }

    private static string? ReadFile(string? path)
        => path is null || !File.Exists(path) ? null : File.ReadAllText(path).Trim();
}
```

- [ ] **Step 3: Options + appsettings**

`src/PgWorker.App/Options.cs`:
- `ApiOptions`: удалить свойство `ApiKey`; добавить `public TlsOptions Tls { get; set; } = new();`. Класс `TlsOptions` — копия `KafkaWorker.App/Options.cs:65-87` (6 PEM/PATH-свойств + `AllowInsecureHttp`) с doc-комментариями под `PGW_API_TLS_*`/arch/14.
- `DockerOptions`: добавить:

```csharp
    /// <summary>TLS к Engine API (arch/14 §2.2.1, t03); null — без TLS (unix/dev).</summary>
    public DockerTlsOptions? Tls { get; set; }

    /// <summary>SSH-туннели ssh://-хостов (arch/14 §2.2.1, t03); null — дефолты.</summary>
    public SshTunnelOptions? Ssh { get; set; }
```

(+ `using PgWorker.Docker.Engine;`)

`src/PgWorker.App/appsettings.json`: секция `"Api"` → `{ "AdvertiseUrl": "", "EnableSeedEndpoint": false, "Tls": { "AllowInsecureHttp": false } }`; в `"Docker"` добавить `"Tls": null, "Ssh": null`.

- [ ] **Step 4: Program.cs — mTLS и удаление ApiKey**

`src/PgWorker.App/Program.cs`:
1. Сразу после `var builder = WebApplication.CreateBuilder(args);` (строка ~29) добавить:

```csharp
// t03: env-секреты TLS (API / Docker / SSH) → конфиг-дерево до всего остального.
PgWorker.App.Api.ApiTlsEndpoints.ApplyEnvOverrides(builder.Configuration);
PgWorker.Docker.Engine.DockerTlsOptions.ApplyEnvOverrides(builder.Configuration);
PgWorker.Docker.Engine.SshTunnelOptions.ApplyEnvOverrides(builder.Configuration);
```

2. Валидации (заменить блок `AddOptions<PgWorkerOptions>` строк ~34–37):

```csharp
builder.Services.AddOptions<PgWorkerOptions>()
    .Validate(o => !string.IsNullOrWhiteSpace(o.Api.AdvertiseUrl),
        "PgWorker:Api:AdvertiseUrl не задан (URL API, достижимый панелью; env PGW_API_ADVERTISE_URL)")
    .Validate(o => o.Api.Tls.AllowInsecureHttp
        || o.Api.AdvertiseUrl.StartsWith("https://", StringComparison.Ordinal),
        "PgWorker:Api:AdvertiseUrl обязан быть https:// (mTLS-only API, arch/14 §1.1)")
    .ValidateOnStart();
```

3. Сразу после блока валидаций:

```csharp
// mTLS HTTP API (arch/14 §1.1, t03): Kestrel с серверным сертом и требованием
// клиентского серта per-install API-CA (порт — из ASPNETCORE_URLS/urls, иначе 8080).
ApiTlsEndpoints.ConfigureMtls(builder);
```

4. Регистрация фабрики docker (заменить `builder.Services.AddSingleton<DockerEngineFactory>();` строку ~127):

```csharp
builder.Services.AddSingleton(sp =>
{
    var docker = sp.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Docker;
    return new DockerEngineFactory(
        docker.Tls, docker.Ssh,
        sp.GetRequiredService<ILoggerFactory>().CreateLogger<DockerEngineFactory>(),
        sp.GetRequiredService<ILoggerFactory>());
});
```

(сигнатура конструктора фабрики из Task 3: `tls`, `ssh`, `logger`, `loggerFactory`.)

5. Удалить строку `app.UseMiddleware<ApiKeyMiddleware>();` (~396); комментарий над `AddAppMetrics` (~41–43) заменить: `// Метрики (arch/18 §3): /metrics на том же mTLS-Kestrel-порту, что /healthz (t03).`
6. После `var app = builder.Build();` добавить (паттерн KafkaWorker Program.cs:379-381):

```csharp
if (app.Services.GetRequiredService<IOptions<PgWorkerOptions>>().Value.Api.Tls.AllowInsecureHttp)
    app.Logger.LogWarning(
        "PgWorker:Api:Tls:AllowInsecureHttp=true — HTTP без TLS (ТОЛЬКО WAF-тесты, arch/14 §1.1)");
```

7. `git rm src/PgWorker.App/Api/ApiKeyMiddleware.cs`.

- [ ] **Step 5: Обновить WAF-фабрики и выбросить ApiKey-кейсы**

- `src/tests/PgWorker.IntegrationTests/Api/PgWorkerApiFactory.cs`: в in-memory-словарь добавить `["PgWorker:Api:Tls:AllowInsecureHttp"] = "true"`; `AdvertiseUrl` → `https://localhost:9999`.
- `src/tests/PgWorker.IntegrationTests/Api/MetricsApiFactory.cs`: то же (`https://localhost:9997`); в конец файла добавить (паттерн `KafkaWorker.IntegrationTests/Api/MetricsApiFactory.cs:60-65`):

```csharp
// Env-флаг до первого Program.Main процесса тестов (WAF-хосты не задают серты).
internal static class TestEnv
{
    [System.Runtime.CompilerServices.ModuleInitializer]
    internal static void SetInsecureEnv()
        => Environment.SetEnvironmentVariable("PgWorker__Api__Tls__AllowInsecureHttp", "true");
}
```

- `src/tests/PgWorker.IntegrationTests/Api/CreateClusterApiTests.cs`: удалить кейс `ApiKey_WithoutHeader_401_WithHeader_201` (~строки 149–170).
- `src/tests/PgWorker.IntegrationTests/Api/MetricsTests.cs`: удалить `ApiKeyFactory` и кейсы `Metrics_Responds_200_WithoutApiKey_EvenWhenApiKeySet` / `Metrics_ApiKeySecuredApi_StaysProtected`; шапку класса-комментария переписать под mTLS; кейс `Metrics_WorkerSeries_AfterFirstTick` остаётся; добавить простой кейс `Metrics_Responds_200_PrometheusText` (200 + `"dotnet_"` в теле — AAA-комментарии).

- [ ] **Step 6: `MtlsApiTests` для PgWorker**

`src/tests/PgWorker.IntegrationTests/Api/MtlsApiTests.cs` — порт `KafkaWorker.IntegrationTests/Api/MtlsApiTests.cs` (целиком, ~115 строк) с отличиями:
- локальный `TestPki` (как в Task 2) вместо `ClusterPki`; CN хоста `"pgworker"`;
- конфиг-ключи `PgWorker:Api:Tls:*`; изоляция `"PgWorker:Api:Tls:AllowInsecureHttp"] = "false"`;
- порт: зонд свободного порта (локальный хелпер `FreePort()` по `TcpListener.Create(0)` — паттерн `E2eFixture.FreePort`) и `builder.Configuration["urls"] = $"https://localhost:{port}"` (проверяет `ResolvePort` на реальном хосте); вызов `ApiTlsEndpoints.ConfigureMtls(builder)` (без порта);
- кейс `Mtls_NoClientCert_Refused_WithCert_Ok` — тот же контракт: без серта `HttpRequestException`, с сертом `/api/ping`=200 и `/healthz`=200 за TLS; TLS 1.2 (macOS), `RemoteCertificateValidationCallback = true`.

Дополнительно в `MtlsApiTests` — кейс https-валидации advertise (spec §8.1; класс коллекции `PgApiCollection`, ctor-инъекция `PgApiFixture fx`):

```csharp
    // Оверрайд базовой фабрики: http-advertise при выключенном AllowInsecureHttp.
    private sealed class HttpAdvertiseFactory(PgWorker.IntegrationTests.Etcd.EtcdFixture etcd)
        : PgWorkerApiFactory(etcd)
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            base.ConfigureWebHost(builder);
            builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["PgWorker:Api:AdvertiseUrl"] = "http://localhost:9999",
                ["PgWorker:Api:Tls:AllowInsecureHttp"] = "false",
            }));
        }
    }

    [Fact]
    public void AdvertiseUrl_HttpWithoutInsecureFlag_HostStartFails()
    {
        // Arrange: валидный http-advertise + mTLS-канон (AllowInsecureHttp=false).
        using var factory = new HttpAdvertiseFactory(fx.Etcd);

        // Act / Assert: ValidateOnStart — старт хоста падает fail-fast
        // (AdvertiseUrl обязан быть https://, arch/14 §1.1).
        Assert.ThrowsAny<Exception>(() => factory.CreateClient());
    }
```

(положительную ветку `https-advertise + AllowInsecureHttp=true` фиксируют все существующие WAF-серии фабрик — отдельный кейс не нужен.)

- [ ] **Step 7: Прогоны с зачисткой**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.UnitTests -c Release \
  --filter "FullyQualifiedName~ApiTlsEnvBindingsTests|FullyQualifiedName~HealthTests" && \
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.IntegrationTests -c Release \
  --filter "FullyQualifiedName~MtlsApiTests|FullyQualifiedName~CreateClusterApiTests|FullyQualifiedName~MetricsTests|FullyQualifiedName~SeedApiTests" ; \
docker rm -f $(docker ps -aq --filter name=pgw-) 2>/dev/null; docker network prune -f
```
Ожидание: PASS (MtlsApiTests на реальном сокете; WAF-серии работают в AllowInsecureHttp; grep `PGW_API_KEY` по `src/` и `deploy/` пуст). Зачистка — только по name-фильтру `pgw-` (стенд `as-*`/`deploy-*` жив); WAF/MtlsApiTests docker-контейнеров с именами не оставляют (etcd-фикстуры подбирает ryuk, сети — prune).

- [ ] **Step 8: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
git add -A src/PgWorker.App src/tests/PgWorker.UnitTests src/tests/PgWorker.IntegrationTests && \
git commit -m "feat(app): mTLS-only HTTP API PgWorker — ApiTlsEndpoints, PGW_API_KEY/X-Api-Key удалены (t03 Ф2)"
```

---

### Task 6: Панель — `WorkerTls`-переименование, gateway без X-Api-Key

**Files:**
- Modify: `src/AdminPanel.Etcd/Workers/WorkerApiOptions.cs` (`KafkaTlsOptions` → `WorkerTlsOptions`, свойство `KafkaTls` → `WorkerTls`, `PgApiKey` удалить)
- Modify: `src/AdminPanel.Etcd/Workers/WorkerTlsHandler.cs` (env `WORKERS_PANEL_TLS_*`, `Build(WorkerTlsOptions)`)
- Modify: `src/AdminPanel.Etcd/Workers/WorkerApiGateway.cs` (`ApiKeyOf`/`X-Api-Key` удалить)
- Modify: `src/AdminPanel.Etcd/ModuleExtensions.cs` (`.Value.KafkaTls` → `.Value.WorkerTls`; комментарий «единый серт на оба воркера»)
- Modify: `src/AdminPanel.Etcd/Workers/WorkerHealthPoller.cs:51` (комментарий: «`/healthz` за mTLS» вместо упоминания ApiKeyMiddleware)
- Test: `src/tests/AdminPanel.UnitTests/Workers/WorkerTlsHandlerTests.cs`; `Workers/WorkerApiGatewayTests.cs`

**Interfaces:**
- Consumes: паттерн `WorkerTlsHandler` (существующий).
- Produces: `WorkerTlsOptions` (тот же набор свойств, что `KafkaTlsOptions`); `WorkerTlsHandler.EnvBindings` → ключи `WORKERS_PANEL_TLS_{CERT,KEY,SERVER_CA}`[+`_PATH`] → `AdminPanel:Workers:WorkerTls:*`.

- [ ] **Step 1: Обновить юнит-тесты (падающие)**

`WorkerTlsHandlerTests.cs`: заменить `KafkaTlsOptions` → `WorkerTlsOptions` (3 места), env-ключи кейса `ApplyEnvOverrides_PanelTlsKeysMapped` → `WORKERS_PANEL_TLS_CERT_PATH`/`WORKERS_PANEL_TLS_SERVER_CA_PATH`, ожидания конфига → `AdminPanel:Workers:WorkerTls:*`; doc-комментарий класса: «клиентский серт панели — ЕДИНЫЙ на оба воркера (t03-pg)».

`WorkerApiGatewayTests.cs` (хелперы файла: `NewGateway(pg, kafka, options)`, `PgStore(params WorkerEndpoint[])`, стаб `WorkerStub` с полями `LastRequestedBy`/`LastApiKey`/`LastBody`): в существующий кейс `SendAsync_201WithBody_ReturnsResult` добавить assert `stub.LastApiKey.Should().BeNull();` и новый кейс (полный код; `WorkerEndpoint("i1", stub.Url, 1)` — по образцу соседних):

```csharp
    [Fact]
    public async Task SendAsync_PgMutation_NoApiKeyHeader_Sent()
    {
        // Arrange: живой pgworker-эндпоинт (стаб) в снапшоте; дефолтные опции.
        using var stub = new WorkerStub();
        var gateway = NewGateway(PgStore(new WorkerEndpoint("i1", stub.Url, 1)));

        // Act: мутация через шлюз (контракт t03-pg: mTLS-only у обоих воркеров).
        var result = await gateway.SendAsync(
            "pgworker", HttpMethod.Post, "/api/clusters",
            new { name = "smoke" }, requestedBy: "opsuser", CancellationToken.None);

        // Assert: X-Api-Key в исходящем запросе НЕТ; X-Requested-By сохранён.
        result.StatusCode.Should().Be(201);
        stub.LastApiKey.Should().BeNull();
        stub.LastRequestedBy.Should().Be("opsuser");
    }
```

(после удаления `PgApiKey` из `WorkerApiOptions` кейсы, конструирующие опции с этим полем, — поправить компилятор; `LastApiKey`-поле стаба остаётся — на нём держится кейс.)

- [ ] **Step 2: Реализовать переименование**

- `WorkerApiOptions.cs`: `public WorkerTlsOptions WorkerTls { get; set; } = new();`; удалить `PgApiKey`; класс `KafkaTlsOptions` переименовать в `WorkerTlsOptions` (doc: «mTLS обращений в API ОБОИХ воркеров (arch/02 §2.3.2, t03): единый клиентский серт per-install API-CA + ServerCA»).
- `WorkerTlsHandler.cs`: `EnvBindings` → 6 записей `WORKERS_PANEL_TLS_{CERT,KEY,SERVER_CA}`[+`_PATH`] → `AdminPanel:Workers:WorkerTls:*`; `Build(WorkerTlsOptions tls)`. Логика Build/ValidateChain — без изменений.
- `WorkerApiGateway.cs`: удалить метод `ApiKeyOf` (строки 85–91) и строки 39, 48–49 (`apiKey`/`Headers.Add("X-Api-Key", ...)`); doc-комментарий класса: «аутентификация — mTLS клиентским сертом (WorkerTlsHandler)».
- `ModuleExtensions.cs:44-46`: `.Value.KafkaTls` → `.Value.WorkerTls`; комментарий: «единый серт панели на оба API (t03-pg)».

- [ ] **Step 3: Прогнать панельные тесты**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/AdminPanel.UnitTests -c Release \
  --filter "FullyQualifiedName~WorkerTlsHandlerTests|FullyQualifiedName~WorkerApiGatewayTests|FullyQualifiedName~WorkerHealthPollerTests|FullyQualifiedName~WorkerProxyCommandTests" && \
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release
```
Ожидание: PASS; сборка 0 warnings; `grep -rn "KafkaTls\|PgApiKey" src/` — пусто.

- [ ] **Step 4: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
git add src/AdminPanel.Etcd src/tests/AdminPanel.UnitTests && \
git commit -m "feat(panel): WorkerTls — единый mTLS-серт панели на оба воркера, X-Api-Key удалён (t03 Ф3)"
```

---

### Task 7: Поставка — `gen.sh` (восстановление+расширение), `gen-docker.sh`, compose, Dockerfile, .env.example

**Files:**
- Modify: `deploy/tls/.gitignore` (`*` → `*`,`!.gitignore`,`!gen.sh`,`!gen-docker.sh`)
- Create: `deploy/tls/gen.sh`
- Create: `deploy/tls/gen-docker.sh`
- Modify: `deploy/docker-compose.yml` (pgworker)
- Modify: `deploy/.env.example`
- Modify: `docker/PgWorker.Dockerfile` (HEALTHCHECK mTLS)

**Interfaces:**
- Consumes: имена файлов из Task 5 env (`pgserver.crt`, `pgserver.key`, `ca.pem`),_HEALTHCHECK из `docker/KafkaWorker.Dockerfile:21-23`.
- Produces: TLS-пакет `kfw-install-ca` = `ca.pem/ca.key` + серверные `server.crt/key` (kafkaworker), `pgserver.crt/key` (pgworker; SAN: `pgworker`, `localhost`, `host.docker.internal`, `127.0.0.1` — R13) + клиентские `panel`, `seed`, `prometheus`, `healthcheck` (`.crt/.key`); docker-пакет `docker-ca.pem/key`, `pgworker-docker.crt/key`, `docker-server.crt/key` (SAN по аргументу). Volume `pgw-api-tls` наполняет `00-up.sh` (Task 8).

- [ ] **Step 1: `.gitignore` + `gen.sh`**

`deploy/tls/.gitignore` (заменить содержимое):

```
*
!.gitignore
!gen.sh
!gen-docker.sh
```

`deploy/tls/gen.sh` (новый; gen.sh ранее НЕ был в git — проглочен `*`-ignore в e14ba9c — файл создаётся заново; существующие выпущенные `ca.pem/server.*` остаются вне git):

```bash
#!/usr/bin/env bash
# Per-install API TLS-пакет (t03, arch/14 §1.1 / arch/16 §1.1, решение О1):
# ЕДИНАЯ CA kfw-install-ca на оба воркера. Серверные серты: server (kafkaworker),
# pgserver (pgworker); клиентские: panel (мутации панели), seed (стендовый сид),
# prometheus (scrape), healthcheck (docker HEALTHCHECK). Идемпотентен: при
# существующем ca.pem не делает ничего (ротация — вручную: rm ca.* и перезапуск).
# Выпущенные файлы в git не попадают (deploy/tls/.gitignore).
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
if [ -f ca.pem ]; then echo "TLS-пакет уже есть (ca.pem); ротация — rm ca.* и перезапуск"; exit 0; fi
DAYS=3650

openssl genrsa -out ca.key 4096 2>/dev/null
openssl req -x509 -new -nodes -key ca.key -sha256 -days "$DAYS" \
  -subj "/CN=kfw-install-ca" -out ca.pem

issue() { # name cn eku san
  local name="$1" cn="$2" eku="$3" san="$4"
  openssl genrsa -out "$name.key" 2048 2>/dev/null
  openssl req -new -key "$name.key" -subj "/CN=$cn" -out "$name.csr"
  local ext="basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=$eku"
  [ -n "$san" ] && ext="$ext
subjectAltName=$san"
  openssl x509 -req -in "$name.csr" -CA ca.pem -CAkey ca.key -CAcreateserial \
    -days "$DAYS" -sha256 -out "$name.crt" 2>/dev/null \
    -extfile <(printf '%s\n' "$ext")
  rm -f "$name.csr"
}

# серверные (SAN покрывает compose-DNS, localhost, host-gateway — R13)
issue server     kafkaworker serverAuth "DNS:kafkaworker,DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1"
issue pgserver   pgworker    serverAuth "DNS:pgworker,DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1"
# клиентские (различимость в журналах сервера, независимый отзыв)
issue panel      panel       clientAuth ""
issue seed       seed        clientAuth ""
issue prometheus prometheus  clientAuth ""
issue healthcheck healthcheck clientAuth ""
chmod 600 ca.key ./*.key
echo "✓ TLS-пакет kfw-install-ca: ca.pem, server.*, pgserver.*, panel.*, seed.*, prometheus.*, healthcheck.*"
```

`chmod +x deploy/tls/gen.sh`.

- [ ] **Step 2: `gen-docker.sh`**

`deploy/tls/gen-docker.sh` (новый):

```bash
#!/usr/bin/env bash
# Per-install docker-CA (t03, arch/14 §2.2.1): изолированное от API-пакеты
# доверие docker-хостов. Выпускает: docker-ca.pem/key (CN=pgw-docker-ca),
# клиентскую пару воркера pgworker-docker.* (PGW_DOCKER_TLS_{CERT,KEY}),
# серверный серт демона docker-server.* (SAN по первому аргументу; демоны
# поднимаются с --tlsverify). Идемпотентен по docker-ca.pem.
# Использование: bash gen-docker.sh <host-dns|ip> [доп. SAN через запятую: DNS:x,IP:y]
set -euo pipefail
[ $# -ge 1 ] || { echo "usage: gen-docker.sh <host-dns|ip> [extra-san]"; exit 1; }
HOST="$1"; EXTRA="${2:-}"
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
DAYS=3650
[ -f docker-ca.pem ] || {
  openssl genrsa -out docker-ca.key 4096 2>/dev/null
  openssl req -x509 -new -nodes -key docker-ca.key -sha256 -days "$DAYS" \
    -subj "/CN=pgw-docker-ca" -out docker-ca.pem
}
SAN="DNS:${HOST},IP:127.0.0.1"
case "$HOST" in *:*) SAN="IP:${HOST},IP:127.0.0.1" ;; esac
[ -n "$EXTRA" ] && SAN="${SAN},${EXTRA}"

issue() { # name cn eku san
  local name="$1" cn="$2" eku="$3" san="$4"
  openssl genrsa -out "$name.key" 2048 2>/dev/null
  openssl req -new -key "$name.key" -subj "/CN=$cn" -out "$name.csr"
  openssl x509 -req -in "$name.csr" -CA docker-ca.pem -CAkey docker-ca.key -CAcreateserial \
    -days "$DAYS" -sha256 -out "$name.crt" 2>/dev/null \
    -extfile <(printf 'basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=%s\nsubjectAltName=%s\n' "$eku" "$san")
  rm -f "$name.csr"
}
issue pgworker-docker pgworker  clientAuth "DNS:pgworker"
issue docker-server    "$HOST"   serverAuth "$SAN"
chmod 600 docker-ca.key ./*.key 2>/dev/null || true
echo "✓ docker-пакет pgw-docker-ca: docker-ca.pem, pgworker-docker.*, docker-server.* (SAN: $SAN)"
echo "  демон: dockerd --tlsverify --tlscacert=docker-ca.pem --tlscert=docker-server.crt --tlskey=docker-server.key"
echo "  воркер: PGW_DOCKER_TLS_CA_PATH=docker-ca.pem PGW_DOCKER_TLS_{CERT,KEY}_PATH=pgworker-docker.{crt,key}"
```

`chmod +x deploy/tls/gen-docker.sh`.

- [ ] **Step 3: Проверка скриптов (в изолированной копии, не трогая прод-пакет)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
TMP=$(mktemp -d) && cp deploy/tls/gen.sh deploy/tls/gen-docker.sh "$TMP/" && \
bash "$TMP/gen.sh" && ls "$TMP" | sort | tr '\n' ' ' && \
bash "$TMP/gen.sh" && bash "$TMP/gen-docker.sh" dock1.example.net && ls "$TMP" | grep -c docker-ca.pem && rm -rf "$TMP"
```
Ожидание: первая генерация создаёт 15 файлов (`ca.key ca.pem ca.srl healthcheck.crt healthcheck.key panel.crt panel.key pgserver.crt pgserver.key prometheus.crt prometheus.key seed.crt seed.key server.crt server.key`); повторный запуск — идемпотентный выход; gen-docker — `docker-ca.pem` создан. Проверить валидность: `openssl verify -CAfile "$TMP/ca.pem" "$TMP/pgserver.crt"` (выполнить до rm).

- [ ] **Step 4: deploy compose + Dockerfile + .env.example**

`deploy/docker-compose.yml`, сервис `pgworker`:
- volumes: добавить `- pgw-api-tls:/tls:ro` и RBAC-комментарий:

```yaml
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - pgw-snapshots:/snapshots
      # mTLS HTTP API (t03, arch/14 §1.1): per-install пакет kfw-install-ca
      # (bash deploy/tls/gen.sh; наполнение volume — dev-stand 00-up.sh).
      - pgw-api-tls:/tls:ro
      # RBAC вместо сокета наружу (arch/14 §2.2.1, НЕ включено: текущие стенды
      # работают root'ом контейнера): выделенный пользователь + группа docker
      # host-машины — раскомментировать и указать gid (getent group docker):
      #   user: "10001:10001"
      #   group_add: ["<gid docker>"]
      # и заменить bind сокета на tcp://…:2376 (mTLS, deploy/tls/gen-docker.sh).
```

- environment: `PgWorker__Api__AdvertiseUrl: ${PGW_API_ADVERTISE_URL:-https://host.docker.internal:8080}`; удалить строку `PgWorker__Api__ApiKey: ${PGW_API_KEY:-}`; добавить:

```yaml
      PGW_API_TLS_CERT_PATH: /tls/pgserver.crt
      PGW_API_TLS_KEY_PATH: /tls/pgserver.key
      PGW_API_TLS_CLIENT_CA_PATH: /tls/ca.pem
```

- комментарий над advertise (строки 39–41) обновить: «mTLS-only, ключи-серты из /tls»; volumes-секция файла: добавить `pgw-api-tls:`.

`docker/PgWorker.Dockerfile` (строки 19–20 заменить — 1:1 KafkaWorker.Dockerfile:19-23):

```dockerfile
# /healthz за mTLS (t03, arch/14 §1.1): клиентская пара healthcheck из
# per-install TLS-пакета (deploy/tls/gen.sh; volume /tls:ro в compose).
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -sf --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key \
    https://localhost:8080/healthz || exit 1
```

`deploy/.env.example`: удалить строку `PGW_API_KEY=`; `PGW_API_ADVERTISE_URL=http://…` → `https://host.docker.internal:8080`; добавить блок документации:

```bash
# mTLS HTTP API воркеров (t03, arch/14 §1.1/arch/16 §1.1): per-install пакет
# `bash deploy/tls/gen.sh` (ca.pem, server.*/pgserver.*, panel/seed/prometheus/
# healthcheck.*). Стендовые compose монтируют каталог/volume с пакетом; здесь
# настраивается только advertise (https). Альтернатива путям — PEM-значения
# PGW_API_TLS_{CERT,KEY,CLIENT_CA} (без _PATH).
# Прод-транспорт к Engine API (arch/14 §2.2.1): tcp://:2376+mTLS (пакет
# `bash deploy/tls/gen-docker.sh <host>`: PGW_DOCKER_TLS_{CA,CERT,KEY}_PATH)
# или ssh://user@host (PGW_DOCKER_SSH_KEY[_PATH], опц. PGW_DOCKER_SSH_FINGERPRINT).
```

- [ ] **Step 5: Проверка и коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
bash -n deploy/tls/gen.sh && bash -n deploy/tls/gen-docker.sh && \
grep -rn "PGW_API_KEY" deploy/ docker/ || echo "PGW_API_KEY зачищен" ; \
git add deploy/tls/.gitignore deploy/tls/gen.sh deploy/tls/gen-docker.sh deploy/docker-compose.yml deploy/.env.example docker/PgWorker.Dockerfile && \
git commit -m "feat(deploy): per-install TLS-пакеты gen.sh (pgserver/seed/prometheus) + gen-docker.sh; pgworker mTLS compose/HEALTHCHECK (t03 Ф4)"
```

---

### Task 8: Стенд — compose панели/прометеуса, prometheus.yml, чеки, полный 00-up

**Files:**
- Modify: `dev-stand/adminpanel/docker-compose.yml` (панель env → `WORKERTLS`; prometheus mount)
- Modify: `dev-stand/adminpanel/metrics/prometheus/prometheus.yml` (https + tls_config обеих джоб воркеров — О4)
- Modify: `dev-stand/adminpanel/checks/00-up.sh`, `05-seed.sh`, `20-alerts.sh`, `65-metrics.sh`
- Modify: `dev-stand/seed.sh`
- Modify: `dev-stand/adminpanel/README.md` (генерация пакета — список файлов)

**Interfaces:**
- Consumes: `WorkerTls` env-имена (Task 6), TLS-пакет (Task 7).
- Produces: зелёный полный стенд на mTLS (критерий spec §8.4), починенный kafka-скрейп (чек 65).

- [ ] **Step 1: Стендовый compose**

`dev-stand/adminpanel/docker-compose.yml`, сервис `adminpanel` (строки 261–264 заменить):

```yaml
      # mTLS API воркеров (t03, arch/02 §2.3.2): ЕДИНЫЙ клиентский серт панели
      # + ServerCA на оба воркера (pgworker и kafkaworker — одна kfw-install-ca).
      ADMINPANEL__WORKERS__WORKERTLS__CLIENTCERT_PATH: /tls-workers/panel.crt
      ADMINPANEL__WORKERS__WORKERTLS__CLIENTKEY_PATH: /tls-workers/panel.key
      ADMINPANEL__WORKERS__WORKERTLS__SERVERCA_PATH: /tls-workers/ca.pem
```

Сервис `prometheus` (строки 312–322): volumes добавить:

```yaml
      # Скрейп воркеров за mTLS (t03, О4): TLS-пакет ro (tls_config prometheus.yml).
      - ../../deploy/tls:/tls:ro
```

- [ ] **Step 2: prometheus.yml**

`dev-stand/adminpanel/metrics/prometheus/prometheus.yml` — джобы pgworker/kafkaworker заменить:

```yaml
  - job_name: pgworker            # deploy-compose, публикация хоста :8080 (mTLS, t03)
    scheme: https
    tls_config:
      ca_file: /tls/ca.pem
      cert_file: /tls/prometheus.crt
      key_file: /tls/prometheus.key
    static_configs: [{targets: ["host.docker.internal:8080"]}]
  - job_name: kafkaworker         # сеть стенда (профиль kafka; mTLS — чинит t03-gap)
    scheme: https
    tls_config:
      ca_file: /tls/ca.pem
      cert_file: /tls/prometheus.crt
      key_file: /tls/prometheus.key
    static_configs: [{targets: ["kafkaworker:8080"]}]
```

- [ ] **Step 3: Чеки**

`00-up.sh`:
1. Перенести `ROOT="$(cd ../.. && pwd)"` с строки 46 ВВЕРХ (сразу после `cd "$(dirname "$0")/.."`, до первого использования на строке 17) — сейчас `set -u` падает «unbound variable» при первом запуске.
2. После генерации пакета — наполнение deploy-volume (комментарий шага 1b уже обещает):

```bash
# Наполнение deploy-volume pgw-api-tls пакетом (ro-монтирование воркером).
docker run --rm \
  -v "$ROOT/deploy/tls:/src:ro" -v deploy_pgw-api-tls:/tls alpine:3.20 \
  sh -c "cp /src/ca.pem /src/pgserver.crt /src/pgserver.key /src/healthcheck.crt /src/healthcheck.key /tls/"
```

3. Проба готовности pgworker (шаг 1b, строки 49–52) → mTLS-вызов (паттерн 57-го):

```bash
MTLS="curl -fsS -m 3 --cacert $ROOT/deploy/tls/ca.pem --cert $ROOT/deploy/tls/healthcheck.crt --key $ROOT/deploy/tls/healthcheck.key"
for i in $(seq 1 60); do $MTLS https://localhost:8080/healthz >/dev/null 2>&1 && break; sleep 1; done
$MTLS https://localhost:8080/healthz >/dev/null \
  || { echo "❌ pgworker не ожил за 60 c (https :8080/healthz по mTLS; docker logs deploy-pgworker-1)"; exit 1; }
echo "  pgworker жив (https :8080/healthz, mTLS, общий etcd-контур)"
```

`05-seed.sh`, `seed_pg` (строки 18–21): ожидание и сид — клиентским сертом `seed.crt` (отдельные креды сида, spec §1.4):

```bash
ROOT="$(cd ../.. && pwd)"
SEED_TLS="curl -fsS -m 3 --cacert $ROOT/deploy/tls/ca.pem --cert $ROOT/deploy/tls/seed.crt --key $ROOT/deploy/tls/seed.key"
for i in $(seq 1 60); do $SEED_TLS https://localhost:8080/healthz >/dev/null 2>&1 && break; sleep 1; done
$SEED_TLS https://localhost:8080/healthz >/dev/null || { echo "❌ pgworker не ожил (https :8080/healthz, seed-серт)"; exit 1; }
echo "  pg-сид: $($SEED_TLS -X POST https://localhost:8080/api/seed/demo)"
```

(`seed_kafka` — не трогать, уже https.)

`20-alerts.sh`: заменить ВСЕ вызовы `http://localhost:8080/healthz` в файле (их ПЯТЬ: строки ~57, 86, 87, 88, 156, 172 — строки 86–88 это цикл ожидания + финальная проверка + перепроверка, строка ~156 — детект живого воркера шага 6) на mTLS-вызовы `https://localhost:8080/healthz` через обёртку `PG_MTLS` (healthcheck-пара, как в 00-up). Определение в шапку файла (после `cd "$(dirname "$0")/.."`):

```bash
ROOT="$(cd ../.. && pwd)"
PG_MTLS="curl -fsS -m 3 --cacert $ROOT/deploy/tls/ca.pem --cert $ROOT/deploy/tls/healthcheck.crt --key $ROOT/deploy/tls/healthcheck.key"
```

КРИТИЧНО: пропуск строки ~156 (детект живого воркера шага 6 «worker-api-unreachable»: `if curl -fsS -m 3 http://localhost:8080/healthz …`) превратит условие в всегда-false — шаг 6 молча пропустится, а с ним проверка алерта недоступности API воркера. Контроль полноты после правки:

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh/dev-stand/adminpanel && \
grep -n "localhost:8080/healthz" checks/20-alerts.sh
```
Ожидание: все строки вывода содержат `https://` и/или вызов идут через `$PG_MTLS`; ни одной строки с `http://localhost:8080/healthz`.

`65-metrics.sh`:
- Шаг 0 (строки 15–22): воркеры живы — pgworker: `$MTLS https://localhost:8080/healthz`; kafkaworker: существующий `--cacert/--cert/--key` уже mTLS (строка 16 — проверить, что там http → https + серты по образцу 05-seed kafka-ветки: `--cacert $ROOT/deploy/tls/ca.pem --cert $ROOT/deploy/tls/healthcheck.crt --key $ROOT/deploy/tls/healthcheck.key https://localhost:8082/healthz`); `ROOT="$(cd ../.. && pwd)"` в шапку.
- Шаг 1 (строка 27): `for u in https://localhost:8080/metrics https://localhost:8082/metrics http://localhost:5050/metrics` — http-сертификаты: 8080/8082 через `$MTLS`-контекст (curl-обёртка с сертами), 5050 — как есть.

`dev-stand/seed.sh`: `API="${1:-https://localhost:8080}"` + mTLS-сертификаты в curl (в шапке: `TLS_DIR="$(cd "$(dirname "$0")/../deploy/tls" && pwd)"`, в curl-вызове: `--cacert "$TLS_DIR/ca.pem" --cert "$TLS_DIR/seed.crt" --key "$TLS_DIR/seed.key"`).

`dev-stand/adminpanel/README.md:134`: обновить список файлов пакета (server/pgserver/panel/seed/prometheus/healthcheck) + WORKERTLS env.

- [ ] **Step 4: Полный подъём стенда (главная проверка задачи)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh/dev-stand/adminpanel && \
rm -f ../../deploy/tls/ca.pem && checks/00-up.sh
```
Ожидание: `✓ стенд поднят` — pgworker поднялся с mTLS (HEALTHCHECK зелёный: `docker inspect --format '{{.State.Health.Status}}' deploy-pgworker-1` = `healthy`), pg-сид налит по https c seed-сертом, панель жива.

- [ ] **Step 5: Чеки 05/15/20/65 (панельная мутация по mTLS + починенный kafka-скрейп — критерии §8.4)**

Порядок: 05 (сид по https с seed-сертом) → 15 (панельная мутация pg-домена) → 20 → 65. `15-cluster-create.sh` — прямое доказательство «панель ходит в оба API клиентским сертом (мутации pg-домена работают)»: POST панель `/api/clusters` = 201 → `WorkerApiGateway` → mTLS-вызов `CreateClusterHandler` живого воркера (запрос упадёт 503 `worker-api-unreachable`, если TLS-конфигурация панели/воркера рассогласована).

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh/dev-stand/adminpanel && \
checks/05-seed.sh pg && checks/15-cluster-create.sh && checks/20-alerts.sh && checks/65-metrics.sh
```
Ожидание: все `✓`; в 15-м `POST /api/clusters = 201` и тело созданного кластера (панель→воркер по mTLS); в 65-м «scrape-джобы up» БЕЗ красного kafkaworker (чинит gap t03-kafka). Если стенд-контейнеры поднимались с нуля и `as-kafkaworker` остановлен чеком 50 — 65-й сам стартует его (шаг 0).

- [ ] **Step 6: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
git add dev-stand deploy && git commit -m "feat(stand): панель/прометеус на mTLS-пакет, scrape за tls_config, чеки 00/05/15/20/65 https (t03 Ф5)"
```

---

### Task 9: E2E за mTLS — фикстурный TLS-пакет, https-хосты, маркер + полный E2eFixture Release

**Files:**
- Create: `src/tests/PgWorker.IntegrationTests/E2e/E2eTestPki.cs`
- Modify: `src/tests/PgWorker.IntegrationTests/E2e/E2eFixture.cs`
- Modify (если есть прямые http-вызовы API воркера): сценарии `E2e/*.cs` (проверить grep'ом)

**Interfaces:**
- Consumes: `ApiTlsEndpoints.ResolvePort` (порт из `ASPNETCORE_URLS`), PEM-дуализм `PGW_API_TLS_*` (Task 5).
- Produces: mertz-гейт (§8.3): `Scale_AddEmptyShard_BlockedRemoveThenAutoDismantle_NameReused` на https; полный E2eFixture 8/8.

- [ ] **Step 1: E2eTestPki**

`src/tests/PgWorker.IntegrationTests/E2e/E2eTestPki.cs` — копия `ClusterPki.GenerateCa(cluster)/IssueBrokerCertificate` (`src/KafkaWorker.Core/Templates/ClusterPki.cs:18-58`) локально в тестовой сборке. Сигнатуры (единая конвенция с `EngineProxyTestPki` из Task 4, отличается только CA-нейминг):

```csharp
public static class E2eTestPki
{
    // CN = pgw-<name>-ca (при name="e2e" → CN=pgw-e2e-ca — spec §5.5/§3.7);
    // RSA-2048, BasicConstraints CA, PEM PKCS#8 — механика ClusterPki.GenerateCa.
    public static (string CaPem, string CaKeyPem) GenerateCa(string name);

    // SAN dns[]+ip, EKU serverAuth+clientAuth — механика ClusterPki.IssueBrokerCertificate.
    public static (string CertPem, string KeyPem) Issue(
        string caCertPem, string caKeyPem, string commonName, IReadOnlyList<string> dnsNames, IPAddress? ip);
}
```

(тела — копия `EngineProxyTestPki` из Task 4 с заменой только `GenerateCa(string name)`: subject `$"CN=pgw-{name}-ca"`; тесты не тянут зависимость от KafkaWorker.Core.)

- [ ] **Step 2: E2eFixture**

`E2eFixture.cs`:
1. Поля: `public static readonly (string CaPem, string CaKeyPem) InstallCa;` + ленивый серверный/клиентский серт. В `InitializeAsync` (после гейта, до старта хостов): `InstallCa = E2eTestPki.GenerateCa("e2e");` (CN внутри хелпера — `pgw-e2e-ca`).
2. `StartHostAsync` env (заменить строки 193–195):

```csharp
            // mTLS HTTP API (t03, arch/14 §1.1): фикстурный per-install пакет —
            // PEM-дуализм env освобождает от файлов; порт — свободный зонд.
            ["PGW_API_TLS_CERT"] = ServerCertPem,
            ["PGW_API_TLS_KEY"] = ServerKeyPem,
            ["PGW_API_TLS_CLIENT_CA"] = InstallCa.CaPem,
            ["PgWorker__Api__AdvertiseUrl"] = $"https://127.0.0.1:{port}",
            ["ASPNETCORE_URLS"] = $"https://127.0.0.1:{port}",
```

(`ServerCertPem/ServerKeyPem` — свойства фикстуры: `E2eTestPki.Issue(InstallCa.CaPem, InstallCa.CaKeyPem, "pgworker", ["localhost", "127.0.0.1"], ip: null)`; держать X509-сертификаты БЕЗ using — паттерн MtlsApiTests.)

3. Health-клиент: заменить `private readonly HttpClient _healthHttp = new() { Timeout = 3s }` на mTLS-клиент:

```csharp
    private readonly HttpClient _healthHttp = new(new SocketsHttpHandler
    {
        // TLS 1.2: macOS SslStream не шлёт клиентские серты в TLS 1.3 (runtime#37961).
        SslOptions = new()
        {
            EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
            ClientCertificates = [E2eClientCert],
            RemoteCertificateValidationCallback = (_, _, _, _) => true, // тест доверяет фикстурной CA
        },
    }) { Timeout = TimeSpan.FromSeconds(3) };
```

4. Проба готовности (строки ~237–239): `await _healthHttp.GetAsync($"https://127.0.0.1:{port}/healthz", …)`.

- [ ] **Step 3: Сценарии**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && grep -rn "http://127.0.0.1\|http://localhost:8" src/tests/PgWorker.IntegrationTests/E2e/*.cs | grep -v patroni
```
Найденные прямые вызовы API воркера (кроме patroni `:8008/primary`) перевести на https-клиент фикстуры (`fixture.CreateApiClient()` — при отсутствии добавить тонкий хелпер в E2eFixture по образцу `_healthHttp`, BaseAddress конкретного инстанса). Если grep пуст — шаг тривиально закрыт.

- [ ] **Step 4: Маркер мерж-гейта (свежий Release)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter FullyQualifiedName~Scale_AddEmptyShard ; \
docker rm -f $(docker ps -aq --filter name=pgw-) 2>/dev/null; docker network prune -f
```
Ожидание: PASS (`Scale_AddEmptyShard_BlockedRemoveThenAutoDismantle_NameReused`); E2eFixture сам собирает Release (инкрементальный no-op).

- [ ] **Step 5: Полный E2eFixture (обязателен: t03 меняет provisioning-путь DockerEngineFactory)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter FullyQualifiedName~E2e ; \
docker rm -f $(docker ps -aq --filter name=pgw-) 2>/dev/null; docker network prune -f; \
docker network ls | grep -c kfw-net || true
```
Ожидание: 8/8 PASS; после зачистки — осиротевших `kfw-net-*`/`pgw-*-net` нет (счёт 0 при нуле pgw-контейнеров).

- [ ] **Step 6: Коммит**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
git add src/tests/PgWorker.IntegrationTests && \
git commit -m "test(e2e): E2eFixture на https с фикстурным mTLS-пакетом pgw-e2e-ca (t03 Ф6)"
```

---

### Task 10: Полировка — README, зачистка имён, финальные серии, roadmap-чистка (мерж-гейт)

**Files:**
- Modify: `README.md` (раздел защищённого docker-транспорта), `dev-stand/adminpanel/README.md`
- Modify: `arch/roadmap/pgworker.md` (удалить запись `t03-docker-tls-ssh`; дополнить `t08-unify-adminpanel-duplicates` третьей группой дублей)

**Interfaces:**
- Consumes: всё выше.
- Produces: критерии §8.5–8.6 + roadmap-чистка §9 (тем же коммитом мержа).

- [ ] **Step 1: README**

`README.md` (корень): добавить раздел «Защищённый транспорт к Docker Engine API» (после раздела о deploy — исполнитель найдёт место по `grep -n "deploy" README.md`): daemon-флаги `--tlsverify` (+ пример команды из вывода `gen-docker.sh`), `ssh://`-эндпоинты (`PgWorker:Docker:Hosts` + `PGW_DOCKER_SSH_*`), RBAC: группа `docker` + `group_add` (пример из compose-комментария Task 7), запрет `:2375` наружу/firewall — канон arch/14 §2.2.1, матрица arch/13 §2. `dev-stand/adminpanel/README.md`: обновить раздел генерации пакета (строка ~134) — состав пакета + WORKERS_PANEL_TLS + https-чеки.

- [ ] **Step 2: Грепп-зачистка**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
grep -rn "PGW_API_KEY\|ApiKeyMiddleware\|PgApiKey" src/ deploy/ docker/ dev-stand/ README.md --include="*.cs" --include="*.json" --include="*.yml" --include="*.sh" --include="*.example" --include="*.md" | grep -v "docs/superpowers" ; \
grep -rn "KafkaTls\|KFW_PANEL_TLS" src/ dev-stand/ | grep -v "docs/superpowers"
```
Ожидание: оба вывода пусты (`X-Api-Key` остаётся только в kafka-исторических упоминаниях arch/16 и docs/superpowers — они канонические, не трогать; если grep показал живой код — почистить).

- [ ] **Step 3: Финальные серии (юниты → интеграция → E2E, каждая с зачисткой)**

Зачистка после каждой серии — ТОЛЬКО name-фильтром `pgw-` (покрывает e2e-артефакты и тест-контейнеры `pgw-t-*`); широкие чистки `docker ps -aq | grep` ЗАПРЕЩЕНЫ (см. Глобальные ограничения — `ps -aq` не содержит имён, фильтр не отсекает стенд).

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/PgWorker.slnx -c Release --filter "FullyQualifiedName!~IntegrationTests&FullyQualifiedName!~E2e" ; \
docker rm -f $(docker ps -aq --filter name=pgw-) 2>/dev/null; docker network prune -f
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/PgWorker.IntegrationTests -c Release ; \
docker rm -f $(docker ps -aq --filter name=pgw-) 2>/dev/null; docker network prune -f
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/AdminPanel.IntegrationTests -c Release ; \
docker rm -f $(docker ps -aq --filter name=pgw-) 2>/dev/null; docker network prune -f
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~E2e ; \
docker rm -f $(docker ps -aq --filter name=pgw-) 2>/dev/null; docker network prune -f
```
Ожидание: все серии зелёные; контейнеры стенда живы: `docker ps --format '{{.Names}}'` всё ещё содержит `as-etcd`/`as-adminpanel`/`deploy-pgworker-1` (проверить после последней серии). KafkaWorker-решение НЕ пересобирается отдельно — входит в `PgWorker.slnx` юнит-фильтр (сборка 0 warnings по всем проектам).

- [ ] **Step 4: Roadmap-чистка (spec §9)**

`arch/roadmap/pgworker.md`:
- Удалить запись `t03-docker-tls-ssh` (строки ~13–…) и её упоминания из `←`-зависимостей других пунктов (grep `t03` по файлу).
- В запись `t08-unify-adminpanel-duplicates` дописать третью группу дублей: `ApiTlsEndpoints`/`TlsEndpoints` (PgWorker.App ↔ KafkaWorker.App) и TLS-валидационные хелперы (`DockerTlsMaterial.ValidateChain`/`WorkerTlsHandler.ValidateChain`/`TlsEndpoints.ValidateChain`) — унификация тем же проходом t08.

- [ ] **Step 5: Финальная сборка + коммит (roadmap-чистка — тем же коммитом, что и финальное состояние ветки; мерж-гейт — см. шаг 6)**

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t03-docker-tls-ssh && \
DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release && \
git add README.md dev-stand/adminpanel/README.md arch/roadmap/pgworker.md && \
git commit -m "docs(t03): защищённый docker-транспорт в README; roadmap-чистка t03 + третья группа дублей t08"
```

- [ ] **Step 6: Мерж-гейт (доклад пользователю; мерж в main — по его команде)**

Итоговый чек-лист доклада: маркер `Scale_AddEmptyShard` зелёный на свежем Release (Task 9 Step 4); полный E2eFixture 8/8 (Task 9 Step 5); полный стенд 00-up + чеки 05/15/20/65 зелёные (вкл. панельную мутацию pg-домена по mTLS — 15-й чек), HEALTHCHECK pgworker healthy, kafka-скрейп up (Task 8 Steps 4–5); `gen.sh`/`gen-docker.sh` воспроизводят пакеты с нуля (Task 7 Step 3); grep-зачистка пуста (Task 10 Step 2); сборка 0 warnings. Мерж в `main` + пуш — только после ревью и явного указания пользователя.

---

## Self-Review (выполнен автором плана; обновлён по итогам независимого ревью Фазы 4 — все 7 замечаний устранены)

- **Покрытие спеки:** §1.1/§5.1 (Tasks 1–4), §1.2/RBAC-канон (Task 7 compose-комментарий + Task 10 README; сам arch/14 §2.2.1 уже внесён spec'ом), §1.3/§5.2 (Task 5), §1.4 (серты seed/prometheus/healthcheck — Tasks 7–8), §5.3 (Task 6), §5.4 (Task 7), §5.5-юниты (Tasks 1–6; парсинг схем — T1, TLS-handler/fail-fast — T2, fingerprint+`TunnelTarget` target-вычисление — T3, env-биндинги — T2/T3/T5, https-advertise — T5 WAF-кейс, отсутствие X-Api-Key — T6), §5.5-интеграция (Tasks 4–5), §5.5-E2E (Task 9), §6-фазы Ф1–Ф7 ↔ Tasks 1–10, §8-критерии распределены по задачам (§8.4 — панельная мутация по mTLS: чек 15 в Task 8 Step 5; §8.3/8.5 — Tasks 8–10), §9 (Task 10 Step 4), §10 — решения О1–О4 отражены (О1: gen.sh единая CA, Tasks 7–8; О2: warning R15, Task 2; О3: ForwardedPortLocal, Task 3; О4: prometheus.yml, Task 8).
- **Правки ревью Фазы 4:** (1) все команды зачистки контейнеров — только name-фильтры docker (`--filter name=pgw-` покрывает e2e-артефакты и тест-контейнеры `pgw-t-*`); запрет `docker ps -aq | grep` вынесен в Глобальные ограничения (`ps -aq` отдаёт только hex-ID — grep по именам не отсекает стенд); (2) 20-alerts.sh — заменены ВСЕ 5 вызовов healthz (вкл. строку ~156 — детект живого воркера шага 6, иначе шаг молча пропускается) + grep-контроль полноты; (3) Task 8 Step 5 — добавлен `checks/15-cluster-create.sh` (панельная мутация pg-домена через mTLS — критерий §8.4 «мутации панели работают»); (4) тест-контейнеры получают явные имена `pgw-t-tlsproxy`/`pgw-t-sshd` (случайные имена testcontainers фильтрами не матчатся); (5) sshd — WaitStrategy на оба порта (2222 sshd, 2376 socat) + StartupTimeout 90 с (анти-флак первого хендшейка после apk add); (6) target-вычисление туннеля — чистая функция `SshTunnelOptions.TunnelTarget()` с юнит-тестами (дефолты/кастом/некорректные — fail-fast); (7) сигнатура `E2eTestPki.GenerateCa(string name)` (CN=pgw-<name>-ca) согласована между Step 1 и вызовом `GenerateCa("e2e")` в Step 2.
- **Мелочи, учтённые от кодовой базы:** `00-up.sh` использует `$ROOT` до объявления (unbound при `set -u`) — чинится в Task 8; volume `kfw-api-tls` deploy-compose никем не наполняется — НЕ трогаем (kafka-часть вне скоупа, стендовый kafkaworker идёт через bind-mount); «ApiKey» в именах тестов координации — lease-ключи, не трогать (вынесено в Глобальные ограничения); `Create`-сигнатура фабрики сохранена для существующих тестов `DockerDriverTests`/`ClusterDriverTests` (optional-параметры).
- **Типы/сигнатуры:** `EndpointScheme.Parse` → Tasks 2–3–4; `DockerTlsOptions.EnvBindings/ApplyEnvOverrides` → Task 5 Program.cs; `SshTunnelOptions.DecideHostKeyTrust`/`TunnelTarget` → Task 3 (используются `SshHostConnection`); `E2eTestPki.GenerateCa(string)`/`Issue(...)` согласованы Task 9 Step 1 ↔ Step 2; конструктор `DockerEngineFactory(tls, ssh, logger, loggerFactory)` согласован между Tasks 2/3/5 (Task 2 фиксирует `tls/ssh/logger`, Task 3 добавляет `loggerFactory` — финальная сигнатура в Interfaces Task 2; Program.cs Task 5 передаёт все четыре).
