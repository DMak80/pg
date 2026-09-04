# t03-kafka-security — план реализации (TLS, ACL, разделение кредов, mTLS API)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Закрыть четыре дыры безопасности Kafka-домена: SASL_SSL на CLIENT/INTERNAL с per-cluster CA, ACL (deny-by-default, роль app), разделение кредов admin/app с ротацией обеих ролей, mTLS-only HTTP API воркера с автоматической converge-миграцией существующих кластеров.

**Architecture:** Arch-first: канон (`arch/15-kafka-clusters.md`, `arch/16-kafkaworker.md`, `arch/adminpanel/02-etcd-contract.md`) уже обновлён в этом worktree — код исполняет контракт. Per-cluster секреты (CA, креды admin/app) — в etcd через ensure txn put-if-absent; PKI-генерация — `CertificateRequest` .NET без внешних инструментов; единственные per-install env-секреты — TLS HTTP API (бутстрап-парадокс etcd). Миграция старых кластеров — процесс M (SecurityMigrator, полный рестарт разом).

**Tech Stack:** .NET 10 (`TreatWarningsAsErrors=true`, `Nullable=enable`), Confluent.Kafka 2.14.2 (`ssl.ca.pem`, DescribeAcls/CreateAcls/DeleteAcls), Testcontainers (динамические порты), openssl (deploy/tls/gen.sh), React/Mantine (панель).

**Spec:** `docs/superpowers/2026-09-04-t03-kafka-security/spec.md` (в этом же каталоге; план аргументируется от спеки — исполнитель читает оба документа).

## Global Constraints

- `TreatWarningsAsErrors=true` — сборка чисто, без warning-ов; .NET 10, `LangVersion=latest`, `Nullable=enable`.
- **Каждый коммит собирается и зелёный на юнит-уровне** (`dotnet build src/PgWorker.slnx` + `dotnet test src/tests/KafkaWorker.UnitTests src/tests/AdminPanel.UnitTests`) — bisect не ломается; полные docker-прогоны — в задачах 9/11/15 и мерж-гейте (Task 16).
- Тесты: комментарии AAA (`// Arrange`, `// Act`, `// Assert`); русский язык комментариев/доков, английские идентификаторы.
- Порты docker-контейнеров в тестах — динамические (`assignRandomHostPort: true` / зонд свободных портов); никаких литералов `:16000`. `BrokerBootSec` в тестовых фикстурах ≤ 100 с.
- Хост-порты тестов не пересекаются с dev-стендом (окно `FreePortWindow.Find()`).
- E2E Release (мерж-гейт): ПОЛНЫЙ прогон `KafkaWorker.IntegrationTests` на свежем Release + маркер `Provisioning_TlsClusterUp` + pg-маркер `Scale_AddEmptyShard` (Task 16, критерий §8.3).
- Дискавери-канон 15 §5: `security.protocol=SASL_SSL` + `sasl.mechanisms=PLAIN` — без per-cluster флага безопасности; после t03 «старого формата» нет.
- Креды/секреты: `admin_user="admin"`, пароли 32 симв `[A-Za-z0-9]` (`KafkaPasswordGenerator`), `ca_key`/`ca_pem` — PEM одной строкой с `\n` (PKCS#8 `BEGIN PRIVATE KEY`).
- `KFW_API_KEY`/`X-Api-Key` для KafkaWorker и `SASL_PLAINTEXT` в env брокеров/клиентов воркера после t03 не существует нигде в `src/`, `deploy/`, `dev-stand/` (критерий §8.6; единственное осознанное исключение — legacy-хелпер теста миграции, Task 15).
- Все команды `dotnet test`/`dotnet build` выполняются из корня worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t03-kafka-security` (абсолютные пути в тексте — от него).
- Коммит после каждой задачи (задача = независимо тестируемый deliverable); сообщения — conventional (`feat(kafka): …`, `refactor(panel): …`).

## Решения plan-фазы (детали, закрытые здесь по §10 спеки)

1. **Bootstrap AdminClient воркера — CLIENT endpoints из etcd, не INTERNAL.** INTERNAL advertised = docker-DNS `broker<k>:9092`, который резолвится только внутри сети `kfw-net-<C>`; процесс воркера (контейнер deploy-compose / хост-процесс тестов) в этой сети не живёт. Все AdminClient-подключения воркера: bootstrap = `snap.Endpoints` (CLIENT, SASL_SSL), креды `admin`, доверие `ca_pem` — симметрия пробам панели (arch/16 §2.3). По INTERNAL ходит только reassign-CLI внутри контейнера брокера (arch/16 §2.4). **Arch-first соблюдён:** формулировки канона `arch/16` §2.3 и §5-M3 синхронизируются первым шагом задачи транспорта (Task 8 Step 0); формулировки spec §5.2 (AdminClient-транспорт и M3) уже поправлены при ревью плана.
2. **Кеш сертификатов нод** — `BrokerCertificateCache` (DI-синглтон, `ConcurrentDictionary` по `(cluster, broker, hash(ca_key))`): серт ноды генерируется один раз на процесс (R3 — повторные сборки env того же кластера дают тот же PEM); смена CA инвалидирует кеш ключом.
3. **WAF-тесты API** — только на `AllowInsecureHttp=true` (существующие классы); реальный mTLS-хендшейк — интеграционный тест на настоящем Kestrel-сокете (`MtlsApiTests`, динамический порт из зонда), общий конфиг-код Kestrel в `TlsEndpoints.cs` (вызов на `WebApplicationBuilder` до `Build()`).
4. **Панельный алерт `kafka-security-missing`** — `KafkaAlertEngine` получает `securityReady`-коллекцию от refresher'а (по `IKafkaSecretsStore`): Active-кластер без admin-креда/CA в сторе → critical. Креды остаются вне `KafkaClusterInfo`/UI/API.
5. **Новый roadmap-тег ротации CA** — `t07-kafka-ca-rotation` (t01–t06 заняты; Task 16).
6. **Расширение `NodeEnvSpec` в Task 2 — Вариант А (канонический порядок параметров + минимальная компенсация прод-вызовов).** Новые поля вставляются на канонические позиции (после `AppPasswords`, до `Config`), а ТРИ прод-вызова `new NodeEnvSpec(...)` (`BrokerEnvBuilder.cs` — покрывает вызовы ротации/add-broker/регенератора; `NodeSupervisor.cs`; `ProvisioningProcess.cs`) компенсируются в Task 2 переходными placeholder-аргументами (admin-пользователь/пароль-заглушка, PEM-заглушки) и коммитятся ВМЕСТЕ с Core-правками — каждый коммит собирается, юнит-набор зелёный, bisect цел. Полная замена placeholder'ов на реальные данные (поля снапшота Task 4 + `BrokerCertificateCache`) — Task 8/10. Отвергнутый Вариант Б (дефолты в конце записи): обязательные по канону поля безопасности получили бы «молчащие» дефолты `= ""` — пропущенный серт не ловился бы компилятором и маскировал бы невалидный env брокера, плюс двойное касание сигнатур (Task 2 и снова Task 8).

## File Structure (карта изменений)

```
src/KafkaWorker.Core/Templates/ClusterPki.cs            (new)  — CA/серт-генерация, PEM-парсинг
src/KafkaWorker.Core/Templates/BrokerCertificateCache.cs (new) — кеш сертов нод (R3)
src/KafkaWorker.Core/Templates/NodeEnvBuilder.cs        (mod)  — env-канон SASL_SSL+ACL (16 §2.2)
src/KafkaWorker.Core/Model/KafkaDomain.cs               (mod)  — снапшот: admin/CA-поля
src/KafkaWorker.Etcd/Parsing/KafkaSnapshotParser.cs     (mod)  — новые ключи, битый PEM → parseError
src/KafkaWorker.Provisioning/Processes/ClusterSecretEnsurer.cs (new; сносит AppSecretEnsurer.cs)
src/KafkaWorker.Provisioning/Kafka/IKafkaAdminClient.cs (mod)  — TLS-фабрика + ACL-операции
src/KafkaWorker.Provisioning/Kafka/KafkaAdminClient.cs  (mod)  — SaslSsl + ssl.ca.pem + ACL-адаптер
src/KafkaWorker.Provisioning/Processes/AclPlan.cs       (new)  — чистый ACL-план/дифф роли app
src/KafkaWorker.Provisioning/Processes/ClusterConfigConverger.cs (mod) — ACL-converge шаг
src/KafkaWorker.Provisioning/Processes/BrokerEnvBuilder.cs (mod) — креды admin+CA+серт в env (переходные placeholder'ы — Task 2)
src/KafkaWorker.Provisioning/Processes/ProvisioningProcess.cs (mod) — K2/K3/K4/K5 новый канон (переходные placeholder'ы — Task 2)
src/KafkaWorker.Provisioning/Processes/{NodeSupervisor,AddBrokerProcess,NodeRegenerator,TopicSyncProcess,PartitionReassignerProcess,RemoveBrokerProcess}.cs (mod) — admin-кред+TLS (NodeSupervisor: переходные placeholder'ы — Task 2)
src/KafkaWorker.Provisioning/Processes/PasswordRotator.cs (new; сносит AppPasswordRotator.cs) — роли app|admin
src/KafkaWorker.Provisioning/Processes/SecurityMigrator.cs (new) — процесс M
src/KafkaWorker.Provisioning/Processes/ReassignCli.cs   (mod)  — SASL_SSL command-config + PEM-truststore
src/KafkaWorker.Docker/Engine/IDockerEngine.cs + DockerEngine.cs (mod) — InspectContainerEnvAsync
src/KafkaWorker.Docker/Drivers/ClusterDriver.cs         (mod)  — IClusterDriver.NodeEnvAsync
src/KafkaWorker.App/Options.cs                          (mod)  — Api.Tls, минус ApiKey
src/KafkaWorker.App/Api/TlsEndpoints.cs                 (new)  — Kestrel mTLS-конфиг + env-биндинг (общая для Program/теста)
src/KafkaWorker.App/Api/ApiKeyMiddleware.cs             (del)
src/KafkaWorker.App/Api/Operations/RotateAdminPasswordHandler.cs (new)
src/KafkaWorker.App/Api/ApiModule.cs                    (mod)  — endpoint №16
src/KafkaWorker.App/Program.cs                          (mod)  — Kestrel TLS, fail-fast, шапка-комментарий секретов, DI-обновления
src/KafkaWorker.App/Loops/KafkaClusterProcesses.cs      (mod)  — M до Active, rotator/converger сигнатуры
src/AdminPanel.Etcd/Workers/WorkerApiOptions.cs         (mod)  — KafkaTlsOptions, минус KafkaApiKey
src/AdminPanel.Etcd/Workers/WorkerApiGateway.cs         (mod)  — без X-Api-Key kafka
src/AdminPanel.Etcd/Workers/WorkerTlsHandler.cs         (new)  — SocketsHttpHandler mTLS + env-биндинг KFW_PANEL_TLS_*
src/AdminPanel.Etcd/ModuleExtensions.cs                 (mod)  — HttpClient "workers" + клиентский серт
src/AdminPanel.Etcd/KafkaSecretsStore.cs                (mod)  — AdminUser/AdminPassword/CaPem
src/AdminPanel.Etcd/KafkaSnapshotRefresher.cs           (mod)  — admin_rotations/, чтение новых кредов
src/AdminPanel.Etcd/Parsing/KafkaParser.cs              (mod)  — ParseAdminRotations, expected-skip
src/AdminPanel.Core/Kafka/KafkaSnapshot.cs              (mod)  — AdminRotations
src/AdminPanel.Core/Kafka/KafkaAlerting/KafkaAlertEngine.cs (mod) — kafka-security-missing, kafka-admin-rotation-pending
src/AdminPanel.Probes/Kafka/KafkaClientCache.cs         (mod)  — SaslSsl + ssl.ca.pem, ключ кэша с caPem (Task 5)
src/AdminPanel.Probes/Kafka/ConfluentKafkaProbeClient.cs (mod) — caPem в seam-вызовах, Invalidate (Task 5)
src/AdminPanel.Probes/Kafka/KafkaProbe.cs               (mod)  — seam-сигнатуры +caPem (Task 5)
src/AdminPanel.Probes/Kafka/KafkaProbeLoop.cs           (mod)  — admin-креды + CaPem из стора (Task 5)
src/AdminPanel.Api/Operations/Kafka/KafkaCommands.cs    (mod)  — RotateKafkaAdminPasswordCommand
frontend/src/pages/kafka-cluster/RotateAdminPasswordButton.tsx (new) + использование
src/tests/KafkaWorker.UnitTests/**                      (mod/new) — все юнит-тесты фаз Ф1–Ф4 (+ env-биндинг TLS)
src/tests/KafkaWorker.IntegrationTests/**               (mod/new) — docker: TLS-кластер, ACL, ротация admin, миграция, mTLS-сокет
src/tests/AdminPanel.UnitTests/**                       (mod)   — панельные тесты (вкл. пробы, Task 5)
deploy/docker-compose.yml, deploy/.env.example, deploy/tls/gen.sh (mod/new)
docker/KafkaWorker.Dockerfile                           (mod)   — HEALTHCHECK https (mTLS-пара healthcheck)
dev-stand/seed.sh, dev-stand/adminpanel/**              (mod)   — TLS-стенд
arch/16-kafkaworker.md                                  (mod)   — §2.3/§5-M3: AdminClient по CLIENT (Task 8 Step 0)
arch/roadmap/kafkaworker.md                             (mod)   — мерж-гейт чистки (Task 16)
```

---

### Task 1: ClusterPki — генерация CA и сертификатов нод (Ф1)

**Files:**
- Create: `src/KafkaWorker.Core/Templates/ClusterPki.cs`
- Create: `src/KafkaWorker.Core/Templates/BrokerCertificateCache.cs`
- Test: `src/tests/KafkaWorker.UnitTests/Templates/ClusterPkiTests.cs`

**Interfaces:**
- Consumes: — (чистая генерация; `KafkaPasswordGenerator`-паттерн Core).
- Produces (используют Tasks 2, 3, 8, 12, 15):
  - `static (string CaPem, string CaKeyPem) ClusterPki.GenerateCa(string cluster)`
  - `static (string CertPem, string KeyPem) ClusterPki.IssueBrokerCertificate(string caCertPem, string caKeyPem, string commonName, IReadOnlyList<string> dnsNames, System.Net.IPAddress? ip)`
  - `static bool ClusterPki.TryParseCertificate(string pem, out X509Certificate2? certificate)`
  - `static bool ClusterPki.TryParseRsaKey(string pem, out RSA? key)`
  - `(string CertPem, string KeyPem) BrokerCertificateCache.GetOrCreate(string cluster, string brokerName, string caCertPem, string caKeyPem, string advertisedClientHost)` — advertised хост без порта; IP-хост → IP-SAN, иначе DNS-SAN.

- [ ] **Step 1: Написать failing-тесты**

`src/tests/KafkaWorker.UnitTests/Templates/ClusterPkiTests.cs`:

```csharp
using System.Net;
using FluentAssertions;
using System.Security.Cryptography.X509Certificates;

namespace KafkaWorker.UnitTests.Templates;

// PKI кластера (arch/16 §2.3): self-signed CA + серверные серты нод, PEM one-line.
public class ClusterPkiTests
{
    [Fact]
    public void GenerateCa_SelfSignedCaWithCanonicalCnAndLongValidity()
    {
        // Arrange: кластер "events".
        // Act: генерация CA.
        var (caPem, caKeyPem) = ClusterPki.GenerateCa("events");

        // Assert: PEM-маркеры, канонический CN, BasicConstraints CA, срок ~10 лет.
        caPem.Should().StartWith("-----BEGIN CERTIFICATE-----").And.Contain("\n-----END CERTIFICATE-----");
        caKeyPem.Should().StartWith("-----BEGIN PRIVATE KEY-----"); // PKCS#8 (15 §2.1)
        using var cert = X509Certificate2.CreateFromPem(caPem);
        cert.Subject.Should().Be("CN=kfw-events-ca");
        cert.HasPrivateKey.Should().BeFalse("публичный серт не несёт ключа");
        (cert.NotAfter - cert.NotBefore).TotalDays.Should().BeInRange(3600, 3700);
    }

    [Fact]
    public void IssueBrokerCertificate_DnsSanCoverNodeAndAdvertisedHost()
    {
        // Arrange: CA + нода broker2, advertised host.docker.internal.
        var (caPem, caKeyPem) = ClusterPki.GenerateCa("events");

        // Act: выпуск серта ноды (SAN: docker-DNS + advertised host).
        var (certPem, keyPem) = ClusterPki.IssueBrokerCertificate(
            caPem, caKeyPem, "broker2", ["broker2", "host.docker.internal"], ip: null);

        // Assert: подписан CA, CN/SAN/EKU, PEM round-trip с приватным ключом.
        using var ca = X509Certificate2.CreateFromPem(caPem);
        using var cert = X509Certificate2.CreateFromPem(certPem);
        cert.Subject.Should().Be("CN=broker2");
        var san = cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().Single();
        san.GetExplicitDnsNames().Should().Contain(["broker2", "host.docker.internal"]);
        cert.Issuer.Should().Be(ca.Subject, "серт подписан ключом CA");
        keyPem.Should().StartWith("-----BEGIN PRIVATE KEY-----");
    }

    [Fact]
    public void IssueBrokerCertificate_IpAdvertisedHostBecomesIpSan()
    {
        // Arrange: advertised-хост — IP-литерал (мульти-хост plain).
        var (caPem, caKeyPem) = ClusterPki.GenerateCa("ev");

        // Act: серт с IP-SAN.
        var (certPem, _) = ClusterPki.IssueBrokerCertificate(
            caPem, caKeyPem, "broker1", ["broker1"], IPAddress.Parse("10.0.0.5"));

        // Assert: IP в SAN.
        using var cert = X509Certificate2.CreateFromPem(certPem);
        cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().Single()
            .GetExplicitIpAddresses().Should().Contain(IPAddress.Parse("10.0.0.5"));
    }

    [Fact]
    public void TryParseCertificate_MalformedPemIsFalse()
    {
        // Arrange: мусор вместо PEM.
        // Act / Assert: мягкий разбор — битый PEM не бросает исключение.
        ClusterPki.TryParseCertificate("not a pem", out var cert).Should().BeFalse();
        cert.Should().BeNull();
    }
}

// Кеш сертов нод: один серт на (кластер, нода, CA) в рамках процесса — R3.
public class BrokerCertificateCacheTests
{
    [Fact]
    public void GetOrCreate_SameInputsReturnSameCertificate()
    {
        // Arrange: кеш и per-cluster CA.
        var cache = new BrokerCertificateCache();
        var (caPem, caKeyPem) = ClusterPki.GenerateCa("events");

        // Act: два вызова для broker1.
        var first = cache.GetOrCreate("events", "broker1", caPem, caKeyPem, "host.docker.internal");
        var second = cache.GetOrCreate("events", "broker1", caPem, caKeyPem, "host.docker.internal");

        // Assert: идентичный PEM (серт не перегенерируется).
        first.CertPem.Should().Be(second.CertPem);
        first.KeyPem.Should().Be(second.KeyPem);
        // SAN покрывает docker-DNS и advertised: host без порта → DNS.
        using var cert = X509Certificate2.CreateFromPem(first.CertPem);
        cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().Single()
            .GetExplicitDnsNames().Should().Contain(["broker1", "host.docker.internal"]);
    }

    [Fact]
    public void GetOrCreate_NewCaInvalidatesCache()
    {
        // Arrange: два разных CA (перегенерация).
        var cache = new BrokerCertificateCache();
        var (ca1, caKey1) = ClusterPki.GenerateCa("events");
        var (ca2, caKey2) = ClusterPki.GenerateCa("events");

        // Act: серт под вторым CA.
        var cert2 = cache.GetOrCreate("events", "broker1", ca2, caKey2, "host.docker.internal");

        // Assert: серт подписан вторым CA (кеш не вернул протухший).
        using var ca2Cert = X509Certificate2.CreateFromPem(ca2);
        X509Certificate2.CreateFromPem(cert2.CertPem).Issuer.Should().Be(ca2Cert.Subject);
    }
}
```

- [ ] **Step 2: Прогнать — убедиться, что падает (нет типов)**

Run: `dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter "FullyQualifiedName~ClusterPkiTests|FullyQualifiedName~BrokerCertificateCacheTests"`
Expected: FAIL — `ClusterPki`/`BrokerCertificateCache` не существуют (CS0103).

- [ ] **Step 3: Реализовать ClusterPki и BrokerCertificateCache**

`src/KafkaWorker.Core/Templates/ClusterPki.cs`:

```csharp
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace KafkaWorker.Core.Templates;

/// <summary>
/// Per-cluster PKI (arch/16 §2.3): self-signed CA (RSA-2048, CN=kfw-&lt;C&gt;-ca,
/// 10 лет) и серверные серты нод (CN=broker&lt;k&gt;, SAN docker-DNS + advertised,
/// EKU ServerAuth, 10 лет) — CertificateRequest .NET, без внешних инструментов.
/// PEM — одной строкой с \n (канон значений etcd, arch/15 §2.1).
/// </summary>
public static class ClusterPki
{
    private static readonly Oid ServerAuthOid = new("1.3.6.1.5.5.7.3.1");

    public static (string CaPem, string CaKeyPem) GenerateCa(string cluster)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN=kfw-{cluster}-ca", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        using var ca = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(10));
        return (ca.ExportCertificatePem(), ca.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());
    }

    public static (string CertPem, string KeyPem) IssueBrokerCertificate(
        string caCertPem, string caKeyPem, string commonName,
        IReadOnlyList<string> dnsNames, IPAddress? ip)
    {
        using var caCertificate = ParseCertificate(caCertPem);
        using var caKey = ParseRsaKey(caKeyPem);
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
            new X509EnhancedKeyUsageExtension([ServerAuthOid], critical: false));
        using var certificate = request.Create(
            caCertificate, DateTimeOffset.UtcNow.AddDays(-1),
            DateTimeOffset.UtcNow.AddYears(10), RandomNumberGenerator.GetBytes(16));
        return (certificate.ExportCertificatePem(), rsa.ExportPkcs8PrivateKeyPem());
    }

    public static bool TryParseCertificate(string pem, out X509Certificate2? certificate)
    {
        try
        {
            certificate = ParseCertificate(pem);
            return true;
        }
        catch (Exception e) when (
            e is ArgumentException or CryptographicException or FormatException)
        {
            certificate = null;
            return false;
        }
    }

    public static bool TryParseRsaKey(string pem, out RSA? key)
    {
        try
        {
            key = ParseRsaKey(pem);
            return true;
        }
        catch (Exception e) when (
            e is ArgumentException or CryptographicException or FormatException)
        {
            key = null;
            return false;
        }
    }

    private static X509Certificate2 ParseCertificate(string pem)
    {
        var chain = X509PemLoader.LoadCertificates(pem);
        if (chain.Count == 0)
            throw new ArgumentException("PEM не содержит сертификат");
        return new X509Certificate2(chain[0].RawData);
    }

    private static RSA ParseRsaKey(string pem)
    {
        var rsa = RSA.Create();
        rsa.ImportFromPem(pem);
        return rsa;
    }
}
```

`src/KafkaWorker.Core/Templates/BrokerCertificateCache.cs`:

```csharp
using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography;
using KafkaWorker.Core.Templates;

namespace KafkaWorker.Core.Templates;

/// <summary>
/// Кеш серверных сертов нод (R3, arch/16 §2.3): серт генерируется один раз
/// на (кластер, нода, CA) в рамках жизни процесса — повторные сборки env
/// (надзор/ротация/регенерация) дают тот же PEM. Смена CA (hash ключа) —
/// новый серт. DI-синглтон.
/// </summary>
public sealed class BrokerCertificateCache
{
    private readonly ConcurrentDictionary<(string Cluster, string Broker, string CaHash), (string CertPem, string KeyPem)> _certificates = new();

    public (string CertPem, string KeyPem) GetOrCreate(
        string cluster, string brokerName, string caCertPem, string caKeyPem, string advertisedClientHost)
    {
        var caHash = Convert.ToHexString(SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(caKeyPem)));
        return _certificates.GetOrAdd((cluster, brokerName, caHash), _ => Issue(caCertPem, caKeyPem, brokerName, advertisedClientHost));
    }

    // SAN-правило arch/16 §2.3: DNS broker<k> (INTERNAL advertised) +
    // advertised-хост CLIENT (DNS либо IP — как резолвят клиенты по endpoints).
    private static (string CertPem, string KeyPem) Issue(
        string caCertPem, string caKeyPem, string brokerName, string advertisedClientHost)
    {
        var host = HostOf(advertisedClientHost);
        IPAddress? ip = IPAddress.TryParse(host, out var parsed) ? parsed : null;
        var dnsNames = ip is null ? new[] { brokerName, host } : new[] { brokerName };
        return ClusterPki.IssueBrokerCertificate(caCertPem, caKeyPem, brokerName, dnsNames, ip);
    }

    // "host.docker.internal:16001" → "host.docker.internal" (порт SAN не входит).
    private static string HostOf(string advertisedClient)
    {
        var separator = advertisedClient.LastIndexOf(':');
        return separator > 0 ? advertisedClient[..separator] : advertisedClient;
    }
}
```

- [ ] **Step 4: Прогнать тесты — PASS**

Run: `dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter "FullyQualifiedName~ClusterPkiTests|FullyQualifiedName~BrokerCertificateCacheTests"`
Expected: PASS (6 тестов).

- [ ] **Step 5: Commit**

```bash
git add src/KafkaWorker.Core/Templates/ClusterPki.cs src/KafkaWorker.Core/Templates/BrokerCertificateCache.cs src/tests/KafkaWorker.UnitTests/Templates/ClusterPkiTests.cs
git commit -m "feat(kafka): ClusterPki — per-cluster CA и серты нод (t03 Ф1)"
```

---

### Task 2: NodeEnvBuilder — новый env-канон SASL_SSL + JAAS ролей (Ф1)

Расширение `NodeEnvSpec` — Вариант А (решение plan-фазы №6): канонические позиции новых параметров + минимальная компенсация ТРЁХ прод-вызовов placeholder'ами в этом же коммите — сборка и юнит-набор зелёные на каждом коммите (bisect). Полная замена placeholder'ов — Task 8/10.

**Files:**
- Modify: `src/KafkaWorker.Core/Templates/NodeEnvBuilder.cs`
- Modify (переходная компенсация, до Task 8): `src/KafkaWorker.Provisioning/Processes/BrokerEnvBuilder.cs`, `src/KafkaWorker.Provisioning/Processes/NodeSupervisor.cs`, `src/KafkaWorker.Provisioning/Processes/ProvisioningProcess.cs`
- Test: `src/tests/KafkaWorker.UnitTests/Templates/NodeEnvBuilderTests.cs` (+точечные фиксы ассертов env в тестах процессов — grep, см. Step 4)

**Interfaces:**
- Consumes: строки PEM (из Task 1 в проде; константы-заглушки в тестах).
- Produces (используют Tasks 8, 10, 15):
  - `NodeEnvSpec` с новыми полями на канонических позициях: `string AdminUser, IReadOnlyList<string> AdminPasswords, string CaPem, string BrokerCertPem, string BrokerKeyPem` (после `AppPasswords`, до `Config`) — БЕЗ дефолтов: обязательность фиксируется компилятором.
  - `NodeEnvBuilder.Build(NodeEnvSpec)` — полный env-набор 16 §2.2 (SASL_SSL map, SSL-PEM пару, StandardAuthorizer, super.users, deny-by-default, JAAS `user_admin`[+2]/`user_app`[+2] на INTERNAL и CLIENT).
  - `NodeEnvBuilder.InterBrokerPassword/ClusterId` — без изменений.
  - Переходные placeholder-аргументы в 3 прод-вызовах (см. Step 3б) — удалит Task 8 (BrokerEnvBuilder/ProvisioningProcess) и Task 10 (ротатор идёт через BrokerEnvBuilder — компенсируется уже Step 3б).

- [ ] **Step 1: Обновить/дописать failing-тесты**

В `src/tests/KafkaWorker.UnitTests/Templates/NodeEnvBuilderTests.cs`: обновить `Spec(...)` под новую сигнатуру (поля `adminUser: "admin"`, `adminPasswords: ["AdminPassword0123456789AbCdEf01"]`, `caPem/brokerCertPem/brokerKeyPem` — константные PEM-строки-заглушки `"-----BEGIN CERTIFICATE-----\n…"`, серты НЕ валидируются билдером) и заменить/добавить кейсы:

```csharp
[Fact]
public void Build_SecurityProtocolMap_SslOnInternalClient_PlaintextController()
{
    // Arrange: штатный спек.
    // Act: генерация env.
    var env = NodeEnvBuilder.Build(Spec());

    // Assert: канон t03 (16 §2.2) — SASL_SSL на INTERNAL/CLIENT.
    env["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"]
        .Should().Be("CONTROLLER:PLAINTEXT,INTERNAL:SASL_SSL,CLIENT:SASL_SSL");
}

[Fact]
public void Build_SslPemKeystoreAndTruststore()
{
    // Arrange: спек с PEM-строками CA/серта/ключа.
    var spec = Spec();

    // Act: генерация env.
    var env = NodeEnvBuilder.Build(spec);

    // Assert: PEM-пара в keystore, CA в truststore (16 §2.2).
    env["KAFKA_SSL_KEYSTORE_TYPE"].Should().Be("PEM");
    env["KAFKA_SSL_KEYSTORE_CERTIFICATE_CHAIN"].Should().Be(spec.BrokerCertPem);
    env["KAFKA_SSL_KEYSTORE_KEY"].Should().Be(spec.BrokerKeyPem);
    env["KAFKA_SSL_TRUSTSTORE_TYPE"].Should().Be("PEM");
    env["KAFKA_SSL_TRUSTSTORE_CERTIFICATES"].Should().Be(spec.CaPem);
}

[Fact]
public void Build_AuthorizerSuperUsersAndDenyByDefault()
{
    // Arrange / Act: штатный env.
    var env = NodeEnvBuilder.Build(Spec());

    // Assert: StandardAuthorizer + super.users + deny-by-default (16 §2.3).
    env["KAFKA_AUTHORIZER_CLASS_NAME"]
        .Should().Be("org.apache.kafka.metadata.authorizer.StandardAuthorizer");
    env["KAFKA_SUPER_USERS"].Should().Be("User:admin;User:inter");
    env["KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND"].Should().Be("false");
}

[Fact]
public void Build_JaasRoles_AdminAndAppOnBothListeners_RotationWindows()
{
    // Arrange: окно ротации ОБЕИХ ролей (app: old+new; admin: old+new).
    var spec = Spec(
        passwords: ["AppOld0123456789AAAAAAAAAAAAAAAA", "AppNew0123456789AAAAAAAAAAAAAAAA"],
        adminPasswords: ["AdmOld0123456789AAAAAAAAAAAAAAAA", "AdmNew0123456789AAAAAAAAAAAAAAAA"]);

    // Act: генерация env.
    var env = NodeEnvBuilder.Build(spec);

    // Assert: INTERNAL несёт inter-креды клиента + пользователей обеих ролей
    // с окнами user_<name>2 (16 §2.2); CLIENT — только пользователей.
    env["KAFKA_LISTENER_NAME_INTERNAL_PLAIN_SASL_JAAS_CONFIG"].Should().ContainAll(
        @"username=""inter""", @"user_inter=""", @"user_admin=""AdmOld", @"user_admin2=""AdmNew",
        @"user_app=""AppOld", @"user_app2=""AppNew");
    env["KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG"].Should().ContainAll(
        @"user_admin=""AdmOld", @"user_admin2=""AdmNew", @"user_app=""AppOld", @"user_app2=""AppNew");
    env["KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG"].Should().NotContain(@"username=");
}
```

Плюс кейс одного пароля роли (без `2`-суффикса) — по образцу существующего `Build_Jaas_SingleAndDualUsers`, адаптированный под обе роли.

- [ ] **Step 2: Прогнать — FAIL (компиляция: нет полей в NodeEnvSpec)**

Run: `dotnet build src/PgWorker.slnx -c Debug`
Expected: FAIL — `NodeEnvSpec` не содержит `AdminPasswords`/`CaPem`/`BrokerCertPem`/`BrokerKeyPem`; `KAFKA_SSL_*` ассерты упадут после компиляции.

- [ ] **Step 3: Реализовать новый Build + компенсация прод-вызовов (Вариант А)**

3а. В `src/KafkaWorker.Core/Templates/NodeEnvBuilder.cs`:
1. Расширить `NodeEnvSpec` (позиционные параметры — `AdminUser`, `AdminPasswords`, `CaPem`, `BrokerCertPem`, `BrokerKeyPem` между `AppPasswords` и `Config`) с doc-комментариями по образцу существующих.
2. Заменить `Build` (ключевые фрагменты; остальная таблица — retention/partitions/служебные топики — без изменений):

```csharp
public static IReadOnlyDictionary<string, string> Build(NodeEnvSpec spec)
{
    var users = BuildRoleUsers(spec.AppUser, spec.AppPasswords, spec.AdminUser, spec.AdminPasswords);
    var interPassword = InterBrokerPassword(spec.Cluster);
    var env = new Dictionary<string, string>
    {
        // ... CLUSTER_ID / NODE_ID / PROCESS_ROLES / QUORUM_VOTERS / LISTENERS /
        // ADVERTISED_LISTENERS — без изменений ...
        ["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"] =
            "CONTROLLER:PLAINTEXT,INTERNAL:SASL_SSL,CLIENT:SASL_SSL",
        ["KAFKA_CONTROLLER_LISTENER_NAMES"] = "CONTROLLER",
        ["KAFKA_INTER_BROKER_LISTENER_NAME"] = "INTERNAL",
        ["KAFKA_SASL_ENABLED_MECHANISMS"] = "PLAIN",
        ["KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL"] = "PLAIN",

        // TLS: PEM-пара серта ноды + доверие per-cluster CA (arch/16 §2.2/§2.3).
        ["KAFKA_SSL_KEYSTORE_TYPE"] = "PEM",
        ["KAFKA_SSL_KEYSTORE_CERTIFICATE_CHAIN"] = spec.BrokerCertPem,
        ["KAFKA_SSL_KEYSTORE_KEY"] = spec.BrokerKeyPem,
        ["KAFKA_SSL_TRUSTSTORE_TYPE"] = "PEM",
        ["KAFKA_SSL_TRUSTSTORE_CERTIFICATES"] = spec.CaPem,

        // Authorization: StandardAuthorizer (KRaft), deny-by-default,
        // super.users — принципалы SASL-имён admin/inter (arch/16 §2.3).
        ["KAFKA_AUTHORIZER_CLASS_NAME"] = "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
        ["KAFKA_SUPER_USERS"] = "User:admin;User:inter",
        ["KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND"] = "false",

        // INTERNAL-JAAS: inter-креды брокера-клиента + пользователи ролей
        // admin/app с окнами ротации user_<name>2 (arch/16 §2.2).
        ["KAFKA_LISTENER_NAME_INTERNAL_PLAIN_SASL_JAAS_CONFIG"] =
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            + $@"username=""inter"" password=""{interPassword}"" user_inter=""{interPassword}"" {users};",
        ["KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG"] =
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            + $"{users};",

        // ... далее — без изменений (RF/minISR/retention/num.partitions/
        // auto.create.topics.enable=false/LOG_DIRS) ...
    };
    return env;
}

// Пользователи обеих ролей одним списком: admin первым, app вторым; у роли
// user_<name> + опционально user_<name>2 (окно ротации); пароли ТОЛЬКО в
// двойных кавычках (JAAS-парсер — комментарий ниже сохранить).
private static string BuildRoleUsers(
    string appUser, IReadOnlyList<string> appPasswords,
    string adminUser, IReadOnlyList<string> adminPasswords)
    => UsersOf(adminUser, adminPasswords) + " " + UsersOf(appUser, appPasswords);

private static string UsersOf(string user, IReadOnlyList<string> passwords)
    => passwords.Count > 1
        ? $@"user_{user}=""{passwords[0]}"" user_{user}2=""{passwords[1]}"""
        : $@"user_{user}=""{passwords[0]}""";
```

3. Удалить старый `BuildJaasUsers` (заменён `BuildRoleUsers`); doc-комментарий класса обновить: «SASL_SSL на INTERNAL/CLIENT, PLAINTEXT CONTROLLER внутри kfw-net-<C>; StandardAuthorizer deny-by-default». Сохранить существующий комментарий о JAAS-парсере и кавычках.

3б. **Компенсация трёх прод-вызовов `new NodeEnvSpec(...)`** — снапшот ещё не несёт admin/CA (Task 4), поэтому переходные placeholder-аргументы (удалит Task 8; env в этом переходном окне не поднимается — интеграционные прогоны задач 9/15 идут после полной замены):

1. `src/KafkaWorker.Provisioning/Processes/BrokerEnvBuilder.cs` (вызов в `Build` — покрывает и AppPasswordRotator/AddBroker/NodeRegenerator, которые зовут `BrokerEnvBuilder.Build`):

```csharp
return NodeEnvBuilder.Build(new NodeEnvSpec(
    snap.Cluster,
    NodeId(broker),
    broker,
    advertisedClient,
    decl.Role == "controller",
    QuorumVoters(snap),
    snap.AppUser ?? "app",
    passwords,
    // Переходное состояние t03 (полный контур admin/CA — Task 8 плана):
    // снапшот ещё не несёт полей безопасности — placeholder до Task 4/8.
    "admin",
    ["AdminPlaceholder0123456789AAAAAAAAA"],
    "-----BEGIN CERTIFICATE-----\nPLACEHOLDER\n-----END CERTIFICATE-----",
    "-----BEGIN CERTIFICATE-----\nPLACEHOLDER\n-----END CERTIFICATE-----",
    "-----BEGIN PRIVATE KEY-----\nPLACEHOLDER\n-----END PRIVATE KEY-----",
    snap.Config,
    snap.Config.Brokers,
    "/var/lib/kafka/data"));
```

2. `src/KafkaWorker.Provisioning/Processes/NodeSupervisor.cs` (~строка 230, прямой вызов `new NodeEnvSpec(...)` в пересоздании) — вставить те же 5 переходных аргументов на те же позиции.
3. `src/KafkaWorker.Provisioning/Processes/ProvisioningProcess.cs` (~строка 276, `EnsureNodesAsync`) — то же.

- [ ] **Step 4: Прогнать сборку решения и юнит-набор — PASS**

Run: `dotnet build src/PgWorker.slnx -c Debug && dotnet test src/tests/KafkaWorker.UnitTests -c Debug`
Expected: сборка чисто (три вызова скомпенсированы); NodeEnvBuilderTests — PASS; тесты процессов, ассертящие env-содержимое JAAS/протокол-мапы, при падении обновить на новый канон с placeholder-admin (grep `SASL_PLAINTEXT|KAFKA_LISTENER_NAME|user_app` по `src/tests/KafkaWorker.UnitTests/Provisioning` — точечные фиксы ассертов, не фикстур создания).

- [ ] **Step 5: Commit (Core + компенсированный прод — bisect-целостный)**

```bash
git add -A src/KafkaWorker.Core/Templates src/KafkaWorker.Provisioning/Processes/BrokerEnvBuilder.cs src/KafkaWorker.Provisioning/Processes/NodeSupervisor.cs src/KafkaWorker.Provisioning/Processes/ProvisioningProcess.cs src/tests/KafkaWorker.UnitTests
git commit -m "feat(kafka): NodeEnvBuilder — SASL_SSL, authorizer, JAAS admin+app; переходные placeholder-вызовы прод-сборок env (t03 Ф1, arch/16 §2.2)"
```

---

### Task 3: ClusterSecretEnsurer — ensure CA + admin + app одной txn (Ф2)

**Files:**
- Create: `src/KafkaWorker.Provisioning/Processes/ClusterSecretEnsurer.cs` (перенос+обобщение)
- Delete: `src/KafkaWorker.Provisioning/Processes/AppSecretEnsurer.cs`
- Modify: `src/KafkaWorker.App/Program.cs` (DI-замена `IAppSecretEnsurer` → `IClusterSecretEnsurer`)
- Test: rename `src/tests/KafkaWorker.UnitTests/Provisioning/AppSecretEnsurerTests.cs` → `ClusterSecretEnsurerTests.cs` (расширить)

**Interfaces:**
- Consumes: `ClusterPki.GenerateCa` (Task 1), `KafkaPasswordGenerator`, `IEtcdGateway`/txn-типы.
- Produces (используют Tasks 8, 15):
  - `public sealed record ClusterSecrets(string AppUser, string AppPassword, string AdminUser, string AdminPassword, string CaPem, string CaKey);`
  - `public interface IClusterSecretEnsurer { Task<Result<ClusterSecrets>> EnsureAsync(string cluster, CancellationToken ct); }`
  - Ключи: `/kafka/clusters/<C>/{app_user,app_password,admin_user,admin_password,ca_pem,ca_key}`; ensure — ОДНА txn put-if-absent только по отсутствующим; проигрыш → re-read; CA генерируется случайно (не из сида).

- [ ] **Step 1: Failing-тесты (порт + новые кейсы)**

В `ClusterSecretEnsurerTests.cs` (после переименования класса/файла) сохранить существующие кейсы (ensure пустого кластера, идемпотентность, гонка txn) на новой записи `ClusterSecrets` и добавить:

```csharp
[Fact]
public async Task Ensure_EmptyCluster_CreatesAllSixKeysInOneTxn()
{
    // Arrange: пустой префикс кластера.
    // Act: ensure.
    var ensured = await ensurer.EnsureAsync(cluster, ct);

    // Assert: admin_user="admin", пароли 32 симв, CA — валидный PEM-ключ+серт.
    ensured.IsSuccess.Should().BeTrue();
    var s = ensured.Value;
    s.AdminUser.Should().Be("admin");
    s.AdminPassword.Should().MatchRegex("^[A-Za-z0-9]{32}$");
    s.CaPem.Should().StartWith("-----BEGIN CERTIFICATE-----");
    s.CaKey.Should().StartWith("-----BEGIN PRIVATE KEY-----");
    ClusterPki.TryParseCertificate(s.CaPem, out _).Should().BeTrue();
    (await GetAsync($"/kafka/clusters/{cluster}/admin_user")).Should().Be("admin");
    (await GetAsync($"/kafka/clusters/{cluster}/ca_pem")).Should().Be(s.CaPem);
    (await GetAsync($"/kafka/clusters/{cluster}/ca_key")).Should().Be(s.CaKey);
}

[Fact]
public async Task Ensure_PartialKeys_ExistingNotOverwritten_CaRegeneratedOnlyIfAbsent()
{
    // Arrange: панель/прошлый ensure записал app_user=app, app_password=Secret…;
    // admin/CA отсутствуют.
    await PutAsync($"/kafka/clusters/{cluster}/app_user", "app");
    await PutAsync($"/kafka/clusters/{cluster}/app_password", "Existing0123456789AAAAAAAAA");

    // Act: ensure.
    var ensured = await ensurer.EnsureAsync(cluster, ct);

    // Assert: существующие не переписаны, отсутствующие добраны той же txn-механикой.
    ensured.Value.AppPassword.Should().Be("Existing0123456789AAAAAAAAA");
    (await GetAsync($"/kafka/clusters/{cluster}/app_password")).Should().Be("Existing0123456789AAAAAAAAA");
    (await GetAsync($"/kafka/clusters/{cluster}/ca_pem")).Should().NotBeNull();
}
```

(Хелперы `GetAsync/PutAsync` — по образцу текущего `AppSecretEnsurerTests`.)

- [ ] **Step 2: Прогнать — FAIL**

Run: `dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter FullyQualifiedName~ClusterSecretEnsurerTests`
Expected: FAIL — нет `IClusterSecretEnsurer`.

- [ ] **Step 3: Реализовать**

`ClusterSecretEnsurer.cs` — перенос тела `AppSecretEnsurer` с обобщением: `ReadAsync` читает 6 ключей (по образцу, failover-цикл); отсутствующим присваивает: `admin_user="admin"`, `admin_password=KafkaPasswordGenerator.Generate()`, `app_user="app"`, `app_password=Generate()`, `ca=ClusterPki.GenerateCa(cluster)` (ОДИН вызов даёт и pem, и key); txn `NotExists`-сравнения только по отсутствующим + `TxnOp.Put` каждого; re-read; финальная проверка полноты (по образцу текущей, текст ошибки: `ensure секретов кластера {cluster}: после txn ключи неполны (…)`). `git rm src/KafkaWorker.Provisioning/Processes/AppSecretEnsurer.cs`.

В `Program.cs` заменить регистрацию:

```csharp
// Ensure per-cluster секретов: CA + креды admin/app (arch/16 §4, t03).
builder.Services.AddSingleton<IClusterSecretEnsurer>(sp => new ClusterSecretEnsurer(
    sp.GetRequiredService<IEtcdGateway>(),
    sp.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Etcd.Endpoints));
```

и конструктор `ProvisioningProcess` — параметр `IAppSecretEnsurer appSecret` → `IClusterSecretEnsurer secrets` (в `ProvisioningProcess.cs` поле/вызов `appSecret.EnsureAsync` → `secrets.EnsureAsync`; результат `ClusterSecrets`). Юнит-тесты `ProvisioningProcessTests` — fake ensure возвращает `ClusterSecrets`-заглушку (PEM-константы).

- [ ] **Step 4: Прогнать — PASS + сборка воркера**

Run: `dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter "FullyQualifiedName~ClusterSecretEnsurerTests|FullyQualifiedName~ProvisioningProcessTests"` затем `dotnet build src/PgWorker.slnx -c Debug`
Expected: PASS; сборка чисто (остальные процессы не трогают ensure).

- [ ] **Step 5: Commit**

```bash
git add -A src/KafkaWorker.Provisioning src/KafkaWorker.App src/tests/KafkaWorker.UnitTests
git commit -m "feat(kafka): ClusterSecretEnsurer — ensure CA+admin+app (t03 Ф2)"
```

---

### Task 4: Снапшот-модель и парсер воркера — ключи admin/CA (Ф2)

**Files:**
- Modify: `src/KafkaWorker.Core/Model/KafkaDomain.cs` (`KafkaClusterSnapshot`)
- Modify: `src/KafkaWorker.Etcd/Parsing/KafkaSnapshotParser.cs`
- Test: `src/tests/KafkaWorker.UnitTests/Etcd/` (найти существующий файл тестов парсера — `KafkaSnapshotParserTests.cs`; если файл называется иначе — дописать туда)

**Interfaces:**
- Consumes: `ClusterPki.TryParseCertificate/TryParseRsaKey`.
- Produces (используют Tasks 5, 8, 10, 15): `KafkaClusterSnapshot` + опциональные поля `string? AdminUser = null, string? AdminPassword = null, string? CaPem = null, string? CaKey = null` (после `AppPassword`); парсер: leaf-ключи `admin_user`/`admin_password`/`ca_pem`/`ca_key` (5 сегментов); битый PEM → `parseErrors`-запись + поле null (15 §6).

- [ ] **Step 1: Failing-тесты**

```csharp
[Fact]
public void Parse_AdminAndCaKeys_FilledIntoSnapshot()
{
    // Arrange: полный набор ключей кластера (вкл. admin_user/admin_password/ca_pem/ca_key).
    var kvs = ClusterKvs("events", extra:
    [
        Kv("/kafka/clusters/events/admin_user", "admin"),
        Kv("/kafka/clusters/events/admin_password", "AdminSecret0123456789AAAAAAA"),
        Kv("/kafka/clusters/events/ca_pem", ValidCaPem),
        Kv("/kafka/clusters/events/ca_key", ValidCaKeyPem),
    ]);

    // Act: разбор.
    var snap = KafkaSnapshotParser.Parse(kvs).Value.Single();

    // Assert: поля дискавери/секретов заполнены, unknownKeys их не считает.
    snap.AdminUser.Should().Be("admin");
    snap.AdminPassword.Should().Be("AdminSecret0123456789AAAAAAA");
    snap.CaPem.Should().Be(ValidCaPem);
    snap.CaKey.Should().Be(ValidCaKeyPem);
    snap.UnknownKeys.Should().Be(0);
}

[Fact]
public void Parse_MalformedCaPem_ParseErrorAndNullField()
{
    // Arrange: ca_pem — мусор (15 §6: битый PEM → parseError, ключ пропускается).
    var kvs = ClusterKvs("events", extra: [Kv("/kafka/clusters/events/ca_pem", "garbage")]);

    // Act: разбор.
    var snap = KafkaSnapshotParser.Parse(kvs).Value.Single();

    // Assert: поле null + запись parseErrors (не исключение).
    snap.CaPem.Should().BeNull();
    snap.ParseErrors.Should().Contain(e => e.Contains("ca_pem", StringComparison.Ordinal));
}

[Fact]
public void Parse_MissingSecurityKeys_NullFields_NoErrors()
{
    // Arrange: премиграционный кластер — только app-креды.
    var kvs = ClusterKvs("old", extra:
    [
        Kv("/kafka/clusters/old/app_user", "app"),
        Kv("/kafka/clusters/old/app_password", "AppSecret0123456789AAAAAAAA"),
    ]);

    // Act: разбор.
    var snap = KafkaSnapshotParser.Parse(kvs).Value.Single();

    // Assert: admin/CA null, ошибок нет — детект премиграционного кластера (M).
    snap.AdminUser.Should().BeNull();
    snap.CaPem.Should().BeNull();
    snap.ParseErrors.Should().BeEmpty();
}
```

`ValidCaPem/ValidCaKeyPem` — генерируются в тесте один раз через `ClusterPki.GenerateCa("test")` (статик-поле класса тестов). Хелперы `ClusterKvs/Kv` — по образцу существующих тестов парсера.

- [ ] **Step 2: Прогнать — FAIL**

Run: `dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter FullyQualifiedName~KafkaSnapshotParser`
Expected: FAIL — нет полей.

- [ ] **Step 3: Реализовать**

1. `KafkaDomain.cs`: добавить 4 опциональных поля в конец `KafkaClusterSnapshot` (doc-комментарий: «поля безопасности t03 (arch/15 §2): null — премиграционный кластер, мигрирует M»).
2. `KafkaSnapshotParser.cs`: в `ClusterAcc` — поля `AdminUser/AdminPassword/CaPem/CaKey`; в switch — 4 кейса по образцу `app_user`; для `ca_pem`: `ClusterPki.TryParseCertificate(value, out _)` false → `Errors.Add($"/kafka/clusters/{name}/ca_pem: битый PEM сертификата")`, поле null; для `ca_key`: `ClusterPki.TryParseRsaKey(...)` false → аналогично; `BuildCluster` — прокинуть поля.

- [ ] **Step 4: Прогнать — PASS**

Run: `dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter FullyQualifiedName~KafkaSnapshotParser`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/KafkaWorker.Core/Model/KafkaDomain.cs src/KafkaWorker.Etcd/Parsing/KafkaSnapshotParser.cs src/tests/KafkaWorker.UnitTests
git commit -m "feat(kafka): снапшот-парсер — ключи admin/CA, битый PEM → parseError (t03 Ф2)"
```

---

### Task 5: Панель — креды admin/CA (стор, парсер, алерты, admin_rotations) и пробы по SASL_SSL (Ф2)

Панельный контур ЦЕЛИКОМ: чтение новых кредов (internal-стор), парсер, алерты, очередь `admin_rotations` и живые пробы (`AdminPanel.Probes`) — admin-кред + `ca_pem`, SASL_SSL, ключ кэша с caPem. После смены формата `KafkaClusterSecrets` пробы не скомпилируются без этого контура — задача едина.

**Files:**
- Modify: `src/AdminPanel.Etcd/KafkaSecretsStore.cs`
- Modify: `src/AdminPanel.Etcd/KafkaSnapshotRefresher.cs`
- Modify: `src/AdminPanel.Etcd/Parsing/KafkaParser.cs`
- Modify: `src/AdminPanel.Core/Kafka/KafkaSnapshot.cs`
- Modify: `src/AdminPanel.Core/Kafka/KafkaAlerting/KafkaAlertEngine.cs`
- Modify: `src/AdminPanel.Probes/Kafka/KafkaClientCache.cs`
- Modify: `src/AdminPanel.Probes/Kafka/KafkaProbe.cs` (seam-интерфейсы)
- Modify: `src/AdminPanel.Probes/Kafka/ConfluentKafkaProbeClient.cs` (оба адаптера)
- Modify: `src/AdminPanel.Probes/Kafka/KafkaProbeLoop.cs`
- Test: `src/tests/AdminPanel.UnitTests/` — `KafkaRefresherTests.cs`, `KafkaAlertRulesTests.cs`, `ProbesKafka/KafkaClientCacheTests.cs`, фиксы fake-интерфейсов в `ProbesKafka/KafkaProbeTests.cs`, `ProbesKafka/KafkaProbeTopicsTests.cs`, `ProbeOrchestratorTests.cs` (grep `IKafkaProbeClient|IKafkaProbeRuntimeClient` по `src/tests/AdminPanel.UnitTests`)

**Interfaces:**
- Consumes: Task 4-канон ключей (панель читает те же etcd-ключи); Confluent.Kafka (`SecurityProtocol.SaslSsl`, `ClientConfig.Set("ssl.ca.pem", …)`).
- Produces (используют Tasks 11, 13):
  - `KafkaClusterSecrets(string Cluster, string AdminUser, string AdminPassword, string CaPem)` — app-креды панель больше НЕ читает; потребители: `KafkaProbeLoop` (передаёт `creds.AdminUser/creds.AdminPassword/creds.CaPem` в seam-вызовы).
  - `KafkaParser.ParseAdminRotations(kvs)` → `KafkaRotationsParseResult` (формат заявки идентичен ротациям).
  - `KafkaSnapshot.AdminRotations` (новое поле `IReadOnlyList<KafkaRotationTicket>`, рядом с `Rotations`).
  - Алерты: critical `kafka-security-missing` (Active кластер без admin-креда/CA в сторе), warning `kafka-admin-rotation-pending:<C>` (порт `kafka-rotation-pending`).
  - `IKafkaProbeClient.DescribeClusterAsync(string bootstrap, string user, string password, string? caPem, TimeSpan timeout, CancellationToken ct)`; `IKafkaProbeRuntimeClient.*` — параметр `string? caPem` после `password` во всех пяти методах.
  - `KafkaClientCache`: `GetAdmin/GetConsumer(string bootstrap, string user, string password, string? caPem)`; `Invalidate(string bootstrap, string user, string password, string? caPem)`; ключ кэша `(bootstrap, user, password, caPem)`; `BaseAdminConfig` — `SaslSsl` + `Set("ssl.ca.pem", caPem)` при caPem != null.

- [ ] **Step 1: Failing-тесты стора/парсера/алертов**

1. `KafkaRefresherTests` — кейс: kvs с `admin_user/admin_password/ca_pem` → `secretsStore.Current[cluster]` несёт admin-кред и CaPem; кейс: битый `ca_pem` → в стор не попадает + parseErrors содержит запись.
2. `KafkaAlertRulesTests`:

```csharp
[Fact]
public void Evaluate_ActiveClusterWithoutSecurity_CriticalKafkaSecurityMissing()
{
    // Arrange: снапшот Active-кластера; securityReady пуст (нет admin/CA в сторе).
    var snapshot = SnapshotWithActiveCluster("events");
    var alerts = engine.Evaluate(snapshot, previous: null, securityReady: []);

    // Assert: critical-алерт канона 15 §6.
    alerts.Should().ContainSingle(a => a.Id == "kafka-security-missing:events"
        && a.Severity == AlertSeverity.Critical);
}

[Fact]
public void Evaluate_ClusterWithAdminAndCa_NoSecurityAlert()
{
    // Arrange: тот же кластер, securityReady содержит его.
    var alerts = engine.Evaluate(snapshot, null, securityReady: ["events"]);

    // Assert: алерта нет.
    alerts.Should().NotContain(a => a.Kind == "kafka-security-missing");
}

[Fact]
public void Evaluate_AdminRotationTicket_PendingWarning()
{
    // Arrange: AdminRotations содержит заявку events (порт kafka-rotation-pending).
    // Act / Assert: warning kafka-admin-rotation-pending:events.
    var alerts = engine.Evaluate(snapshot, null, securityReady: ["events"]);
    alerts.Should().ContainSingle(a => a.Id == "kafka-admin-rotation-pending:events"
        && a.Severity == AlertSeverity.Warning);
}
```

(Сигнатура `Evaluate` получает третий параметр `IReadOnlyCollection<string> securityReady` — список кластеров с полным набором admin+CA в сторе; refresher вычисляет из `ReadSecrets`. Прямая передача коллекции — меньше DI-связанности.)

- [ ] **Step 2: Прогнать — FAIL**

Run: `dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~KafkaRefresherTests|FullyQualifiedName~KafkaAlertRulesTests"`
Expected: FAIL.

- [ ] **Step 3: Реализовать стор/парсер/алерты**

1. `KafkaSecretsStore.cs`: `KafkaClusterSecrets(Cluster, AdminUser, AdminPassword, CaPem)`; doc: «t03: панель читает admin_user/admin_password/ca_pem ТОЛЬКО для SASL_SSL-проб (arch/02 §10.1); ca_key и app-креды панель не читает».
2. `KafkaSnapshotRefresher.cs`:
   - `Prefixes.AdminRotations = "/kafkaworker/admin_rotations/"`; добавить чтение + `KafkaParser.ParseAdminRotations` → `KafkaSnapshot.AdminRotations`; ошибки в общий parseErrors.
   - `ReadSecrets`: читать `admin_user/admin_password/ca_pem` (по образцу текущего метода); собрать `securityReady`-набор имён кластеров с непустыми admin_user+admin_password+валидным `ca_pem` (валидация — PEM-маркеры `BEGIN CERTIFICATE`/`END CERTIFICATE`, без библы Core: панель не ссылается на KafkaWorker.Core); битый `ca_pem` → parseErrors-запись и исключение кластера из стора; передать `securityReady` в `alertEngine.Evaluate(built, previous, securityReady)`.
3. `KafkaParser.cs`: `ParseAdminRotations` — точная копия `ParseRotations` с другим префиксом (doc: «заявка ротации admin-пароля, arch/15 §4»); в `ParseClusters` — expected-skip: case `"admin_user" or "admin_password" or "ca_pem" or "ca_key"` (рядом с текущим `app_user or "app_password"`).
4. `KafkaSnapshot.cs`: поле `AdminRotations` (в конец записи, значение по умолчанию `[]`, чтобы не ломать существующие конструкторы тестов; обновить FailTick-заглушку).
5. `KafkaAlertEngine.cs`: сигнатура `Evaluate(KafkaSnapshot current, KafkaSnapshot? previous, IReadOnlyCollection<string>? securityReady = null)`; правила:
   - `kafka-security-missing`: для каждого Active-кластера не в `securityReady` → `Alert(critical, id: $"kafka-security-missing:{c}", kind: "kafka-security-missing", ...)`;
   - `kafka-admin-rotation-pending`: точный порт `kafka-rotation-pending` (см. текущую реализацию) по `current.AdminRotations`.
   Все прочие вызовы `Evaluate` (тесты/сторонние) — компилируются из-за default-параметра.

- [ ] **Step 4: Failing-тесты проб (кэш с caPem + seam-сигнатуры)**

`ProbesKafka/KafkaClientCacheTests.cs` — новый кейс:

```csharp
[Fact]
public void GetAdmin_CacheKeyIncludesCaPem_SameCaReuses_NewCaRecreates()
{
    // Arrange: кэш; два разных CA-PEM (t03: смена CA → пересоздание клиентов).
    var cache = new KafkaClientCache();

    // Act: два GetAdmin с CA-A, затем один с CA-B.
    cache.GetAdmin("localhost:19094", "admin", "p1", "CAPEM-A");
    cache.GetAdmin("localhost:19094", "admin", "p1", "CAPEM-A");
    cache.GetAdmin("localhost:19094", "admin", "p1", "CAPEM-B");

    // Assert: переиспользование при том же CA, пересоздание при смене CA
    // (метрика churn'а CreatedClients — без сетевых подключений).
    cache.CreatedClients.Should().Be(2);
}
```

Плюс: обновить fake-реализации `IKafkaProbeClient`/`IKafkaProbeRuntimeClient` в `KafkaProbeTests.cs`/`KafkaProbeTopicsTests.cs` под новые сигнатуры (+`string? caPem`), fake фиксирует последний переданный caPem; кейс в `KafkaProbeTests`: цикл с кредами стора `{AdminUser:"admin", AdminPassword:"p", CaPem:"CAPEM"}` → fake получил `caPem: "CAPEM"` (до правки — не компилируется/пусто).

- [ ] **Step 5: Прогнать — FAIL**

Run: `dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~KafkaClientCacheTests|FullyQualifiedName~KafkaProbe"`
Expected: FAIL (сигнатуры/ключ кэша).

- [ ] **Step 6: Реализовать пробы**

1. `KafkaClientCache.cs`:
   - Ключ: `Dictionary<(string Bootstrap, string User, string Password, string? CaPem), Entry>` — все три публичных метода (`GetAdmin/GetConsumer/Invalidate`) принимают `string? caPem` и используют его в ключе (смена CA → другой ключ → пересоздание; старый клиент диспозится фоновой заменой при `Invalidate`/`Dispose` — механика t11 не меняется).
   - `BaseAdminConfig(bootstrap, user, password, caPem)`:

```csharp
private static AdminClientConfig BaseAdminConfig(string bootstrap, string user, string password, string? caPem)
{
    var config = new AdminClientConfig
    {
        BootstrapServers = bootstrap,
        SecurityProtocol = SecurityProtocol.SaslSsl, // t03: дискавери-канон arch/15 §5
        SaslMechanism = SaslMechanism.Plain,
        SaslUsername = user,
        SaslPassword = password,
        RetryBackoffMs = BackoffMs,
        ReconnectBackoffMs = BackoffMs,
        ReconnectBackoffMaxMs = BackoffMaxMs,
    };
    if (caPem is not null)
        config.Set("ssl.ca.pem", caPem); // доверие per-cluster CA (librdkafka >= 1.5)
    return config;
}
```

2. `KafkaProbe.cs` — сигнатуры обоих seam-интерфейсов: `string? caPem` после `password` (все 6 методов); doc-обновление: «SASL_SSL/PLAIN + ca_pem (arch/15 §5); креда — роль admin».
3. `ConfluentKafkaProbeClient.cs` — оба адаптера (`ConfluentKafkaProbeClient`, `ConfluentKafkaRuntimeProbeClient`): каждый `cache.GetAdmin/GetConsumer(bootstrap, user, password)` → `…(bootstrap, user, password, caPem)`; каждый `cache.Invalidate(bootstrap, user, password)` → `…(bootstrap, user, password, caPem)`; doc-комментарий адаптера: SASL_SSL.
4. `KafkaProbeLoop.cs`:
   - `creds`-использование: `client.DescribeClusterAsync(bootstrap, creds.AdminUser, creds.AdminPassword, creds.CaPem, timeout, ct)` и все `runtimeClient.*`-вызовы — `creds.AdminUser, creds.AdminPassword, creds.CaPem`;
   - текст отсутствия кредов: `"нет admin-кредов/CA кластера в etcd (премиграционный кластер или ensure не выполнен)"`.

- [ ] **Step 7: Прогон панельного набора — PASS**

Run: `dotnet test src/tests/AdminPanel.UnitTests -c Debug && dotnet build src/PgWorker.slnx -c Debug`
Expected: PASS (весь панельный набор; сборка чисто — Probes/Etcd/Core согласованы).

- [ ] **Step 8: Commit**

```bash
git add -A src/AdminPanel.Etcd src/AdminPanel.Core src/AdminPanel.Probes src/tests/AdminPanel.UnitTests
git commit -m "feat(panel): admin-креды/CA, kafka-security-missing, admin_rotations, пробы SASL_SSL (t03 Ф2)"
```

---

### Task 6: KafkaAdminClient — TLS (SaslSsl + ssl.ca.pem) и ACL-операции (Ф3)

**Files:**
- Modify: `src/KafkaWorker.Provisioning/Kafka/IKafkaAdminClient.cs`
- Modify: `src/KafkaWorker.Provisioning/Kafka/KafkaAdminClient.cs`
- Test: `src/tests/KafkaWorker.UnitTests/Provisioning/FakeKafkaAdminClient.cs` (+ интерфейсные фиксы)

**Interfaces:**
- Consumes: Confluent.Kafka 2.14.2 (`AdminClientConfig.Set`, `DescribeAclsAsync/CreateAclsAsync/DeleteAclsAsync`).
- Produces (используют Tasks 7, 8, 9, 10, 15):
  - `IKafkaAdminClientFactory.Create(string bootstrap, string user, string password, string? caPem)` — caPem null => без TLS-доверия (тесты/fake); real: `SecurityProtocol.SaslSsl` + `Set("ssl.ca.pem", caPem)`.
  - Свои ACL-типы (без Confluent-типов в сигнатурах — seam):
    ```csharp
    public enum KafkaAclResourceType { Unknown, Any, Topic, Group, Cluster, TransactionalId, DelegationToken, User }
    public enum KafkaAclPatternType { Unknown, Any, Match, Literal, Prefixed }
    public enum KafkaAclOperation { Unknown, Any, All, Read, Write, Create, Delete, Alter, Describe, IdempotentWrite, ClusterAction, DescribeConfigs, AlterConfigs }
    public enum KafkaAclPermission { Unknown, Any, Allow, Deny }
    public sealed record KafkaAclBinding(
        KafkaAclResourceType ResourceType, string ResourceName, KafkaAclPatternType PatternType,
        string Principal, KafkaAclOperation Operation, KafkaAclPermission Permission);
    ```
  - `IKafkaAdminClient`: `Task<Result<IReadOnlyList<KafkaAclBinding>>> DescribeAclsAsync(CancellationToken ct);` + `Task<Result> CreateAclsAsync(IReadOnlyList<KafkaAclBinding> acls, CancellationToken ct);` + `Task<Result> DeleteAclsAsync(IReadOnlyList<KafkaAclBinding> acls, CancellationToken ct);`

- [ ] **Step 1: Обновить fake + интерфейс**

`FakeKafkaAdminClient.cs` — реализовать 3 новых метода (список-заглушка `Acls` в fake, `CreateAclsAsync` добавляет, `DeleteAclsAsync` удаляет). Юнит-«тест на компиляцию интерфейса» не нужен: интерфейс проверяется тестами Task 7; здесь достаточно, чтобы юнит-проект собрался после правки (механика + фиксация API).

- [ ] **Step 2: Реализовать адаптер**

`KafkaAdminClient.cs`:
1. `KafkaAdminClientFactory.Create(bootstrap, user, password, caPem)` → конструктор `KafkaAdminClient(bootstrap, user, password, caPem, requestTimeout)`.
2. `EnsureClient()` — конфиг собирается в переменную, опция TLS задаётся ДО Build:

```csharp
private IAdminClient EnsureClient()
{
    if (_client is not null)
        return _client;

    var config = new AdminClientConfig
    {
        BootstrapServers = bootstrap,
        SecurityProtocol = SecurityProtocol.SaslSsl, // t03: дискавери-канон arch/15 §5
        SaslMechanism = SaslMechanism.Plain,
        SaslUsername = user,
        SaslPassword = password,
    };
    if (_caPem is not null)
        config.Set("ssl.ca.pem", _caPem); // доверие per-cluster CA (librdkafka >= 1.5)
    _client = new AdminClientBuilder(config).Build();
    return _client;
}
```

3. ACL-методы (маппинг своих enum ↔ `ResourceType/PatternType/AclOperation/AclPermission` Confluent; ошибки → `Result.Failed` тем же каркасом `RunAsync`):

```csharp
public Task<Result<IReadOnlyList<KafkaAclBinding>>> DescribeAclsAsync(CancellationToken ct)
    => RunAsync<IReadOnlyList<KafkaAclBinding>>(async client =>
    {
        var described = await client.DescribeAclsAsync(
            AclBindingFilter.Any, // все ACL кластера; фильтрацию делает AclPlan.Diff
            new DescribeAclsOptions { RequestTimeout = requestTimeout });
        return described.Acls.Select(ToBinding).ToList();
    }, ct);

// ToBinding(AclBinding b) => new KafkaAclBinding(Map(b.ResourceType), b.ResourceName,
//     Map(b.PatternType), b.Principal, Map(b.Operation), Map(b.Permission));
// CreateAclsAsync: client.CreateAclsAsync(acls.Select(ToConfluentBinding),
//     new CreateAclsOptions { RequestTimeout = requestTimeout });
// DeleteAclsAsync: по одному binding — AclBindingFilter-эквивалент (exact-match все поля)
//     client.DeleteAclsAsync([filter], new DeleteAclsOptions { RequestTimeout = requestTimeout });
```

(Если `AclBindingFilter.Any` в 2.14 отсутствует — конструктор `new AclBindingFilter()` без заполненных полей семантически «любой».)

4. Обновить ВСЕ вызовы фабрики в юнит-фикстурах (`+ null` последним аргументом), в интеграционных фикстурах — Task 8/9.

- [ ] **Step 3: Сборка + юнит-прогон**

Run: `dotnet test src/tests/KafkaWorker.UnitTests -c Debug`
Expected: PASS (адаптер реального клиента в юнит-тестах не создаётся — сетевых тестов нет).

- [ ] **Step 4: Commit**

```bash
git add -A src/KafkaWorker.Provisioning src/tests/KafkaWorker.UnitTests
git commit -m "feat(kafka): AdminClient SASL_SSL + ssl.ca.pem, Describe/Create/DeleteAcls (t03 Ф3)"
```

---

### Task 7: AclPlan + ACL-converge в ClusterConfigConverger (Ф3)

**Files:**
- Create: `src/KafkaWorker.Provisioning/Processes/AclPlan.cs`
- Modify: `src/KafkaWorker.Provisioning/Processes/ClusterConfigConverger.cs`
- Test: `src/tests/KafkaWorker.UnitTests/Provisioning/AclPlanTests.cs`, `ClusterConfigConvergerTests.cs`

**Interfaces:**
- Consumes: `KafkaAclBinding` + ACL-методы Task 6.
- Produces (используют Tasks 8, 15): 
  - `AclPlan.AppPrincipal = "User:app"`;
  - `static IReadOnlySet<KafkaAclBinding> AclPlan.Target()` — канон 16 §2.3: TOPIC `*` {READ, WRITE, DESCRIBE}; GROUP `*` {READ, DESCRIBE}; TRANSACTIONAL_ID `*` {WRITE, DESCRIBE}; все LITERAL/Allow;
  - `static (IReadOnlyList<KafkaAclBinding> Create, IReadOnlyList<KafkaAclBinding> Delete) AclPlan.Diff(IReadOnlyList<KafkaAclBinding> current)` — цель минус факт (create), лишние ACL принципала `User:app` (delete); чужие принципалы не трогаем;
  - `IClusterConfigConverger.ApplyAsync(string cluster, string bootstrap, string user, string password, string? caPem, KafkaClusterConfig config, CancellationToken ct)` — после конфиг-шага выполняет ACL-converge (DescribeAcls → Diff → Create/Delete; пустой diff — no-op).

- [ ] **Step 1: Failing-тесты AclPlan**

```csharp
public class AclPlanTests
{
    [Fact]
    public void Target_CanonicalSevenBindingsForAppRole()
    {
        // Arrange / Act: канонический план роли app (16 §2.3).
        var target = AclPlan.Target();

        // Assert: 7 ACL, все LITERAL/Allow, принципал User:app, wildcard-ресурс.
        target.Should().HaveCount(7);
        target.Should().OnlyContain(b =>
            b.Principal == "User:app" && b.Permission == KafkaAclPermission.Allow
            && b.PatternType == KafkaAclPatternType.Literal && b.ResourceName == "*");
        target.Where(b => b.ResourceType == KafkaAclResourceType.Topic)
            .Should().OnlyContain(b =>
                b.Operation is KafkaAclOperation.Read or KafkaAclOperation.Write or KafkaAclOperation.Describe);
        target.Where(b => b.ResourceType == KafkaAclResourceType.Group)
            .Should().OnlyContain(b =>
                b.Operation is KafkaAclOperation.Read or KafkaAclOperation.Describe);
        target.Where(b => b.ResourceType == KafkaAclResourceType.TransactionalId)
            .Should().OnlyContain(b =>
                b.Operation is KafkaAclOperation.Write or KafkaAclOperation.Describe);
    }

    [Fact]
    public void Diff_MissingCreated_SuperfluousDeleted_ForeignUntouched()
    {
        // Arrange: факт = половина цели + лишний ACL app (Create на TOPIC *) +
        // ACL чужого принципала User:someone.
        var current = new List<KafkaAclBinding>
        {
            new(KafkaAclResourceType.Topic, "*", KafkaAclPatternType.Literal, "User:app",
                KafkaAclOperation.Read, KafkaAclPermission.Allow),
            new(KafkaAclResourceType.Topic, "orders", KafkaAclPatternType.Literal, "User:app",
                KafkaAclOperation.Create, KafkaAclPermission.Allow),
            new(KafkaAclResourceType.Group, "*", KafkaAclPatternType.Literal, "User:someone",
                KafkaAclOperation.Read, KafkaAclPermission.Allow),
        };

        // Act: дифф.
        var (create, delete) = AclPlan.Diff(current);

        // Assert: создать 6 недостающих, удалить 1 лишний у app, чужого не трогать.
        create.Should().HaveCount(6);
        delete.Should().ContainSingle(b => b.Operation == KafkaAclOperation.Create);
        delete.Should().NotContain(b => b.Principal == "User:someone");
    }

    [Fact]
    public void Diff_Converged_NoChanges()
    {
        // Arrange: факт == цель.
        // Act / Assert: пустой дифф (идемпотентность).
        var (create, delete) = AclPlan.Diff([.. AclPlan.Target()]);
        create.Should().BeEmpty();
        delete.Should().BeEmpty();
    }
}
```

- [ ] **Step 2: Прогнать — FAIL**

Run: `dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter FullyQualifiedName~AclPlanTests`
Expected: FAIL — нет типа.

- [ ] **Step 3: Реализовать AclPlan + расширить Converger**

`AclPlan.cs` (namespace `KafkaWorker.Provisioning.Processes`; чистые функции, doc по 16 §2.3/E). Реализация `Target()` — литеральный `HashSet<KafkaAclBinding>` из 7 записей; `Diff` — `target.Except(current)` / `current.Where(b => b.Principal == AppPrincipal && !target.Contains(b))`.

`ClusterConfigConverger.ApplyAsync` — новая сигнатура (см. Interfaces); внутри после конфиг-шага (в т.ч. при конфиг-no-op — НЕ ранний выход из метода):

```csharp
// ACL-converge (arch/16 §5 E, t03): идемпотентная сходимость роли app.
var acls = await admin.DescribeAclsAsync(ct);
if (!acls.IsSuccess)
    return Result.Failed(acls.Error!);
var (create, delete) = AclPlan.Diff(acls.Value);
if (create.Count > 0)
{
    var created = await admin.CreateAclsAsync(create, ct);
    if (!created.IsSuccess)
        return created;
}
if (delete.Count > 0)
{
    var deleted = await admin.DeleteAclsAsync(delete, ct);
    if (!deleted.IsSuccess)
        return deleted;
}
return Result.Success();
```

Обновить `ClusterConfigConvergerTests` (fake из Task 6: `ApplyAsync` с caPem=null; кейс «конфиг сошёлся, но ACL пуст → CreateAcls вызван», кейс «ACL сошлись → no-op»).

- [ ] **Step 4: Прогон — PASS**

Run: `dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter "FullyQualifiedName~AclPlanTests|FullyQualifiedName~ClusterConfigConvergerTests"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/KafkaWorker.Provisioning/Processes/AclPlan.cs src/KafkaWorker.Provisioning/Processes/ClusterConfigConverger.cs src/tests/KafkaWorker.UnitTests
git commit -m "feat(kafka): ACL-converge роли app в E (t03 Ф3, arch/16 §2.3)"
```

---

### Task 8: Процессы воркера на admin+TLS; ReassignCli SASL_SSL; Provisioning K2/K3 новый канон (Ф3)

Задача снимает переходные placeholder'ы Task 2 (реальные admin/CA/серт из снапшота + `BrokerCertificateCache`).

**Files:**
- Modify: `arch/16-kafkaworker.md` (Step 0 — синхронизация канона)
- Modify: `src/KafkaWorker.Provisioning/Processes/BrokerEnvBuilder.cs`
- Modify: `src/KafkaWorker.Provisioning/Processes/ProvisioningProcess.cs`
- Modify: `src/KafkaWorker.Provisioning/Processes/NodeSupervisor.cs`
- Modify: `src/KafkaWorker.Provisioning/Processes/AddBrokerProcess.cs`
- Modify: `src/KafkaWorker.Provisioning/Processes/NodeRegenerator.cs`
- Modify: `src/KafkaWorker.Provisioning/Processes/TopicSyncProcess.cs`
- Modify: `src/KafkaWorker.Provisioning/Processes/PartitionReassignerProcess.cs`
- Modify: `src/KafkaWorker.Provisioning/Processes/RemoveBrokerProcess.cs`
- Modify: `src/KafkaWorker.Provisioning/Processes/ReassignCli.cs`
- Modify: `src/KafkaWorker.App/Loops/KafkaClusterProcesses.cs`
- Modify: `src/KafkaWorker.App/Program.cs`
- Test: `src/tests/KafkaWorker.UnitTests/Provisioning/ReassignCliTests.cs` + фиксы процессов

**Interfaces:**
- Consumes: `ClusterSecrets` (Task 3), `BrokerCertificateCache` (Task 1), `NodeEnvSpec`-канон (Task 2), снапшот-поля admin/CA (Task 4), `IKafkaAdminClientFactory.Create(…, caPem)` (Task 6), `IClusterConfigConverger.ApplyAsync(…, caPem, …)` (Task 7).
- Produces (используют Tasks 9, 10, 15): 
  - `internal static IReadOnlyDictionary<string, string> BrokerEnvBuilder.Build(KafkaClusterSnapshot snap, string broker, NodeAddress addr, IReadOnlyList<string> appPasswords, IReadOnlyList<string> adminPasswords, ProvisioningOptions options, BrokerCertificateCache certificates)` — guard: `snap.CaPem/CaKey/AdminPassword` null → `ApplicationException` «премиграционный кластер — сначала SecurityMigrator (M)»; placeholder-аргументы Task 2 удаляются.
  - Все AdminClient-подключения воркера: `adminFactory.Create(snap.Endpoints, snap.AdminUser ?? "admin", snap.AdminPassword!, snap.CaPem)` — bootstrap по CLIENT endpoints из etcd (решение plan-фазы №1; канон синхронизирован в Step 0).
  - `ReassignCli.BuildAdminProperties(string user, string password, string caPem)` — SASL_SSL + PEM-truststore; `BuildExecCommand(moves, bootstrap, user, password, caPem)`.

- [ ] **Step 0: Синхронизация канона arch/16 (arch-first — ДО кода транспорта)**

Две точечные замены в `arch/16-kafkaworker.md`:

1. §2.3 «Клиентские подключения воркера»:

OLD:
```
**Клиентские подключения воркера**: AdminClient и reassign-CLI ходят как
`admin` по INTERNAL (SASL_SSL, доверие `ca_pem`, bootstrap docker-DNS).
Пробы панели — `admin` + `ca_pem` по CLIENT. Приложения — `app` + `ca_pem`
по CLIENT (15 §5).
```

NEW:
```
**Клиентские подключения воркера**: AdminClient воркера — `admin` + `ca_pem`
по CLIENT endpoints из etcd (SASL_SSL, 15 §5; INTERNAL advertised — docker-DNS,
недостижим из процесса воркера вне сети `kfw-net-<C>`). reassign-CLI — `admin`
по INTERNAL (docker exec в контейнере брокера, §2.4). Пробы панели — `admin` +
`ca_pem` по CLIENT. Приложения — `app` + `ca_pem` по CLIENT (15 §5).
```

2. §5 M. SecurityMigrator, фаза M3:

OLD: `M3 ждать готовности (K4-паттерн: DescribeCluster c admin-кредом по`
     `   INTERNAL SASL_SSL, бюджет BrokerBootSec) → state=RUNNING у всех`

NEW: `M3 ждать готовности (K4-паттерн: DescribeCluster с admin-кредом по`
     `   CLIENT endpoints из etcd — SASL_SSL, бюджет BrokerBootSec) → state=RUNNING у всех`

Коммитятся вместе с кодом задачи (arch+код одним коммитом шага 5). Формулировки spec §5.2 уже синхронизированы при ревью плана.

- [ ] **Step 1: Failing-тест ReassignCli**

Обновить `ReassignCliTests.cs`:

```csharp
[Fact]
public void BuildAdminProperties_SaslSslWithPemTruststore()
{
    // Arrange: admin-креда + CA-PEM.
    // Act: properties для --command-config (arch/16 §2.4).
    var props = ReassignCli.BuildAdminProperties("admin", "AdminSecret0123456789AAAAAAA", "CAPEM");

    // Assert: SASL_SSL + JAAS admin + PEM-truststore файлом.
    props.Should().Be(
        "security.protocol=SASL_SSL\n"
        + "sasl.mechanism=PLAIN\n"
        + """sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required """
        + $"""username="admin" password="AdminSecret0123456789AAAAAAA";""" + "\n"
        + "ssl.truststore.type=PEM\n"
        + "ssl.truststore.location=/tmp/kfw-ca.pem");
}

[Fact]
public void BuildExecCommand_WritesCaPemFileThenProperties()
{
    // Arrange: CA-PEM из трёх строк (BEGIN/body/END — без апострофов и \).
    var caPem = "-----BEGIN CERTIFICATE-----\nZmFrZQ==\n-----END CERTIFICATE-----";

    // Act: sh -c команда.
    var cmd = ReassignCli.BuildExecCommand([new ReassignMove("t", 0, [1])], "broker1:9092", "admin", "p", caPem);

    // Assert: первым — файл CA (printf '%s\n' по строкам), затем properties, затем CLI.
    cmd.Should().HaveCount(3);
    cmd[2].Should().StartWith(
        "printf '%s\\n%s\\n%s\\n' '-----BEGIN CERTIFICATE-----' 'ZmFrZQ==' '-----END CERTIFICATE-----' > /tmp/kfw-ca.pem && ");
    cmd[2].Should().Contain("> /tmp/kfw-cmd.properties &&");
    cmd[2].Should().Contain("KAFKA_HEAP_OPTS=-Xmx256m /opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server broker1:9092");
}
```

- [ ] **Step 2: Прогнать — FAIL**

Run: `dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter FullyQualifiedName~ReassignCliTests`
Expected: FAIL — сигнатуры.

- [ ] **Step 3: Реализовать ReassignCli + все переходы процессов**

1. `ReassignCli.cs`:

```csharp
private const string CaPath = "/tmp/kfw-ca.pem";

/// <summary>SASL_SSL/PLAIN properties для --command-config (креды admin из etcd,
/// доверие per-cluster CA PEM-файлом, arch/16 §2.4).</summary>
public static string BuildAdminProperties(string user, string password, string caPem)
    => "security.protocol=SASL_SSL\n"
        + "sasl.mechanism=PLAIN\n"
        + """sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required """
        + $"""username="{user}" password="{password}";""" + "\n"
        + "ssl.truststore.type=PEM\n"
        + $"ssl.truststore.location={CaPath}";
```

`BuildExecCommand` — первым printf-блоком записать строки `caPem.Split('\n')` в `CaPath` (формат `printf '%s\n' …` по образцу properties), затем properties, затем CLI (без изменений). CA-PEM base64-алфавит — без `'` и `\` — обёртка безопасна (комментарий сохранить).

2. `BrokerEnvBuilder.cs` — новая сигнатура `Build` (см. Interfaces): placeholder-аргументы Task 2 удаляются; кворум/NodeId/AdvertisedClient без изменений; сборка `NodeEnvSpec`:

```csharp
var (certPem, keyPem) = certificates.GetOrCreate(
    snap.Cluster, broker, snap.CaPem!, snap.CaKey!, advertisedClient);
return NodeEnvBuilder.Build(new NodeEnvSpec(
    snap.Cluster, NodeId(broker), broker, advertisedClient,
    decl.Role == "controller", QuorumVoters(snap),
    snap.AppUser ?? "app", appPasswords,
    snap.AdminUser ?? "admin", adminPasswords,
    snap.CaPem!, certPem, keyPem,
    snap.Config, snap.Config.Brokers, "/var/lib/kafka/data"));
```

с guard-строкой до `GetOrCreate` (см. Interfaces; text: `$"env {snap.Cluster}/{broker}: премиграционный кластер (нет CA/admin-ключей) — сначала SecurityMigrator M"`).

3. `ProvisioningProcess.cs`:
   - `EnsureNodesAsync` — параметр `ClusterSecrets secret` (уже из Task 3): per-node серт из `certificates` (новый параметр конструктора `BrokerCertificateCache certificates`), env = новый канон (`appPasswords: [secret.AppPassword]`, `adminPasswords: [secret.AdminPassword]`, CaPem/cert/key); placeholder-аргументы Task 2 в `EnsureNodesAsync` — удалить.
   - `WaitReadyAsync` — `adminFactory.Create(endpoints, secret.AdminUser, secret.AdminPassword, secret.CaPem)`.
   - K5 `converger.ApplyAsync(cluster, endpoints, secret.AdminUser, secret.AdminPassword, secret.CaPem, snap.Config, ct)`.
4. `NodeSupervisor.cs`, `AddBrokerProcess.cs`, `NodeRegenerator.cs`: `BrokerEnvBuilder.Build(snap, …, appPasswords: [snap.AppPassword!], adminPasswords: [snap.AdminPassword!], options, certificates)` (конструкторы процессов + `certificates`); в `NodeSupervisor` прямой вызов `new NodeEnvSpec(...)` заменить на `BrokerEnvBuilder.Build(...)` (placeholder Task 2 удалён); их `adminFactory.Create(...)` → `Create(snap.Endpoints, snap.AdminUser ?? "admin", snap.AdminPassword!, snap.CaPem)`.
5. `TopicSyncProcess.cs` / `PartitionReassignerProcess.cs` / `RemoveBrokerProcess.cs`: все `adminFactory.Create(snap.Endpoints…AppUser/AppPassword)` → admin+caPem (4+1+1 мест по грепу `adminFactory.Create`). В `PartitionReassignerProcess` вызов `ReassignCli.BuildExecCommand/BuildAdminProperties` — добавить аргумент `snap.CaPem!` и admin-креды (grep `ReassignCli.` по файлу — все вызовы).
6. `KafkaClusterProcesses.ActiveAsync` — converge-условие и вызов: `snap.AdminUser is not null && snap.AdminPassword is not null && snap.CaPem is not null` (вместо AppUser/AppPassword); вызов `converger.ApplyAsync(snap.Cluster, snap.Endpoints, snap.AdminUser, snap.AdminPassword, snap.CaPem, snap.Config, ct)`.
7. `Program.cs`: `builder.Services.AddSingleton(new BrokerCertificateCache());` + пробросить `certificates` в конструкторы `ProvisioningProcess/NodeSupervisor/AddBrokerProcess/NodeRegenerator` (`sp.GetRequiredService<BrokerCertificateCache>()`).
8. Юнит-фиксы (`ProvisioningProcessTests`, `NodeSupervisorTests`, `AddBrokerProcessTests`, `NodeRegeneratorTests`, `PartitionReassignerProcessTests`, `TopicSyncProcessTests`, `AppPasswordRotatorTests`): снапшоты-фикстуры получают `AdminUser="admin"/AdminPassword/CaPem/CaKey` (валидный PEM из `ClusterPki.GenerateCa("unit")` — статик-поле фикса), fake-Create фабрики принимает caPem. Где фиксы строят снапшоты вручную — дополнить именованные аргументы. Тесты с ассертами env на placeholder-admin (из Task 2 Step 4) — обновить на реальные креды фикстуры.

- [ ] **Step 4: Прогон юнит-набора воркера — PASS**

Run: `dotnet test src/tests/KafkaWorker.UnitTests -c Debug`
Expected: PASS.

- [ ] **Step 5: Commit (канон + код одним коммитом)**

```bash
git add arch/16-kafkaworker.md src/KafkaWorker.Provisioning src/KafkaWorker.App src/tests/KafkaWorker.UnitTests
git commit -m "feat(kafka): процессы воркера на admin+SASL_SSL по CLIENT endpoints; placeholder'ы Task 2 сняты (канон 16 §2.3 синхронизирован) (t03 Ф3)"
```

---

### Task 9: Интеграционные docker-тесты TLS-кластера: Provisioning_TlsClusterUp + ACL (Ф3)

**Files:**
- Modify: `src/tests/KafkaWorker.IntegrationTests/Kafka/KafkaClusterFixture.cs`
- Modify: `src/tests/KafkaWorker.IntegrationTests/Kafka/ProvisioningTests.cs`
- Create: `src/tests/KafkaWorker.IntegrationTests/Kafka/TlsClusterTests.cs`

**Interfaces:**
- Consumes: Tasks 1–8 (полный provisioning-конвейер нового канона).
- Produces: маркер-кейс мерж-гейта `Provisioning_TlsClusterUp` (критерий §8.3); fixture-хелперы `DiscoveryAdminBuilderAsync(cluster, role)` и `DiscoveryPartsAsync(cluster)` для Tasks 10, 11, 15.

- [ ] **Step 1: Обновить фикстуру**

`KafkaClusterFixture.cs`:
1. `DiscoveryAdminBuilderAsync(string cluster, string role = "app")` — читает `endpoints`, `ca_pem`, креды по роли (`app`/`admin` → ключи `app_user/app_password` или `admin_user/admin_password`):

```csharp
var config = new AdminClientConfig
{
    BootstrapServers = endpoints!.Replace("host.docker.internal", "localhost", StringComparison.Ordinal),
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.Plain,
    SaslUsername = user,
    SaslPassword = password,
};
config.Set("ssl.ca.pem", caPem!);
return new AdminClientBuilder(config);
```

2. `DiscoveryPartsAsync(string cluster)` → `(string Bootstrap, string CaPem, string AppUser, string AppPassword)` из etcd-ключей (bootstrap с localhost-заменой).
3. `public BrokerCertificateCache Certificates { get; } = new();` — для процессов в тестах.
4. `AdminFactory` — без изменений (фабрика Task 6 принимает caPem).

- [ ] **Step 2: Обновить существующий FullLifecycle-тест**

`ProvisioningTests.FullLifecycle_ProvisionDiscoveryDeprovision`:
- Конструктор `ProvisioningProcess` — с `fixture.Certificates`.
- Дискавери-ассерты (Assert 2): `fixture.DiscoveryAdminBuilderAsync(cluster, "admin")` + отдельный ассерт: `app`-дискавери тоже подключается (`role: "app"`).
- Assert 1 дополнить: `admin_password` — 32 симв `[A-Za-z0-9]`; `ca_pem` начинается с `-----BEGIN CERTIFICATE-----`; `ca_key` — `-----BEGIN PRIVATE KEY-----`; `admin_user == "admin"`.

- [ ] **Step 3: Новый маркер-тест**

`TlsClusterTests.cs`:

```csharp
// Маркер-кейс мерж-гейта t03 (spec §8.3): поднятие TLS-кластера канона t03 —
// provisioning -> SASL_SSL endpoints, приложение производит/потребляет через
// ca_pem + app-кред, ACL: app отказ на админ-операции, admin выполняет.
[Collection(KafkaCollection.Name)]
public class TlsClusterTests(KafkaClusterFixture fixture)
{
    [Fact]
    public async Task Provisioning_TlsClusterUp()
    {
        var cluster = fixture.Cluster("tls");
        var ct = TestContext.Current.CancellationToken;

        // Arrange: заявка 1-брокерного кластера + provisioning-цикл (поллинг, как FullLifecycle).
        await fixture.SeedClusterAsync(cluster, brokers: 1);
        var claims = new ClaimStore([fixture.Endpoint], fixture.Gateway, TimeProvider.System);
        await claims.TryClaimClusterAsync(cluster, ct);
        var journal = new WorkJournal(fixture.Gateway, [fixture.Endpoint]);
        var provision = new ProvisioningProcess(/* как в ProvisioningTests, с fixture.Certificates */);
        var deadline = DateTimeOffset.UtcNow.AddSeconds(200);
        while (DateTimeOffset.UtcNow < deadline)
        {
            var snap = await fixture.SnapshotAsync(cluster);
            if (snap!.Config.State is null) break;
            (await provision.RunAsync(snap, ct)).IsSuccess.Should().BeTrue();
            await Task.Delay(3000, ct);
        }

        // Act 1: приложение (app-кред + ca_pem из etcd) производит и потребляет.
        var (bootstrap, caPem, appUser, appPassword) = await fixture.DiscoveryPartsAsync(cluster);
        var topic = $"orders-{fixture.RunTag}";
        using (var admin = fixture.DiscoveryAdminBuilderAsync(cluster, "admin").GetAwaiter().GetResult().Build())
            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }],
                new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });
        var producerConfig = new ProducerConfig { BootstrapServers = bootstrap, SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain, SaslUsername = appUser, SaslPassword = appPassword };
        producerConfig.Set("ssl.ca.pem", caPem);
        using (var producer = new ProducerBuilder<Null, string>(producerConfig).Build())
            (await producer.ProduceAsync(topic, new Message<Null, string> { Value = "hello-tls" }, ct)).Status
                .Should().Be(PersistenceStatus.Persisted);
        var consumerConfig = new ConsumerConfig { BootstrapServers = bootstrap, SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.Plain, SaslUsername = appUser, SaslPassword = appPassword,
            GroupId = $"g-{fixture.RunTag}", AutoOffsetReset = AutoOffsetReset.Earliest };
        consumerConfig.Set("ssl.ca.pem", caPem);
        using (var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
        {
            consumer.Subscribe(topic);
            var consumed = consumer.Consume(TimeSpan.FromSeconds(30));
            consumed.Message.Value.Should().Be("hello-tls");
        }

        // Act 2 / Assert 2: ACL deny-by-default — app-креду отказ в админ-операциях.
        using (var appAdmin = fixture.DiscoveryAdminBuilderAsync(cluster, "app").GetAwaiter().GetResult().Build())
        {
            var act = () => appAdmin.CreateTopicsAsync(
                [new TopicSpecification { Name = "forbidden-" + fixture.RunTag, NumPartitions = 1, ReplicationFactor = 1 }],
                new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(15) });
            await act.Should().ThrowAsync<CreateTopicsException>(
                "принципал User:app не имеет Create-ACL (16 §2.3), deny-by-default");
        }

        // Assert 3: admin-кред — super.user (CreateTopics прошёл выше); финальная
        // DescribeCluster admin-кредом успешна.
        using (var admin = fixture.DiscoveryAdminBuilderAsync(cluster, "admin").GetAwaiter().GetResult().Build())
            admin.GetMetadata(TimeSpan.FromSeconds(15)).Brokers.Should().HaveCount(1);

        await claims.DisposeAsync();
    }
}
```

- [ ] **Step 4: Прогнать docker-интеграционные**

Run: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/KafkaWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~ProvisioningTests|FullyQualifiedName~TlsClusterTests"`
Expected: PASS (брокер поднимается на SASL_SSL; при падении — диагностика по логам контейнера `docker logs kfw-<C>-broker1`).

- [ ] **Step 5: Commit**

```bash
git add src/tests/KafkaWorker.IntegrationTests
git commit -m "test(kafka): Provisioning_TlsClusterUp — TLS-кластер, produser/consumer, ACL (t03 Ф3)"
```

---

### Task 10: PasswordRotator — роли app|admin (Ф4)

**Files:**
- Create: `src/KafkaWorker.Provisioning/Processes/PasswordRotator.cs` (перенос+обобщение)
- Delete: `src/KafkaWorker.Provisioning/Processes/AppPasswordRotator.cs`
- Modify: `src/KafkaWorker.App/Loops/KafkaClusterProcesses.cs`, `src/KafkaWorker.App/Program.cs`
- Test: rename `src/tests/KafkaWorker.UnitTests/Provisioning/AppPasswordRotatorTests.cs` → `PasswordRotatorTests.cs`

**Interfaces:**
- Consumes: `BrokerEnvBuilder.Build(…, appPasswords, adminPasswords, …)` (Task 8), снапшот-поля admin (Task 4), `IKafkaAdminClientFactory.Create(…, caPem)`.
- Produces (использует Task 11-панель через etcd-заявки): `PasswordRotator.RunAsync(KafkaClusterSnapshot snap, CancellationToken ct)` — обрабатывает не более ОДНОЙ заявки за тик: сначала `app` (`/kafkaworker/rotations/<C>` → `app_password` → `user_app`), при отсутствии — `admin` (`/kafkaworker/admin_rotations/<C>` → `admin_password` → `user_admin`); фазы A/B/C механически прежние; окно ротации — ТОЛЬКО ротируемой роли, вторая роль несёт текущий пароль.

- [ ] **Step 1: Тесты (порт + admin-кейсы)**

`PasswordRotatorTests.cs`: все существующие кейсы app-ротации сохранить (замена типов/хелперов при переименовании) и добавить:

```csharp
[Fact]
public async Task RunAsync_AdminTicket_RotatesAdminPasswordKeepsApp()
{
    // Arrange: заявка /kafkaworker/admin_rotations/<C>; снапшот с AdminPassword/CaPem.
    await PutTicket("/kafkaworker/admin_rotations/events", """{"requested_unix":1756500900,"requested_by":"test"}""");
    var snap = Snapshot(endpoints: "localhost:1", appPassword: "AppOld0123456789AAAAAAAAAAAAAAAA",
        adminPassword: "AdmOld0123456789AAAAAAAAAAAAAAAA");

    // Act: тик ротации.
    var result = await rotator.RunAsync(snap, ct);

    // Assert: admin_password заменён (фаза B), app_password не тронут;
    // env пересозданий несёт user_admin+user_admin2 и одиночный user_app.
    result.IsSuccess.Should().BeTrue();
    (await GetAsync("/kafka/clusters/events/admin_password")).Should().NotBe("AdmOld0123456789AAAAAAAAAAAAAAAA");
    (await GetAsync("/kafka/clusters/events/app_password")).Should().Be("AppOld0123456789AAAAAAAAAAAAAAAA");
    envDriver.LastCreatedEnv!["KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG"]
        .Should().Contain(@"user_admin=""AdmOld").And.Contain(@"user_admin2=""")
        .And.Contain(@"user_app=""AppOld").And().NotContain(@"user_app2=");
}

[Fact]
public async Task RunAsync_AppTicketFirst_AdminTicketWaitsNextTick()
{
    // Arrange: живы ОБЕ заявки.
    await PutTicket("/kafkaworker/rotations/events", """{"requested_unix":1,"requested_by":"t"}""");
    await PutTicket("/kafkaworker/admin_rotations/events", """{"requested_unix":2,"requested_by":"t"}""");

    // Act: один тик.
    await rotator.RunAsync(snap, ct);

    // Assert: исполнена ТОЛЬКО app (det-порядок spec §5.2), admin-заявка жива.
    (await GetAsync("/kafkaworker/admin_rotations/events")).Should().NotBeNull();
    (await GetAsync("/kafkaworker/rotations/events")).Should().BeNull();
}
```

- [ ] **Step 2: Прогнать — FAIL**

Run: `dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter FullyQualifiedName~PasswordRotatorTests`
Expected: FAIL.

- [ ] **Step 3: Реализовать**

`PasswordRotator.cs` — перенос `AppPasswordRotator` с изменениями:
1. Приватная запись ролей:

```csharp
// Роли ротации (spec §5.2): заявка/пароль/JAAS-пользователь на роль; app —
// раньше admin (детерминированный порядок, одна заявка за тик).
private sealed record RotationRole(string Name, string TicketKeyPrefix, string PasswordKey, string User)
{
    public static readonly RotationRole App = new("app", "rotations", "app_password", "app");
    public static readonly RotationRole Admin = new("admin", "admin_rotations", "admin_password", "admin");
    public static readonly IReadOnlyList<RotationRole> Order = [App, Admin];
}
```

2. `RunAsync`: цикл по `RotationRole.Order`; для роли — весь существующий алгоритм A/B/C (переименования: `RotationKey(cluster)` → `$/kafkaworker/{role.TicketKeyPrefix}/{cluster}`, `PasswordKey` → `$/kafka/clusters/{cluster}/{role.PasswordKey}`); внутри тика — если заявка роли была и её цикл завершился — `break` (вторая заявка — следующим тиком); if-возвраты сохранить (waiting-кластер и т.д.).
3. `RollingRecreateAsync` — параметры `appPasswords/adminPasswords`; для ротируемой роли — `[old, new]`, для второй — `[current из снапшота]`; вызов `BrokerEnvBuilder.Build(snap, broker, addr, appPasswords, adminPasswords, options, certificates)`; конструктор получает `BrokerCertificateCache certificates`.
4. `WaitForBrokersAsync` — `adminFactory.Create(snap.Endpoints!, snap.AdminUser ?? "admin", snap.AdminPassword!, snap.CaPem)`.
5. `git rm AppPasswordRotator.cs`; `KafkaClusterProcesses`: поле `AppPasswordRotator rotator` → `PasswordRotator rotator`; `Program.cs` — DI-замена (добавить `sp.GetRequiredService<BrokerCertificateCache>()`).

- [ ] **Step 4: Прогон юнит — PASS**

Run: `dotnet test src/tests/KafkaWorker.UnitTests -c Debug`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A src/KafkaWorker.Provisioning src/KafkaWorker.App src/tests/KafkaWorker.UnitTests
git commit -m "feat(kafka): PasswordRotator app|admin — одна заявка за тик (t03 Ф4)"
```

---

### Task 11: Мутация №16 — хендлер воркера, команда/UI/алерт панели, интеграционная ротация admin (Ф4)

**Files:**
- Create: `src/KafkaWorker.App/Api/Operations/RotateAdminPasswordHandler.cs`
- Modify: `src/KafkaWorker.App/Api/ApiModule.cs`
- Modify: `src/AdminPanel.Api/Operations/Kafka/KafkaCommands.cs` (+регистрация в `KafkaOperationsModule.cs`, если проксируются атрибутами — по месту)
- Create: `frontend/src/pages/kafka-cluster/RotateAdminPasswordButton.tsx` (+использование в деталях кластера рядом с `RotatePasswordButton`)
- Modify: `frontend/src/api/queries.ts` (+`rotateKafkaAdminPassword`), `frontend/src/api/dto.ts` (при типизации)
- Test: `src/tests/KafkaWorker.IntegrationTests/Api/ClusterMutationsApiTests.cs` (новый кейс), `src/tests/KafkaWorker.IntegrationTests/Kafka/AdminRotationTests.cs` (new), панельные юнит — по месту

**Interfaces:**
- Consumes: `PasswordRotator` (Task 10), панельный `IWorkerApiGateway` + `AdminRotations` (Task 5).
- Produces: endpoint `POST /api/kafka/clusters/{cluster}/admin-password/rotate` (воркер) — 201/404/409/503 идентичны app-ротации; панель `RotateKafkaAdminPasswordCommand`; UI-кнопка; warning `kafka-admin-rotation-pending` уже из Task 5.

- [ ] **Step 1: Failing API-тест (воркер)**

В `ClusterMutationsApiTests.cs` по образцу существующего кейса app-ротации:

```csharp
[Fact]
public async Task RotateAdminPassword_ClaimsTicket_409OnRepeat()
{
    // Arrange: Active-кластер events (сид-хелпер класса).
    // Act 1: POST admin-password/rotate.
    var first = await client.PostAsync("/api/kafka/clusters/events/admin-password/rotate", null);
    // Assert 1: 201 + DTO.
    first.StatusCode.Should().Be(HttpStatusCode.Created);
    // Act 2: повтор.
    var second = await client.PostAsync("/api/kafka/clusters/events/admin-password/rotate", null);
    // Assert 2: 409 «уже запрошена» (клэйм-txn version==0).
    second.StatusCode.Should().Be(HttpStatusCode.Conflict);
    // Cleanup: del заявки (общая чистка класса).
}
```

- [ ] **Step 2: Прогнать — FAIL**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.IntegrationTests -c Debug --filter FullyQualifiedName~RotateAdminPassword_ClaimsTicket`
Expected: FAIL — 404 (нет endpoint).

- [ ] **Step 3: Реализовать хендлер + endpoint**

`RotateAdminPasswordHandler.cs` — точный порт `RotateAppPasswordHandler` (замены: ключ `$"/kafkaworker/admin_rotations/{cluster}"`, DTO `KafkaAdminPasswordRotatedDto(Cluster, RequestedUnix, RequestedBy)`, doc: «мутация №16, adminpanel/02 §10.2; исполнение — PasswordRotator роли admin»). `ApiModule.cs` — endpoint по образцу app-ротации (тот же switch ошибок). `Program.cs` — DI-регистрация хендлера по образцу.

Панель `KafkaCommands.cs` — порт `RotateKafkaPasswordCommand`:

```csharp
public sealed record RotateKafkaAdminPasswordCommand(string Cluster, string RequestedBy)
    : ICommand<KafkaAdminPasswordRotatedDto>;

public sealed record KafkaAdminPasswordRotatedDto(string Cluster, long RequestedUnix, string RequestedBy);

[InjectAsScoped]
public sealed class RotateKafkaAdminPasswordCommandHandler(IWorkerApiGateway api)
    : ICommandHandler<RotateKafkaAdminPasswordCommand, KafkaAdminPasswordRotatedDto>
{
    public async ValueTask<Result<KafkaAdminPasswordRotatedDto>> Handle(
        RotateKafkaAdminPasswordCommand command, CancellationToken ct)
        => await WorkerProxy.SendAsync<KafkaAdminPasswordRotatedDto>(
            api, "kafkaworker", HttpMethod.Post,
            $"/api/kafka/clusters/{command.Cluster}/admin-password/rotate",
            body: null, command.RequestedBy, ct);
}
```

Регистрация — по образцу соседних kafka-команд в этом же файле/модуле. Фронтенд: `RotateAdminPasswordButton.tsx` — копия `RotatePasswordButton.tsx` (тексты: «Сменить admin-пароль», предупреждение о rolling-рестартах брокеров — фазы A/B/C, клиенты-приложения не затрагиваются), `queries.ts` — `rotateKafkaAdminPassword(cluster)` → POST `kafka/clusters/${cluster}/admin-password/rotate`; кнопка рядом с app-ротацией в деталях кластера (файл-страница kafka-кластера — рядом с использованием `RotatePasswordButton`).

- [ ] **Step 4: Интеграционный docker-тест ротации admin**

`src/tests/KafkaWorker.IntegrationTests/Kafka/AdminRotationTests.cs`: поднять 1-брокерный TLS-кластер (как Task 9), поставить заявку `admin_rotations`, крутить тики `PasswordRotator` до завершения (поллинг ≤ 200 с; тик-цикл как в `ProvisioningTests`), затем:
- Assert: `admin_password` изменился; заявка удалена;
- Assert (окно A/B/C — app работает непрерывно): ПОСЛЕ КАЖДОГО тика ротации выполнять `Produce+Consume` app-кредом по ca_pem (непрерывность покрыта точками на всём окне фаз A→C: между тиками A, между A и B, между B и C, после финала); ни одна точка не упала;
- Assert: admin-дискавери с НОВЫм паролем `DescribeCluster` успешен, со старым — падает (SaslAuthenticationException).

- [ ] **Step 5: Прогоны — PASS**

Run: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/KafkaWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~RotateAdminPassword_ClaimsTicket|FullyQualifiedName~AdminRotationTests"` и `dotnet test src/tests/AdminPanel.UnitTests -c Debug`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A src/KafkaWorker.App src/AdminPanel.Api frontend/src src/tests
git commit -m "feat(kafka): мутация №16 ротация admin-пароля — API/панель/UI + интеграционный тест (t03 Ф4)"
```

---

### Task 12: mTLS HTTP API воркера — Options/Kestrel, env-биндинг, удаление ApiKeyMiddleware (Ф5)

**Files:**
- Modify: `src/KafkaWorker.App/Options.cs`
- Create: `src/KafkaWorker.App/Api/TlsEndpoints.cs`
- Delete: `src/KafkaWorker.App/Api/ApiKeyMiddleware.cs`
- Modify: `src/KafkaWorker.App/Program.cs` (Kestrel TLS, fail-fast, шапка-комментарий секретов), `src/KafkaWorker.App/appsettings.json` (комментарии секции Api)
- Test: `src/tests/KafkaWorker.UnitTests/App/TlsEnvBindingsTests.cs` (new), `src/tests/KafkaWorker.IntegrationTests/Api/KafkaApiFactory.cs`, Create: `src/tests/KafkaWorker.IntegrationTests/Api/MtlsApiTests.cs`

**Interfaces:**
- Consumes: `ClusterPki` (тестовые серты); `ConfigurationManager` (env-перенос).
- Produces (используют Tasks 13, 14): 
  - `ApiOptions { AdvertiseUrl, EnableSeedEndpoint, Tls }`, `TlsOptions { string? ServerCertPem, string? ServerCertPath, string? ServerKeyPem, string? ServerKeyPath, string? ClientCaPem, string? ClientCaPath, bool AllowInsecureHttp = false }`.
  - `static readonly (string Env, string Key)[] TlsEndpoints.EnvBindings` — таблица `KFW_API_TLS_{CERT,KEY,CLIENT_CA}[_PATH]` → `KafkaWorker:Api:Tls:{ServerCertPem|ServerKeyPem|ClientCaPem|…Path}` (единственная константа для Program и теста).
  - `static void TlsEndpoints.ApplyEnvOverrides(ConfigurationManager configuration, Func<string, string?>? getenv = null)` — перенос env → конфиг (default `Environment.GetEnvironmentVariable`; inject для юнит-теста).
  - `static void TlsEndpoints.ConfigureMtls(WebApplicationBuilder builder, int port)` — вызывается ДО `builder.Build()`; `AllowInsecureHttp=true` → без TLS (warning логирует Program.cs после Build); иначе Kestrel `ListenAnyIP(port)` + `UseHttps(HttpsConnectionAdapterOptions { ServerCertificate, ClientCertificateMode.RequireCertificate, ClientCertificateValidation })` — валидация цепочки против ClientCA; сертификаты живут всё приложение (НЕ using — колбэк вызывается на каждом хендшейке). Fail-fast: серт/ключ/ClientCA не заданы → `ApplicationException` при конфигурации хоста.

- [ ] **Step 1: Failing-тест env-биндинга (юнит)**

`src/tests/KafkaWorker.UnitTests/App/TlsEnvBindingsTests.cs`:

```csharp
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using KafkaWorker.App.Api;

namespace KafkaWorker.UnitTests.App;

// Options-биндинг TLS (spec §8.1): KFW_API_TLS_* → конфиг-дерево; таблица
// EnvBindings — единственный источник соответствий (Program + тест).
public class TlsEnvBindingsTests
{
    [Fact]
    public void ApplyEnvOverrides_PemAndPathKeysMapped()
    {
        // Arrange: env-словарь без реального окружения (inject в чистую функцию).
        var env = new Dictionary<string, string>
        {
            ["KFW_API_TLS_CERT"] = "PEM-CERT",
            ["KFW_API_TLS_CLIENT_CA_PATH"] = "/tls/ca.pem",
        };
        var config = new ConfigurationManager();

        // Act: перенос.
        TlsEndpoints.ApplyEnvOverrides(config, key => env.GetValueOrDefault(key));

        // Assert: каждое имя env легло в свой конфиг-ключ; незаданные — нет.
        config["KafkaWorker:Api:Tls:ServerCertPem"].Should().Be("PEM-CERT");
        config["KafkaWorker:Api:Tls:ClientCaPath"].Should().Be("/tls/ca.pem");
        config["KafkaWorker:Api:Tls:ServerKeyPem"].Should().BeNull();

        // Таблица покрывает все 6 секретов (spec §5.3 / arch/16 §8).
        TlsEndpoints.EnvBindings.Should().HaveCount(6);
    }
}
```

- [ ] **Step 2: Failing-тест на реальном Kestrel-сокете**

`src/tests/KafkaWorker.IntegrationTests/Api/MtlsApiTests.cs`:

```csharp
// mTLS HTTP API (spec §8.2): клиент без серта — отказ TLS-хендшейка, с сертом
// API-CA — 200; /healthz за тем же TLS. Реальный Kestrel-сокет (WAF-транспорт
// in-memory TLS не исполняет) — порт динамический (зонд FreePortWindow).
// Клиентский серт возвращается НАРУЖУ (без using в хелпере — серт живёт в
// SslOptions.ClientCertificates хендшейков после возврата; диспоз — тест).
public class MtlsApiTests
{
    private static readonly (string CaPem, string CaKeyPem) ApiCa = ClusterPki.GenerateCa("api-test");

    private sealed record TlsHost(WebApplication App, HttpClient Client, X509Certificate2 ClientCert);

    private static TlsHost StartTlsHost(int port)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddHealthChecks(); // маршрут /healthz в тесте
        var (serverCertPem, serverKeyPem) = ClusterPki.IssueBrokerCertificate(
            ApiCa.CaPem, ApiCa.CaKeyPem, "kafkaworker", ["localhost"], ip: null);
        builder.Configuration["KafkaWorker:Api:Tls:ServerCertPem"] = serverCertPem;
        builder.Configuration["KafkaWorker:Api:Tls:ServerKeyPem"] = serverKeyPem;
        builder.Configuration["KafkaWorker:Api:Tls:ClientCaPem"] = ApiCa.CaPem;
        TlsEndpoints.ConfigureMtls(builder, port); // ДО Build — ConfigureKestrel этап хоста
        var app = builder.Build();
        app.MapGet("/api/ping", () => Results.Ok("pong"));
        app.MapHealthChecks("/healthz");
        app.Start();

        var (clientCertPem, clientKeyPem) = ClusterPki.IssueBrokerCertificate(
            ApiCa.CaPem, ApiCa.CaKeyPem, "panel", ["panel"], ip: null);
        var clientCert = X509Certificate2.CreateFromPem(clientCertPem, clientKeyPem);
        var handler = new SocketsHttpHandler
        {
            SslOptions = new()
            {
                ClientCertificates = [clientCert],
                RemoteCertificateValidationCallback = (_, _, _, _) => true, // тест доверяет всё
            },
        };
        return new TlsHost(app, new HttpClient(handler) { BaseAddress = new Uri($"https://localhost:{port}") }, clientCert);
    }

    [Fact]
    public async Task Mtls_NoClientCert_Refused_WithCert_Ok()
    {
        // Arrange: TLS-хост на свободном порту (зонд FreePortWindow — динамический).
        var port = FreePortWindow.Find().From;
        var host = StartTlsHost(port);
        using var _ = host.ClientCert; // серт нужен хендшейкам до конца теста
        using var app = host.App;
        using var badClient = new HttpClient(new SocketsHttpHandler
        {
            SslOptions = new() { RemoteCertificateValidationCallback = (_, _, _, _) => true },
        }) { BaseAddress = new Uri($"https://localhost:{port}") };

        // Act 1: запрос без клиентского серта.
        var refused = await Assert.ThrowsAnyAsync<HttpRequestException>(
            () => badClient.GetAsync("/api/ping"));

        // Assert 1: TLS-отказ (хендшейк не прошёл — ClientCertificateMode.Required).
        refused.Should().NotBeNull();

        // Act 2 / Assert 2: с сертом API-CA — 200; /healthz — тоже за TLS.
        (await host.Client.GetAsync("/api/ping")).StatusCode.Should().Be(HttpStatusCode.OK);
        (await host.Client.GetAsync("/healthz")).StatusCode.Should().Be(HttpStatusCode.OK);
    }
}
```

- [ ] **Step 3: Прогнать — FAIL**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test -c Debug src/tests/KafkaWorker.UnitTests --filter FullyQualifiedName~TlsEnvBindingsTests` и `DOTNET_CLI_UI_LANGUAGE=en dotnet test -c Debug src/tests/KafkaWorker.IntegrationTests --filter FullyQualifiedName~MtlsApiTests`
Expected: FAIL — нет `TlsEndpoints`.

- [ ] **Step 4: Реализовать**

1. `Options.cs`: удалить `ApiKey`; добавить `public TlsOptions Tls { get; set; } = new();` + класс `TlsOptions` (поля Interfaces; doc: «arch/16 §1.1: mTLS-only HTTP API; AllowInsecureHttp — только WAF-тесты»). Заголовочный комментарий файла: «Env-секретов per-install — только TLS API (arch/16 §4)».
2. `TlsEndpoints.cs`:

```csharp
using System.Security.Cryptography.X509Certificates;
using KafkaWorker.App;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Server.Kestrel.Https;

namespace KafkaWorker.App.Api;

// mTLS HTTP-грани воркера (arch/16 §1.1, t03): вся грань (вкл. /healthz) —
// только TLS; клиентские серты — per-install API-CA (ClientCaPem|ClientCaPath).
// Вызывается на WebApplicationBuilder ДО Build() (ConfigureKestrel — этап
// хоста) — общий код Program.cs и MtlsApiTests. Сертификаты живут всё
// приложение: ClientCertificateValidation вызывается на КАЖДОМ хендшейке
// (никаких using — иначе use-after-dispose).
public static class TlsEndpoints
{
    // env-секреты → конфиг-дерево (arch/16 §8): PEM-значения и _PATH-файлы.
    public static readonly (string Env, string Key)[] EnvBindings =
    [
        ("KFW_API_TLS_CERT", "KafkaWorker:Api:Tls:ServerCertPem"),
        ("KFW_API_TLS_KEY", "KafkaWorker:Api:Tls:ServerKeyPem"),
        ("KFW_API_TLS_CLIENT_CA", "KafkaWorker:Api:Tls:ClientCaPem"),
        ("KFW_API_TLS_CERT_PATH", "KafkaWorker:Api:Tls:ServerCertPath"),
        ("KFW_API_TLS_KEY_PATH", "KafkaWorker:Api:Tls:ServerKeyPath"),
        ("KFW_API_TLS_CLIENT_CA_PATH", "KafkaWorker:Api:Tls:ClientCaPath"),
    ];

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

    public static void ConfigureMtls(WebApplicationBuilder builder, int port)
    {
        var tls = builder.Configuration.GetSection("KafkaWorker:Api:Tls").Get<TlsOptions>() ?? new TlsOptions();
        if (tls.AllowInsecureHttp)
            return; // без TLS — только WAF-тесты; warning логирует Program.cs

        // Fail-fast при конфигурации хоста: серт/ключ/ClientCA обязаны быть заданы.
        var serverCert = LoadServerCertificate(tls) ?? throw new ApplicationException(
            "KafkaWorker:Api:Tls: серверный серт/ключ не заданы (KFW_API_TLS_CERT/KEY или *_PATH; arch/16 §1.1)");
        var clientCa = LoadClientCa(tls) ?? throw new ApplicationException(
            "KafkaWorker:Api:Tls: ClientCA не задан (KFW_API_TLS_CLIENT_CA[_PATH])");

        // Явный Listen подавляет default-URL (ASPNETCORE_HTTP_PORTS) — только mTLS.
        builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port, listenOptions => listenOptions.UseHttps(
            new HttpsConnectionAdapterOptions
            {
                ServerCertificate = serverCert,
                ClientCertificateMode = ClientCertificateMode.RequireCertificate,
                ClientCertificateValidation = (certificate, _, _) => ValidateChain(certificate, clientCa),
            })));
    }

    // Валидация цепочки клиентского серта против per-install API-CA.
    private static bool ValidateChain(X509Certificate2? certificate, X509Certificate2 clientCa)
    {
        if (certificate is null)
            return false;
        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(clientCa);
        return chain.Build(certificate);
    }

    private static X509Certificate2? LoadServerCertificate(TlsOptions tls)
    {
        var certPem = tls.ServerCertPem ?? ReadFile(tls.ServerCertPath);
        var keyPem = tls.ServerKeyPem ?? ReadFile(tls.ServerKeyPath);
        return certPem is null || keyPem is null
            ? null
            : X509Certificate2.CreateFromPem(certPem, keyPem);
    }

    private static X509Certificate2? LoadClientCa(TlsOptions tls)
    {
        var caPem = tls.ClientCaPem ?? ReadFile(tls.ClientCaPath);
        return caPem is null ? null : X509Certificate2.CreateFromPem(caPem);
    }

    private static string? ReadFile(string? path)
        => path is null || !File.Exists(path) ? null : File.ReadAllText(path).Trim();
}
```

3. `Program.cs`:
   - Шапка-комментарий (строки 18–21, spec §5.3): фразу «Env-секретов per-install НЕТ (единственный секрет — per-cluster app_password в etcd).» заменить на «Per-install env-секреты — только TLS HTTP API (arch/16 §4); per-cluster секреты (app/admin/CA) — в etcd.»
   - До `builder.Build()`: `TlsEndpoints.ApplyEnvOverrides(builder.Configuration);` и `TlsEndpoints.ConfigureMtls(builder, port: 8080);`
   - После `var app = builder.Build();`: warning-лог для insecure-режима:

```csharp
if (app.Services.GetRequiredService<IOptions<KafkaWorkerOptions>>().Value.Api.Tls.AllowInsecureHttp)
    app.Logger.LogWarning(
        "KafkaWorker:Api:Tls:AllowInsecureHttp=true — HTTP без TLS (ТОЛЬКО WAF-тесты, arch/16 §1.1)");
```

   - Убрать `app.UseMiddleware<ApiKeyMiddleware>();` и `git rm src/KafkaWorker.App/Api/ApiKeyMiddleware.cs`.
   - Валидации `ValidateOnStart` дополнить: `!o.Api.Tls.AllowInsecureHttp` → `o.Api.AdvertiseUrl.StartsWith("https://")` («AdvertiseUrl обязан быть https:// (arch/16 §1.1)»).
4. `KafkaApiFactory.cs`: в InMemory-конфиг добавить `["KafkaWorker:Api:Tls:AllowInsecureHttp"] = "true"` (существующие WAF-тесты работают по http) + комментарий «mTLS — MtlsApiTests на реальном сокете»; doc-комментарий фабрики обновить.

- [ ] **Step 5: Прогнать API-набор + mTLS + env-биндинг — PASS**

Run: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/KafkaWorker.IntegrationTests -c Debug --filter "FullyQualifiedName~Api"` и `dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter FullyQualifiedName~TlsEnvBindingsTests`
Expected: PASS (все прежние WAF-кейсы на AllowInsecureHttp + новые mTLS/env-биндинг).

- [ ] **Step 6: Commit**

```bash
git add -A src/KafkaWorker.App src/tests/KafkaWorker.UnitTests src/tests/KafkaWorker.IntegrationTests
git commit -m "feat(kafka): mTLS-only HTTP API воркера, env-биндинг KFW_API_TLS_*, KFW_API_KEY удалён (t03 Ф5, arch/16 §1.1)"
```

---

### Task 13: Панель — WorkerApiGateway по mTLS, env-биндинг KFW_PANEL_TLS_* (Ф5)

**Files:**
- Modify: `src/AdminPanel.Etcd/Workers/WorkerApiOptions.cs`
- Modify: `src/AdminPanel.Etcd/Workers/WorkerApiGateway.cs`
- Create: `src/AdminPanel.Etcd/Workers/WorkerTlsHandler.cs`
- Modify: `src/AdminPanel.Etcd/ModuleExtensions.cs`
- Modify: панельный host (`src/AdminPanel.App`/Program — точка конфигурации по grep `ADMINPANEL__`/`AddEtcd`)
- Test: `src/tests/AdminPanel.UnitTests/` (тест gateway/handler + env-маппинг), `src/tests/AdminPanel.IntegrationTests/` (если есть HTTP-кейсы gateway)

**Interfaces:**
- Consumes: `WorkerApiGateway.HttpClientName = "workers"`; env `KFW_PANEL_TLS_*`.
- Produces: `KafkaTlsOptions { string? ClientCertPem|ClientCertPath, string? ClientKeyPem|ClientKeyPath, string? ServerCaPem|ServerCaPath }`; `static readonly (string Env, string Key)[] WorkerTlsHandler.EnvBindings` — таблица `KFW_PANEL_TLS_{CERT,KEY,SERVER_CA}[_PATH]` → `AdminPanel:Workers:KafkaTls:{ClientCertPem|ClientKeyPem|ServerCaPem|…Path}`; `static void WorkerTlsHandler.ApplyEnvOverrides(ConfigurationManager configuration, Func<string, string?>? getenv = null)`; `static HttpMessageHandler WorkerTlsHandler.Build(KafkaTlsOptions tls)` — SocketsHttpHandler с клиентским сертом (ClientCertificates) и доверием ServerCA (RemoteCertificateValidationCallback, X509Chain CustomRootTrust — по образцу Task 12; серты без using — время жизни handler'а); для `http://`-запросов (PgWorker) TLS-опции не применяются — один HttpClient на оба воркера, `X-Api-Key` pg остаётся.

- [ ] **Step 1: Failing-тесты (handler + env-маппинг)**

```csharp
[Fact]
public void Build_ClientCertAndServerCa_SocketsHandlerTlsOptions()
{
    // Arrange: тестовые PEM (сгенерировать CertificateRequest-ом прямо в тесте,
    // AdminPanel.UnitTests не ссылается на KafkaWorker.Core) + опции.
    var (caPem, caKeyPem) = TestPki.GenerateCa(); // локальный хелпер-обёртка CertificateRequest
    var (certPem, keyPem) = TestPki.Issue(caPem, caKeyPem, "panel");
    var tls = new KafkaTlsOptions { ClientCertPem = certPem, ClientKeyPem = keyPem, ServerCaPem = caPem };

    // Act: сборка handler'а.
    var handler = WorkerTlsHandler.Build(tls) as SocketsHttpHandler;

    // Assert: клиентский серт подан, колбэк доверия установлен.
    handler.Should().NotBeNull();
    handler!.SslOptions.ClientCertificates.Should().NotBeEmpty();
    handler.SslOptions.RemoteCertificateValidationCallback.Should().NotBeNull();
}

[Fact]
public void Build_NoTls_PlainSocketsHandler()
{
    // Arrange: пустые опции (pg-only конфигурация).
    // Act / Assert: handler без TLS-настроек (http к pgworker работает).
    (WorkerTlsHandler.Build(new KafkaTlsOptions()) as SocketsHttpHandler)!
        .SslOptions.ClientCertificates.Should().BeEmpty();
}

[Fact]
public void ApplyEnvOverrides_PanelTlsKeysMapped()
{
    // Arrange: env-словарь (inject, без окружения).
    var env = new Dictionary<string, string>
    {
        ["KFW_PANEL_TLS_CERT_PATH"] = "/tls/panel.crt",
        ["KFW_PANEL_TLS_SERVER_CA_PATH"] = "/tls/ca.pem",
    };
    var config = new ConfigurationManager();

    // Act.
    WorkerTlsHandler.ApplyEnvOverrides(config, key => env.GetValueOrDefault(key));

    // Assert: ключи легли в AdminPanel:Workers:KafkaTls:*; таблица — 6 записей.
    config["AdminPanel:Workers:KafkaTls:ClientCertPath"].Should().Be("/tls/panel.crt");
    config["AdminPanel:Workers:KafkaTls:ServerCaPath"].Should().Be("/tls/ca.pem");
    WorkerTlsHandler.EnvBindings.Should().HaveCount(6);
}
```

(`TestPki` — маленький локальный хелпер теста на `CertificateRequest`, ~15 строк: GenerateCa + Issue по образцу ClusterPki; дублирование осознанное — панельные тесты не тянут зависимость от KafkaWorker.Core.)

- [ ] **Step 2: Прогнать — FAIL**

Run: `dotnet test src/tests/AdminPanel.UnitTests -c Debug --filter "FullyQualifiedName~WorkerTls"`
Expected: FAIL — нет типа.

- [ ] **Step 3: Реализовать**

1. `WorkerApiOptions.cs`: удалить `KafkaApiKey`; добавить `public KafkaTlsOptions KafkaTls { get; set; } = new();` + класс `KafkaTlsOptions` (поля Interfaces; doc: «arch/02 §2.3.2: mTLS KafkaWorker; pg-ключ не трогается»).
2. `WorkerTlsHandler.cs`:

```csharp
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Configuration;

namespace AdminPanel.Etcd.Workers;

// HTTP-handler обращений в KafkaWorker (arch/02 §2.3.2, t03): клиентский серт
// per-install API-CA + доверие ServerCA (валидация цепочки на каждый хендшейк —
// серты живут время жизни handler'а, БЕЗ using). Для http://-запросов
// (PgWorker) TLS-опции не применяются — один HttpClient на оба воркера.
public static class WorkerTlsHandler
{
    public static readonly (string Env, string Key)[] EnvBindings =
    [
        ("KFW_PANEL_TLS_CERT", "AdminPanel:Workers:KafkaTls:ClientCertPem"),
        ("KFW_PANEL_TLS_KEY", "AdminPanel:Workers:KafkaTls:ClientKeyPem"),
        ("KFW_PANEL_TLS_SERVER_CA", "AdminPanel:Workers:KafkaTls:ServerCaPem"),
        ("KFW_PANEL_TLS_CERT_PATH", "AdminPanel:Workers:KafkaTls:ClientCertPath"),
        ("KFW_PANEL_TLS_KEY_PATH", "AdminPanel:Workers:KafkaTls:ClientKeyPath"),
        ("KFW_PANEL_TLS_SERVER_CA_PATH", "AdminPanel:Workers:KafkaTls:ServerCaPath"),
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

    public static HttpMessageHandler Build(KafkaTlsOptions tls)
    {
        var handler = new SocketsHttpHandler { PooledConnectionLifetime = TimeSpan.FromMinutes(5) };
        var certPem = tls.ClientCertPem ?? ReadFile(tls.ClientCertPath);
        var keyPem = tls.ClientKeyPem ?? ReadFile(tls.ClientKeyPath);
        var serverCaPem = tls.ServerCaPem ?? ReadFile(tls.ServerCaPath);
        if (certPem is not null && keyPem is not null)
        {
            var clientCert = X509Certificate2.CreateFromPem(certPem, keyPem);
            handler.SslOptions.ClientCertificates = new X509CertificateCollection { clientCert };
            if (serverCaPem is not null)
            {
                var ca = X509Certificate2.CreateFromPem(serverCaPem);
                handler.SslOptions.RemoteCertificateValidationCallback =
                    (_, certificate, chain, _) => ValidateChain(certificate, chain, ca);
            }
        }

        return handler;
    }

    private static bool ValidateChain(X509Certificate2? certificate, X509Chain? chain, X509Certificate2 ca)
    {
        if (certificate is null)
            return false;
        using var custom = new X509Chain();
        custom.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        custom.ChainPolicy.CustomTrustStore.Add(ca);
        return custom.Build(certificate);
    }

    private static string? ReadFile(string? path)
        => path is null || !File.Exists(path) ? null : File.ReadAllText(path).Trim();
}
```

3. `ModuleExtensions.cs`: `services.AddHttpClient(WorkerApiGateway.HttpClientName).ConfigurePrimaryHttpMessageHandler(sp => WorkerTlsHandler.Build(sp.GetRequiredService<IOptions<WorkerApiOptions>>().Value.KafkaTls));`
4. `WorkerApiGateway.cs`: `ApiKeyOf` возвращает `PgApiKey` только для "pgworker", kafkaworker → null (doc: «X-Api-Key для KafkaWorker удалён (t03); mTLS — клиентским сертом HttpClient»). `WorkerHealthPoller` — без правок логики (тот же именованный клиент).
5. Панельный host: `WorkerTlsHandler.ApplyEnvOverrides(configuration)` на этапе сборки конфигурации (точка — по grep `ADMINPANEL__`/хост-билдер панели).

- [ ] **Step 4: Прогон панельного набора — PASS**

Run: `dotnet test src/tests/AdminPanel.UnitTests -c Debug && dotnet build src/PgWorker.slnx -c Debug`
Expected: PASS; сборка чисто.

- [ ] **Step 5: Commit**

```bash
git add -A src/AdminPanel.Etcd src/AdminPanel.Api src/tests/AdminPanel.UnitTests
git commit -m "feat(panel): WorkerApiGateway — mTLS kafkaworker + env-биндинг KFW_PANEL_TLS_* (t03 Ф5)"
```

---

### Task 14: deploy, Dockerfile, gen.sh, dev-стенд, seed — mTLS-поставка (Ф5)

**Files:**
- Modify: `deploy/docker-compose.yml`, `deploy/.env.example`
- Create: `deploy/tls/gen.sh`, `deploy/tls/.gitignore`
- Modify: `docker/KafkaWorker.Dockerfile`
- Modify: `dev-stand/seed.sh`, `dev-stand/adminpanel/checks/{00-up.sh,05-seed.sh,55-kafka-e2e.sh,57-kafka-worker-health.sh}` (и прочие чеки с curl на :8081 — grep), `dev-stand/adminpanel/adminpanel.appsettings.json`, `dev-stand/adminpanel/README.md`, `dev-stand/adminpanel/docker-compose.yml` (env панели/воркера)

**Interfaces:**
- Consumes: Task 12/13 (env-имена `KFW_API_TLS_*` / `KFW_PANEL_TLS_*` / `ADMINPANEL__WORKERS__KAFKATLS__*_PATH`-пути).
- Produces: per-install TLS-пакет (`ca.pem`, `server.crt/key`, `panel.crt/key`, `healthcheck.crt/key`) в gitignored-каталогах; стенд поднимается целиком по mTLS (критерий §8.4).

- [ ] **Step 1: deploy/tls/gen.sh + gitignore**

`deploy/tls/gen.sh`:

```bash
#!/usr/bin/env bash
# Генерация per-install API-TLS-пакета KafkaWorker (spec t03 §5.5): CA,
# серверный серт воркера (SAN: localhost, host.docker.internal, kafkaworker),
# клиентские серты панели и docker-HEALTHCHECK. Вызывается оператором/стендом;
# серты вне git.
set -euo pipefail
cd "$(dirname "$0")"

openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
  -keyout ca.key -out ca.pem -subj "/CN=kfw-api-ca" \
  -addext "basicConstraints=critical,CA:TRUE"

openssl req -newkey rsa:2048 -nodes \
  -keyout server.key -out server.csr -subj "/CN=kafkaworker"
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -days 825 -out server.crt \
  -extfile <(printf "subjectAltName=DNS:localhost,DNS:host.docker.internal,DNS=kafkaworker")

openssl req -newkey rsa:2048 -nodes -keyout panel.key -out panel.csr -subj "/CN=kfw-panel"
openssl x509 -req -in panel.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 825 -out panel.crt

openssl req -newkey rsa:2048 -nodes -keyout healthcheck.key -out healthcheck.csr -subj "/CN=kfw-healthcheck"
openssl x509 -req -in healthcheck.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 825 -out healthcheck.crt

rm -f server.csr panel.csr healthcheck.csr
echo "TLS-пакет готов: ca.pem/server.crt/server.key/panel.crt/panel.key/healthcheck.crt/healthcheck.key (+ca.key — хранить отдельно)"
```

`deploy/tls/.gitignore`: содержимое `*` + `!.gitignore` (каталог gitignored, коммитится только сам файл). `chmod +x deploy/tls/gen.sh`.

- [ ] **Step 2: deploy/docker-compose.yml + .env.example + Dockerfile**

`kafkaworker`-сервис compose:

```yaml
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - kfw-snapshots:/snapshots
      - kfw-api-tls:/tls:ro
    environment:
      KafkaWorker__Etcd__Endpoints__0: ${KFW_ETCD_ENDPOINT:-http://localhost:2379}
      KafkaWorker__AdvertisedClientHost: ${KFW_ADVERTISED_CLIENT_HOST:-host.docker.internal}
      # mTLS HTTP API (arch/16 §1.1): KFW_API_KEY удалён (t03); серты — volume kfw-api-tls.
      KafkaWorker__Api__AdvertiseUrl: ${KFW_API_ADVERTISE_URL:-https://host.docker.internal:8081}
      KafkaWorker__Api__EnableSeedEndpoint: ${KFW_API_ENABLE_SEED:-false}
      KFW_API_TLS_CERT_PATH: /tls/server.crt
      KFW_API_TLS_KEY_PATH: /tls/server.key
      KFW_API_TLS_CLIENT_CA_PATH: /tls/ca.pem
# volumes: + kfw-api-tls:
```

`deploy/.env.example`: секция kafkaworker — комментарии про TLS-пакет (`bash deploy/tls/gen.sh`; наполнение volume `kfw-api-tls` файлами пакета — `docker cp`/пере-создание volume стендом, выбранный способ описать одинаково в `.env.example` и README стенда); `KFW_API_KEY` — удалить. `docker/KafkaWorker.Dockerfile` (healthz за mTLS — клиентская пара healthcheck из gen.sh, spec §5.5):

```dockerfile
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -sf --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key \
    https://localhost:8080/healthz || exit 1
```

- [ ] **Step 3: dev-stand**

1. `00-up.sh` — после проверки инструментов: генерация пакета `bash "$ROOT/deploy/tls/gen.sh"` в `deploy/tls/` (идемпотентно — только если `ca.pem` отсутствует); подъём kafkaworker через deploy-compose с bind-монтировкой `"$ROOT/deploy/tls:/tls:ro"` и env-путями; панель — env `ADMINPANEL__WORKERS__KAFKATLS__CLIENTCERT_PATH=/tls-workers/panel.crt`, `ADMINPANEL__WORKERS__KAFKATLS__CLIENTKEY_PATH=/tls-workers/panel.key`, `ADMINPANEL__WORKERS__KAFKATLS__SERVERCA_PATH=/tls-workers/ca.pem` (проброс файлов в контейнер панели через volumes стендового `dev-stand/adminpanel/docker-compose.yml`: `../../deploy/tls:/tls-workers:ro`).
2. Все curl к API воркера (`:8081`) в чеках (`00-up.sh`/`05-seed.sh`/`50-kafka-api.sh`/`55-kafka-e2e.sh`/`57-kafka-worker-health.sh`/`58-*`/`59-*`) — заменить на `curl -sf --cacert "$ROOT/deploy/tls/ca.pem" --cert "$ROOT/deploy/tls/panel.crt" --key "$ROOT/deploy/tls/panel.key" https://…` (grep `8081` по `dev-stand/` — точный список по факту; healthz-ожидания тоже https).
3. `dev-stand/seed.sh` — вызовы API воркера: mTLS-curl (те же флаги).
4. `adminpanel.appsettings.json` — Path-настройки НЕ дублировать (env-механизм приоритетен и единогласен с compose).
5. `README.md` стенда — раздел «mTLS API KafkaWorker»: gen.sh, где лежат серты, как ходят чеки.

- [ ] **Step 4: Проверка стенда (ручной прогон)**

Run: `bash /Users/demakaev/ZCodeProject/worktrees/feat-t03-kafka-security/dev-stand/adminpanel/checks/00-up.sh && bash /Users/demakaev/ZCodeProject/worktrees/feat-t03-kafka-security/dev-stand/adminpanel/checks/57-kafka-worker-health.sh && bash /Users/demakaev/ZCodeProject/worktrees/feat-t03-kafka-security/dev-stand/adminpanel/checks/55-kafka-e2e.sh`
Expected: стенд поднимается; панель видит `/healthz` воркера по https (mTLS), kafka-пробы зелёные через admin+CA, чеки проходят; после — `90-down.sh` (не оставлять стенд).

- [ ] **Step 5: Commit**

```bash
git add deploy docker dev-stand
git commit -m "feat(stand): mTLS-поставка kafkaworker — gen.sh, compose, чеки https (t03 Ф5)"
```

---

### Task 15: SecurityMigrator — converge-миграция PLAINTEXT→SASL_SSL (Ф6)

**Files:**
- Modify: `src/KafkaWorker.Docker/Engine/IDockerEngine.cs` + `DockerEngine.cs` (grep реализации — файл движка)
- Modify: `src/KafkaWorker.Docker/Drivers/ClusterDriver.cs` (`IClusterDriver`)
- Create: `src/KafkaWorker.Provisioning/Processes/SecurityMigrator.cs`
- Modify: `src/KafkaWorker.App/Loops/KafkaClusterProcesses.cs`, `src/KafkaWorker.App/Program.cs`
- Test: `src/tests/KafkaWorker.UnitTests/Provisioning/SecurityMigratorTests.cs`, Create: `src/tests/KafkaWorker.IntegrationTests/Kafka/SecurityMigrationTests.cs`

**Interfaces:**
- Consumes: `IClusterSecretEnsurer` (Task 3), `BrokerEnvBuilder.Build`-guard (Task 8), `ClusterPki`-валидация (Task 1/4), `IClusterConfigConverger` (ACL-шаг, Task 7).
- Produces: 
  - `IClusterDriver`: `Task<Result<IReadOnlyDictionary<string, string>?>> NodeEnvAsync(string cluster, string nodeName, CancellationToken ct)` — env живого контейнера (null = объекта нет); plain — inspect контейнера; swarm — inspect таска/сервиса.
  - `SecurityMigrator`: `public static bool NeedsMigration(KafkaClusterSnapshot snap, IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>> containerEnvs)` — чистый детект: `snap.CaPem/CaKey/AdminPassword is null` ИЛИ любой env без ключа `KAFKA_SSL_TRUSTSTORE_TYPE`; `public enum MigrationOutcome { NotNeeded, InProgress }`; `public Task<Result<MigrationOutcome>> RunAsync(KafkaClusterSnapshot snap, CancellationToken ct)` — M0–M4 (16 §5 M): премиграционный кластер → миграция (InProgress — отработал/ждёт следующим тиком), канонический → NotNeeded.
  - `KafkaClusterProcesses.ActiveAsync` — первым шагом M; `InProgress` → возврат из Active-ветки (остальное — следующим тиком).

- [ ] **Step 1: Failing-тесты детекта (чистая функция)**

```csharp
public class SecurityMigratorTests
{
    [Fact]
    public void NeedsMigration_NoCaInSnapshot_True()
    {
        // Arrange: Active-снапшот без CA/admin-полей (премиграционный).
        var snap = Snapshot(caPem: null, adminPassword: null);
        var envs = new Dictionary<string, IReadOnlyDictionary<string, string>>();

        // Act / Assert: детект по etcd-полям (16 §5 M).
        SecurityMigrator.NeedsMigration(snap, envs).Should().BeTrue();
    }

    [Fact]
    public void NeedsMigration_KeysPresentButPlainContainerEnv_True()
    {
        // Arrange: ключи ensure уже положил (M1 частично), контейнер жив на
        // SASL_PLAINTEXT (нет KAFKA_SSL_TRUSTSTORE_TYPE).
        var snap = Snapshot(caPem: "PEM", adminPassword: "Adm…");
        var envs = new Dictionary<string, IReadOnlyDictionary<string, string>>
        {
            ["broker1"] = new Dictionary<string, string> { ["KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"] = "CONTROLLER:PLAINTEXT,INTERNAL:SASL_PLAINTEXT,CLIENT:SASL_PLAINTEXT" },
        };

        // Act / Assert: детект по env контейнеров.
        SecurityMigrator.NeedsMigration(snap, envs).Should().BeTrue();
    }

    [Fact]
    public void NeedsMigration_CanonicalCluster_False()
    {
        // Arrange: ключи есть + env SSL.
        var envs = new Dictionary<string, IReadOnlyDictionary<string, string>>
        {
            ["broker1"] = new Dictionary<string, string> { ["KAFKA_SSL_TRUSTSTORE_TYPE"] = "PEM" },
        };

        // Act / Assert: повторный проход M — no-op (идемпотентность).
        SecurityMigrator.NeedsMigration(Snapshot("PEM", "Adm…"), envs).Should().BeFalse();
    }
}
```

Плюс 2–3 кейса `RunAsync` на fake-хостах (по образцу AppPasswordRotatorTests-фиков): M0-guard (живая заявка ротации → journal `waiting-rotation`, брокеры не тронуты; результат `InProgress`), M2 пересоздаёт ВСЕ брокеры (драйвер-fake фиксирует `RemoveNodeAsync(removeVolume: false)` для каждого), M3 waiting до готовности (fake admin не отвечает → journal `waiting-brokers`, тик успешен, `InProgress`).

- [ ] **Step 2: Прогнать — FAIL**

Run: `dotnet test src/tests/KafkaWorker.UnitTests -c Debug --filter FullyQualifiedName~SecurityMigratorTests`
Expected: FAIL.

- [ ] **Step 3: Реализовать драйвер-инспект env + SecurityMigrator**

1. `IDockerEngine`: `Task<Result<IReadOnlyDictionary<string, string>?>> InspectContainerEnvAsync(string idOrName, CancellationToken ct);` — `GET /containers/{id}/json` → `Config.Env[]` (`KEY=VALUE` → словарь); null при 404. `DockerEngine.cs` — реализация по образцу `InspectContainerResourcesAsync`. Swarm: env сервиса из спеки (endpoint `/services/{name}` → `Spec.TaskTemplate.ContainerSpec.Env`) — по образцу существующего сервисного инспекта.
2. `IClusterDriver.NodeEnvAsync(cluster, nodeName)` — plain: перебор хостов `InspectContainerEnvAsync(kfw-<C>-<b>)`, первый найденный; swarm: сервисный env. Оба драйвера.
3. `SecurityMigrator.cs` (конструктор — по образцу `AppPasswordRotator`: etcd, endpoints, driver, claims, journal, secrets (IClusterSecretEnsurer), adminFactory, converger, options, certificates (BrokerCertificateCache), snapshot-делегат; `Op = "migrate-security"`):

```csharp
// M0: claim-гвард (claims.IsMine), снапшот P12 «до»; guard'ы: живые заявки
// /kafkaworker/{rotations,admin_rotations,rebalances}/<C> или прогресс
// {reassignments,regens}/<C> → journal("waiting-rotation"/"waiting-reassignment"),
// результат InProgress — передержка тиком.
// M1: secrets.EnsureAsync(cluster) — CA + admin (+ app добором той же txn).
// M2: для каждого живого брокера (state != TO_REMOVE/REMOVING): env-инспект
// (driver.NodeEnvAsync) — KAFKA_SSL_TRUSTSTORE_TYPE уже есть → пропуск;
// иначе RemoveNodeAsync(removeVolume: false) + EnsureNodeAsync (env нового
// канона через BrokerEnvBuilder.Build со снапшотными паролями и certificates)
// — ВСЕ разом (не rolling: смешанные inter-broker протоколы невозможны, 16 §5 M);
// адреса — portalloc (чтение — по образцу PasswordRotator.ReadPortAllocAsync).
// M3: цикл готовности — adminFactory.Create(endpoints, admin, caPem)
// .DescribeClusterAsync: брокеров = числу живых; не готово — journal
// ("waiting-brokers"), InProgress — следующий тик (бюджет BrokerBootSec —
// по образцу ProvisioningProcess._bootWaitSince); готово → state=RUNNING всем.
// M4: converger.ApplyAsync(cluster, endpoints, admin…, caPem, snap.Config)
// (стартовый ACL-converge); endpoints НЕ пишем (хосты/порты не менялись);
// снапшот P12 «после»; journal done → InProgress (закрывающий тик: следующий
// прогон NeedsMigration вернёт false → NotNeeded).
// Канонический кластер (NeedsMigration=false при входе) → сразу NotNeeded.
```

4. `KafkaClusterProcesses`: в начало `ActiveAsync`:

```csharp
// Премиграционный кластер (SASL_PLAINTEXT) — SecurityMigrator ДО всего
// Active (arch/16 §5 M): converge/пробы старого кластера бессмысленны.
var migrated = await migrator.RunAsync(snap, ct);
if (!migrated.IsSuccess)
    return migrated;
if (migrated.Value == MigrationOutcome.InProgress)
    return Result.Success(); // M отработал/ждёт — остальное следующим тиком
```

`Program.cs` — DI `SecurityMigrator` (все зависимости из DI) + конструктор `KafkaClusterProcesses` + `migrator`-параметр.

- [ ] **Step 4: Прогон юнит — PASS**

Run: `dotnet test src/tests/KafkaWorker.UnitTests -c Debug`
Expected: PASS.

- [ ] **Step 5: Интеграционный docker-тест миграции**

`SecurityMigrationTests.cs`: 
1. Поднять «премиграционный» кластер: сид `SeedClusterAsync`, ensure вручную ТОЛЬКО app-кредов (`PutAsync app_user/app_password`), создать контейнеры со СТАРЫМ env — хелпер теста `LegacyPlainEnv(cluster, broker, port)` — копия прежней таблицы `NodeEnvBuilder` (SASL_PLAINTEXT-канон, JAAS только `user_app`) локально в файле теста (старого кода в src больше нет — тест фиксирует «как было»; единственное допустимое место `SASL_PLAINTEXT` после t03 — см. Task 16 Step 3).
2. Дождаться готовности PLAINTEXT-кластера (DescribeCluster PLAINTEXT-клиентом), создать топик `legacy`, записать сообщение (PLAINTEXT producer, app-кред).
3. `SecurityMigrator.RunAsync` тиками до `NotNeeded` (поллинг ≤ 200 с); Assert: `ca_pem/ca_key/admin_password` появились; env контейнера содержит `KAFKA_SSL_TRUSTSTORE_TYPE`; endpoints НЕ изменились (строка та же); SASL_SSL-consumer (admin+ca_pem) читает `legacy`-сообщение (данные живы); app-кред по SASL_SSL производит (ACL после M4); повторный `RunAsync` — `NotNeeded` сразу.

- [ ] **Step 6: Прогон docker-миграции — PASS**

Run: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/KafkaWorker.IntegrationTests -c Debug --filter FullyQualifiedName~SecurityMigrationTests`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add -A src/KafkaWorker.Docker src/KafkaWorker.Provisioning src/KafkaWorker.App src/tests
git commit -m "feat(kafka): SecurityMigrator — converge-миграция PLAINTEXT→SASL_SSL (t03 Ф6, arch/16 §5 M)"
```

---

### Task 16: Финал — полный прогон, e2e Release, roadmap-чистка, доки (Ф7)

**Files:**
- Modify: `arch/roadmap/kafkaworker.md`
- Modify: `README.md` (если упоминает X-Api-Key/KFW_API_KEY для kafkaworker — grep), `deploy/.env.example`-комментарии (Task 14), `dev-stand/README.md` (если есть упоминания)

**Interfaces:**
- Consumes: все задачи.
- Produces: мерж-гейт-грины + чистый roadmap (спека §9).

- [ ] **Step 1: Полный юнит+интеграционный прогон Debug**

Run: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Debug`
Expected: PASS (весь slnx — kafka/pg/panel юниты + docker-интеграционные).

- [ ] **Step 2: E2E Release (мерж-гейт, критерий §8.3 — полный + маркеры)**

Run:
```bash
# Полный прогон KafkaWorker.IntegrationTests на свежем Release (сборка инкрементальная)
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/KafkaWorker.IntegrationTests -c Release
# Маркер-кейс t03 (быстрая проверка при итерациях)
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Provisioning_TlsClusterUp
# pg-маркер: общие слои не задеты регрессией
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard
```
Expected: все три зелёные (Release собирается сам; `PGW_TEST_E2E_NOBUILD` НЕ использовать).

- [ ] **Step 3: Гигиенический grep (критерий §8.6)**

Run: `grep -rn "KFW_API_KEY\|X-Api-Key" src/ deploy/ dev-stand/ docker/ --include='*.cs' --include='*.yml' --include='*.yaml' --include='*.sh' --include='*.json' | grep -vi pgworker | grep -vi PgApiKey` → пусто; `grep -rn "SASL_PLAINTEXT" src/ deploy/ dev-stand/` → единственное вхождение — legacy-env хелпер `SecurityMigrationTests.cs` (комментарий «старый канон, до t03»).
Expected: как описано (осознанные исключения — только тест-легаси и pg-домен).

- [ ] **Step 4: Roadmap-чистка (мерж-гейт, спека §9)**

`arch/roadmap/kafkaworker.md`: удалить запись `t03-kafka-security` (вкл. `← t03`-зависимости в других пунктах — grep `t03` по `arch/roadmap/*.md`); добавить:

```markdown
- **`t07-kafka-ca-rotation`** — ротация per-cluster CA и серверных сертификатов
  (окно двойного доверия CA/серт-версий в env, rolling-пересоздание брокеров;
  отложено из t03: серты долгоживущие — 10 лет; зависит от канона безопасности
  arch/16 §2.3 и `BrokerCertificateCache`).
```

- [ ] **Step 5: Финальный коммит**

```bash
git add arch/roadmap README.md deploy dev-stand
git commit -m "chore(roadmap): t03-kafka-security исполнена — запись удалена, добавлена t07-kafka-ca-rotation (t03 Ф7)"
```

---

## Самопроверка (выполнена при написании + обновлена по двум ревью Фазы 4)

- **Покрытие спеки**: Ф1=Tasks 1–2; Ф2=Tasks 3–5; Ф3=Tasks 6–9; Ф4=Tasks 10–11; Ф5=Tasks 12–14; Ф6=Task 15; Ф7=Task 16. Критерии §8: (1) юнит — Tasks 1,2,4,5,7,8,10,12,13,15 (вкл. env-биндинг TLS — §8.1); (2) интеграционные docker — Tasks 9,11,12,15 (+portalloc/t91 не сломаны — Task 16 Step 1); (3) E2E Release — Task 16 Step 2 (ПОЛНЫЙ Release-прогон KafkaWorker.IntegrationTests + оба маркера); (4) стенд — Task 14 Step 4; (5) контракт/парсеры — Tasks 3–5, 9 Step 2; (6) код-гигиена — Task 16 Step 3. Разделение кредов — Tasks 3,5,8,13; ротация admin — Tasks 10–11; mTLS API — Tasks 12–14; миграция — Task 15; дискавери-контракт 15 §5 — Tasks 6,9.
- **Bisect-целостность Task 2** (замечание повторного ревью 1): Вариант А — решение plan-фазы №6; Step 3б компенсирует ТРИ прод-вызова `new NodeEnvSpec(...)` placeholder-аргументами (BrokerEnvBuilder — покрывает ротатор/add-broker/регенератора; NodeSupervisor; ProvisioningProcess), Step 4 проверяет `dotnet build src/PgWorker.slnx` + полный юнит-набор, Step 5 коммитит Core вместе с компенсированным продом. Placeholder'ы снимает Task 8 (реальные данные из снапшота Task 4 + `BrokerCertificateCache` Task 1); NodeSupervisor-вызов переводится на `BrokerEnvBuilder.Build`.
- **Панельные пробы** (первое ревью 1): закрыты Task 5 Steps 4–6 — `KafkaClientCache` (SaslSsl + `ssl.ca.pem`, ключ кэша с caPem, метрика CreatedClients в тесте), seam-интерфейсы `IKafkaProbeClient`/`IKafkaProbeRuntimeClient` (+`caPem`), оба Confluent-адаптера (`GetAdmin/GetConsumer/Invalidate` с caPem), `KafkaProbeLoop` (admin-креды + CaPem из стора) — панель компилируется и ходит по TLS как admin.
- **Согласование с каноном** (первое ревью 2): транспорт AdminClient по CLIENT endpoints зафиксирован в решении plan-фазы №1; канон `arch/16` §2.3 и §5-M3 синхронизируется Task 8 Step 0 (arch-first, одним коммитом с кодом); spec §5.2 уже поправлен при ревью плана.
- **HEALTHCHECK за mTLS** (повторное ревью 3): план Task 14 (клиентская пара `healthcheck.crt/key` в curl + выпуск в gen.sh) и spec §5.5 синхронизированы (правка внесена при ревью плана).
- **Шапка-комментарий Program.cs** (повторное ревью 4): Task 12 Step 4 п.3 — явная замена фразы «Env-секретов per-install НЕТ…» на «Per-install env-секреты — только TLS HTTP API (arch/16 §4); per-cluster секреты (app/admin/CA) — в etcd.» — закрывает требование spec §5.3 по обоим файлам (Options.cs — там же, п.1).
- **Placeholder-скан**: механические правки описаны точными списками файлов/паттернов (grep-инструкции); кодные шаги несут код; `TlsEndpoints`/`WorkerTlsHandler` — без using на долгоживущих сертах (валидация цепочки на каждом хендшейке; то же — клиентский серт в `MtlsApiTests`: возвращается наружу кортежем `TlsHost`, dispose в тесте — замечание повторного ревью 2), ConfigureKestrel на `WebApplicationBuilder` до Build, сигнатуры `ConfigureMtls(WebApplicationBuilder, int)` / `ApplyEnvOverrides(ConfigurationManager, Func)` едины во всех местах.
- **Консистентность типов**: `ClusterSecrets` (T3) → ProvisioningProcess/SecurityMigrator (T8/T15); `BrokerCertificateCache.GetOrCreate(cluster, broker, caCertPem, caKeyPem, advertisedClientHost)` (T1) → BrokerEnvBuilder (T8); `NodeEnvSpec`-поля (T2, канонические позиции, без дефолтов) → все вызовы Build (T8/T10/T15); `IKafkaAdminClientFactory.Create(bootstrap, user, password, caPem)` (T6) → все процессы (T8/T10/T15) и фикстуры (T9); `ApplyAsync(cluster, bootstrap, user, password, caPem, config, ct)` (T7) → K5/Active/M4 (T8/T15); `KafkaClusterSecrets(Cluster, AdminUser, AdminPassword, CaPem)` (T5) → `KafkaProbeLoop` (T5) и стора-контур панели; `MigrationOutcome` (T15) — единый в тестах и `KafkaClusterProcesses`.

## Execution Handoff

План сохранён в `docs/superpowers/2026-09-04-t03-kafka-security/plan.md`. Варианты исполнения: (1) Subagent-Driven (рекомендуется) — свежий субагент на задачу с ревью между задачами; (2) Inline — executing-plans батчами с чекпоинтами.
