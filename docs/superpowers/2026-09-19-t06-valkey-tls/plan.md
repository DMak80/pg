# t06-valkey-tls — план реализации (TLS клиентских подключений Valkey-кластеров)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Цель:** перевести клиентские подключения Valkey-кластеров на TLS (per-cluster CA, `--tls-port 6379 --port 0`, volume-доставка сертов, авто-миграция plain→TLS, панель/Puzzle-клиенты по TLS) — риск R5 arch/21 закрыт.

**Архитектура:** образец — kafka t03/t07 (`ClusterPki`, `SecurityMigrator`, ensure CA одной txn с кредами). Отличия зафиксированы: файлы-серты вместо PEM-env (named volume `vwk-<C>-tls` + Docker tar-API), один порт вместо listener-карты, ACL вместо JAAS, nodes=1 (миграция = одно пересоздание контейнера). Клиенты валидируют сервер `X509Chain CustomRootTrust` против `ca_pem` + SAN advertised-хоста.

**Технологии:** .NET 10 (`TreatWarningsAsErrors=true`, Nullable), CertificateRequest/SslStream/X509Chain без внешних инструментов; Docker Engine API v1.44 (`POST /volumes/create`, `PUT|GET /volumes/{name}/archive`); StackExchange.Redis (certificate-validation callback); xunit v3 + Testcontainers.

**Spec:** [`spec.md`](spec.md) (рядом) — план аргументируется от спека, исполнитель читает оба.

## Каталоги/ветки

- **pg-worktree**: `/Users/demakaev/ZCodeProject/worktrees/feat-t06-valkey-tls`, ветка `feat-t06-valkey-tls` (создана). Все пути ниже относительны worktree, если не указано иное.
- **Puzzle**: `/Users/demakaev/ZCodeProject/Puzzle`, отдельный git-репозиторий. Ветка `feat-t06-valkey-tls` создаётся от `main` в задаче 11. Там действует `/Users/demakaev/ZCodeProject/Puzzle/AGENTS.md` + `AGENTS.base.md`. Staged-файл `src/global.json` НЕ трогать. Коммиты в ветке — свободно (spec §10.3); мерж в `main` обоих репо — ТОЛЬКО по отдельной явной просьбе пользователя.

## Глобальные ограничения (каждая задача неявно включает этот раздел)

- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`; пакеты только через `Directory.Packages.props`; НОВЫХ пакетов нет (spec §2.3, §7).
- Идентификаторы — английские; комментарии/докуметация — русские; тесты — AAA-комментарии (// Arrange / // Act / // Assert).
- Режим Result-монады pg (`Result`/`Result<T>`), никаких `throw` через границы процессов.
- Идемпотентность каждого шага; journal-before-manipulations; мутации только держателем клэйма `<C>` (spec §2.4).
- Тесты: динамические порты (`FreePortWindow`/`assignRandomHostPort`), guid-изоляция, полный teardown + ассерт чистоты ВКЛ. volume `vwk-*`, `NodeBootSec` ≤ 100, зачистка docker-артефактов после каждой серии (AGENTS.md, spec §2.7).
- PEM в значениях etcd — одной строкой с `\n` (переносами внутри строки; канон arch/20 §2.1, образец arch/15 §2.1).
- CA: RSA-2048, 10 лет, subject уникален на генерацию (отпечаток ключа); серт ноды: CN=`node<k>`, SAN только advertised-хост (DNS|IP), EKU только ServerAuth, NotAfter зажат в CA. `ca_next_*` НЕ вводятся; клиентские серты — нет (`--tls-auth-clients no`); TLS-тюнинг — дефолты (spec §1.1).
- KafkaWorker/PgWorker-код не трогается; локальные образы в registry не класть; новых внешних образов нет (`valkey/valkey:9.1.2` прежний).
- Сборка-гейт каждой задачи pg: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Debug` — 0 warnings (TreatWarningsAsErrors); Puzzle: `dotnet build src/PuzzleServer.Api.slnx`.
- Перед docker-тестами: `dev-stand/images/pull-images.sh` уже выполнен (образ etcd/valkey локально); после КАЖДОЙ серии — `docker ps -a | grep vwk-`, `docker volume ls | grep vwk-`, `docker network ls | grep -c 'vwk\|kfw'` → чисто (страховочный гейт).

## Карта файлов

**pg — новые:** `src/ValkeyWorker.Core/Valkey/ValkeyPki.cs`; `src/ValkeyWorker.Docker/Engine/TarArchive.cs`; `src/ValkeyWorker.Provisioning/Processes/NodeTlsProvisioner.cs`; `src/ValkeyWorker.Provisioning/Processes/TlsMigrator.cs`; тесты (см. задачи).

**pg — правки:** `arch/20-valkey-clusters.md`, `arch/21-valkeyworker.md`, `arch/adminpanel/02-etcd-contract.md`, `arch/roadmap/valkey.md` (мерж-гейт), `docs/runbook.md`; `src/ValkeyWorker.Core/Model/ValkeyDomain.cs`, `src/ValkeyWorker.Core/Valkey/{IValkeyConnection,ValkeyConnection}.cs`; `src/ValkeyWorker.Docker/Engine/{IDockerEngine,DockerEngine}.cs`, `src/ValkeyWorker.Docker/Drivers/ClusterDriver.cs`; `src/ValkeyWorker.Etcd/Parsing/ValkeySnapshotParser.cs`; `src/ValkeyWorker.Provisioning/Processes/{ClusterSecretEnsurer,NodeArgsBuilder,ProvisioningProcess,NodeSupervisor,DeprovisioningProcess,ProcessCommon}.cs`; `src/ValkeyWorker.App/Loops/ValkeyClusterProcesses.cs`, `src/ValkeyWorker.App/Program.cs`; `src/ValkeyWorker.Core/ValkeyWorker.Core.csproj` (+Shared.Tls); панель: `src/AdminPanel.Core/Valkey/ValkeySnapshot.cs` (+HasCaPem), `src/AdminPanel.Etcd/{ValkeySecretsStore,ValkeySnapshotRefresher}.cs`, `src/AdminPanel.Etcd/Parsing/ValkeyParser.cs`, `src/AdminPanel.Probes/Valkey/{IValkeyProbeClient,ValkeyConnection}.cs`, `src/AdminPanel.Core/Valkey/ValkeyAlerting/ValkeyAlertEngine.cs`; стенд: `dev-stand/adminpanel/checks/51-valkey-api.sh`.

**Puzzle — правки:** `src/PuzzleServer.Infrastructure.App.HA.Valkey/Parsing/ValkeyClusterParser.cs`, `.../Model/{ValkeyClusterSnapshot,ValkeyClientConfig}.cs`, `.../Model/ValkeyClusterData` (тот же файл парсера); `src/PuzzleServer.Infrastructure.App.Valkey/{ValkeyConnectionParams,ValkeyConnectionOptions,DiscoveryValkeyConnectionProvider}.cs`; доки `docs/01.21-ha-valkey.md`, `docs/01.22-valkey.md`; тесты (см. задачи 11–13).

---

## Задача 1. Arch-правки (arch-first, до любого кода)

**Spec:** §3.1–§3.3 (полный список правок), §6 фаза 1.
**Вход:** worktree чистый, ветка `feat-t06-valkey-tls`; файлы `arch/20-valkey-clusters.md`, `arch/21-valkeyworker.md`, `arch/adminpanel/02-etcd-contract.md` прочитаны.
**Файлы (Modify):** три arch-файла выше.

- [ ] **Шаг 1.1. `arch/20-valkey-clusters.md`** — по букве spec §3.1:
  - §2 таблица: две новые строки после `admin_password`:
    - `ca_pem` | PEM-серт CA одной строкой с `\n` | воркер (ensure, txn put-if-absent) | публичный CA per-cluster — **точка дискавери TLS** внешних клиентов; панель читает для live-проб (internal); парсеры читателей не падают (unknownKeys-толерантность §5).
    - `ca_key` | PEM PKCS#8 приватного ключа CA | воркер (ensure, txn put-if-absent) | подпись серверных сертов нод; секрет воркера — панель и приложения НЕ читают.
  - Сноска «Отличия от kafka-таблицы» §2: заменить фрагмент «нет `ca_pem`/`ca_key`/`ca_next_*` (TLS — t06-valkey-tls; после t06 добавятся `ca_pem`/`ca_key`)» на «`ca_pem`/`ca_key` есть (t06); `ca_next_*` нет (ротация CA — roadmap)».
  - §2.1: добавить канонические примеры: `ca_pem`/`ca_key` — PEM (`-----BEGIN CERTIFICATE-----…`) одной строкой с `\n`; CN CA — `vwk-<C>-ca-<8hex>`; серт ноды CN=`node<k>`, SAN advertised-хоста.
  - §4: добавить шаг 4 «`/valkey/clusters/<C>/ca_pem` → TLS-доверие клиента»; правило «`ssl=true ⟺ ca_pem` прочитан»; неполный набор (Active без `ca_pem`) — переходное/миграционное состояние (см. §5); убрать оговорку «ssl=false (v1 без TLS)» — заменить на «после t06 `GetClientConfig()` отдаёт `ssl=true` + CA».
  - §5 таблица сбоев: + «битый PEM в `ca_pem`/`ca_key` → parseError + warning `valkey-key-malformed`»; + «Active-кластер без `ca_pem` → critical-алерт `valkey-security-missing` (миграция не доиграна / ключ потерян)».

- [ ] **Шаг 1.2. `arch/21-valkeyworker.md`** — по букве spec §3.2:
  - Преамбула/границы: TLS клиентских подключений — входит (t06); из границ убрать «TLS» — вместо неё «ротация CA/сертов (окно двойного доверия) — roadmap»; зафиксировать обязанность t05-коллектора ходить по TLS-транспорту (§7).
  - §2 модель размещения: нода слушает `--tls-port 6379 --port 0` (тот же клиентский host-порт из portalloc; plain закрыт); канонические аргументы: `--tls-cert-file /tls/node.crt --tls-key-file /tls/node.key --tls-ca-cert-file /tls/ca.pem --tls-auth-clients no --tls-replication no`; серт CN=`node<k>`, SAN advertised-хоста (DNS|IP по §2), 10 лет, RSA-2048, подпись `ca_key`; доставка — named volume `vwk-<C>-tls` (воркер пишет `node.crt`/`node.key`/`ca.pem` Docker tar-API ДО старта контейнера, mount → `/tls`; volume переживает пересоздания контейнера, удаляется в X1); перечисление объектов домена дополняется volume.
  - §3.1/§3.2: читаемые/пишемые ключи дополняются `ca_pem`/`ca_key` (ensure txn put-if-absent при provisioning V2 и миграции T1).
  - §4 секреты: + группа CA (`ca_key` — подпись, `ca_pem` — дискавери; компрометация etcd = зона доверия контроль-плейна, образец kafka R10).
  - §5: классификация Active-ветки — миграция TLS первым шагом (детект до надзора: Active без `ca_pem`/`ca_key` ИЛИ контейнер без `--tls-port` → TlsMigrator); V2 — ensure кредов + CA одной txn; V3 — генерация серта ноды + запись файлов в volume + контейнер с TLS-args; V4 — PING по TLS (SslStream, доверие `ca_pem`); новый процесс **T. TlsMigrator (t06)** с фазами T0–T3 (claim + journal op=migrate-tls + снапшот «до»; T1 ensure CA + re-read; T2 пересоздание контейнера с каноническими TLS-args — порт/лимиты те же; T3 PING по TLS → state=RUNNING, снапшот «после», journal done; идемпотентность по факту; отработавший миграцию кластер неотличим от поднятого канонически); C/D/E — по TLS-соединению.
  - §6/§7: пробы/команды воркера — TLS (SslStream + ручная валидация цепочки против `ca_pem` + SAN-хост); коллектор t05 — тот же транспорт.
  - §9: R5 переписан (TLS есть; остаточный вектор — внутри закрытой сети контроль-плейна); +R8-аналог (SAN vs смена `AdvertisedClientHost` — серт пересобирается при пересоздании); +R9-аналог (окно миграции: TLS-неготовые клиенты получают отказ после пересоздания — заявлено релизом t06); +R10-аналог (`ca_key` в etcd — зона доверия; ротация — roadmap).

- [ ] **Шаг 1.3. `arch/adminpanel/02-etcd-contract.md` §11** — по букве spec §3.3:
  - §11.1 таблица: строка `admin_user`, `admin_password` дополняется `ca_pem` (internal-стор, рядом с admin-кредами; наружу не отдаётся); live-пробы PING — TLS (SslStream + доверие `ca_pem`); ошибок чтения/валидации PEM — толерантность как у кредов.
  - Абзац «v1-упрощение» преамбулы §11: убрать «TLS» из списка roadmap-ограничений, сослаться на arch/20 §2 (t06).
  - §11.1 примечание: Active-кластер без `ca_pem` → critical-алерт `valkey-security-missing` (arch/20 §5).

**Выход:** канон трёх доменов описывает TLS-контракт до кода; противоречий с §20–21 старой редакции нет (STUB-оговорки «v1 без TLS» убраны).
**Проверка:** `grep -rn 't06' arch/20-valkey-clusters.md arch/21-valkeyworker.md arch/adminpanel/02-etcd-contract.md` — правки на месте; `grep -n 'v1 без TLS\|TLS — roadmap' arch/20-valkey-clusters.md` — пусто (оговорки сняты).
**Коммит:** `git add arch/20-valkey-clusters.md arch/21-valkeyworker.md arch/adminpanel/02-etcd-contract.md && git commit -m "docs(arch): t06 — TLS-контракт valkey (ca_pem/ca_key, tls-port, TlsMigrator, панель)"`.

---

## Задача 2. PKI-ядро: `ValkeyPki`

**Spec:** §4.1 (ValkeyPki), §1.1 (параметры CA/серта), §6 фаза 2.
**Вход:** задача 1 закоммичена.
**Файлы:** Create `src/ValkeyWorker.Core/Valkey/ValkeyPki.cs`; Test `src/tests/ValkeyWorker.UnitTests/Valkey/ValkeyPkiTests.cs` (новый каталог).
**Интерфейсы (Produces):** используются задачами 3, 6, 7 и тестами.
- `static (string CaPem, string CaKeyPem) GenerateCa(string cluster)`
- `static (string CertPem, string KeyPem) IssueNodeCertificate(string caCertPem, string caKeyPem, string commonName, string advertisedHost)`
- `static bool TryParseCertificate(string pem, out X509Certificate2? certificate)`
- `static bool TryParseRsaKey(string pem, out RSA? key)`

- [ ] **Шаг 2.1. Тесты (падающие).** Создать `ValkeyPkiTests.cs` (порт стиля `ClusterPki`-тестов kafka, если есть — свериться `src/tests/KafkaWorker.UnitTests`):

```csharp
using System.Net;
using System.Security.Cryptography.X509Certificates;
using ValkeyWorker.Core.Valkey;

namespace ValkeyWorker.UnitTests.Valkey;

// PKI per-cluster (spec §4.1): CA RSA-2048/10 лет/уникальный subject;
// серт ноды CN=node<k>, SAN advertised (DNS|IP), EKU ServerAuth, PEM round-trip.
public sealed class ValkeyPkiTests
{
    [Fact]
    public void GenerateCa_UniqueSubjectPerGeneration()
    {
        // Arrange — две генерации CA одного кластера
        // Act
        var (pem1, _) = ValkeyPki.GenerateCa("c1");
        var (pem2, _) = ValkeyPki.GenerateCa("c1");
        // Assert — subject уникален (отпечаток ключа; фикс t07 kafka)
        Assert.NotEqual(ExtractSubject(pem1), ExtractSubject(pem2));
        Assert.StartsWith("CN=vwk-c1-ca-", ExtractSubject(pem1));
    }

    [Fact]
    public void IssueNodeCertificate_SanDnsAndIp()
    {
        // Arrange
        var (caPem, caKeyPem) = ValkeyPki.GenerateCa("c1");
        // Act — DNS-хост и IP-хост advertised
        var (dnsCert, _) = ValkeyPki.IssueNodeCertificate(caPem, caKeyPem, "node1", "host.example");
        var (ipCert, _) = ValkeyPki.IssueNodeCertificate(caPem, caKeyPem, "node1", "127.0.0.1");
        // Assert — SAN покрывает advertised, EKU только ServerAuth
        Assert.Contains("host.example", SanNames(dnsCert));
        Assert.Contains(IPAddress.Parse("127.0.0.1"), SanIps(ipCert));
    }

    [Fact]
    public void Pem_RoundTrip_FromSingleLineWithEscapes()
    {
        // Arrange — etcd-канон: PEM одной строкой с \n
        var (caPem, caKeyPem) = ValkeyPki.GenerateCa("c1");
        var flat = caPem.Replace("\r\n", "\n");
        // Act
        var ok = ValkeyPki.TryParseCertificate(flat, out var cert);
        var keyOk = ValkeyPki.TryParseRsaKey(caKeyPem, out _);
        // Assert
        Assert.True(ok);
        Assert.NotNull(cert);
        Assert.True(keyOk);
    }

    [Fact]
    public void TryParse_BrokenPem_False()
    {
        // Arrange / Act / Assert — мусор не бросает исключений
        Assert.False(ValkeyPki.TryParseCertificate("not a pem", out var cert));
        Assert.Null(cert);
        Assert.False(ValkeyPki.TryParseRsaKey("not a pem", out var key));
        Assert.Null(key);
    }
}
```

Хелперы `ExtractSubject`/`SanNames`/`SanIps` — приватные в тест-классе: `X509Certificate2.CreateFromPem(pem)` + `X509SubjectAltNameExtension` (`EnumerateDnsNames()`/`EnumerateIPAddresses()` — .NET 9+ API, доступен в .NET 10).

- [ ] **Шаг 2.2. Прогнать — падают.** `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.UnitTests -c Debug --filter FullyQualifiedName~ValkeyPki` → FAIL/ошибка сборки «ValkeyPki не существует».

- [ ] **Шаг 2.3. Реализация.** Создать `src/ValkeyWorker.Core/Valkey/ValkeyPki.cs` — порт `src/KafkaWorker.Core/Templates/ClusterPki.cs` с отличиями домена (читать образец целиком при исполнении):

```csharp
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace ValkeyWorker.Core.Valkey;

/// <summary>
/// Per-cluster PKI valkey (arch/20 §2, arch/21 §2; t06): self-signed CA
/// (RSA-2048, CN=vwk-&lt;C&gt;-ca-&lt;отпечаток&gt; — subject уникален на генерацию,
/// 10 лет) и серверные серты нод (CN=node&lt;k&gt;, SAN ТОЛЬКО advertised-хост
/// DNS|IP, EKU ServerAuth, 10 лет зажаты в CA) — CertificateRequest .NET,
/// без внешних инструментов. PEM — одной строкой с \n (канон значений etcd).
/// Порт kafka ClusterPki (t03/t07); клиентские серты не выпускает —
/// принципалы из ACL (--tls-auth-clients no).
/// </summary>
public static class ValkeyPki
{
    private static readonly Oid ServerAuthOid = new("1.3.6.1.5.5.7.3.1");

    public static (string CaPem, string CaKeyPem) GenerateCa(string cluster)
    {
        using var rsa = RSA.Create(2048);
        // Отпечаток ключа в CN: одинаковые subject поколений ломали
        // верификацию в бандлах двойного доверия (t07 kafka) — фикс переносится.
        var fingerprint = Convert.ToHexString(
            SHA256.HashData(rsa.ExportSubjectPublicKeyInfo()), 0, 4).ToLowerInvariant();
        var request = new CertificateRequest(
            $"CN=vwk-{cluster}-ca-{fingerprint}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        using var ca = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddYears(10));
        return (ca.ExportCertificatePem(), ca.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());
    }

    public static (string CertPem, string KeyPem) IssueNodeCertificate(
        string caCertPem, string caKeyPem, string commonName, string advertisedHost)
    {
        using var caCertificate = ParseCertificate(caCertPem);
        using var caKey = ParseRsaKey(caKeyPem);
        using var caWithKey = caCertificate.CopyWithPrivateKey(caKey);
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN={commonName}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        // SAN — ТОЛЬКО advertised-хост кластера (arch/21 §2): standalone,
        // inter-node-алиаса нет; IP-хост — IP-запись SAN.
        if (IPAddress.TryParse(advertisedHost, out var ip))
            san.AddIpAddress(ip);
        else
            san.AddDnsName(advertisedHost);
        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(
            new X509EnhancedKeyUsageExtension([ServerAuthOid], critical: false));
        var notAfter = DateTimeOffset.UtcNow.AddYears(10);
        if (notAfter > caWithKey.NotAfter)
            notAfter = caWithKey.NotAfter;
        using var certificate = request.Create(
            caWithKey, DateTimeOffset.UtcNow.AddDays(-1), notAfter,
            RandomNumberGenerator.GetBytes(16));
        return (certificate.ExportCertificatePem(), rsa.ExportPkcs8PrivateKeyPem());
    }

    public static bool TryParseCertificate(string pem, out X509Certificate2? certificate)
    {
        try { certificate = ParseCertificate(pem); return true; }
        catch (Exception e) when (e is ArgumentException or CryptographicException or FormatException)
        { certificate = null; return false; }
    }

    public static bool TryParseRsaKey(string pem, out RSA? key)
    {
        try { key = ParseRsaKey(pem); return true; }
        catch (Exception e) when (e is ArgumentException or CryptographicException or FormatException)
        { key = null; return false; }
    }

    private static X509Certificate2 ParseCertificate(string pem)
        => X509CertificateLoader.LoadCertificate(DecodePemBlock(pem, "CERTIFICATE"));

    private static RSA ParseRsaKey(string pem)
    {
        var rsa = RSA.Create();
        try { rsa.ImportFromPem(pem); return rsa; }
        catch { rsa.Dispose(); throw; }
    }

    private static byte[] DecodePemBlock(string pem, string label)
    {
        var beginMarker = $"-----BEGIN {label}-----";
        var endMarker = $"-----END {label}-----";
        var start = pem.IndexOf(beginMarker, StringComparison.Ordinal);
        if (start < 0) throw new ArgumentException($"PEM не содержит блока {label}");
        var bodyStart = start + beginMarker.Length;
        var end = pem.IndexOf(endMarker, bodyStart, StringComparison.Ordinal);
        if (end < 0) throw new ArgumentException($"PEM не содержит конца блока {label}");
        var base64 = new string(pem[bodyStart..end].Where(c => !char.IsWhiteSpace(c)).ToArray());
        return Convert.FromBase64String(base64);
    }
}
```

- [ ] **Шаг 2.4. Прогнать — зелёные.** Та же команда → PASS.
- [ ] **Шаг 2.5. Коммит:** `git add src/ValkeyWorker.Core/Valkey/ValkeyPki.cs src/tests/ValkeyWorker.UnitTests/Valkey/ && git commit -m "feat(valkey): ValkeyPki — per-cluster CA и серверные серты (порт ClusterPki kfw, t06)"`.

**Выход:** PKI-примитивы для ensurer'а, процессов и тестов.
**Проверка задачи:** шаг 2.4 зелёный + `dotnet build src/PgWorker.slnx -c Debug` без warnings.

---

## Задача 3. Контракт etcd в коде: модель, парсер, ensure CA

**Spec:** §3.1 (ключи), §4.3 (парсер), §4.4 (ClusterSecretEnsurer), §6 фаза 4.
**Вход:** задача 2 (ValkeyPki) закоммичена.
**Файлы:** Modify `src/ValkeyWorker.Core/Model/ValkeyDomain.cs`; Modify `src/ValkeyWorker.Etcd/Parsing/ValkeySnapshotParser.cs`; Modify `src/ValkeyWorker.Provisioning/Processes/ClusterSecretEnsurer.cs`; Test `src/tests/ValkeyWorker.UnitTests/Etcd/ValkeySnapshotParserTests.cs`, `src/tests/ValkeyWorker.UnitTests/Provisioning/ClusterSecretEnsurerTests.cs`.
**Интерфейсы (Produces):**
- `ValkeyClusterSnapshot` + поля `string? CaPem, string? CaKey` (позиционно после `AdminPassword`, до `UnknownKeys`).
- `ValkeyCredentials(string AdminPassword, string AppPassword, string CaPem, string CaKey)` — расширение существующего рекорда (spec §4.4: «возвращает креды + CA»).

- [ ] **Шаг 3.1. Тесты парсера (падающие).** В `ValkeySnapshotParserTests.cs` добавить (формат существующих кейсов файла сохранить):

```csharp
[Fact]
public void Parse_CaPemAndCaKey_Populated()
{
    // Arrange — ключи CA в префиксе кластера (валидный PEM от ValkeyPki)
    var (caPem, caKeyPem) = ValkeyPki.GenerateCa("c1");
    var kvs = new[]
    {
        Kv("/valkey/clusters/c1/config", """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"allkeys-lru","created_unix":1}"""),
        Kv("/valkey/clusters/c1/ca_pem", caPem),
        Kv("/valkey/clusters/c1/ca_key", caKeyPem),
    };
    // Act
    var parsed = ValkeySnapshotParser.Parse(kvs);
    // Assert
    var cluster = Assert.Single(parsed.Value.Clusters);
    Assert.Equal(caPem, cluster.CaPem);
    Assert.Equal(caKeyPem, cluster.CaKey);
}

[Fact]
public void Parse_BrokenCaPem_ParseErrorClusterSurvives()
{
    // Arrange — битый PEM не роняет парсер: parseError, кластер жив (arch/20 §5)
    var kvs = new[]
    {
        Kv("/valkey/clusters/c1/config", """{"nodes":1,"maxmemory_bytes":1,"maxmemory_policy":"allkeys-lru","created_unix":1}"""),
        Kv("/valkey/clusters/c1/ca_pem", "garbage"),
        Kv("/valkey/clusters/c1/ca_key", "-----BEGIN PRIVATE KEY-----\nZ2FyYmFnZQ==\n-----END PRIVATE KEY-----\n"),
    };
    // Act
    var parsed = ValkeySnapshotParser.Parse(kvs);
    // Assert — по ошибке на каждый битый ключ; кластер в снапшоте
    Assert.Equal(2, parsed.Value.Clusters.Single().ParseErrors.Count);
    Assert.Single(parsed.Value.Clusters);
}
```

(`Kv(key, value)` — существующий хелпер теста; если его нет — построить `Kv` напрямую.)

- [ ] **Шаг 3.2. Тесты ensurer'а (падающие).** В `ClusterSecretEnsurerTests.cs` добавить кейс пор образцу kafka `src/tests/KafkaWorker.UnitTests` (`ClusterSecretEnsurerTests` kafka): пустой etcd → `EnsureAsync` → txn с 6 put-if-absent (4 креды + ca_pem + ca_key), одна txn; повторный `EnsureAsync` → без записи; txn проигран (гонка — ключи появились) → re-read отдаёт существующие. Проверить точные имена моков существующего файла и дополнить:

```csharp
[Fact]
public async Task Ensure_CreatesSixKeysInSingleTxn()
{
    // Arrange — пустой кластер; мок etcd (существующий FakeEtcdGateway теста)
    // Act
    var ensured = await ensurer.EnsureAsync("c1", ct);
    // Assert — ca_pem/ca_key созданы той же txn, что и креды; PEM валиден
    Assert.True(ensured.IsSuccess);
    Assert.True(ValkeyPki.TryParseCertificate(ensured.Value.CaPem, out _));
    Assert.True(ValkeyPki.TryParseRsaKey(ensured.Value.CaKey, out _));
    // txn ровно одна, в ней 6 put-if-absent
}
```

- [ ] **Шаг 3.3. Прогнать — падают** (`--filter "FullyQualifiedName~ValkeySnapshotParser|FullyQualifiedName~ClusterSecretEnsurer"`).

- [ ] **Шаг 3.4. Реализация.**
  - `ValkeyDomain.cs` — `ValkeyClusterSnapshot`: вставить `string? CaPem, string? CaKey` после `AdminPassword`; обновить XML-doc («CA-ключи t06: неполный набор → null-поля»).
  - `ValkeySnapshotParser.cs`: в `ClusterAcc` + `string? CaPem, CaKey`; в `switch (segments[4])`:

```csharp
case "ca_pem" when segments.Length == 5:
    acc.CaPem = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
    break;

case "ca_key" when segments.Length == 5:
    acc.CaKey = string.IsNullOrWhiteSpace(kv.Value) ? null : kv.Value.Trim();
    break;
```

    После цикла — валидация PEM (битый → parseError, поле в null — кластер жив, arch/20 §5): в `BuildCluster` или отдельным проходом по acc — при `CaPem is not null && !ValkeyPki.TryParseCertificate(CaPem, out _)` → `Errors.Add($"/valkey/clusters/{acc.Name}/ca_pem: битый PEM")` и `CaPem = null`; аналогично `ca_key` (`TryParseRsaKey`). Проект `ValkeyWorker.Etcd` уже ссылается на `ValkeyWorker.Core` (модель) — ValkeyPki доступен.
  - `ClusterSecretEnsurer.cs` — порт kafka-версии (она уже написана для 6 ключей — `src/KafkaWorker.Provisioning/Processes/ClusterSecretEnsurer.cs`, сверить при исполнении): `RawSecrets` + `CaPem, CaKey`; `ValkeyCredentials` + `CaPem, CaKey`; `MissingKeys`: `if (current?.CaPem is null || current?.CaKey is null) { var (caPem, caKeyPem) = ValkeyPki.GenerateCa(cluster); ... }` — каждая пара из ОДНОЙ генерации (как kafka); чтение 6 ключей; сообщение о неполном наборе дополнить `ca_pem`/`ca_key`.

- [ ] **Шаг 3.5. Прогнать — зелёные** (та же команда) + весь юнит-проект: `dotnet test src/tests/ValkeyWorker.UnitTests -c Debug` (поймать потребителей `ValkeyClusterSnapshot`/`ValkeyCredentials` с позиционными конструкторами — TreatWarningsAsErrors подсветит все места; обновить конструкторы-вызовы добавлением двух полей).
- [ ] **Шаг 3.6. Коммит:** `feat(valkey): контракт ca_pem/ca_key — снапшот, парсер (битый PEM → parseError), ensure шести секретов одной txn`.

**Выход:** etcd-контракт CA читается/пишется кодом воркера; канон §2.1 отражён.
**Проверка задачи:** юниты парсера/ensurer'а зелёные; сборка решения чистая.

---

## Задача 4. Docker-механика: volume + tar + mount; `NodeArgsBuilder` TLS

> **Примечание исполнения (2026-09-20, решение пользователя):** транспорт
> `Put/GetVolumeArchiveAsync` реализован через короткоживущий helper-контейнер
> (volume-archive API локальных томов недоступен без swarm — 503/404 на
> Docker 29.8; детали — примечание в spec §1.1). Сигнатуры расширены
> параметром `image` (образ helper'а = образ ноды, прокидывается из
> `NodeTlsProvisioner(driver, nodeImage)`); права файлов — из заголовка tar,
> ключ ноды `0644` (процесс valkey в образе не root).

**Spec:** §4.2 (драйверы), §4.4 (NodeArgsBuilder), §1 п.4 (volume-доставка), §6 фаза 3.
**Вход:** задача 3 закоммичена.
**Файлы:** Modify `src/ValkeyWorker.Docker/Engine/{IDockerEngine,DockerEngine}.cs`; Create `src/ValkeyWorker.Docker/Engine/TarArchive.cs`; Modify `src/ValkeyWorker.Docker/Drivers/ClusterDriver.cs`; Modify `src/ValkeyWorker.Provisioning/Processes/NodeArgsBuilder.cs`; Test `src/tests/ValkeyWorker.UnitTests/Docker/TarArchiveTests.cs` (новый каталог), `src/tests/ValkeyWorker.UnitTests/Provisioning/NodeArgsBuilderTests.cs`.
**Интерфейсы (Produces):** используются задачами 6–7.
- `IDockerEngine`: `Task<Result> EnsureVolumeAsync(string name, CancellationToken ct)`; `Task<Result> PutVolumeArchiveAsync(string name, byte[] tar, CancellationToken ct)`; `Task<Result<byte[]?>> GetVolumeArchiveAsync(string name, CancellationToken ct)` (null = volume нет); `Task<Result> DeleteVolumeAsync(string name, CancellationToken ct)` (404 = успех).
- `ContainerSpec` + `IReadOnlyList<string>? Binds` (формат `"volume:/path"`); swarm-тело маппит в `Mounts`.
- `ValkeyNodeSpec` + `string? TlsVolume = null`.
- `IClusterDriver`: `Task<Result> EnsureTlsVolumeAsync(string cluster, string host, CancellationToken ct)`; `Task<Result> PutTlsArchiveAsync(string cluster, string host, byte[] tar, CancellationToken ct)`; `Task<Result<byte[]?>> GetTlsArchiveAsync(string cluster, string host, CancellationToken ct)`; `Task<Result> RemoveTlsVolumeAsync(string cluster, CancellationToken ct)` (plain — все хоста, swarm — manager).
- `NodeArgsBuilder.Build(...)` — без изменений сигнатуры; массив пополняется TLS-флагами.

- [ ] **Шаг 4.1. Тест TarArchive (падающий).**

```csharp
namespace ValkeyWorker.UnitTests.Docker;

// ustar-писатель/читатель для Docker volume-archive API (spec §4.2):
// round-trip имя→данные, права ключа 0600 сохраняются в заголовке.
public sealed class TarArchiveTests
{
    [Fact]
    public void Build_Read_RoundTrip()
    {
        // Arrange
        var entries = new[]
        {
            new TarArchive.Entry("node.key", 0b1_100_000, Encoding.UTF8.GetBytes("key-data")),
            new TarArchive.Entry("ca.pem", 0b1_101_100, Encoding.UTF8.GetBytes("ca-data")),
        };
        // Act
        var tar = TarArchive.Build(entries);
        var read = TarArchive.Read(tar);
        // Assert
        Assert.Equal("key-data", Encoding.UTF8.GetString(read["node.key"]));
        Assert.Equal("ca-data", Encoding.UTF8.GetString(read["ca.pem"]));
    }
}
```

- [ ] **Шаг 4.2. Реализация TarArchive** (`src/ValkeyWorker.Docker/Engine/TarArchive.cs`):

```csharp
using System.Text;

namespace ValkeyWorker.Docker.Engine;

/// <summary>
/// Минимальный ustar-тар (t06): запись/чтение набора файлов для Docker
/// volume-archive API (PUT/GET /volumes/&lt;name&gt;/archive). Только то, что
/// нужно TLS-сертам: короткие имена, малые размеры, без pax-расширений.
/// </summary>
public static class TarArchive
{
    public sealed record Entry(string Name, int Mode, byte[] Data);

    private const int Block = 512;

    public static byte[] Build(IReadOnlyList<Entry> entries)
    {
        using var ms = new MemoryStream();
        foreach (var e in entries)
        {
            var header = new byte[Block];
            WriteString(header, 0, 100, e.Name);
            WriteOctal(header, 100, 8, e.Mode);          // mode (0600/0644)
            WriteOctal(header, 108, 8, 0);               // uid
            WriteOctal(header, 116, 8, 0);               // gid
            WriteOctal(header, 124, 12, e.Data.Length);  // size
            WriteOctal(header, 136, 12, 0);              // mtime
            header[156] = (byte)'0';                     // typeflag: обычный файл
            Encoding.ASCII.GetBytes("ustar\0").CopyTo(header, 257);
            Encoding.ASCII.GetBytes("00").CopyTo(header, 263);
            // checksum: поле заполняется пробелами, сумма байтов — octal
            for (var i = 148; i < 156; i++) header[i] = (byte)' ';
            var sum = header.Sum(b => b);
            WriteOctal(header, 148, 7, sum);
            header[155] = 0;
            ms.Write(header);
            ms.Write(e.Data);
            var pad = (Block - e.Data.Length % Block) % Block;
            ms.Write(new byte[pad]);
        }
        ms.Write(new byte[Block * 2]); // завершающие нулевые блоки
        return ms.ToArray();
    }

    public static IReadOnlyDictionary<string, byte[]> Read(byte[] tar)
    {
        var result = new Dictionary<string, byte[]>();
        var offset = 0;
        while (offset + Block <= tar.Length)
        {
            if (tar.AsSpan(offset, Block).ToArray().All(b => b == 0))
                break; // терминатор
            var name = ReadString(tar, offset, 100);
            var sizeField = ReadString(tar, offset + 124, 12).TrimEnd('\0', ' ');
            var size = Convert.ToInt32(sizeField, 8);
            if (name.Length == 0 || sizeField.Length == 0)
                throw new ApplicationException($"tar: некорректный заголовок на смещении {offset}");
            var data = tar.AsSpan(offset + Block, size).ToArray();
            result[name] = data;
            offset += Block + size + (Block - size % Block) % Block;
        }
        return result;
    }

    private static void WriteString(byte[] header, int at, int len, string value)
    {
        var bytes = Encoding.ASCII.GetBytes(value);
        if (bytes.Length > len - 1)
            throw new ArgumentException($"tar-поле длиной {len} не вместило '{value}'");
        bytes.CopyTo(header, at);
    }

    private static void WriteOctal(byte[] header, int at, int len, long value)
    {
        var octal = Convert.ToString(value, 8).PadLeft(len - 1, '0');
        Encoding.ASCII.GetBytes(octal).CopyTo(header, at);
        header[at + len - 1] = 0;
    }

    private static string ReadString(byte[] source, int at, int len)
    {
        var end = at;
        while (end < at + len && source[end] != 0) end++;
        return Encoding.ASCII.GetString(source, at, end - at);
    }
}
```

- [ ] **Шаг 4.3. Тест NodeArgsBuilder (падающий).** В `NodeArgsBuilderTests.cs`:

```csharp
[Fact]
public void Build_IncludesCanonicalTlsFlags()
{
    // Arrange / Act
    var args = NodeArgsBuilder.Build(536870912, "allkeys-lru", "adm", "app");
    // Assert — канон arch/21 §2: TLS-порт = контейнерный 6379, plain закрыт,
    // серты из /tls, клиенты без сертов (ACL)
    Assert.Contains("--tls-port", args);
    Assert.Equal("6379", args[Array.IndexOf(args, "--tls-port") + 1]);
    Assert.Contains("--port", args);
    Assert.Equal("0", args[Array.IndexOf(args, "--port") + 1]);
    Assert.Equal(new[]
    {
        "--tls-port", "6379", "--port", "0",
        "--tls-cert-file", "/tls/node.crt",
        "--tls-key-file", "/tls/node.key",
        "--tls-ca-cert-file", "/tls/ca.pem",
        "--tls-auth-clients", "no",
        "--tls-replication", "no",
    }, args.Skip(args.Length - 14).ToArray());
}
```

- [ ] **Шаг 4.4. Реализация NodeArgsBuilder** — TLS-блок в конец массива `Build` (значения — канонические константы, настройками не вынесены, spec §4.5):

```csharp
            "--save", "",
            "--appendonly", "no",
            // TLS (t06, arch/21 §2): тот же клиентский порт portalloc (контейнерный
            // 6379 слушает TLS), plain закрыт; серты — /tls (volume vwk-<C>-tls);
            // клиенты без сертификатов — принципалы из ACL.
            "--tls-port", "6379",
            "--port", "0",
            "--tls-cert-file", "/tls/node.crt",
            "--tls-key-file", "/tls/node.key",
            "--tls-ca-cert-file", "/tls/ca.pem",
            "--tls-auth-clients", "no",
            "--tls-replication", "no",
```

- [ ] **Шаг 4.5. Реализация движка и драйверов.**
  - `IDockerEngine` + 4 метода volume (сигнатуры выше; XML-комментарии — идемпотентность create 409, delete 404).
  - `DockerEngine`: `EnsureVolumeAsync` → `POST /volumes/create` body `{"Name":name}` (ловить 409 — уже есть); `PutVolumeArchiveAsync` → `PUT /volumes/{name}/archive?path=/` с `ByteArrayContent(tar)` media-type `application/x-tar`; `GetVolumeArchiveAsync` → `GET /volumes/{name}/archive?path=/` как `byte[]` (404 → null); `DeleteVolumeAsync` → `DELETE /volumes/{name}` (404 = успех). Для байтовых тел добавить приватные `SendBytesAsync`/`GetBytesAsync` рядом с `SendAsync`/`GetAsync` (не трогать существующие).
  - `ContainerSpec` + `IReadOnlyList<string>? Binds`; `BuildContainerBody`: `if (spec.Binds is { Count: > 0 }) hostConfig["Binds"] = spec.Binds.ToArray();`; `BuildServiceBody`: Binds → `container["Mounts"] = spec.Template.Binds.Select(b => new { Type = "volume", Source = b[..b.IndexOf(':')], Target = b[(b.IndexOf(':') + 1)..] })`.
  - `ClusterDriver.cs`:
    - `ValkeyNodeSpec` + `string? TlsVolume = null`; в `PlainClusterDriver.EnsureNodeAsync`/`SwarmClusterDriver.EnsureNodeAsync` `ContainerSpec` прокидывается автоматически (поле в ContainerSpec).
    - Константа `public static string TlsVolumeName(string cluster) => $"vwk-{cluster}-tls";` (на уровне файла драйверов, `PlainClusterDriver`).
    - `IClusterDriver` + 4 метода; `PlainClusterDriver`: engine по `host` из `_engines` (как `EnsureNodeAsync`; отсутствующий хост → Failed тем же текстом); `RemoveTlsVolumeAsync` — перебор ВСЕХ engines (404 = успех на каждом); `SwarmClusterDriver`: host игнорируется, всё через manager `_engine`.
    - Комментарий-инвариант в `RemoveNodeAsync`: «TLS-volume НЕ удаляется — переживает пересоздания контейнера (arch/21 §2); демонтаж — RemoveTlsVolumeAsync в X1».
  - `DeprovisioningProcess.cs` (X1): после цикла `RemoveNodeAsync` добавить:

```csharp
        // X1: TLS-volume кластера (t06): контейнеры снесены — том больше не нужен.
        var volumeRemoved = await driver.RemoveTlsVolumeAsync(cluster, ct);
        if (!volumeRemoved.IsSuccess)
            return Fail(cluster, volumeRemoved.Error!, "remove-tls-volume");
```

- [ ] **Шаг 4.6. Прогнать — зелёные:** `dotnet test src/tests/ValkeyWorker.UnitTests -c Debug` (TarArchive, NodeArgsBuilder + регресс всей сборки; юниты депровижининга обновить: мок-драйвер в Fakes получает 4 метода — заглушки `Result.Success()`).
- [ ] **Шаг 4.7. Коммит:** `feat(valkey): TLS-volume vwk-<C>-tls — tar-API движка, mount /tls, TLS-args ноды, X1 чистит volume`.

**Выход:** драйвер умеет доставлять серты в контейнер и чистить их при демонтаже; канонические TLS-args собираются.
**Проверка задачи:** юниты зелёные; сборка чистая.

---

## Задача 5. Транспорт воркера: `ValkeyConnection` TLS

**Spec:** §4.1 (ValkeyConnection), §6 фаза 5.
**Вход:** задачи 2–4 закоммичены.
**Файлы:** Modify `src/ValkeyWorker.Core/Valkey/IValkeyConnection.cs`; Modify `src/ValkeyWorker.Core/Valkey/ValkeyConnection.cs`; Modify `src/ValkeyWorker.Core/ValkeyWorker.Core.csproj` (+`ProjectReference` на `Shared.Tls`); Test `src/tests/ValkeyWorker.UnitTests/Core/ValkeyConnectionTests.cs` + новый хелпер `src/tests/ValkeyWorker.UnitTests/Core/TlsTestServer.cs`.
**Интерфейсы (Produces):**
- `public sealed record ValkeyEndpoint(string Host, int Port, string User, string Password, string CaPem)` — CaPem 5-й, ОБЯЗАТЕЛЬНЫЙ (plain-ветка удаляется; все потребители — V4, надзор, converger, rotator — ходят по TLS одним транспортом).

- [ ] **Шаг 5.1. Тест-хелпер фейк-SslStream-сервера.** `TlsTestServer.cs`: `TcpListener` на `127.0.0.1:0` (динамический порт, `Port`-свойство), `SslStream` поверх принятого сокета с серверным сертом, подписанным тестовым CA (мини-генератор RSA-2048 в том же файле — как `IntegrationTests/Api/TestPki.cs`, но в юнитах своя копия); на кадр `AUTH …` отвечает `+OK`, на `PING` — `+PONG`. API: `static async Task<TlsTestServer> StartAsync(string certPem, string keyPem)` + обработка одного соединения на запрос (`HandleOneAsync()`). Существующий `RespStub.cs` — источник RESP-логики; переиспользовать его формат кадров.

- [ ] **Шаг 5.2. Тесты (падающие).** В `ValkeyConnectionTests.cs`:

```csharp
[Fact]
public async Task Ping_TlsTrustedCa_Pong()
{
    // Arrange — фейк-сервер с сертом от CA1; клиент доверяет CA1
    using var server = await TlsTestServer.StartAsync(certOfCa1);
    var ep = new ValkeyEndpoint("127.0.0.1", server.Port, "admin", "pw", ca1Pem);
    // Act
    var ping = await new ValkeyConnection().PingAsync(ep, TestContext.Current.CancellationToken);
    // Assert
    Assert.True(ping.IsSuccess, ping.Error?.Message);
}

[Fact]
public async Task Ping_TlsForeignCa_Failed()
{
    // Arrange — серт сервера от CA2, клиент доверяет CA1 (чужой якорь)
    using var server = await TlsTestServer.StartAsync(certOfCa2);
    var ep = new ValkeyEndpoint("127.0.0.1", server.Port, "admin", "pw", ca1Pem);
    // Act
    var ping = await new ValkeyConnection().PingAsync(ep, TestContext.Current.CancellationToken);
    // Assert — аутентификация сервера отвергнута (не доверяем)
    Assert.False(ping.IsSuccess);
}

[Fact]
public async Task Ping_TlsSanMismatch_Failed()
{
    // Arrange — SAN серта "other.host", endpoint-хост 127.0.0.1
    // Act / Assert — сверка SAN против endpoint-хоста отклоняет
}
```

- [ ] **Шаг 5.3. Прогнать — падают** (не компилируется: `ValkeyEndpoint` без CaPem).
- [ ] **Шаг 5.4. Реализация.**
  - `ValkeyWorker.Core.csproj`: `<ProjectReference Include="..\Shared.Tls\Shared.Tls.csproj"/>`.
  - `IValkeyConnection.cs`: `ValkeyEndpoint` + обязательный `string CaPem`; комментарий: «CA per-cluster (t06): PEM якоря для TLS-валидации сервера».
  - `ValkeyConnection.cs`, `ExecuteAsync` — между `ConnectAsync` и AUTH:

```csharp
    // TLS (t06, arch/21 §6): доверие — ТОЛЬКО per-cluster CA; системные
    // якоря не участвуют (SslPolicyErrors игнорируем — строим свою цепочку
    // CustomRootTrust) + SAN обязан покрывать хост endpoint'а.
    // Валидатор — ЗАМЫКАНИЕ на распарсенный CA и ep.Host (не поле класса:
    // соединение = один endpoint).
    if (!ValkeyPki.TryParseCertificate(ep.CaPem, out var ca) || ca is null)
        return Result<T>.Failed(new ApplicationException(
            $"valkey {ep.Host}:{ep.Port}: ca_pem — невалидный PEM ({command[0]})"));
    using (ca)
    {
        bool ValidateServerCertificate(object _, X509Certificate? certificate, X509Chain? _, SslPolicyErrors _)
            => certificate is not null
               && Shared.Tls.TlsChain.ValidateChain(certificate, ca) // CustomRootTrust + NoCheck
               && SanMatchesHost(certificate, ep.Host);

        using var ssl = new SslStream(client.GetStream(), false, ValidateServerCertificate);
        await ssl.AuthenticateAsClientAsync(ep.Host, clientCertificates: null,
            enabledSslProtocols: System.Security.Authentication.SslProtocols.None,
            checkCertificateRevocation: false, timeout.Token);
        using var stream = new BufferedStream(ssl, 8192);
        // … AUTH + команда без изменений (stream)
    }
```

    (старую строку `using var stream = new BufferedStream(client.GetStream(), 8192);` заменить; AUTH/команда/проекции — без изменений, работают поверх `stream`). `SanMatchesHost(X509Certificate certificate, string host)` — принимает `X509Certificate` и при надобности конвертирует в `X509Certificate2` (паттерн `TlsChain`):

```csharp
    // SAN-хост (arch/21 §2): advertised DNS либо IP; ровно один SAN у сертов
    // домена, но сверяем весь список — отказ при отсутствии покрытия.
    private static bool SanMatchesHost(X509Certificate certificate, string host)
    {
        var cert = certificate as X509Certificate2 ?? new X509Certificate2(certificate);
        using var _ = cert;
        var san = cert.Extensions.OfType<X509SubjectAltNameExtension>().FirstOrDefault();
        if (san is null)
            return false;
        if (System.Net.IPAddress.TryParse(host, out var ip))
            return san.EnumerateIPAddresses().Contains(ip);
        return san.EnumerateDnsNames().Contains(host, StringComparer.OrdinalIgnoreCase);
    }
```

    Ошибки аутентификации: `AuthenticationException` из `AuthenticateAsClientAsync` при отвергнутом callback → уйдёт в существующий catch `Exception` → `Result.Failed` (сообщение дополнить «TLS: » контекстом — обернуть в catch `System.Security.Authentication.AuthenticationException ex` отдельной веткой с тем же Failed-паттерном).
  - Обновить ВСЕХ потребителей `ValkeyEndpoint` (компилятор покажет): `ProvisioningProcess.AwaitBootAsync`, `NodeSupervisor.TickAsync`, `ConfigConverger`, `PasswordRotator` — 5-м аргументом передать CA из снапшота (`snap.CaPem` после ensure; для процессов, где CA ещё нет — задача 6/7 доставит). Временная склейка этой задачи: передавать `snap.CaPem ?? ""` НЕ допускается — вместо этого в этой задаче обновить только компилируемые места вызовов тестов/Fakes, а производственные вызовы процессов — в задаче 6 (порядок: задача 5 завершается зелёной сборкой юнитов Core; интеграционные вызовы процессов правятся в 6).

- [ ] **Шаг 5.5. Прогнать — зелёные:** `dotnet test src/tests/ValkeyWorker.UnitTests -c Debug --filter FullyQualifiedName~ValkeyConnection`; сборка решения чистая (если процессы ещё не пропатчены и не собираются — патч их конструктор-вызовов перенести сюда минимально: передача `snap.CaPem!` с guard `CaPem is null → Failed("нет ca_pem — TLS невозможен")` в начале `TickAsync`).
- [ ] **Шаг 5.6. Коммит:** `feat(valkey): ValkeyConnection — TLS-транспорт (SslStream + CustomRootTrust ca_pem + SAN-хост)"`.

**Выход:** единый TLS-транспорт RESP-клиента воркера; фейк-сервер для юнитов.
**Проверка задачи:** юниты ValkeyConnection (доверие/чужой CA/SAN) зелёные.

---

## Задача 6. Процессы: provisioning V2/V3/V4, `NodeTlsProvisioner`, надзор

**Spec:** §4.4 (ProvisioningProcess, NodeSupervisor, NodeTlsProvisioner — новый), §6 фаза 6.
**Вход:** задачи 3–5 закоммичены.
**Файлы:** Create `src/ValkeyWorker.Provisioning/Processes/NodeTlsProvisioner.cs`; Modify `src/ValkeyWorker.Provisioning/Processes/ProvisioningProcess.cs`, `NodeSupervisor.cs`; Modify `src/ValkeyWorker.App/Program.cs` (DI); Test `src/tests/ValkeyWorker.UnitTests/Provisioning/NodeTlsProvisionerTests.cs`, `ProvisioningProcessTests.cs`, `NodeSupervisorTests.cs`, `Fakes/Fakes.cs` (мок-драйвер + volume-методы).
**Интерфейсы (Produces):** используются задачей 7 (TlsMigrator) и надзором.

- [ ] **Шаг 6.1. `NodeTlsProvisioner`** — ensure volume + валидного серта:

```csharp
using System.Security.Cryptography.X509Certificates;
using Shared.Core;
using ValkeyWorker.Core.Valkey;
using ValkeyWorker.Docker.Drivers;

namespace ValkeyWorker.Provisioning.Processes;

/// <summary>
/// Ensure TLS-материала ноды (t06, arch/21 §2/V3): named volume
/// vwk-&lt;C&gt;-tls с node.crt/node.key/ca.pem. Валидность = ca.pem совпадает с
/// текущим CA кластера, node.crt подписан этим CA, SAN покрывает advertised-
/// хост, NotAfter в будущем → переиспользование; иначе — перевыпуск
/// (IssueNodeCertificate) и запись tar поверх. PING по TLS в процессах —
/// финальный критерий (spec §5). Вызывается ДО EnsureNodeAsync (файлы сертов
/// обязаны быть в volume к старту контейнера).
/// </summary>
public sealed class NodeTlsProvisioner(
    IClusterDriver driver,
    TimeProvider? clock = null)
{
    public const string MountPath = "/tls";

    private readonly TimeProvider _clock = clock ?? TimeProvider.System;

    public static string VolumeName(string cluster) => $"vwk-{cluster}-tls";

    public async Task<Result> EnsureNodeTlsAsync(
        string cluster, string node, string host, string advertisedHost,
        string caPem, string caKeyPem, CancellationToken ct)
    {
        var volume = await driver.EnsureTlsVolumeAsync(cluster, host, ct);
        if (!volume.IsSuccess)
            return volume;

        var existing = await driver.GetTlsArchiveAsync(cluster, host, ct);
        if (!existing.IsSuccess)
            return existing;
        if (existing.Value is { } tar && IsValidTar(tar, advertisedHost, caPem, _clock))
            return Result.Success(); // валидный серт уже в volume — переиспользование

        var (certPem, keyPem) = ValkeyPki.IssueNodeCertificate(caPem, caKeyPem, node, advertisedHost);
        var entries = new[]
        {
            new TarArchive.Entry("node.crt", 0b1_101_100, System.Text.Encoding.UTF8.GetBytes(certPem)),
            new TarArchive.Entry("node.key", 0b1_100_000, System.Text.Encoding.UTF8.GetBytes(keyPem)), // 0600
            new TarArchive.Entry("ca.pem", 0b1_101_100, System.Text.Encoding.UTF8.GetBytes(caPem)),
        };
        return await driver.PutTlsArchiveAsync(cluster, host, TarArchive.Build(entries), ct);
    }

    // Валидность факта (идемпотентность по факту, spec §2.4): ca.pem == текущему
    // CA, серт подписан им, SAN покрывает advertised, срок жив.
    internal static bool IsValidTar(byte[] tar, string advertisedHost, string caPem, TimeProvider clock)
    {
        try
        {
            var files = TarArchive.Read(tar);
            if (!files.TryGetValue("ca.pem", out var caBytes)
                || !files.TryGetValue("node.crt", out var certBytes)
                || !files.TryGetValue("node.key", out _))
                return false;
            if (System.Text.Encoding.UTF8.GetString(caBytes).Trim() != caPem.Trim())
                return false; // чужой/старый CA — перевыпуск
            if (!ValkeyPki.TryParseCertificate(PemOf(certBytes), out var cert) || cert is null)
                return false;
            using var _ = cert;
            if (!Shared.Tls.TlsChain.ValidateChain(cert, ParseCa(caPem)))
                return false;
            if (cert.NotAfter < clock.GetUtcNow())
                return false;
            var san = cert.Extensions.OfType<X509SubjectAltNameExtension>().FirstOrDefault();
            if (san is null)
                return false;
            if (System.Net.IPAddress.TryParse(advertisedHost, out var ip))
                return san.EnumerateIPAddresses().Contains(ip);
            return san.EnumerateDnsNames()
                .Contains(advertisedHost, StringComparer.OrdinalIgnoreCase);
        }
        catch (Exception e) when (e is ArgumentException or FormatException)
        {
            return false; // битый tar/PEM — перевыпуск
        }
    }

    private static string PemOf(byte[] bytes) => System.Text.Encoding.UTF8.GetString(bytes);

    private static X509Certificate2 ParseCa(string caPem)
        => ValkeyPki.TryParseCertificate(caPem, out var ca) && ca is not null
            ? ca
            : throw new ArgumentException("ca_pem: невалидный PEM");
}
```

(Проект `ValkeyWorker.Provisioning` ссылается на `ValkeyWorker.Docker` — уже есть; на `Shared.Tls` — проверить, при отсутствии добавить `ProjectReference`.)

- [ ] **Шаг 6.2. Юнит-тесты NodeTlsProvisioner** (Fake-драйвер из Fakes с volume-хранилищем `Dictionary<(cluster, host), byte[]>`):
  - `Ensure_WritesVolumeTar` — пусто → PutTlsArchive с тремя файлами (assert содержимого tar: node.key права `0600` — читать заголовок).
  - `Ensure_ReusesValidCert` — второй вызов не пишет (PutTlsArchive не вызван).
  - `Ensure_ReissuesOnForeignCa` — в volume ca.pem чужого CA → перезапись.
  - `Ensure_ReissuesOnSanDrift` — advertised сменился → перезапись (SAN).

- [ ] **Шаг 6.3. `ProvisioningProcess` V2/V3/V4:**
  - V2: без изменений кода — `secrets.EnsureAsync` теперь возвращает и CA (задача 3).
  - V3 `EnsureContainersAsync`: после вычисления `args` и ДО сверки/`EnsureNodeAsync`:

```csharp
            var advertised = options.AdvertisedClientHost ?? address.Host;
            var tls = await tlsProvisioner.EnsureNodeTlsAsync(
                cluster, node, address.Host, advertised, creds.CaPem, creds.CaKey, ct);
            if (!tls.IsSuccess)
                return tls;
```

    `EnsureNodeAsync` — spec с `TlsVolume: PlainClusterDriver.TlsVolumeName(cluster)`; сверка re-run уже включает TLS-args (args из Build). Конструктор `ProvisioningProcess` + параметр `NodeTlsProvisioner tlsProvisioner` (после `secrets`).
  - V4 `AwaitBootAsync`: `new ValkeyEndpoint(advertised, address.ClientPort, "admin", creds.AdminPassword, creds.CaPem)`.
  - Guard в начале `TickAsync` не нужен (Provision = NOT_INITIALIZED, ensure сам создаёт CA).

- [ ] **Шаг 6.4. `NodeSupervisor`:** `RecreateAsync` — перед `EnsureNodeAsync` тот же блок `EnsureNodeTlsAsync` (аргументы: `snap.AdminPassword`-паттерн сохраняется; CA из снапшота: `snap.CaPem`/`snap.CaKey`; при `snap.CaPem is null || snap.CaKey is null` → Recreate невозможен: записать supervision-warning «нет CA в etcd — пересоздание отложено (миграция T)» и вернуть Success — миграция доиграет, надзор не создаёт ноду без сертов); PING-проба: `new ValkeyEndpoint(..., snap.CaPem!)` — при null PING-блок пропускается аналогично отсутствию кредов (запись supervision «нет ca_pem — пробы пропущены»). `EnsureNodeAsync` — spec с `TlsVolume`. Конструктор + `NodeTlsProvisioner tlsProvisioner`.

- [ ] **Шаг 6.5. `Program.cs`:** регистрация `builder.Services.AddSingleton<NodeTlsProvisioner>();` и передача в `ProvisioningProcess`/`NodeSupervisor`-фабрики.

- [ ] **Шаг 6.6. Обновить юниты процессов** (`ProvisioningProcessTests`, `NodeSupervisorTests`, `ConfigConvergerTests`, `PasswordRotatorTests`): риги получают CA (fixture-генерация `ValkeyPki.GenerateCa`), Fake-драйвер с volume-хранилищем; PING-моки отвечают на TLS-эндпоинты (мок `IValkeyConnection` — CA не проверяет, но сигнатура требует CaPem — передать из рига). Новые кейсы:
  - `Provision_V3_WritesTlsVolumeAndStartsContainer` (Fake-драйвер: volume записан, spec.TlsVolume == `vwk-<C>-tls`, args содержат `--tls-port`).
  - `Supervise_Recreate_ReusesVolumeAndTlsArgs` (снос → пересоздание: volume жив — второй `IssueNodeCertificate` не происходит, args TLS).
  - `Supervise_NoCaInEtcd_RecreationDeferred` (warning, без пересоздания).

- [ ] **Шаг 6.7. Прогнать:** `dotnet test src/tests/ValkeyWorker.UnitTests -c Debug` — зелёные.
- [ ] **Шаг 6.8. Коммит:** `feat(valkey): provisioning V3/V4 и надзор на TLS — NodeTlsProvisioner (volume+серт), PING по TLS`.

**Выход:** полный provisioning-путь поднимает TLS-кластер; пересоздания сохраняют канон.
**Проверка задачи:** юниты процессов зелёные; сборка чистая.

---

## Задача 7. `TlsMigrator` + Active-классификация

**Spec:** §3.2 (процесс T), §4.4 (TlsMigrator, Loops), §4.5, §6 фаза 6; образец — `src/KafkaWorker.Provisioning/Processes/SecurityMigrator.cs` (читать целиком при исполнении).
**Вход:** задача 6 закоммичена.
**Файлы:** Create `src/ValkeyWorker.Provisioning/Processes/TlsMigrator.cs`; Modify `src/ValkeyWorker.App/Loops/ValkeyClusterProcesses.cs`, `src/ValkeyWorker.App/Program.cs`; Test `src/tests/ValkeyWorker.UnitTests/Provisioning/TlsMigratorTests.cs`, `src/tests/ValkeyWorker.UnitTests/App/ValkeyClusterClassifierTests.cs` (если классификация меняется — нет, ветка та же Active; тесты процессов цикла).
**Интерфейсы (Produces):**
- `public sealed class TlsMigrator(IEtcdGateway gateway, string[] endpoints, IClusterDriver driver, ClaimStore claims, WorkJournal journal, IClusterSecretEnsurer secrets, NodeTlsProvisioner tlsProvisioner, IValkeyConnection valkey, ValkeyProvisioningOptions options, Func<CancellationToken, Task<Result>>? snapshot = null, TimeProvider? clock = null)`
- `public const string Op = "migrate-tls";`
- `public static bool NeedsMigration(ValkeyClusterSnapshot snap, IReadOnlyList<string>? liveNodeArgs)` — `snap.CaPem is null || snap.CaKey is null || liveNodeArgs?.Contains("--tls-port") != true`.
- `public async Task<Result<MigrationOutcome>> RunAsync(ValkeyClusterSnapshot snap, CancellationToken ct)`; `enum MigrationOutcome { NotNeeded, InProgress }`.

- [ ] **Шаг 7.1. Тесты (падающие).** `TlsMigratorTests.cs` (по образцу kafka SecurityMigrator-тестов; риг = Fake-драйвер + Fake-etcd + journal + Fake-`IValkeyConnection`):

```csharp
[Fact]
public void NeedsMigration_MissingKeys_True()
{
    // Arrange — Active-кластер без ca_pem/ca_key (премиграционный)
    // Act / Assert
    Assert.True(TlsMigrator.NeedsMigration(SnapWithoutCa(), liveArgs: TlsArgs()));
}

[Fact]
public void NeedsMigration_PlainContainerArgs_True()
{
    // Arrange — CA в etcd есть, контейнер без --tls-port (старый канон)
    // Act / Assert
    Assert.True(TlsMigrator.NeedsMigration(SnapWithCa(), liveArgs: PlainArgs()));
}

[Fact]
public void NeedsMigration_CanonicalCluster_False()
{
    // Arrange — CA есть, args TLS
    // Act / Assert
    Assert.False(TlsMigrator.NeedsMigration(SnapWithCa(), liveArgs: TlsArgs()));
}

[Fact]
public async Task Run_MigratesPlainCluster_PhasesT0T3()
{
    // Arrange — plain-контейнер жив (args без TLS), ca-ключей нет; snapshots-делегат пишет счётчик
    // Act
    var outcome = await migrator.RunAsync(snap, ct);
    // Assert — контейнер пересоздан с TLS-args ТЕМ ЖЕ портом; volume записан;
    // ca_pem/ca_key появились; journal: started → ensured-ca → recreated → done;
    // PING-мок позвался с TLS-эндпоинтом (CaPem передан); снапшоты «до»/«после» сняты
    Assert.Equal(TlsMigrator.MigrationOutcome.InProgress, outcome.Value);
}

[Fact]
public async Task Run_RerunOnMigratedCluster_Noop()
{
    // Arrange — канонический кластер (CA + TLS-args + journal done)
    // Act
    var outcome = await migrator.RunAsync(canonicalSnap, ct);
    // Assert — NotNeeded; драйвер не тронут (RemoveNode/EnsureNode не вызываны)
    Assert.Equal(TlsMigrator.MigrationOutcome.NotNeeded, outcome.Value);
}

[Fact]
public async Task Run_ClusterRemovedMidMigration_Aborts()
{
    // Arrange — config.state=TO_REMOVE появляется после T1 (мок перечитки)
    // Act / Assert — T2 не выполняется; journal aborted-state-changed; клэйм жив
}
```

- [ ] **Шаг 7.2. Прогнать — падают.**
- [ ] **Шаг 7.3. Реализация `TlsMigrator.cs`** (порт структуры `SecurityMigrator` с упрощениями nodes=1 — читать образец; скелет):

```csharp
public async Task<Result<MigrationOutcome>> RunAsync(ValkeyClusterSnapshot snap, CancellationToken ct)
{
    var cluster = snap.Cluster;
    // Мутации — только держателем живого клэйма (arch/21 §6).
    if (!claims.IsMine(cluster))
        return Result<MigrationOutcome>.Failed(new ApplicationException(
            $"migrate-tls {cluster}: клэйм не наш (или потерян) — мутации запрещены"));

    // Детект: etcd-ключи + args живого контейнера (перебор нод; nodes=1 — node1).
    var liveArgs = await ReadLiveNodeArgsAsync(cluster, snap, ct);   // best-effort
    var journalState = await journal.ReadAsync(cluster, ct);
    if (!journalState.IsSuccess) return Result<MigrationOutcome>.Failed(journalState.Error!);
    var done = !NeedsMigration(snap, liveArgs)
               && (journalState.Value is null || journalState.Value.Op != Op
                   || journalState.Value.Phase == PhaseDone);
    if (done) return Result<MigrationOutcome>.Success(MigrationOutcome.NotNeeded);

    // T0: journal-before-manipulations + снапшот «до».
    var started = await journal.WritePhaseAsync(cluster, Op, "started", claims.InstanceId, null, ct);
    if (!started.IsSuccess) return Result<MigrationOutcome>.Failed(started.Error!);
    if (snapshot is not null)
    {
        var before = await snapshot(ct);
        if (!before.IsSuccess)
            return await FailAsync(cluster, before.Error!, "snapshot-before", ct);
    }

    // T1: ensure CA (+ креды re-read той же txn).
    var ensured = await secrets.EnsureAsync(cluster, ct);
    if (!ensured.IsSuccess) return await FailAsync(cluster, ensured.Error!, "ensured-ca", ct);
    snap = snap with { CaPem = ensured.Value.CaPem, CaKey = ensured.Value.CaKey,
                       AdminPassword = snap.AdminPassword ?? ensured.Value.AdminPassword,
                       AppPassword = snap.AppPassword ?? ensured.Value.AppPassword };
    var ensuredPhase = await journal.WritePhaseAsync(cluster, Op, "ensured-ca", claims.InstanceId, null, ct);

    // Гонка TO_REMOVE посреди миграции — перечитывание перед T2 (образец V3).
    if (await ConfigRemovedAsync(cluster, ct))
        return await AbortAsync(cluster);

    // T2: пересоздание контейнера с каноническими TLS-args (серты в volume,
    // порт/лимиты те же — portalloc не меняется); уже-TLS контейнер не трогаем.
    // (адрес — portalloc; RecreateNodeAsync: RemoveNode → EnsureNodeTls → EnsureNode)

    // T3: PING по TLS → state=RUNNING; бюджет NodeBootSec (цикл 100 мс);
    // снапшот «после»; journal done.
    // journal done ⇒ следующий тик: NeedsMigration=false → NotNeeded.
}
```

  Полные детали реализации (T2/T3 по образцу `SecurityMigrator.M2/M3` + `ProvisioningProcess.AwaitBootAsync`; фазы journal: `started`/`ensured-ca`/`recreated`/`running`/`done`/`boot-timeout`/`aborted-state-changed`; `ValkeyEndpoint(advertised, port, "admin", ensured.Value.AdminPassword, ensured.Value.CaPem)`; `ProcessCommon.WriteNodeStateAsync(..., "RUNNING")`; journal-перезапись фаз — как в образце).
  Note: `RecreateNodeAsync` — приватный метод мигратора: `RemoveNodeAsync(cluster, node)` → `NodeTlsProvisioner.EnsureNodeTlsAsync(...)` → `EnsureNodeAsync(new ValkeyNodeSpec(..., args NodeArgsBuilder.Build(...), ..., TlsVolume: PlainClusterDriver.TlsVolumeName(cluster)))`; лимиты — `ProcessCommon.ParseResources(nodeSnap?.Resources)`; порт — из portalloc-записи (`NodeSupervisor.ResolveAddressAsync`-паттерн: чтение `/valkeyworker/portalloc/<C>`).

- [ ] **Шаг 7.4. `ValkeyClusterProcesses` Active-ветка** — миграция первым шагом (до надзора C):

```csharp
                case ValkeyClusterKind.Active:
                    // t06: миграция TLS — ПЕРВЫМ шагом Active-ветки (arch/21 §5):
                    // InProgress ⇒ надзор/converge/ротация в этом тике не идут
                    // (миграция доиграет тиками; узкое окно plain→TLS).
                    await RunClusterOpAsync(cluster, "active", async () =>
                    {
                        var migration = await tlsMigrator.RunAsync(snap, ct);
                        if (!migration.IsSuccess)
                            return migration.Error!;
                        if (migration.Value == TlsMigrator.MigrationOutcome.InProgress)
                            return Result.Success();

                        var supervised = await supervisor.TickAsync(snap, ct);
                        // ... далее без изменений (D при endpoints+admin-кред, E)
                    }, ct);
```

  Конструктор `ValkeyClusterProcesses` + `TlsMigrator tlsMigrator`; `Program.cs` — регистрация.

- [ ] **Шаг 7.5. Прогнать — зелёные:** `dotnet test src/tests/ValkeyWorker.UnitTests -c Debug` (вкл. новые кейсы цикла: Active-тик вызывает migrate-tls до supervise — мок-проверка порядка).
- [ ] **Шаг 7.6. Коммит:** `feat(valkey): TlsMigrator — авто-миграция plain→TLS (T0–T3) первым шагом Active-ветки`.

**Выход:** существующие кластеры мигрируют автоматически; повторный детект — no-op.
**Проверка задачи:** юниты TlsMigrator зелёные (детект/фазы/идемпотентность/TO_REMOVE).

---

## Задача 8. Панель: `ca_pem` в internal-стор, TLS-пробы, алерт

**Spec:** §3.3, §4.6, §6 фаза 7.
**Вход:** задача 7 закоммичена (панель-код независим от воркера, но канон §11 уже в arch — задача 1).
**Файлы:** Modify `src/AdminPanel.Etcd/ValkeySecretsStore.cs`; Modify `src/AdminPanel.Etcd/ValkeySnapshotRefresher.cs`; Modify `src/AdminPanel.Core/Valkey/ValkeySnapshot.cs` (модель `ValkeyClusterInfo`); Modify `src/AdminPanel.Etcd/Parsing/ValkeyParser.cs`; Modify `src/AdminPanel.Probes/Valkey/{IValkeyProbeClient,ValkeyConnection}.cs`; Modify `src/AdminPanel.Core/Valkey/ValkeyAlerting/ValkeyAlertEngine.cs`; Test `src/tests/AdminPanel.UnitTests/{ValkeyRefresherTests,ValkeyParserTests}.cs`, `src/tests/AdminPanel.UnitTests/ProbesValkey/ValkeyConnectionTests.cs`, `src/tests/AdminPanel.UnitTests/ValkeyAlertRulesTests.cs`.
**Интерфейсы (Produces):**
- `ValkeyClusterSecrets(string Cluster, string AdminUser, string AdminPassword, string? CaPem)`.
- `ValkeyProbeTarget(string Host, int Port, string AdminUser, string AdminPassword, string? CaPem = null)`.
- `ValkeyClusterInfo` + `bool HasCaPem` (bool-флаг в API допустим; сам PEM — никогда).

- [ ] **Шаг 8.1. Тесты (падающие):**
  - `ValkeyRefresherTests`: `ReadSecrets`/`RefreshOnce` — кластер с admin-парой + ca_pem → стор содержит CaPem; без ca_pem → запись стора с `CaPem = null` (частичный набор кредов — как раньше, без записи).
  - `ValkeyParserTests`: `ca_pem` → `ValkeyClusterInfo.HasCaPem = true`; отсутствие → false; в `unknownKeys` НЕ попадает.
  - `ValkeyConnectionTests` (ProbesValkey): TLS-фейк-сервер (копия `TlsTestServer` из задачи 5 — свой экземпляр в AdminPanel.UnitTests): `Ping_TlsTrustedCa_Pong`, `Ping_TlsForeignCa_Failed`, `Ping_NoCaPem_FailedWithMessage`.
  - `ValkeyAlertRulesTests`: Active-кластер `HasCaPem=false` → critical-алерт `valkey-security-missing`; с `HasCaPem=true` — алерта нет.

- [ ] **Шаг 8.2. Прогнать — падают.**
- [ ] **Шаг 8.3. Реализация:**
  - `ValkeySecretsStore.cs`: `ValkeyClusterSecrets` + `string? CaPem`; комментарий: «internal; наружу не отдаётся (арх/02 §11.1)».
  - `ValkeySnapshotRefresher.ReadSecrets`: case `"ca_pem"` → словарь `cas[cluster]`; сборка: `new ValkeyClusterSecrets(cluster, user, password, ca)` при полной кред-паре; ca без пары — запись не создаётся (пробы невозможны без кредов), толерантность как у кредов.
  - `ValkeyParser.cs`: case `"ca_pem"` → `acc.HasCaPem = true` (строку в модель не поднимаем); в списке «известных вне подмножества» убрать будущий `ca_pem` из unknown-обработки.
  - `ValkeyClusterInfo` (в `ValkeySnapshot.cs`): + `bool HasCaPem` (позиционно в конец, дефолт false для совместимости вызовов `with`).
  - `IValkeyProbeClient.cs`: `ValkeyProbeTarget` + `string? CaPem = null`.
  - `ValkeyConnection.cs` (probes): в `PingAsync` — `target.CaPem is null` → `Result.Failed(new ApplicationException($"valkey {target.Host}:{target.Port}: нет ca_pem — TLS-проба невозможна (миграция t06 не доиграна?)"))`; иначе `SslStream` + валидация — ПОРТ кода из задачи 5 (`TlsChain.ValidateChain` + SAN-хост; `ValkeyPki`-независимый парс `X509CertificateLoader` — панель не ссылается на ValkeyWorker.Core: свой мини-парсер PEM в файле коннекта, как в `Shared.Tls`; ссылка на Shared.Tls уже транзитивна через AdminPanel.Etcd). `AuthenticateAsClientAsync(target.Host, ...)`.
  - `ValkeyAlertEngine.Enumerate`: правило (по стилю существующих — severity Critical, `Remedy: AlertRemedy.OperatorRunbook`):

```csharp
        // valkey-security-missing (critical, t06): Active-кластер без ca_pem —
        // миграция TLS не доиграна либо ключ потерян (arch/20 §5).
        if (cluster.State == ValkeyClusterState.Active && !cluster.HasCaPem)
            yield return new Alert(
                "valkey-security-missing:" + cluster.Name,
                AlertSeverity.Critical,
                $"кластер {cluster.Name}: Active без ca_pem — TLS-канон не соблюдён",
                Remedy: AlertRemedy.OperatorRunbook, /* sinceUnix — механика файла */);
```

  (точный конструктор `Alert` и enum state — сверить с файлом при исполнении; id-стабильность — по образцу соседних правил.)
  - Проб-луп (`ValkeyProbeLoop.cs`): прокидывает `CaPem` из стора в target — проверить и обновить построение `ValkeyProbeTarget` (grep `new ValkeyProbeTarget`).

- [ ] **Шаг 8.4. Прогнать — зелёные:** `dotnet test src/tests/AdminPanel.UnitTests -c Debug`.
- [ ] **Шаг 8.5. Коммит:** `feat(adminpanel): valkey TLS — ca_pem internal-стор, SslStream-пробы, алерт valkey-security-missing`.

**Выход:** панель ходит по TLS и алертит отсутствие CA.
**Проверка задачи:** юниты панели зелёные; `grep -rn 'CaPem' src/AdminPanel.Api/` — PEM не покидает внутренний контур (bool HasCaPem в API — допустимо).

---

## Задача 9. Интеграционные тесты pg (docker)

**Spec:** §4.9 (интеграционные), §6 фаза 9; каноны AGENTS.md (teardown+чистота, динамические порты, NodeBootSec ≤ 100).
**Вход:** задачи 4–7 закоммичены (воркер полностью на TLS); docker запущен, образы вытянуты.
**Файлы:** Modify `src/tests/ValkeyWorker.IntegrationTests/Valkey/{ValkeyClusterFixture,RespProbe,ProvisioningTests,SupervisionTests,RotationTests,ConvergeTests,DeprovisioningTests,AclMatrixTests}.cs`; Create `src/tests/ValkeyWorker.IntegrationTests/Valkey/TlsMigrationTests.cs`.
**Вход-предусловие серии:** `docker ps -a | grep vwk-` — пусто; `docker volume ls -q | grep '^vwk-'` — пусто.

- [ ] **Шаг 9.1. `RespProbe` + TLS:** добавить в `RespProbe.cs`:

```csharp
    // TLS-проба (t06): SslStream с валидацией против ca_pem (CustomRootTrust +
    // SAN-хост) — тот же доверительный путь, что у воркера/панели.
    public static (bool Ok, string Error, string? Value) ExecuteTls(
        string host, int port, string user, string password, string caPem, params string[] command)
```

  (реализация — TcpClient → SslStream(валидатор пор шаблону задачи 5) → AUTH → команда; переиспользовать `ValkeyConnection.Resp` для кадров).

- [ ] **Шаг 9.2. Обновить риги существующих тестов** (Provisioning/Supervision/Rotation/Converge/Deprovisioning/AclMatrix): фикстура после provisioning читает `ca_pem` из etcd (`GetAsync(Endpoint, "/valkey/clusters/<C>/ca_pem")`); все пробы — `RespProbe.ExecuteTls`; asserts портов/args дополнить `--tls-port` (фактический host-порт из portalloc — как раньше, литералов нет).

- [ ] **Шаг 9.3. Новые кейсы.**
  - `ProvisioningTests`: `NewCluster_TlsOnly_PlainRejected` — (Arrange) заявка; (Act) тики до RUNNING; (Assert): 1) `ca_pem`/`ca_key` в etcd (PEM валиден `ValkeyPki.TryParseCertificate`); 2) `docker volume inspect vwk-<C>-tls` существует (через driver.GetTlsArchiveAsync — tar содержит три файла); 3) `RespProbe.ExecuteTls` с app-кредом: SET/GET roundtrip; 4) plain-подключение отклонено: `TcpClient` + сырой `PING` без TLS → исключение/таймаут (assert `!Ok`), 5) ACL-матрица прежняя (app без `+@all`: `CONFIG GET` → отказ).
  - `SupervisionTests`: `Recreate_KeepsTlsCanonical` — снести контейнер руками (`docker rm -f vwk-<C>-node1` через Driver.RemoveNodeAsync) → тик надзора → args содержат TLS-флаги, тот же host-порт, volume переиспользован (GetTlsArchive до/после — PEM узла идентичен), PING TLS ок.
  - `TlsMigrationTests.cs` (новый):
    - `PlainCluster_MigratesToTls`: (Arrange) симуляция премиграционного кластера: записать заявку как в ProvisioningTests, поднять контейнер СТАРЫМ каноном вручную — `driver.EnsureNodeAsync(new ValkeyNodeSpec(..., args: NodeArgsBuilder.Build(...) MINUS 14 TLS-элементов (собрать массив без TLS-хвоста), TlsVolume: null))`, endpoints/state проставить руками, ca-ключи НЕ записывать; (Act) тик `tlsMigrator.RunAsync` (или полный Active-тик через `ValkeyClusterProcesses`-уровень — как проще в риге: прямой вызов мигратора); (Assert): контейнер пересоздан (ID изменился), args = канонические TLS, host-порт ТОТ ЖЕ (portalloc не тронут), volume записан, `ca_pem` появился, PING по TLS admin-кредом, journal `work/<C>` содержит `op=migrate-tls` фазы до `done`; повторный `RunAsync` → `NotNeeded` (контейнер не тронут — ID неизменен).
    - `Deprovision_AfterMigration_CleansVolumeAndKeys`: демонтаж X0–X3 → (Assert) нет контейнера, НЕТ volume `vwk-<C>-tls` (docker volume ls через driver: GetTlsArchiveAsync → null/Failed), пустой префикс etcd.
  - `DeprovisioningTests`: дополнить существующий кейс ассертом отсутствия volume.
  - `RotationTests`/`ConvergeTests`: пробы по TLS (риг из 9.2) — кейсы механики не меняются (spec §4.4: «без изменений механики»).

- [ ] **Шаг 9.4. Прогон серии (по одному классу, между классами — проверка чистоты):**

```bash
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/ValkeyWorker.IntegrationTests -c Debug \
  --filter "FullyQualifiedName~ProvisioningTests|FullyQualifiedName~TlsMigrationTests|FullyQualifiedName~SupervisionTests|FullyQualifiedName~RotationTests|FullyQualifiedName~ConvergeTests|FullyQualifiedName~DeprovisioningTests|FullyQualifiedName~AclMatrixTests"
```

  Ожидание: PASS. После прогона: `docker ps -a --format '{{.Names}}' | grep vwk-` → пусто; `docker volume ls -q | grep '^vwk-'` → пусто; иначе — разбор до коммита (teardown фикстуры обязан чистить volume — `ValkeyClusterFixture.DisposeAsync` + вызов `RemoveTlsVolumeAsync` в teardown кластеров фикстуры).

- [ ] **Шаг 9.5. Коммит:** `test(valkey): интеграция TLS — tls-only поднятие, plain отклонён, миграция plain→TLS, чистота volume`.

**Выход:** docker-уровень подтверждает критерии приёмки 2–4 spec.
**Проверка задачи:** серия зелёная + чистота docker после.

---

## Задача 10. E2E Release + стенд-чеки + runbook

**Spec:** §4.9 (E2E Release), §4.7 (стенд), §6 фазы 9–10; AGENTS.md (E2E-телеметрия, e2e-launch.md).
**Вход:** задача 9 зелёная; `docker network ls | grep -c 'vwk\|kfw'` без осиротевших сетей.
**Файлы:** Modify `src/tests/ValkeyWorker.IntegrationTests/E2e/ValkeyE2eLifecycleTests.cs` (+ при необходимости `ValkeyE2eEnvironment.cs` — снятие volume в teardown); Modify `dev-stand/adminpanel/checks/51-valkey-api.sh`; Modify `docs/runbook.md`.

- [ ] **Шаг 10.1. E2E-маркер TLS** (мерж-гейт; имя фиксирует план — spec §4.9): в `ValkeyE2eLifecycleTests.cs` новый `[Fact] Tls_ClusterLifecycleTlsOnly` (полный собственный контур по канону e2e-isolation: guid-имена, динамические порты, teardown + ассерт чистоты ВКЛ. volume — `docker volume ls` через окружение/Docker API; телеметрия по `docs/e2e-launch.md` — артефакты `/tmp/pgw-e2e-artifacts-<guid>/`, `MarkFailed()` останавливает без удаления):
  1. API-создание кластера (как в `Lifecycle_ProvisionToClean`) → ждём ACTIVE/RUNNING;
  2. контейнер `vwk-<C>-node1` жив, args содержат `--tls-port 6379`/`--port 0` (docker inspect);
  3. дискавери-ключи: `endpoints` прежний формат, `ca_pem`/`ca_key` существуют, PEM валиден;
  4. RESP-проба по TLS app-кредом с `ca_pem` из etcd (RespProbe.ExecuteTls): SET→GET roundtrip;
  5. plain отклонён (сырой PING без TLS → отказ);
  6. live-проба панели по TLS (если контур E2E поднимает панель; иначе — пропустить, панель покрыта чеком 10.3);
  7. удаление → ассерт чистоты: ни контейнера, ни volume `vwk-<C>-tls`, ни ключей префикса.

- [ ] **Шаг 10.2. Прогон E2E (Release, свежая сборка):**

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter FullyQualifiedName~Tls_ClusterLifecycleTlsOnly
```

  Ожидание: PASS; затем полный прогон ValkeyWorker-серии E2E: `--filter FullyQualifiedName~ValkeyWorker.IntegrationTests.E2e`. Зачистка артефактов между прогонами (канон AGENTS.md).

- [ ] **Шаг 10.3. Стенд-чек `51-valkey-api.sh`** — дополнить после шага «create $TAG → RUNNING» (existing-структуру сохранить; etcd-хелпер `etcd_key` уже есть):
  - CA на диск: `etcd_key "/valkey/clusters/$TAG/ca_pem" | sed 's/\\n/\n/g' > "$TMP/ca.pem"` (PEM etcd однострочный → многострочный файл; фактический формат значения проверить на живом стенде — если переносы реальные, sed не нужен);
  - TLS-проба: `docker run --rm --network host -v "$TMP/ca.pem:/tmp/ca.pem:ro" valkey/valkey:9.1.2 valkey-cli --tls --cacert /tmp/ca.pem -h <advertised-host> -p <port> -u app --no-auth-warning -a "$app_password" PING` → `PONG` (хост/порт/пароль — из endpoints-ключа etcd и app_password);
  - plain закрыт: тот же вызов БЕЗ `--tls` → ненулевой exit/ошибка (допустим retry-цикл ≤ 30 с суммарно);
  - коммент-шапка скрипта дополнить «TLS-проверки t06».
  Прогон: подъём стенда `dev-stand/adminpanel/checks/00-up.sh` → `bash dev-stand/adminpanel/checks/51-valkey-api.sh` → EXIT 0 (панель live=true по TLS входит в существующие шаги скрипта). После — зачистка сетей по канону AGENTS.md.

- [ ] **Шаг 10.4. `docs/runbook.md`:** найти упоминания valkey-подключений/plain-порта; дополнить заметкой: клиентский порт Valkey-кластеров — TLS (`--tls-port`), подключение требует `ca_pem` из etcd (`redis-cli --tls --cacert …`, SE.Redis ssl+CA); plain-порт закрыт с t06.

- [ ] **Шаг 10.5. Коммит:** `test(e2e)+stand: TLS-маркер Tls_ClusterLifecycleTlsOnly, чеки 51-valkey-api по TLS, runbook`.

**Выход:** мерж-гейт-доказательства TLS (Release E2E + стенд).
**Проверка задачи:** шаги 10.2–10.3 зелёные.

---

## Задача 11. Puzzle: ветка + HA.Valkey (`ca_pem`, `ssl`)

**Spec:** §4.8 (HA.Valkey), §3.4 (Puzzle-канон), §6 фаза 8.
**Вход:** задачи pg 1+3 закоммичены (канон ключа `ca_pem` стабилен: PEM одной строкой); Puzzle-репозиторий чист, кроме staged `src/global.json` (НЕ трогать).
**Файлы (все пути — `/Users/demakaev/ZCodeProject/Puzzle`):** ветка; Modify `src/PuzzleServer.Infrastructure.App.HA.Valkey/Parsing/ValkeyClusterParser.cs`, `src/PuzzleServer.Infrastructure.App.HA.Valkey/Model/ValkeyClientConfig.cs`, `src/PuzzleServer.Infrastructure.App.HA.Valkey/Model/ValkeyClusterSnapshot.cs`; Test `src/PuzzleServer.UnitTests/HA/Valkey/ValkeyClusterParserTests.cs`, новый `src/PuzzleServer.UnitTests/HA/Valkey/ValkeyClientConfigTests.cs`.

- [ ] **Шаг 11.1. Ветка:** `git -C /Users/demakaev/ZCodeProject/Puzzle switch -c feat-t06-valkey-tls` (от `main`; staged `global.json` остаётся staged — не коммитить его в этой ветке).

- [ ] **Шаг 11.2. Тесты (падающие).**
  - `ValkeyClusterParserTests`: `Parse_CaPem_Populated` (ключ → `ValkeyClusterData.CaPem`); `Parse_UnknownKeys_DoNotIncludeCaPem` (ca_pem НЕ в unknownKeys).
  - `ValkeyClientConfigTests`:

```csharp
[Fact]
public void Of_CaPresent_SslTrue()
{
    // Arrange / Act
    var cc = ValkeyClientConfig.Of("h:1", "app", "pw", "-----BEGIN CERTIFICATE-----…");
    // Assert — ssl=true ⟺ ca_pem (arch/20 §4 после t06)
    Assert.True(cc.Ssl);
    Assert.NotNull(cc.CaPem);
}

[Fact]
public void Of_CaAbsent_SslFalse_BackwardCompatible()
{
    // Arrange / Act — снапшот без CA (окно миграции/старый контур)
    var cc = ValkeyClientConfig.Of("h:1", "app", "pw", null);
    // Assert
    Assert.False(cc.Ssl);
    Assert.Null(cc.CaPem);
}

[Fact]
public void GetClientConfig_WithCa_ReturnsCa()
{
    // Arrange — снапшот с endpoints+кредами+ca
    // Act / Assert — GetClientConfig() отдаёт ssl=true и CA
}
```

- [ ] **Шаг 11.3. Реализация.**
  - `ValkeyClusterParser.Parse`: переменная `string? caPem = null`; case `"ca_pem" when segments.Length == 5: caPem = kv.Value; break;`; `ValkeyClusterData` + `string? CaPem`; комментарий про «полный набор — креды парой; ca_pem самостоятельно (может появиться окном раньше/позже)».
  - `ValkeyClientConfig`: рекорд + `string? CaPem`; КОНСТАНТУ `SslValue` удалить; фабрика `public static ValkeyClientConfig Of(string endpoints, string username, string password, string? caPem) => new(endpoints, username, password, caPem is not null, caPem);`; `ToString` — `CaPem` НЕ печатать (только `Ssl = {Ssl}`; редакция секретов сохраняется).
  - `ValkeyClusterSnapshot.GetClientConfig()`:

```csharp
    public ValkeyClientConfig? GetClientConfig()
        => Endpoints is null || App is null
            ? null
            : ValkeyClientConfig.Of(Endpoints, App.Username, App.Password, CaPem);
```

  (`ValkeyClusterSnapshot` + `string? CaPem` поле — из парсера; конструктор вызывается в `ValkeyDiscoveryRefresher`/`ValkeyDiscoveryStore` — обновить построение снапшота передачей `data.CaPem`; grep `new ValkeyClusterSnapshot`.)

- [ ] **Шаг 11.4. Прогнать:** `dotnet test src/PuzzleServer.Api.slnx --filter "FullyQualifiedName~UnitTests" --filter "FullyQualifiedName~Valkey"` → PASS (юниты; docker не нужен). `dotnet build src/PuzzleServer.Api.slnx` чисто.
- [ ] **Шаг 11.5. Коммит (в ветке Puzzle):** `feat(ha-valkey): чтение ca_pem — ssl ⟺ CA, GetClientConfig отдаёт CA (t06)`.

**Выход:** дискавери-библиотека видит CA и вычисляет ssl.
**Проверка задачи:** юниты HA.Valkey зелёные; `grep -rn SslValue src/` — пусто.

---

## Задача 12. Puzzle: `App.Valkey` TLS-доверие

**Spec:** §4.8 (Infrastructure.App.Valkey), §6 фаза 8.
**Вход:** задача 11 закоммичена.
**Файлы (Puzzle):** Modify `src/PuzzleServer.Infrastructure.App.Valkey/ValkeyConnectionParams.cs`, `ValkeyConnectionOptions.cs`, `DiscoveryValkeyConnectionProvider.cs`; Test `src/PuzzleServer.UnitTests/Valkey/ValkeyConnectionOptionsTests.cs`, `DiscoveryValkeyConnectionProviderTests.cs`.

- [ ] **Шаг 12.1. Тесты (падающие).**
  - `ValkeyConnectionOptionsTests`: `Map_CaPemPresent_SslAndCallback` — `Map(new ValkeyConnectionParams("h:1", "app", "pw", Ssl: true, CaPem: ca))` → `options.Ssl == true`, `options.CertificateValidationCallback != null`; колбэк: валидный серт от CA + SAN `h` → true; чужой CA → false; SAN мимо → false (серты генерит тест-хелпер — мини-PKI копией).
  - `Map_CaPemAbsent_NoCallback` — `CaPem: null` → callback null, Ssl false.
  - `DiscoveryValkeyConnectionProviderTests`: `OnChange_ValueEqualityIncludesCaPem` — снапшот-обновление, меняющее ТОЛЬКО ca_pem (null → CA) → параметры изменились → handler позван (ssl false→true); обновление, меняющее только Revision/FetchedAt → handler НЕ позван (шум).

- [ ] **Шаг 12.2. Реализация.**
  - `ValkeyConnectionParams` + `string? CaPem` (последним полем; в `ToString` не печатать — только `Ssl`); `DiscoveryValkeyConnectionProvider.ComputeFromSnapshot`:

```csharp
    private static ValkeyConnectionParams? ComputeFromSnapshot(ValkeyClusterSnapshot snapshot)
        => snapshot.GetClientConfig() is { } cc
            ? new ValkeyConnectionParams(cc.Endpoints, cc.Username, cc.Password, cc.Ssl, cc.CaPem)
            : null;
```

  (value-equality рекорда автоматически включает CaPem — фильтр шума сохраняет семантику «соединительные параметры изменились»).
  - `ValkeyConnectionOptions.Map` (точная подпись callback: `RemoteCertificateValidationCallback`, SE.Redis передаёт её в SslStream):

```csharp
    public static ConfigurationOptions Map(ValkeyConnectionParams p)
    {
        var options = new ConfigurationOptions
        {
            AbortOnConnectFail = false,
            Ssl = p.Ssl,
        };
        // TLS-доверие (t06): SE.Redis не имеет прямой PEM-CA-опции — certificate-
        // validation callback строит цепочку CustomRootTrust против CaPem и
        // сверяет SAN с хостами endpoints (колбэк per-соединение: хосты списка).
        if (p.Ssl && p.CaPem is { Length: > 0 } caPem)
        {
            var hosts = p.Endpoints.Split(',', StringSplitOptions.RemoveEmptyEntries |
                                               StringSplitOptions.TrimEntries)
                .Select(e => e[..e.LastIndexOf(':')]).ToArray();
            var ca = X509Certificate2.CreateFromPem(caPem);
            options.CertificateValidationCallback = (_, certificate, _, _) =>
                certificate is not null
                && ValidateAgainstCa(certificate, ca)
                && hosts.Any(h => SanMatchesHost(certificate, h));
        }
        // ... endpoints/креды — без изменений
    }

    // Копия Shared.Tls TlsChain (Puzzle не ссылается на pg): CustomRootTrust,
    // NoCheck — частная CA без CRL/OCSP.
    private static bool ValidateAgainstCa(X509Certificate certificate, X509Certificate2 ca)
    {
        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(ca);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        return chain.Build(certificate is X509Certificate2 c2 ? c2 : new X509Certificate2(certificate));
    }

    private static bool SanMatchesHost(X509Certificate certificate, string host) // X509SubjectAltNameExtension — как в задаче 5
```

  (внимание: `X509Certificate2.CreateFromPem` бросает на мусор — обернуть try/catch → callback не ставится + лог не доступен (статик) ⇒ при битом PEM селективно отключить TLS нельзя: оставить `Ssl=true` с системной валидацией нельзя тоже — решение: битый PEM → НЕ ставить callback и НЕ сбрасывать Ssl; подключение честно упадёт на хендшейке, диагностика — в app-логах SE.Redis. Зафиксировать комментарием).
  - Aspire-ветка провайдера (конфиг-источник без CA, ssl=false) — НЕ трогается (spec §4.8).

- [ ] **Шаг 12.3. Прогнать:** `dotnet test src/PuzzleServer.Api.slnx --filter FullyQualifiedName~UnitTests` → PASS; сборка чисто.
- [ ] **Шаг 12.4. Коммит:** `feat(valkey-client): TLS-доверие per-cluster CA в ConfigurationOptions (callback), CaPem в value-equality (t06)`.

**Выход:** приложение подключается к TLS-valkey с доверием CA; hot-reload жив.
**Проверка задачи:** юниты App.Valkey зелёные.

---

## Задача 13. Puzzle: интеграционный TLS-контур + доки 01.21/01.22

**Spec:** §4.8 (доки), §4.9 (Puzzle-интеграция), §3.4, §6 фаза 8.
**Вход:** задачи 11–12 закоммичены; docker запущен.
**Файлы (Puzzle):** Create `src/PuzzleServer.IntegrationTests/HA/Valkey/ValkeyTlsFixture.cs`, `src/PuzzleServer.IntegrationTests/HA/Valkey/ValkeyTlsDiscoveryTests.cs`; Modify `docs/01.21-ha-valkey.md`, `docs/01.22-valkey.md` (+ строка индекса `docs/01-infrastructure.md` при необходимости).

- [ ] **Шаг 13.1. Фикстура `ValkeyTlsFixture`** (порт `ValkeyEtcdFixture` + `ValkeyClientFixture`): Testcontainers etcd + Testcontainers valkey `valkey/valkey:9.1.2` с `--tls-port 6379 --port 0 --tls-cert-file … --tls-key-file … --tls-ca-cert-file … --tls-auth-clients no` + ACL-креды как у воркера; серты генерирует фикстура (RSA-2048 CA + серт SAN=localhost; файлы — во временный каталог хоста, mount в контейнер); в etcd пишет полный набор дискавери: `config`, `endpoints` (`localhost:<динамический порт>` — `GetMappedPublicPort`), `app_user`/`app_password`, `ca_pem`, `ca_key`. Teardown: контейнеры + etcd-префикс (guid-изоляция).

- [ ] **Шаг 13.2. Интеграционные кейсы `ValkeyTlsDiscoveryTests`:**
  - `TlsCluster_ClientRoundtrip` — (Arrange) фикстура + `ValkeyDiscoveryStore` на её etcd; (Act) актуализация + `GetClientConfig()` → `ValkeyConnectionOptions.Map` → `ConnectionMultiplexer.Connect` → SET/GET; (Assert) roundtrip ок, `cc.Ssl == true`, `cc.CaPem` == CA фикстуры.
  - `CaAppeared_SslFlipsFalseToTrue` — (Arrange) стартовать etcd-набор БЕЗ `ca_pem` → актуализация → `Ssl == false` (старый контур жив); (Act) дописать `ca_pem` → актуализация; (Assert) `Ssl == true`, подписка провайдера сработала (OnChange позван — окно миграции прозрачно).
  - `ForeignCa_ConnectionFails` — подменить `ca_pem` чужим CA → подключение падает (доверие не проходит).

- [ ] **Шаг 13.3. Прогон:** `dotnet test src/PuzzleServer.Api.slnx --filter FullyQualifiedName~ValkeyTlsDiscoveryTests` → PASS (docker). Зачистка контейнеров после прогона.
- [ ] **Шаг 13.4. Доки (русский язык):**
  - `docs/01.21-ha-valkey.md`: раздел «Читаемые ключи» + `ca_pem`; «Модель снапшота» + CaPem; оговорки «модуль пока без TLS» → актуализировать (ssl ⟺ ca_pem, окно миграции ssl false→true).
  - `docs/01.22-valkey.md`: §1a/§2 — маппинг `CaPem` → certificate-validation callback (CustomRootTrust + SAN), поведение при битом PEM; «Границы v1» — снять TLS-оговорку.
  - Индекс `docs/01-infrastructure.md` — строка при необходимости.
- [ ] **Шаг 13.5. Коммит:** `test+docs(valkey): TLS-контур интеграции, 01.21/01.22 — ca_pem/ssl/callback (t06)`.

**Выход:** Puzzle-сторона t06 завершена (код+тесты+доки).
**Проверка задачи:** шаг 13.3 зелёный.

---

## Задача 14. Мерж-гейт: roadmap-правки + финальный E2E (обоих репо)

**Spec:** §8 (критерии 8–9), §10 (roadmap-правки), §6 фаза 11.
**Вход:** задачи 1–13 закоммичены; обе ветки зелёные.
**Файлы:** Modify `arch/roadmap/valkey.md` (pg); финальные прогоны.

- [ ] **Шаг 14.1. Roadmap (pg, до мерж-коммита — по букве spec §10):**
  - Удалить пункт `t06-valkey-tls` из `arch/roadmap/valkey.md` (список «Задачи») — мерж-гейт: тем же коммитом мержа в `main` (коммит правки готовится сейчас, едет с мержем).
  - Добавить пункт: `tNN-valkey-ca-rotation` — «ротация per-cluster CA и сертов Valkey (окно двойного доверия, образец CaRotator arch/16 §5 K; ca_next_* staging, bundle ca_pem, пересоздание нод с перевыпуском)» — со ссылкой на канон после t06 (arch/20 §2, arch/21 §9 R10-аналог); номер `tNN` — следующий свободный в треке valkey (сверить `arch/roadmap/README.md`).
  - Проверить `←`-зависимости на `t06-valkey-tls` в других файлах `arch/roadmap/*.md` (grep) — снять.

- [ ] **Шаг 14.2. Финальный E2E pg (свежий Release, полный):**

```bash
DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release \
  --filter FullyQualifiedName~ValkeyWorker.IntegrationTests.E2e
```

  + маркер `--filter FullyQualifiedName~Tls_ClusterLifecycleTlsOnly` (если не входит). Ожидание: PASS. Зачистка после (контейнеры/тома/сети; телеметрия — артефакты разбираются, упавших нет).

- [ ] **Шаг 14.3. Полные сборки-гейты:** pg: `dotnet build src/PgWorker.slnx -c Release` (0 warnings, TreatWarningsAsErrors); полный юнит-прогон: `dotnet test src/PgWorker.slnx -c Debug --filter FullyQualifiedName~UnitTests`. Puzzle: `dotnet build src/PuzzleServer.Api.slnx` + `dotnet test src/PuzzleServer.Api.slnx --filter FullyQualifiedName~UnitTests`.

- [ ] **Шаг 14.4. Коммит roadmap (pg, в ветке — попадёт в мерж):** `docs(roadmap): снять t06-valkey-tls (мерж-гейт), добавить tNN-valkey-ca-rotation`.

**Выход:** ветки обоих репо готовы к ревью и мержу; далее — superpowers:finishing-a-development-branch (запрос-гейт пользователя).
**Проверка задачи:** шаги 14.2–14.3 зелёные; `grep -rn 't06-valkey-tls' arch/roadmap/` — пусто (в roadmap-файлах; упоминания в docs/superpowers — история, не трогаются).

---

## Self-review плана (исполнено составителем)

- **Покрытие spec:** §3.1–3.4 → задача 1; §4.1 → 2, 5; §4.2 → 4; §4.3 → 3; §4.4 → 3, 6, 7; §4.5 → 7; §4.6 → 8; §4.7 → 10; §4.8 → 11–13; §4.9 → 9, 10, 13; §5 (сбои) → 3 (битый PEM), 6 (SAN drift/слет volume — перевыпуск), 7 (частичные ключи/TO_REMOVE/отказ между T1–T3 — идемпотентность re-run); §8.1–8.9 → задачи 3, 9, 10, 13, 14; §10 → 14. Пробелов нет.
- **Типовая консистентность:** `ValkeyPki.IssueNodeCertificate(caPem, caKeyPem, commonName, advertisedHost)` — одинаково в задачах 2/6; `ValkeyEndpoint(Host, Port, User, Password, CaPem)` — 5/6/7; `TlsMigrator.NeedsMigration(snap, liveNodeArgs)`/`MigrationOutcome` — 7; `TarArchive.Build/Read` — 4/6; `ValkeyClientConfig.Of(...)` — 11/12; `ValkeyConnectionParams(..., CaPem)` — 12.
- **Плейсхолдеры:** шаги с пометкой «сверить при исполнении» ссылаются на конкретные образцовые файлы (kafka `ClusterPki`/`SecurityMigrator`/kafka-ensurer) — код-скелеты даны полностью; отсутствуют TBD/«реализовать потом».
- **Известные развилки, закрытые по букве spec:** коммиты в ветке Puzzle — свободно (spec §10.3), мерж обоих main — только по явной просьбе; Puzzle-канон (§3.4) правится в задаче 13 вместе с доками 01.21/01.22 (Puzzle-сторона).
