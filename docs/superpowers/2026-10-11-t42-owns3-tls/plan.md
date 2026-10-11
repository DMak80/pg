# t42-owns3-tls: TLS-опция S3-грани ownS3 — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** ownS3 умеет HTTPS-only на порту сервера по per-install сертификату (fail-fast на битый материал), подсистема бэкапов (SDK воркера/WAL-приёмника, `mc` в джобах) доверяет per-install CA при https-endpoint; стенд остаётся HTTP-only.

**Architecture:** TLS — опция: материал (PEM или `_PATH`) задан → Kestrel слушает `https://*:{port}` с сертификатом (один порт, один режим, без редиректов); не задан — поведение не меняется ни на бит (HTTP). Загрузка материала — общий `Shared.Tls.TlsMaterial`, серт выпускает `deploy/tls/gen.sh` из единой `kfw-install-ca`. Клиенты доверяют CA: AWS SDK .NET — программная фабрика HttpClient `S3CaHttpClientFactory` (валидация корня цепочки сервера по отпечатку per-install CA; ставится конструктором `BackupS3` только при заданном `CaBundlePath` — env `AWS_CA_BUNDLE` линейкой AWSSDK.Core 3.7.x не читается, эмпирический факт E2E t42, spec §2 п.5/§5); агенты/джобы получают CA значением PEM через env `S3_CA_PEM` и материализуют в файл (SDK-клиент приёмника — та же фабрика через `S3CaBundlePath`; `mc` — `SSL_CERT_FILE`).

**Tech Stack:** .NET 10 (`Nullable=enable`, `TreatWarningsAsErrors=true`), xUnit + FluentAssertions (AAA-комментарии), testcontainers (динамические порты), openssl CLI в gen.sh, `CertificateRequest` BCL для тестовых CA.

**Spec:** `docs/superpowers/2026-10-11-t42-owns3-tls/spec.md`. Канон: `arch/owns3/03-protocol.md` §6, `arch/owns3/05-access-config.md` §4/§6, `arch/19-backups.md` §7/§9 (обновлены в Фазе 1 этой задачи — сверяться с worktree-версией).

## Global Constraints

- .NET 10, `TreatWarningsAsErrors=true`, `Nullable=enable`; новые NuGet-пакеты НЕ добавляются; централизованное версионирование `Directory.Packages.props` (spec §5).
- Серты в gen.sh — `openssl` CLI; тестовые CA/серты — `CertificateRequest` из BCL (spec §5).
- Docker-образ ownS3 не меняется; локальные образы в registry `192.168.0.1:5000` не кладутся (spec §5).
- Доверие CA у AWS SDK .NET — ТОЛЬКО программная фабрика HttpClient `S3CaHttpClientFactory` (валидация корня цепочки по отпечатку CA; ExtraStore + AllowUnknownCertificateAuthority, без revocation — внутренний контур): env `AWS_CA_BUNDLE` линейкой AWSSDK.Core 3.7.x не читается — эмпирический факт E2E t42 (строки в сборке отсутствуют; соединение без системного доверия даёт `PartialChain`) — spec §2 п.5/§5. `mc` (Go) — `SSL_CERT_FILE`.
- Тесты: ТОЛЬКО динамические порты (`WithPortBinding(..., assignRandomHostPort: true)` / зонд свободных портов), полный teardown при любом исходе + ассерт чистоты, таймауты серий ≤ 100 с, никаких хардкод-портов (spec §5, AGENTS.md).
- Матрица прав ownS3 (arch/owns3/05 §3) в тестах: CreateBucket/DeleteBucket — только admin/root-клиент; read-write (Writer) — объектные операции и multipart. Бакет-операции в SDK-сценариях делает Root/Admin-клиент по образцу `AwsSdkSimpleScenarios` (PutGet_Roundtrip: root создаёт бакет, writer пишет).
- Инсталляция без `OWNS3_TLS_*` / без `PGW_BACKUP_S3_CA_BUNDLE_PATH` не меняет поведение ни на бит: фабрика не ставится, env-набор джобов прежний (spec §2 п.1, AC1/AC5).
- Один порт, один режим: без HTTP→HTTPS-редиректов, без второго порта, mTLS нет — `ClientCertificateMode` не трогаем (spec §1, §2 п.2, §3.1).
- WAF-тесты ownS3 НЕ доводят исполнение до веток `Environment.Exit(1)` Program.cs (fail-fast режима/тома/TLS): под WebApplicationFactory top-level-код исполняется in-process, exit убьёт весь testhost. Ветки fail-fast Program проверяются docker-кейсом (шаг 5.4), юнитами покрыт сам резолвер (шаги 1.1/1.2).
- Комментарии/документация — по-русски; идентификаторы — английские.
- Коммиты — в feature-ветке worktree свободно; мерж/пуш — только по явной просьбе пользователя.
- Всё в worktree `/Users/demakaev/ZCodeProject/worktrees/feat-t42-owns3-tls` (код, тесты, deploy, docs).
- Команды сборки/тестов — в фоне (`run_in_background`), проверять результат по task_id; sleep-поллинг ≤ 30 с.

---

### Task 1: ownS3 — TLS-материал, env, Kestrel HTTPS, fail-fast

**Вход (предусловие):** worktree чист (изменений нет); канон `arch/owns3/05-access-config.md` §4 уже несёт строки `OWNS3_TLS_*` (Фаза 1 слита в ветку задачи).

**Выход:** `OwnS3Options.Tls` + env-трансляция `OWNS3_TLS_{CERT,KEY}[_PATH]` + резолвер `OwnS3Tls` (null=HTTP / сертификат / исключение) + Program.cs: HTTPS-Urls при материале, fail-fast (Critical + `Environment.Exit(1)`) при невалидном. Юниты зелёные.

**Спека (связь):** §3.1 целиком; AC1 (HTTP-по-умолчанию), AC2 (HTTPS-only), AC3 (fail-fast). Канон: arch/owns3/03 §6, 05 §4.

**Files:**
- Modify: `src/OwnS3.App/OwnS3.App.csproj` — добавить `<ProjectReference Include="..\Shared.Tls\Shared.Tls.csproj"/>` в ItemGroup к существующим (после `Shared.Metrics`).
- Modify: `src/OwnS3.App/OwnS3Options.cs` — секция Tls.
- Create: `src/OwnS3.App/OwnS3Tls.cs`.
- Modify: `src/OwnS3.App/Program.cs` — env-трансляция (массив строк 24–33), ConfigureKestrel (строка 195), блок Urls (строки 197–200).
- Test: `src/tests/OwnS3.UnitTests/App/OwnS3TlsTests.cs`, `src/tests/OwnS3.IntegrationTests/Api/FailFastScenarios.cs`.

**Interfaces (Produces):**
```csharp
namespace OwnS3.App;
// OwnS3Options.Tls → TlsOptions { string? ServerCertPem, ServerKeyPem, ServerCertPath, ServerKeyPath }
public static class OwnS3Tls
{
    // null — HTTP-режим; сертификат — HTTPS; неполная пара/нечитаемый файл/битый PEM — InvalidOperationException.
    public static X509Certificate2? Resolve(TlsOptions tls);
}
```

- [ ] **Шаг 1.1. Тест резолвера (красный)**

  Действие: создать `src/tests/OwnS3.UnitTests/App/OwnS3TlsTests.cs`. Генерация пары — BCL:
  ```csharp
  using System.Security.Cryptography;
  using System.Security.Cryptography.X509Certificates;
  using FluentAssertions;
  using OwnS3.App;

  namespace OwnS3.UnitTests.App;

  // Резолв TLS-материала ownS3 (t42, arch/owns3/05 §4): пусто — HTTP (null);
  // валидная пара — сертификат; неполная пара/нет файла/битый PEM — исключение
  // (fail-fast старта). PEM приоритетнее _PATH — образец RestTlsMaterial.
  public class OwnS3TlsTests
  {
      private static (string CertPem, string KeyPem) NewPair()
      {
          using var rsa = RSA.Create(2048);
          var req = new CertificateRequest("CN=os3-tls-test", rsa,
              HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
          using var cert = req.CreateSelfSigned(
              DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
          return (cert.ExportCertificatePem(), rsa.ExportPkcs8PrivateKeyPem());
      }

      // AAA: материал не задан — HTTP-режим (null), поведение прежнее
      [Fact]
      public void Empty_ReturnsNull_HttpMode()
      {
          OwnS3Tls.Resolve(new TlsOptions()).Should().BeNull();
      }

      // AAA: валидная PEM-пара — сертификат с приватным ключом
      [Fact]
      public void ValidPemPair_ReturnsCertificate()
      {
          var (certPem, keyPem) = NewPair();
          var cert = OwnS3Tls.Resolve(new TlsOptions { ServerCertPem = certPem, ServerKeyPem = keyPem });
          cert.Should().NotBeNull();
          cert!.HasPrivateKey.Should().BeTrue();
      }

      // AAA: только сертификат (значением) — отказ
      [Fact]
      public void OnlyCert_Throws()
      {
          var (certPem, _) = NewPair();
          var act = () => OwnS3Tls.Resolve(new TlsOptions { ServerCertPem = certPem });
          act.Should().Throw<InvalidOperationException>().WithMessage("*OWNS3_TLS*");
      }

      // AAA: только ключ (путём) — отказ
      [Fact]
      public void OnlyKeyPath_Throws()
      {
          var act = () => OwnS3Tls.Resolve(new TlsOptions { ServerKeyPath = "/tmp/no-matter.pem" });
          act.Should().Throw<InvalidOperationException>();
      }

      // AAA: путь задан, файла нет — отказ (не «молча на HTTP»)
      [Fact]
      public void MissingCertFile_Throws()
      {
          var (_, keyPem) = NewPair();
          var act = () => OwnS3Tls.Resolve(new TlsOptions
          {
              ServerCertPath = Path.Combine(Path.GetTempPath(), $"os3-no-{Guid.NewGuid():N}.crt"),
              ServerKeyPem = keyPem,
          });
          act.Should().Throw<InvalidOperationException>().WithMessage("*не читается*");
      }

      // AAA: пара через _PATH-файлы читается
      [Fact]
      public void PathPair_ReadsFiles()
      {
          var (certPem, keyPem) = NewPair();
          var dir = Path.Combine(Path.GetTempPath(), $"os3-tls-{Guid.NewGuid():N}");
          Directory.CreateDirectory(dir);
          File.WriteAllText(Path.Combine(dir, "s.crt"), certPem);
          File.WriteAllText(Path.Combine(dir, "s.key"), keyPem);
          OwnS3Tls.Resolve(new TlsOptions
          {
              ServerCertPath = Path.Combine(dir, "s.crt"),
              ServerKeyPath = Path.Combine(dir, "s.key"),
          }).Should().NotBeNull();
      }

      // AAA: PEM-значение приоритетнее _PATH (образец RestTlsMaterial воркеров)
      [Fact]
      public void PemTakesPriorityOverPath()
      {
          var (certPem, keyPem) = NewPair();
          var (_, otherKey) = NewPair(); // «путевой» ключ не подходит к серту
          var dir = Path.Combine(Path.GetTempPath(), $"os3-tls-{Guid.NewGuid():N}");
          Directory.CreateDirectory(dir);
          File.WriteAllText(Path.Combine(dir, "k.key"), otherKey);
          OwnS3Tls.Resolve(new TlsOptions
          {
              ServerCertPem = certPem, ServerKeyPem = keyPem, ServerKeyPath = Path.Combine(dir, "k.key"),
          }).Should().NotBeNull("PEM-значения побеждают — путевой ключ не читается");
      }

      // AAA: битый PEM — отказ с диагностикой
      [Fact]
      public void GarbagePem_Throws()
      {
          var act = () => OwnS3Tls.Resolve(new TlsOptions
          {
              ServerCertPem = "not a pem", ServerKeyPem = "not a key",
          });
          act.Should().Throw<InvalidOperationException>().WithMessage("*PEM*");
      }
  }
  ```
  Выход: тест-файл существует, проект тестов не собирается (нет `OwnS3Tls`/`TlsOptions`).
  Проверка: `dotnet build src/tests/OwnS3.UnitTests -c Debug` (фон) → FAIL компиляции `OwnS3Tls не существует`.
  Спека: §3.5 юниты (fail-fast, HTTP-режим по умолчанию).

- [ ] **Шаг 1.2. Минимальная реализация резолвера (зелёный)**

  Действие: `src/OwnS3.App/OwnS3Options.cs` — в `OwnS3Options` добавить `public TlsOptions Tls { get; set; } = new();` (после `HostId`) и новый класс в конце файла:
  ```csharp
  // TLS-материал HTTPS-грани (arch/owns3/05 §4): PEM-значения или _PATH-файлы;
  // env OWNS3_TLS_{CERT,KEY}[_PATH]. Пусто — HTTP-режим (стенд).
  public sealed class TlsOptions
  {
      public string? ServerCertPem { get; set; }
      public string? ServerKeyPem { get; set; }
      public string? ServerCertPath { get; set; }
      public string? ServerKeyPath { get; set; }
  }
  ```
  Создать `src/OwnS3.App/OwnS3Tls.cs`:
  ```csharp
  using System.Security.Cryptography.X509Certificates;
  using Shared.Tls;

  namespace OwnS3.App;

  // Резолв TLS-материала ownS3 (t42, arch/owns3/03 §6 / 05 §4): PEM-пара
  // (ServerCertPem/ServerKeyPem) или файлы _PATH — PEM приоритетнее пути
  // (образец RestTlsMaterial воркеров). null — HTTP-режим (материал не задан);
  // неполная пара/нечитаемый файл/битый PEM — исключение → fail-fast старта
  // Program (Critical + exit 1), сервис не поднимается «молча на HTTP».
  public static class OwnS3Tls
  {
      public static X509Certificate2? Resolve(TlsOptions tls)
      {
          var certGiven = !string.IsNullOrWhiteSpace(tls.ServerCertPem)
                          || !string.IsNullOrWhiteSpace(tls.ServerCertPath);
          var keyGiven = !string.IsNullOrWhiteSpace(tls.ServerKeyPem)
                         || !string.IsNullOrWhiteSpace(tls.ServerKeyPath);
          if (!certGiven && !keyGiven)
              return null; // HTTP-режим (канон 05 §4: дефолт/стенд)
          if (!certGiven || !keyGiven)
              throw new InvalidOperationException(
                  "OwnS3:Tls: задан только один из пары сертификат/ключ — укажите оба " +
                  "(env OWNS3_TLS_CERT[_PATH] + OWNS3_TLS_KEY[_PATH])");

          var certPem = tls.ServerCertPem ?? TlsMaterial.ReadPemFile(tls.ServerCertPath);
          var keyPem = tls.ServerKeyPem ?? TlsMaterial.ReadPemFile(tls.ServerKeyPath);
          if (string.IsNullOrWhiteSpace(certPem) || string.IsNullOrWhiteSpace(keyPem))
              throw new InvalidOperationException(
                  "OwnS3:Tls: файл сертификата/ключа не читается или пуст (OWNS3_TLS_*_PATH)");

          try
          {
              return TlsMaterial.LoadPemPair(certPem, keyPem);
          }
          catch (Exception ex)
          {
              throw new InvalidOperationException(
                  $"OwnS3:Tls: невалидный PEM TLS-материала — {ex.Message}", ex);
          }
      }
  }
  ```
  В `src/OwnS3.App/OwnS3.App.csproj` добавить `<ProjectReference Include="..\Shared.Tls\Shared.Tls.csproj"/>`.
  Выход: резолвер собирается, юниты зелёные.
  Проверка: `dotnet test src/tests/OwnS3.UnitTests -c Debug --filter FullyQualifiedName~OwnS3TlsTests` (фон) → PASS (8 тестов).
  Спека: §3.1 «Загрузка материала», §2 п.4 fail-fast.

- [ ] **Шаг 1.3. Program.cs ownS3: env-трансляция + HTTPS + fail-fast**

  Действие: `src/OwnS3.App/Program.cs`, три правки.

  (а) В массив env-трансляции (строки 24–33, после `("OWNS3_HOST_ID", ...)`) добавить:
  ```csharp
  ("OWNS3_TLS_CERT", "OwnS3:Tls:ServerCertPem"),
  ("OWNS3_TLS_KEY", "OwnS3:Tls:ServerKeyPem"),
  ("OWNS3_TLS_CERT_PATH", "OwnS3:Tls:ServerCertPath"),
  ("OWNS3_TLS_KEY_PATH", "OwnS3:Tls:ServerKeyPath"),
  ```

  (б) Блок ConfigureKestrel (строка 193–195) заменить на:
  ```csharp
  // Kestrel: any-IP, h1+h2c (HTTP) / h2-by-ALPN (HTTPS) (arch/owns3/03 §6),
  // лимит тела отключён — лимит 5 ГБ уровня хендлера (глава 02), не транспорта.
  // TLS-опция (t42): сертификат резолвится ниже ПОСЛЕ Build (fail-fast) но ДО
  // старта прослушивания — Kestrel строит bindings при RunAsync, замыкание
  // читает уже заполненную переменную.
  X509Certificate2? tlsCert = null;
  builder.WebHost.ConfigureKestrel(o =>
  {
      o.Limits.MaxRequestBodySize = null;
      o.ConfigureHttpsDefaults(h => h.ServerCertificate = tlsCert);
  });
  ```

  (в) Блок Urls (строки 197–200) заменить на:
  ```csharp
  var app = builder.Build();
  var options = app.Services.GetRequiredService<IOptions<OwnS3Options>>().Value;

  // TLS-материал (t42, arch/owns3/03 §6 / 05 §4): резолв fail-fast ДО старта
  // прослушивания — неполный/битый материал = Critical + exit 1 (как режим/том
  // ниже), НЕ «молча на HTTP». Материал задан — HTTPS-only на том же порту.
  try
  {
      tlsCert = OwnS3Tls.Resolve(options.Tls);
  }
  catch (Exception ex)
  {
      app.Logger.LogCritical(ex, "OwnS3:Tls: невалидный TLS-материал — отказ старта (env OWNS3_TLS_*)");
      Environment.Exit(1);
  }

  app.Urls.Clear();
  app.Urls.Add($"{(tlsCert is null ? "http" : "https")}://*:{options.Server.Port}");
  ```
  Using-дополнение в топ файла Program.cs (текущий using-блок, строки 1–13, его не содержит — без него CS0246 на `X509Certificate2? tlsCert`):
  ```csharp
  using System.Security.Cryptography.X509Certificates; // X509Certificate2? tlsCert
  ```
  (`ConfigureHttpsDefaults` — instance-метод `KestrelServerOptions` (Microsoft.AspNetCore.Server.Kestrel.Core, FrameworkReference AspNetCore.App): вызывается на параметре лямбды `o` без using — как уже используемый `o.Limits` в текущем Program.cs.)
  Выход: грань слушает https при материале, http без; невалидный материал — Critical + exit 1.
  Проверка: `dotnet build src/OwnS3.App -c Debug` (фон) → 0 warnings/errors (`TreatWarningsAsErrors`); полный юнит-прогон шага 1.2 остаётся зелёным.
  Спека: §3.1 (Kestrel, fail-fast, env), §2 п.2 (один порт/один режим).

- [ ] **Шаг 1.4. Интеграционная проверка HTTP-инварианта (WAF)**

  Действие: в `src/tests/OwnS3.IntegrationTests/Api/FailFastScenarios.cs` дописать контрольный кейс и фабрику с томом (образец `OwnS3AppFactory`: `OwnS3:DataDir` = temp-каталог, удаление в `Dispose` — БЕЗ него валидный кейс исполнит `drive.Initialize()` на дефолтном `/data` → fail-fast ветка режима/тома → `Environment.Exit(1)` → смерть testhost):
  ```csharp
  // TLS не задан — хост стартует как раньше (AC1: поведение по умолчанию не меняется).
  [Fact]
  public void TlsEmpty_StartsHttp()
  {
      using var factory = new TlsFactory();
      using var client = factory.CreateClient();
      client.Should().NotBeNull();
  }

  private sealed class TlsFactory : WebApplicationFactory<Program>
  {
      // Томат данных (образец OwnS3AppFactory.TempVolumeDir): дефолтный /data
      // в testhost недоступен/невалиден — без подмены старт упал бы в fail-fast
      // режима/тома (Environment.Exit) ещё до этого кейса.
      private readonly string _volumeDir =
          Path.Combine(Path.GetTempPath(), "owns3-tls-waf-" + Guid.NewGuid().ToString("N"));

      protected override void ConfigureWebHost(IWebHostBuilder builder)
      {
          builder.UseEnvironment("Testing");
          builder.ConfigureAppConfiguration((_, cfg) => cfg.AddInMemoryCollection(new Dictionary<string, string?>
          {
              ["OwnS3:DataDir"] = _volumeDir,
              ["OwnS3:Root:User"] = "root",
              ["OwnS3:Root:Password"] = "rootpassword",
          }));
      }

      protected override void Dispose(bool disposing)
      {
          base.Dispose(disposing);
          if (disposing)
          {
              try
              {
                  if (Directory.Exists(_volumeDir))
                      Directory.Delete(_volumeDir, recursive: true);
              }
              catch (IOException)
              {
                  // хост мог ещё держать файлы — том в temp, ОС уберёт
              }
          }
      }
  }
  ```
  Примечание о границах WAF-покрытия (осознанная подмена, зеркально веткам fail-fast режима/тома, которые WAF-тестами сознательно не трогаются): half-pair/битый-PEM кейсы Program-ветки `Critical + Environment.Exit(1)` под WebApplicationFactory НЕ проверяются — top-level-код исполняется in-process, exit завершил бы весь testhost, не оставив ассерту шанса. Невалидный материал покрыт юнитами резолвера (шаги 1.1/1.2: OnlyCert/MissingCertFile/GarbagePem), а Program-ветка (Critical-диагностика + exit ≠ 0) — docker-кейсом шага 5.4 (контейнер `owns3:e2e` с одним `OWNS3_TLS_CERT` умирает с ненулевым кодом, диагностика в docker logs).
  Примечание о покрытии env-биндингов (spec §3.5 «env-биндинги OWNS3_TLS_* → конфиг»): env-имена живут в top-level Program.cs (массив строк 24–33) и из юнитов недостижимы; контракт `OWNS3_TLS_CERT_PATH`/`OWNS3_TLS_KEY_PATH` → `OwnS3:Tls:*` → HTTPS-грань проверяется end-to-end E2E-шагом 5.1 (контейнер поднимается именно по этим env) и docker-кейсом 5.4 (env `OWNS3_TLS_CERT`). Отдельных юнитов на env-трансляцию не заводим — по образцу существующих секций env (OWNS3_ROOT_* и др. также покрываются E2E/интеграцией, не юнитами).
  Выход: контрольный кейс старта HTTP без TLS; границы покрытия зафиксированы.
  Проверка: `dotnet test src/tests/OwnS3.IntegrationTests -c Debug --filter "FullyQualifiedName~FailFast"` (фон) → PASS (старые + 1 новый; testhost жив).
  Спека: AC1; §3.5 (env-биндинги — E2E, см. примечание).

- [ ] **Шаг 1.5. Регресс серий OwnS3 + коммит**

  Действие: прогнать полные серии.
  Выход: существующие серии зелёные без правок.
  Проверка (фон, последовательно, между сериями — контроль остаточных контейнеров `docker ps -a | grep os3-` пуст):
  1. `dotnet test src/tests/OwnS3.UnitTests -c Debug`
  2. `dotnet test src/tests/OwnS3.IntegrationTests -c Debug` (Api-сценарии без docker-гейта)
  Спека: AC1.
  Действие (коммит):
  ```bash
  git add src/OwnS3.App src/tests/OwnS3.UnitTests src/tests/OwnS3.IntegrationTests
  git commit -m "t42: ownS3 TLS-режим — OwnS3:Tls, OWNS3_TLS_* env, Kestrel HTTPS-only, fail-fast"
  ```
  Спека: фаза 2 закрыта.

---

### Task 2: deploy — серт owns3 в gen.sh, compose-инструкция, .env.example

**Вход:** Task 1 слит (сервер умеет TLS).

**Выход:** `deploy/tls/gen.sh` выпускает `owns3.crt/key` идемпотентно (SAN owns3/owns3-ec/localhost/host.docker.internal/127.0.0.1, serverAuth, из `kfw-install-ca`); финальный echo упоминает `owns3.*`; у сервиса `owns3` compose — комментарий-инструкция прод-включения; `.env.example` несёт закомментированные `OWNS3_TLS_*_PATH` и `PGW_BACKUP_S3_CA_BUNDLE_PATH`.

**Спека:** §3.2, §3.3; AC4, AC6 (серты в подъёме).

**Files:**
- Modify: `deploy/tls/gen.sh` (после блока `pgserver`, строка 48; финальный echo, строка 59).
- Modify: `deploy/docker-compose.yml` (сервис `owns3`, строки 260–276).
- Modify: `deploy/.env.example` (секция PGW_BACKUP_* ~строка 92; секция OWNS3_* ~строка 124).

- [ ] **Шаг 2.1. gen.sh — выпуск серта owns3**

  Действие: в `deploy/tls/gen.sh` после блока `pgserver` (строки 43–48) добавить:
  ```bash
  # t42: серверный серт ownS3 (arch/owns3/03 §6/05 §4): S3-грань по TLS в
  # прод-инсталляциях из единой kfw-install-ca; SAN — compose-имена сервисов,
  # хост-публикация и локальные клиенты. Идемпотентность по SAN-маркеру
  # DNS:owns3-ec (серт старого выпуска перегенерируется, CA жив).
  if [ ! -f owns3.crt ] || ! openssl x509 -in owns3.crt -noout -text 2>/dev/null | grep -q 'DNS:owns3-ec'; then
    issue owns3 owns3 serverAuth "DNS:owns3,DNS:owns3-ec,DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1"
  fi
  ```
  Финальный `echo` (строка 59) дополнить: `..., hc.*, panel.*, seed.*, prometheus.*, healthcheck.*, owns3.*"`.
  Выход: скрипт выпускает серт; повторный запуск — no-op.
  Проверка (все команды — фон, cwd = корень worktree):
  ```bash
  TMP=$(mktemp -d) && cp deploy/tls/gen.sh "$TMP/" && bash "$TMP/gen.sh" \
    && openssl verify -CAfile "$TMP/ca.pem" "$TMP/owns3.crt" \
    && openssl x509 -in "$TMP/owns3.crt" -noout -text | grep -A1 "Subject Alternative Name" \
    && M1=$(stat -f %m "$TMP/owns3.crt") && bash "$TMP/gen.sh" \
    && M2=$(stat -f %m "$TMP/owns3.crt") && [ "$M1" = "$M2" ] && echo IDEMPOTENT-OK
  ```
  Ожидание: verify OK; SAN содержит все 5 имён; `IDEMPOTENT-OK`. Плюс негативный маркер старого выпуска: `rm "$TMP/owns3.crt" && cp deploy/tls/…` не нужен — вместо него: создать фиктивный `owns3.crt` (`echo bad > "$TMP/owns3.crt"`), повторный gen.sh заменяет его на валидный (`openssl verify` снова OK).
  Спека: §3.2 (SAN, идемпотентность, echo), AC4.

- [ ] **Шаг 2.2. compose-комментарий + .env.example**

  Действие: `deploy/docker-compose.yml`, сервис `owns3` (строки 260–276) — после блока `volumes: - owns3-data:/data` добавить комментарий-инструкцию (сервисы owns3/owns3-ec НЕ меняются функционально):
  ```yaml
    # TLS-опция ПРОД-инсталляции (t42, arch/owns3/05 §6): раскомментировать —
    # грань станет HTTPS-only на том же порту 9000 (серт owns3 из deploy/tls/gen.sh,
    # PEM-альтернатива — OWNS3_TLS_CERT/KEY). Добавить строку к volumes выше и
    # ключи к environment выше. Стенд живёт HTTP-only — не включать.
    #   - ./tls:/tls:ro
    # environment:
    #   OWNS3_TLS_CERT_PATH: /tls/owns3.crt
    #   OWNS3_TLS_KEY_PATH: /tls/owns3.key
  ```
  `deploy/.env.example`:
  (а) в секции ownS3 (после `#OWNS3_EC_HOST_PORT=9011`, строка 127) добавить:
  ```
  # TLS-опция прод-инсталляции (t42, arch/owns3/05 §4/§6): серт owns3 выпускает
  # `bash deploy/tls/gen.sh`; сервис compose owns3 монтирует TLS-каталог ro и
  # получает OWNS3_TLS_CERT_PATH/_KEY_PATH (комментарий-инструкция в
  # deploy/docker-compose.yml). Стенд — HTTP, не задавать.
  #OWNS3_TLS_CERT_PATH=/tls/owns3.crt
  #OWNS3_TLS_KEY_PATH=/tls/owns3.key
  ```
  (б) в секции PGW_BACKUP_* (после `#PGW_BACKUP_S3_ADVERTISED_ENDPOINT=`, строка 92) добавить:
  ```
  # Доверие CA при https S3-endpoint (t42, arch/19 §7): путь ca.pem ВНУТРИ контейнера
  # воркера (ro-монтирование TLS-пакета установки, /tls/ca.pem). SDK-клиент воркера —
  # фабрика доверия из CaBundlePath (BackupS3), джобы/агенты — S3_CA_PEM;
  # операторский mc — SSL_CERT_FILE=<ca.pem>. Стенд (http-endpoint) — не задавать.
  #PGW_BACKUP_S3_CA_BUNDLE_PATH=/tls/ca.pem
  ```
  Выход: инструкции на месте; стендовые сервисы не изменились.
  Проверка (фон; `deploy/.env` в worktree отсутствует — gitignored, а интерполяция обязательных `${VAR:?…}` падает ДО валидации YAML, поэтому задаём все обязательные переменные команды, значения-заглушки): `PGW_PG_SUPERUSER_PASSWORD=x PGW_PG_STANDBY_PASSWORD=x PGW_BUCKET_ADMIN_PASSWORD=x PGW_BUCKET_MOVER_PASSWORD=x OWNS3_ROOT_USER=x OWNS3_ROOT_PASSWORD=y docker compose -f deploy/docker-compose.yml config -q` → exit 0.
  Спека: §3.3, §3.4 (операторский mc — комментарий), AC6.

- [ ] **Шаг 2.3. Коммит**

  Действие:
  ```bash
  git add deploy/tls/gen.sh deploy/docker-compose.yml deploy/.env.example
  git commit -m "t42: deploy — серт owns3 в TLS-пакете, инструкция прод-TLS, env-шаблон CA"
  ```
  Проверка: `git status --porcelain` (фон) → пусто.
  Спека: фаза 3 закрыта.

---

### Task 3: воркер — S3CaBundlePath, хелпер BackupCaPem, фабрика доверия S3CaHttpClientFactory

**Вход:** Task 2 слит.

**Выход:** конфиг `PgWorker:Backups:S3:CaBundlePath` (env `PGW_BACKUP_S3_CA_BUNDLE_PATH` через compose) → runtime-опция `S3CaBundlePath`; `BackupS3` при заданном пути ставит SDK-фабрику HttpClient `S3CaHttpClientFactory` — валидация корня цепочки сервера по отпечатку per-install CA; путь не задан — фабрика не ставится, поведение прежнее. Program воркера env процесса НЕ трогает (механизм `AWS_CA_BUNDLE` не используется — линейкой AWSSDK.Core 3.7.x не читается, эмпирический факт E2E t42; spec §2 п.5/§5). Юниты (прокидывание опций, хелпер, фабрика) зелёные.

**Спека:** §3.4 (воркер: опции + фабрика в BackupS3), §2 п.5, §5 (механизм фабрики); AC5 (первая половина). Канон: arch/19 §7/§9.

**Files:**
- Modify: `src/PgWorker.Backups/Options.cs` — `BackupsRuntimeOptions` (строки 7–64).
- Create: `src/PgWorker.Backups/BackupCaPem.cs`.
- Create: `src/PgWorker.Backups/S3CaHttpClientFactory.cs`.
- Modify: `src/PgWorker.Backups/BackupS3.cs` — конструктор ставит фабрику при `S3CaBundlePath`.
- Modify: `src/PgWorker.App/Options.cs` — `BackupsS3Options` (строки 618–638) и `ToRuntime()` (строки 472–510).
- Modify: `deploy/docker-compose.yml` — `x-pgworker-env` (после строки 54).
- Test: `src/tests/PgWorker.UnitTests/App/BackupsOptionsTests.cs` — дополнить; `src/tests/PgWorker.UnitTests/Backups/BackupCaPemTests.cs`, `src/tests/PgWorker.UnitTests/Backups/BackupS3CaTests.cs` — новые.

**Interfaces (Produces):**
```csharp
namespace PgWorker.Backups;
// BackupsRuntimeOptions.S3CaBundlePath : string? (null = http/прежде); BackupsS3Options.CaBundlePath : string?
public static class BackupCaPem
{
    public const string EnvName = "S3_CA_PEM";
    // null — путь не задан; нечитаемый/пустой файл — InvalidOperationException (диагностика с путём).
    public static string? ReadIfConfigured(BackupsRuntimeOptions opts);
}
// Фабрика SDK-доверия CA: HttpClient с валидацией корня цепочки сервера по отпечатку CA.
public static class S3CaHttpClientFactory
{
    public static HttpClient CreateHttpClient(X509Certificate2 ca);
}
```

- [ ] **Шаг 3.1. Тест прокидывания (красный)**

  Действие: в `src/tests/PgWorker.UnitTests/App/BackupsOptionsTests.cs` дописать (образец соседних `ToRuntime_*` тестов, файлы 128–170):
  ```csharp
  // AAA: CaBundlePath прокидывается в runtime (t42, arch/19 §7) — null по умолчанию
  [Fact]
  public void ToRuntime_склеивает_CaBundlePath()
  {
      var options = new PgWorkerOptions
      {
          Backups = new BackupsOptions { S3 = new BackupsS3Options { CaBundlePath = "/tls/ca.pem" } },
      };
      options.Backups.ToRuntime().S3CaBundlePath.Should().Be("/tls/ca.pem");
  }

  [Fact]
  public void ToRuntime_без_CaBundle_дает_null()
  {
      new BackupsOptions().ToRuntime().S3CaBundlePath.Should().BeNull();
  }
  ```
  Выход: тесты не собираются (поля нет).
  Проверка: `dotnet build src/tests/PgWorker.UnitTests -c Debug` (фон) → FAIL компиляции.
  Спека: §3.4 (поле S3CaBundlePath в опциях).

- [ ] **Шаг 3.2. Опции + хелпер BackupCaPem + compose**

  Действие:
  (а) `src/PgWorker.Backups/Options.cs` — в `BackupsRuntimeOptions` после `bool S3PathStyle = true,` добавить:
  ```csharp
  // t42 (arch/19 §7): путь ca.pem per-install CA — доверие https S3-endpoint;
  // null — http-endpoint, поведение прежнее. BackupS3 при заданном пути ставит
  // фабрику доверия S3CaHttpClientFactory (шаг 3.3).
  string? S3CaBundlePath = null,
  ```
  (б) `src/PgWorker.App/Options.cs` — в `BackupsS3Options` после `AdvertisedEndpoint` (строка 637):
  ```csharp
  /// <summary>Путь CA-bundle (ca.pem) для доверия https S3-endpoint (t42,
  /// arch/19 §7; env PGW_BACKUP_S3_CA_BUNDLE_PATH → compose-биндинг).
  /// Пусто — http-endpoint, фабрика доверия не ставится.</summary>
  public string? CaBundlePath { get; set; }
  ```
  в `ToRuntime()` после `S3PathStyle: S3.PathStyle,` добавить `S3CaBundlePath: S3.CaBundlePath,`.
  (в) Создать `src/PgWorker.Backups/BackupCaPem.cs` — чтение CA-PEM для фабрики и проброса в контейнеры (используется и шагом 3.3, и Task 4):
  ```csharp
  namespace PgWorker.Backups;

  /// <summary>Материал CA https S3-endpoint (t42, arch/19 §7): воркер читает
  /// ca.pem по S3CaBundlePath (фабрика доверия — шаг BackupS3, проброс в
  /// контейнеры — env S3_CA_PEM значением PEM, multi-host). null — путь не
  /// задан (http-endpoint, прежний env-набор).</summary>
  public static class BackupCaPem
  {
      public const string EnvName = "S3_CA_PEM";

      /// <summary>null — CaBundlePath не задан; нечитаемый/пустой файл — исключение
      /// (фабрика/спека джоба падает с диагностикой, а не молча без доверия CA).</summary>
      public static string? ReadIfConfigured(BackupsRuntimeOptions opts)
      {
          if (string.IsNullOrWhiteSpace(opts.S3CaBundlePath))
              return null;
          string pem;
          try
          {
              pem = File.ReadAllText(opts.S3CaBundlePath);
          }
          catch (Exception ex)
          {
              throw new InvalidOperationException(
                  $"PgWorker:Backups:S3:CaBundlePath={opts.S3CaBundlePath}: файл CA не читается — {ex.Message}", ex);
          }

          return pem.Trim() is { Length: > 0 } trimmed
              ? trimmed
              : throw new InvalidOperationException(
                  $"PgWorker:Backups:S3:CaBundlePath={opts.S3CaBundlePath}: файл CA пуст");
      }
  }
  ```
  Тест `src/tests/PgWorker.UnitTests/Backups/BackupCaPemTests.cs` (AAA-комментарии):
  ```csharp
  using PgWorker.Backups;

  namespace PgWorker.UnitTests.Backups;

  // CA-PEM (t42, arch/19 §7): без пути — null; файл — Trim-PEM;
  // нет файла/пустой — исключение с путём в диагностике.
  public class BackupCaPemTests
  {
      // AAA: путь не задан — null (http-endpoint, прежний env-набор)
      [Fact]
      public void NoPath_ReturnsNull()
      {
          BackupCaPem.ReadIfConfigured(new BackupsRuntimeOptions()).Should().BeNull();
      }

      // AAA: валидный файл — содержимое с Trim
      [Fact]
      public void ValidFile_ReturnsTrimmedPem()
      {
          var path = Path.Combine(Path.GetTempPath(), $"ca-{Guid.NewGuid():N}.pem");
          File.WriteAllText(path, "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----\n");
          BackupCaPem.ReadIfConfigured(new BackupsRuntimeOptions { S3CaBundlePath = path })
              .Should().Be("-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----");
      }

      // AAA: файла нет — исключение с путём
      [Fact]
      public void MissingFile_Throws()
      {
          var path = Path.Combine(Path.GetTempPath(), $"ca-{Guid.NewGuid():N}.pem");
          var act = () => BackupCaPem.ReadIfConfigured(new BackupsRuntimeOptions { S3CaBundlePath = path });
          act.Should().Throw<InvalidOperationException>().WithMessage("*не читается*");
      }

      // AAA: пустой файл — исключение
      [Fact]
      public void EmptyFile_Throws()
      {
          var path = Path.Combine(Path.GetTempPath(), $"ca-{Guid.NewGuid():N}.pem");
          File.WriteAllText(path, "  \n");
          var act = () => BackupCaPem.ReadIfConfigured(new BackupsRuntimeOptions { S3CaBundlePath = path });
          act.Should().Throw<InvalidOperationException>().WithMessage("*пуст*");
      }
  }
  ```
  (г) `deploy/docker-compose.yml` — в `x-pgworker-env` после строки `PgWorker__Backups__S3__AdvertisedEndpoint` (54) добавить:
  ```yaml
    # Доверие CA https S3-endpoint (t42, arch/19 §7): путь ca.pem в контейнере
    # (ro-монтирование TLS-пакета, прод) — SDK-клиент воркера строит фабрику
    # доверия из CaBundlePath (BackupS3); пусто — http, прежнее поведение.
    PgWorker__Backups__S3__CaBundlePath: ${PGW_BACKUP_S3_CA_BUNDLE_PATH:-}
  ```
  Выход: путь доходит до runtime-опций; хелпер чтения с диагностикой.
  Проверка (фон): `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~BackupsOptionsTests|FullyQualifiedName~BackupCaPemTests"` → PASS; `PGW_PG_SUPERUSER_PASSWORD=x PGW_PG_STANDBY_PASSWORD=x PGW_BUCKET_ADMIN_PASSWORD=x PGW_BUCKET_MOVER_PASSWORD=x OWNS3_ROOT_USER=x OWNS3_ROOT_PASSWORD=y docker compose -f deploy/docker-compose.yml config -q` → exit 0 (обязательные env-заглушки — см. примечание шага 2.2).
  Спека: §3.4 (опции, BackupCaPem), AC5.

- [ ] **Шаг 3.3. Фабрика доверия S3CaHttpClientFactory + BackupS3 (тест → код)**

  Действие (код): создать `src/PgWorker.Backups/S3CaHttpClientFactory.cs` — программное SDK-доверие per-install CA (spec §2 п.5/§5, arch/19 §7):
  ```csharp
  // Фабрика HttpClient для AWS SDK (t42, arch/19 §7): HttpClientHandler с
  // ServerCertificateCustomValidationCallback — строится цепочка серверного
  // сертификата (leaf, ExtraStore=[CA], AllowUnknownCertificateAuthority, без
  // revocation — внутренний контур) и корень цепочки сверяется с отпечатком
  // (SHA256) per-install CA; macOS без стора строит цепочку leaf-only —
  // эмпирика E2E t42, сверка идёт по элементам цепочки. env AWS_CA_BUNDLE
  // линейкой AWSSDK.Core 3.7.x не читается — потому фабрика, а не env.
  public static HttpClient CreateHttpClient(X509Certificate2 ca) { ... }
  ```
  (Тело — `HttpClientHandler` + callback сверки отпечатка; полный код — по фактической реализации ветки, юниты ниже фиксируют контракт.)
  В `src/PgWorker.Backups/BackupS3.cs` — в конструкторе, при `options.S3CaBundlePath is { Length: > 0 }`:
  ```csharp
  // Доверие CA https S3-endpoint (t42, arch/19 §7): фабрика ставится ТОЛЬКО при
  // заданном CaBundlePath (без пути — прежнее поведение); CA читает BackupCaPem
  // (нечитаемый/пустой файл — исключение с диагностикой старта/фабрики).
  var caPem = BackupCaPem.ReadIfConfigured(options);
  if (caPem is not null)
      config.HttpClientFactory = new S3CaHttpClientFactoryHolder(
          Shared.Tls.TlsMaterial.LoadPem(caPem));
  ```
  (обёртка-держатель `S3CaHttpClientFactoryHolder : IHttpClientFactory` — `CreateHttpClient` SDK; CA — `TlsMaterial.LoadPem(BackupCaPem.ReadIfConfigured(options))`).
  Действие (тесты): `src/tests/PgWorker.UnitTests/Backups/BackupS3CaTests.cs` — кейсы (AAA):
  - `Factory_TrustedServerCert_Accepted`: тестовая CA (RSA, self-signed) + серверный серт, подписанный ею (`CertificateRequest` + `Create`, образец NewPki шага 5.1) — валидация фабрики принимает цепочку;
  - `Factory_ForeignCA_Rejected`: серверный серт ДРУГОЙ CA — валидация отклоняет (отпечаток не совпал);
  - `BackupS3_CaBundlePathSet_FabrikaСтавится`: конструктор `BackupS3` с `S3CaBundlePath` → файл-CA — исключения нет (фабрика собрана); нечитаемый путь — `InvalidOperationException` с диагностикой `BackupCaPem`;
  - `BackupS3_WithoutCaBundle_Обычное поведение`: путь null — конструктор без фабрики (наблюдаемо: клиент создаётся, исключений нет).
  Выход: SDK-доверие CA программной фабрикой; без пути — прежнее поведение.
  Проверка: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter FullyQualifiedName~BackupS3CaTests` (фон) → PASS.
  Спека: §3.4 (BackupS3 — фабрика S3CaHttpClientFactory), §5 (механизм: отпечаток, ExtraStore, без revocation), AC5 (юниты фабрики).

- [ ] **Шаг 3.4. Коммит**

  Действие:
  ```bash
  git add src/PgWorker.Backups src/PgWorker.App/Options.cs deploy/docker-compose.yml src/tests/PgWorker.UnitTests
  git commit -m "t42: воркер — CaBundlePath + фабрика доверия S3CaHttpClientFactory в BackupS3"
  ```
  Проверка: `git status --porcelain` → пусто.
  Спека: фаза 4 (часть «воркер»).

---

### Task 4: S3_CA_PEM — спеки джобов, агент, приёмник, bash-материализация

**Вход:** Task 3 слит (`S3CaBundlePath` + `BackupCaPem` + фабрика в `BackupS3`).

**Выход:** все контейнеры бэкапов (джоб полного, verify, restore, drill, wal-агент, приёмник) получают env `S3_CA_PEM` при заданном `S3CaBundlePath` и НЕ получают без него (env-диф прежний); bash-скрипты/скрипты команд материализуют PEM → файл + `SSL_CERT_FILE` (mc); WalReceiver материализует PEM → файл → `S3CaBundlePath` (SDK-клиент приёмника — та же фабрика доверия из Task 3).

**Спека:** §3.4 (агенты/джобы), §2 п.6 (multi-host: PEM значением); AC5 (вторая половина). Канон: arch/19 §7.

**Files:**
- Modify: `src/PgWorker.Backups/Job/BackupJobSpec.cs`, `Job/VerifyJobSpec.cs`, `Restore/RestoreJobSpec.cs`, `Drill/DrillJobSpec.cs`, `WalStreamProcess.cs` (AgentEnv, строки 451–474), `VerifyJobCommand.cs` (script), `Restore/RestoreJobCommand.cs` (Script), `docker/backup/entrypoint.sh`.
- Modify: `src/PgWorker.WalReceiver/WalReceiverOptions.cs`, `Program.cs`.
- Test: дополнить `BackupJobSpecTests.cs` (полный джоб И verify-спека — verify-тесты живут в этом файле, отдельного `VerifyJobSpecTests.cs` в репо нет), `RestoreJobSpecTests.cs`, `DrillJobSpecTests.cs`, `WalReceiverOptionsTests.cs`, `WalAgentFactsTests.cs`.

**Interfaces (Produces):** env-контракт `S3_CA_PEM` (источник имени — `BackupCaPem.EnvName`, Task 3); `WalReceiverOptions.S3CaPem : string?` (env `S3_CA_PEM`, опциональный).

- [ ] **Шаг 4.1. Спеки джобов и AgentEnv передают S3_CA_PEM (тест → код)**

  Действие (тесты): дополнить по паре кейсов (с заданным `S3CaBundlePath` → env несёт PEM-значение; без — ключа нет, юнит-диф AC5) в `BackupJobSpecTests.cs` — ДВЕ пары: для `BackupJobSpec.Build` и для `VerifyJobSpec.Build` (verify-спека тестируется этим же файлом — отдельного `VerifyJobSpecTests.cs` нет), и по одной паре в `RestoreJobSpecTests.cs`, `DrillJobSpecTests.cs`; плюс `WalAgentFactsTests.cs` для `AgentEnv`. Общий паттерн (пример для BackupJobSpecTests):
  ```csharp
  // AAA: CaBundlePath задан → env несёт S3_CA_PEM значением PEM (t42, arch/19 §7)
  [Fact]
  public void Build_CaBundle_ПередаётPemЗначением()
  {
      var caPath = Path.Combine(Path.GetTempPath(), $"ca-{Guid.NewGuid():N}.pem");
      File.WriteAllText(caPath, "-----BEGIN CERTIFICATE-----\nxyz\n-----END CERTIFICATE-----\n");
      var spec = BackupJobSpec.Build(_opts with { S3CaBundlePath = caPath },
          "demo", "s1", "id1", new NodeAddress("h", new NodePorts(5432, 8008, 6432)), "pw");
      spec.Env!["S3_CA_PEM"].Should().Be("-----BEGIN CERTIFICATE-----\nxyz\n-----END CERTIFICATE-----");
  }

  // AAA: без CaBundlePath env-набор прежний — S3_CA_PEM отсутствует (юнит-диф, AC5)
  [Fact]
  public void Build_БезCaBundle_НетS3CaPem()
  {
      var spec = BackupJobSpec.Build(_opts, "demo", "s1", "id1",
          new NodeAddress("h", new NodePorts(5432, 8008, 6432)), "pw");
      spec.Env!.Should().NotContainKey("S3_CA_PEM");
  }

  // AAA: verify-спека — CaBundlePath задан → S3_CA_PEM значением; без — нет
  [Fact]
  public void VerifyJobSpec_CaBundle_ПередаётPemЗначением()
  {
      var caPath = Path.Combine(Path.GetTempPath(), $"ca-{Guid.NewGuid():N}.pem");
      File.WriteAllText(caPath, "-----BEGIN CERTIFICATE-----\nxyz\n-----END CERTIFICATE-----\n");
      var spec = VerifyJobSpec.Build(_opts with { S3CaBundlePath = caPath }, "c1", "s1", "id1");
      spec.Env!["S3_CA_PEM"].Should().Be("-----BEGIN CERTIFICATE-----\nxyz\n-----END CERTIFICATE-----");
  }

  [Fact]
  public void VerifyJobSpec_БезCaBundle_НетS3CaPem()
  {
      var spec = VerifyJobSpec.Build(_opts, "c1", "s1", "id1");
      spec.Env!.Should().NotContainKey("S3_CA_PEM");
  }
  ```
  Для `WalAgentFactsTests.cs` — `WalStreamProcess.AgentEnv` сделать `internal static` (InternalsVisibleTo уже есть) и тест:
  ```csharp
  // AAA: AgentEnv передаёт S3_CA_PEM при заданном пути; без пути — не передаёт
  [Fact]
  public void AgentEnv_CaBundle_ПередачаПравильная()
  {
      var caPath = Path.Combine(Path.GetTempPath(), $"ca-{Guid.NewGuid():N}.pem");
      File.WriteAllText(caPath, "PEM-BODY");
      var withCa = WalStreamProcess.AgentEnv(
          new BackupsRuntimeOptions { S3CaBundlePath = caPath }, "c1", "s1", "slot", "h", 5432, "pw");
      var withoutCa = WalStreamProcess.AgentEnv(
          new BackupsRuntimeOptions { S3CaBundlePath = null }, "c1", "s1", "slot", "h", 5432, "pw");
      withCa["S3_CA_PEM"].Should().Be("PEM-BODY");
      withoutCa.Should().NotContainKey("S3_CA_PEM");
  }
  ```
  Действие (код): в `BackupJobSpec.Build`, `VerifyJobSpec.Build`, `RestoreJobSpec.Build`, `DrillJobSpec.Build` — после сборки `env` перед `return new ContainerSpec(...)`:
  ```csharp
  // t42 (arch/19 §7): CA https-endpoint — PEM значением (multi-host), материализация внутри контейнера
  var caPem = BackupCaPem.ReadIfConfigured(opts);
  if (caPem is not null)
      env[BackupCaPem.EnvName] = caPem;
  ```
  В `WalStreamProcess.AgentEnv` — то же перед `return env;`. Изменить модификатор `private static IReadOnlyDictionary... AgentEnv` → `internal static`.
  Выход: env-контракт расширен условно; все четыре спеки имеют юнит-свидетельства.
  Проверка: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~JobSpecTests|FullyQualifiedName~WalAgentFactsTests"` (фон) → PASS.
  Спека: §3.4 (спеки джобов/агентов), AC5 (юнит-диф).

- [ ] **Шаг 4.2. Bash-материализация: entrypoint.sh + VerifyJobCommand + RestoreJobCommand**

  Действие:
  (а) `docker/backup/entrypoint.sh` — после `set -u` (строка 5) добавить:
  ```sh
  # Доверие CA https-endpoint (t42, arch/19 §7): PEM из env — в файл + SSL_CERT_FILE (mc, Go).
  if [ -n "${S3_CA_PEM:-}" ]; then
    printf '%s\n' "$S3_CA_PEM" > /tmp/s3-ca.pem
    export SSL_CERT_FILE=/tmp/s3-ca.pem
  fi
  ```
  (б) `src/PgWorker.Backups/VerifyJobCommand.cs` — в `Build()` script сразу после `set -euo pipefail`:
  ```bash
  # Доверие CA https-endpoint (t42, arch/19 §7): PEM из env — в файл + SSL_CERT_FILE (mc)
  if [ -n "${S3_CA_PEM:-}" ]; then printf '%s\n' "$S3_CA_PEM" > /tmp/s3-ca.pem; export SSL_CERT_FILE=/tmp/s3-ca.pem; fi
  ```
  (в) `src/PgWorker.Backups/Restore/RestoreJobCommand.cs` — тот же пролог в `Script` после `set -euo pipefail` (export наследуется `mc cp` и `restore-wal.sh` через exec mc; drill использует тот же скрипт).
  Действие (тесты): дополнить `VerifyJobCommandTests.cs` и `RestoreJobCommandTests.cs`:
  ```csharp
  // AAA: скрипт материализует S3_CA_PEM → SSL_CERT_FILE (t42, arch/19 §7)
  [Fact]
  public void Build_МатериализуетCaPem_и_SslCertFile()
  {
      VerifyJobCommand.Build()[2].Should().Contain("S3_CA_PEM").And.Contain("SSL_CERT_FILE");
  }
  ```
  (для Restore — `RestoreJobCommand.Build()`).
  Выход: mc-подпроцессы доверяют CA.
  Проверка: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter "FullyQualifiedName~VerifyJobCommandTests|FullyQualifiedName~RestoreJobCommandTests"` (фон) → PASS.
  Спека: §3.4 (entrypoint, verify/restore/drill-команды).

- [ ] **Шаг 4.3. WalReceiver: S3_CA_PEM → файл → S3CaBundlePath (фабрика Task 3)**

  Действие:
  (а) `src/PgWorker.WalReceiver/WalReceiverOptions.cs` — в record после `bool S3PathStyle`:
  ```csharp
  // t42 (arch/19 §7): PEM CA https S3-endpoint (материализуется Program приёмника
  // в файл → S3CaBundlePath; SDK-клиент приёмника — фабрика из Task 3); null — http-endpoint.
  string? S3CaPem = null);
  ```
  в `WalReceiverEnv.Parse` после `var s3SecretKey = Require("S3_SECRET_KEY");`:
  ```csharp
  var s3CaPem = env("S3_CA_PEM"); // опциональный (t42): отсутствие — http-endpoint
  ```
  и в конструктор результата: `..., s3PathStyle, string.IsNullOrWhiteSpace(s3CaPem) ? null : s3CaPem);` (порядок аргументов — по позиции нового поля).
  (б) `src/PgWorker.WalReceiver/Program.cs` — после Parse/валидации, ПЕРЕД `new BackupS3(...)`: PEM материализуется в файл и передаётся как `S3CaBundlePath` — `BackupS3` приёмника ставит ту же фабрику доверия (`S3CaHttpClientFactory`, Task 3); env `AWS_CA_BUNDLE` НЕ используется (линейкой AWSSDK.Core 3.7.x не читается, spec §5):
  ```csharp
  // Доверие CA https S3-endpoint (t42, arch/19 §7): PEM материализуется в файл
  // (multi-host: файла хоста воркера в контейнере нет) и передаётся как
  // S3CaBundlePath — BackupS3 приёмника ставит фабрику S3CaHttpClientFactory.
  string? caBundlePath = null;
  if (options.S3CaPem is { Length: > 0 })
  {
      caBundlePath = Path.Combine(Path.GetTempPath(), $"s3-ca-{Guid.NewGuid():N}.pem");
      await File.WriteAllTextAsync(caBundlePath, options.S3CaPem + "\n", cts.Token);
  }

  // BackupS3 из S3-полей приёмника (реюз математики/клиента Backups);
  // S3CaBundlePath — путь материализованного CA (фабрика доверия).
  var s3Options = new BackupsRuntimeOptions(
      S3Endpoint: options.S3Endpoint,
      S3Region: options.S3Region,
      S3Bucket: options.S3Bucket,
      S3AccessKey: options.S3AccessKey,
      S3SecretKey: options.S3SecretKey,
      S3PathStyle: options.S3PathStyle,
      S3CaBundlePath: caBundlePath);
  await using var s3 = new BackupS3(s3Options);
  ```
  (заменяет прежний блок `Environment.SetEnvironmentVariable("AWS_CA_BUNDLE", ...)` — фактическая реализация ветки).
  (в) Тесты `WalReceiverOptionsTests.cs`:
  ```csharp
  // AAA: S3_CA_PEM опционален — без него S3CaPem null (http-endpoint, прежний контракт)
  [Fact]
  public void Parse_БезS3CaPem_ДаетNull()
  {
      WalReceiverEnv.Parse(k => Valid().TryGetValue(k, out var v) ? v : null, out _)
          !.S3CaPem.Should().BeNull();
  }

  // AAA: S3_CA_PEM передан — попадает в опции значением
  [Fact]
  public void Parse_S3CaPem_Значение()
  {
      var parsed = WalReceiverEnv.Parse(k => With("S3_CA_PEM", "PEM-BODY").TryGetValue(k, out var v) ? v : null, out _);
      parsed!.S3CaPem.Should().Be("PEM-BODY");
  }
  ```
  Выход: приёмник доверяет CA той же фабрикой, что и воркер (без правки `WalReceiverCore`).
  Проверка: `dotnet test src/tests/PgWorker.UnitTests -c Debug --filter FullyQualifiedName~WalReceiverOptionsTests` (фон) → PASS.
  Спека: §3.4 (WalStreamProcess.AgentEnv + WalReceiver — та же фабрика доверия).

- [ ] **Шаг 4.4. Полные юниты + коммит**

  Действие: `dotnet test src/tests/PgWorker.UnitTests -c Debug` (фон) → PASS целиком.
  ```bash
  git add src/PgWorker.Backups src/PgWorker.WalReceiver docker/backup/entrypoint.sh src/tests/PgWorker.UnitTests
  git commit -m "t42: S3_CA_PEM — проброс CA в джобы/агенты/приёмник, материализация SSL_CERT_FILE/CaBundlePath"
  ```
  Проверка: `git status --porcelain` → пусто; юниты зелёные.
  Спека: фаза 4 закрыта.

---

### Task 5: E2E TLS-сценарии OwnS3

**Вход:** Tasks 1–4 слиты; образ `owns3:e2e` несёт TLS-код (`OwnS3E2eImage.BuildOnceAsync` соберёт из текущего кода).

**Выход:** `src/tests/OwnS3.IntegrationTests/E2e` — TLS-серия: собственный контур (своя CA+серт `CertificateRequest` на хосте теста, SAN localhost/127.0.0.1/имя контейнера; ro bind-mount сертов; динамический порт), SDK-smoke с per-install CA тестовым зерналом фабрики воркера (`TlsCaHttpClientFactory`, тот же механизм — spec §3.5; бакет-операции — root-клиент, объектные — writer, по матрице прав arch/owns3/05 §3), `mc` с `SSL_CERT_FILE`, негативные кейсы (клиент без доверия; HTTP на HTTPS-порт; docker-кейс fail-fast — контейнер с одним `OWNS3_TLS_CERT` умирает с exit ≠ 0 и Critical-диагностикой), полный teardown + ассерт чистоты.

**Спека:** §3.5 (E2E, вкл. «SDK с per-install CA — программная валидация цепочки, тот же механизм, что у фабрики воркера»), AC2, AC3 (docker-ветка Program), AC7. Канон: docs/e2e-isolation.md, docs/e2e-launch.md, AGENTS.md (E2E-правила).

**Files:**
- Create: `src/tests/OwnS3.IntegrationTests/E2e/OwnS3TlsE2eEnvironment.cs`, `AwsSdkTlsScenarios.cs`, `McTlsScenarios.cs`, `TlsNegativeScenarios.cs`.

**Interfaces (Consumes):** `OwnS3E2eImage.BuildOnceAsync(artifactsDir, ct)` (общая сборка образа), `DockerTrait.SkipIfUnavailable()`, коллекция `OwnS3E2eCollection.Name` (последовательность серий).

- [ ] **Шаг 5.1. TLS-окружение E2E + тестовое зеркало фабрики**

  Действие: создать `OwnS3TlsE2eEnvironment.cs` по образцу `OwnS3E2eEnvironment` (скопировать каркас: runId/os3-en-/os3-vd-/os3-es- префиксы, [PHASE]-телеметрия, MarkFailed/README-cleanup, retry×3, teardown-ассерты own-only — строки 27–404 эталона).

  Using-блок нового файла — каркас эталона (`System.Diagnostics`, `System.Net.Sockets`, `DotNet.Testcontainers.*`, `FluentAssertions`, `XUnit`) этих пространств имён НЕ содержит (эталон пишет `System.Net.IPAddress` с полной квалификацией); без дополнений — CS0246 на `IPAddress` и крипто-типы сниппета:
  ```csharp
  using System.Net;                                    // IPAddress (SAN)
  using System.Security.Cryptography;                  // RSA, HashAlgorithmName, RSASignaturePadding
  using System.Security.Cryptography.X509Certificates; // CertificateRequest, SubjectAlternativeNameBuilder, X509*
  ```

  Отличия от каркаса — генерация тестовой PKI (BCL, на хосте теста): CA + серверный серт:
  ```csharp
  private static (string CaPem, string CertPem, string KeyPem) NewPki(string serverDns)
  {
      using var caRsa = RSA.Create(2048);
      var caReq = new CertificateRequest("CN=os3-e2e-ca", caRsa,
          HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
      caReq.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
      using var ca = caReq.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(2));
      using var srvRsa = RSA.Create(2048);
      var srvReq = new CertificateRequest("CN=os3-e2e-server", srvRsa,
          HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
      var san = new SubjectAlternativeNameBuilder();
      san.AddDnsName("localhost");
      san.AddIpAddress(IPAddress.Loopback);
      san.AddDnsName(serverDns); // имя контейнера — mc изнутри сети
      srvReq.CertificateExtensions.Add(san.Build());
      // serial: byte[] → неявная конвертация в ReadOnlyMemory<byte> параметра Create
      // (using НЕЛЬЗЯ — byte[] не IDisposable, CS1674)
      var serial = BitConverter.GetBytes(DateTime.UtcNow.Ticks);
      using var srv = srvReq.Create(ca, ca.SubjectName, X509SignatureGenerator.Create(caRsa),
          DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(2), serial);
      return (ca.ExportCertificatePem(), srv.ExportCertificatePem(), srvRsa.ExportPkcs8PrivateKeyPem());
  }
  ```
  Контейнер: `WithBindMount(tlsDir, "/tls", AccessMode.ReadOnly)` — ro bind-mount каталога сертов (spec §3.5: «файлы — ro bind-mount»; `AccessMode` из `DotNet.Testcontainers.Builders`, трёхаргументная перегрузка), env `OWNS3_TLS_CERT_PATH=/tls/server.crt`, `OWNS3_TLS_KEY_PATH=/tls/server.key` (+root-пара/ключи как в эталоне) — тем самым env-биндинги `OWNS3_TLS_*_PATH` (spec §3.5) проверяются end-to-end именно здесь. Готовность — БЕЗ http-wait: после `StartAsync` поллить `/healthz` своим HttpClient с доверенным CA (новый `SocketsHttpHandler` + `SslClientAuthenticationOptions` с `RemoteCertificateValidationCallback` сверкой отпечатка CA) — бюджет 30 с, полл 1 с. Публичные члены: `S3BaseUrl => $"https://localhost:{_hostPort}"`, `S3InternalUrl => $"https://{_containerName}:9000"`, `CaPemPath` (файл ca.pem в tlsDir — для mc bind-mount), `CaPem` (PEM-строка — для тестовой фабрики доверия SDK). Каталог PKI живёт в `ArtifactsDir` и удаляется teardown'ом (в список README-cleanup).
  Тестовое зеркало фабрики воркера — в `OwnS3E2eClients.cs` публичная фабрика для TLS-окружения (spec §3.5: «тот же механизм, что у фабрики воркера»; фактическая реализация ветки — `TlsCaHttpClientFactory`):
  ```csharp
  /// <summary>TLS-окружение (t42): клиент против https-грани; доверие CA —
  /// тестовое зеркало фабрики воркера (TlsCaHttpClientFactory: валидация корня
  /// цепочки по отпечатку CA — тот же механизм, что S3CaHttpClientFactory
  /// BackupS3; env AWS_CA_BUNDLE SDK 3.7.x не читается — spec §3.5/§5).</summary>
  public static IAmazonS3 CreateS3(OwnS3TlsE2eEnvironment fx, OwnS3Role role,
      OwnS3ChecksumMode checksum = OwnS3ChecksumMode.WhenRequired)
  {
      // те же параметры, что private CreateS3(baseUrl, …): ServiceURL=fx.S3BaseUrl,
      // ForcePathStyle, us-east-1, таймаут, checksum-режим — плюс:
      // config.HttpClientFactory = TlsCaHttpClientFactory из fx.CaPem;
  }
  ```
  Выход: переиспользуемый TLS-контур с ro-монтированием сертов; SDK-доверие — зеркалом фабрики воркера.
  Проверка: `dotnet build src/tests/OwnS3.IntegrationTests -c Debug` (фон) → 0 errors.
  Спека: §3.5 (контур, CA/серт теста, ro bind-mount, динамический порт, env-биндинги OWNS3_TLS_*, SDK — тот же механизм фабрики).

- [ ] **Шаг 5.2. SDK TLS-smoke**

  Действие: `AwsSdkTlsScenarios.cs` (`[Collection(OwnS3E2eCollection.Name)]`, `DockerTrait.SkipIfUnavailable()`). Роли по матрице arch/owns3/05 §3 (образец эталона `AwsSdkSimpleScenarios.PutGet_Roundtrip`: root-клиент создаёт бакет, writer — объектные операции; CreateBucket/DeleteBucket для read-write запрещены). Доверие CA — фабрика из шага 5.1 внутри `CreateS3(fx, …)` (без управления env процесса):
  ```csharp
  // AAA: полный smoke по https — put/get/head/list/multipart + /healthz;
  // доверие — тестовое зеркало фабрики воркера (TlsCaHttpClientFactory — тот же
  // механизм, что S3CaHttpClientFactory BackupS3; t42 AC2, spec §3.5).
  // Бакет создаёт/удаляет root (матрица 05 §3), объекты/multipart — writer.
  [Fact]
  public async Task Sdk_CaFabrika_ПолныйSmoke()
  {
      DockerTrait.SkipIfUnavailable();
      var ct = TestContext.Current.CancellationToken;
      await OwnS3TlsE2eEnvironment.RunScenarioAsync("t1", async fx =>
      {
          using var root = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Root);
          using var s3 = OwnS3E2eClients.CreateS3(fx, OwnS3Role.Writer);
          var bucket = $"os3e2e{fx.RunTag}tls";
          await root.PutBucketAsync(new PutBucketRequest { BucketName = bucket }, ct);
          var body = OwnS3E2eBytes.Deterministic(256 * 1024, 42);
          await s3.PutObjectAsync(new PutObjectRequest
          { BucketName = bucket, Key = "smoke.bin", InputStream = new MemoryStream(body) }, ct);

          // GET: чтение тела — MemoryStream + CopyToAsync (эталон AwsSdkSimpleScenarios:83–86)
          using var get = await s3.GetObjectAsync(
              new GetObjectRequest { BucketName = bucket, Key = "smoke.bin" }, ct);
          using var ms = new MemoryStream();
          await get.ResponseStream.CopyToAsync(ms, ct);
          ms.ToArray().Should().Equal(body, "GET возвращает байты PUT без искажений");

          // HEAD — GetObjectMetadataRequest с BucketName+Key (эталон, строки 93–95)
          (await s3.GetObjectMetadataAsync(
                  new GetObjectMetadataRequest { BucketName = bucket, Key = "smoke.bin" }, ct))
              .ContentLength.Should().Be(256 * 1024);
          (await s3.ListObjectsV2Async(new ListObjectsV2Request { BucketName = bucket }, ct))
              .S3Objects.Should().Contain(o => o.Key == "smoke.bin");

          // multipart (2 части × 5 МиБ) — writer: read-write имеет право (05 §3)
          var upload = await s3.InitiateMultipartUploadAsync(
              new InitiateMultipartUploadRequest { BucketName = bucket, Key = "mp.bin" }, ct);
          var parts = new List<PartETag>();
          for (var i = 1; i <= 2; i++)
          {
              var resp = await s3.UploadPartAsync(new UploadPartRequest
              {
                  BucketName = bucket, Key = "mp.bin", UploadId = upload.UploadId, PartNumber = i,
                  InputStream = new MemoryStream(OwnS3E2eBytes.Deterministic(5 * 1024 * 1024, i)),
              }, ct);
              parts.Add(new PartETag { PartNumber = i, ETag = resp.ETag });
          }
          await s3.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
          { BucketName = bucket, Key = "mp.bin", UploadId = upload.UploadId, PartETags = parts }, ct);

          await s3.DeleteObjectAsync(new DeleteObjectRequest { BucketName = bucket, Key = "smoke.bin" }, ct);
          await s3.DeleteObjectAsync(new DeleteObjectRequest { BucketName = bucket, Key = "mp.bin" }, ct);
          await root.DeleteBucketAsync(new DeleteBucketRequest { BucketName = bucket }, ct);
          // /healthz по https доверенным HttpClient (хелпер окружения шага 5.1)
          (await fx.HealthzAsync(ct)).Should().BeTrue();
      }, ct);
  }
  ```
  (Бакет пустеет удалением обоих объектов перед DeleteBucket — незачистленный mp.bin давал бы 409 BucketNotEmpty.)
  Выход: SDK-клиенты (root + writer) проходят полный smoke по https в рамках матрицы прав; механизм доверия — зеркало фабрики воркера.
  Проверка: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter FullyQualifiedName~AwsSdkTlsScenarios` (фон) → PASS.
  Спека: AC2 (SDK smoke), §3.5; матрица — arch/owns3/05 §3.

- [ ] **Шаг 5.3. mc TLS-сценарий**

  Действие: `McTlsScenarios.cs` — по образцу `McScenarios.McAsync`, но docker run с CA:
  ```csharp
  private static Task<string> McTlsAsync(OwnS3TlsE2eEnvironment fx, string script, CancellationToken ct)
      => fx.RunDockerAsync(
      [
          "run", "--rm", "--network", fx.NetName,
          "-v", $"{fx.CaPemPath}:/tmp/s3-ca.pem:ro",
          "-e", "SSL_CERT_FILE=/tmp/s3-ca.pem",
          "--entrypoint", "/bin/sh", OwnS3E2eEnvironment.McImage,
          "-c", $"mc alias set t {fx.S3InternalUrl} '{OwnS3E2eEnvironment.RootUser}' " +
                $"'{OwnS3E2eEnvironment.RootPassword}' >/dev/null && {script}",
      ], ct);

  // AAA: mc доверяет CA через SSL_CERT_FILE (Go crypto/x509) — mb→cp→cat→rm→rb по https
  // (mb/rb — root-креды alias'а: матрица 05 §3, бакетные операции — admin/root)
  [Fact]
  public async Task Mc_SslCertFile_ПолныйЦикл()
  {
      DockerTrait.SkipIfUnavailable();
      var ct = TestContext.Current.CancellationToken;
      await OwnS3TlsE2eEnvironment.RunScenarioAsync("t2", async fx =>
      {
          var bucket = $"os3e2e{fx.RunTag}tmc";
          var output = await McTlsAsync(fx, $"""
              mc mb t/{bucket} \
                && head -c 65536 /dev/urandom > /tmp/b.bin \
                && SRC=$(sha256sum /tmp/b.bin | cut -d ' ' -f1) \
                && mc cp /tmp/b.bin t/{bucket}/o.bin >/dev/null \
                && GOT=$(mc cat t/{bucket}/o.bin | sha256sum | cut -d ' ' -f1) \
                && mc rm t/{bucket}/o.bin >/dev/null \
                && mc rb t/{bucket} >/dev/null \
                && echo "OS3E2E_SHA $SRC $GOT"
              """, ct);
          var shas = output.Split(' ', StringSplitOptions.RemoveEmptyEntries)
              .Where(t => t.Length == 64 && t.All(char.IsAsciiHexDigit)).ToList();
          shas.Should().HaveCountGreaterThanOrEqualTo(2);
          shas[0].Should().Be(shas[1], "roundtrip mc по https сходится побайтово");
      }, ct);
  }
  ```
  Выход: mc работает по https с `SSL_CERT_FILE`.
  Проверка: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter FullyQualifiedName~McTlsScenarios` (фон) → PASS.
  Спека: AC2 (mc), §3.5.

- [ ] **Шаг 5.4. Негативные кейсы + docker fail-fast**

  Действие: `TlsNegativeScenarios.cs`:
  ```csharp
  // AAA (1): клиент БЕЗ доверия CA получает TLS-отказ — грань валидирует цепочку
  [Fact]
  public async Task КлиентБезДоверия_Отказ()
  {
      DockerTrait.SkipIfUnavailable();
      var ct = TestContext.Current.CancellationToken;
      await OwnS3TlsE2eEnvironment.RunScenarioAsync("t3", async fx =>
      {
          using var http = new HttpClient(); // системный магазин — тестовой CA там нет
          var act = () => http.GetAsync($"{fx.S3BaseUrl}/healthz", ct);
          (await act.Should().ThrowAsync<HttpRequestException>())
              .WithMessage("*SSL*"); // SslAuthenticationException в inner — текст уточнить по факту прогона
      }, ct);
  }

  // AAA (2): обычный HTTP-запрос на HTTPS-порт — отказ, не тихий downgrade
  [Fact]
  public async Task HttpНаHttpsПорт_Отказ()
  {
      DockerTrait.SkipIfUnavailable();
      var ct = TestContext.Current.CancellationToken;
      await OwnS3TlsE2eEnvironment.RunScenarioAsync("t4", async fx =>
      {
          using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
          var httpUrl = fx.S3BaseUrl.Replace("https://", "http://", StringComparison.Ordinal);
          var act = () => http.GetAsync($"{httpUrl}/healthz", ct);
          await act.Should().ThrowAsync<Exception>(); // разрыв соединения TLS-листенером
      }, ct);
  }

  // AAA (3): fail-fast Program-ветки (AC3, вне WAF — см. план Task 1): контейнер
  // с одним OWNS3_TLS_CERT (нет ключа) умирает ненулевым exit-кодом, в логах —
  // Critical-диагностика с OWNS3_TLS; живой сервис не поднимается «молча на HTTP»
  [Fact]
  public async Task HalfPair_КонтейнерУмирает_СДиагностикой()
  {
      DockerTrait.SkipIfUnavailable();
      var ct = TestContext.Current.CancellationToken;
      await OwnS3TlsE2eEnvironment.RunScenarioAsync("t5", async fx =>
      {
          var name = $"os3-tlsfail-{fx.RunId}";
          try
          {
              // Act: отдельный контейнер (не сервис окружения) — образ уже собран окружением
              await fx.RunDockerAsync(
              [
                  "run", "--detach", "--name", name,
                  "-e", $"OWNS3_ROOT_USER={OwnS3E2eEnvironment.RootUser}",
                  "-e", $"OWNS3_ROOT_PASSWORD={OwnS3E2eEnvironment.RootPassword}",
                  "-e", "OWNS3_TLS_CERT=garbage-without-key", // один из пары
                  OwnS3E2eImage.Tag,
              ], ct);

              // поллинг exited ≤ 30 с (шаг 1 с) — fail-fast мгновенный
              string state = "";
              for (var i = 0; i < 30; i++)
              {
                  state = await fx.RunDockerAsync(
                      ["inspect", "--format", "{{.State.Status}} {{.State.ExitCode}}", name], ct);
                  if (state.StartsWith("exited", StringComparison.Ordinal))
                      break;
                  await Task.Delay(TimeSpan.FromSeconds(1), ct);
              }

              // Assert: exited с ненулевым кодом; в логах — Critical-диагностика
              state.Should().StartWith("exited ", "невалидный материал — отказ старта");
              var exitCode = state.Split(' ').ElementAt(1);
              exitCode.Should().NotBe("0", "fail-fast = exit ≠ 0");
              var logs = await fx.RunDockerAsync(["logs", name], ct);
              logs.Should().Contain("OWNS3_TLS").And.Contain("TLS", "диагностика указывает на TLS-материал");
          }
          finally
          {
              // Чистка — ЛУЧШЕЕ УСИЛИЕ (образец TryDockerAsync эталона): RunDockerAsync
              // бросает исключение при ненулевом exit docker (вкл. «No such container»,
              // если упал сам docker run) — исключение из finally маскировало бы
              // первопричину сценария; контейнер own-only по runId.
              try { await fx.RunDockerAsync(["rm", "-f", name], ct); }
              catch { /* уже нет / docker недоступен — чистка лучшим усилием */ }
          }
      }, ct);
  }
  ```
  (Контейнер fail-fast — own-only по runId, снос в finally сценария; teardown-ассерт чистоты окружения его не видит — имя несёт runId, контроль отдельной строкой не нужен: finally обязателен при любом исходе тела.)
  Выход: негативная семантика и Program-ветка fail-fast (Critical + exit ≠ 0) доказаны docker-кейсом.
  Проверка: `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests -c Release --filter FullyQualifiedName~TlsNegativeScenarios` (фон) → PASS; после серии контейнеров `os3-tlsfail-*` нет (`docker ps -a --format '{{.Names}}' | grep os3-tlsfail` пуст).
  Спека: AC2 (негативные), AC3 (docker-ветка), §3.5.

- [ ] **Шаг 5.5. Коммит**

  Действие:
  ```bash
  git add src/tests/OwnS3.IntegrationTests
  git commit -m "t42: E2E TLS-сценарии ownS3 — SDK-фабрика доверия, mc SSL_CERT_FILE, негативные и fail-fast кейсы"
  ```
  Проверка: `git status --porcelain` → пусто; после серии — `docker ps -a | grep os3-` пуст, `docker network ls | grep os3-en-` пуст (зачистка подтверждена).
  Спека: AC7.

---

### Task 6: прогоны, стенд, мерж-гейт

**Вход:** Tasks 1–5 слиты; код полон.

**Выход:** все серии зелёные; стенд поднимается и чеки 46/47 работают без изменений; маркер PgWorker E2E зелёный на свежем Release.

**Спека:** фаза 5; AC1, AC6, AC7, AC8.

- [ ] **Шаг 6.1. Юниты обеих подсистем**

  Действие/Проверка (фон, последовательно): `dotnet test src/tests/OwnS3.UnitTests -c Debug` → PASS; `dotnet test src/tests/PgWorker.UnitTests -c Debug` → PASS. Между сериями docker-остатков нет (юниты docker не поднимают).
  Выход: юнит-регресс зелёный.
  Спека: AC1, AC5.

- [ ] **Шаг 6.2. Полная серия OwnS3.IntegrationTests (Release, docker)**

  Действие/Проверка (фон): `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/tests/OwnS3.IntegrationTests -c Release` → PASS (Api + E2E HTTP + E2E TLS + EC). После финальной строки — контроль зачистки: `docker ps -a --format '{{.Names}}' | grep os3-` пуст; `docker network ls --format '{{.Name}}' | grep os3-en-` пуст; `docker volume ls --format '{{.Name}}' | grep os3-vd-` пуст. Остатки — разбор по логам (`/tmp/pgw-e2e-artifacts-<guid>/`), не перезапуск тестов без анализа.
  Выход: AC1 (существующие серии без правок) + AC2/AC3/AC7 (TLS-серия).
  Спека: AC1, AC2, AC3, AC7.

- [ ] **Шаг 6.3. Мерж-гейт: docker-E2E маркер PgWorker на свежем Release**

  Действие/Проверка (фон): `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test src/PgWorker.slnx -c Release --filter FullyQualifiedName~Scale_AddEmptyShard` → PASS (E2eFixture соберёт Release сам; инкрементальный no-op — секунды). Зачистка контейнеров/сетей серии после финальной строки (правило AGENTS.md).
  Выход: AC8.
  Спека: AC8, AGENTS.md (правило для задач, трогающих `src/PgWorker.Backups`).

- [ ] **Шаг 6.4. Стенд: подъём + чеки 46/47 + серт owns3**

  Действие (фон, последовательные команды): `bash dev-stand/images/pull-images.sh` (если давно не тянули) → `bash dev-stand/adminpanel/checks/00-up.sh` → `bash dev-stand/adminpanel/checks/46-owns3.sh` → `bash dev-stand/adminpanel/checks/47-owns3-ec.sh` → `ls -la deploy/tls/owns3.crt deploy/tls/owns3.key`.
  Проверка: чеки 46/47 зелёные (HTTP-only, без изменений); `owns3.crt/key` существуют после подъёма без действий оператора; `openssl verify -CAfile deploy/tls/ca.pem deploy/tls/owns3.crt` → OK. Отладочная зачистка после разбора — стенд не сносить (это общий dev-стенд), если не попросили.
  Выход: AC6.
  Спека: AC6, §3.3 (чеки без изменений).

- [ ] **Шаг 6.5. Финальный коммит служебных правок (если появились) и отчёт**

  Действие: при правках по итогам прогонов — коммит `git add -A && git commit -m "t42: правки по итогам прогонов"` (только осмысленные диффы). Дорожка задач `docs/superpowers/2026-10-11-t42-owns3-tls/` (spec/plan) — уже в истории ветки.
  Проверка: `git status --porcelain` → пусто; `git log --oneline -6` — коммиты задач 1–5(+6) на месте.
  Выход: ветка готова к ревью/мержу (мерж — только по явной просьбе пользователя).
  Спека: §4 фаза 5 закрыта.

---

## Self-Review (выполнен при составлении; обновлён по итогам четырёх проходов независимого ревью и синхронизирован с обновлённой спекой после contract-правки Фазы 7: механизм SDK-доверия CA — фабрика S3CaHttpClientFactory, а не env AWS_CA_BUNDLE)

1. **Покрытие спеки:** §3.1 → Task 1; §3.2 → Task 2 (шаг 2.1); §3.3 → Task 2 (шаги 2.2, 6.4); §3.4 → Tasks 3–4 (воркер: шаги 3.2 опции+хелпер BackupCaPem, 3.3 фабрика S3CaHttpClientFactory в BackupS3; спеки/агент/приёмник/bash шаги 4.1–4.3, приёмник — PEM→файл→S3CaBundlePath, та же фабрика); §3.5 → Task 5 + Task 6 (SDK-сценарии — тестовое зеркало фабрики воркера, шаги 5.1/5.2); §2 п.5/§5 (механизм фабрики) → Global Constraints + шаги 3.3/4.3/5.1; AC1 → 1.4/1.5/6.1/6.2; AC2 → 5.2–5.4; AC3 → 1.1/1.2 (юниты резолвера) + 5.4(3) (docker-кейс Program-ветки Critical+exit≠0 — WAF неприменим, см. Global Constraints); AC4 → 2.1; AC5 → 3.1–3.3 (опции/BackupCaPem/фабрика, юниты BackupS3CaTests) + 4.1–4.3 (S3_CA_PEM-проброс + материализация; юнит-диф env-набора); AC6 → 2.2/6.4; AC7 → 5.1–5.5/6.2; AC8 → 6.3. Env-биндинги `OWNS3_TLS_*` (spec §3.5 «юниты env-биндингов») — юнитами недостижимы (top-level Program.cs): покрыты end-to-end шагом 5.1 (контейнер поднимается по `OWNS3_TLS_*_PATH`) и 5.4(3) (`OWNS3_TLS_CERT`) — подмена зафиксирована в шаге 1.4, по образцу существующих env-секций. Не-цели спеки (mTLS, vhost-style, второй порт, стендовый TLS-сервис, t41) — в план не входят, задач нет — верно. Прежний раздел Task 3b/5b (fallback-фабрика) ликвидирован — его содержимое влито в основные шаги (3.3, 4.3(б), 5.1/5.2) без дублирования.
2. **Плейсхолдеры:** конкретный код во всех шагах; шаг 3.3 описывает контракт фабрики и кейсы юнитов (реализация — фактическая ветка, прошедшая код-ревью Фазы 7). Единственные оговорки с уточнением по факту прогона: текст TLS-исключения в 5.4(1) (тип уже зафиксирован — `HttpRequestException`) и хелпер `fx.HealthzAsync` (описан в шаге 5.1 как часть окружения). WAF-кейсов, доводящих исполнение до `Environment.Exit`, в плане нет (Global Constraints).
3. **Консистентность типов и контрактов:** `TlsOptions` (4 поля) используется `OwnS3Tls.Resolve` и Program; `BackupsRuntimeOptions.S3CaBundlePath : string?` — единое имя в Task 3/4 (воркер и WalReceiver-материализация используют один и тот же контракт фабрики через BackupS3); `BackupCaPem.EnvName = "S3_CA_PEM"` — единый источник для спек/AgentEnv/WalReceiverEnv; `WalReceiverOptions.S3CaPem` — позиционно последний аргумент record (шаг 4.3 синхронизирует конструктор вызова в `WalReceiverEnv.Parse`); SDK-вызовы в 5.2 сверены с эталоном `AwsSdkSimpleScenarios.cs`: HEAD — `GetObjectMetadataRequest { BucketName, Key }` (строки 93–95), чтение тела — `MemoryStream` + `CopyToAsync` (строки 83–86), роли — root создаёт/удаляет бакет, writer — объектные операции и multipart (матрица arch/owns3/05 §3); bind-mount сертов в 5.1 — трёхаргументная ro-перегрузка `WithBindMount(..., AccessMode.ReadOnly)` (соответствие spec §3.5 «ro bind-mount» и Выходу Task 5). Механизм SDK-доверия — единый на всех потребителях: фабрика описана один раз (шаг 3.3), WalReceiver приходит к ней через файл `S3CaBundlePath` (шаг 4.3), E2E — через тестовое зеркало `TlsCaHttpClientFactory` (шаги 5.1/5.2); формулировок «Program выставляет процессу AWS_CA_BUNDLE» в плане не осталось — env упоминается только как эмпирически нечитаемый (Global Constraints, шаг 3.3 комментарий). Компилируемость сниппетов: `serial` в NewPki — без `using` (byte[] не IDisposable; неявная конвертация в `ReadOnlyMemory<byte>`); шаг 1.3 — единственное using-дополнение `System.Security.Cryptography.X509Certificates` (`ConfigureHttpsDefaults` — instance-метод `KestrelServerOptions` (Microsoft.AspNetCore.Server.Kestrel.Core), вызывается на параметре лямбды без using — как уже используемый `o.Limits`); шаг 5.1 фиксирует using-блок нового файла — `System.Net`, `System.Security.Cryptography`, `System.Security.Cryptography.X509Certificates` сверх каркаса эталона (который эти пространства не содержит); finally в 5.4(3) — try/catch-обёртка `docker rm -f` (лучшее усилие, образец `TryDockerAsync`).
