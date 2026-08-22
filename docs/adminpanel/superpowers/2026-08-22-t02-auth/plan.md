# t02-auth: cookie-аутентификация админа AdminPanel — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cookie-аутентификация единственного админа из настроек (`AdminPanel:Auth`): `POST /api/auth/login` (constant-time + rate-limit 5/мин на IP), `POST /api/auth/logout`, `GET /api/auth/me`, default-deny guard `/api/*` → 401.

**Architecture:** Auth-модуль в `AdminPanel.Api/Auth` на паттернах скелета t01: attribute-DI (`[InjectAs*]`), `[Config]`-POCO + `IOptions<T>`, query-ветка CQRS. Стандартный ASP.NET Core Cookie Authentication (`adminpanel_session`), собственный fixed-window `LoginRateLimiter` (unit-тестируем через `TimeProvider`), собственный конвенционный guard-middleware. В IntegrationTests — ровно один хост на процесс (collection fixture): статический кеш сборок DI-каркаса не допускает второй. Никаких новых NuGet-пакетов.

**Tech Stack:** .NET 10, ASP.NET Core Minimal API, System.Security.Cryptography (PBKDF2/FixedTimeEquals), xunit v3 + FluentAssertions, WebApplicationFactory.

**Spec:** `docs/superpowers/2026-08-22-t02-auth/spec.md` — план реализует её; исполнители читают обе. Номера § ниже — из spec (§10/§14 — в редакции после согласования ревью Фазы 4).

## Global Constraints

- WORKTREE (все пути файлов ниже относительны к нему): `/Users/demakaev/ZCodeProject/worktrees/feat-t02-auth`; ветка `feat-t02-auth`.
- .NET 10, `LangVersion=latest`, `Nullable=enable`, `TreatWarningsAsErrors=true` — код без warning'ов (spec §2).
- Идентификаторы английские, комментарии в коде русские (spec §2).
- Тесты xunit v3 + FluentAssertions; в каждом тестовом методе AAA-комментарии на русском: `// Arrange`, `// Act`, `// Assert` (spec §2).
- Новых NuGet-пакетов и записей в `Directory.Packages.props` НЕТ; `csproj`-правки только `ProjectReference` без `Version` (spec §1, §11, §13.5).
- Секретов в `appsettings.json` нет: только `Username`; dev-креды — только в `appsettings.Development.json` (spec §3.2, §6.2).
- Один хост на процесс в IntegrationTests: статический кеш просканированных сборок attribute-DI (`ServiceCollectionExtensions`, заметка t01 §9.1) не позволяет вторую `WebApplicationFactory<Program>` — все интеграционные тесты живут в коллекции `"api"` на общей фабрике (spec §10, §14).
- Сверх spec не добавлять: ни эндпоинтов, ни опций, ни тестов (spec §2, инструкция фазы).
- `arch/01–03` не мутировать; `arch/roadmap/` меняется только делегированным шагом Task 5 (spec §11, §12).
- Коммиты — в ветке `feat-t02-auth`, формат сообщений `t02: <что>`; после каждого Task — прогон всего решения (spec §13.1–13.2).
- Команды даются полностью (с `cd` в WORKTREE или абсолютными путями) — рабочая директория между вызовами не сохраняется.

---

## Порядок задач

| Task | Deliverable | Зависит от |
|---|---|---|
| 1 | `AuthOptions` + `AdminAuthenticator` (unit, constant-time) | — |
| 2 | `LoginRateLimiter` + `AdminLoginService` (unit, rate-limit) | 1 |
| 3 | `MeQuery`/`MeDto`/`MeQueryHandler` (unit, CQRS) | — |
| 4 | `AuthModule` + `AddApi()` + `Program.cs` + appsettings + единая тест-фабрика + интеграционные `AuthTests` (правка `HealthzTests` — общая коллекция) | 1, 2, 3 |
| 5 | Полный прогон + §13.5 (пакеты) + smoke §13.3 + roadmap-деливерабл + финальный коммит | 4 |

---

### Task 1: AuthOptions + AdminAuthenticator (constant-time проверка учётных данных)

**Связь со spec:** §3.2–3.5 (обе формы пароля, приоритет hash, fail-closed, timing-равномерность), §6.1 (AuthOptions), §7.1 (алгоритм), §9.1 (тесты).

**Files:**
- Modify: `src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj` — добавить `ProjectReference` на Api (spec §3.13).
- Create: `src/AdminPanel.Api/Auth/AuthOptions.cs`
- Create: `src/AdminPanel.Api/Auth/AdminAuthenticator.cs`
- Test: `src/tests/AdminPanel.UnitTests/AdminAuthenticatorTests.cs`

**Interfaces:**
- Consumes: t01 `AdminPanel.Infrastructure.DI.ConfigAttribute`, `InjectAsSingletonAttribute`; BCL crypto.
- Produces (используют Task 2 и 4):
  - `namespace AdminPanel.Api.Auth`; `class AuthOptions { string? Username; string? Password; string? PasswordHash; double SessionHours = 8; bool AllowHttp; }` с `[Config("AdminPanel:Auth")]`.
  - `interface IAdminAuthenticator { bool Authenticate(string? username, string? password); }`
  - `sealed class AdminAuthenticator(IOptions<AuthOptions> options, ILogger<AdminAuthenticator> logger) : IAdminAuthenticator`, помечен `[InjectAsSingleton]`.

- [ ] **Step 1.1: Ссылка UnitTests → Api**

Вход: скелет t01 собирается зелёным; у `AdminPanel.UnitTests.csproj` единственная ссылка — на `AdminPanel.Infrastructure`.

Действие: в `src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj` заменить ItemGroup ProjectReference на:

```xml
    <ItemGroup>
        <ProjectReference Include="..\..\AdminPanel.Infrastructure\AdminPanel.Infrastructure.csproj"/>
        <ProjectReference Include="..\..\AdminPanel.Api\AdminPanel.Api.csproj"/>
    </ItemGroup>
```

Ссылка на Web-проект транзитивно даёт `FrameworkReference Microsoft.AspNetCore.App` — `Microsoft.Extensions.Logging.Abstractions` (`NullLogger<T>`) и `Microsoft.Extensions.Options` станут доступны тестам без новых пакетов (spec §3.13).

Выход: csproj с двумя ссылками.

Проверка: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && dotnet build src/AdminPanel.slnx`
Expected: `Build succeeded` / `0 Warning(s)` / `0 Error(s)`.

- [ ] **Step 1.2: Пишем падающие тесты**

Вход: Step 1.1 (ссылка на Api есть).

Действие: создать `src/tests/AdminPanel.UnitTests/AdminAuthenticatorTests.cs`:

```csharp
using System.Security.Cryptography;
using System.Text;
using AdminPanel.Api.Auth;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Тесты constant-time проверки учётных данных админа (spec t02 §9.1).
public class AdminAuthenticatorTests
{
    private static AdminAuthenticator Make(AuthOptions options)
        => new(Options.Create(options), NullLogger<AdminAuthenticator>.Instance);

    // Строит PBKDF2-hash в формате $pbkdf2-sha256$i$salt-b64$hash-b64 (32-байтный ключ).
    private static string MakeHash(string password, byte[] salt, int iterations)
    {
        var hash = Rfc2898DeriveBytes.Pbkdf2DeriveBytes(
            Encoding.UTF8.GetBytes(password), salt, iterations, HashAlgorithmName.SHA256, 32);
        return $"$pbkdf2-sha256${iterations}${Convert.ToBase64String(salt)}${Convert.ToBase64String(hash)}";
    }

    [Fact]
    public void PlainPassword_ValidCredentials_ReturnsTrue()
    {
        // Arrange
        var authenticator = Make(new AuthOptions { Username = "admin", Password = "s3cret" });

        // Act
        var ok = authenticator.Authenticate("admin", "s3cret");

        // Assert
        ok.Should().BeTrue();
    }

    [Fact]
    public void PlainPassword_WrongPassword_ReturnsFalse()
    {
        // Arrange
        var authenticator = Make(new AuthOptions { Username = "admin", Password = "s3cret" });

        // Act
        var ok = authenticator.Authenticate("admin", "wrong");

        // Assert
        ok.Should().BeFalse();
    }

    [Fact]
    public void WrongUsername_ReturnsFalse()
    {
        // Arrange
        var authenticator = Make(new AuthOptions { Username = "admin", Password = "s3cret" });

        // Act
        var ok = authenticator.Authenticate("root", "s3cret");

        // Assert
        ok.Should().BeFalse();
    }

    [Fact]
    public void PasswordHash_PrecedenceOverPlainPassword()
    {
        // Arrange: заданы оба — проверяется hash, plain игнорируется.
        var authenticator = Make(new AuthOptions
        {
            Username = "admin",
            Password = "plain",
            PasswordHash = MakeHash("hashed", [1, 2, 3], 1000),
        });

        // Act / Assert
        authenticator.Authenticate("admin", "hashed").Should().BeTrue();
        authenticator.Authenticate("admin", "plain").Should().BeFalse();
    }

    [Fact]
    public void PasswordHash_ValidPbkdf2_ReturnsTrue()
    {
        // Arrange
        var authenticator = Make(new AuthOptions
        {
            Username = "admin",
            PasswordHash = MakeHash("s3cret", [9, 8, 7, 6, 5], 2000),
        });

        // Act / Assert
        authenticator.Authenticate("admin", "s3cret").Should().BeTrue();
        authenticator.Authenticate("admin", "other").Should().BeFalse();
    }

    [Theory]
    [InlineData("not-a-hash")]
    [InlineData("$pbkdf2-sha256$0$c2FsdA==$aGFzaA==aGFzaA==")]
    [InlineData("$pbkdf2-sha256$1000$!!notbase64!!$aGFzaA==")]
    [InlineData("$pbkdf2-sha256$1000$c2FsdA==$c2hvcnQ=")]
    public void PasswordHash_Malformed_ReturnsFalse(string hash)
    {
        // Arrange: битый формат — fail-closed (spec t02 §3.4).
        var authenticator = Make(new AuthOptions { Username = "admin", PasswordHash = hash });

        // Act
        var ok = authenticator.Authenticate("admin", "whatever");

        // Assert
        ok.Should().BeFalse();
    }

    [Fact]
    public void EmptyConfig_ReturnsFalse()
    {
        // Arrange: пароль не сконфигурирован вовсе — fail-closed (spec t02 §3.5).
        var authenticator = Make(new AuthOptions { Username = "admin" });

        // Act
        var ok = authenticator.Authenticate("admin", "anything");

        // Assert
        ok.Should().BeFalse();
    }

    [Fact]
    public void EmptyUsernameAndPassword_Input_ReturnsFalse()
    {
        // Arrange
        var authenticator = Make(new AuthOptions { Username = "admin", Password = "s3cret" });

        // Act / Assert: пустые входы не совпадают с конфигом.
        authenticator.Authenticate(null, null).Should().BeFalse();
        authenticator.Authenticate("", "").Should().BeFalse();
    }
}
```

Выход: файл тестов на диске; типы `AuthOptions`/`AdminAuthenticator` ещё не существуют.

- [ ] **Step 1.3: Прогон — убедиться, что RED**

Вход: Step 1.2.

Действие → Проверка: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.AdminAuthenticatorTests"`
Expected: **FAIL** — ошибка компиляции: `error CS0246: The type or namespace name 'AdminAuthenticator' could not be found` (и аналогично для `AuthOptions`). Это ожидаемый красный: типы ещё не созданы.

- [ ] **Step 1.4: Реализация AuthOptions + AdminAuthenticator**

Вход: Step 1.3 (RED зафиксирован).

Действие: создать `src/AdminPanel.Api/Auth/AuthOptions.cs`:

```csharp
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Auth;

// [Config]-POCO аутентификации: секция AdminPanel:Auth (arch/01 §6, spec t02 §6.1).
[Config("AdminPanel:Auth")]
public class AuthOptions
{
    public string? Username { get; set; }

    // Plain-пароль — только dev/стенд; в git не попадает.
    public string? Password { get; set; }

    // $pbkdf2-sha256$<iterations>$<salt-b64>$<hash-b64> — приоритет над Password.
    public string? PasswordHash { get; set; }

    public double SessionHours { get; set; } = 8;

    // true только для стенда по http (Secure-политика cookie).
    public bool AllowHttp { get; set; }
}
```

Создать `src/AdminPanel.Api/Auth/AdminAuthenticator.cs`:

```csharp
using System.Security.Cryptography;
using System.Text;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Api.Auth;

// Проверка учётных данных единственного админа: constant-time, без rate-limit.
public interface IAdminAuthenticator
{
    bool Authenticate(string? username, string? password);
}

// Constant-time-проверка username+password по настройкам AdminPanel:Auth (spec t02 §7.1).
[InjectAsSingleton]
public sealed class AdminAuthenticator(IOptions<AuthOptions> options, ILogger<AdminAuthenticator> logger)
    : IAdminAuthenticator
{
    // Защита от тривиальных hash-значений.
    private const int MinHashLength = 16;

    public bool Authenticate(string? username, string? password)
    {
        var auth = options.Value;
        if (string.IsNullOrEmpty(auth.Username))
            return false;

        // Обе проверки выполняются всегда: время ответа не раскрывает, какое поле неверно.
        var usernameOk = FixedTimeEquals(username, auth.Username);
        var passwordOk = VerifyPassword(password, auth);
        return usernameOk & passwordOk;
    }

    // Приоритет PasswordHash над plain Password (arch/01 §4); пустой конфиг — fail-closed.
    private bool VerifyPassword(string? password, AuthOptions auth)
    {
        if (!string.IsNullOrEmpty(auth.PasswordHash))
            return VerifyPbkdf2(password, auth.PasswordHash);

        return !string.IsNullOrEmpty(auth.Password) && FixedTimeEquals(password, auth.Password);
    }

    // Формат $pbkdf2-sha256$<iterations>$<salt-b64>$<hash-b64>; битый формат — fail-closed.
    private bool VerifyPbkdf2(string? password, string configured)
    {
        var parts = configured.Split('$');
        if (parts.Length != 5 || parts[1] != "pbkdf2-sha256")
            return MalformedHash();

        if (!int.TryParse(parts[2], out var iterations) || iterations < 1)
            return MalformedHash();

        byte[] salt;
        byte[] expected;
        try
        {
            salt = Convert.FromBase64String(parts[3]);
            expected = Convert.FromBase64String(parts[4]);
        }
        catch (FormatException)
        {
            return MalformedHash();
        }

        if (expected.Length < MinHashLength)
            return MalformedHash();

        var actual = Rfc2898DeriveBytes.Pbkdf2DeriveBytes(
            Encoding.UTF8.GetBytes(password ?? ""),
            salt,
            iterations,
            HashAlgorithmName.SHA256,
            expected.Length);
        return CryptographicOperations.FixedTimeEquals(actual, expected);
    }

    private bool MalformedHash()
    {
        logger.LogWarning("AdminPanel:Auth:PasswordHash имеет битый формат — логин отклоняется (fail-closed)");
        return false;
    }

    // Дайджесты дают равные длины — сравнение постоянно по времени для любых входов.
    private static bool FixedTimeEquals(string? a, string? b)
        => CryptographicOperations.FixedTimeEquals(
            SHA256.HashData(Encoding.UTF8.GetBytes(a ?? "")),
            SHA256.HashData(Encoding.UTF8.GetBytes(b ?? "")));
}
```

Выход: два новых файла; тесты должны скомпилироваться.

- [ ] **Step 1.5: Прогон — GREEN по классу**

Вход: Step 1.4.

Действие → Проверка: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.AdminAuthenticatorTests"`
Expected: **PASS**, `Passed: 11` (7 фактов + Theory из 4 кейсов), `Failed: 0`. Компиляция без warning'ов (иначе build падает — `TreatWarningsAsErrors`).

- [ ] **Step 1.6: Полный прогон решения**

Вход: Step 1.5.

Действие → Проверка: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && dotnet test src/AdminPanel.slnx`
Expected: все тесты зелёные: t01 (`Result`, `AutoRegistration`, `CQRS`, `Healthz`) + новые 11. `Failed: 0`.

- [ ] **Step 1.7: Коммит**

Вход: Step 1.6 зелёный.

Действие:

```bash
git -C /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth add src/AdminPanel.Api/Auth/AuthOptions.cs src/AdminPanel.Api/Auth/AdminAuthenticator.cs src/tests/AdminPanel.UnitTests/AdminPanel.UnitTests.csproj src/tests/AdminPanel.UnitTests/AdminAuthenticatorTests.cs
git -C /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth commit -m "t02: AuthOptions + AdminAuthenticator — constant-time проверка учётных данных (unit)"
```

Выход: коммит в `feat-t02-auth`.

Проверка: `git -C /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth log --oneline -1` → коммит `t02: AuthOptions + AdminAuthenticator…`.

---

### Task 2: LoginRateLimiter + AdminLoginService (rate-limit и оркестрация логина)

**Связь со spec:** §3.6 (fixed window 60 с/5, все попытки, RemoteIpAddress без XFF, 429+Retry-After), §3.14–3.15 (TimeProvider в DI, оркестратор: лимит до проверки), §7.2–7.3, §9.2, §9.4.

**Files:**
- Create: `src/AdminPanel.Api/Auth/LoginRateLimiter.cs` (включая `SystemTimeProvider`)
- Create: `src/AdminPanel.Api/Auth/AdminLoginService.cs`
- Test: `src/tests/AdminPanel.UnitTests/AdminLoginServiceTests.cs`
- Create: `src/tests/AdminPanel.UnitTests/FixedTimeProvider.cs`

**Interfaces:**
- Consumes: Task 1 `IAdminAuthenticator`; t01 attribute-DI.
- Produces (использует Task 4):
  - `record LoginRateDecision(bool Allowed, int RetryAfterSeconds)`.
  - `interface ILoginRateLimiter { LoginRateDecision TryAcquire(string clientKey); }`
  - `sealed class LoginRateLimiter(TimeProvider timeProvider) : ILoginRateLimiter` + `public const int MaxAttempts = 5;` + `public static readonly TimeSpan Window = TimeSpan.FromSeconds(60);`, `[InjectAsSingleton]`.
  - `sealed class SystemTimeProvider() : TimeProvider` c `[InjectAsSingleton(typeof(TimeProvider))]` — DI-каркас регистрирует и базовый тип `TimeProvider`.
  - `enum LoginStatus { Ok, InvalidCredentials, RateLimited }`.
  - `record LoginResult(LoginStatus Status, int RetryAfterSeconds = 0)`.
  - `interface IAdminLoginService { LoginResult Login(string? username, string? password, string clientKey); }`
  - `sealed class AdminLoginService(ILoginRateLimiter rateLimiter, IAdminAuthenticator authenticator) : IAdminLoginService`, `[InjectAsSingleton]`.

- [ ] **Step 2.1: FixedTimeProvider + падающие тесты оркестратора**

Вход: Task 1 смержён в ветку (коммит есть), `IAdminAuthenticator` доступен.

Действие: создать `src/tests/AdminPanel.UnitTests/FixedTimeProvider.cs`:

```csharp
namespace AdminPanel.UnitTests;

// Управляемый TimeProvider для тестов фиксированных окон rate-limiter'а (spec t02 §9.4).
public sealed class FixedTimeProvider : TimeProvider
{
    // Текущее «время»; старт — фиксированная дата, двигается из тестов.
    public DateTimeOffset Utc { get; set; } = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    public override DateTimeOffset GetUtcNow() => Utc;
}
```

Создать `src/tests/AdminPanel.UnitTests/AdminLoginServiceTests.cs`:

```csharp
using AdminPanel.Api.Auth;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AdminPanel.UnitTests;

// Тесты оркестратора логина: rate-limit и учётные данные (spec t02 §9.2).
public class AdminLoginServiceTests
{
    private static (AdminLoginService Service, FixedTimeProvider Time) Make()
    {
        var time = new FixedTimeProvider();
        var authenticator = new AdminAuthenticator(
            Options.Create(new AuthOptions { Username = "admin", Password = "pw" }),
            NullLogger<AdminAuthenticator>.Instance);
        var service = new AdminLoginService(new LoginRateLimiter(time), authenticator);
        return (service, time);
    }

    [Fact]
    public void ValidCredentials_ReturnsOk()
    {
        // Arrange
        var (service, _) = Make();

        // Act
        var result = service.Login("admin", "pw", "1.1.1.1");

        // Assert
        result.Status.Should().Be(LoginStatus.Ok);
        result.RetryAfterSeconds.Should().Be(0);
    }

    [Fact]
    public void WrongPassword_ReturnsInvalidCredentials()
    {
        // Arrange
        var (service, _) = Make();

        // Act
        var result = service.Login("admin", "nope", "1.1.1.1");

        // Assert
        result.Status.Should().Be(LoginStatus.InvalidCredentials);
    }

    [Fact]
    public void WrongUsername_ReturnsInvalidCredentials()
    {
        // Arrange
        var (service, _) = Make();

        // Act
        var result = service.Login("root", "pw", "1.1.1.1");

        // Assert
        result.Status.Should().Be(LoginStatus.InvalidCredentials);
    }

    [Fact]
    public void RateLimit_SixthAttemptSameIp_ReturnsRateLimited()
    {
        // Arrange
        var (service, _) = Make();

        // Act: пять неудачных попыток в одном окне.
        for (var i = 0; i < 5; i++)
            service.Login("admin", "wrong", "1.1.1.1");
        var sixth = service.Login("admin", "wrong", "1.1.1.1");

        // Assert
        sixth.Status.Should().Be(LoginStatus.RateLimited);
        sixth.RetryAfterSeconds.Should().BeInRange(1, 60);
    }

    [Fact]
    public void RateLimit_WindowReset_AllowsAgain()
    {
        // Arrange
        var (service, time) = Make();
        for (var i = 0; i < 5; i++)
            service.Login("admin", "wrong", "1.1.1.1");

        // Act: окно сместилось — время ушло на 61 c вперёд.
        time.Utc += TimeSpan.FromSeconds(61);
        var result = service.Login("admin", "wrong", "1.1.1.1");

        // Assert
        result.Status.Should().Be(LoginStatus.InvalidCredentials);
    }

    [Fact]
    public void RateLimit_DifferentIp_Independent()
    {
        // Arrange
        var (service, _) = Make();

        // Act: IP-A исчерпал окно, IP-B приходит впервые.
        for (var i = 0; i < 6; i++)
            service.Login("admin", "wrong", "1.1.1.1");
        var other = service.Login("admin", "wrong", "2.2.2.2");

        // Assert
        other.Status.Should().Be(LoginStatus.InvalidCredentials);
    }

    [Fact]
    public void RateLimit_CountsSuccessfulLogins()
    {
        // Arrange
        var (service, _) = Make();

        // Act: пять успешных логинов тоже занимают слоты окна (spec t02 §3.6).
        for (var i = 0; i < 5; i++)
            service.Login("admin", "pw", "1.1.1.1");
        var sixth = service.Login("admin", "pw", "1.1.1.1");

        // Assert
        sixth.Status.Should().Be(LoginStatus.RateLimited);
    }
}
```

Выход: два файла; `LoginRateLimiter`/`AdminLoginService` ещё не существуют.

- [ ] **Step 2.2: Прогон — RED**

Вход: Step 2.1.

Действие → Проверка: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.AdminLoginServiceTests"`
Expected: **FAIL** — `error CS0246` для `LoginRateLimiter` и `AdminLoginService` (типы не созданы).

- [ ] **Step 2.3: Реализация LoginRateLimiter + AdminLoginService**

Вход: Step 2.2 (RED зафиксирован).

Действие: создать `src/AdminPanel.Api/Auth/LoginRateLimiter.cs`:

```csharp
using System.Collections.Concurrent;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Auth;

// Решение лимитера: разрешено ли и сколько секунд ждать до конца окна.
public sealed record LoginRateDecision(bool Allowed, int RetryAfterSeconds);

// Fixed-window лимитер попыток логина: 5 за 60 c на ключ клиента (spec t02 §3.6).
public interface ILoginRateLimiter
{
    LoginRateDecision TryAcquire(string clientKey);
}

[InjectAsSingleton]
public sealed class LoginRateLimiter(TimeProvider timeProvider) : ILoginRateLimiter
{
    public const int MaxAttempts = 5;

    public static readonly TimeSpan Window = TimeSpan.FromSeconds(60);

    // Состояние окна на ключ: идентификатор окна + счётчик попыток.
    private readonly ConcurrentDictionary<string, (long WindowId, int Count)> _windows = new();

    public LoginRateDecision TryAcquire(string clientKey)
    {
        var now = timeProvider.GetUtcNow().UtcTicks;
        var windowId = now / Window.Ticks;
        var count = _windows.AddOrUpdate(
            clientKey,
            _ => (windowId, 1),
            (_, current) => current.WindowId == windowId ? (windowId, current.Count + 1) : (windowId, 1))
           .Count;
        return count <= MaxAttempts
            ? new LoginRateDecision(true, 0)
            : new LoginRateDecision(false, RetryAfterSeconds(now, windowId));
    }

    // Остаток текущего окна в секундах (1..60) для заголовка Retry-After.
    private static int RetryAfterSeconds(long nowTicks, long windowId)
    {
        var windowEndTicks = (windowId + 1) * Window.Ticks;
        var left = (int)Math.Ceiling(TimeSpan.FromTicks(windowEndTicks - nowTicks).TotalSeconds);
        return Math.Max(left, 1);
    }
}

// Регистрация TimeProvider в DI: базовый тип TimeProvider резолвится в SystemTimeProvider.
[InjectAsSingleton(typeof(TimeProvider))]
public sealed class SystemTimeProvider() : TimeProvider;
```

Создать `src/AdminPanel.Api/Auth/AdminLoginService.cs`:

```csharp
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Auth;

// Статус попытки логина.
public enum LoginStatus
{
    Ok,
    InvalidCredentials,
    RateLimited,
}

// Результат попытки логина: статус + секунды до конца окна (для Retry-After).
public sealed record LoginResult(LoginStatus Status, int RetryAfterSeconds = 0);

// Оркестратор логина: rate-limit до проверки учётных данных (PBKDF2 дорог).
public interface IAdminLoginService
{
    LoginResult Login(string? username, string? password, string clientKey);
}

[InjectAsSingleton]
public sealed class AdminLoginService(ILoginRateLimiter rateLimiter, IAdminAuthenticator authenticator)
    : IAdminLoginService
{
    public LoginResult Login(string? username, string? password, string clientKey)
    {
        var decision = rateLimiter.TryAcquire(clientKey);
        if (!decision.Allowed)
            return new LoginResult(LoginStatus.RateLimited, decision.RetryAfterSeconds);

        return authenticator.Authenticate(username, password)
            ? new LoginResult(LoginStatus.Ok)
            : new LoginResult(LoginStatus.InvalidCredentials);
    }
}
```

Выход: два файла; тесты должны скомпилироваться.

- [ ] **Step 2.4: Прогон — GREEN по классу**

Вход: Step 2.3.

Действие → Проверка: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.AdminLoginServiceTests"`
Expected: **PASS**, `Passed: 7`, `Failed: 0`.

- [ ] **Step 2.5: Полный прогон решения**

Вход: Step 2.4.

Действие → Проверка: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && dotnet test src/AdminPanel.slnx`
Expected: все зелёные (t01 + Task 1 + Task 2), `Failed: 0`.

- [ ] **Step 2.6: Коммит**

Вход: Step 2.5 зелёный.

Действие:

```bash
git -C /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth add src/AdminPanel.Api/Auth/LoginRateLimiter.cs src/AdminPanel.Api/Auth/AdminLoginService.cs src/tests/AdminPanel.UnitTests/AdminLoginServiceTests.cs src/tests/AdminPanel.UnitTests/FixedTimeProvider.cs
git -C /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth commit -m "t02: LoginRateLimiter + AdminLoginService — fixed window 5/мин и оркестрация (unit)"
```

Выход: коммит.

Проверка: `git -C /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth log --oneline -1`.

---

### Task 3: MeQuery/MeDto/MeQueryHandler (query-ветка CQRS)

**Связь со spec:** §3.10 (me через CQRS — задел паттерна t03+), §7.4, §9.3.

**Files:**
- Create: `src/AdminPanel.Api/Auth/MeQuery.cs`
- Test: `src/tests/AdminPanel.UnitTests/MeQueryHandlerTests.cs`

**Interfaces:**
- Consumes: t01 `IQuery<T>`, `IQueryHandler<TQ,TR>`, `Result<T>`.
- Produces (использует Task 4): `sealed record MeQuery(string Username) : IQuery<MeDto>`; `sealed record MeDto(string Username)`; `sealed class MeQueryHandler : IQueryHandler<MeQuery, MeDto>` c `[InjectAsScoped]`.

- [ ] **Step 3.1: Падающий тест**

Вход: Tasks 1–2 в ветке.

Действие: создать `src/tests/AdminPanel.UnitTests/MeQueryHandlerTests.cs`:

```csharp
using AdminPanel.Api.Auth;
using FluentAssertions;
using Xunit;

namespace AdminPanel.UnitTests;

// Тест хендлера запроса текущей сессии (spec t02 §9.3).
public class MeQueryHandlerTests
{
    [Fact]
    public async Task Handle_ReturnsUsernameFromQuery()
    {
        // Arrange
        var handler = new MeQueryHandler();

        // Act
        var result = await handler.Handle(new MeQuery("admin"), CancellationToken.None);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Username.Should().Be("admin");
    }
}
```

Выход: файл теста; `MeQuery`/`MeQueryHandler` не существуют.

- [ ] **Step 3.2: Прогон — RED**

Вход: Step 3.1.

Действие → Проверка: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.MeQueryHandlerTests"`
Expected: **FAIL** — `error CS0246` для `MeQueryHandler` (и `MeQuery`).

- [ ] **Step 3.3: Реализация**

Вход: Step 3.2 (RED зафиксирован).

Действие: создать `src/AdminPanel.Api/Auth/MeQuery.cs`:

```csharp
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.CQRS;
using AdminPanel.Infrastructure.DI;

namespace AdminPanel.Api.Auth;

// Запрос текущей сессии: username кладётся в query из ClaimsPrincipal эндпоинтом.
public sealed record MeQuery(string Username) : IQuery<MeDto>;

// Ответ GET /api/auth/me.
public sealed record MeDto(string Username);

// Хендлер: чистое чтение без внешних зависимостей.
[InjectAsScoped]
public sealed class MeQueryHandler : IQueryHandler<MeQuery, MeDto>
{
    public ValueTask<Result<MeDto>> Handle(MeQuery query, CancellationToken ct)
        => ValueTask.FromResult(Result<MeDto>.Success(new MeDto(query.Username)));
}
```

Выход: файл; тест компилируется.

- [ ] **Step 3.4: Прогон — GREEN по классу**

Вход: Step 3.3.

Действие → Проверка: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.UnitTests.MeQueryHandlerTests"`
Expected: **PASS**, `Passed: 1`, `Failed: 0`.

- [ ] **Step 3.5: Полный прогон + коммит**

Вход: Step 3.4.

Действие → Проверка: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && dotnet test src/AdminPanel.slnx`
Expected: все зелёные, `Failed: 0`.

Действие:

```bash
git -C /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth add src/AdminPanel.Api/Auth/MeQuery.cs src/tests/AdminPanel.UnitTests/MeQueryHandlerTests.cs
git -C /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth commit -m "t02: MeQuery/MeQueryHandler — сессия через query-ветку CQRS (unit)"
```

Проверка: `git -C /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth log --oneline -1` → коммит `t02: MeQuery/MeQueryHandler…`.

---

### Task 4: AuthModule + AddApi + Program.cs + appsettings + единая тест-фабрика + интеграционные тесты (HTTP-контракт)

**Связь со spec:** §3.1 (эндпоинты), §3.7 (cookie-опции, без 302), §3.8 (guard default-deny), §3.11 (модуль Api-сборки), §3.12 (logout под auth), §4 (контракт API), §5 (дерево), §6.2 (appsettings), §7.5–7.6 (AuthModule), §8 (Program.cs), §10 (в редакции после ревью Фазы 4: единая фабрика-коллекция `"api"`, изоляция окна через `FixedTimeProvider`), §14 (Retry-After; один хост на процесс).

**Files:**
- Create: `src/AdminPanel.Api/Auth/AuthModule.cs` (константы, `LoginRequest`, `AddCookieAuth`, `UseApiAuthorization`, `MapAuthApi`)
- Create: `src/AdminPanel.Api/ModuleExtensions.cs` (`AddApi()`)
- Modify: `src/AdminPanel.Api/Program.cs` (целиком, см. Step 4.4)
- Modify: `src/AdminPanel.Api/appsettings.json`, `src/AdminPanel.Api/appsettings.Development.json`
- Create: `src/tests/AdminPanel.IntegrationTests/AuthTests.cs` (включая `FixedTimeProvider`, `AuthWebFactory`, `ApiCollection`)
- Modify: `src/tests/AdminPanel.IntegrationTests/HealthzTests.cs` (перевод в коллекцию `"api"`, общий хост)

**Interfaces:**
- Consumes: Task 1 `AuthOptions`, `IAdminAuthenticator`; Task 2 `IAdminLoginService`, `LoginStatus`; Task 3 `MeQuery`; t01 `IHandler`.
- Produces: `static class AuthModule` c `AddCookieAuth(this IServiceCollection)`, `UseApiAuthorization(this IApplicationBuilder)`, `MapAuthApi(this IEndpointRouteBuilder)`; `record LoginRequest(string? Username, string? Password)`; `static class ModuleExtensions { IServiceCollection AddApi(this IServiceCollection) }` (namespace `AdminPanel.Api`); новый `Program` (эндпоинты `/api/auth/login|logout|me`); в IntegrationTests — `AuthWebFactory : WebApplicationFactory<Program>` (свойство `Time` — `FixedTimeProvider`), коллекция `"api"` (`ApiCollection : ICollectionFixture<AuthWebFactory>`), используемая и `AuthTests`, и `HealthzTests`.

- [ ] **Step 4.1: Интеграционные тесты и единая фабрика (RED-фаза — эндпоинтов ещё нет)**

Вход: Tasks 1–3 в ветке; `Program` экспонирует только `/api/healthz`.

Действие: создать `src/tests/AdminPanel.IntegrationTests/AuthTests.cs`:

```csharp
using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using FluentAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Управляемое время: изоляция окна rate-limiter'а между тестами общей фабрики.
public sealed class FixedTimeProvider : TimeProvider
{
    public DateTimeOffset Utc { get; set; } = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    public override DateTimeOffset GetUtcNow() => Utc;
}

// Единая на сборку фабрика (collection fixture "api"): статический кеш сборок
// attribute-DI не допускает второй хост в процессе (spec t02 §10, §14).
public sealed class AuthWebFactory : WebApplicationFactory<Program>
{
    public FixedTimeProvider Time { get; } = new();

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        // http-стенд: без AllowHttp Secure-cookie не вернётся по http (spec t02 §10, §14).
        builder.UseSetting("AdminPanel:Auth:Username", "admin");
        builder.UseSetting("AdminPanel:Auth:Password", "adminpw");
        builder.UseSetting("AdminPanel:Auth:AllowHttp", "true");

        // Подмена времени ПОСЛЕ композиции Program (ConfigureTestServices):
        // singleton-лимитер живёт на управляемом времени фабрики.
        builder.ConfigureTestServices(services =>
            services.Replace(ServiceDescriptor.Singleton(typeof(TimeProvider), Time)));
    }
}

// Единственный хост на тестовую сборку: AuthTests и HealthzTests.
[CollectionDefinition("api")]
public sealed class ApiCollection : ICollectionFixture<AuthWebFactory>;

// Интеграция auth-модуля: login/logout/me, 401 без cookie, rate-limit (spec t02 §10).
[Collection("api")]
public class AuthTests
{
    private readonly AuthWebFactory _factory;

    public AuthTests(AuthWebFactory factory) => _factory = factory;

    // Свежее окно лимитера: сдвиг времени — fixed window сбрасывается по windowId.
    private void NewRateWindow() => _factory.Time.Utc += TimeSpan.FromSeconds(61);

    [Fact]
    public async Task Login_ValidCredentials_Returns204AndSessionCookie()
    {
        // Arrange
        NewRateWindow();
        using var client = _factory.CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });

        // Act
        var response = await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "admin", password = "adminpw" },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
        response.Headers.TryGetValues("Set-Cookie", out var cookies).Should().BeTrue();
        cookies.Should().Contain(c => c.StartsWith("adminpanel_session="));
    }

    [Fact]
    public async Task Login_WrongPassword_Returns401ProblemDetails()
    {
        // Arrange
        NewRateWindow();
        using var client = _factory.CreateClient();

        // Act
        var response = await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "admin", password = "wrong" },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        response.Content.Headers.ContentType!.MediaType.Should().Be("application/problem+json");
    }

    [Fact]
    public async Task Login_WrongUsername_Returns401()
    {
        // Arrange
        NewRateWindow();
        using var client = _factory.CreateClient();

        // Act
        var response = await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "root", password = "adminpw" },
            TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Login_MalformedJson_Returns400()
    {
        // Arrange
        using var client = _factory.CreateClient();
        using var content = new StringContent("{ not json", Encoding.UTF8, "application/json");

        // Act
        var response = await client.PostAsync("/api/auth/login", content, TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Login_RateLimit_SixthAttempt_Returns429()
    {
        // Arrange
        NewRateWindow();
        using var client = _factory.CreateClient();

        // Act: пять неудачных попыток исчерпывают окно 5/мин.
        for (var i = 0; i < 5; i++)
            await client.PostAsJsonAsync(
                "/api/auth/login",
                new { username = "admin", password = "wrong" },
                TestContext.Current.CancellationToken);
        var sixth = await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "admin", password = "wrong" },
            TestContext.Current.CancellationToken);

        // Assert
        sixth.StatusCode.Should().Be(HttpStatusCode.TooManyRequests);
        sixth.Headers.TryGetValues("Retry-After", out _).Should().BeTrue();
    }

    [Fact]
    public async Task Me_WithoutCookie_Returns401NotRedirect()
    {
        // Arrange
        using var client = _factory.CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });

        // Act
        var response = await client.GetAsync("/api/auth/me", TestContext.Current.CancellationToken);

        // Assert: ровно 401 — никаких 302-редиректов на логин (spec t02 §3.7).
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Me_WithCookie_ReturnsUsername()
    {
        // Arrange: default-клиент хранит cookie из Set-Cookie (HandleCookies=true).
        NewRateWindow();
        using var client = _factory.CreateClient();
        await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "admin", password = "adminpw" },
            TestContext.Current.CancellationToken);

        // Act
        var response = await client.GetAsync("/api/auth/me", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("username").GetString().Should().Be("admin");
    }

    [Fact]
    public async Task Logout_WithCookie_Returns204AndInvalidatesSession()
    {
        // Arrange
        NewRateWindow();
        using var client = _factory.CreateClient();
        await client.PostAsJsonAsync(
            "/api/auth/login",
            new { username = "admin", password = "adminpw" },
            TestContext.Current.CancellationToken);

        // Act
        var logout = await client.PostAsync("/api/auth/logout", null, TestContext.Current.CancellationToken);
        var me = await client.GetAsync("/api/auth/me", TestContext.Current.CancellationToken);

        // Assert
        logout.StatusCode.Should().Be(HttpStatusCode.NoContent);
        me.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Api_DefaultDeny_WithoutCookie_Returns401()
    {
        // Arrange
        using var client = _factory.CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });

        // Act: защищённые пути без cookie; healthz — исключение guard'а.
        var logout = await client.PostAsync("/api/auth/logout", null, TestContext.Current.CancellationToken);
        var me = await client.GetAsync("/api/auth/me", TestContext.Current.CancellationToken);
        var healthz = await client.GetAsync("/api/healthz", TestContext.Current.CancellationToken);

        // Assert
        logout.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        me.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        healthz.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}
```

Затем заменить содержимое `src/tests/AdminPanel.IntegrationTests/HealthzTests.cs` на (перевод в общую коллекцию `"api"` — без второй фабрики в процессе; контракт теста не меняется):

```csharp
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using FluentAssertions;
using Xunit;

namespace AdminPanel.IntegrationTests;

// Смоук живости панели: /api/healthz без авторизации отвечает контрактом {"status":"ok"}.
// t02: тест в общей коллекции "api" — второй хост в процессе невозможен (кеш DI-скана сборок).
[Collection("api")]
public class HealthzTests
{
    private readonly AuthWebFactory _factory;

    public HealthzTests(AuthWebFactory factory) => _factory = factory;

    [Fact]
    public async Task Healthz_ReturnsOkStatus()
    {
        // Arrange
        using var client = _factory.CreateClient();

        // Act
        var response = await client.GetAsync("/api/healthz", TestContext.Current.CancellationToken);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(TestContext.Current.CancellationToken);
        body.GetProperty("status").GetString().Should().Be("ok");
    }
}
```

Выход: тесты компилируются (ссылаются только на `Program` и фабрику); эндпоинтов `/api/auth/*` ещё нет.

- [ ] **Step 4.2: Прогон — RED**

Вход: Step 4.1.

Действие → Проверка: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.IntegrationTests.AuthTests"`
Expected: **FAIL** — все 9 тестов: эндпоинтов `/api/auth/*` нет, маршрутизатор отдаёт 404 (`Expected response.StatusCode to be NoContent {value: 204}, but found NotFound {value: 404}` и аналогичные). Подмена `TimeProvider` в `ConfigureTestServices` безвредна (Replace при отсутствии дескриптора просто добавляет его).

- [ ] **Step 4.3: AuthModule.cs + ModuleExtensions.cs (Api)**

Вход: Step 4.2 (RED зафиксирован).

Действие: создать `src/AdminPanel.Api/Auth/AuthModule.cs`:

```csharp
using System.Security.Claims;
using System.Text.Json;
using AdminPanel.Infrastructure.CQRS;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AdminPanel.Api.Auth;

// Тело POST /api/auth/login.
public sealed record LoginRequest(string? Username, string? Password);

// Композиция auth-модуля: cookie-схема, guard /api/*, эндпоинты (spec t02 §7.5).
public static class AuthModule
{
    public const string CookieName = "adminpanel_session";
    public const string ApiPrefix = "/api";
    public const string LoginPath = "/api/auth/login";
    public const string HealthzPath = "/api/healthz";

    // Cookie-схема аутентификации; значения — из [Config]-POCO AdminPanel:Auth.
    public static IServiceCollection AddCookieAuth(this IServiceCollection services)
    {
        services
           .AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
           .AddCookie(CookieAuthenticationDefaults.AuthenticationScheme, o =>
            {
                o.Cookie.Name = CookieName;
                o.Cookie.HttpOnly = true;
                o.Cookie.SameSite = SameSiteMode.Lax;
                o.Cookie.SecurePolicy = CookieSecurePolicy.Always;
                o.SlidingExpiration = true;
                // API не редиректит на логин-страницу: чистые 401/403 (spec t02 §3.7).
                o.Events.OnRedirectToLogin = ctx =>
                {
                    ctx.Response.StatusCode = StatusCodes.Status401Unauthorized;
                    return Task.CompletedTask;
                };
                o.Events.OnRedirectToAccessDenied = ctx =>
                {
                    ctx.Response.StatusCode = StatusCodes.Status403Forbidden;
                    return Task.CompletedTask;
                };
            });

        // Срок сессии и Secure-политика — из AuthOptions (spec t02 §3.7).
        // ILogger<Program>: маркер-тип не-static (AuthModule — static, иначе CS0718).
        services
           .AddOptions<CookieAuthenticationOptions>(CookieAuthenticationDefaults.AuthenticationScheme)
           .Configure<IOptions<AuthOptions>, ILogger<Program>>((o, auth, logger) =>
            {
                if (auth.Value.SessionHours <= 0)
                {
                    // Опечатка в конфиге — не роняем хост, откатываемся к 8 часам.
                    logger.LogWarning("AdminPanel:Auth:SessionHours <= 0 — использую 8 часов");
                    o.ExpireTimeSpan = TimeSpan.FromHours(8);
                }
                else
                    o.ExpireTimeSpan = TimeSpan.FromHours(auth.Value.SessionHours);

                o.Cookie.SecurePolicy = auth.Value.AllowHttp
                    ? CookieSecurePolicy.SameAsRequest
                    : CookieSecurePolicy.Always;
            });

        return services;
    }

    // Default-deny guard: всё /api/*, кроме login и healthz, требует cookie (spec t02 §3.8).
    public static IApplicationBuilder UseApiAuthorization(this IApplicationBuilder app)
        => app.Use(ApiGuard);

    private static async Task ApiGuard(HttpContext context, Func<Task> next)
    {
        var path = context.Request.Path;
        var isApi = path.StartsWithSegments(ApiPrefix);
        var isException = PathEquals(path, LoginPath) || PathEquals(path, HealthzPath);
        if (isApi && !isException && context.User.Identity?.IsAuthenticated != true)
        {
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            context.Response.ContentType = "application/problem+json";
            await context.Response.WriteAsync(JsonSerializer.Serialize(new
            {
                title = "Unauthorized",
                status = 401,
            }));
            return;
        }

        await next();
    }

    // Сравнение пути без учёта регистра (spec t02 §3.8).
    private static bool PathEquals(PathString path, string value)
        => path.Equals((PathString)value, StringComparison.OrdinalIgnoreCase);

    // Эндпоинты логина/логаута/сессии (arch/03 §1, spec t02 §4).
    public static IEndpointRouteBuilder MapAuthApi(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapPost(
            LoginPath,
            async (LoginRequest request, IAdminLoginService service, IOptions<AuthOptions> authOptions, HttpContext context) =>
            {
                var clientKey = context.Connection.RemoteIpAddress?.ToString() ?? "unknown";
                var result = service.Login(request.Username, request.Password, clientKey);
                if (result.Status == LoginStatus.Ok)
                {
                    // Имя в сессии — каноническое из настроек, не из запроса.
                    var principal = MakePrincipal(authOptions.Value.Username ?? request.Username ?? string.Empty);
                    await context.SignInAsync(CookieAuthenticationDefaults.AuthenticationScheme, principal);
                    return Results.NoContent();
                }

                if (result.Status == LoginStatus.RateLimited)
                {
                    context.Response.Headers["Retry-After"] = result.RetryAfterSeconds.ToString();
                    return Results.Problem(statusCode: StatusCodes.Status429TooManyRequests);
                }

                // Generic-ответ: не раскрываем, какое поле неверно.
                return Results.Problem(statusCode: StatusCodes.Status401Unauthorized, detail: "Invalid credentials");
            });

        endpoints.MapPost(
            "/api/auth/logout",
            async (HttpContext context) =>
            {
                await context.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
                return Results.NoContent();
            });

        endpoints.MapGet(
            "/api/auth/me",
            async (ClaimsPrincipal user, IHandler handler, CancellationToken ct) =>
            {
                var result = await handler.HandleQuery(new MeQuery(user.Identity!.Name!), ct);
                return result.IsSuccess
                    ? Results.Ok(new { username = result.Value.Username })
                    : Results.Problem(statusCode: StatusCodes.Status500InternalServerError);
            });

        return endpoints;
    }

    // Principal сессии: единственный claim — имя админа из настроек.
    private static ClaimsPrincipal MakePrincipal(string username)
        => new(new ClaimsIdentity(
            [new Claim(ClaimTypes.Name, username)],
            CookieAuthenticationDefaults.AuthenticationScheme));
}
```

Создать `src/AdminPanel.Api/ModuleExtensions.cs`:

```csharp
using System.Reflection;
using AdminPanel.Infrastructure.DI;
using Microsoft.Extensions.DependencyInjection;

namespace AdminPanel.Api;

// Модуль хоста: auth-сервисы и [Config]-POCO Api-сборки через attribute-DI (spec t02 §3.11).
public static class ModuleExtensions
{
    private static Assembly Assembly => typeof(ModuleExtensions).Assembly;

    public static IServiceCollection AddApi(this IServiceCollection services)
        => services.AutoRegistration(Assembly);
}
```

Выход: композиция модуля; `Program.cs` ещё не подключает её.

- [ ] **Step 4.4: Program.cs — целиком новый состав**

Вход: Step 4.3.

Действие: заменить содержимое `src/AdminPanel.Api/Program.cs` на (spec §8 дословно + warning §3.5):

```csharp
using AdminPanel.Api;
using AdminPanel.Api.Auth;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.DI;
using AdminPanel.Infrastructure.Traces;
using AdminPanel.Probes;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

// Точка входа панели: сборка хоста и модульная композиция сервисов.
var builder = WebApplication.CreateBuilder(args);

// Инициализация ActivitySource каркаса до первого HandleQuery (по образцу референса).
Tracing.Init(builder.Environment.ApplicationName);

builder
   .Services.UseDiBehaviours(builder.Configuration)
   .AddInfrastructure()
   .AddApi() // t02: auth-сервисы и [Config]-POCO Api-сборки
   .AddCore()
   .AddEtcd()
   .AddProbes()
   .AddOpenApi()
   .AddHealthChecks()
   .AddCheck("self", () => HealthCheckResult.Healthy(), ["live"]);

// t02: cookie-схема аутентификации (настройки — AdminPanel:Auth).
builder.Services.AddCookieAuth();

var app = builder.Build();

// t02: fail-closed — без пароля в конфиге логин невозможен, предупреждаем на старте.
var auth = app.Services.GetRequiredService<IOptions<AuthOptions>>().Value;
if (string.IsNullOrEmpty(auth.Password) && string.IsNullOrEmpty(auth.PasswordHash))
    app.Logger.LogWarning("AdminPanel:Auth: не задан ни Password, ни PasswordHash — логин отключён");

// OpenAPI-схема — только в dev-окружении.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

// t02: аутентификация + default-deny guard — всё /api/*, кроме login и healthz, → 401.
app.UseAuthentication();
app.UseApiAuthorization();
app.MapAuthApi();

// Живость самой панели; без авторизации.
app.MapHealthChecks(
    "/api/healthz",
    new HealthCheckOptions { ResponseWriter = HealthzWriter.WriteStatus });

app.Run();

// Экспозиция точки входа для WebApplicationFactory в интеграционных тестах.
public partial class Program;
```

Выход: хост с полной auth-композицией.

- [ ] **Step 4.5: appsettings**

Вход: Step 4.4.

Действие: заменить `src/AdminPanel.Api/appsettings.json` на:

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning"
    }
  },
  "AllowedHosts": "*",
  "AdminPanel": {
    "Auth": {
      "Username": "admin"
    }
  }
}
```

Заменить `src/AdminPanel.Api/appsettings.Development.json` на:

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning"
    }
  },
  "AllowedHosts": "*",
  "AdminPanel": {
    "Auth": {
      "Username": "admin",
      "Password": "admin",
      "AllowHttp": true
    }
  }
}
```

Выход: секции `AdminPanel:Auth` на месте; в базовом appsettings секрета нет (spec §6.2, критерий §13.4).

- [ ] **Step 4.6: Сборка**

Вход: Step 4.5.

Действие → Проверка: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && dotnet build src/AdminPanel.slnx`
Expected: `Build succeeded`, `0 Warning(s)`, `0 Error(s)`.

- [ ] **Step 4.7: Прогон — GREEN по классу**

Вход: Step 4.6.

Действие → Проверка: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && dotnet test src/AdminPanel.slnx --filter "FullyQualifiedName~AdminPanel.IntegrationTests.AuthTests"`
Expected: **PASS**, `Passed: 9`, `Failed: 0`. Изоляция окна — через `NewRateWindow()` (сдвиг `Time.Utc` на 61 c → новый `windowId` → счётчик с нуля); подмена `TimeProvider` через `ConfigureTestServices` выполняется после композиции Program, поэтому перекрывает дескриптор attribute-DI.

- [ ] **Step 4.8: Полный прогон решения**

Вход: Step 4.7.

Действие → Проверка: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && dotnet test src/AdminPanel.slnx`
Expected: все зелёные: t01 + Tasks 1–3 + `AuthTests` (9) + `HealthzTests` (общий хост коллекции `"api"`, контракт теста прежний). `Failed: 0`. Фабрика задаёт `Password=adminpw`, поэтому warning «логин отключён» в логе не появляется.

- [ ] **Step 4.9: Коммит**

Вход: Step 4.8 зелёный.

Действие:

```bash
git -C /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth add src/AdminPanel.Api/Auth/AuthModule.cs src/AdminPanel.Api/ModuleExtensions.cs src/AdminPanel.Api/Program.cs src/AdminPanel.Api/appsettings.json src/AdminPanel.Api/appsettings.Development.json src/tests/AdminPanel.IntegrationTests/AuthTests.cs src/tests/AdminPanel.IntegrationTests/HealthzTests.cs
git -C /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth commit -m "t02: auth-композиция — cookie-схема, guard /api/*, login/logout/me (integration, единый хост)"
```

Проверка: `git -C /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth log --oneline -1`.

---

### Task 5: Полный прогон, §13.5 (пакеты), smoke §13.3, roadmap-деливерабл, финальный коммит

**Связь со spec:** §12 (удаление пункта t02-auth из `arch/roadmap/infra.md`; зависимости `t04-etcd-api ← t02-auth` и `t07-frontend-base ← t02-auth` НЕ трогать), §13 (критерии приёмки: полные прогоны, отсутствие новых пакетов, curl-сценарий, отсутствие секрета, roadmap).

**Files:**
- Modify: `arch/roadmap/infra.md` (удалить пункт `t02-auth` — строки списка)
- Коммит: `docs/superpowers/2026-08-22-t02-auth/spec.md` и `docs/superpowers/2026-08-22-t02-auth/plan.md` (сейчас untracked)

**Interfaces:**
- Consumes: Tasks 1–4 слиты в ветку.
- Produces: чистый `main`-кандидат ветки `feat-t02-auth` (код + тесты + roadmap-деливерабл + документация задачи).

- [ ] **Step 5.1: Полный прогон build + test**

Вход: Tasks 1–4 в ветке.

Действие → Проверка:

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && dotnet build src/AdminPanel.slnx && dotnet test src/AdminPanel.slnx
```

Expected: `Build succeeded`, `0 Warning(s)`; все тесты зелёные, `Failed: 0` (spec §13.1–13.2).

- [ ] **Step 5.2: Проверка §13.5 — пакетов не добавлено**

Вход: Step 5.1 зелёный; коммиты задачи в `feat-t02-auth`.

Действие → Проверка (три команды):

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && git diff --exit-code -- src/Directory.Packages.props && echo CLEAN
```

Expected: `CLEAN` (рабочее дерево не меняло файл; exit 0).

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && git log --oneline origin/main..HEAD -- src/Directory.Packages.props
```

Expected: пустой вывод (ни один коммит задачи не трогал `Directory.Packages.props`; если `origin/main` недоступен — использовать `main`).

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && grep -rn 'Version="' src --include='*.csproj'; grep -rn "PackageReference" src --include='*.csproj'
```

Expected: первый grep — пусто (ни одна ссылка не несёт `Version` — CPM); второй — тот же список ссылок, что был в t01, плюс ровно одна новая строка (`AdminPanel.Api.csproj` в UnitTests.csproj из Task 1) и ничего сверх.

- [ ] **Step 5.3: Ручной smoke-сценарий (spec §13.3)**

Вход: Step 5.2 пройден; порт 5000 свободен (проверка: `lsof -i :5000` — пусто).

Действие. Запустить панель фоновым процессом (Development-профиль с `appsettings.Development.json` — креды admin/admin, `AllowHttp=true`):

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && (dotnet run --project src/AdminPanel.Api --launch-profile http > /tmp/t02-run.log 2>&1 &)
```

Дождаться старта ~10–15 c, затем выполнить по очереди (каждая — отдельная команда, кавычки не менять; JSON-тела в одинарных кавычках — без экранирования):

```bash
curl -s -o /dev/null -w 'healthz=%{http_code}\n' http://localhost:5000/api/healthz
curl -s -o /dev/null -w 'wrong=%{http_code}\n' -X POST http://localhost:5000/api/auth/login -H 'Content-Type: application/json' -d '{"username":"admin","password":"wrong"}'
rm -f /tmp/t02.jar
curl -s -c /tmp/t02.jar -o /dev/null -w 'login=%{http_code}\n' -X POST http://localhost:5000/api/auth/login -H 'Content-Type: application/json' -d '{"username":"admin","password":"admin"}'
curl -s -b /tmp/t02.jar http://localhost:5000/api/auth/me
curl -s -o /dev/null -w 'me_no_cookie=%{http_code}\n' http://localhost:5000/api/auth/me
curl -s -b /tmp/t02.jar -o /dev/null -w 'logout=%{http_code}\n' -X POST http://localhost:5000/api/auth/logout
curl -s -b /tmp/t02.jar -o /dev/null -w 'me_after_logout=%{http_code}\n' http://localhost:5000/api/auth/me
for i in 1 2 3 4; do curl -s -o /dev/null -w 'ratelimit=%{http_code}\n' -X POST http://localhost:5000/api/auth/login -H 'Content-Type: application/json' -d '{"username":"admin","password":"wrong"}'; done
pkill -f "AdminPanel.Api"
```

Выход: строки с кодами ответов (curl) и тело `me`.

Проверка (Expected, сверка со spec §13.3; окно лимитера 5/мин считает все попытки логина — их в сценарии ровно 6):

- `healthz=200` (без cookie);
- `wrong=401`;
- `login=204` (cookie записан в `/tmp/t02.jar`);
- тело `me`: `{"username":"admin"}`;
- `me_no_cookie=401` (не 302);
- `logout=204`;
- `me_after_logout=401` (сессия погашена);
- `ratelimit=` — четыре строки: `401`, `401`, `401`, `429` (попытки №3–6 сценария: №3–5 отклонены как неверный пароль, №6 — лимитом; слоты №1 `wrong` и №2 `login` уже заняты).

Если вывод не совпал — не коммитить, разбираться по `/tmp/t02-run.log` (порт занят: `lsof -i :5000`; окно лимитера не сбросилось после прошлого прогона — подождать 60 c и повторить; остаточный процесс панели: `pkill -f "AdminPanel.Api"`).

- [ ] **Step 5.4: Удалить пункт t02-auth из roadmap**

Вход: Step 5.3 пройден.

Действие: в `arch/roadmap/infra.md` удалить пункт (строки, начинающиеся с `` - `t02-auth` ← `` и его продолжение до конца пункта — 6 строк перед пунктом `t11-finalize`):

```markdown
- `t02-auth` ← `t01-skeleton` — аутентификация. Cookie-сессия из настроек
  (`AdminPanel:Auth:*`: Username, Password|PasswordHash PBKDF2, SessionHours,
  AllowHttp), `POST /api/auth/login` (rate-limit 5/мин на IP, constant-time
  сравнение), `POST /api/auth/logout`, `GET /api/auth/me`; middleware:
  всё `/api/*`, кроме login и healthz, → 401. Integration-тесты
  (WebApplicationFactory): login ok/bad, 401 без cookie, logout.
```

После удаления раздел «## Задачи» `infra.md` начинается сразу с пункта `t11-finalize`. Файлы `arch/roadmap/etcd.md` и `arch/roadmap/frontend.md` НЕ трогать (там `← t02-auth` остаётся по правилу spec §12).

Выход: пункт удалён.

Проверка:

```bash
cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && grep -c "t02-auth" arch/roadmap/infra.md; grep -rn "t02-auth" arch/roadmap/ | cut -d: -f1 | sort -u
```

Expected: первая команда — `0`; вторая — ровно два файла: `arch/roadmap/etcd.md` и `arch/roadmap/frontend.md` (spec §13.6).

- [ ] **Step 5.5: Проверка отсутствия секрета в базовом appsettings (spec §13.4)**

Вход: Step 5.4.

Действие → Проверка: `cd /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth && grep -n "Password" src/AdminPanel.Api/appsettings.json`
Expected: вывод пуст (exit code 1 от grep — это ожидаемо, не ошибка шага): в `appsettings.json` нет `Password`/`PasswordHash`.

- [ ] **Step 5.6: Финальный коммит**

Вход: Steps 5.1–5.5 пройдены; `git status` показывает изменённый `arch/roadmap/infra.md` и untracked `docs/superpowers/2026-08-22-t02-auth/`.

Действие:

```bash
git -C /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth add arch/roadmap/infra.md docs/superpowers/2026-08-22-t02-auth
git -C /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth commit -m "t02: roadmap-деливерабл (удаление пункта t02-auth) + spec/plan задачи"
```

Выход: финальный коммит; ветка `feat-t02-auth` готова к ревью и мержу (мерж-гейт выполняет координатор).

Проверка:

```bash
git -C /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth log --oneline && git -C /Users/demakaev/ZCodeProject/worktrees/feat-t02-auth status --short
```

Expected: 5 коммитов задачи (`t02: …`), рабочий каталог чист (нет untracked/modified).

---

## Контроль соответствия spec (для ревьюера)

| Spec § | Что проверять | Где |
|---|---|---|
| §3.2–3.5 | Приоритет hash, fail-closed, timing-единообразие | Task 1 (код + тесты) |
| §3.6 | Окно 60с/5, все попытки, Retry-After, без XFF | Task 2 |
| §3.7 | Cookie `adminpanel_session`, Lax/HttpOnly/sliding, Secure по AllowHttp, без 302 | Task 4 (AuthModule, `Me_WithoutCookie_Returns401NotRedirect`) |
| §3.8 | Guard default-deny, исключения login/healthz, ProblemDetails-401 | Task 4 (`ApiGuard`, `Api_DefaultDeny_WithoutCookie_Returns401`) |
| §3.10–3.11 | me через IHandler; `AddApi()`-скан сборки | Tasks 3–4 |
| §3.13–3.15 | UnitTests→Api ссылка; SystemTimeProvider; оркестратор лимит→креды | Tasks 1–2 |
| §4 | Контракт эндпоинтов (коды, тела) | Task 4 (AuthTests) |
| §6.2 | appsettings без секретов; dev-креды в Development | Task 4 Step 4.5, Task 5 Step 5.5 |
| §9.1–9.4 | Unit-набор по spec | Tasks 1–3 |
| §10 (ред. Фазы 4) | Единая фабрика-коллекция `"api"`, FixedTimeProvider+Replace, HealthzTests в коллекции | Task 4 Step 4.1 |
| §11 | Ничего сверх: нет новых пакетов, нет XFF/lockout/анти-forgery | все задачи |
| §12–13.2 | Roadmap-деливерабл; полные прогоны | Task 5 Steps 5.1, 5.4 |
| §13.3 | Ручной curl-сценарий, включая 429 на 6-й попытке | Task 5 Step 5.3 |
| §13.4 | Секрета в `appsettings.json` нет | Task 5 Step 5.5 |
| §13.5 | Нет новых PackageReference/Version; Directory.Packages.props не менялся | Task 5 Step 5.2 |
| §13.6 | `t02-auth` удалён только из `infra.md`; `← t02-auth` в etcd/frontend сохранён | Task 5 Step 5.4 |
