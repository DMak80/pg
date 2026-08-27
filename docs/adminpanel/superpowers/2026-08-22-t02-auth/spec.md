# Спецификация t02-auth — cookie-аутентификация админа AdminPanel

Дата: 2026-08-22. Фаза dev-flow: spec. Источники истины: `arch/roadmap/infra.md`
(пункт `t02-auth`), `arch/01-architecture.md` §4 (аутентификация) и §6
(настройки), `arch/03-panels.md` §1 (эндпоинты). Референс `../Puzzle` аутентификации
не содержит (подтверждено `docs/superpowers/2026-08-22-arch-design/research/
puzzle-report.md`); образец хэширования Puzzle (`Account.MakePasswordHash`) —
устаревший MD5 и не переносится: схема пароля задаётся arch/01 §4.

## 1. Цель

Cookie-аутентификация единственного администратора панели: учётные данные из
настроек (`[Config]`-секция `AdminPanel:Auth`), эндпоинты `POST /api/auth/login`
(rate-limit 5/мин на IP, constant-time сравнение), `POST /api/auth/logout`,
`GET /api/auth/me`, и default-deny guard: всё `/api/*`, кроме `login` и
`healthz`, без аутентификации → 401. Auth-модуль живёт в `AdminPanel.Api`
(arch/01 §2) и ложится на паттерны скелета t01: attribute-DI (`[InjectAs*]`),
`[Config]`-POCO + `IOptions<T>`, query-ветка CQRS (`IQuery`/`IQueryHandler`/
`IHandler.HandleQuery`), модульная композиция `Program.cs`. Тесты: unit на
логику логина (успех/неверный пароль/неверный логин/rate-limit), integration
через `WebApplicationFactory` (401 без cookie, отсутствие 302-редиректов,
выдача/погашение cookie).

Новых NuGet-пакетов нет: cookie-аутентификация, rate-limit и JSON — shared
framework ASP.NET Core; PBKDF2 и constant-time сравнение — BCL
(`System.Security.Cryptography`).

## 2. Принципы

- Источник истины — `arch/`; всё, что arch/ не оговаривает, решено минимальным
  способом и зафиксировано в §3. Расхождение с arch/ запрещено (SPEC_DEVIATION).
- Идентификаторы — английские; комментарии в коде — русские.
- Тесты — xunit v3 + FluentAssertions, комментарии по нотации AAA
  (`// Arrange` / `// Act` / `// Assert`), на русском.
- YAGNI: один админ, одна cookie, никаких JWT/refresh-token/ролей/анти-forgery/
  кластерного rate-limit (обоснования — §3).
- Секреты (пароль/hash) не попадают в git: только env-переменными поверх
  `appsettings.json` (`AdminPanel__Auth__PasswordHash` и т.п., arch/01 §6).

## 3. Решения в рамках контракта arch/ (уточнения неоднозначностей)

1. **Имя эндпоинта сессии — `GET /api/auth/me`** (arch/01 §4, arch/03 §1,
   roadmap). Формулировка «GET /api/auth/session» в тексте задания — описка;
   канон — arch/03 §1. Возвращает `{"username":"…"}` либо 401.
2. **Хранение пароля — оба варианта из arch/01 §4**: dev — plain `Password`,
   прод — `PasswordHash` в формате `$pbkdf2-sha256$<iterations>$<salt-b64>$
   <hash-b64>` (passlib-совместимый формат). Заданы оба → используется hash
   (приоритет по arch). В `appsettings.json` паролей нет (только `Username`);
   dev-креды — в `appsettings.Development.json`; прод — env. Утилита генерации
   hash в задачу не входит (готовится внешним инструментом, например passlib).
3. **Constant-time сравнение**: username и plain-password сравниваются через
   SHA256-дайджесты обеих сторон + `CryptographicOperations.FixedTimeEquals`
   (трюк с дайджестами даёт равные длины независимо от входа); PBKDF2-результат
   сравнивается `FixedTimeEquals` напрямую. При неверном username полный
   password-путь всё равно выполняется — время ответа не раскрывает, какое
   именно поле неверно (единый путь вычислений для обеих проверок, комбинация
   через `&`).
4. **PBKDF2-верификация**: `Rfc2898DeriveBytes.Pbkdf2DeriveBytes(password,
   salt, iterations, HashAlgorithmName.SHA256, outputLength)`, где iterations/
   salt/длина берутся из сконфигурированного hash. Минимальный порог iterations
   кодом не форсируется (значение выбирает оператор). Битый формат hash
   (не разбирается / не base64 / iterations < 1 / hash короче 16 байт) —
   fail-closed: логин отклоняется (401), `LogWarning` на каждую попытку —
   сигнал битой конфигурации; хост не падает.
5. **Пустая конфигурация пароля** (ни `Password`, ни `PasswordHash`) —
   fail-closed: все попытки логина 401; на старте хоста один `LogWarning`
   («аутентификация не сконфигурирована»). Хост поднимается — `healthz` жив.
6. **Rate-limit — собственный in-memory сервис** `LoginRateLimiter`
   (singleton): фиксированное окно 60 с / 5 попыток на ключ клиента; считаются
   все попытки, включая успешные (простота и предсказуемость). Ключ —
   `HttpContext.Connection.RemoteIpAddress` (или `"unknown"`, если null).
   `X-Forwarded-For` не читается: панель не за reverse-proxy по умолчанию,
   чтение XFF открыло бы спуфинг ключа. Счётчик в памяти процесса: перезапуск
   сбрасывает — для home-системы допустимо. 429 + заголовок `Retry-After`
   (секунды до конца окна). Своё, а не встроенный `AddRateLimiter`, — чтобы
   логика окна была unit-тестируемой через `TimeProvider`.
7. **Аутентификация — стандартный ASP.NET Core Cookie Authentication**
   (`AddAuthentication` + `AddCookie`), схема `CookieAuthenticationDefaults.
   AuthenticationScheme`. Cookie: имя `adminpanel_session`, `HttpOnly=true`,
   `SameSite=Lax`, `SlidingExpiration=true`, `ExpireTimeSpan =
   TimeSpan.FromHours(SessionHours)`; `SecurePolicy=Always`, либо
   `SameAsRequest` при `AllowHttp=true` (стенд/тесты по http). Значения
   cookie-опций берутся из `IOptions<AuthOptions>` через
   `AddOptions<CookieAuthenticationOptions>(scheme).Configure<IOptions<
   AuthOptions>>(…)` — т.е. `[Config]`-биндинг остаётся единственным источником.
   `SessionHours <= 0` → Warning и fallback 8 ч (защита от опечатки).
   `OnRedirectToLogin`/`OnRedirectToAccessDenied` переопределены: чистые
   401/403 без redirect (дефолт cookie-auth делает 302 на логин-страницу —
   для API запрещено). Claim: `ClaimTypes.Name` = сконфигурированный
   `Username` (каноническое значение).
8. **Guard `/api/*` — собственный конвенционный middleware** `UseApiAuthorization`
   после `UseAuthentication`: путь начинается с `/api` и не равен
   `/api/auth/login` и не равен `/api/healthz` и пользователь не
   аутентифицирован → немедленно 401 `ProblemDetails`; иначе `next()`.
   Сравнение путей — точное, без учёта регистра (вариации с trailing slash не
   исключаются: `/api/auth/login/` получит 401 — приемлемо, наш фронтенд
   такие пути не шлёт). Это точная реализация правила arch/01 §4 «всё
   `/api/*`, кроме login и healthz» как default-deny: будущие эндпоинты t03+
   защищены автоматически; статика/OpenAPI (вне `/api`) не затрагиваются —
   SPA-fallback в t07 не потребует ослаблений. `AddAuthorization`/
   FallbackPolicy/`RequireAuthorization` не используются.
9. **CSRF не вводится**: cookie `SameSite=Lax` (браузер не пришлёт её на
   cross-site POST) + JSON API без форм; анти-forgery-токены — YAGNI для
   home-системы (arch/01 §4 их не требует).
10. **`me` — через query-ветку CQRS**: `MeQuery(string Username) : IQuery<
    MeDto>` + `MeQueryHandler : IQueryHandler<MeQuery, MeDto>`
    (`[InjectAsScoped]`); эндпоинт достаёт имя из `ClaimsPrincipal`, кладёт в
    query и зовёт `IHandler.HandleQuery` — закладывает форму всех
    эндпоинтов t03+ (endpoint → dispatcher → `Result<T>` → 2xx/5xx).
11. **Модуль Api-сборки**: новый `ModuleExtensions.AddApi()` —
    `services.AutoRegistration(Api-сборка)`, симметрично `AddCore`/`AddEtcd`/
    `AddProbes`. Скан Api-сборки регистрирует auth-сервисы (`[InjectAs*]`) и
    `AuthOptions` (`[Config]`) и дальше автоподхватывает будущие типы Api.
    ASP.NET-регистрации, не выражаемые атрибутами (`AddAuthentication`/
    `AddCookie`), — в `AuthModule.AddCookieAuth()`; вызывается из `Program.cs`.
12. **Logout требует аутентификации** (по общему правилу «всё кроме login и
    healthz»): без cookie → 401 от guard; с cookie → `SignOutAsync` → 204,
    cookie погашается заголовком `Set-Cookie` с истечением.
13. **Unit-тесты auth-логики требуют ссылки на `AdminPanel.Api`**: auth живёт в
    Api по arch/01 §2, поэтому `AdminPanel.UnitTests.csproj` добавляет
    `ProjectReference` на `AdminPanel.Api` (тесто-проект ссылается на Web-SDK
    проект — так же уже устроен IntegrationTests). Сервисы в unit-тестах
    конструируются напрямую (`new` + `Options.Create`), без хоста.
14. **`TimeProvider` в DI**: `[InjectAsSingleton(typeof(TimeProvider))] sealed
    class SystemTimeProvider : TimeProvider` — регистрация базового типа через
    attribute-DI; `LoginRateLimiter` принимает `TimeProvider` единственным
    конструктором. В тестах подставляется собственный `FixedTimeProvider`
    (новый пакет `Microsoft.Extensions.TimeProvider.Testing` не тянется).
15. **Оркестратор логина**: `IAdminLoginService.Login(username, password,
    clientKey) → LoginResult(Status, RetryAfterSeconds)`: сперва rate-limit
    (иначе атакующий сжигал бы CPU на PBKDF2), затем `IAdminAuthenticator.
    Authenticate`. Endpoint тонкий: маппинг статуса в 204/401/429. Разделение
    даёт три независимо-тестируемых узла: окно лимитера, constant-time проверка
    учётных данных, связка.

## 4. Контракт API (фиксируется, сверка с arch/03 §1)

JSON camelCase; ошибки — `ProblemDetails` (`application/problem+json`).

| Метод+путь | Вход | Выход |
|---|---|---|
| `POST /api/auth/login` | `{"username":"…","password":"…"}` (JSON body) | 204 + `Set-Cookie: adminpanel_session=…; HttpOnly; SameSite=Lax` при успехе; 401 ProblemDetails (generic «Invalid credentials») при неверных данных; 429 ProblemDetails + `Retry-After` при исчерпании лимита; 400 при неразобранном JSON (авто-биндинг Minimal API) |
| `POST /api/auth/logout` | — (требует cookie) | 204 + `Set-Cookie` с истечением; без cookie → 401 (guard) |
| `GET /api/auth/me` | — (требует cookie) | 200 `{"username":"admin"}`; без cookie → 401 (guard) |
| `GET /api/healthz` | — (без auth) | как в t01, без изменений |
| прочее `/api/*` | — | 401 ProblemDetails без аутентификации (default-deny guard) |

Пустые/null `username`/`password` в теле логина трактуются как неверные
данные → 401 (не 400): не раскрываем причину отказа.

## 5. Состав изменений (дерево файлов)

```
src/AdminPanel.Api/
├── Program.cs                         [правка] + AddApi/AddCookieAuth/UseAuthentication/
│                                                UseApiAuthorization/MapAuthApi
├── ModuleExtensions.cs                [новый]  AddApi() → AutoRegistration(Api-сборка)
├── appsettings.json                   [правка] + "AdminPanel": { "Auth": { "Username": "admin" } }
├── appsettings.Development.json       [правка] + "AdminPanel": { "Auth": { "Username": "admin",
│                                                "Password": "admin", "AllowHttp": true } }
├── HealthzWriter.cs                   [без изменений]
└── Auth/
    ├── AuthOptions.cs                 [новый] [Config("AdminPanel:Auth")]-POCO
    ├── AuthModule.cs                  [новый] константы + AddCookieAuth + UseApiAuthorization
    │                                           + MapAuthApi + LoginRequest
    ├── AdminAuthenticator.cs          [новый] IAdminAuthenticator + constant-time реализация
    ├── LoginRateLimiter.cs            [новый] ILoginRateLimiter + LoginRateDecision + fixed window
    ├── AdminLoginService.cs           [новый] IAdminLoginService + LoginStatus + LoginResult +
    │                                           оркестратор
    └── MeQuery.cs                     [новый] MeQuery/MeDto/MeQueryHandler
src/tests/AdminPanel.UnitTests/
├── AdminPanel.UnitTests.csproj        [правка] + ProjectReference AdminPanel.Api
├── AdminAuthenticatorTests.cs         [новый]
├── AdminLoginServiceTests.cs          [новый]
├── MeQueryHandlerTests.cs             [новый]
└── FixedTimeProvider.cs               [новый] управляемый TimeProvider для окон лимитера
src/tests/AdminPanel.IntegrationTests/
└── AuthTests.cs                       [новый] + внутрифайловый helper-фабрика
arch/roadmap/infra.md                  [правка] удалить пункт t02-auth (см. §12)
```

`Directory.Packages.props`, `Directory.Build.props`, `.slnx`, проекты
Core/Etcd/Probes/Infrastructure — без изменений.

## 6. Настройки

### 6.1. `Auth/AuthOptions.cs`

```csharp
// [Config]-POCO аутентификации: секция AdminPanel:Auth (arch/01 §6).
[Config("AdminPanel:Auth")]
public class AuthOptions
{
    public string? Username { get; set; }       // единственный администратор
    public string? Password { get; set; }       // plain-пароль — только dev/стенд
    public string? PasswordHash { get; set; }   // $pbkdf2-sha256$i$salt-b64$hash-b64 — приоритет над Password
    public double SessionHours { get; set; } = 8;
    public bool AllowHttp { get; set; }         // true только для стенда по http
}
```

Свойства `get; set;` (не init) — так биндится `services.Configure<T>` в
`AutoRegistrationConfigDiTypeBehaviour` (паттерн t01, см. `TestConfigOptions`).

### 6.2. appsettings

`appsettings.json` (прод-базовая; секрета нет):

```json
"AdminPanel": {
  "Auth": {
    "Username": "admin"
  }
}
```

`appsettings.Development.json` (локальный запуск `dotnet run`, http-профиль):

```json
"AdminPanel": {
  "Auth": {
    "Username": "admin",
    "Password": "admin",
    "AllowHttp": true
  }
}
```

`AllowHttp=true` в Development обязателен: launch-профиль t01 — http, а
`SecurePolicy=Always` без него не выставит cookie. Прод: env
`AdminPanel__Auth__PasswordHash` (+ при необходимости `Username`,
`SessionHours`, `AllowHttp`).

## 7. Сервисы auth-модуля (`src/AdminPanel.Api/Auth/`)

### 7.1. `AdminAuthenticator.cs`

```csharp
// Проверка учётных данных единственного админа: constant-time, без rate-limit.
public interface IAdminAuthenticator
{
    bool Authenticate(string? username, string? password);
}

// Реализация: [InjectAsSingleton]; constructor(IOptions<AuthOptions>,
// ILogger<AdminAuthenticator>).
```

Алгоритм (фиксируется):

1. Достать конфиг `auth`. `auth.Username` null/пуст → false (fail-closed).
2. `usernameOk = FixedTimeEqualsSha256(username ?? "", auth.Username)`.
3. Password-ветка (выполняется всегда, даже при `!usernameOk`):
   - задан `PasswordHash` (непустой) → `VerifyPbkdf2(password, hash)`;
   - иначе задан `Password` → `FixedTimeEqualsSha256(password ?? "", auth.Password)`;
   - иначе → false (конфигурация пуста).
4. Результат `usernameOk & passwordOk` (обе проверки всегда выполнены).

`VerifyPbkdf2`: разбить hash по `'$'` → ожидается ровно 5 частей (первая
пустая), `parts[1] == "pbkdf2-sha256"`, `int.Parse(parts[2]) >= 1`,
`Convert.FromBase64String(parts[3])` — salt, `Convert.FromBase64String(
parts[4])` — ожидаемый ключ длиной ≥ 16 байт; вычислить
`Rfc2898DeriveBytes.Pbkdf2DeriveBytes(Encoding.UTF8.GetBytes(password ?? ""),
salt, iterations, HashAlgorithmName.SHA256, expected.Length)` и сравнить
`CryptographicOperations.FixedTimeEquals`. Любое нарушение формата —
`LogWarning` (ILogger) + false.

`FixedTimeEqualsSha256(a, b)`: `CryptographicOperations.FixedTimeEquals(
SHA256.HashData(utf8(a)), SHA256.HashData(utf8(b)))` — дайджесты равной длины,
сравнение по времени постоянно.

### 7.2. `LoginRateLimiter.cs`

```csharp
// Решение лимитера: разрешено ли и сколько секунд ждать до конца окна.
public sealed record LoginRateDecision(bool Allowed, int RetryAfterSeconds);

public interface ILoginRateLimiter
{
    LoginRateDecision TryAcquire(string clientKey);
}

// Реализация: [InjectAsSingleton]; fixed window 60 c / 5 попыток.
public const int MaxAttempts = 5;
public static readonly TimeSpan Window = TimeSpan.FromSeconds(60);
// constructor(TimeProvider); состояние — ConcurrentDictionary<string, (long windowId, int count)>.
```

`windowId = utcNow.Ticks / Window.Ticks`; `AddOrUpdate`: совпал `windowId` →
инкремент, иначе сброс в 1. `Allowed = count <= MaxAttempts`;
`RetryAfterSeconds` — остаток текущего окна (1..60). Очистки устаревших
ключей нет: ключей мало (реальные IP, без XFF), рост неограничен не будет.

### 7.3. `AdminLoginService.cs`

```csharp
public enum LoginStatus { Ok, InvalidCredentials, RateLimited }

// Результат попытки логина: статус + секунды до конца окна (для Retry-After).
public sealed record LoginResult(LoginStatus Status, int RetryAfterSeconds = 0);

// Оркестратор логина: rate-limit до проверки учётных данных (PBKDF2 дорог).
public interface IAdminLoginService
{
    LoginResult Login(string? username, string? password, string clientKey);
}

// Реализация: [InjectAsSingleton]; constructor(ILoginRateLimiter, IAdminAuthenticator).
```

Порядок: `TryAcquire(clientKey)` → не пропущено → `RateLimited` (+остаток);
иначе `Authenticate(username, password)` → `Ok | InvalidCredentials`.

### 7.4. `MeQuery.cs`

```csharp
// Запрос текущей сессии: username кладётся в query из ClaimsPrincipal.
public sealed record MeQuery(string Username) : IQuery<MeDto>;
public sealed record MeDto(string Username);

// Хендлер: чистое чтение без внешних зависимостей.
[InjectAsScoped]
public class MeQueryHandler : IQueryHandler<MeQuery, MeDto> { /* Success(MeDto) */ }
```

### 7.5. `AuthModule.cs`

Константы: `CookieName = "adminpanel_session"`, `LoginPath = "/api/auth/login"`,
`HealthzPath = "/api/healthz"`, `ApiPrefix = "/api"`. Три extension-метода:

- `AddCookieAuth(this IServiceCollection)` — `AddAuthentication(scheme).
  AddCookie(scheme, o => { имя/HttpOnly/SameSite=Lax/SlidingExpiration=true/
  SecurePolicy=Always; Events.OnRedirectToLogin → 401; OnRedirectToAccessDenied
  → 403 })` + `AddOptions<CookieAuthenticationOptions>(scheme).Configure<
  IOptions<AuthOptions>>(…)` (`ExpireTimeSpan` из `SessionHours` c fallback 8 и
  Warning; `SecurePolicy` из `AllowHttp`). `AddAuthorization` не вызывается —
  guard §3.8 заменяет.
- `UseApiAuthorization(this IApplicationBuilder)` — `app.Use(…)` (конвенционный
  middleware без DI): см. §3.8; тело 401 — ProblemDetails JSON
  (`{"title":"Unauthorized","status":401}`), `application/problem+json`.
- `MapAuthApi(this IEndpointRouteBuilder)` — три эндпоинта из §4:
  - `POST /api/auth/login(LoginRequest body, IAdminLoginService, HttpContext)`:
    `clientKey = RemoteIpAddress?.ToString() ?? "unknown"`; `Ok` →
    `ctx.SignInAsync(scheme, principal)` c `ClaimsIdentity` (claim
    `ClaimTypes.Name = auth.Username`) → `Results.NoContent()`; `RateLimited`
    → заголовок `Retry-After` + `Results.Problem(statusCode: 429)`;
    иначе → `Results.Problem(statusCode: 401, detail: "Invalid credentials")`.
    `LoginRequest` — `record LoginRequest(string? Username, string? Password)`
    (тоже в этом файле).
  - `POST /api/auth/logout(HttpContext)` → `SignOutAsync(scheme)` → 204.
  - `GET /api/auth/me(ClaimsPrincipal, IHandler, CancellationToken)` →
    `HandleQuery(new MeQuery(user.Identity!.Name!), ct)`; success →
    `Results.Ok(new { username = dto.Username })`; неуспех →
    `Results.Problem(statusCode: 500)`.

### 7.6. `SystemTimeProvider`

В `LoginRateLimiter.cs`: `[InjectAsSingleton(typeof(TimeProvider))] public
sealed class SystemTimeProvider() : TimeProvider;` — регистрация базового
типа для резолва `TimeProvider` в DI.

## 8. `Program.cs` после t02 (полностью)

```csharp
using AdminPanel.Api;
using AdminPanel.Core;
using AdminPanel.Etcd;
using AdminPanel.Infrastructure;
using AdminPanel.Infrastructure.DI;
using AdminPanel.Infrastructure.Traces;
using AdminPanel.Probes;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;

// Точка входа панели: сборка хоста и модульная композиция сервисов.
var builder = WebApplication.CreateBuilder(args);

// Инициализация ActivitySource каркаса до первого HandleQuery (по образцу референса).
Tracing.Init(builder.Environment.ApplicationName);

builder
   .Services.UseDiBehaviours(builder.Configuration)
   .AddInfrastructure()
   .AddApi()                       // [t02] auth-сервисы + [Config]-POCO Api-сборки
   .AddCore()
   .AddEtcd()
   .AddProbes()
   .AddOpenApi()
   .AddHealthChecks()
   .AddCheck("self", () => HealthCheckResult.Healthy(), ["live"]);

// [t02] cookie-схема аутентификации (настройки — AdminPanel:Auth).
builder.Services.AddCookieAuth();

var app = builder.Build();

// OpenAPI-схема — только в dev-окружении (вне /api, guard не защищает).
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

// [t02] аутентификация + default-deny guard: всё /api/*, кроме login и healthz, → 401.
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

Порядок пайплайна значим: `UseAuthentication` → `UseApiAuthorization` →
эндпоинты. Warning о пустой конфигурации пароля (§3.5) — одна строка после
`builder.Build()` через `app.Services.GetRequiredService<IOptions<AuthOptions>>()`.

## 9. Unit-тесты (`src/tests/AdminPanel.UnitTests/`)

Сервисы конструируются напрямую (`new AdminAuthenticator(Options.Create(
new AuthOptions { … }), NullLogger<AdminAuthenticator>.Instance)`,
`new LoginRateLimiter(new FixedTimeProvider())`) —
без TestHost (он остаётся для DI-тестов каркаса). Комментарии — AAA, русские.

### 9.1. `AdminAuthenticatorTests.cs`

- `PlainPassword_ValidCredentials_ReturnsTrue` — задан только `Password`,
  верные username+password → true.
- `PlainPassword_WrongPassword_ReturnsFalse`.
- `WrongUsername_ReturnsFalse` (верный пароль, чужой username).
- `PasswordHash_PrecedenceOverPlainPassword` — заданы оба: верный пароль по
  hash → true; верный plain, но неверный по hash → false.
- `PasswordHash_ValidPbkdf2_ReturnsTrue` — тест сам строит hash через
  `Rfc2898DeriveBytes.Pbkdf2DeriveBytes` (фиксированные salt/iterations,
  формат `$pbkdf2-sha256$…$salt-b64$hash-b64`) → true; изменённый пароль → false.
- `PasswordHash_Malformed_ReturnsFalse` — `"not-a-hash"`, отрицательные
  iterations, битый base64 → false (fail-closed).
- `EmptyConfig_ReturnsFalse` — только `Username`, пароля нет → false.
- `EmptyUsernameAndPassword_Input_ReturnsFalse` — null/пустые входы → false.

### 9.2. `AdminLoginServiceTests.cs`

Реальный `LoginRateLimiter` + `FixedTimeProvider`; authenticator — с
произвольными кредами.

- `ValidCredentials_ReturnsOk`.
- `WrongPassword_ReturnsInvalidCredentials`; `WrongUsername_ReturnsInvalidCredentials`.
- `RateLimit_SixthAttemptSameIp_ReturnsRateLimited` — 5 попыток → не
  `RateLimited`, 6-я → `RateLimited` с `RetryAfterSeconds` в 1..60.
- `RateLimit_WindowReset_AllowsAgain` — после `Utc += 61 c` попытка снова
  обрабатывается (`InvalidCredentials`, не `RateLimited`).
- `RateLimit_DifferentIp_Independent` — 5 попыток с IP-A, IP-B всё ещё
  допускается.
- `RateLimit_CountsSuccessfulLogins` — 5 успешных с одного IP, 6-я (с верными
  кредами) → `RateLimited` (фиксация решения §3.6).

### 9.3. `MeQueryHandlerTests.cs`

- `Handle_ReturnsUsernameFromQuery` — `Result<MeDto>` success с исходным
  username.

### 9.4. `FixedTimeProvider.cs`

`sealed class FixedTimeProvider : TimeProvider` с mutable `UtcNow` (старт —
фиксированная дата) — управляемое время для окон лимитера.

## 10. Integration-тесты (`src/tests/AdminPanel.IntegrationTests/AuthTests.cs`)

Единая фабрика на тестовую сборку — xunit collection fixture: файл объявляет
`FixedTimeProvider : TimeProvider` (управляемое `Utc`), `AuthWebFactory :
WebApplicationFactory<Program>` (в `ConfigureWebHost` — `UseSetting`
Username=admin, Password=adminpw, AllowHttp=true; http-сервер тестов — иначе
`Secure` cookie не вернётся клиенту) и `[CollectionDefinition("api")] ApiCollection
: ICollectionFixture<AuthWebFactory>`. Причина единственности хоста:
статический кеш просканированных сборок у attribute-DI (t01 §9.1, заметка
`TestHost`) не позволяет построить второй хост в том же процессе — повторный
`AutoRegistration` молча не регистрирует сервисы, и эндпоинты падали бы 500.
Поэтому `HealthzTests` переводится в ту же коллекцию `"api"` и использует
общий хост (правка t01-файла — только механика, контракт healthz не меняется).
Изоляция rate-limit между тестами: singleton-`LoginRateLimiter` один на хост,
окно сбрасывается управляемым временем — фабрика подменяет `TimeProvider`
на `FixedTimeProvider` через `ConfigureTestServices` +
`Replace(ServiceDescriptor.Singleton(typeof(TimeProvider), time))` (после
композиции Program), а каждый тест с логинами в Arrange делает
`factory.Time.Utc += 61 c` → новый `windowId` → счётчик с нуля. Клиент для
строгих проверок статуса — `CreateClient(new WebApplicationFactoryClientOptions
{ AllowAutoRedirect = false })`; default-клиент (`HandleCookies = true`)
используется там, где cookie нужно нести между запросами.

- `Login_ValidCredentials_Returns204AndSessionCookie` — 204, заголовок
  `Set-Cookie` содержит `adminpanel_session=`.
- `Login_WrongPassword_Returns401ProblemDetails` — 401,
  `application/problem+json`.
- `Login_WrongUsername_Returns401`.
- `Login_MalformedJson_Returns400`.
- `Login_RateLimit_SixthAttempt_Returns429` — 5 неудачных → 401, 6-я → 429 с
  заголовком `Retry-After`.
- `Me_WithoutCookie_Returns401NotRedirect` — `AllowAutoRedirect=false`;
  статус ровно 401 (нет 302 — фиксация §3.7).
- `Me_WithCookie_ReturnsUsername` — логин default-клиентом, затем `GET
  /api/auth/me` → 200 `{"username":"admin"}`.
- `Logout_WithCookie_Returns204AndInvalidatesSession` — 204; повторный `GET
  me` тем же клиентом → 401.
- `Api_DefaultDeny_WithoutCookie_Returns401` — произвольный защищённый путь
  (например `GET /api/auth/me` и `POST /api/auth/logout`) без cookie → 401;
  `GET /api/healthz` без cookie → 200 (исключение guard'а).

## 11. Ограничения (что НЕ делается)

- Пользователи/роли/аудит/БД учётных записей, JWT/refresh-token — нет
  (arch/01 §4, §9).
- Анти-forgery, lockout по учётной записи, парсинг `X-Forwarded-For`,
  распределённый rate-limit — нет (§3.6, §3.9).
- Встроенная `AddRateLimiter`-политика — не используется (§3.6).
- Утилита генерации PBKDF2-hash и фронтенд-страница Login — не входят
  (frontend.md `t07`; hash готовится внешним инструментом).
- SPA-статика/wwwroot, CORS, HTTPS-конфигурация Kestrel — не трогаются.
- Мутации `arch/01–03` запрещены; из `arch/roadmap/` меняется только §12.
- `Directory.Packages.props` без изменений — новых пакетов нет.

## 12. Деливерабл roadmap

Тем же мерж-коммитом удалить пункт `t02-auth` (строку) из
`arch/roadmap/infra.md`. Зависимости других пунктов от `t02-auth` не трогаются:
`arch/roadmap/etcd.md` (`t04-etcd-api ← t02-auth`) и
`arch/roadmap/frontend.md` (`t07-frontend-base ← t02-auth`) остаются как есть —
по указанию координатора и прецеденту t01 (зависимость `← t01-skeleton`
в самой строке t02 после мержа t01 не очищалась).

## 13. Критерии приёмки

1. `dotnet build src/AdminPanel.slnx` — успех, 0 warnings
   (`TreatWarningsAsErrors=true` не подавлен).
2. `dotnet test src/AdminPanel.slnx` — все тесты зелёные; Docker не нужен
   (Testcontainers в t02 не используется).
3. Ручной сценарий (Development, `dotnet run --project src/AdminPanel.Api`):
   - `curl -i http://localhost:5000/api/healthz` → 200 без cookie;
   - `curl -i -X POST http://localhost:5000/api/auth/login -H "Content-Type:
     application/json" -d '{"username":"admin","password":"wrong"}'` → 401;
   - то же с `"password":"admin"` → 204 + `Set-Cookie: adminpanel_session=…`;
   - с cookie `curl …/api/auth/me` → 200 `{"username":"admin"}`;
   - без cookie `…/api/auth/me` → 401 (не 302);
   - `POST …/api/auth/logout` с cookie → 204, повторный `me` → 401;
   - шестой логин в минуту с одного адреса → 429 + `Retry-After`.
4. `appsettings.json` не содержит `Password`/`PasswordHash` (секретов в git
   нет); dev-креды живут только в `appsettings.Development.json`.
5. `grep PackageReference` по всем csproj не даёт новых ссылок;
   `Directory.Packages.props` не изменился.
6. Пункт `t02-auth` отсутствует в `arch/roadmap/infra.md`; `← t02-auth` в
   `etcd.md`/`frontend.md` сохранён; других мутаций `arch/` нет.
7. Все решения §3 не противоречат arch/01 §4/§6 и arch/03 §1 (проверка на
   ревью).

## 14. Риски и заметки

- **Secure cookie по http**: запуск по http с `AllowHttp=false` приведёт к
  тому, что cookie не выставится (браузер/`CookieContainer` не вернёт `Secure`
  cookie по http) — симптомы «логин 204, но me 401». Прод тихо не пострадает
  (там https); для стенда/тестов `AllowHttp=true` обязателен. Зафиксировано в
  §6.2 и конфигурации тестов (§10).
- **Rate limiter в памяти**: перезапуск процесса обнуляет счётчик; несколько
  инстансов панели (не наш деплой) не делили бы счётчик — принято.
- **Timing-равномерность не тестируется автотестами** (флаки по природе):
  фиксируется структурой кода (§3.3 — единый путь вычислений) и проверяется
  ревью.
- **WAF-окружение**: фабрика стартует хост в Production — `appsettings.json`
  применится; все auth-настройки тесты задают явно через `UseSetting`,
  значения по умолчанию на тесты не завязаны.
- **Один хост на процесс в IntegrationTests**: статический кеш сборок
  `ServiceCollectionExtensions` (t01) означает, что вторая
  `WebApplicationFactory<Program>` в том же процессе останется без сервисов
  сборок (тихие 500 на эндпоинтах) — поэтому collection fixture `"api"` и
  перевод `HealthzTests` в общую коллекцию; в UnitTests отдельный процесс,
  `TestHost` там не конфликтует.
- **`Activator.CreateInstance` в `[Config]`-поведении** требует
  параметрless-конструктор: `AuthOptions` — только свойства с инициализаторами
  (§6.1), как `TestConfigOptions` в t01.
- **Гарды и routing**: `UseApiAuthorization` стоит до endpoint-мидлварей —
  несуществующие `/api/…`-пути без cookie получают 401 (а не 404); с cookie —
  404. Считается корректным поведением default-deny.
- **`Results.Problem` + заголовок `Retry-After`**: заголовок ставится в
  `HttpContext.Response.Headers` до возврата `Results.Problem(429)` —
  Minimal-API result пишет в тот же ответ, заголовок сохраняется.
