# t04-open-metrics-healthz-facet — план реализации (кодовая миграция открытой грани /healthz и /metrics)

> **Для исполняющих агентов:** план исполняется по шагам A→D строго по порядку.
> Шаги — для СИЛЬНОЙ модели (по умолчанию субагент coder; при executor="main" —
> main-агент). Подпункты (нумерация `N.M`) — самодостаточные задания для СЛАБОЙ
> модели (субагент coder-flash БЕЗ контекста проекта): сильная модель выдаёт их
> по одному, затем ревьюит результат и исправляет сама. Всё, что не выделено в
> подпункты (ревью, отладка, прогоны, разбор логов), делает сильная модель.

**Цель:** зеркалировать в код принятый arch-канон открытой грани: клиентский
сертификат опционален на TLS-хендшейке, валидный серт per-install API-CA
обязателен только на `/api` (HTTP 401 без него), `/healthz` и `/metrics`
отвечают 200 без клиентского серта; упразднить выпуск клиентских сертов
`prometheus.*`/`healthcheck.*` и перевести всех их потребителей на безсертный
доступ.

**Архитектура:** общий middleware `Shared.Tls.ClientCertAuthMiddleware`
(401-политика на `/api`) + `ClientCertificateMode.AllowCertificate` без
`ClientCertificateValidation` в Kestrel четырёх воркеров; потребители сертов
`healthcheck`/`prometheus` (HEALTHCHECK Dockerfile, Prometheus, чеки стенда)
переводятся на `curl --cacert`/`tls_config {ca_file}` без клиентской пары;
`/api`-вызовы чеков — на пару `seed.crt/key`.

**Стек:** .NET 10, C# (`LangVersion=latest`, `Nullable=enable`,
`TreatWarningsAsErrors=true`), ASP.NET Core Kestrel, bash-чеки стенда.

**Spec:** `docs/superpowers/2026-10-30-t04-open-metrics-healthz-facet/spec.md`
(исполнитель читает spec и этот план вместе).

Все пути ниже — от корня worktree
`/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet`
(в подпунктах — абсолютные).

## Глобальные ограничения

- P/Invoke запрещён; только управляемый API .NET (spec §5).
- Таймауты/бюджеты тестов НЕ менять; скипов не добавлять (spec §5).
- Комментарии/доки — по-русски; идентификаторы и строки сообщений валидации —
  английские (правило языка проекта).
- E2E-изоляция и зачистка docker-контейнеров/сетей после КАЖДОЙ тестовой серии
  (AGENTS.md); лог длинных команд — только в файл, чтение из файла.
- AdminPanel (код и тесты), E2E-тесты всех воркеров, etcd-контракт, дискавери,
  `WorkerApiCertReader`, серверные серты, `deploy/tls/gen-docker.sh`,
  docker-CA, `arch/` (кроме мерж-гейтового удаления тега задачи из
  `arch/roadmap/` — шаг D4) — НЕ трогать (spec §3.4).
- arch-номер воркера в комментариях: PgWorker=arch/14, KafkaWorker=arch/16,
  ValkeyWorker=arch/21, S3Worker=arch/24.
- Режим `AllowInsecureHttp=true` (WAF-тесты) — pass-through без 401 (spec §2,
  §6 п.3).

## Review Focus (классы входов, покрываемые дополнительно к ассертам spec)

1. Путь-имитатор префикса (`/apifoo`) без серта — обязан быть 404 (открытая
   грань), НЕ 401 (spec §6 п.4) — ассерт 7 в тестах шага B1.
2. Невалидный серт на `/healthz` — 200, серт игнорируется открытой гранью,
   хендшейк не рвётся (spec §6 п.6) — ассерт 6 в тестах шага B1.
3. WAF-режим `AllowInsecureHttp=true`: `/api` без серта — 200 (pass-through,
   spec §6 п.3) — уже покрыт существующими in-memory WAF-тестами мутаций
   (не трогаем, регрессия ловится прогонами шага B3).
4. Регистр пути (`OrdinalIgnoreCase`-сравнение) — закреплён кодом middleware
   (подпункт A1.2 содержит `OrdinalIgnoreCase` дословно); отдельного теста нет —
   проверяется ревью кода на шаге A1.
5. Истёкший/отозванный клиентский серт — та же ветка `TlsChain.ValidateChain`
   `NoCheck`/`CustomRootTrust`, что у невалидного CA (spec §6 п.5: «без
   изменения семантики доверия») — покрывается ассертом 5 шага B1; отдельного
   теста не добавляем.

---

## Фаза A — код воркеров (spec §3.1, фаза 1)

### Шаг A1: `src/Shared.Tls` — FrameworkReference + middleware `ClientCertAuthMiddleware`

**Вход:** worktree чист, ветка `feat-t04-open-metrics-healthz-facet` актуальна.

**Действие (кодер):** выполняет подпункты A1.1 и A1.2, затем ревьюит созданный
файл: сигнатуры совпадают с spec §3.1.1 (`ClientCertAuthOptions(bool Enabled,
X509Certificate2? ClientCa)`, конструктор
`ClientCertAuthMiddleware(RequestDelegate, ClientCertAuthOptions)`), порядок
веток 1–5 дословно из spec, комментарии русские со ссылкой на arch/14 §1.1.
Проверяет, что другие проекты решения не требуют правок из-за
`FrameworkReference` (runtime-пакеты `microsoft.aspnetcore.app.runtime.*` уже
в локальном NuGet-кеше — self-contained publish NodeTools/WalReceiver не
падает; если упадёт — разбирать по логу сборки, не менять бюджетов).

**Выход:** `src/Shared.Tls` содержит middleware; проект собирается.

**Проверка:** из корня worktree, в фоне, лог в файл:
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/Shared.Tls/Shared.Tls.csproj -c Release --nologo > /tmp/t04-build-A1.log 2>&1`
— `Build succeeded`, `0 Warning(s)`, `0 Error(s)`.

**Связь со spec:** §3.1.1 (полностью), §6 п.1/п.2/п.4/п.11.

**Подпункты (слабая модель):**

- **A1.1.** В файле `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/src/Shared.Tls/Shared.Tls.csproj`
  после существующего `<ItemGroup>…</ItemGroup>` добавь новый ItemGroup:

  ```xml
    <ItemGroup>
        <FrameworkReference Include="Microsoft.AspNetCore.App" />
    </ItemGroup>
  ```

  Больше в файле ничего не менять.

- **A1.2.** Создай новый файл
  `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/src/Shared.Tls/ClientCertAuthMiddleware.cs`
  ровно с таким содержимым:

  ```csharp
  using System.Security.Cryptography.X509Certificates;
  using Microsoft.AspNetCore.Http;

  namespace Shared.Tls;

  // 401-политика клиентского серта на HTTPS-грани воркеров (arch/14 §1.1 —
  // канон всех четырёх воркеров: PgWorker/KafkaWorker/ValkeyWorker/S3Worker):
  // хендшейк серт не требует (ClientCertificateMode.AllowCertificate), здесь —
  // точечная авторизация. Валидный клиентский серт per-install API-CA обязателен
  // ТОЛЬКО на префиксе /api: без серта или с невалидным сертом — HTTP 401 (НЕ
  // разрыв TLS). /healthz и /metrics — открытая грань доверенной docker-сети:
  // присланный серт игнорируется. Enabled=false — pass-through (режим
  // AllowInsecureHttp, только in-memory WAF-тесты).
  public sealed record ClientCertAuthOptions(bool Enabled, X509Certificate2? ClientCa);

  public sealed class ClientCertAuthMiddleware(RequestDelegate next, ClientCertAuthOptions options)
  {
      public async Task InvokeAsync(HttpContext context)
      {
          // 1) WAF-режим (AllowInsecureHttp): без TLS-грани — pass-through.
          if (!options.Enabled)
          {
              await next(context);
              return;
          }

          // 2) Открытая грань: всё, кроме точного сегмент-префикса /api
          // ("/api" или "/api/…", case-insensitive; "/apifoo" — не /api).
          var path = context.Request.Path.Value ?? "/";
          if (!path.Equals("/api", StringComparison.OrdinalIgnoreCase)
              && !path.StartsWith("/api/", StringComparison.OrdinalIgnoreCase))
          {
              await next(context);
              return;
          }

          // 3) Нет клиентского серта — 401 (без тела, без WWW-Authenticate).
          if (context.Connection.ClientCertificate is null)
          {
              context.Response.StatusCode = StatusCodes.Status401Unauthorized;
              return;
          }

          // 4) Серт не проходит цепочку к per-install API-CA — 401.
          if (!TlsChain.ValidateChain(context.Connection.ClientCertificate, options.ClientCa!))
          {
              context.Response.StatusCode = StatusCodes.Status401Unauthorized;
              return;
          }

          // 5) Валидный серт — пропустить дальше.
          await next(context);
      }
  }
  ```

### Шаг A2: четыре `TlsEndpoints`/`ApiTlsEndpoints` — AllowCertificate, без ClientCertificateValidation, регистрация опции middleware

**Вход:** шаг A1 завершён (middleware и опция существуют).

**Действие (кодер):** выполняет подпункты A2.1–A2.4 (правки идентичны по
механике в четырёх файлах), затем ревьюит каждый файл целиком: в TLS-ветке
нет `RequireCertificate` и `ClientCertificateValidation`; в обеих ветках
зарегистрирован `ClientCertAuthOptions`; шапки-комментарии соответствуют
канону «HTTPS-грань; серт опционален, mTLS только на /api». Исправления после
ревью — сам.

**Выход:** четыре файла мигрированы; решение собирается (новая опция пока
используется только DI-регистрацией).

**Проверка:** по завершении фазы A — шаг A4.

**Связь со spec:** §3.1.2, §6 п.1/п.3/п.5.

**Подпункты (слабая модель):**

- **A2.1.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/src/PgWorker.App/Api/ApiTlsEndpoints.cs`.
  Три правки:

  1) Замени шапку-комментарий класса (6 строк, начиная
     `// mTLS HTTP-грани PgWorker (arch/14 §1.1): вся грань (вкл. /healthz и`):

  ```csharp
  // HTTPS HTTP-грань PgWorker (arch/14 §1.1): клиентский серт опционален на
  // хендшейке, mTLS обязателен только на /api (401 без валидного серта —
  // Shared.Tls.ClientCertAuthMiddleware); /healthz и /metrics — открытая грань
  // доверенной docker-сети. Вызывается на WebApplicationBuilder ДО Build().
  // Сертификаты живут всё приложение (без using).
  ```

  2) Замени блок раннего возврата WAF-режима

  ```csharp
          if (tls.AllowInsecureHttp)
              return new ApiTlsSetup(null, null, null); // без TLS — только WAF-тесты; warning логирует Program.cs
  ```

  на

  ```csharp
          if (tls.AllowInsecureHttp)
          {
              // WAF-режим (in-memory тесты, без TLS): middleware в pipeline у всех —
              // pass-through (Enabled=false).
              builder.Services.AddSingleton(new ClientCertAuthOptions(Enabled: false, ClientCa: null));
              return new ApiTlsSetup(null, null, null); // без TLS — только WAF-тесты; warning логирует Program.cs
          }
  ```

  3) Замени блок `ConfigureKestrel`

  ```csharp
          var port = ResolvePort(builder.Configuration);
          // Явный Listen подавляет default-URL — только mTLS-грань.
          builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port, listenOptions => listenOptions.UseHttps(
              new HttpsConnectionAdapterOptions
              {
                  ServerCertificate = serverCert,
                  ClientCertificateMode = ClientCertificateMode.RequireCertificate,
                  ClientCertificateValidation = (certificate, _, _) => TlsChain.ValidateChain(certificate, clientCa),
              })));
  ```

  на

  ```csharp
          // 401-политика /api (arch/14 §1.1): опция middleware — DI-синглтон;
          // clientCa живёт всё приложение (уже загружен без using, как серверный).
          builder.Services.AddSingleton(new ClientCertAuthOptions(Enabled: true, ClientCa: clientCa));

          var port = ResolvePort(builder.Configuration);
          // Явный Listen подавляет default-URL — только HTTPS-грань. Серт
          // запрашивается, но НЕ требуется (AllowCertificate) и НЕ валидируется
          // на хендшейке: валидация — в ClientCertAuthMiddleware (401 только на
          // /api; /healthz и /metrics — открытая грань доверенной docker-сети).
          builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port, listenOptions => listenOptions.UseHttps(
              new HttpsConnectionAdapterOptions
              {
                  ServerCertificate = serverCert,
                  ClientCertificateMode = ClientCertificateMode.AllowCertificate,
              })));
  ```

- **A2.2.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/src/KafkaWorker.App/Api/TlsEndpoints.cs`.
  Три правки:

  1) Замени шапку-комментарий класса (6 строк, начиная
     `// mTLS HTTP-грани воркера (arch/16 §1.1): вся грань (вкл. /healthz) —`):

  ```csharp
  // HTTPS HTTP-грань воркера (arch/16 §1.1): клиентский серт опционален на
  // хендшейке, mTLS обязателен только на /api (401 без валидного серта —
  // Shared.Tls.ClientCertAuthMiddleware); /healthz и /metrics — открытая грань
  // доверенной docker-сети. Вызывается на WebApplicationBuilder ДО Build()
  // (ConfigureKestrel — этап хоста) — общий код Program.cs и MtlsApiTests.
  // Сертификаты живут всё приложение (без using).
  ```

  2) Замени блок раннего возврата WAF-режима

  ```csharp
          if (tls.AllowInsecureHttp)
              return new ApiTlsSetup(null, null, null); // без TLS — только WAF-тесты; warning логирует Program.cs
  ```

  на

  ```csharp
          if (tls.AllowInsecureHttp)
          {
              // WAF-режим (in-memory тесты, без TLS): middleware в pipeline у всех —
              // pass-through (Enabled=false).
              builder.Services.AddSingleton(new ClientCertAuthOptions(Enabled: false, ClientCa: null));
              return new ApiTlsSetup(null, null, null); // без TLS — только WAF-тесты; warning логирует Program.cs
          }
  ```

  3) Замени блок `ConfigureKestrel`

  ```csharp
          // Явный Listen подавляет default-URL (ASPNETCORE_HTTP_PORTS) — только mTLS.
          builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port, listenOptions => listenOptions.UseHttps(
              new HttpsConnectionAdapterOptions
              {
                  ServerCertificate = serverCert,
                  ClientCertificateMode = ClientCertificateMode.RequireCertificate,
                  ClientCertificateValidation = (certificate, _, _) => TlsChain.ValidateChain(certificate, clientCa),
              })));
  ```

  на

  ```csharp
          // 401-политика /api (arch/16 §1.1): опция middleware — DI-синглтон;
          // clientCa живёт всё приложение (уже загружен без using, как серверный).
          builder.Services.AddSingleton(new ClientCertAuthOptions(Enabled: true, ClientCa: clientCa));

          // Явный Listen подавляет default-URL (ASPNETCORE_HTTP_PORTS) — только
          // HTTPS-грань. Серт запрашивается, но НЕ требуется (AllowCertificate) и
          // НЕ валидируется на хендшейке: валидация — в ClientCertAuthMiddleware
          // (401 только на /api; /healthz и /metrics — открытая грань).
          builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port, listenOptions => listenOptions.UseHttps(
              new HttpsConnectionAdapterOptions
              {
                  ServerCertificate = serverCert,
                  ClientCertificateMode = ClientCertificateMode.AllowCertificate,
              })));
  ```

- **A2.3.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/src/ValkeyWorker.App/Api/TlsEndpoints.cs`.
  Три правки:

  1) Замени шапку-комментарий класса (6 строк, начиная
     `// mTLS HTTP-грани воркера (arch/21 §1.1): вся грань (вкл. /healthz) —`):

  ```csharp
  // HTTPS HTTP-грань воркера (arch/21 §1.1): клиентский серт опционален на
  // хендшейке, mTLS обязателен только на /api (401 без валидного серта —
  // Shared.Tls.ClientCertAuthMiddleware); /healthz и /metrics — открытая грань
  // доверенной docker-сети. Вызывается на WebApplicationBuilder ДО Build()
  // (ConfigureKestrel — этап хоста) — общий код Program.cs и MtlsApiTests.
  // Сертификаты живут всё приложение (без using).
  ```

  2) Замени блок раннего возврата WAF-режима

  ```csharp
          if (tls.AllowInsecureHttp)
              return new ApiTlsSetup(null, null, null); // без TLS — только WAF-тесты; warning логирует Program.cs
  ```

  на

  ```csharp
          if (tls.AllowInsecureHttp)
          {
              // WAF-режим (in-memory тесты, без TLS): middleware в pipeline у всех —
              // pass-through (Enabled=false).
              builder.Services.AddSingleton(new ClientCertAuthOptions(Enabled: false, ClientCa: null));
              return new ApiTlsSetup(null, null, null); // без TLS — только WAF-тесты; warning логирует Program.cs
          }
  ```

  3) Замени блок `ConfigureKestrel`

  ```csharp
          // Явный Listen подавляет default-URL (ASPNETCORE_HTTP_PORTS) — только mTLS.
          builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port, listenOptions => listenOptions.UseHttps(
              new HttpsConnectionAdapterOptions
              {
                  ServerCertificate = serverCert,
                  ClientCertificateMode = ClientCertificateMode.RequireCertificate,
                  ClientCertificateValidation = (certificate, _, _) => TlsChain.ValidateChain(certificate, clientCa),
              })));
  ```

  на

  ```csharp
          // 401-политика /api (arch/21 §1.1): опция middleware — DI-синглтон;
          // clientCa живёт всё приложение (уже загружен без using, как серверный).
          builder.Services.AddSingleton(new ClientCertAuthOptions(Enabled: true, ClientCa: clientCa));

          // Явный Listen подавляет default-URL (ASPNETCORE_HTTP_PORTS) — только
          // HTTPS-грань. Серт запрашивается, но НЕ требуется (AllowCertificate) и
          // НЕ валидируется на хендшейке: валидация — в ClientCertAuthMiddleware
          // (401 только на /api; /healthz и /metrics — открытая грань).
          builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port, listenOptions => listenOptions.UseHttps(
              new HttpsConnectionAdapterOptions
              {
                  ServerCertificate = serverCert,
                  ClientCertificateMode = ClientCertificateMode.AllowCertificate,
              })));
  ```

- **A2.4.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/src/S3Worker.App/Api/TlsEndpoints.cs`.
  Три правки:

  1) Замени шапку-комментарий класса (6 строк, начиная
     `// mTLS HTTP-грани воркера (arch/24 §1.1): вся грань (вкл. /healthz) —`):

  ```csharp
  // HTTPS HTTP-грань воркера (arch/24 §1.1): клиентский серт опционален на
  // хендшейке, mTLS обязателен только на /api (401 без валидного серта —
  // Shared.Tls.ClientCertAuthMiddleware); /healthz и /metrics — открытая грань
  // доверенной docker-сети. Вызывается на WebApplicationBuilder ДО Build()
  // (ConfigureKestrel — этап хоста) — общий код Program.cs и MtlsApiTests.
  // Сертификаты живут всё приложение (без using).
  ```

  2) Замени блок раннего возврата WAF-режима

  ```csharp
          if (tls.AllowInsecureHttp)
              return new ApiTlsSetup(null, null, null); // без TLS — только WAF-тесты; warning логирует Program.cs
  ```

  на

  ```csharp
          if (tls.AllowInsecureHttp)
          {
              // WAF-режим (in-memory тесты, без TLS): middleware в pipeline у всех —
              // pass-through (Enabled=false).
              builder.Services.AddSingleton(new ClientCertAuthOptions(Enabled: false, ClientCa: null));
              return new ApiTlsSetup(null, null, null); // без TLS — только WAF-тесты; warning логирует Program.cs
          }
  ```

  3) Замени блок `ConfigureKestrel`

  ```csharp
          // Явный Listen подавляет default-URL (ASPNETCORE_HTTP_PORTS) — только mTLS.
          builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port, listenOptions => listenOptions.UseHttps(
              new HttpsConnectionAdapterOptions
              {
                  ServerCertificate = serverCert,
                  ClientCertificateMode = ClientCertificateMode.RequireCertificate,
                  ClientCertificateValidation = (certificate, _, _) => TlsChain.ValidateChain(certificate, clientCa),
              })));
  ```

  на

  ```csharp
          // 401-политика /api (arch/24 §1.1): опция middleware — DI-синглтон;
          // clientCa живёт всё приложение (уже загружен без using, как серверный).
          builder.Services.AddSingleton(new ClientCertAuthOptions(Enabled: true, ClientCa: clientCa));

          // Явный Listen подавляет default-URL (ASPNETCORE_HTTP_PORTS) — только
          // HTTPS-грань. Серт запрашивается, но НЕ требуется (AllowCertificate) и
          // НЕ валидируется на хендшейке: валидация — в ClientCertAuthMiddleware
          // (401 только на /api; /healthz и /metrics — открытая грань).
          builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port, listenOptions => listenOptions.UseHttps(
              new HttpsConnectionAdapterOptions
              {
                  ServerCertificate = serverCert,
                  ClientCertificateMode = ClientCertificateMode.AllowCertificate,
              })));
  ```

### Шаг A3: четыре `Program.cs` + `KafkaWorker.App/Options.cs` + `S3StartupChecks.cs` — подключение middleware и комментарии

**Вход:** шаг A2 завершён.

**Действие (кодер):** выполняет подпункты A3.1–A3.6, затем ревьюит: в каждом
`Program.cs` вызов `app.UseMiddleware<Shared.Tls.ClientCertAuthMiddleware>();`
стоит ПОСЛЕ `builder.Build()`/warning-логов и ДО `app.MapAppMetrics()`;
формулировки «mTLS-only» исчезли; идентификаторы/env не тронуты.

**Выход:** middleware в pipeline четырёх воркеров; комментарии мигрированы.

**Проверка:** шаг A4 (сборка).

**Связь со spec:** §3.1.3, §6 п.1.

**Подпункты (слабая модель):**

- **A3.1.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/src/PgWorker.App/Program.cs`.
  Пять правок:

  1) Строку
     `// Fail-fast: advertise без URL бессмысленен; http-advertise запрещён при mTLS-каноне (arch/14 §1.1).`
     замени на
     `// Fail-fast: advertise без URL бессмысленен; http-advertise запрещён (HTTPS-грань, arch/14 §1.1).`

  2) Строку
     `"PgWorker:Api:AdvertiseUrl must be https:// (mTLS-only API, arch/14 §1.1)")`
     замени на
     `"PgWorker:Api:AdvertiseUrl must be https:// (HTTPS-only edge, mTLS on /api only; arch/14 §1.1)")`

  3) Двухстрочный комментарий
     `// mTLS HTTP API (arch/14 §1.1): Kestrel с серверным сертом и требованием`
     `// клиентского серта per-install API-CA (порт — из ASPNETCORE_URLS/urls, иначе 8080).`
     замени на
     `// HTTPS HTTP API (arch/14 §1.1): Kestrel с серверным сертом; клиентский серт`
     `// опционален, mTLS (401) — только /api (порт — из ASPNETCORE_URLS/urls, иначе 8080).`

  4) Строку
     `// Метрики (arch/18 §3): /metrics на том же mTLS-Kestrel-порту, что /healthz.`
     замени на
     `// Метрики (arch/18 §3): /metrics на том же HTTPS-Kestrel-порту, что /healthz.`

  5) Между строкой `app.Logger.LogWarning("PgWorker:Api:Tls: {Warning}", certWarning);`
     и строкой `app.MapAppMetrics();` вставь:

  ```csharp
  // Открытая грань (arch/14 §1.1): клиентский серт опционален на хендшейке;
  // 401 без валидного серта — только на /api; /healthz и /metrics — без серта.
  app.UseMiddleware<Shared.Tls.ClientCertAuthMiddleware>();
  ```

- **A3.2.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/src/KafkaWorker.App/Program.cs`.
  Пять правок:

  1) В шапке файла строку
     `// Точка входа KafkaWorker (arch/16 §8): host-builder с mTLS-гранью HTTP API`
     замени на
     `// Точка входа KafkaWorker (arch/16 §8): host-builder с HTTPS-гранью HTTP API`
     а следующую строку `// (вкл. /healthz), конфигурация appsettings+env, DI всех слоёв (etcd →`
     замени на
     `// (mTLS — только /api; /healthz — открытая грань), конфигурация appsettings+env, DI (etcd →`

  2) Двухстрочный комментарий
     `// Метрики (arch/18 §3): /metrics на том же mTLS-Kestrel-порту, что /healthz`
     `//  — scrape ходит клиентским сертом per-install пакета.`
     замени на
     `// Метрики (arch/18 §3): /metrics на том же HTTPS-Kestrel-порту, что /healthz`
     `//  — открытая грань скрейпа без клиентского серта (arch/18 §5.2).`

  3) Строку
     `"AdvertiseUrl must be https:// (mTLS-only API, arch/16 §1.1)")`
     замени на
     `"AdvertiseUrl must be https:// (HTTPS-only edge, mTLS on /api only; arch/16 §1.1)")`

  4) Двухстрочный комментарий
     `// mTLS HTTP API (arch/16 §1.1): env-секреты TLS → конфиг, Kestrel c`
     `// серверным сертом и требованием клиентского серта per-install API-CA.`
     замени на
     `// HTTPS HTTP API (arch/16 §1.1): env-секреты TLS → конфиг, Kestrel c`
     `// серверным сертом; клиентский серт опционален, mTLS — только /api.`

  5) Между строкой `app.Logger.LogWarning("KafkaWorker:Api:Tls: {Warning}", certWarning);`
     и строкой `app.MapAppMetrics();` вставь:

  ```csharp
  // Открытая грань (arch/16 §1.1): клиентский серт опционален на хендшейке;
  // 401 без валидного серта — только на /api; /healthz и /metrics — без серта.
  app.UseMiddleware<Shared.Tls.ClientCertAuthMiddleware>();
  ```

- **A3.3.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/src/ValkeyWorker.App/Program.cs`.
  Пять правок:

  1) В шапке строку
     `// Точка входа ValkeyWorker (arch/21 §8): host-builder с mTLS-гранью HTTP API`
     замени на
     `// Точка входа ValkeyWorker (arch/21 §8): host-builder с HTTPS-гранью HTTP API`
     а строку `// (вкл. /healthz), конфигурация appsettings+env, DI всех слоёв (etcd →`
     замени на
     `// (mTLS — только /api; /healthz — открытая грань), конфигурация appsettings+env, DI (etcd →`

  2) Комментарий
     `// Метрики (arch/18 §2.2): /metrics на том же mTLS-Kestrel-порту, что /healthz —`
     `// scrape ходит клиентским сертом per-install пакета. Коллектор доменных метрик`
     замени на
     `// Метрики (arch/18 §2.2): /metrics на том же HTTPS-Kestrel-порту, что /healthz —`
     `// открытая грань скрейпа без клиентского серта (arch/18 §5.2). Коллектор доменных метрик`

  3) Строку
     `"AdvertiseUrl must be https:// (mTLS-only API, arch/21 §1.1)")`
     замени на
     `"AdvertiseUrl must be https:// (HTTPS-only edge, mTLS on /api only; arch/21 §1.1)")`

  4) Двухстрочный комментарий
     `// mTLS HTTP API (arch/21 §1.1): env-секреты TLS → конфиг, Kestrel c`
     `// серверным сертом и требованием клиентского серта per-install API-CA.`
     замени на
     `// HTTPS HTTP API (arch/21 §1.1): env-секреты TLS → конфиг, Kestrel c`
     `// серверным сертом; клиентский серт опционален, mTLS — только /api.`

  5) Между строкой `app.Logger.LogWarning("ValkeyWorker:Api:Tls: {Warning}", certWarning);`
     и строкой `app.MapAppMetrics();` вставь:

  ```csharp
  // Открытая грань (arch/21 §1.1): клиентский серт опционален на хендшейке;
  // 401 без валидного серта — только на /api; /healthz и /metrics — без серта.
  app.UseMiddleware<Shared.Tls.ClientCertAuthMiddleware>();
  ```

- **A3.4.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/src/S3Worker.App/Program.cs`.
  Пять правок:

  1) В шапке строку
     `// Точка входа S3Worker (arch/24 §8): host-builder с mTLS-гранью HTTP API`
     замени на
     `// Точка входа S3Worker (arch/24 §8): host-builder с HTTPS-гранью HTTP API`
     а строку `// (вкл. /healthz), конфигурация appsettings+env, DI слоёв. Per-install`
     замени на
     `// (mTLS — только /api; /healthz — открытая грань), конфигурация appsettings+env, DI. Per-install`

  2) Комментарий
     `// Метрики (arch/18 §2.2): /metrics на том же mTLS-Kestrel-порту, что /healthz —`
     `// scrape ходит клиентским сертом per-install пакета. Meter "S3Worker" —`
     замени на
     `// Метрики (arch/18 §2.2): /metrics на том же HTTPS-Kestrel-порту, что /healthz —`
     `// открытая грань скрейпа без клиентского серта (arch/18 §5.2). Meter "S3Worker" —`

  3) Строку
     `"S3Worker:Api:AdvertiseUrl is not set or not https:// (env S3W_API_ADVERTISE_URL; mTLS-only API — arch/24 §1.1)")`
     замени на
     `"S3Worker:Api:AdvertiseUrl is not set or not https:// (env S3W_API_ADVERTISE_URL; HTTPS-only edge, mTLS on /api only — arch/24 §1.1)")`

  4) Двухстрочный комментарий
     `// mTLS HTTP API (arch/24 §1.1): env-секреты TLS → конфиг, Kestrel c`
     `// серверным сертом и требованием клиентского серта per-install API-CA.`
     замени на
     `// HTTPS HTTP API (arch/24 §1.1): env-секреты TLS → конфиг, Kestrel c`
     `// серверным сертом; клиентский серт опционален, mTLS — только /api.`

  5) Между строкой `app.Logger.LogWarning("S3Worker:Api:Tls: {Warning}", certWarning);`
     и строкой `app.MapAppMetrics();` вставь:

  ```csharp
  // Открытая грань (arch/24 §1.1): клиентский серт опционален на хендшейке;
  // 401 без валидного серта — только на /api; /healthz и /metrics — без серта.
  app.UseMiddleware<Shared.Tls.ClientCertAuthMiddleware>();
  ```

- **A3.5.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/src/KafkaWorker.App/Options.cs`.
  Три правки xml-doc:

  1) Строку
     `/// обязан начинаться с https:// (mTLS-only грань).</summary>`
     замени на
     `/// обязан начинаться с https:// (HTTPS-грань; mTLS — только /api).</summary>`

  2) Строку
     `/// <summary>mTLS-конфигурация HTTP API (arch/16 §1.1).</summary>`
     замени на
     `/// <summary>TLS-конфигурация HTTP API (arch/16 §1.1; mTLS — только /api).</summary>`

  3) Блок

  ```
  /// <summary>
  /// mTLS HTTP API воркера (arch/16 §1.1): серверный серт+ключ и CA клиентских
  /// сертов per-install API-PKI; env KFW_API_TLS_{CERT,KEY,CLIENT_CA}[_PATH]
  /// (таблица TlsEndpoints.EnvBindings). AllowInsecureHttp — ТОЛЬКО WAF-тесты.
  /// </summary>
  ```

  замени на

  ```
  /// <summary>
  /// HTTPS HTTP API воркера (arch/16 §1.1): серверный серт+ключ и CA клиентских
  /// сертов per-install API-PKI (mTLS — только /api); env
  /// KFW_API_TLS_{CERT,KEY,CLIENT_CA}[_PATH] (таблица TlsEndpoints.EnvBindings).
  /// AllowInsecureHttp — ТОЛЬКО WAF-тесты.
  /// </summary>
  ```

- **A3.6.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/src/S3Worker.App/S3StartupChecks.cs`.
  Двухстрочный комментарий

  ```
      // AdvertiseUrl: непустой; https:// (mTLS-only, arch/24 §1.1) либо AllowInsecureHttp
      // (только WAF-тесты).
  ```

  замени на

  ```
      // AdvertiseUrl: непустой; https:// (HTTPS-грань, mTLS — только /api,
      // arch/24 §1.1) либо AllowInsecureHttp (только WAF-тесты).
  ```

### Шаг A4: сборка решения

**Вход:** шаги A1–A3 завершены.

**Действие (кодер):** собирает решение; при ошибках/варнингах
(`TreatWarningsAsErrors=true`) — исправляет сам (не меняя сигнатур из плана);
неисправимое — стоп и разбор (не расширять scope).

**Выход:** `src/PgWorker.slnx` собирается в Release без предупреждений.

**Проверка:** из корня worktree, в фоне, лог в файл:
`DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo > /tmp/t04-build-A4.log 2>&1`
— в логе `Build succeeded`, `0 Warning(s)`, `0 Error(s)`.

**Связь со spec:** фаза 1 (§4).

## Фаза B — WAF-тесты (spec §3.2, фаза 2)

### Шаг B1: четыре `MtlsApiTests` — новая семантика открытой грани

**Вход:** фаза A собрана.

**Действие (кодер):** выполняет подпункты B1.1–B1.4, затем ревьюит каждый
тест-файл: (а) `app.UseMiddleware<Shared.Tls.ClientCertAuthMiddleware>()`
вызван в `StartTlsHost` сразу после `builder.Build()` и ДО всех `Map`-вызовов
— без этого 401-семантика в тест-хосте невозможна (ConfigureMtls регистрирует
только DI-опцию, pipeline строит владелец `app`; в Program.cs воркера
middleware подключается сам — там правок не нужно); (б) маршрут-заглушка
`/metrics` добавлен; (в) AAA-комментарии на месте; (г) вторая CA для
невалидного серта не совпадает с CA сервера; (д) PFX round-trip и TLS 1.2
сохранены у ВСЕХ клиентов; (е) имена/using соответствуют файлу.

**Выход:** четыре тест-файла проверяют: `/api` без серта = 401; `/api` с
невалидным сертом = 401 (хендшейк не рвётся); `/healthz` и `/metrics` без
серта = 200; `/api` с сертом API-CA = 200; `/healthz` с невалидным сертом =
200; `/apifoo` без серта = 404.

**Проверка:** шаг B3.

**Связь со spec:** §3.2.1 (ассерты 1–5), §6 п.4/п.6/п.8/п.12; Review Focus 1–2.

**Подпункты (слабая модель):**

- **B1.1.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/src/tests/PgWorker.IntegrationTests/Api/MtlsApiTests.cs`.
  Четыре правки:

  1) Шапку-комментарий класса (строки над `[Collection(NonE2eCollection.Name)]`,
     начиная `// mTLS HTTP API PgWorker (spec §8.2): клиент без серта — отказ TLS-хендшейка,`)
     замени на:

  ```csharp
  // HTTPS HTTP API PgWorker (arch/14 §1.1): клиентский серт опционален на
  // хендшейке; /api без серта и с невалидным сертом — 401 (HTTP-ответ, не
  // TLS-отказ); /healthz и /metrics — открытая грань (200 без серта, невалидный
  // серт игнорируется). Реальный Kestrel-сокет (WAF-транспорт in-memory TLS не
  // исполняет) — порт динамический (зонд FreePort). https-валидация advertise:
  // http-URL при выключенном AllowInsecureHttp — fail-fast старта хоста
  // (ValidateOnStart, arch/14 §1.1).
  ```

  2) В методе `StartTlsHost` замени блок

  ```csharp
          var app = builder.Build();
          app.MapGet("/api/ping", () => Results.Ok("pong"));
          app.MapHealthChecks("/healthz");
  ```

  на

  ```csharp
          var app = builder.Build();
          // 401-политика /api в pipeline тест-хоста (в Program.cs воркера
          // подключается так же — здесь хост собирает тест).
          app.UseMiddleware<Shared.Tls.ClientCertAuthMiddleware>();
          app.MapGet("/api/ping", () => Results.Ok("pong"));
          app.MapHealthChecks("/healthz");
          app.MapGet("/metrics", () => Results.Ok("")); // заглушка: транспортная семантика, не формат выгрузки
  ```

  3) Полностью замени метод `[Fact] public async Task Mtls_NoClientCert_Refused_WithCert_Ok() …`
  (от строки `[Fact]` перед ним до закрывающей `}` метода включительно) на:

  ```csharp
      [Fact]
      public async Task Api_WithoutCert_401_HealthzMetrics_WithoutCert_200_InvalidCert_401_WithCert_200()
      {
          // Arrange: TLS-хост на свободном порту (зонд FreePort — динамический).
          var port = FreePort();
          var host = StartTlsHost(port);
          using var _ = host.ClientCert; // серт нужен хендшейкам до конца теста
          using var app = host.App;
          // Готовность листенера: Start() вернулся, но под параллельным
          // docker-нагрузом прогона первый хендшейк изредка ловил «refused» —
          // ждём фактического приёма TCP, чтобы не маскировать это под TLS-отказ.
          var readyDeadline = DateTimeOffset.UtcNow.AddSeconds(15);
          while (true)
          {
              try
              {
                  using var probe = new TcpClient();
                  await probe.ConnectAsync(IPAddress.Loopback, port, TestContext.Current.CancellationToken);
                  break; // порт принимает — Kestrel слушает
              }
              catch (SocketException) when (DateTimeOffset.UtcNow < readyDeadline)
              {
                  await Task.Delay(200, TestContext.Current.CancellationToken);
              }
          }
          using var noCertClient = new HttpClient(new SocketsHttpHandler
          {
              SslOptions = new()
              {
                  EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                  RemoteCertificateValidationCallback = (_, _, _, _) => true,
              },
          }) { BaseAddress = new Uri($"https://localhost:{port}") };
          // Невалидный клиентский серт: подписан ДРУГОЙ тестовой CA (не CA сервера).
          var (badCaPem, badCaKeyPem) = TestPki.GenerateCa();
          var (badCertPem, badKeyPem) = TestPki.Issue(badCaPem, badCaKeyPem, "panel");
          // PFX round-trip: эфемерный ключ CreateFromPem не годится для SslStream
          // на macOS (ре-импорт делает ключ экспортируемым).
          var badPemCert = X509Certificate2.CreateFromPem(badCertPem, badKeyPem);
          using var invalidCert = X509CertificateLoader.LoadPkcs12(badPemCert.Export(X509ContentType.Pkcs12), null);
          using var invalidClient = new HttpClient(new SocketsHttpHandler
          {
              SslOptions = new()
              {
                  EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                  ClientCertificates = [invalidCert],
                  RemoteCertificateValidationCallback = (_, _, _, _) => true,
              },
          }) { BaseAddress = new Uri($"https://localhost:{port}") };

          var ct = TestContext.Current.CancellationToken;

          // Act 1 / Assert 1: /api без серта — 401 (HTTP-ответ, не TLS-отказ).
          (await noCertClient.GetAsync("/api/ping", ct)).StatusCode.Should().Be(HttpStatusCode.Unauthorized);

          // Act 2 / Assert 2: /healthz без серта — 200 (открытая грань).
          (await noCertClient.GetAsync("/healthz", ct)).StatusCode.Should().Be(HttpStatusCode.OK);

          // Act 3 / Assert 3: /metrics без серта — 200 (открытая грань, заглушка).
          (await noCertClient.GetAsync("/metrics", ct)).StatusCode.Should().Be(HttpStatusCode.OK);

          // Act 4 / Assert 4: /api с сертом API-CA — 200.
          (await host.Client.GetAsync("/api/ping", ct)).StatusCode.Should().Be(HttpStatusCode.OK);

          // Act 5 / Assert 5: /api с НЕВАЛИДНЫМ сертом — 401, хендшейк НЕ рвётся
          // (сервер принял соединение, middleware отверг путь).
          (await invalidClient.GetAsync("/api/ping", ct)).StatusCode.Should().Be(HttpStatusCode.Unauthorized);

          // Act 6 / Assert 6: /healthz с невалидным сертом — 200 (серт игнорируется
          // открытой гранью).
          (await invalidClient.GetAsync("/healthz", ct)).StatusCode.Should().Be(HttpStatusCode.OK);

          // Act 7 / Assert 7: /apifoo без серта — 404 (не /api: сегмент-префикс
          // точный, прочие пути — открытая грань).
          (await noCertClient.GetAsync("/apifoo", ct)).StatusCode.Should().Be(HttpStatusCode.NotFound);
      }
  ```

  4) Ничего больше в файле не меняй (второй тест
  `AdvertiseUrl_HttpWithoutInsecureFlag_HostStartFails` и класс
  `HttpAdvertiseFactory` остаются как есть).

- **B1.2.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/src/tests/KafkaWorker.IntegrationTests/Api/MtlsApiTests.cs`.
  Четыре правки:

  1) Шапку-комментарий класса (5 строк над `public class MtlsApiTests`, начиная
     `// mTLS HTTP API (spec §8.2): клиент без серта — отказ TLS-хендшейка, с сертом`)
     замени на:

  ```csharp
  // HTTPS HTTP API (arch/16 §1.1): клиентский серт опционален на хендшейке;
  // /api без серта и с невалидным сертом — 401 (HTTP-ответ, не TLS-отказ);
  // /healthz и /metrics — открытая грань (200 без серта, невалидный серт
  // игнорируется). Реальный Kestrel-сокет (WAF-транспорт in-memory TLS не
  // исполняет) — порт динамический (зонд FreePortWindow). Клиентский серт
  // возвращается НАРУЖУ (диспоз — тест).
  ```

  2) В методе `StartTlsHost` замени блок

  ```csharp
          var app = builder.Build();
          app.MapGet("/api/ping", () => Results.Ok("pong"));
          app.MapHealthChecks("/healthz");
  ```

  на

  ```csharp
          var app = builder.Build();
          // 401-политика /api в pipeline тест-хоста (в Program.cs воркера
          // подключается так же — здесь хост собирает тест).
          app.UseMiddleware<Shared.Tls.ClientCertAuthMiddleware>();
          app.MapGet("/api/ping", () => Results.Ok("pong"));
          app.MapHealthChecks("/healthz");
          app.MapGet("/metrics", () => Results.Ok("")); // заглушка: транспортная семантика, не формат выгрузки
  ```

  3) Полностью замени метод `[Fact] public async Task Mtls_NoClientCert_Refused_WithCert_Ok() …`
  (от `[Fact]` до закрывающей `}` метода) на:

  ```csharp
      [Fact]
      public async Task Api_WithoutCert_401_HealthzMetrics_WithoutCert_200_InvalidCert_401_WithCert_200()
      {
          // Arrange: TLS-хост на свободном порту (зонд FreePortWindow — динамический).
          var port = FreePortWindow.Find().From;
          var host = StartTlsHost(port);
          using var _ = host.ClientCert; // серт нужен хендшейкам до конца теста
          using var app = host.App;
          // Готовность листенера: Start() вернулся, но под параллельным
          // docker-нагрузом прогона первый хендшейк изредка ловил «refused» —
          // ждём фактического приёма TCP, чтобы не маскировать это под TLS-отказ.
          var readyDeadline = DateTimeOffset.UtcNow.AddSeconds(15);
          while (true)
          {
              try
              {
                  using var probe = new TcpClient();
                  await probe.ConnectAsync(IPAddress.Loopback, port, TestContext.Current.CancellationToken);
                  break; // порт принимает — Kestrel слушает
              }
              catch (SocketException) when (DateTimeOffset.UtcNow < readyDeadline)
              {
                  await Task.Delay(200, TestContext.Current.CancellationToken);
              }
          }
          using var noCertClient = new HttpClient(new SocketsHttpHandler
          {
              SslOptions = new()
              {
                  EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                  RemoteCertificateValidationCallback = (_, _, _, _) => true,
              },
          }) { BaseAddress = new Uri($"https://localhost:{port}") };
          // Невалидный клиентский серт: подписан ДРУГОЙ тестовой CA (не CA сервера).
          var badCa = ClusterPki.GenerateCa("api-test-invalid");
          var (badCertPem, badKeyPem) = ClusterPki.IssueBrokerCertificate(
              badCa.CaPem, badCa.CaKeyPem, "panel", ["panel"], ip: null);
          // PFX round-trip: эфемерный ключ CreateFromPem не годится для SslStream
          // на macOS (ре-импорт делает ключ экспортируемым).
          var badPemCert = X509Certificate2.CreateFromPem(badCertPem, badKeyPem);
          using var invalidCert = X509CertificateLoader.LoadPkcs12(badPemCert.Export(X509ContentType.Pkcs12), null);
          using var invalidClient = new HttpClient(new SocketsHttpHandler
          {
              SslOptions = new()
              {
                  EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                  ClientCertificates = [invalidCert],
                  RemoteCertificateValidationCallback = (_, _, _, _) => true,
              },
          }) { BaseAddress = new Uri($"https://localhost:{port}") };

          var ct = TestContext.Current.CancellationToken;

          // Act 1 / Assert 1: /api без серта — 401 (HTTP-ответ, не TLS-отказ).
          (await noCertClient.GetAsync("/api/ping", ct)).StatusCode.Should().Be(HttpStatusCode.Unauthorized);

          // Act 2 / Assert 2: /healthz без серта — 200 (открытая грань).
          (await noCertClient.GetAsync("/healthz", ct)).StatusCode.Should().Be(HttpStatusCode.OK);

          // Act 3 / Assert 3: /metrics без серта — 200 (открытая грань, заглушка).
          (await noCertClient.GetAsync("/metrics", ct)).StatusCode.Should().Be(HttpStatusCode.OK);

          // Act 4 / Assert 4: /api с сертом API-CA — 200.
          (await host.Client.GetAsync("/api/ping", ct)).StatusCode.Should().Be(HttpStatusCode.OK);

          // Act 5 / Assert 5: /api с НЕВАЛИДНЫМ сертом — 401, хендшейк НЕ рвётся
          // (сервер принял соединение, middleware отверг путь).
          (await invalidClient.GetAsync("/api/ping", ct)).StatusCode.Should().Be(HttpStatusCode.Unauthorized);

          // Act 6 / Assert 6: /healthz с невалидным сертом — 200 (серт игнорируется
          // открытой гранью).
          (await invalidClient.GetAsync("/healthz", ct)).StatusCode.Should().Be(HttpStatusCode.OK);

          // Act 7 / Assert 7: /apifoo без серта — 404 (не /api: сегмент-префикс
          // точный, прочие пути — открытая грань).
          (await noCertClient.GetAsync("/apifoo", ct)).StatusCode.Should().Be(HttpStatusCode.NotFound);
      }
  ```

  4) Больше в файле ничего не меняй.

- **B1.3.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/src/tests/ValkeyWorker.IntegrationTests/Api/MtlsApiTests.cs`.
  Четыре правки:

  1) Шапку-комментарий класса (5 строк над `public class MtlsApiTests`, начиная
     `// mTLS HTTP API (arch/21 §1.1; порт MtlsApiTests kfw): клиент без серта —`)
     замени на:

  ```csharp
  // HTTPS HTTP API (arch/21 §1.1): клиентский серт опционален на хендшейке;
  // /api без серта и с невалидным сертом — 401 (HTTP-ответ, не TLS-отказ);
  // /healthz и /metrics — открытая грань (200 без серта, невалидный серт
  // игнорируется). Реальный Kestrel-сокет (WAF-транспорт in-memory TLS не
  // исполняет) — порт динамический (зонд). Клиентский серт живёт в SslOptions.
  ```

  2) В методе `StartTlsHost` замени блок

  ```csharp
          var app = builder.Build();
          app.MapGet("/api/ping", () => Results.Ok("pong"));
          app.MapHealthChecks("/healthz");
  ```

  на

  ```csharp
          var app = builder.Build();
          // 401-политика /api в pipeline тест-хоста (в Program.cs воркера
          // подключается так же — здесь хост собирает тест).
          app.UseMiddleware<Shared.Tls.ClientCertAuthMiddleware>();
          app.MapGet("/api/ping", () => Results.Ok("pong"));
          app.MapHealthChecks("/healthz");
          app.MapGet("/metrics", () => Results.Ok("")); // заглушка: транспортная семантика, не формат выгрузки
  ```

  3) Полностью замени метод `[Fact] public async Task Mtls_NoClientCertRejected_WithCertOk() …`
  (от `[Fact]` до закрывающей `}` метода) на метод ниже (имя и тело; отличается
  от Kafka-версии только генерацией невалидного серта через TestPki):

  ```csharp
      [Fact]
      public async Task Api_WithoutCert_401_HealthzMetrics_WithoutCert_200_InvalidCert_401_WithCert_200()
      {
          // Arrange: TLS-хост на свободном порту (динамический).
          var port = FreePort();
          var host = StartTlsHost(port);
          using var _ = host.ClientCert; // серт нужен хендшейкам до конца теста
          using var app = host.App;
          // Готовность листенера: ждём фактического приёма TCP (параллельный
          // docker-нагрузк прогона может дать «refused» на первом хендшейке).
          var readyDeadline = DateTimeOffset.UtcNow.AddSeconds(15);
          while (true)
          {
              try
              {
                  using var probe = new TcpClient();
                  await probe.ConnectAsync(IPAddress.Loopback, port, TestContext.Current.CancellationToken);
                  break;
              }
              catch (SocketException) when (DateTimeOffset.UtcNow < readyDeadline)
              {
                  await Task.Delay(200, TestContext.Current.CancellationToken);
              }
          }
          using var noCertClient = new HttpClient(new SocketsHttpHandler
          {
              SslOptions = new()
              {
                  EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                  RemoteCertificateValidationCallback = (_, _, _, _) => true,
              },
          }) { BaseAddress = new Uri($"https://localhost:{port}") };
          // Невалидный клиентский серт: подписан ДРУГОЙ тестовой CA (не CA сервера).
          var badCa = TestPki.GenerateCa("api-test-invalid");
          var (badCertPem, badKeyPem) = TestPki.IssueCertificate(badCa.CaPem, badCa.CaKeyPem, "panel");
          // PFX round-trip: эфемерный ключ CreateFromPem не годится для SslStream
          // на macOS (ре-импорт делает ключ экспортируемым).
          var badPemCert = X509Certificate2.CreateFromPem(badCertPem, badKeyPem);
          using var invalidCert = X509CertificateLoader.LoadPkcs12(badPemCert.Export(X509ContentType.Pkcs12), null);
          using var invalidClient = new HttpClient(new SocketsHttpHandler
          {
              SslOptions = new()
              {
                  EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                  ClientCertificates = [invalidCert],
                  RemoteCertificateValidationCallback = (_, _, _, _) => true,
              },
          }) { BaseAddress = new Uri($"https://localhost:{port}") };

          var ct = TestContext.Current.CancellationToken;

          // Act 1 / Assert 1: /api без серта — 401 (HTTP-ответ, не TLS-отказ).
          (await noCertClient.GetAsync("/api/ping", ct)).StatusCode.Should().Be(HttpStatusCode.Unauthorized);

          // Act 2 / Assert 2: /healthz без серта — 200 (открытая грань).
          (await noCertClient.GetAsync("/healthz", ct)).StatusCode.Should().Be(HttpStatusCode.OK);

          // Act 3 / Assert 3: /metrics без серта — 200 (открытая грань, заглушка).
          (await noCertClient.GetAsync("/metrics", ct)).StatusCode.Should().Be(HttpStatusCode.OK);

          // Act 4 / Assert 4: /api с сертом API-CA — 200.
          (await host.Client.GetAsync("/api/ping", ct)).StatusCode.Should().Be(HttpStatusCode.OK);

          // Act 5 / Assert 5: /api с НЕВАЛИДНЫМ сертом — 401, хендшейк НЕ рвётся
          // (сервер принял соединение, middleware отверг путь).
          (await invalidClient.GetAsync("/api/ping", ct)).StatusCode.Should().Be(HttpStatusCode.Unauthorized);

          // Act 6 / Assert 6: /healthz с невалидным сертом — 200 (серт игнорируется
          // открытой гранью).
          (await invalidClient.GetAsync("/healthz", ct)).StatusCode.Should().Be(HttpStatusCode.OK);

          // Act 7 / Assert 7: /apifoo без серта — 404 (не /api: сегмент-префикс
          // точный, прочие пути — открытая грань).
          (await noCertClient.GetAsync("/apifoo", ct)).StatusCode.Should().Be(HttpStatusCode.NotFound);
      }
  ```

  4) Больше в файле ничего не меняй.

- **B1.4.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/src/tests/S3Worker.IntegrationTests/Api/MtlsApiTests.cs`.
  Четыре правки:

  1) Шапку-комментарий класса (5 строк над `public class MtlsApiTests`, начиная
     `// mTLS HTTP API (arch/24 §1.1; порт MtlsApiTests valkey): клиент без серта —`)
     замени на:

  ```csharp
  // HTTPS HTTP API (arch/24 §1.1): клиентский серт опционален на хендшейке;
  // /api без серта и с невалидным сертом — 401 (HTTP-ответ, не TLS-отказ);
  // /healthz и /metrics — открытая грань (200 без серта, невалидный серт
  // игнорируется). Реальный Kestrel-сокет (WAF-транспорт in-memory TLS не
  // исполняет) — порт динамический (зонд). Клиентский серт живёт в SslOptions.
  ```

  2) В методе `StartTlsHost` замени блок

  ```csharp
          var app = builder.Build();
          app.MapGet("/api/ping", () => Results.Ok("pong"));
          app.MapHealthChecks("/healthz");
  ```

  на

  ```csharp
          var app = builder.Build();
          // 401-политика /api в pipeline тест-хоста (в Program.cs воркера
          // подключается так же — здесь хост собирает тест).
          app.UseMiddleware<Shared.Tls.ClientCertAuthMiddleware>();
          app.MapGet("/api/ping", () => Results.Ok("pong"));
          app.MapHealthChecks("/healthz");
          app.MapGet("/metrics", () => Results.Ok("")); // заглушка: транспортная семантика, не формат выгрузки
  ```

  3) Полностью замени метод `[Fact] public async Task Mtls_NoClientCertRefused_WithCertOk() …`
  (от `[Fact]` до закрывающей `}` метода) на:

  ```csharp
      [Fact]
      public async Task Api_WithoutCert_401_HealthzMetrics_WithoutCert_200_InvalidCert_401_WithCert_200()
      {
          // Arrange: TLS-хост на свободном порту (динамический).
          var port = FreePort();
          var host = StartTlsHost(port);
          using var _ = host.ClientCert; // серт нужен хендшейкам до конца теста
          using var app = host.App;
          // Готовность листенера: ждём фактического приёма TCP.
          var readyDeadline = DateTimeOffset.UtcNow.AddSeconds(15);
          while (true)
          {
              try
              {
                  using var probe = new TcpClient();
                  await probe.ConnectAsync(IPAddress.Loopback, port, TestContext.Current.CancellationToken);
                  break;
              }
              catch (SocketException) when (DateTimeOffset.UtcNow < readyDeadline)
              {
                  await Task.Delay(200, TestContext.Current.CancellationToken);
              }
          }
          using var noCertClient = new HttpClient(new SocketsHttpHandler
          {
              SslOptions = new()
              {
                  EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                  RemoteCertificateValidationCallback = (_, _, _, _) => true,
              },
          }) { BaseAddress = new Uri($"https://localhost:{port}") };
          // Невалидный клиентский серт: подписан ДРУГОЙ тестовой CA (не CA сервера).
          var badCa = TestPki.GenerateCa("api-test-invalid");
          var (badCertPem, badKeyPem) = TestPki.IssueCertificate(badCa.CaPem, badCa.CaKeyPem, "panel");
          // PFX round-trip: эфемерный ключ CreateFromPem не годится для SslStream
          // на macOS (ре-импорт делает ключ экспортируемым).
          var badPemCert = X509Certificate2.CreateFromPem(badCertPem, badKeyPem);
          using var invalidCert = X509CertificateLoader.LoadPkcs12(badPemCert.Export(X509ContentType.Pkcs12), null);
          using var invalidClient = new HttpClient(new SocketsHttpHandler
          {
              SslOptions = new()
              {
                  EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12,
                  ClientCertificates = [invalidCert],
                  RemoteCertificateValidationCallback = (_, _, _, _) => true,
              },
          }) { BaseAddress = new Uri($"https://localhost:{port}") };

          var ct = TestContext.Current.CancellationToken;

          // Act 1 / Assert 1: /api без серта — 401 (HTTP-ответ, не TLS-отказ).
          (await noCertClient.GetAsync("/api/ping", ct)).StatusCode.Should().Be(HttpStatusCode.Unauthorized);

          // Act 2 / Assert 2: /healthz без серта — 200 (открытая грань).
          (await noCertClient.GetAsync("/healthz", ct)).StatusCode.Should().Be(HttpStatusCode.OK);

          // Act 3 / Assert 3: /metrics без серта — 200 (открытая грань, заглушка).
          (await noCertClient.GetAsync("/metrics", ct)).StatusCode.Should().Be(HttpStatusCode.OK);

          // Act 4 / Assert 4: /api с сертом API-CA — 200.
          (await host.Client.GetAsync("/api/ping", ct)).StatusCode.Should().Be(HttpStatusCode.OK);

          // Act 5 / Assert 5: /api с НЕВАЛИДНЫМ сертом — 401, хендшейк НЕ рвётся
          // (сервер принял соединение, middleware отверг путь).
          (await invalidClient.GetAsync("/api/ping", ct)).StatusCode.Should().Be(HttpStatusCode.Unauthorized);

          // Act 6 / Assert 6: /healthz с невалидным сертом — 200 (серт игнорируется
          // открытой гранью).
          (await invalidClient.GetAsync("/healthz", ct)).StatusCode.Should().Be(HttpStatusCode.OK);

          // Act 7 / Assert 7: /apifoo без серта — 404 (не /api: сегмент-префикс
          // точный, прочие пути — открытая грань).
          (await noCertClient.GetAsync("/apifoo", ct)).StatusCode.Should().Be(HttpStatusCode.NotFound);
      }
  ```

  4) Больше в файле ничего не меняй.

### Шаг B2: `MetricsTests` — только комментарии

**Вход:** шаг B1 завершён.

**Действие (кодер):** выполняет подпункт B2.1. Файл `MetricsTests.cs`
существует только в проектах PgWorker и KafkaWorker — spec §3.2.2 «4 проекта,
где есть»: в Valkey/S3 файлов с таким именем нет, там правок ноль. Ревьюит:
код и ассерты не тронуты.

**Выход:** комментарии MetricsTests соответствуют канону открытой грани.

**Проверка:** пересборка (комментарии не ломают сборку) — команда шага A4 зелёная.

**Связь со spec:** §3.2.2.

**Подпункты (слабая модель):**

- **B2.1.** Две правки в двух файлах:

  1) Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/src/tests/PgWorker.IntegrationTests/Api/MetricsTests.cs`.
     Трёхстрочный комментарий над классом

  ```
  // Интеграционные тесты /metrics PgWorker (arch/18 §3): scrape-грань на том же
  // mTLS-Kestrel-порту, что /healthz; защита API транспортная (mTLS — MtlsApiTests),
  // здесь проверяем экспозицию Prometheus-формата и живые серии циклов.
  ```

     замени на

  ```
  // Интеграционные тесты /metrics PgWorker (arch/18 §3): scrape-грань на том же
  // HTTPS-Kestrel-порту, что /healthz; защита API — транспортная (mTLS: 401 без
  // валидного серта на /api покрывает MtlsApiTests); /metrics — открытая грань
  // (arch/18 §3). Здесь проверяем экспозицию Prometheus-формата и живые серии.
  ```

  2) Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/src/tests/KafkaWorker.IntegrationTests/Api/MetricsTests.cs`.
     Трёхстрочный комментарий над классом

  ```
  // Интеграционные тесты /metrics KafkaWorker (arch/18 §3): scrape-грань открыта
  // без ApiKey/cookie (symметрия /healthz); защита API — транспортная (mTLS,
  // arch/16 §1.1) — отказ без клиентского серта покрывает MtlsApiTests.
  ```

     замени на

  ```
  // Интеграционные тесты /metrics KafkaWorker (arch/18 §3): scrape-грань открыта
  // без ApiKey/cookie (симметрия /healthz); защита API — транспортная (mTLS: 401
  // без валидного серта на /api покрывает MtlsApiTests); /metrics — открытая
  // грань (arch/18 §3).
  ```

### Шаг B3: интеграционные прогоны четырёх проектов + зачистка серий

**Вход:** шаги B1–B2 завершены, сборка зелёная.

**Действие (кодер):** для каждого из четырёх проектов по очереди (проект за
проектом; между проектами — дождаться финальной строки прогона и зачистить
docker-контейнеры/сети серии по AGENTS.md: `docker ps` пуст от контейнеров
прогона; осиротевшие `kfw-net-*` при нуле контейнеров — `docker network prune -f`):
пересборка (команда шага A4), затем тесты; лог целиком в файл, чтение из
файла, разбор падений. Упавший вне области задачи тест — НЕ чинить:
фиксировать в `arch/roadmap/bugs.md` (все сведения для повтора: тест/фильтр,
команда, коммит/ветка, текст ассерта, пути к логам) и продолжать (spec §5).
Запуск в фоне; контроль старта в течение 15 с и отсутствия зависаний каждые
2 мин (базовые правила §13).

**Выход:** обновлённые `MtlsApiTests` зелёные в 4 проектах; остальные тесты
затронутых сборок — без регрессий (либо зафиксированы в bugs.md как вне области).

**Проверка** (из корня worktree; каждую — отдельным фоновым процессом; формат
для всех четырёх, подставляя имя проекта `PgWorker`/`KafkaWorker`/`ValkeyWorker`/`S3Worker`):

```
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/<Проект>.IntegrationTests -c Release --no-build --filter FullyQualifiedName~MtlsApiTests > /tmp/t04-test-<проект>-mtls.log 2>&1
DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/<Проект>.IntegrationTests -c Release --no-build > /tmp/t04-test-<проект>-all.log 2>&1
```

Критерий: `Passed!` в финальной строке каждого лога (или зафиксированные в
bugs.md падения вне области — тогда перечислить их в конечном ответе).

**Связь со spec:** фаза 2 (§4), критерий приёмки 1, §5 (правило «вне задачи»).

## Фаза C — deploy/стенд (spec §3.3, фаза 3)

### Шаг C1: `deploy/tls/gen.sh` — упразднение prometheus/healthcheck + самозачистка

**Вход:** фаза B завершена.

**Действие (кодер):** выполняет подпункт C1.1, ревьюит: скрипт остаётся
идемпотентным; `rm -f` не падает на несуществующих файлах; финальный echo не
упоминает удалённые пакеты.

**Выход:** gen.sh не выпускает и удаляет `prometheus.*`/`healthcheck.*`.

**Проверка:** `bash -n deploy/tls/gen.sh` — без ошибок; визуальная проверка.

**Связь со spec:** §3.3.1, §6 п.9.

**Подпункты (слабая модель):**

- **C1.1.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/deploy/tls/gen.sh`.
  Четыре правки:

  1) Строку
     `# panel, seed, prometheus, healthcheck. Идемпотентен: при существующем ca.pem не`
     замени на
     `# panel, seed. Идемпотентен: при существующем ca.pem не`.

  2) Две строки выпуска удали полностью:

  ```bash
  [ -f prometheus.crt ] || issue prometheus prometheus clientAuth ""
  [ -f healthcheck.crt ] || issue healthcheck healthcheck clientAuth ""
  ```

  3) Сразу после строки `[ -f seed.crt ]       || issue seed       seed       clientAuth ""`
     добавь:

  ```bash
  # Клиентские серты prometheus/healthcheck упразднены каноном открытой грани
  # /healthz и /metrics (arch/14 §1.1 / arch/18 §5.2): скрейп и HEALTHCHECK
  # ходят без клиентского серта. Удаляются и наследие живых стендов — gen.sh
  # идемпотентен и вызывается каждым 00-up.sh.
  rm -f prometheus.crt prometheus.key healthcheck.crt healthcheck.key
  ```

  4) Финальную строку

  ```bash
  echo "✓ kfw-install-ca TLS package: ca.pem, server.* (kafkaworker+valkeyworker+s3worker), pgserver.*, hc.*, panel.*, seed.*, prometheus.*, healthcheck.*, owns3.*"
  ```

  замени на

  ```bash
  echo "✓ kfw-install-ca TLS package: ca.pem, server.* (kafkaworker+valkeyworker+s3worker), pgserver.*, hc.*, panel.*, seed.*, owns3.*"
  ```

### Шаг C2: `dev-stand/adminpanel/metrics/prometheus/prometheus.yml` — скрейп без клиентского серта

**Вход:** шаг C1 завершён.

**Действие (кодер):** выполняет подпункт C2.1, ревьюит: в джобах `pgworker`,
`kafkaworker`, `valkeyworker` остались `scheme: https` + `tls_config.ca_file`
без `cert_file`/`key_file`; остальные джобы не тронуты.

**Выход:** конфиг Prometheus соответствует канону arch/18 §5.2.

**Проверка:** визуальная проверка YAML (фактическая — в шаге D2 чеком 65: все
scrape-джобы up без `cert_file`).

**Связь со spec:** §3.3.2.

**Подпункты (слабая модель):**

- **C2.1.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/dev-stand/adminpanel/metrics/prometheus/prometheus.yml`.
  Три правки:

  1) Джоба `pgworker`: комментарий-строку `# deploy-compose (mTLS, t03)` замени на
     `# deploy-compose (открытая https-грань без клиентского серта, arch/18 §5.2)`;
     строки

  ```yaml
      tls_config:
        ca_file: /tls/ca.pem
        cert_file: /tls/prometheus.crt
        key_file: /tls/prometheus.key
  ```

     замени на

  ```yaml
      tls_config:
        ca_file: /tls/ca.pem
  ```

  2) Джоба `kafkaworker`: комментарий-строку
     `# сеть стенда (профиль kafka; mTLS — чинит t03-gap)` замени на
     `# сеть стенда (профиль kafka; открытая https-грань без клиентского серта, arch/18 §5.2)`;
     строки

  ```yaml
      tls_config:
        ca_file: /tls/ca.pem
        cert_file: /tls/prometheus.crt
        key_file: /tls/prometheus.key
  ```

     замени на

  ```yaml
      tls_config:
        ca_file: /tls/ca.pem
  ```

  3) Джоба `valkeyworker`: комментарий-строку
     `# сеть стенда (профиль valkey; mTLS — общий tls_config, t05)` замени на
     `# сеть стенда (профиль valkey; открытая https-грань без клиентского серта, arch/18 §5.2)`;
     строки

  ```yaml
      tls_config:
        ca_file: /tls/ca.pem
        cert_file: /tls/prometheus.crt
        key_file: /tls/prometheus.key
  ```

     замени на

  ```yaml
      tls_config:
        ca_file: /tls/ca.pem
  ```

### Шаг C3: Dockerfile×4 — HEALTHCHECK без клиентского серта

**Вход:** шаг C2 завершён.

**Действие (кодер):** выполняет подпункты C3.1–C3.4, ревьюит: параметры
HEALTHCHECK (`--interval=30s --timeout=3s --start-period=10s --retries=3`) и
путь `/healthz` сохранены; убраны только `--cert/--key`.

**Выход:** HEALTHCHECK четырёх образов не использует клиентскую пару.

**Проверка:** визуальная (фактическая — D2: `docker inspect` → healthy).

**Связь со spec:** §3.3.3.

**Подпункты (слабая модель):**

- **C3.1.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/docker/PgWorker.Dockerfile`.
  Замени блок

  ```dockerfile
  # /healthz за mTLS (t03, arch/14 §1.1): клиентская пара healthcheck из
  # per-install TLS-пакета (deploy/tls/gen.sh; volume /tls:ro в compose).
  HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
      CMD curl -sf --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key \
      https://localhost:8080/healthz || exit 1
  ```

  на

  ```dockerfile
  # /healthz — открытая https-грань без клиентского серта (arch/14 §1.1):
  # серверный TLS + доверие ca.pem (volume /tls:ro в compose).
  HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
      CMD curl -sf --cacert /tls/ca.pem https://localhost:8080/healthz || exit 1
  ```

- **C3.2.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/docker/KafkaWorker.Dockerfile`.
  Замени блок

  ```dockerfile
  # /healthz за mTLS (t03, arch/16 §1.1): клиентская пара healthcheck из
  # per-install TLS-пакета (deploy/tls/gen.sh; volume /tls:ro в compose).
  HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
      CMD curl -sf --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key \
      https://localhost:8080/healthz || exit 1
  ```

  на

  ```dockerfile
  # /healthz — открытая https-грань без клиентского серта (arch/16 §1.1):
  # серверный TLS + доверие ca.pem (volume /tls:ro в compose).
  HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
      CMD curl -sf --cacert /tls/ca.pem https://localhost:8080/healthz || exit 1
  ```

- **C3.3.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/docker/ValkeyWorker.Dockerfile`.
  Замени блок

  ```dockerfile
  # /healthz за mTLS (arch/21 §1.1): клиентская пара healthcheck из
  # per-install TLS-пакета (deploy/tls/gen.sh; volume /tls:ro в compose).
  HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
      CMD curl -sf --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key \
      https://localhost:8080/healthz || exit 1
  ```

  на

  ```dockerfile
  # /healthz — открытая https-грань без клиентского серта (arch/21 §1.1):
  # серверный TLS + доверие ca.pem (volume /tls:ro в compose).
  HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
      CMD curl -sf --cacert /tls/ca.pem https://localhost:8080/healthz || exit 1
  ```

- **C3.4.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/docker/S3Worker.Dockerfile`.
  Замени блок

  ```dockerfile
  # /healthz за mTLS (arch/24 §1.1): клиентская пара healthcheck из TLS-пакета.
  HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
      CMD curl -sf --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key \
      https://localhost:8080/healthz || exit 1
  ```

  на

  ```dockerfile
  # /healthz — открытая https-грань без клиентского серта (arch/24 §1.1):
  # серверный TLS + доверие ca.pem (volume /tls:ro в compose).
  HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
      CMD curl -sf --cacert /tls/ca.pem https://localhost:8080/healthz || exit 1
  ```

### Шаг C4: `dev-stand/adminpanel/checks/00-up.sh` — безсертные пробы и чистка volume

**Вход:** шаг C3 завершён.

**Действие (кодер):** выполняет подпункт C4.1, ревьюит: (а) в cp-списке
volume нет `healthcheck.*`; (б) добавлен `rm -f /tls/healthcheck.crt
/tls/healthcheck.key` в том же cp-контейнере — это ЗАКРЫТИЕ критерия приёмки 4
spec §7 («в volume `deploy_pgw-api-tls` нет `healthcheck.*`»): volume на живом
стенде уже содержит старые файлы, cp их больше не пишет — чистит ближайший
00-up.sh (тот же принцип самозачистки, что и `rm -f` в gen.sh, решение spec
§6 п.9); (в) переменная `MTLS` переименована в `HEALTH` во ВСЕХ применениях.

**Выход:** 00-up.sh наполняет volume без healthcheck-пары и чистит её наследие;
все healthz-пробы безсертные.

**Проверка:** `bash -n dev-stand/adminpanel/checks/00-up.sh` — без ошибок;
`grep -n "MTLS\|healthcheck" dev-stand/adminpanel/checks/00-up.sh` — пусто.

**Связь со spec:** §3.3.4, §6 п.9, критерий приёмки 4.

**Подпункты (слабая модель):**

- **C4.1.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/dev-stand/adminpanel/checks/00-up.sh`.
  Правки:

  1) Строку

  ```bash
    sh -c "cp /src/ca.pem /src/ca.key /src/pgserver.crt /src/pgserver.key /src/server.crt /src/server.key /src/healthcheck.crt /src/healthcheck.key /tls/"
  ```

     замени на

  ```bash
    sh -c "cp /src/ca.pem /src/ca.key /src/pgserver.crt /src/pgserver.key /src/server.crt /src/server.key /tls/ && rm -f /tls/healthcheck.crt /tls/healthcheck.key"
  ```

     и сразу ПОСЛЕ строки комментария `# ca.key — выпуск REST-сертов нод (R16: бэкап пакета — runbook)`
     (и ПЕРЕД `docker run`) добавь строку комментария:

  ```bash
  # healthcheck.* упразднены (открытая грань /healthz, arch/14 §1.1): из volume
  # удаляются — наследие живых стендов чистится этим же прогоном (gen.sh чистит deploy/tls).
  ```

  2) Строку

  ```bash
  MTLS=(curl -fsS -m 3 --cacert "$ROOT/deploy/tls/ca.pem" --cert "$ROOT/deploy/tls/healthcheck.crt" --key "$ROOT/deploy/tls/healthcheck.key")
  ```

     замени на

  ```bash
  # Проба открытой грани: https без клиентского серта (healthz воркеров).
  HEALTH=(curl -fsS -m 3 --cacert "$ROOT/deploy/tls/ca.pem")
  ```

  3) Во ВСЁМ файле замени каждое вхождение `"${MTLS[@]}"` на `"${HEALTH[@]}"`
     (8 вхождений: 4 у pgworker-проб, 4 у s3worker-проб).

  4) В сообщениях об ошибках замени формулировки (4 строки):
     - `(https :${PGW_API_HOST_PORT:-8080}/healthz via mTLS; docker logs deploy-pgworker-1)` →
       `(https :${PGW_API_HOST_PORT:-8080}/healthz without a client certificate; docker logs deploy-pgworker-1)`;
     - `(https :${PGW_API_HOST_PORT2:-8083}/healthz via mTLS; docker logs deploy-pgworker-2-1)` →
       `(https :${PGW_API_HOST_PORT2:-8083}/healthz without a client certificate; docker logs deploy-pgworker-2-1)`;
     - `(https :${S3W_API_HOST_PORT:-8086}/healthz via mTLS; docker logs deploy-s3worker-1)` →
       `(https :${S3W_API_HOST_PORT:-8086}/healthz without a client certificate; docker logs deploy-s3worker-1)`;
     - `(https :${S3W_API_HOST_PORT2:-8089}/healthz via mTLS; docker logs deploy-s3worker-2-1)` →
       `(https :${S3W_API_HOST_PORT2:-8089}/healthz without a client certificate; docker logs deploy-s3worker-2-1)`.

  5) Echo-строку

  ```bash
  echo "  pgworker alive ×2 (:${PGW_API_HOST_PORT:-8080} + :${PGW_API_HOST_PORT2:-8083}/healthz mTLS; /pgworker/api/ keys: ${pgw_api_keys})"
  ```

     замени на

  ```bash
  echo "  pgworker alive ×2 (:${PGW_API_HOST_PORT:-8080} + :${PGW_API_HOST_PORT2:-8083}/healthz https; /pgworker/api/ keys: ${pgw_api_keys})"
  ```

  6) Секцию 7c — комментарий

  ```bash
  # 7c) s3worker жив ×2 (arch/24): healthz по mTLS healthcheck-парой
  #     (общий TLS-пакет pgw-api-tls, server-серт с SAN s3worker*) +
  #     дискавери /s3worker/api/ по ключам (по образцу pgworker/kafkaworker).
  ```

     замени на

  ```bash
  # 7c) s3worker жив ×2 (arch/24): healthz по https без клиентского серта
  #     (открытая грань; общий TLS-пакет pgw-api-tls, server-серт с SAN s3worker*) +
  #     дискавери /s3worker/api/ по ключам (по образцу pgworker/kafkaworker).
  ```

### Шаг C5: чеки 05/20/35/43/57/60/65/75 — безсертные пробы, `/api` на seed-паре

**Вход:** шаг C4 завершён.

**Действие (кодер):** выполняет подпункты C5.1–C5.8, затем по каждому файлу —
`bash -n` и grep-проверка отсутствия `healthcheck.`; ревьюит разделение
проб: `/healthz` и `/metrics` — БЕЗ серта; мутации `/api` (05-seed
`/api/seed/demo`, 75-s3worker `POST/DELETE /api/s3/clusters*` и
`/api/…/sync-probe`) — с парой `seed.crt/seed.key` (конвенция «чек =
стендовый сид», spec §6 п.7: у kafka/valkey bind-маунт всего `deploy/tls` в
`/tls` — `/tls/seed.*` доступен внутри контейнера; у 75-s3worker пробы идут
с хоста из `$ROOT/deploy/tls`).

**Выход:** восемь чеков не используют `healthcheck.*`; `/api`-вызовы — на
seed-паре.

**Проверка:** `grep -rn "healthcheck\." dev-stand/adminpanel/checks/` — пусто;
`bash -n` каждого из восьми файлов — без ошибок.

**Связь со spec:** §3.3.5, §6 п.7.

**Подпункты (слабая модель):**

- **C5.1.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/dev-stand/adminpanel/checks/05-seed.sh`.
  Правки в функции `seed_kafka`:

  1) Замени комментарий и переменную

  ```bash
    # mTLS (t03): /healthz и сид — только с клиентской парой healthcheck + ca.pem
    MTLS="curl -fsS -m 3 --cacert $ROOT/deploy/tls/ca.pem --cert $ROOT/deploy/tls/healthcheck.crt --key $ROOT/deploy/tls/healthcheck.key"
  ```

     на

  ```bash
    # Открытая грань (arch/16 §1.1): /healthz — https без клиентского серта;
    # мутации /api — клиентская пара сида seed (конвенция «чек = стендовый сид»).
    HEALTH="curl -fsS -m 3 --cacert $ROOT/deploy/tls/ca.pem"
    API="curl -fsS -m 3 --cacert $ROOT/deploy/tls/ca.pem --cert $ROOT/deploy/tls/seed.crt --key $ROOT/deploy/tls/seed.key"
  ```

  2) Две строки healthz-поллинга: `$MTLS https://localhost:8082/healthz` замени
     на `$HEALTH https://localhost:8082/healthz`; в сообщении об ошибке
     `(:8082/healthz via mTLS)` → `(:8082/healthz via https)`.

  3) Строку
     `echo "  kafka seed: $($MTLS -X POST https://localhost:8082/api/seed/demo)"`
     замени на
     `echo "  kafka seed: $($API -X POST https://localhost:8082/api/seed/demo)"`.

  Правки в функции `seed_valkey`:

  4) Замени определение функции-хелпера

  ```bash
    vwk_curl() {
      docker compose exec -T valkeyworker curl -fsS -m 3 \
        --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key "$@"
    }
  ```

     на

  ```bash
    vwk_health() {
      docker compose exec -T valkeyworker curl -fsS -m 3 --cacert /tls/ca.pem "$@"
    }
    vwk_api() {
      docker compose exec -T valkeyworker curl -fsS -m 3 \
        --cacert /tls/ca.pem --cert /tls/seed.crt --key /tls/seed.key "$@"
    }
  ```

     и комментарий над ней
     `# mTLS-курл внутри образа воркера (curl установлен HEALTHCHECK'ом; серты /tls);`
     замени на
     `# Курл внутри образа воркера (curl установлен HEALTHCHECK'ом; серты /tls):`
     (следующую строку комментария `# API-порт на хост не публикуется — ходим изнутри контейнера на localhost:8080.` не менять).

  5) Все `vwk_curl https://localhost:8080/healthz` (поллинг и проверка) замени
     на `vwk_health https://localhost:8080/healthz`; в сообщении об ошибке
     `via mTLS from inside the container` → `via https from inside the container`.

  6) Строку
     `echo "  valkey seed: $(vwk_curl -X POST https://localhost:8080/api/seed/demo)"`
     замени на
     `echo "  valkey seed: $(vwk_api -X POST https://localhost:8080/api/seed/demo)"`.

- **C5.2.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/dev-stand/adminpanel/checks/20-alerts.sh`.
  1) Строку `# t03: проба живости pgworker — только mTLS (healthcheck-пара).` замени на
     `# Проба живости pgworker — https без клиентского серта (открытая грань).`
  2) Строку

  ```bash
  PG_MTLS="curl -fsS -m 3 --cacert $ROOT/deploy/tls/ca.pem --cert $ROOT/deploy/tls/healthcheck.crt --key $ROOT/deploy/tls/healthcheck.key"
  ```

     замени на

  ```bash
  PG_HEALTH="curl -fsS -m 3 --cacert $ROOT/deploy/tls/ca.pem"
  ```

  3) Во всём файле замени каждое `$PG_MTLS` на `$PG_HEALTH` (все строки с
     `https://localhost:${PGW_API_HOST_PORT:-8080}/healthz`).

- **C5.3.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/dev-stand/adminpanel/checks/35-worker-second-instance.sh`.
  1) Строку

  ```bash
  MTLS="curl -fsS -m 3 --cacert $ROOT/deploy/tls/ca.pem --cert $ROOT/deploy/tls/healthcheck.crt --key $ROOT/deploy/tls/healthcheck.key"
  ```

     замени на

  ```bash
  HEALTH="curl -fsS -m 3 --cacert $ROOT/deploy/tls/ca.pem"
  ```

  2) Все `$MTLS https://localhost:${PGW_PORT1}/healthz` и `$MTLS https://localhost:${PGW_PORT2}/healthz`
     (4 места) замени на `$HEALTH …`; в двух сообщениях об ошибках
     `(:…/healthz mTLS;` → `(:…/healthz https;` (строки про pgworker-1/pgworker-2).

  3) Блок kafkaworker

  ```bash
  docker exec as-kafkaworker-2 curl -fsS -m 3 \
    --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key \
    https://localhost:8080/healthz >/dev/null \
    || { echo "❌ surviving as-kafkaworker-2 not alive (/healthz mTLS from inside)"; exit 1; }
  ```

     замени на

  ```bash
  docker exec as-kafkaworker-2 curl -fsS -m 3 --cacert /tls/ca.pem \
    https://localhost:8080/healthz >/dev/null \
    || { echo "❌ surviving as-kafkaworker-2 not alive (/healthz https from inside)"; exit 1; }
  ```

  4) Блок valkeyworker

  ```bash
  docker exec as-valkeyworker-2 curl -fsS -m 3 \
    --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key \
    https://localhost:8080/healthz >/dev/null \
    || { echo "❌ surviving as-valkeyworker-2 not alive (/healthz mTLS from inside)"; exit 1; }
  ```

     замени на

  ```bash
  docker exec as-valkeyworker-2 curl -fsS -m 3 --cacert /tls/ca.pem \
    https://localhost:8080/healthz >/dev/null \
    || { echo "❌ surviving as-valkeyworker-2 not alive (/healthz https from inside)"; exit 1; }
  ```

- **C5.4.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/dev-stand/adminpanel/checks/43-etcd-ha.sh`.
  Строку

  ```bash
  MTLS="curl -fsS -m 5 --cacert $ROOT/deploy/tls/ca.pem --cert $ROOT/deploy/tls/healthcheck.crt --key $ROOT/deploy/tls/healthcheck.key"
  ```

  замени на

  ```bash
  HEALTH="curl -fsS -m 5 --cacert $ROOT/deploy/tls/ca.pem"
  ```

  и оба применения — `$MTLS "https://localhost:${PGW_API_HOST_PORT}/healthz"` и
  `$MTLS "https://localhost:${PGW_API_HOST_PORT2}/healthz"` — на `$HEALTH …`.

- **C5.5.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/dev-stand/adminpanel/checks/57-kafka-worker-health.sh`.
  1) Строку
     `# mTLS (t03): /healthz воркера — только по https с клиентским сертом healthcheck.`
     замени на
     `# /healthz воркера — открытая https-грань без клиентского серта (arch/16 §1.1).`
  2) В однострочном определении функции `wait_healthz()` фрагмент
     `--cacert "$TLS_DIR/ca.pem" --cert "$TLS_DIR/healthcheck.crt" --key "$TLS_DIR/healthcheck.key" "$WORKER_HEALTHZ"`
     замени на
     `--cacert "$TLS_DIR/ca.pem" "$WORKER_HEALTHZ"`.
  3) В строке предусловия фрагмент
     `--cacert "$TLS_DIR/ca.pem" --cert "$TLS_DIR/healthcheck.crt" --key "$TLS_DIR/healthcheck.key" "$WORKER_HEALTHZ"`
     замени на
     `--cacert "$TLS_DIR/ca.pem" "$WORKER_HEALTHZ"`.

- **C5.6.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/dev-stand/adminpanel/checks/60-move-ops.sh`.
  В функции `workers_up()` строки

  ```bash
    curl -fsS -m 3 --cacert "$ROOT/deploy/tls/ca.pem" --cert "$ROOT/deploy/tls/healthcheck.crt" \
      --key "$ROOT/deploy/tls/healthcheck.key" "https://localhost:${port}/healthz" >/dev/null 2>&1
  ```

  замени на

  ```bash
    curl -fsS -m 3 --cacert "$ROOT/deploy/tls/ca.pem" \
      "https://localhost:${port}/healthz" >/dev/null 2>&1
  ```

- **C5.7.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/dev-stand/adminpanel/checks/65-metrics.sh`.
  Правки:

  1) Шапку (строки 7–8)

  ```bash
  # t03: воркеры живут за mTLS — healthz/metrics только с клиентской парой
  # healthcheck (dev-конвенция чеков; панель/прометеус — свои серты).
  ```

     замени на

  ```bash
  # Воркеры: /healthz и /metrics — открытая https-грань без клиентского серта
  # (arch/18 §5.2); панель — http.
  ```

  2) Строку

  ```bash
  MTLS="curl -fsS -m 3 --cacert $ROOT/deploy/tls/ca.pem --cert $ROOT/deploy/tls/healthcheck.crt --key $ROOT/deploy/tls/healthcheck.key"
  ```

     замени на

  ```bash
  HEALTH="curl -fsS -m 3 --cacert $ROOT/deploy/tls/ca.pem"
  ```

  3) Все `$MTLS https://localhost:8082/healthz` и `$MTLS https://localhost:${PGW_API_HOST_PORT:-8080}/healthz`
     (поллинг и проверки, 4 строки) замени на `$HEALTH …`; в сообщениях
     `(mTLS; docker compose logs kafkaworker)` → `(https; docker compose logs kafkaworker)`
     и `not alive on :8080 (mTLS)` → `not alive on :8080 (https)`.

  4) Комментарий `#      предыстория), healthz — mTLS-парой healthcheck ИЗНУТРИ контейнера.`
     замени на `#      предыстория), healthz — https без серта ИЗНУТРИ контейнера.`

  5) Внутриконтейнерные пробы valkeyworker/valkeyworker-2 — в каждой из 4
     curl-строк фрагмент

  ```bash
      --cacert /tls/ca.pem --cert /tls/healthcheck.crt --key /tls/healthcheck.key \
  ```

     замени на

  ```bash
      --cacert /tls/ca.pem \
  ```

     а в 2 сообщениях `mTLS from inside` → `https from inside`.

  6) Комментарий
     `# t03: /metrics воркеров — только за mTLS (клиентская пара healthcheck); панель — http.`
     замени на
     `# /metrics воркеров — открытая https-грань без клиентского серта; панель — http.`

  7) В цикле

  ```bash
  for u in "$MTLS https://localhost:${PGW_API_HOST_PORT:-8080}/metrics" "$MTLS https://localhost:8082/metrics" "curl -fsS -m 10 http://localhost:5050/metrics"; do
  ```

     замени ОБА `$MTLS` на `$HEALTH`.

- **C5.8.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/dev-stand/adminpanel/checks/75-s3worker.sh`.
  Правки:

  1) В шапке (строка 3) фрагмент
     `воркера (s3worker из deploy-контура, mTLS healthcheck-парой): демо-кластер`
     замени на
     `воркера (s3worker из deploy-контура: /healthz и /metrics — https без клиентского серта, /api — пара seed): демо-кластер`.

  2) Строку

  ```bash
  MTLS=(curl -s -m 10 --cacert "$ROOT/deploy/tls/ca.pem" --cert "$ROOT/deploy/tls/healthcheck.crt" --key "$ROOT/deploy/tls/healthcheck.key")
  ```

     замени на две строки

  ```bash
  HEALTH=(curl -s -m 10 --cacert "$ROOT/deploy/tls/ca.pem")
  API=(curl -s -m 10 --cacert "$ROOT/deploy/tls/ca.pem" --cert "$ROOT/deploy/tls/seed.crt" --key "$ROOT/deploy/tls/seed.key")
  ```

  3) Healthz-пробы: комментарий `# healthz живого s3worker (mTLS) —Arrange.` замени на
     `# healthz живого s3worker (открытая грань) — Arrange.`;
     обе строки `"${MTLS[@]}" -o /dev/null "$API/healthz"` (поллинг и проверка)
     замени на `"${HEALTH[@]}" -o /dev/null "$API/healthz"`;
     echo `echo "  s3worker alive: /healthz via mTLS"` замени на
     `echo "  s3worker alive: /healthz via https"`.

  4) Мутации `/api` (5 мест: create 201, валидация 400, валидация 422,
     sync-probe, delete): каждое `"${MTLS[@]}"` замени на `"${API[@]}"`.

  5) Пробу `/metrics`: `"${MTLS[@]}" "$API/metrics"` замени на
     `"${HEALTH[@]}" "$API/metrics"`.

### Шаг C6: `dev-stand/adminpanel/README.md` + `deploy/docker-compose.yml` — документация

**Вход:** шаг C5 завершён.

**Действие (кодер):** выполняет подпункты C6.1–C6.2, ревьюит соответствие
итоговому состоянию (без исторических пассажей — правило «текущее и
планируемое»).

**Выход:** README без переходной оговорки t04; compose-комментарии без «mTLS-only».

**Проверка:** `grep -n "t04-open-metrics-healthz-facet\|prometheus.crt\|healthcheck.crt" dev-stand/adminpanel/README.md` — пусто;
`grep -n "mTLS-only" deploy/docker-compose.yml` — пусто.

**Связь со spec:** §3.3.6, §3.3.7.

**Подпункты (слабая модель):**

- **C6.1.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/dev-stand/adminpanel/README.md`,
  раздел «## TLS API воркеров (pgworker + kafkaworker)». Правки:

  1) Фрагмент

  ```markdown
  без клиентского серта (arch/18 §3/§5.2). Ключевая аутентификация
  X-Api-Key удалена у обоих воркеров. До миграции
  t04-open-metrics-healthz-facet (arch/roadmap/pgworker.md) клиентские
  серты `prometheus.crt`/`healthcheck.crt` продолжают выпускаться и
  использоваться скрейпом/чеками — код и конфиги стенда ещё на
  прежней модели.
  ```

     замени на

  ```markdown
  без клиентского серта (arch/18 §3/§5.2). Ключевая аутентификация
  X-Api-Key удалена у обоих воркеров.
  ```

  2) Фрагмент
     «клиентские `panel.crt/key`, `seed.crt/key`, `prometheus.crt/key`,
     `healthcheck.crt/key` (каталог gitignored)»
     замени на
     «клиентские `panel.crt/key`, `seed.crt/key` (каталог gitignored)».

  3) Фрагмент
     «(канон arch/18 §5.2 — только `ca_file`, без клиентского серта; до
     миграции t04 в `prometheus.yml` остаётся `prometheus.crt`)»
     замени на
     «(канон arch/18 §5.2 — только `ca_file`, без клиентского серта)».

  4) Пункт «**Чеки**» —

  ```markdown
  - **Чеки**: прямые вызовы API воркера (05-seed pg, 20, 65) ходят по
    `https://localhost:8080|8082` с `--cacert ca.pem --cert seed|healthcheck.crt
    --key ...`; остальные — через панель, чей `IWorkerApiGateway` сам
    подписывает запросы клиентским сертом (mTLS-проксирование).
  ```

     замени на

  ```markdown
  - **Чеки**: прямые пробы `/healthz` и `/metrics` — `https` + `--cacert`
    без клиентского серта; `/api`-вызовы чеков — клиентский серт `seed.crt`;
    остальные — через панель, чей `IWorkerApiGateway` сам подписывает запросы
    клиентским сертом (mTLS-проксирование `panel.crt`).
  ```

- **C6.2.** Файл `/Users/demakaev/ZCodeProject/worktrees/feat-t04-open-metrics-healthz-facet/deploy/docker-compose.yml`.
  Только комментарии (env-имена, volumes, сервисы — НЕ трогать):

  1) Двухстрочный комментарий

  ```yaml
    # HTTP API воркера (arch/14 §1.1, t03): mTLS-only грань — advertise
    # https://, ключи-серты из /tls (per-install пакет kfw-install-ca);
  ```

     замени на

  ```yaml
    # HTTP API воркера (arch/14 §1.1, t03): HTTPS-грань, mTLS только на /api —
    # advertise https://, ключи-серты из /tls (per-install пакет kfw-install-ca);
  ```

  2) Строку `# удалён (t03) — грань mTLS-only.` (секция kafkaworker, про `KFW_API_KEY`)
     замени на `# удалён (t03) — грань HTTPS, mTLS только на /api.`

## Фаза D — приёмка/мерж-гейт (spec §7)

### Шаг D1: docker-E2E PgWorker на свежем Release

**Вход:** фазы A–C завершены и закоммичены в ветке.

**Действие (кодер):** гейт обязателен — меняется код `PgWorker.App`. ВСЕГДА
двумя ОТДЕЛЬНЫМИ командами, в таком порядке, каждая в фоне с логом в файл
`/tmp/t04-gate-*.log`; между командами — дождаться финальной строки и читать
лог из файла; после серии — зачистка docker-контейнеров/сетей (AGENTS.md;
осиротевшие `kfw-net-*` при нуле контейнеров — `docker network prune -f`).
Упавшие тесты НЕ перезапускать без полного разбора логов (AGENTS.md).

1) Сборка: `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`
2) Статическая подготовка: `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`
3) Тесты: `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard`

**Выход:** E2E-маркер зелёный на свежем бинаре.

**Проверка:** `Passed!` в финальной строке лога п.3; вывод сборки п.1 — часть гейта.

**Связь со spec:** критерий приёмки 2.

### Шаг D2: полный подъём стенда + чеки 65 и 43

**Вход:** шаг D1 завершён; на хосте выполнена подготовка образов
`dev-stand/images/pull-images.sh` (по ранбуку, если ещё не делалась в сессии).

**Действие (кодер):** полный подъём `dev-stand/adminpanel/checks/00-up.sh`
(пересоберёт образы воркеров с новым HEALTHCHECK; прогон gen.sh зачистит
`deploy/tls` от `prometheus.*/healthcheck.*`; cp-шаг почистит volume) — в
фоне, лог в файл, контроль по `[PHASE]`-строкам (фаза дольше 60 с — сбор логов
и отчёт «почему так долго», AGENTS.md). Затем последовательно:
`checks/65-metrics.sh` и `checks/43-etcd-ha.sh` (каждый — финальная строка
`✓/✅` в своём логе). После серии — зачистка по AGENTS.md. Фейл — разбор по
логам без перезапуска; дефект вне области задачи — `arch/roadmap/bugs.md`.

**Выход:** стенд поднят на новой модели; чеки 65/43 зелёные.

**Проверка:** финальные строки `✓ check 65 …` и скрипта 43 в логах;
`docker inspect --format '{{.State.Health.Status}}' deploy-pgworker-1 deploy-pgworker-2-1` → `healthy`.

**Связь со spec:** критерии приёмки 3, 4.

### Шаг D3: канон-сверка grep + пакет TLS

**Вход:** шаг D2 завершён (стенд поднят).

**Действие (кодер):** проверки (каждая — вывод в файл и чтение из файла):

1) `grep -rn "healthcheck\.crt\|healthcheck\.key\|prometheus\.crt\|prometheus\.key" <корень worktree> --exclude-dir=docs --exclude-dir=.git` —
   единственное допустимое вхождение: строка `rm -f prometheus.crt prometheus.key healthcheck.crt healthcheck.key`
   в `deploy/tls/gen.sh`; вхождения в `arch/roadmap/pgworker.md` исчезнут
   после шага D4 — сверку по этому файлу повторить после D4;
2) `grep -rn "ClientCertificateMode.RequireCertificate" src/ --include="*.cs"` — пусто;
3) `grep -rn "mTLS-only\|вся грань mTLS" src/ deploy/ dev-stand/ docker/` — пусто;
4) `ls deploy/tls/ | grep -E "prometheus|healthcheck"` — пусто;
5) `docker run --rm -v deploy_pgw-api-tls:/tls:ro alpine:3.20 ls /tls` — нет `healthcheck.*`;
6) `docker inspect deploy-pgworker-1 | grep -A3 -i health` — HEALTHCHECK без `--cert`.

**Выход:** все сверки чистые.

**Проверка:** пункты 1–6 выше.

**Связь со spec:** критерии приёмки 4, 5.

### Шаг D4: удаление тега задачи из `arch/roadmap/pgworker.md` (мерж-гейт)

**Вход:** шаги D1–D3 завершены; правки ветки готовы к мержу.

**Действие (кодер):** удалить пункт
`- **`t04-open-metrics-healthz-facet`** — открытая грань /healthz + …`
(целиком, до следующего пункта `- **`tNN…` или конца списка) из
`arch/roadmap/pgworker.md`, а также упоминания `t04-open-metrics-healthz-facet`
из `←`-зависимостей других пунктов этого файла, если они есть (правило
мерж-гейта AGENTS.md: тег слитой задачи удаляется тем же коммитом; без этого
не проходит канон-сверка критерия 5 — пункт роадмапа упоминает
`prometheus.crt`/`healthcheck.crt`). Это единственное допустимое изменение
`arch/` в задаче (обслуживание роадмапа, не канона).

**Выход:** в `arch/roadmap/` нет тега `t04-open-metrics-healthz-facet`.

**Проверка:** `grep -rn "t04-open-metrics-healthz-facet" arch/` — пусто;
повторить пункт 1 шага D3 — теперь пусто и по `arch/`.

**Связь со spec:** критерий приёмки 5 + правило мерж-гейта AGENTS.md (roadmap).

---

## Итоговая самопроверка плана (для исполнителя)

- Все требования spec §3.1 закрыты шагами A1–A4; §3.2 — B1–B3; §3.3 — C1–C6;
  §7 — D1–D4.
- Шагов вне scope spec нет (D4 — требование мерж-гейта проекта, без него
  критерий 5 spec невыполним).
- Границы §3.4 соблюдаются: AdminPanel, E2E-тесты, etcd-контракт, дискавери,
  gen-docker.sh, arch/ (кроме тега роадмапа в D4) — не трогаются.
- После каждой тестовой серии (B3, D1, D2) — зачистка docker-контейнеров/сетей.
