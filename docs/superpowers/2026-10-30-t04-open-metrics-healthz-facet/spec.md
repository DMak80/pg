# t04-open-metrics-healthz-facet — вывод /healthz и /metrics воркеров из-под mTLS (кодовая миграция)

## 1. Цель

Кодовая миграция под уже принятый arch-канон (мерж-коммит `d0e3b7e6`; задача —
зеркалит контракт в код, arch/ НЕ трогаем — расхождений канона с фактическим
кодом эта фаза анализа не выявила: канон описывает целевое состояние, код
ещё на прежней модели «вся HTTP-грань — mTLS»).

Целевое состояние (arch/14 §1.1, arch/16 §1.1, arch/21 §1.1, arch/24 §1.1,
arch/13 §2, arch/18 §3/§5.2, adminpanel/02 §2.3.1/§2.3.2/§9.9):

- У каждого воркера (PgWorker, KafkaWorker, ValkeyWorker, S3Worker) ОДИН
  HTTPS-порт `:8080` — как сейчас.
- Клиентский сертификат на TLS-хендшейке — ОПЦИОНАЛЕН (сервер не требует,
  хендшейк без серта проходит).
- Валидный клиентский сертификат (per-install API-CA) обязателен ТОЛЬКО на
  префиксе `/api`: запрос `/api` без серта или с невалидным сертом —
  HTTP 401 (НЕ разрыв TLS).
- `/healthz` и `/metrics` — открытая грань доверенной docker-сети: отвечают
  200 без клиентского серта (серверный TLS сохраняется — `https` + доверие
  `ca.pem`; паттерн Patroni REST `:8008`, verify_client нет).
- Prometheus-скрейп — `scheme: https` + `tls_config {ca_file}` без
  `cert_file`/`key_file`.
- docker-HEALTHCHECK воркеров — `curl --cacert` без клиентского серта.
- Панель БЕЗ изменений: `IWorkerApiGateway` и healthz-поллер продолжают
  слать `panel.crt` (послать серт на открытую грань — допустимо; опционален,
  не запрещён).

Упраздняется выпуск клиентских сертов `prometheus.crt/key` и
`healthcheck.crt/key` (`deploy/tls/gen.sh`); все потребители этих сертов
(Prometheus, HEALTHCHECK, чеки стенда) переводятся на безсертный доступ.

## 2. Принципы

- **arch-first**: контракт уже изменён; код зеркалит его без отсебятины.
  Любое новое расхождение кода с arch/ — блокирующее.
- **Минимальная грань изменений**: панель, etcd-контракт, дискавери-ключи,
  E2E-тесты всех воркеров — НЕ трогаются (их mTLS-клиенты работают на
  открытой грани как работали).
- **Один канон на четырёх воркеров**: TLS-механика изменения идентична в
  `PgWorker.App/Api/ApiTlsEndpoints.cs`, `KafkaWorker.App/Api/TlsEndpoints.cs`,
  `ValkeyWorker.App/Api/TlsEndpoints.cs`, `S3Worker.App/Api/TlsEndpoints.cs`;
  новая общая часть — в `src/Shared.Tls` (политика дедупликации проекта:
  прецеденты `TlsChain`/`TlsEnv` — «4 копии слиты»).
- **WAF-совместимость**: режим `AllowInsecureHttp=true` (только in-memory
  WAF-тесты, arch/14 §1.1) сохраняет сегодняшнее поведение — `/api` без
  авторизации (иначе падают все интеграционные WAF-тесты мутаций, ходящие
  in-memory без TLS).
- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`; P/Invoke
  запрещён; скипов в тестах нет; таймауты/бюджеты не меняются.

## 3. Структура/компоненты (что меняется)

### 3.1. Код воркеров — TLS-грань (4 проекта)

**3.1.1. `src/Shared.Tls`** — новый общий middleware:

- `src/Shared.Tls/Shared.Tls.csproj`: добавить
  `<FrameworkReference Include="Microsoft.AspNetCore.App" />` (сборка
  получает API-поверхность ASP.NET Core для middleware; на потребителей
  влияет только compile-time).
- Новый файл `src/Shared.Tls/ClientCertAuthMiddleware.cs`:
  - `public sealed record ClientCertAuthOptions(bool Enabled, X509Certificate2? ClientCa);`
    — DI-синглтон; `Enabled=false` — pass-through (режим
    `AllowInsecureHttp`/WAF), `ClientCa` — валидатор per-install API-CA.
    Инвариант опции: `Enabled=true ⇒ ClientCa≠null`; нарушение инварианта —
    guard в конструкторе middleware (`InvalidOperationException`),
    fail-fast при конфигурировании, не пер-запросный 500.
  - `public sealed class ClientCertAuthMiddleware(RequestDelegate next,
    ClientCertAuthOptions options)`:
    1. `options.Enabled == false` → `await next(context)`.
    2. Путь НЕ `/api` (точный сегмент-префикс: `path == "/api"` ИЛИ
       `path.StartsWith("/api/")`, сравнение `OrdinalIgnoreCase`) → `next`.
    3. `context.Connection.ClientCertificate is null` → `401` (без тела,
       без `WWW-Authenticate`), НЕ вызывая `next`.
    4. `TlsChain.ValidateChain(cert, options.ClientCa!) == false` → `401`.
    5. Иначе → `next`.
  - Комментарии — по-русски, со ссылкой на arch/14 §1.1 (канон всех
    четырёх воркеров).

**3.1.2. Четыре `TlsEndpoints`/`ApiTlsEndpoints`** (файлы перечислены в
принципах; правки идентичны в каждом):

- В `ConfigureMtls` TLS-ветки (не-`AllowInsecureHttp`):
  - `ClientCertificateMode = ClientCertificateMode.RequireCertificate` →
    **`ClientCertificateMode.AllowCertificate`** (сервер запрашивает серт,
    но не требует; клиент без серта проходит хендшейк).
  - Отключить валидацию клиентского серта на хендшейке штатным механизмом
    **`AllowAnyClientCertificate()`** на `HttpsConnectionAdapterOptions`:
    присланный серт НЕ валидируется на хендшейке — соединение с невалидным
    сертом не рвётся; валидация переезжает в middleware п.3.1.1, где даёт
    401 только на `/api`; на `/healthz`//`/metrics` присланный серт
    игнорируется. Просто удалить присваивание
    `ClientCertificateValidation` НЕЛЬЗЯ: при null-калбэке Kestrel
    (`HttpsConnectionMiddleware`) применяет дефолтную системную валидацию
    цепочки (`sslPolicyErrors != None` → отказ) и рвёт хендшейк для
    per-install сертов приватной API-CA. `AllowAnyClientCertificate()`
    мутирует опцию до захвата её listener'ом; вызываем до
    `ConfigureKestrel` для определённости. Целевой блок
    (замена текущего inline-`UseHttps(...)`):

    ```
    var httpsOptions = new HttpsConnectionAdapterOptions
    {
        ServerCertificate = serverCert,
        ClientCertificateMode = ClientCertificateMode.AllowCertificate,
    };
    httpsOptions.AllowAnyClientCertificate();
    builder.WebHost.ConfigureKestrel(o => o.ListenAnyIP(port,
        listenOptions => listenOptions.UseHttps(httpsOptions)));
    ```

  - Зарегистрировать опцию middleware:
    `builder.Services.AddSingleton(new ClientCertAuthOptions(Enabled: true, ClientCa: clientCa));`
    (вызов — до `builder.Build()`, `builder.Services` доступен; серт
    `clientCa` живёт всё приложение — уже загружен без using, как серверный).
- В `AllowInsecureHttp`-ветке (ранний `return`): зарегистрировать
  `new ClientCertAuthOptions(Enabled: false, ClientCa: null)` — middleware
  есть в pipeline у всех, но при WAF-режиме pass-through.
- Обновить шапки-комментарии файлов: вместо «вся грань (вкл. /healthz и
  /metrics) — только TLS; клиентские серты …» — канон «HTTPS-грань;
  клиентский серт опционален на хендшейке, mTLS обязателен только на
  `/api` (401 без валидного серта); `/healthz`//`/metrics` — открытая
  грань доверенной docker-сети (arch/NN §1.1)».

**3.1.3. Четыре `Program.cs`** (PgWorker/KafkaWorker/ValkeyWorker/S3Worker.App):

- После `builder.Build()` (рядом с существующими warning-логами), ДО
  `app.MapAppMetrics()/MapHealthChecks()/MapWorkerApi()`:
  `app.UseMiddleware<Shared.Tls.ClientCertAuthMiddleware>();`
- Обновить формулировки «mTLS-only API/грань» в конфиг-валидациях и
  комментариях (AdvertiseUrl обязан оставаться `https://` — требование
  сохраняется, меняется только пояснение: «HTTPS-грань, mTLS только на
  /api, arch/NN §1.1»). Аналогично `src/KafkaWorker.App/Options.cs`
  (xml-doc «mTLS-only грань») и сообщения
  `src/S3Worker.App/S3StartupChecks.cs` / валидаций Program.cs.
- Комментарий про `/metrics` «на том же mTLS-Kestrel-порту» → «на том же
  HTTPS-Kestrel-порту» (arch/18 §3 — открытая грань).

### 3.2. Тесты воркеров (интеграционные WAF)

**3.2.1. Четыре `MtlsApiTests`**
(`src/tests/PgWorker.IntegrationTests/Api/MtlsApiTests.cs`,
`src/tests/KafkaWorker.IntegrationTests/Api/MtlsApiTests.cs`,
`src/tests/ValkeyWorker.IntegrationTests/Api/MtlsApiTests.cs`,
`src/tests/S3Worker.IntegrationTests/Api/MtlsApiTests.cs`) — переориентировать
с «без серта — TLS-отказ» на канон открытой грани (структура тест-хоста с
реальным Kestrel-сокетом, TLS 1.2 и PFX round-trip — сохраняется; тест
добавляет в хост маршрут-заглушку `app.MapGet("/metrics", () => Results.Ok(""))`
— проверяется транспортная семантика открытой грани, а не формат выгрузки;
формат `/metrics` уже покрыт `MetricsTests`). Новые ассерты (AAA-комментарии
по канону тестов проекта):

1. без клиентского серта: `GET /api/ping` → **401** (HTTP-ответ; был
   TLS-отказ — `HttpRequestException` на хендшейке);
2. без клиентского серта: `GET /healthz` → **200**;
3. без клиентского серта: `GET /metrics` → **200**;
4. с клиентским сертом API-CA: `GET /api/ping` → 200 (как сегодня);
5. с НЕВАЛИДНЫМ клиентским сертом (серт, подписанный ДРУГОЙ тестовой CA —
   выпустить вторую `GenerateCa` и серт от неё): `GET /api/ping` → **401**
   (хендшейк НЕ рвётся — сервер принял соединение, middleware отверг путь).
   Клиент для п.5 — тот же `SocketsHttpHandler` c TLS 1.2 и
   `ClientCertificates = [invalidCert]`.
- Имя метода переименовать по новой семантике (например
  `Api_WithoutCert_401__HealthzMetrics_WithoutCert_200__WithCert_200`; в
  каждом проекте сохранить локальный стиль имён).
- Шапку-комментарий файла обновить на канон открытой грани.

**3.2.2. `MetricsTests`** (4 проекта, где есть): обновить только
комментарии — «защита API — транспортная (mTLS): 401 без валидного серта
на `/api` покрывает MtlsApiTests; `/metrics` — открытая грань (arch/18 §3)».
Код/ассерты не меняются.

### 3.3. deploy/стенд

**3.3.1. `deploy/tls/gen.sh`**:

- Удалить строки выпуска:
  `[ -f prometheus.crt ] || issue prometheus prometheus clientAuth ""` и
  `[ -f healthcheck.crt ] || issue healthcheck healthcheck clientAuth ""`.
- Добавить самозачистку старых пакетов (решение пользователя):
  `rm -f prometheus.crt prometheus.key healthcheck.crt healthcheck.key`
  (с комментарием: клиентские серты упразднены каноном открытой грани —
  arch/14 §1.1/arch/18 §5.2; удаляются и наследие живых стендов — gen.sh
  идемпотентен и вызывается каждым 00-up.sh).
- Обновить шапку-комментарий (клиентские: `panel`, `seed`) и финальный
  `echo`-список файлов пакета.

**3.3.2. `dev-stand/adminpanel/metrics/prometheus/prometheus.yml`** — джобы
`pgworker`, `kafkaworker`, `valkeyworker`: убрать `cert_file:
/tls/prometheus.crt` и `key_file: /tls/prometheus.key`; остаётся `scheme:
https` + `tls_config { ca_file: /tls/ca.pem }`. Комментарии джоб обновить
(«открытая https-грань без клиентского серта, arch/18 §5.2»). Джобы
`patroni-nodes` (уже без серта), `walreceiver-nodes`/`adminpanel`/
`sd-generator` (http) — без изменений.

**3.3.3. Dockerfile×4** (`docker/PgWorker.Dockerfile`,
`docker/KafkaWorker.Dockerfile`, `docker/ValkeyWorker.Dockerfile`,
`docker/S3Worker.Dockerfile`) — HEALTHCHECK без клиентского серта:

```
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -sf --cacert /tls/ca.pem https://localhost:8080/healthz || exit 1
```

(параметры HEALTHCHECK и путь — как в текущем файле; убрать только
`--cert /tls/healthcheck.crt --key /tls/healthcheck.key`; комментарии
«/healthz за mTLS … клиентская пара healthcheck» → «/healthz — открытая
https-грань без клиентского серта (arch/NN §1.1)»). Сан серта
`localhost` уже покрыт (`pgserver`/`server` SAN включают `DNS:localhost`).

**3.3.4. `dev-stand/adminpanel/checks/00-up.sh`**:

- cp-строка наполнения volume `deploy_pgw-api-tls` (docker run alpine):
  убрать `/src/healthcheck.crt /src/healthcheck.key` из списка копирования.
- Переменную `MTLS=(curl … --cert healthcheck.crt --key healthcheck.key)`
  (строка ~201) заменить на безсертную пробу открытой грани:
  `HEALTH=(curl -fsS -m 3 --cacert "$ROOT/deploy/tls/ca.pem")`; все её
  применения — healthz-пробы воркеров (pgworker×2, s3worker×2 и далее по
  файлу) — перевести на `"${HEALTH[@]}"` без клиентского серта. Комментарии
  «via mTLS» → «https без клиентского серта (открытая грань)».
- Секцию 7c (s3worker healthz «по mTLS healthcheck-парой») — комментарий и
  пробу обновить соответственно.

**3.3.5. Чеки стенда** — правило: пробы `/healthz` и `/metrics` → `curl
--cacert …/ca.pem` БЕЗ `--cert/--key`; вызовы `/api` (сид/мутации) →
клиентский серт **`seed.crt/key`** (конвенция «чек = стендовый сид»,
arch/13 §2 — клиентские серты панели `panel.crt` и сида `seed.crt`;
`healthcheck.crt` упразднён):

- `checks/05-seed.sh`: kafka/valkey — `/healthz`-поллинг и внутриконтейнерный
  `vwk_curl` без `--cert/--key`; `POST /api/seed/demo` — с парой
  `seed.crt`/`seed.key`. Комментарий «mTLS … с клиентской парой healthcheck»
  переписать.
- `checks/20-alerts.sh`: `PG_MTLS`-пробы `/healthz` → без `--cert/--key`
  (переименовать в `PG_HEALTH`), комментарий обновить.
- `checks/35-worker-second-instance.sh`: переменная `MTLS` → безсертная
  (переименовать `HEALTH`); внутренние пробы `docker exec as-kafkaworker-2/
  as-valkeyworker-2 curl … --cert /tls/healthcheck.crt …` — убрать
  `--cert/--key` (в volume `deploy_pgw-api-tls` этих файлов больше нет —
  обязательность правки). Комментарии «mTLS» обновить.
- `checks/43-etcd-ha.sh`: `MTLS` → безсертная `HEALTH` (обе healthz-пробы
  pgworker), комментарий.
- `checks/57-kafka-worker-health.sh`: `wait_healthz` и предусловие
  `/healthz` — `--cacert` без `--cert/--key`; комментарии «/healthz … по
  https с клиентским сертом healthcheck» → открытая грань.
- `checks/60-move-ops.sh`: healthz-проба (строки ~120–121) — убрать
  `--cert/--key`.
- `checks/65-metrics.sh`: `MTLS` → безсертная `HEALTH` (пробы `/healthz`
  pgworker/kafkaworker, `/metrics` шага 1); внутриконтейнерные пробы
  `valkeyworker`/`valkeyworker-2` — убрать `--cert/--key`; комментарии
  «mTLS-парой healthcheck» обновить (заголовочный и внутри шагов).
- `checks/75-s3worker.sh`: разделить пробы — `HEALTH` (без серта) для
  `/healthz` и `/metrics`; `API` (с `--cert seed.crt --key seed.key`) для
  мутаций `POST/DELETE /api/s3/clusters*` и `/api/…/sync-probe`; комментарии
  («mTLS healthcheck-парой») обновить.

**3.3.6. `dev-stand/adminpanel/README.md`** — раздел «TLS API воркеров»:
снять переходную оговорку t04 («до миграции … продолжают выпускаться и
использоваться»); убрать `prometheus.crt/key`, `healthcheck.crt/key` из
перечня генерации; пункт Prometheus — «`tls_config` только `ca_file`
(канон arch/18 §5.2, в конфиге уже так)»; пункт «Чеки» — «прямые пробы
`/healthz`//`/metrics` — `https` + `--cacert` без клиентского серта;
`/api`-вызовы чеков — клиентский серт `seed.crt`; остальные — через панель
(mTLS-проксирование `panel.crt`)».

**3.3.7. `deploy/docker-compose.yml`** — только комментарии: «mTLS-only
грань» → «HTTPS-грань, mTLS только на `/api`» (env-имена `*_API_TLS_*`,
монтирования volumes — НЕ меняются: серверные серты и CLIENT_CA нужны как
прежде).

### 3.4. Не трогаем (границы задачи)

- **AdminPanel** (код и тесты): `WorkerApiGateway`/`WorkerTlsHandler` шлют
  `panel.crt` на `/api` и healthz-тики — работает на открытой грани без
  изменений.
- **E2E-тесты всех воркеров** (PgWorker.E2eTests, KafkaWorker/ValkeyWorker/
  S3Worker.E2eTests): их mTLS-клиенты (`CreateMtlsHttpClient`, E2E-контуры
  со своей CA) продолжают работать — серт опционален, но допустим.
- **etcd-контракт, дискавери-ключи `/workers/api/<id>`**,
  `WorkerApiCertReader`, серверные серты (`/workers/api_tls/*`, env-фоллбэк)
  — без изменений.
- **`deploy/tls/gen-docker.sh`**, docker-CA — без изменений.
- **arch/** — без изменений (уже каноничен; при выполнении обнаружится
  расхождение — стоп и разбор, правка arch/ отдельным решением).

## 4. Фазы исполнения

1. **Фаза A — код воркеров** (3.1): Shared.Tls (csproj + middleware),
   4×TlsEndpoints, 4×Program.cs; сборка `src/PgWorker.slnx` зелёная.
2. **Фаза B — WAF-тесты** (3.2): 4×MtlsApiTests по новой семантике,
   комментарии MetricsTests; интеграционные прогоны 4 проектов зелёные.
3. **Фаза C — deploy/стенд** (3.3): gen.sh, prometheus.yml, Dockerfile×4,
   00-up.sh, чеки 05/20/35/43/57/60/65/75, README, комментарии
   deploy-compose.
4. **Фаза D — приёмка/мерж-гейт** (раздел 7).

## 5. Ограничения

- P/Invoke запрещён; только управляемый API .NET.
- Таймауты/бюджеты тестов не менять; скипов не добавлять.
- Язык: комментарии/доки — русские; идентификаторы — английские.
- E2E-изоляция и правила зачистки серий — по AGENTS.md (после каждой серии
  зачистка; перед следующей — финальная строка прогона).
- Правило «буква приказа»: ничего сверх перечисленного; обнаруженное вне
  задачи (фейлы чужих тестов, новые баги) — в `arch/roadmap/bugs.md`, не
  чинить в этой ветке.

## 6. Принятые решения (закрытые развилки)

1. **Механизм открытой грани**: `ClientCertificateMode.AllowCertificate` +
   **`AllowAnyClientCertificate()`** (штатный способ ASP.NET Core отключить
   валидацию клиентского серта на хендшейке — устанавливает
   `ClientCertificateValidation`, всегда принимающий присланный серт) +
   middleware `401` на `/api`. Удалять `ClientCertificateValidation` в
   изоляции НЕЛЬЗЯ: при null-калбэке Kestrel (`HttpsConnectionMiddleware`)
   применяет дефолтную системную валидацию цепочки
   (`sslPolicyErrors != None` → отказ) и рвёт хендшейк для per-install
   сертов приватной API-CA — цель «хендшейк не рвётся никогда, в т.ч. с
   невалидным сертом» достигается только парой
   AllowCertificate + AllowAnyClientCertificate(). Решение однозначно из
   roadmap («опциональный клиентский серт на TLS-хендшейке + политика 401
   только на /api») и arch/14 §1.1 («запрос /api без валидного клиентского
   серта — 401» — HTTP-статус, не TLS-разрыв). `DelayCertificate`/
   renegotiation не используем (ненадёжен на macOS/Linux).
2. **Общий middleware в `Shared.Tls`** (+`FrameworkReference`): дедуп-политика
   проекта (прецеденты слияния `TlsChain`/`TlsEnv`); 4 копии в App-проектах
   не плодим.
3. **`401`-политика активна только при TLS-грани**: при
   `AllowInsecureHttp=true` (WAF) — pass-through, иначе падают все
   интеграционные WAF-тесты мутаций (in-memory, без TLS). Канон:
   `AllowInsecureHttp` существует только для WAF-тестов.
4. **Область 401 — точный сегмент-префикс `/api`** (`"/api"` или
   `"/api/…"`, case-insensitive); `/apifoo` — не `/api`. Все прочие пути —
   открытая грань (несуществующие — 404 без серта).
5. **Валидация серта — тот же механизм, что сегодня**:
   `TlsChain.ValidateChain(cert, clientCa)` (CustomRootTrust, NoCheck) —
   переезжает из хендшейка в middleware без изменения семантики доверия.
6. **Невалидный серт на `/healthz`//`/metrics` — игнорируется** (200):
   открытая грань не смотрит на серт вовсе; следствие решения 1.
7. **Чеки на `/api` используют `seed.crt`/`seed.key`** (05-seed
   `/api/seed/demo`, 75-s3worker мутации): конвенция «чек = стендовый сид»,
   серт остаётся в выпуске gen.sh и описан в arch/13 §2; `panel.crt` —
   только панель.
8. **`/metrics` в MtlsApiTests — маршрут-заглушка** в тест-хосте: тест
   проверяет транспортную семантику (200 без серта), не формат выгрузки
   (формат уже покрыт `MetricsTests`).
9. **gen.sh самозачистка**: при прогоне удалять `prometheus.*` и
   `healthcheck.*` (решение пользователя через AskUserQuestion) — старые
   пакеты живых стендов очищаются ближайшим 00-up.sh.
10. **E2E-тесты и панель — не трогаем** (3.4): mTLS-клиенты корректны на
    открытой грани.
11. **401 — без тела и `WWW-Authenticate`**: договор о теле отсутствует в
    каноне; WAF-тесты проверяют только статус.
12. **Тест невалидного серта — обязательный кейс** (401, не TLS-разрыв):
    канон «без ВАЛИДНОГО серта — 401» требует обеих ветвей (нет серта /
    невалидный серт); ассерт закрепляет, что
    AllowCertificate + AllowAnyClientCertificate() действительно не рвёт
    хендшейк (регрессия к дефолтной системной валидации Kestrel).

## 7. Критерии приёмки (мерж-гейт)

1. **Юниты/интеграции**: интеграционные прогоны 4 воркер-проектов зелёные
   (обновлённые MtlsApiTests: `/api` без серта = 401, `/api` с невалидным
   сертом = 401, `/healthz` и `/metrics` без серта = 200, `/api` с сертом
   API-CA = 200); остальные тесты затронутых сборок — без регрессий.
2. **docker-E2E PgWorker на свежем Release** (код `PgWorker.App` меняется —
   гейт обязателен): сборка и тесты ДВУМЯ отдельными командами —
   1) `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`;
   2) статическая подготовка `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project
   src/tests/PgWorker.E2ePrepare -c Release`, затем
   `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c
   Release --no-build --filter FullyQualifiedName~Scale_AddEmptyShard`
   (полный E2eFixture — не требуется: provisioning/portalloc/moves-процессы
   не меняются; минимум — кейс-маркер по AGENTS.md).
3. **Чеки стенда**: полный подъём `dev-stand/adminpanel/checks/00-up.sh`
   (пересборка образов воркеров с новым HEALTHCHECK) → зелёные
   `checks/65-metrics.sh` (все scrape-джобы up без `cert_file` в
   prometheus.yml; `/metrics` воркеров отвечают без клиентского серта) и
   `checks/43-etcd-ha.sh`.
4. **Пакет TLS**: после `00-up.sh` в `deploy/tls/` нет
   `prometheus.*`/`healthcheck.*`; `docker inspect` воркеров — HEALTHCHECK
   healthy; в volume `deploy_pgw-api-tls` нет `healthcheck.*`.
5. **Канон-сверка**: grep по репо (вне `docs/superpowers/`) не находит
   использований `healthcheck.crt`/`prometheus.crt` (единственное допустимое
   вхождение имён — строка самозачистки `rm -f` в `deploy/tls/gen.sh`) и
   `ClientCertificateMode.RequireCertificate` в HTTP-грани воркеров;
   в HTTP-грани воркеров присутствует пара `AllowCertificate` +
   `AllowAnyClientCertificate()`; формулировки «mTLS-only»/«вся грань mTLS»
   из комментариев убраны.
6. Зачистка docker-контейнеров/сетей после каждой тестовой серии (AGENTS.md).
