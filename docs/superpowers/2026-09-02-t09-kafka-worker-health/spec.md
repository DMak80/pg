# Спецификация: t09-kafka-worker-health — честная наблюдаемость здоровья KafkaWorker

Дата: 2026-09-02. Фаза dev-flow: spec. Источники: `arch/roadmap/kafkaworker.md`
(тег `t09-kafka-worker-health`), `arch/16-kafkaworker.md` §7/§1.1 (канон
воркера — обновлён этой задачей), `arch/adminpanel/02-etcd-contract.md`
§2.3.2 (контракт панели — обновлён), `arch/adminpanel/03-panels.md` §4
(каталог алертов — обновлён), диагностика живого стенда 2026-08-31
(`as-kafkaworker` unhealthy 18+ мин при живом heartbeat-lease и успешных
тиках «active test: ok»; `/healthz` → 503), прецеденты
`docs/superpowers/2026-08-31-pgworker-adopt-repair/` (паттерн «живой-Ф7»
сброса ошибки тика в PgWorker-циклах) и `docs/superpowers/
2026-09-01-pg-probe-alerts/` (spec-паттерн, arch-first).

## 0. Контекст (что сейчас и почему меняется)

`/healthz` KafkaWorker сегодня **лжёт о живом воркере** — три дефекта
одного корня (диагностика 2026-08-31):

1. **Sticky-`StatusError`**: `ReconcileLoop.ExecuteAsync`
   (`src/KafkaWorker.App/Loops/ReconcileLoop.cs:52`) записывает
   `StatusError` при ошибке тика, но никогда не сбрасывает при
   последующих успешных тиках; тот же паттерн в `SnapshotLoop.cs:60`
   (`KeepaliveLoop` ему не подвержен — у него фейлящих тиков нет, но и
   сброса нет). Через `HealthCheckAbstract<T>` (Unhealthy «service has
   error») один transient-сбой (пересоздание etcd-контейнера — DNS-имя
   пропало на секунды) даёт вечный 503 до рестарта воркера, docker
   HEALTHCHECK копит `FailingStreak`. В PgWorker этот же дефект уже
   починен (adopt-repair, «живой-Ф7»: `StatusError = Result.Success()`
   при успешном тике — `src/PgWorker.App/Loops/ReconcileLoop.cs:51`,
   `SnapshotLoop.cs:53`, `KeepaliveLoop.cs:42`) — KafkaWorker остался на
   старом каркасе (копия циклов до фикса).
2. **Флейд активных проб**: `ServiceProbes.EtcdReachableAsync`
   (`src/KafkaWorker.App/HealthChecks/ServiceProbes.cs`) рассчитывает на
   `Result.Failed` от шлюза, но при сетевых исключениях открытия новых
   соединений (`HttpRequestException: Name or service not known
   (etcd:2379)` — .NET DNS-клиент флейпит, при этом `curl`/`getent` из
   того же контейнера резолвят стабильно) чек роняется исключением
   (`DefaultHealthCheckService[103]` стектрейсы в логе) — оператор видит
   пустой Unhealthy без данных секций вместо Degraded с причиной.
   Корень флейпа: параллельные A/AAAA-резолвы .NET против Docker
   embedded DNS (127.0.0.11) + вечный пул коннектов etcd-клиента
   (`AddHttpClient("etcd")` без настройки, клиент захвачен синглтоном
   навсегда — после пересоздания etcd-контейнера пул держит мёртвые
   адреса).
3. **Две правды о живости**: панель судит о живости воркера по
   heartbeat/дискавери-ключам `/kafkaworker/api/*` (lease TTL 15 c жив —
   «всё хорошо») и молчит, когда docker-health unhealthy: docker красный,
   панель зелёная. Для PgWorker решено (2026-08-31, `WorkerHealthPoller` +
   алерт `worker-unhealthy`): панель опрашивает `/healthz` живых инстансов
   по URL из lease-ключей. Для KafkaWorker аналога нет — только critical
   `worker-api-unreachable` при полном исчезновении ключей.

Решения пользователя (зафиксированы, вопросы заданы по одному):

- **Д1 — канал правды для панели**: панель пробит `/healthz` напрямую
  (порт паттерна PgWorker); heartbeat-ключи etcd НЕ расширяются статусом.
- **Д2 — глубина DNS-фикса**: полный — catch-all проб + SocketsHttpHandler
  с `PooledConnectionLifetime` + IPv4-first `ConnectCallback`.
- **Д3 — e2e**: новый отдельный чек `57-kafka-worker-health.sh`
  (failover etcd-контейнера как transient-стимул).

## 1. Цель

1. **Healthz отражает последнее состояние, а не первый сбой**: успешный
   тик каждого цикла (Reconcile/Snapshot/Keepalive) гасит `StatusError`
   прошлого тика — transient-сбой ≠ вечный 503; после восстановления
   зависимости `/healthz` возвращает 200 без рестарта контейнера.
2. **Чек всегда отдаёт структуру**: любое исключение активной пробы
   (etcd/docker-hosts) оборачивается в `Result.Failed` → Degraded с
   данными секций; чек никогда не падает исключением (event 103,
   пустой ответ).
3. **DNS-флейп устранён**: etcd-клиент воркера на `SocketsHttpHandler` с
   `PooledConnectionLifetime` (пере-резолв после пересоздания
   etcd-контейнера) и последовательным IPv4-first-резолвом в
   `ConnectCallback` (обход параллельных A/AAAA против Docker embedded
   DNS).
4. **Единая правда для панели**: панель опрашивает `/healthz` живых
   инстансов KafkaWorker (тот же поллер/интервал, что для PgWorker) —
   degraded/unhealthy воркер виден как warning-алерт `worker-unhealthy`
   ≤ 2 тиков поллера, после восстановления гаснет; docker-health и панель
   больше не расходятся.

Не-цели: Prometheus-метрики (roadmap t04), TLS/mTS API (t03), правки
PgWorker-циклов (они уже честные), расширение heartbeat-ключей статусом
(отвергнуто решением Д1), фронтенд (алерты видны существующими
компонентами), унификация дублей панели/воркеров (t08).

## 2. Принципы

1. **arch-first**: канон обновлён до кода (§8) — `arch/16` §7 (честный
   health воркера), `arch/adminpanel/02` §2.3.2 (health-опрос KafkaWorker
   панелью), `arch/adminpanel/03` §4 (`worker-unhealthy` на оба воркера).
2. **Порт паттерна, не изобретение**: сброс ошибки тика — дословный порт
   «живой-Ф7» из PgWorker-циклов (adopt-repair); health-опрос панели —
   дословный порт `WorkerHealthPoller`/`WorkerUnhealthyRule`
   (pg-грань) на kafka-домен.
3. **Здоровье = последнее наблюдение**: ни один разовый сбой не должен
   жить дольше следующего успешного цикла — симметрично для циклов
   (StatusError), проб (Degraded по последней пробе) и панели (алерт по
   последнему опросу поллера).
4. **Структура вместо исключения**: observability-код не имеет права
   ронять потребителя — проба возвращает `Result`, чек —
   `HealthCheckResult` c Data-секциями при любых отказах.
5. **Один факт — один канал**: правдой о здоровье процесса признаётся
   его собственный `/healthz` (то же видит docker-healthcheck); etcd —
   правда о lease/координации, не о здоровье.
6. Единый kind `worker-unhealthy` для обоих воркеров (target различает:
   `pgworker/<id>` / `kafkaworker/<id>`) — без нового kind
   `worker-degraded` (следование каталогу 03 §4, Д-решение в §9).
7. .NET 10, C# latest, `Nullable=enable`, `TreatWarningsAsErrors=true`;
   воркер и панель — только docker; документация русская, идентификаторы
   английские.

## 3. Структура / компоненты

### 3.1. Воркер — сброс `StatusError` (дефект 1)

Три точечные правки циклов (дословный порт PgWorker, комментарий-канон
«healthz = „последний тик" (живой-Ф7): успешный тик гасит ошибку
прошлого — иначе единственный упавший тик = вечный unhealthy»):

- `src/KafkaWorker.App/Loops/ReconcileLoop.cs`: в ветке
  `tick.IsSuccess` — `StatusError = Result.Success();` перед задержкой
  (порт `PgWorker.App/Loops/ReconcileLoop.cs:49-51`).
- `src/KafkaWorker.App/Loops/SnapshotLoop.cs`: в ветке
  `shot.IsSuccess` — сброс (порт `PgWorker.App/Loops/SnapshotLoop.cs:
  51-53`). Не-лидер не сбрасывает (как и в PgWorker): у не-лидера
  ошибок взятия снапшота не бывает, сброс бессмысленен.
- `src/KafkaWorker.App/Loops/KeepaliveLoop.cs`: в теле цикла —
  `StatusError = Result.Success();` перед `MarkKeepaliveTick()`
  (порт `PgWorker.App/Loops/KeepaliveLoop.cs:40-42`; проход контура
  жив — ошибка прошлого, если появится, гасится; симметрия циклов).

`HealthCheckAbstract<T>` и регистрация чеков (`Program.cs`: `reconcile-loop`,
`keepalive-loop`, `snapshot-loop`, `kafkaworker`) не меняются — они
начинают работать правильно сами: `StatusError` теперь живёт ровно до
первого успешного тика.

### 3.2. Воркер — catch-all проб и чека (дефект 2)

- `src/KafkaWorker.App/HealthChecks/ServiceProbes.cs`:
  - `EtcdReachableAsync`: весь перебор endpoints — в try/catch
    `Exception` (внешняя отмена — пробрасывается: `OperationCanceled
    Exception` при `ct.IsCancellationRequested` — это остановка запроса,
    а не «etcd молчит»; отмена ProbeTimeout ловится шлюзом и приходит
    как `Result.Failed`): сетевое исключение → `Result.Failed`
    (структура, не бросок). Итог «все endpoints молчат» — последний
    Failed (как сегодня).
  - `PingDockerHostsAsync`: try/catch уже есть в `PingAsync` per-host —
    добавить страховочный catch-all на весь метод (исключение вне
    per-host-вызовов, например изменение конфигурации в момент итерации)
    → словарь с единственной записью `all: Failed`.
- `src/KafkaWorker.App/HealthChecks/KafkaWorkerHealth.cs`: тело
  `CheckHealthAsync` — оборачивается в catch-all `Exception` →
  `HealthCheckResult.Degraded("health-чек выполнился с ошибкой: …",
  data: { "error" })`. Гарантия контракта «чек всегда отдаёт структуру»
  даже при будущих изменениях проб (defence-in-depth поверх §3.2
  первого пункта).

### 3.3. Воркер — etcd-клиент против DNS-флейпа (дефект 2, корень)

`src/KafkaWorker.App/Program.cs`, регистрация
`builder.Services.AddHttpClient("etcd")` — конфигурация handler'а
(прецедент в этом же воркере: `src/KafkaWorker.Docker/Engine/
DockerEngine.cs:18-21` уже создаёт `SocketsHttpHandler` с
`PooledConnectionLifetime=5 мин`):

```csharp
builder.Services.AddHttpClient("etcd").ConfigurePrimaryHttpMessageHandler(() =>
    new SocketsHttpHandler
    {
        PooledConnectionLifetime = TimeSpan.FromMinutes(5),
        ConnectCallback = EtcdConnectCallback.ConnectAsync, // §3.3.1
    });
```

Важно: `EtcdGateway`-синглтон захватил `HttpClient` фабрики навсегда —
ротация handler'ов фабрики на него не действует; поэтому явный
`SocketsHttpHandler` с `PooledConnectionLifetime` (пул сам пере-резолвит
DNS раз в 5 мин, переживая пересоздание etcd-контейнера).

#### 3.3.1. `EtcdConnectCallback` — IPv4-first последовательный резолв

Новый статический класс `src/KafkaWorker.App/EtcdConnectCallback.cs`
(unit-тестируемый без сети):

- `ConnectAsync(SocketsHttpConnectionContext ctx, CancellationToken ct)`:
  `Dns.GetHostAddressesAsync(ctx.DnsEndPoint.Host, ct)` → сортировка
  IPv4-first (`AddressFamily.InterNetwork` раньше `InterNetworkV6`) →
  последовательные попытки `Socket.ConnectAsync(address, port, ct)`
  (первый успех → `NetworkStream(socket, ownsSocket: true)`); все
  попытки упали → бросок последнего исключения (шлюз обернёт в
  `Result.Failed`).
- Логирование резолва не добавляется (observability — уровень чека).
- IP-литерал (`IPAddress.TryParse`) — коннект напрямую, без DNS.

Обоснование: дефолтный резолв `SocketsHttpHandler` выполняет A/AAAA
параллельно; Docker embedded DNS (127.0.0.11) на параллельных запросах
отдаёт отказы («Name or service not known») при живом имени —
последовательный IPv4-first резолв + кэш пул-коннектов снимает флап
(диагностика живого стенда: `curl`/`getent` из того же контейнера
резолвили 10/10 стабильно).

### 3.4. Панель — health-опрос KafkaWorker (дефект 3, канал Д1)

Симметрия pg-грани (`WorkerHealthPoller` → `IWorkerHealthStore` →
`EtcdSnapshot.WorkerHealth` → `WorkerUnhealthyRule`), kafka-проекция:

- **Модель**: `KafkaSnapshot`
  (`src/AdminPanel.Core/Kafka/KafkaSnapshot.cs`) — новое поле
  `IReadOnlyList<WorkerHealth> WorkerHealth` (модель `WorkerHealth`/
  `WorkerHealthStatus` общая — `src/AdminPanel.Core/WorkerHealth.cs`).
- **Стор**: новый `IKafkaWorkerHealthStore` + реализация
  `src/AdminPanel.Etcd/Workers/KafkaWorkerHealthStore.cs` (паттерн
  `IWorkerHealthStore`/`WorkerHealthStore`: Replace/Current,
  `[InjectAsSingleton]`).
- **Поллер**: расширение `src/AdminPanel.Etcd/Workers/
  WorkerHealthPoller.cs` — `RunOnceAsync` после pg-инстансов пробит
  kafka-инстансы: эндпоинты — `IKafkaSnapshotStore.Current?.
  WorkerEndpoints`, результаты — `IKafkaWorkerHealthStore.Replace`.
  Тот же тик/таймер/конфиг (`AdminPanel:Workers:HealthEnabled`/
  `HealthIntervalSec`/`TimeoutSec`), тот же `HttpClient`
  (`WorkerApiGateway.HttpClientName`); `/healthz` не под `X-Api-Key`
  (`ApiKeyMiddleware` проверяет только `/api`-префикс — ключ не нужен).
- **Refresher**: `src/AdminPanel.Etcd/KafkaSnapshotRefresher.cs` —
  при сборке `KafkaSnapshot` вносить `WorkerHealth` из стора (симметрия
  pg `SnapshotRefresher.cs:156`); в `FailTick` сохранять
  `previous?.WorkerHealth ?? []` (симметрия pg
  `SnapshotRefresher.cs:225` — health-пробы переживают отказ etcd).
- **Алерт**: `src/AdminPanel.Core/Kafka/KafkaAlerting/
  KafkaAlertEngine.cs` — в `Enumerate` новое правило: для каждого
  `next.WorkerHealth` со `Status != Healthy` — алерт
  warning `worker-unhealthy`:
  - id `worker-unhealthy:kafkaworker/<instanceId>`, target
    `kafkaworker/<instanceId>`;
  - message: Degraded → «инстанс KafkaWorker `<id>` нездоров: /healthz
    отвечает не-200 (`<detail>`)»; Unreachable → «…недостижим по URL
    lease-ключа (`<detail>`)»;
  - details: `url`, `checked_unix`; Hint/Remedy — порт текста
    `WorkerUnhealthyRule` с kafka-спецификой RemedyText («смотрите
    docker logs kafkaworker и /healthz напрямую (секции
    etcd-reachable/docker-hosts/loops-alive/snapshot); поднимите
    зависимость (etcd/docker) или перезапустите контейнер воркера»).
  `sinceUnix` работает штатно (ResolveSince по prev.Alerts). Healthy →
  алерта нет, погасший воркер убирает алерт следующим снапшотом.
- **UI/DTO**: не меняются — алерты проявляются в существующих
  счётчиках/лентах (`/api/alerts`, Overview, AlertsPage).

### 3.5. Стенд — e2e-чек 57 (Д3)

Новый `dev-stand/adminpanel/checks/57-kafka-worker-health.sh`
(full-профиль + kafka; после 55-го, перед 90-down). Механика — порт
паттерна `30-failover.sh` (login → `api`/`has_alert`/`wait_alert`/
`wait_no_alert` поверх `/api/alerts`):

1. *Preconditions*: живые ключи `/kafkaworker/api/` (иначе критичный
   `worker-api-unreachable` — чек требует живого стенда); алертов
   `worker-unhealthy:*` нет; запомнить `docker inspect` uptime
   контейнера `as-kafkaworker` (доказательство «без рестарта»).
2. *Act 1 (transient-стимул)*: `docker compose stop etcd`; удерживать
   ~40 c (≥ 2 тиков поллера по 15 c + запас): healthz воркера с хоста
   (`http://localhost:8082/healthz` — порт 8082:8080 в compose) → 503
   (etcd-секция Degraded), docker-health копит FailingStreak (интервал
   HEALTHCHECK 30 c × retries 3 — контейнер НЕ убивается и не
   рестартуется: `restart: unless-stopped` не реагирует на unhealthy).
3. *Assert 2*: `docker compose start etcd`; `/healthz` воркера → 200
   Healthy ≤ 30 c после подъёма etcd (первый успешный тик: ScanInterval
   5 c + ErrorDelayMs), контейнер тот же (uptime не сброшен) — сброс
   sticky-`StatusError` работает без рестарта.
4. *Assert 3*: алерт `worker-unhealthy` c target `kafkaworker/<id>`
   появился в `/api/alerts` (первый успешный kafka-refresher-тик после
   подъёма etcd мерджит Degraded-результат поллера из стора) и **погас**
   ≤ 2 тиков поллера (~35 c) после восстановления healthz.
5. *Assert 4 (эстафета)*: `worker-api-unreachable:kafkaworker` не висит
   (lease истёк за downtime — ключи восстановились через
   `EnsureInstanceKeyAsync` ≤ 15 c + тик); в конце чека — ни одного
   `worker-unhealthy`/`worker-api-unreachable` алерта.

Замечание о порядке событий (почему алерт загорится после подъёма etcd,
а не во время downtime): снапшот панели строится только при успешном
KV-чтении — пока etcd лежит, алерты не пересчитываются; поллер при этом
независим и фиксирует Degraded в стор. Первый тик refresher'а после
подъёма etcd строит снапшот со стор-результатом → алерт загорается;
следующий тик поллера видит уже Healthy → алерт гаснет. Это ровно
семантика «виден ≤ 2 тиков, гаснет после восстановления».

## 4. Фазы

1. **Контракт** (сделано в spec-фазе, arch-first): `arch/16` §7,
   `arch/adminpanel/02` §2.3.2, `arch/adminpanel/03` §4 (см. §8).
2. **Воркер — честный healthz**: сброс `StatusError` в трёх циклах
   (§3.1); catch-all проб и чека (§3.2). Unit-тесты фазы 6.
3. **Воркер — etcd-клиент**: `EtcdConnectCallback` + конфигурация
   handler'а (§3.3).
4. **Панель**: `KafkaSnapshot.WorkerHealth` + стор + расширение поллера
   + refresher-мердж + правило `KafkaAlertEngine` (§3.4).
5. **Стенд**: чек `57-kafka-worker-health.sh` (§3.5).
6. **Тесты** (по фазам 2–5, TDD — сначала красные, см. §6); сборка
   `dotnet build` без ворнингов.

Порядок 2→3→4 свободный по компонентам, но e2e (5) требует всех; чек 57
выполняется на full-стенде после 55-го.

## 5. Ограничения

- **Out of scope** (одной строкой каждое, не расширять):
  Prometheus-метрики (t04); TLS/mTLS HTTP API (t03); правки циклов
  PgWorker (уже честные — паттерн оттуда портируется); публикация
  агрегированного статуса в etcd / расширение heartbeat-ключей
  (отвергнуто Д1); фронтенд-компоненты (алерты видны существующими);
  ConnectCallback для docker-клиента воркера (у него PooledConnection
  Lifetime уже есть, DNS-флейп не наблюдался — unix-socket/local);
  унификация дублей AdminPanel/PgWorker (t08); алертинг при полностью
  легшем etcd (снапшот не строится — панель молчит до восстановления,
  это существующая семантика обоих доменов).
- `/healthz` остаётся composite-чеком всех четырёх регистраций
  (`kafkaworker` + три per-loop) — состав чеков не меняется.
- Docker HEALTHCHECK настроек не меняется (30 s × retries 3
  достаточно: рестарт контейнера по unhealthy не настроен).
- Тексты алертов на русском; id/kind/target форматы стабильны
  (единственный новый id-шаблон `worker-unhealthy:kafkaworker/<id>` —
  симметрия pg-грани).

## 6. Тесты

- **Unit — воркер** (`src/tests/KafkaWorker.UnitTests/`, AAA-комментарии):
  - ReconcileLoop: // Arrange — стаб etcd с одним Failed-тком →
    // Act — успешный тик → // Assert — `StatusError.IsSuccess` true
    (transient-сбой → восстановление → Healthy без рестарта);
    неудачный тик → `StatusError` Failed (как сегодня).
  - SnapshotLoop: успешный `TakeAsync` сбрасывает; не-лидер не трогает.
  - KeepaliveLoop: проход цикла сбрасывает.
  - ServiceProbes: шлюз бросает `HttpRequestException` →
    `EtcdReachableAsync` возвращает `Result.Failed` (не бросок);
    docker-фабрика бросает → per-host Failed / catch-all-запись.
  - KafkaWorkerHealth: любая проба бросает → Degraded с Data-секцией
    `error` (никогда не исключение).
  - Handler-конфигурация: именованный клиент `etcd` — primary handler
    `SocketsHttpHandler`, `PooledConnectionLifetime == 5 мин`,
    `ConnectCallback` назначен (по образцу
    `PgWorker.UnitTests/Docker/DockerEngineTests.cs`);
    `EtcdConnectCallback`: IPv4 сортируется раньше IPv6,
    IP-литерал — без DNS, все адреса недоступны → бросок последнего.
- **Unit — панель** (`src/tests/AdminPanel.UnitTests/`):
  - KafkaAlertEngine: Degraded → warning `worker-unhealthy` с target
    `kafkaworker/<id>`; Unreachable → warning с текстом «недостижим»;
    Healthy → нет алерта; prev с тем же id → `sinceUnix` перенесён.
  - WorkerHealthPoller: kafka-эндпоинты из `IKafkaSnapshotStore`
    пробятся (стаб-`HttpMessageHandler`: 200/503/исключение →
    Healthy/Degraded/Unreachable), результаты — в
    `IKafkaWorkerHealthStore`.
  - KafkaSnapshotRefresher: `WorkerHealth` вносится из стора; FailTick
    сохраняет previous.
- **E2e стенд**: `57-kafka-worker-health.sh` (§3.5) на full-стенде —
  главный прогон приёмки; 20/30/40/50/55 без регрессий (чек 57
  возвращает стенд в согласованное состояние).
- TreatWarningsAsErrors=true — весь новый код без ворнингов.

## 7. Критерии приёмки

1. **Сброс sticky**: unit — ошибка тика → успешный тик →
   `StatusError` Success; e2e — после ~40 с лежащего etcd `/healthz`
   воркера возвращает 200 ≤ 30 c после подъёма, контейнер не
   рестартован (uptime тот же).
2. **Структура вместо исключения**: unit — броски шлюза/фабрики/проб →
   Degraded/Failed с данными; в логах живого прогона нет новых
   `DefaultHealthCheckService[103]` от чеков воркера.
3. **DNS**: unit — handler сконфигурирован (PooledConnectionLifetime +
   ConnectCallback), IPv4-first порядок; e2e — после рестарта
   etcd-контейнера тики воркера восстанавливаются без вечного
   «Name or service not known» (косвенно: критерий 1).
4. **Единая правда**: e2e чек 57 — `worker-unhealthy:kafkaworker/<id>`
   загорается ≤ 2 тиков поллера после degraded-окна и гаснет ≤ 35 c
   после восстановления healthz; docker-health и панель показывают
   согласованную картину (красный transient → оба зелёные).
5. **Эстафета доступности**: `worker-api-unreachable:kafkaworker`
   отрабатывает как раньше (нет ключей → critical; ключи вернулись →
   погас) — e2e п.5 чека 57.
6. `dotnet build` без ворнингов; все unit-тесты (воркер + панель)
   зелёные; чеки 00→…→57 на full-стенде зелёные, в финале нет
   `worker-*` алертов.
7. Канон синхронен: `arch/16` §7, `arch/adminpanel/02` §2.3.2,
   `arch/adminpanel/03` §4 соответствуют коду (сделано arch-first).

## 8. Изменения arch/ (сделано в spec-фазе, arch-first)

- `arch/16-kafkaworker.md` §7 «Наблюдаемость»: добавлен канон честного
  health — «healthz = последнее состояние цикла» (сброс StatusError,
  живой-Ф7), «чек всегда отдаёт структуру» (catch-all проб), etcd-клиент
  на SocketsHttpHandler (PooledConnectionLifetime + IPv4-first
  ConnectCallback против Docker embedded DNS), абзац «единая правда для
  панели» (опрос /healthz по URL из `/kafkaworker/api/<id>`, алерт
  `worker-unhealthy` ≤ 2 тиков).
- `arch/adminpanel/02-etcd-contract.md` §2.3.2: дописан health-опрос
  KafkaWorker панелью — тот же поллер и интервал
  (`AdminPanel:Workers:HealthIntervalSec`), результат
  `WorkerHealth[]` в `KafkaSnapshot.WorkerHealth`, warning-алерт
  `worker-unhealthy`, `/healthz` не под `X-Api-Key`, критерий
  «≤ 2 тиков / гашение / не расходиться с docker-health».
- `arch/adminpanel/03-panels.md` §4: строка каталога `worker-unhealthy`
  расширена на `/kafkaworker/api/<id>` (t09) — kind один на оба воркера,
  target различает.

## 9. Принятые решения (сводка, с обоснованиями)

| # | Решение | Обоснование |
|---|---|---|
| Д1 | Канал правды — панель пробит `/healthz` (heartbeat-ключи не расширяются) | выбор пользователя; прецедент PgWorker уже в проде (`WorkerHealthPoller`); нулевой новый etcd-контракт; панель видит ровно то же, что docker-healthcheck |
| Д2 | DNS-фикс полный: catch-all + PooledConnectionLifetime + IPv4-first ConnectCallback | выбор пользователя; закрывает и застарелый пул после пересоздания etcd-контейнера, и сам A/AAAA-флейп против Docker embedded DNS |
| Д3 | Kind `worker-unhealthy` переиспользуется (без `worker-degraded`) | единый каталог 03 §4 уже описывает kind для обоих воркеров; pg-прецедент; меньше новых сущностей |
| Д4 | E2e — новый чек 57 с stop/start etcd ~40 c как transient-стимулом | выбор пользователя; roadmap явно называет failover etcd-контейнера стимулом; окно ≥ 2 тиков поллера детерминированно ловит алерт; HEALTHCHECK 30 s × retries 3 гарантирует «контейнер не убит» |
| Д5 | Сброс StatusError — дословный порт «живой-Ф7» из PgWorker-циклов | требование roadmap («паттерн сброса ошибки тика взять из PgWorker-циклов после adopt-repair»); один канон поведения на оба воркера |
| Д6 | Поллер один на оба воркера (расширение `WorkerHealthPoller`), стор — отдельный kafka | один таймер/конфиг/HttpClient; стор отдельный, т.к. снапшоты доменов независимы (KafkaSnapshot ≠ EtcdSnapshot) |

## 10. Риски и митигации

- **40-с stop etcd на полном стенде бьёт по всем сервисам** (панель,
  PgWorker, эмуляторы): всё self-healing по построению (контракт
  takeover ≤ TTL 15 c + тик); чек завершается только после возврата
  чистого состояния (нет `worker-*` алертов) — следующему чеку/прогону
  стенд передаётся согласованным.
- **Гонка «поллер тикнул уже после восстановления»** — алерт может
  загореться всего на один тик или не загореться вовсе (если downtime
  короче тика поллера): для e2e окно 40 c > 2 тиков снимает
  нестабильность; в проде семантика остаётся честной («алерт = был
  реально больной последний опрос»).
- **IPv6-only окружения**: IPv4-first не теряет IPv6 — сортировка, а не
  фильтр; недоступность IPv4 → попытка IPv6 тем же коннектом.
- **Кастомный ConnectCallback** — точка отказа сетевого стека: покрыт
  unit-тестами (порядок, литералы, отказ всех адресов), поведение при
  исключении идентично дефолтному (бросок → `Result.Failed` шлюза).
- **Sticky-сброс скрывает серию transient-сбоев**: единичный успешный
  тик гасит ошибку — осознанно (живой-Ф7: healthz про «жив ли сейчас»);
  серийность видна в логах циклов (LogError каждого неудачного тика) и
  degraded-секциях probes во время сбоя.
