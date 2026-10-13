# t37-owns3-replication-ping — активная проба репликационных линков ownS3

Спецификация задачи dev-flow. Постановка — тег `t37-owns3-replication-ping`
в [`arch/roadmap/owns3.md`](../../../arch/roadmap/owns3.md); канон механики —
[`arch/owns3/07-replication.md`](../../../arch/owns3/07-replication.md);
потребитель — зонд `t33-s3-sync-connectivity` трека s3
([`arch/roadmap/s3.md`](../../../arch/roadmap/s3.md)).

## 1. Цель

Дать владельцу/зонду детектор живости репликационных линков mesh, которого
сегодня нет:

- `online` в `GET /admin/replication/status` — **пассивный кэш** исходов
  notifier (глава 07 §8/§10): без исходящего трафика уведомлений поле не
  обновляется, «тихий» линк неотличим от мёртвого.
- Клиентские PUT-пробы связность ownS3 **не детектируют**: путь записи не
  содержит синхронных взаимодействий с соседями (принцип 1, §1) — PUT всегда
  отвечает за локальную фиксацию, 0 с.
- `/healthz` недостаточен: нода, изолированная от всех целей репликации,
  отвечает health (её локальный том жив).

Решение — два элемента:

1. **Грань `GET /replication/ping`** на replication-грани приёмника (§6):
   лёгкий подписанный запрос; приёмник ничего не пишет и не применяет.
2. **Админ-триггер `POST /admin/replication/probe`** (§2, рядом со
   status/reconcile): нода синхронно пингует всех (или одного) соседей
   исходящим репликационным клиентом и возвращает per-target результат.

Семантика ping-ока — «**канал жив**»: отдельно от доставки (лаг/очередь
статуса) и сверки данных (sync-report §10); `link_online` и курсоры по
итогам probe **не меняются**.

## 2. Принципы

1. **Arch-first**: контракт сначала меняется в `arch/owns3/` (фаза 0), затем
   код зеркалит его. Источник истины один.
2. **Тот же конвейер, что у notifier**: probe исходит тем же репликационным
   транспортом (`IReplicationTransport`/`ReplicationHttpClient`), теми же
   per-соседними кредами `TargetRecord` и тем же маршрутом
   (advertised endpoint соседа, SigV4-подпись), но **мимо outbox** — probe
   не читает журнал, не двигает курсоры, не порождает событий.
3. **Ping бесплатен для приёмника**: обработка `GET /replication/ping` не
   касается дедупа, fetch-очередей, курсоров, tombstones, счётчиков
   применения — состояние приёмника после любого числа ping-ов байт-в-байт
   то же (проверяется тестом).
4. **Путь записи клиента не затрагивается** (принцип 1 §1): probe —
   админ-операция по требованию, никаких новых синхронных взаимодействий
   в пути 21 операции.
5. **Только по требованию**: периодический автопинг/фон-цикл НЕ вводится —
   периодика принадлежит зонду t33 (потребитель). Retry-логики у probe нет:
   один запрос — один исход.
6. **Existing-регресс неприкосновенен**: без соседей поведение байт-в-байт
   как single — события не ставятся, каталог `replication/` не создаётся,
   грань `/replication/*` отвечает 403/404 по §6, метрики репликации не
   экспортируются; `POST /admin/replication/probe` при неактивной
   репликации отвечает осмысленным «соседей нет» (см. §4.2), не ошибкой.

## 3. Решения (согласованы с пользователем)

| Развилка | Решение |
|---|---|
| `reachable` при не-200 HTTP-ответе соседа (403/400) | **Строго 200**: `reachable=true` только при валидном 200 ping-грани; 403/400/таймаут/обрыв = `false` — репликационный линк не работает, для зонда t33 тот же offline |
| Метод и тело ping | **GET `/replication/ping` → 200, пустое тело** (минимальный лёгкий запрос; SigV4-подпись GET с пустым payload-hash — механика транспорта уже есть) |
| Выбор одного соседа в probe | **query-параметр `?target=<name>`**: без параметра — все соседи; неизвестное имя — **400** |
| Таймаут | **env `OWNS3_REPLICATION_PROBE_TIMEOUT_MS`** (секция `OwnS3:Replication:ProbeTimeoutMs`), дефолт **2000** — per-target, короткий |
| Параллельность пингов | **Параллельно** (`Task.WhenAll` с per-target таймаутом): задержка ответа probe = max, не sum |
| Наблюдаемость (§10) | **Без расширения**: результат только в JSON-ответе вызывающему; ping-запросы автоматически считаются существующей `ownS3_requests_total{operation=replication}`; новых серий/лог-событий probe не вводит |
| Форма ответа probe | `{"active": <bool>, "targets": [{"name", "reachable", "rttMs"?}]}` — симметрия с `GET status` (`active`) и `PUT targets` (`targets`); неактивная репликация → `{"active": false, "targets": []}` |

`rttMs` (long, миллисекунды) присутствует **при любом полученном HTTP-ответе**
(включая не-200 — диагностика «отвечает, но не пускает»); при таймауте/обрыве
транспорта поле отсутствует.

## 4. Структура/компоненты

### 4.1. Грань `GET /replication/ping` (приёмник)

Полный существующий конвейер replication-грани без изъятий:

- аутентификация SigV4 (заголовочная подпись, как у всей грани §6);
- политика ключа строго `replication` (глава 05 §3), прочие — **403
  `AccessDenied`** (A13-изоляция);
- accessKey подписи обязан числиться соседом в targets приёмника (резолв
  `sourceName`), иначе **403** — как у всей грани;
- **успех: 200, тело отсутствует**; приёмник ничего не пишет и не применяет;
- `POST /replication/ping` и прочие методы — **400 `InvalidArgument`**
  («Unknown replication resource») по существующему правилу грани.

### 4.2. Админ-триггер `POST /admin/replication/probe` (источник)

- Права: admin/root тем же конвейером админ-грани (глава 05 §7); replication
  и прочие политики — **403**; неизвестный админ-ресурс — 400.
- Параметры: без `?target` — пинг **всех** соседей снапшота targets;
  `?target=<name>` — ровно одного; имя не из состава — **400** с пояснением
  в JSON (`{"error": ...}`).
- Исполнение: **синхронный** ответ (не 202/фон — в отличие от reconcile):
  параллельные ping-запросы `GET /replication/ping` исходящим транспортом
  (креды/маршрут notifier'а), per-target таймаут
  `OwnS3:Replication:ProbeTimeoutMs` (linked CTS поверх общего таймаута
  HttpClient 10 с).
- Ответ **200**:

```json
{"active": true,
 "targets": [{"name": "peer-a", "reachable": true, "rttMs": 4},
             {"name": "peer-b", "reachable": false}]}
```

- Неактивная репликация (соседей нет): **200** `{"active": false,
  "targets": []}` — осмысленный ответ «соседей нет», не ошибка.
- Probe не меняет `link_online`, курсоры, outbox, fetch-очереди, счётчики.

### 4.3. Кодовая раскладка

| Артефакт | Изменение |
|---|---|
| `src/OwnS3.Replication/Transport/IReplicationTransport.cs` | `Task<PingResult> PingAsync(TargetRecord target, TimeSpan timeout, CancellationToken ct)`; `record PingResult(bool Reachable, long? RttMs)` |
| `src/OwnS3.Replication/Transport/ReplicationHttpClient.cs` | Реализация: `GET /replication/ping`, SigV4 с пустым payload-hash, per-вызов `CancellationTokenSource.CreateLinkedTokenSource + CancelAfter(timeout)`; rtt — от старта запроса до статус-строки ответа (`ResponseHeadersRead`); 200 → `(true, rtt)`; не-200 → `(false, rtt)`; транспортная ошибка/таймаут → `(false, null)`; отмена внешнего ct — проброс (`OperationCanceledException` при `ct.IsCancellationRequested`) |
| `src/OwnS3.Replication/ReplicationOptions.cs` | `int ProbeTimeoutMs { get; set; } = 2000` (секция `OwnS3:Replication`) |
| `src/OwnS3.Replication/ReplicationProber.cs` (новый) | Владелец логики probe (тестируем без хоста — по прецеденту `ReplicationMaintenance`/`ReconcileRunner`): вход — `ReplicationTargets`, `IReplicationTransport`, `ReplicationOptions`; `Task<ProbeReport?> ProbeAsync(string? targetName, CancellationToken ct)` с записями `record ProbeReport(bool Active, IReadOnlyList<ProbeOutcome> Targets)` и `record ProbeOutcome(string Name, bool Reachable, long? RttMs)`; **null-возврат — явный маркер «имя не из состава»** для 400-ответа хендлера (прецедент nullable-возврата в кодовой базе — `SendNotificationsAsync → long?`); фильтр по `targetName`; пинги параллельно, каждая ветка гасит свои исключения |
| `src/OwnS3.App/Handlers/ReplicationHandlers.cs` | `HandlePingAsync(HttpContext)` — 200, пустое тело, ничего больше |
| `src/OwnS3.App/Handlers/ReplicationAdminHandlers.cs` | `HandleProbeAsync(HttpContext)` — `?target=`, вызов `ReplicationProber` (null — 400), JSON `{"active", "targets":[{name, reachable, rttMs?}]}`; неизвестный target — 400 |
| `src/OwnS3.App/Pipeline/S3Middleware.cs` | Replication-ветка: маршрут `GET /replication/ping` → `HandlePingAsync` (после auth/политики/резолва sourceName — конвейер единый). Admin-ветка: `/admin/replication/probe` в список допустимых ресурсов + диспетчеризация `POST` → `HandleProbeAsync` |
| `src/OwnS3.App/Program.cs` | env-скаляр `OWNS3_REPLICATION_PROBE_TIMEOUT_MS` → `OwnS3:Replication:ProbeTimeoutMs`; DI `ReplicationProber` (singleton, зависит от targets/transport/options) |

### 4.4. Arch-first правки (фаза 0, до кода)

| Документ | Правка |
|---|---|
| `arch/owns3/07-replication.md` §2 | В перечень админ-грани (рядом со status/reconcile) добавить `POST /admin/replication/probe`: активная проба линков — пинг всех (или `?target=`) соседей параллельно, per-target таймаут, синхронный ответ `{active, targets:[{name, reachable, rttMs?}]}`; reachable строго по 200; без соседей — `{active:false, targets:[]}` |
| `arch/owns3/07-replication.md` §6 | В таблицу операций replication-грани добавить строку «ping»: `GET /replication/ping` → 200 без тела; тот же SigV4-конвейер/политика/резолв соседа; приёмник ничего не пишет и не применяет (дедуп/fetch-очереди/курсоры не затрагиваются); прочие методы — 400 |
| `arch/owns3/07-replication.md` §11 | Env-таблица: `OWNS3_REPLICATION_PROBE_TIMEOUT_MS` — per-target таймаут ping-пробы (§2), дефолт **2000** |
| `arch/owns3/05-access-config.md` §4 | Зеркало env-строки (`OwnS3:Replication:ProbeTimeoutMs`) |
| `arch/owns3/05-access-config.md` §7 | В перечень эндпоинтов Mesh-репликации админ-грани добавить `POST /admin/replication/probe` со ссылкой на 07 §2 |

§10 главы 07 (наблюдаемость) **не меняется** — расширения метрик/логов
probe нет; ping-трафик учитывается существующей
`ownS3_requests_total{operation=replication}` автоматически.

## 5. Фазы

### Фаза 0 — arch-first

Правки по таблице §4.4; канон описывает контракт до его появления в коде.

### Фаза 1 — ядро репликации (`src/OwnS3.Replication`)

1. `PingResult` + `PingAsync` в интерфейсе и реализация
   `ReplicationHttpClient` (подпись GET, per-вызов таймаут, замер rtt).
2. `ProbeTimeoutMs` в `ReplicationOptions`.
3. `ReplicationProber` — параллельный проход по снапшоту targets, фильтр
   одного, гашение исключений per-target.
4. Юнит-тесты (`src/tests/OwnS3.UnitTests/Replication/ReplicationProberTests.cs`,
   фейк `IReplicationTransport` по прецедентам `FakeTransport`):
   - все соседи reachable при 200 от фейка; rttMs прокидывается;
   - не-200 от соседа → `reachable=false`, rttMs присутствует;
   - исключение транспорта → `reachable=false`, rttMs отсутствует;
   - `targetName`-фильтр: ровно один исход; неизвестное имя — маркер 400;
   - пустой состав → `active=false`, пустой список;
   - `ReplicationOptionsTests`: дефолт `ProbeTimeoutMs = 2000`, привязка
     env-скаляра.

### Фаза 2 — грани (`src/OwnS3.App`)

1. `HandlePingAsync`, `HandleProbeAsync`, маршруты в `S3Middleware`,
   env-скаляр и DI в `Program.cs` — по §4.3.
2. Интеграционные сценарии (`src/tests/OwnS3.IntegrationTests/Api/`):
   - `ReplicationScenarios`: `GET /replication/ping` с валидной подписью
     replication-соседа → 200 пустое; read-write/admin-ключ → 403;
     replication-ключ, не числящийся соседом → 403; `POST /replication/ping`
     → 400; неизвестный путь грани — прежние 400 (регресс маршрутизации);
   - `ReplicationAdminScenarios`: `POST /admin/replication/probe` без
     соседей → 200 `{"active":false,"targets":[]}`; права (не-admin → 403);
     `?target=неизвестный` → 400; с недостижимым соседом в составе
     (фикстурный `127.0.0.1:9` и заниженный `ProbeTimeoutMs` в конфиге
     фикстуры — быстрый тест-таймаут) → `reachable=false` за таймаут;
   - приёмник-чистота ping: серия ping-запросов к узлу-приёмнику —
     `GET /admin/replication/status` до/после: счётчики, cursors, fetch-очереди
     неизменны (принцип 3).

### Фаза 3 — E2E mesh (`src/tests/OwnS3.IntegrationTests/E2e/Replication/`)

Новый `ProbeScenarios.cs` поверх существующего
`OwnS3MeshE2eEnvironment` (контур/изоляция/teardown — каноны
`docs/e2e-isolation.md`, `docs/e2e-launch.md`):

1. Живой mesh (2–3 ноды): probe каждой ноды — все соседи `reachable=true`,
   `rttMs` присутствует; `?target=` одиночный — ровно один исход.
2. Остановка контейнера соседа: probe живой ноды → сосед `reachable=false`
   (за per-target таймаут, без rttMs при обрыве), возврат контейнера —
   снова `true` (канал жив, доставка не оценивается).
3. Недостижимый сосед в составе (PUT targets на закрытый порт): probe →
   `reachable=false` в пределах таймаута дефолта.
4. `?target=неизвестный` → 400; без соседей → `{"active":false,"targets":[]}`.
5. Симметрия матрицы: probe всех нод живого mesh — каждая пара ok в обе
   стороны (модель потребления t33).

### Фаза 4 — полные прогоны

Юниты → интеграции → E2E на свежем Release (`PGW_TEST_DOCKER=1`,
инкрементальная сборка E2eFixture); после каждой серии — зачистка
контейнеров/сетей по канонам AGENTS.md; телеметрия прогона —
`docs/e2e-launch.md` (`[PHASE]`-метки, артефакты teardown).

## 6. Ограничения и границы

- **Вне задачи**: зонд t33 (потребитель probe, arch/23 §3 / arch/24 §5F —
  правки той задачей), периодический автопинг, retry/бэкофф probe, метрики
  и лог-события probe, обновление `link_online` по итогам probe, изменения
  матрицы прав (05 §3) и полей `GET status`.
- Ping не является клиентской операцией: событий репликации не ставит ни
  источник (probe), ни приёмник (обработка ping) — анти-петля не затронута.
- Existing-регресс (§12 главы 07) сохранён буквально: без соседей
  `/replication/ping` недоступен тем же 403/404-конвейером грани, каталог
  `replication/` не создаётся, метрики репликации не экспортируются.
- Multi-host: probe адресует advertised endpoint соседя — сетевая модель
  канона §2, ничего нового.
- Креды probe — per-соседние из targets.json; отдельных ключей не вводится.

## 7. Критерии приёмки

1. Arch-правки (§4.4) закоммичены **раньше** кодовых (arch-first) и
   соответствуют реализованным контрактам дословно.
2. `GET /replication/ping`: 200-пустое с валидной replication-подписью
   соседа; 403 для прочих политик и не-соседей; прочие методы — 400; на
   приёмнике не изменяется ни одно состояние (тест приёмник-чистоты зелёный).
3. `POST /admin/replication/probe`: `{"active","targets":[{name,
   reachable, rttMs?}]}`; `reachable=true` строго при 200; `rttMs` при любом
   HTTP-ответе; таймаут/обрыв — без `rttMs`; `?target=` фильтрует одного,
   неизвестное имя — 400; без соседей — `{"active":false,"targets":[]}`;
   права admin/root.
4. Per-target таймаут — `OWNS3_REPLICATION_PROBE_TIMEOUT_MS` (дефолт
   2000); пинги параллельны (ответ probe ≈ max per-target, не sum).
5. Серии зелёные: юниты (`OwnS3.UnitTests`, включая новые
   `ReplicationProberTests`) и Api-интеграции (`OwnS3.IntegrationTests`,
   включая ping/probe-сценарии) — в `-c Debug`; docker-E2E серия
   E2e Replication (включая `ProbeScenarios`) — на свежем Release
   (`PGW_TEST_DOCKER=1`). «Свежий Release» относится **только** к
   docker-E2E серии (канон AGENTS.md: Release-гейт для docker-E2E задач,
   трогающих код воркеров); между сериями — зачистка контуров
   (ассерты чистоты teardown).
6. Existing-регресс подтверждён: прогоны сценариев без соседей
   (AccessScenarios/RoutingScenarios/MetricsHealthScenarios) unchanged.
7. Мерж-гейт задачи: код трогает `src/OwnS3.*` — docker-E2E на свежем
   Release обязателен (правило AGENTS.md); полный прогон серии E2e
   Replication.
