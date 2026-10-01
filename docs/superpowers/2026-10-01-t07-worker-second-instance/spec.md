# t07-worker-second-instance — второй инстанс воркеров в деплое

- **Roadmap**: [`arch/roadmap/reliability.md`](../../../arch/roadmap/reliability.md), трек сквозной надёжности, приоритет P2, целевая характеристика **R** (быстрая самовосстанавливаемость).
- **Формулировка**: «код полностью готов (lease-клэймы, takeover ≤15 с, PortAllocLock — доказано E2E AC3/mid-add/mid-move), но деплой поднимает по одному контейнеру — SPOF надзора (датаплейн при этом живёт сам)».
- **Решения пользователя (2026-10-01)**: охват — все три воркера (PgWorker, KafkaWorker, ValkeyWorker); включение — всегда по 2 инстанса, без профилей-переключателей (default); dev-стенд — тоже по 2; приёмка — чек kill→takeover на стенде + docker-E2E сценарий.

## 1. Контекст и цель

### 1.1. Проблема

Координационный слой воркеров мультинстансен по коду: пер-кластерные
lease-клэймы (`/pgworker/claims/<C>`), глобальный лидер снапшотов, глобальный
`PortAllocLock`, takeover ≤ TTL 15 с — всё доказано E2E (AC3, mid-add,
mid-move). Но `deploy/docker-compose.yml` поднимает по одному контейнеру
каждого воркера: смерть контейнера = отсутствие надзора до ручного
вмешательства (docker-политика `unless-stopped` спасает только от падения
процесса, не от отказа/обслуживания контейнера или зависания без исключения).
Датаплейн (PG-кластеры, Kafka, Valkey) при этом живёт сам — SPOF именно
надзора: provisioning/rebuild/ротации/бэкапы замирают.

### 1.2. Цель

Деплой-канон и dev-стенд поднимают по **два инстанса каждого воркера**:
смерть одного контейнера воркера не прерывает надзор — клэймы мигрируют
второму за ≤15 с (существующий механизм, без изменений кода). Панель и
Prometheus видят оба инстанса каждого воркера.

### 1.3. НЕ-цели (границы)

- Не меняем код `src/PgWorker.*`, `src/KafkaWorker.*`, `src/ValkeyWorker.*`,
  `src/Shared.*`, `src/AdminPanel.*` — координация, дискавери, панель,
  серт-сервис уже готовы к N инстансам (см. §2). Единственные новые
  код-артефакты — тест docker-E2E сценария (§4.5). Если в ходе реализации
  выяснится, что для второго инстанса нужен код воркера/панели — СТОП и
  пересогласование с пользователем (это будет значить, что «код полностью
  готов» опровергнут).
- Не делаем параметризуемое N (генераторы инстансов, scale-скрипты): ровно
  два, как сформулировано в roadmap.
- Не трогаем etcd-контракт: ключи, payload, lease-семантика — без изменений.
- Не решаем HA etcd (`t09`), выгрузку снапшотов (`t08`) — соседние задачи
  трека.

## 2. Что уже готово (доказано исследованием, не трогаем)

| Механизм | Где | Гарантия |
|---|---|---|
| InstanceId = случайный GUID при старте | `src/Shared.Etcd/Coordination/ClaimStore.cs` (`InstanceId { get; } = Guid…`) | два контейнера одного образа — два разных инстанса; никакой env-идентичности (`PGW_INSTANCE_ID` и т.п.) НЕ нужно и не вводим |
| Lease TTL 15 с, keepalive 5 с, takeover | `ClaimStore` | смерть инстанса гасит `/claims/*`, `/leader`, `/api/<id>`, `/instances/<id>` ≤15 с; другой инстанс захватывает |
| Глобальный portalloc-мьютекс | `src/Shared.Etcd/Coordination/PortAllocLock.cs` | кросс-инстансные гонки довыделения портов закрыты |
| Дискавери API | ключи `<prefix>/api/<InstanceId>` (lease) | каждый инстанс публикует свой URL; панель резолвит список |
| Панель: failover + broadcast | `src/AdminPanel.Etcd/Workers/WorkerApiGateway.cs` | запросы — по списку endpoints с сортировкой по InstanceId; `POST /api/restart` — broadcast на все инстансы |
| Панель: healthz-опрос всех | `src/AdminPanel.Etcd/Workers/WorkerHealthPoller.cs` | тик пробит ВСЕ живые endpoints |
| Алерт worker-api-unreachable | `src/AdminPanel.Core/Alerting/Rules/WorkerApiUnreachableRule.cs` | срабатывает только при НУЛЕ живых ключей — один живой инстанс алерта не даёт |
| Серт API: SAN по хостам | `WorkersCommands.LiveHosts` (Distinct по хостам URL) | второй инстанс на том же хосте (другой порт) SAN не меняет |
| Канон мультинстансности | `arch/14-pgworker.md` §2 («Масштабирование — N реплик: координацию разбирает etcd»), `arch/16` §2.1, `arch/21` §2 («несколько инстансов работают одновременно») | контракт уже декларирует мультинстансность |
| Takeover интеграционно | E2E `Scale_TakeoverMidAdd_…`, `E2eScenarios` (AC3/mid-add/mid-move) | на хост-процессах; docker-E2E добавляем этой задачей (§4.5) |

**Реальные препятствия — только деплой-слой** (`deploy/docker-compose.yml`,
стендовые compose, чеки, Prometheus file_sd): фиксированные хост-порты
(8080/8081/8082), один общий `AdvertiseUrl` на сервис, один таргет
Prometheus, чеки, рассчитанные на один инстанс.

## 3. Принципы

1. **Максимальная надёжность без переключателей**: вторые инстансы —
   обычные сервисы compose, `docker compose up -d` сразу даёт 2×3
   контейнеров. Никаких профилей `ha`/`redundancy` — SPOF устраняется
   дефолтом, а не опцией.
2. **Уникальность публикации**: каждый инстанс обязан иметь уникальный
   хост-порт публикации и уникальный `Api:AdvertiseUrl` (панель пробит по
   URL из `/api/<id>`; одинаковые URL у двух инстансов = оба ключа мёртввы
   при смерти контейнера-владельца порта).
3. **Общие per-install ресурсы на оба инстанса**: docker.sock, TLS-тома
   (`pgw-api-tls` и аналоги, `:ro`), **общий том снапшотов** (лидерство
   снапшотов мигрирует между инстансами — у каждого свой том = потеря
   снапшотов при миграции лидера). Дублировать тома НЕЛЬЗЯ.
4. **Никаких новых секретов**: второй инстанс наследует все env-секреты
   первого (per-install секреты установки одни на установку).
5. **arch-first**: сначала дополнения в `arch/` (рецепт поставки ≥2
   инстансов), затем compose/стенд/чеки/тест.
6. **Каноны проекта**: воркеры всегда в докере; порты тестов динамические;
   E2E полностью чистит за собой; etcd один; язык доков — русский.

## 4. Структура и компоненты

### 4.1. arch/ — дополнения рецепта поставки (первая фаза, только документация)

Контракт etcd и интерфейсы НЕ меняются. Дополняем абзацы «сам воркер —
контейнер…» каноном поставки второго инстанса:

- **arch/14-pgworker.md** §2 (абзац «Сам PgWorker — контейнер с
  примонтированным `/var/run/docker.sock`… Масштабирование — N реплик»):
  дополнить — деплой-рецепт (`deploy/docker-compose.yml`) поднимает
  **два** инстанса по умолчанию; каждый инстанс: уникальный хост-порт
  публикации API + уникальный `Api:AdvertiseUrl`; общий volume снапшотов и
  общий TLS-том на оба; уникальность инстанса — случайный `InstanceId`
  (координация §3).
- **arch/16-kafkaworker.md** §2.1 (абзац «Сам воркер — контейнер с
  `docker.sock`… поставляется через `deploy/docker-compose.yml`»): то же
  дополнение (порты/advertise/общие тома; `kfw-snapshots`, `kfw-api-tls`).
- **arch/21-valkeyworker.md** §2 (абзац «Сам воркер — контейнер с
  `docker.sock`, поставляется через `deploy/docker-compose.yml`»): то же
  дополнение (`vw-snapshots`, `vw-api-tls`).

### 4.2. deploy/docker-compose.yml + deploy/.env.example

**Новый ряд хост-портов** (продолжение канона 8080 pgw / 8081 kfw /
8082 vwk):

| Сервис | Хост-порт (env, default) | AdvertiseUrl |
|---|---|---|
| `pgworker` | `${PGW_API_HOST_PORT:-8080}` (как сейчас) | `https://host.docker.internal:${PGW_API_HOST_PORT:-8080}` |
| `pgworker-2` | `${PGW_API_HOST_PORT2:-8083}` | `https://host.docker.internal:${PGW_API_HOST_PORT2:-8083}` |
| `kafkaworker` | `8081` (как сейчас) | как сейчас |
| `kafkaworker-2` | `${KFW_API_HOST_PORT2:-8084}` | `https://host.docker.internal:${KFW_API_HOST_PORT2:-8084}` |
| `valkeyworker` | `${VWK_API_HOST_PORT:-8082}` (как сейчас) | как сейчас |
| `valkeyworker-2` | `${VWK_API_HOST_PORT2:-8085}` | `https://host.docker.internal:${VWK_API_HOST_PORT2:-8085}` |

**Правила реализации**:

- Дублирование env-наборов устраняем YAML-анкорами (`x-pgworker-base:
  &pgworker-base {…}` — build/volumes/extra_hosts/environment), второй
  сервис наследует (`<< : *pgworker-base`) и переопределяет ТОЛЬКО порты
  публикации и AdvertiseUrl. Тома и image общие: `pgworker-2` монтирует те
  же `pgw-snapshots` и `pgw-api-tls:ro`.
- `container_name` НЕ задаём (compose-проект даёт `deploy-pgworker-2-1` и
  т.п. — уникально).
- HEALTHCHECK в образах уже на внутренний `:8080` (mTLS, `/tls`) —
  инвариантен к хост-порту, не трогаем.
- Секреты (`PGW_PG_*`, `PGW_BUCKET_*`), etcd-endpoint, `AdvertisedHost`,
  pgtune, backups — идентичны первому инстансу (наследуются анкором).
- `deploy/.env.example`: секция «вторые инстансы» — `PGW_API_HOST_PORT2`,
  `KFW_API_HOST_PORT2`, `VWK_API_HOST_PORT2` (закомментированные
  defaults 8083/8084/8085 с пояснением ряда) + примечание, что advertise
  вторых инстансов следует за их портами автоматически (compose-дефолт).
- Сервис `pgworker-backup` (профиль build) не трогаем.

### 4.3. Dev-стенд (вторые инстансы + наблюдение)

**Стендовые compose** (`dev-stand/adminpanel/docker-compose.yml`, профили
kafka/valkey — там живут стендовые kfw/vwk):

- `kafkaworker-2`: тот же образ/env, `container_name: as-kafkaworker-2`,
  тот же профиль `kafka`, общие тома `kfw-snapshots` + `../../deploy/tls:ro`,
  `KafkaWorker__Api__AdvertiseUrl: https://kafkaworker-2:8080`
  (compose-DNS, как у первого), **без хост-публикации порта** (панель и
  Prometheus ходят по compose-DNS из сети стенда; хост-порт 8082 остаётся
  у первого для curl-чеков).
- `valkeyworker-2`: симметрично — `container_name: as-valkeyworker-2`,
  `ValkeyWorker__Api__AdvertiseUrl: https://valkeyworker-2:8080`, без
  хост-публикации.
- PgWorker-второй в стенд не входит отдельным сервисом: стенд поднимает
  pgworker из `deploy/` — 00-up.sh поднимает ОБА (см. ниже).

**00-up.sh** (`dev-stand/adminpanel/checks/00-up.sh`):

- Подъём pgworker: `docker compose … up -d --build --force-recreate pgworker
  pgworker-2` (обе записи в ретрай-цикле); синхронизация в `.env` — теперь и
  `PGW_API_HOST_PORT2` (ряд `PGW_API_HOST_PORT`).
- Healthz-чеки: оба инстанса pgworker (`:${PGW_API_HOST_PORT}` и
  `:${PGW_API_HOST_PORT2}`); живость kfw/vwk — усилить существующие
  prefix-проверки `/kafkaworker/api/`, `/valkeyworker/api/` до
  **количества ≥ 2 ключей** (два инстанса = два lease-ключа).
- Prometheus file_sd: `pgworker-targets.json` — оба таргета
  (`host.docker.internal:8080`, `host.docker.internal:8083` при defaults).
- `90-down.sh`: аудит, что сносит всё (deploy-проект гасит pgworker-2 —
  `docker compose down` в deploy уже сносит все сервисы проекта; проверить,
  что нигде нет перечисления сервисов по одному).

**Prometheus** (`dev-stand/adminpanel/metrics/prometheus/prometheus.yml`):

- job `pgworker` — file_sd уже переписывается 00-up.sh (2 таргета, выше).
- job `kafkaworker` static_configs → `["kafkaworker:8080",
  "kafkaworker-2:8080"]`; job `valkeyworker` → `["valkeyworker:8080",
  "valkeyworker-2:8080"]`.

**Аудит существующих чеков на однопоточность**: чеки, адресующие воркеров
по одному порту/имени (10-smoke-api, 57-kafka-worker-health,
66-kafka-worker-churn, 70-worker-cert и др.), проверяются на предмет
допущения «инстанс один»: хост-порт первого инстанса остаётся валиден для
чинов (первый никуда не девается), поэтому ожидание — БЕЗ правок по умолчанию;
меняем только те чеки, где ассерт семантически противоречит двум инстансам
(например, счёт ключей/инстансов). Список — в plan-фазе.

### 4.4. Чек kill→takeover на стенде (приёмка R)

Новый чек `dev-stand/adminpanel/checks/35-worker-second-instance.sh`
(номер — первый свободный в ряду 30-failover/40-live-probes; уточняется в
plan-фазе). Сценарий для каждого воркера (pgw — deploy-контейнер, kfw/vwk —
стендовые `as-*-2`):

1. **Arrange**: полный стенд поднят (проверка: оба инстанса живы — healthz
   обоих портов pgw, ≥2 ключа `/kafkaworker/api/`, `/valkeyworker/api/`,
   ≥2 ключа `/pgworker/api/`).
2. **Act**: `docker stop` ПЕРВОГО контейнера воркера (для pgw —
   `deploy-pgworker-1`; stop, не kill: `unless-stopped` не поднимает
   контейнер после явного stop — окно владения чисто уходит второму).
3. **Assert (бюджет ≤60 с)**: lease-ключи первого погасли
   (`/pgworker/api/`, `/instances/` первого id нет); второй жив (healthz);
   `/pgworker/claims/<C>` (кластерный клэйм живого кластера стенда, при
   наличии) содержит instance-id второго; панельный ответ не 503
   (опционально: smoke-запрос через панель).
4. **Restore**: `docker start` первого → оба инстанса снова живы, оба
   ключа `/api/*` на месте (≤60 с).
5. Аналогично для kafkaworker (стоп `as-kafkaworker`) и valkeyworker
   (`as-valkeyworker`).

Чек идемпотентен, оставляет стенд в исходном состоянии (все контейнеры
подняты). Пороги ожидания — как у соседних чеков (60–120 с), никаких
хардкодов портов вне env-переменных (порт pgw — `${PGW_API_HOST_PORT2}`).

### 4.5. Docker-E2E: второй инстанс контейнером (деплой-слой)

Новый сценарий в `src/tests/PgWorker.IntegrationTests/E2e/`
(например, `E2eSecondInstanceScenarios.cs`) — по канонам
`docs/e2e-isolation.md` / `docs/e2e-launch.md`:

- **Окружение**: собственное (guid-имена), etcd-контейнер фикстуры
  (динамический порт — как в существующих E2E), docker-сеть сценария.
- **Образ**: `docker build -f docker/PgWorker.Dockerfile -t
  pgworker:e2e-<guid> .` — сборка в сценарии (кэш слоёв делает повторные
  прогоны быстрыми); тег с guid, удаление в teardown.
- **Два инстанса-контейнера** (`docker run -d --restart=no`): динамические
  хост-порты (зонд `E2eFixture.FreePort()`), env как у хост-инстансов
  фикстуры (etcd endpoint `http://host.docker.internal:<dynamicPort>`,
  секреты установки, `AdvertisedHost=host.docker.internal`,
  `extra_hosts local:host-gateway`), mTLS — **PEM-дуализм env**
  (`PGW_API_TLS_CERT/KEY/CLIENT_CA` значениями PEM, без volume — уже
  поддержано воркером), уникальные `AdvertiseUrl` per инстанс. Тома
  снапшотов НЕ монтируем (ephemeral ФС контейнера достаточна для теста).
- **Сценарий** (аналог AC3/mid-add, но инстансы — контейнеры):
  1. seed декларации кластера;
  2. ждать первого контейнера шарда (A3 начался);
  3. `docker kill` ПЕРВОГО инстанса-контейнера воркера
     (`--restart=no` — не воскреснет сам);
  4. ждать: шард донесён до Active/registered (бюджет как у AC3, ≤360 с),
     контейнеров нового шарда ровно по числу replicas (нет дублей);
  5. asserts: выживший инстанс держит живые `/pgworker/api/<id>` и
     `/pgworker/instances/<id>`; work-journal показывает продолжение фаз
     вторым instance-id.
- **Teardown при любом исходе** (finally/`IAsyncDisposable`): оба
  контейнера воркеров + контейнеры шардов + сеть + тома сценария + образ
  `pgworker:e2e-<guid>`; ассерт чистоты (ни одного объекта guid-префикса).
  Телеметрия по `docs/e2e-launch.md`: docker-логи/inspect в
  `/tmp/pgw-e2e-artifacts-<guid>/` до удаления; `MarkFailed()` — контейнеры
  останавливаем, не удаляем.
- **Правила**: динамические порты только; таймауты фаз короткие
  (start-бюджет инстанса ≤100 с); прогон — Release (сборка фикстурой,
  `PGW_TEST_E2E_NOBUILD=1` только для бисекта); маркер
  `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test … --filter
  FullyQualifiedName~SecondInstance` в мерж-гейте задачи.

### 4.6. Документация

- `docs/runbook.md`: раздел deploy — второй инстанс каждого воркера
  (порты 8083/8084/8085, «зачем», как остановить один для обслуживания).
- `arch/roadmap/`: **мерж-гейт трека** — закрытие задачи тем же
  мерж-коммитом снимает пункт `t07-worker-second-instance` из
  `arch/roadmap/reliability.md` И обновляет строку/контекст в
  `arch/roadmap/reliability-report.md` («деплой воркеров … SPOF надзора»
  → закрыто).

## 5. Фазы

1. **arch-first**: дополнения `arch/14` §2, `arch/16` §2.1, `arch/21` §2
   (рецепт поставки двух инстансов; только документация).
2. **deploy**: `deploy/docker-compose.yml` (анкоры + 3 вторых сервиса) и
   `deploy/.env.example`.
3. **Стенд**: стендовые compose (kafkaworker-2, valkeyworker-2),
   Prometheus (static_configs + file_sd), `00-up.sh` (подъём обоих pgw,
   healthz обоих, ≥2 ключей), аудит `90-down.sh`.
4. **Чек**: `35-worker-second-instance.sh` (kill→takeover, §4.4); прогон
   полного стенда `00-up.sh` зелёный.
5. **docker-E2E**: сценарий §4.5; локальный прогон зелёный; зачистка
   контейнеров/сетей после серии.
6. **Доки + мерж-гейт**: runbook, roadmap/reliability.md, reliability-report.md.

Фазы 2–3 можно вести одним план-этапом (deploy+стенд связаны портами);
фазы 4 и 5 независимы друг от друга (после 2–3).

## 6. Ограничения

- Код воркеров/панели/Shared не меняется (см. §1.3).
- Хост-порты контуров стены и деплоя не пересекаются с диапазонами
  portalloc (15xxx/17xxx/18xxx) и портами dev-станда; новый ряд —
  8083/8084/8085 (свободны; `METRICS_*`-прецедент env-override при
  коллизиях сохраняем через `*_HOST_PORT2`).
- В тестах — только динамические порты; никаких литералов вида `:16000`.
- E2E: полная самоочистка, никакого общего мутабельного контура между
  сценариями; BrokerBootSec-аналоги ≤100 с.
- Стендовые kfw-2/vwk-2 без хост-публикации портов — коллизии хост-портов
  стены не расширяем.
- Локально собираемые образы в registry `192.168.0.1:5000` не кладём
  (вторых сервисов это тоже касается — образ тот же `*:dev`).

## 7. Критерии приёмки

- **AC1 (деплой)**: `cd deploy && docker compose up -d` поднимает 6
  контейнеров воркеров (2×3); healthz (mTLS) отвечает на обоих портах
  каждого pg/vwk-инстанса деплой-ряда и обоих kfw; в etcd по два живых
  ключа `/pgworker/api/*`, `/kafkaworker/api/*`, `/valkeyworker/api/*`.
- **AC2 (панель)**: грань «Воркеры» показывает по 2 инстанса каждого
  воркера, статусы Healthy; мутации через панель работают (failover по
  списку endpoints).
- **AC3 (стенд)**: `00-up.sh` полного стенда зелёный, поднимает по два
  инстанса каждого воркера; `90-down.sh` сносит всё без остатка.
- **AC4 (чек R)**: `35-worker-second-instance.sh`: `docker stop` первого
  инстанса → lease-ключи первого гаснут ≤15–60 с, надзор/клэймы у второго,
  healthz второго жив; `docker start` → оба живы снова. Чек идемпотентен.
- **AC5 (docker-E2E)**: сценарий §4.5 зелёный на свежем Release: kill
  первого контейнера PgWorker посреди provisioning → шард донесён вторым,
  дублей контейнеров нет, teardown оставляет ноль объектов сценария.
- **AC6 (наблюдение)**: Prometheus скрейпит оба инстанса каждого воркера
  (targets up в /targets); метрики инстансов различимы.
- **AC7 (канон)**: arch/14/16/21 дополнены (§4.1); `docs/runbook.md`
  описывает вторые инстансы; roadmap/reliability.md и
  reliability-report.md обновлены мерж-коммитом закрытия.
