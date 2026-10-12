# 24. S3Worker: оркестратор S3-кластеров ★

**S3Worker** — фоновый сервис (.NET 10), исполнительная сторона
декларативного контракта [23-s3-clusters.md](23-s3-clusters.md): панель
AdminPanel **заявляет** кластер через **HTTP API воркера** (§1.1;
`state=NOT_INITIALIZED`) — воркер **поднимает** ноды (volume данных +
контейнер ownS3 на хосте из placement-селектора), создаёт
bucket per-install, пишет `endpoints` и снимает `state`; перевод в
`TO_REMOVE` — воркер демонтирует кластер полностью. **Ответственность
изменений etcd**: префиксы `/s3/`, `/s3worker/` пишет ТОЛЬКО S3Worker
(панель и сиды ходят через его API); панель etcd домена S3 только читает.

Шесть процессов (машины состояний):
1. **ProvisioningProcess** (A, P0–P5) — от `NOT_INITIALIZED` до рабочего
   кластера (ноды, bucket, `endpoints`);
2. **DeprovisioningProcess** (B, X0–X3) — от `TO_REMOVE` до чистого etcd
   и снесённых docker-объектов;
3. **NodeSupervisor** (C, надзор) — снесённый контейнер пересоздаётся,
   недоступная нода помечается, занятость диска собирается;
4. **AddNodeProcess** (D) — добавление ноды в живой кластер (подъём по A;
   включение в mesh + backfill — с t35);
5. **RemoveNodeProcess** (E) — вывод ноды (гварды, снос, чистка);
6. **SyncProbeProcess** (F) — зонд связности/синхронности
   (`connectivity`/`sync` arch/23 §3; каркас канонизируется, реализация —
   t33).

Свойства: несколько инстансов работают одновременно (координация —
lease-клэймы, arch/23 §8); смерть контролирующего инстанса не роняет
процессы — takeover ≤ TTL 15 с + тик; все операции идемпотентны; значимое
состояние переживает смерть контроллера (etcd).

Границы (что НЕ входит): механика mesh-репликации ownS3 (t35); зонды
синхронности (t33); панель домена S3 (t34); клиентская интеграция пула
(t36).

---

## 1. Роль и разделение ответственности

```
AdminPanel (UI)          S3Worker (исполнитель)                 docker-хосты
──────────────           ──────────────────────                 ────────────
мутации (создание/ ──►   HTTP API воркера (§1.1)                 контейнер
удаление кластера/       ──пишет──► /s3/clusters/<C>/config      s3w-<C>-node<k>
состава нод/ресурсы;     .state=NOT_INITIALIZED|TO_REMOVE,       (ownS3),
ротация секретов;        nodes/node<k>/state (заявки),           volume данных,
зонд по команде)         /s3worker/rotations/<C>                 S3-порт из portalloc
                         ──читает──► декларации
                         ──создаёт/удаляет──► контейнеры s3w-<C>-node<k>,
                         endpoints, states (PROVISIONING/RUNNING/...),
                         снятие state
инспекция (read-   ◄──
only, всё видит;          ключ /s3worker/api/<id> =
URL API — из etcd)        URL воркера (§1.1)
```

Отдельная строка: **PgWorker — клиент пула** (читатель `endpoints` в t36),
участником контракта НЕ является — в `/s3/` и `/s3worker/` не пишет и
docker-объектов домена не создаёт.

- **Панель** — декларатор и наблюдатель: **etcd только читает**
  (арх.23 §9); все мутации S3-домена отправляет в HTTP API воркера (§1.1).
- **S3Worker** — исполнитель: единственный, кто создаёт/удаляет
  контейнеры и volumes нод, пишет `endpoints`, `nodes/node<k>/state`,
  `placed_on`, `usage`, снимает `state` у config, чистит префикс кластера
  при TO_REMOVE — и единственный, кто **записывает декларации/заявки** в
  etcd (приёмник мутаций панели и сида через свой API, §1.1).

### 1.1. HTTP API воркера (мутации панели, сиды)

Та же HTTP-грань, что `/healthz` и `/metrics`. mTLS по общему паттерну
воркеров ([14-pgworker.md](14-pgworker.md) §1.1): клиенты (панель)
аутентифицируются клиентским сертификатом, подписанным единой per-install
API-CA `kfw-install-ca` (единая пакета воркеров установки — отдельные
клиентские серты панели); клиент без валидного серта — отказ на
TLS-хендшейке. Префикс `/api` — приёмник ВСЕХ мутаций S3-домена (панель в
etcd домена не пишет ничего).

**Серверный серт API** — ключ `/workers/api_tls/s3worker` (канон ключа и
протокол — [adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md)
§9.9; пишет ТОЛЬКО панель — t34): воркер читает при старте и поднимает
грань на этом серте; env-секреты `S3W_API_TLS_{CERT,KEY,CLIENT_CA}` —
бутстрап-фоллбек при отсутствии ключа. `/healthz`, `/metrics` — та же
mTLS-грань (скрейп Prometheus — по mTLS, arch/18).

**Дискавери API**: ключ `/s3worker/api/<id>` (arch/23 §8) — ставит сам
инстанс; URL — из `S3Worker:Api:AdvertiseUrl` (достижим панелью);
`cert_thumbprint` — SHA-256 серта, фактически применённого на грани.
Панель зовёт любой живой ключ (failover на следующий; все умерли — 503 +
critical-алерт, t34).

**Эндпоинты** (сигнатуры/гварды — канон поведения исполнителя, панель —
прокси; протоколы записи — по образцу
[adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md)
§9.2/§9.4/§9.5/§9.6):

| Метод+путь | Назначение |
|---|---|
| `POST /api/s3/clusters` | создание кластера: клэйм-txn `version(config)==0` + пакет PUT (config + nodes × {placement, resources, state=NOT_INITIALIZED}); сбой — компенсация `del --prefix /s3/clusters/<C>/` (§9.1-образец) |
| `DELETE /api/s3/clusters/{c}` | перевод `config.state=TO_REMOVE` (RMW; already → 204 без записи) |
| `POST /api/s3/clusters/{c}/nodes` | добавление ноды (`node<max+1>`): клэйм-txn + put `placement`/`resources`/`state=NOT_INITIALIZED` |
| `DELETE /api/s3/clusters/{c}/nodes/{n}` | маркер демонтажа ноды `nodes/<n>/state=TO_REMOVE` (one-way; гварды: не последняя нода кластера) |
| `PUT /api/s3/clusters/{c}/nodes/{n}/resources` | мутация ресурсов ноды (cpu/mem — rolling-пересоздание контейнера надзором; disk — декларация/наблюдение) — образец: kafka-мутация №15 «Изменение ресурсов брокера» ([adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md) §10.2) |
| `POST /api/s3/clusters/{c}/rotations` | заявка ротации секретов профиля кластера (`/s3worker/rotations/<C>`, клэйм-txn — §9.8-паттерн) |
| `POST /api/s3/clusters/{c}/sync-probe` | внеплановый зонд синхронности/связности (202; результат — ключи arch/23 §3) — реализация t33, эндпоинт канонизируется сейчас |

**Валидация** (сервер — источник истины): `name` — паттерн arch/23;
`platform` — единственное значение `owns3`; `nodes` — 1..9 (рекомендация
2–3 — гео-минимум и бюджет R1, arch/23 §2);
`placement` — обязательный валидный
селектор для каждой ноды (400); `resources` — cpu 0.01..64, mem/disk
1..65536 GiB (канон форматов — adminpanel/02 §9.3); `secret_profile` —
непустое имя, существующее в env воркера (422 с перечнем доступных имён,
без значений секретов).

## 2. Модель размещения

- **Транспорт docker** — общий клиент `Shared.Docker` (канон-суперсет,
  образец [14-pgworker.md](14-pgworker.md) §2.2.1): plain/swarm, транспорт
  unix/tcp+TLS/SSH; env `S3W_DOCKER_TLS_{CA,CERT,KEY}`,
  `S3W_DOCKER_SSH_KEY[_PATH]`, `S3W_DOCKER_SSH_FINGERPRINT`.
- **Конфиг** (§8): `S3Worker:Docker { Mode, Hosts[{Name,Endpoint}],
  SwarmManager, PortRange{From,To}, Images{OwnS3}, Tls, Ssh,
  ContourNetwork, AdvertisedHost }`.
- **Атрибуты хостов** для placement-селекторов (модель — arch/23 §4):
  plain — имя из `Hosts[]` (атрибут `docker_host`); swarm — hostname нод и
  `node.labels.*` (атрибуты `swarm_node`/`swarm_labels`, `GET /nodes`).
- **Portalloc**: S3-порт ноды (контейнерный 9000) → последовательные
  host-слоты диапазона `PortRange`; закрепление
  `/s3worker/portalloc/<C>` (переживает rebuild); довыделение — только
  держателем глобального клэйма `/s3worker/locks/portalloc` (t90-паттерн:
  занятость = docker-публикации ∪ portalloc всех кластеров — образец
  [14-pgworker.md](14-pgworker.md) §2.4).
- **Advertised-адреса** — только от фактических публикаций
  (`S3Worker:Docker:AdvertisedHost` по образцу
  [14-pgworker.md](14-pgworker.md) §2.4 п.5 — single-host/стенд; multi-host —
  фактические имена хостов). Требование: значение обязано резолвиться
  клиентами — оно попадает в `endpoints`.
- **Сеть**: per-cluster сеть НЕ создаётся (межнодовая коммуникация mesh и
  клиенты — по advertised `host:port`, единый namespace адресов; отказ от
  аналога `pgw-net-<C>` осознанный — Patroni-подобного внутреннего
  кластерного протокола у нод S3 нет). Опция `ContourNetwork` (стенд/E2E) —
  ноды создаются в сети контура поставки, адресация по именам сети, сеть
  воркером не создаётся/не удаляется (как
  [14-pgworker.md](14-pgworker.md) §2.1).
- **Имена docker-объектов**: контейнер ноды `s3w-<C>-node<k>` (детерминирован),
  volume данных `s3w-<C>-node<k>-data`; restart-политика `unless-stopped`.
  Сам воркер — контейнер с `docker.sock`, поставка через
  `deploy/docker-compose.yml`; **два инстанса по умолчанию** (образец pg
  t07 — [14-pgworker.md](14-pgworker.md) §2.4): уникальный хост-порт API и
  уникальный `Api:AdvertiseUrl` на инстанс, уникальность — случайный
  `InstanceId`.

## 3. Контракт etcd

Транспорт и схема ключей — [23-s3-clusters.md](23-s3-clusters.md) (§2/§3 —
декларации и статусы, §8 — координация `/s3worker/`); читаемые/пишемые
ключи — ссылками туда (источник истины — не пересказ). Здесь — только
координационное поведение:

- **Инвариант мутаций**: любые записи в `/s3/` — только держателем клэйма
  `<C>` (`/s3worker/claims/<C>`); API воркера пишет декларации/заявки под
  клэйм-txn `version==0` (создание кластера/ноды, заявка ротации) —
  панель через него, никогда напрямую.
- **Txn с compare**: снятие `config.state` (P5) и перевод в `TO_REMOVE`
  сравниваются по `mod_revision` config — внешняя правка между read и
  write закрывается ретраем тика; RMW `endpoints` — по value
  прочитанного значения.

## 4. Секреты

- **Per-install env-профили** `S3W_NODE_SECRETS__<PROFILE>__*` (формат и
  имена переменных — arch/23 §6): root-пара + опциональные статические
  access keys (read-write бэкапам / read-only панели); профиль един на
  кластер (поле `config.secret_profile`).
- **Передача в ноды** — env контейнера при создании:
  `OWNS3_ROOT_USER`/`OWNS3_ROOT_PASSWORD` + `OWNS3_ACCESS_KEYS__<i>__*`;
  значения никогда не попадают в etcd/журнал.
- **Ротация**: заявка `/s3worker/rotations/<C>` (§1.1) →
  rolling-пересоздание нод (по одной за тик, данные volume не трогаются;
  рестарт-семантика платформ: ownS3 — переконфигурация env + рестарт,
  owns3/05 §2) → del заявки по завершении.
- **Транспортные секреты** API/docker — env `S3W_*` (§1.1/§2).
  Серверный серт API — ключ `/workers/api_tls/s3worker` (панель пишет —
  t34; до того env-фоллбек `S3W_API_TLS_*`).

## 5. Процессы (машины состояний)

**Состояния ноды** (`nodes/node<k>/state`): `NOT_INITIALIZED` (панель
заявила) → `PROVISIONING` (воркер создаёт) → `RUNNING` (health-грань
отвечает); `UNREACHABLE` (молчит дольше NodeDeadSec); `REMOVING`
(демонтаж); `TO_REMOVE` (маркер панели, one-way).

Классификация тика: `config.state=NOT_INITIALIZED` → Provisioning (A);
`TO_REMOVE` → Deprovisioning (B); иначе Active-ветка: надзор (C) →
add-node (D) → remove-node (E) → зонд (F). Все операции — только под
живым клэймом `<C>`; journal-before-manipulations
(`/s3worker/work/<C>`); все шаги идемпотентны (перепроверка факта).

### A. ProvisioningProcess (P0–P5)

```
P0 claim + journal(op=provision); перечитка config
   (гонка TO_REMOVE посреди provisioning — безопасная остановка)
P1 резолв placement всех нод (arch/23 §4: селектор → кандидаты →
   детерминированный выбор → placed_on) + portalloc — под глобальным
   клэймом /s3worker/locks/portalloc (§2); не взял клэйм → journal
   waiting-portalloc-lock (следующий тик); валидация профиля секретов
   (env воркера; неполный → permanent-отказ фазы с диагностикой, §4)
P2 создание нод: volume данных + контейнер ownS3 (образ из
   Images{OwnS3}; env: креды профиля — OWNS3_ROOT_*/OWNS3_ACCESS_KEYS__*
   (arch/23 §6), OWNS3_DATA_DIR=/data; адреса mesh-соседей — конфиг
   ownS3 t35 (до неё не передаются); лимиты resources; S3-порт из
   portalloc);
   health-ожидание — бюджет NodeBootSec, transient-толерантный цикл
   (поллинг-инвариант t19 — без одиночных блокирующих вызовов)
P3 bucket per-install на каждой ноде — идемпотентно (exists → пропуск;
   CreateBucket root-кредом профиля — права admin, owns3/05 §3)
P4 mesh-синхронизация — внутренняя механика ownS3 (конфиг поставки
   per-install), реализация и канон arch/owns3/07 — t35; до неё фаза
   no-op (кластеры N≥2 декларативно валидны без синхронизации,
   arch/23 §5)
P5 put endpoints (advertised host:S3-порт) + config: txn
   (compare mod_revision) → put канонического JSON без state; journal done
```

### B. DeprovisioningProcess (X0–X3)

```
X0 claim + journal(op=deprovision)
X1 маркеры REMOVING всем нодам
X2 docker: снос контейнеров s3w-<C>-node* и volumes данных (все хосты
   Hosts[]; 404 = ок) + чистка portalloc <C>; порядок «сначала docker,
   потом etcd»
X3 etcd: del --prefix /s3/clusters/<C>/ + del
   /s3worker/{work,portalloc,rotations}/<C>; journal done
```

Снос кластера уничтожает данные бэкапов его нод — осознанное действие
оператора (панель t34 — confirm); фиксируется в рисках §9.

### C. NodeSupervisor (надзор, тик)

- **Health-проба каждой ноды** — платформенная health-грань (arch/23 §5)
  по `host:port` из portalloc; проба — не доказательство доступности
  записи, только живость контейнера/грани.
- **Снесённый контейнер** (положительное свидетельство docker-инспекции,
  S7 [17-synchronization-principles.md](17-synchronization-principles.md))
  → пересоздание на том же хосте (`placed_on` фиксирован; volume
  сохраняется — данные не теряются), `state=PROVISIONING`; в `RUNNING`
  переводит следующий цикл по health.
- **Недоступная нода** (проба молчит дольше NodeDeadSec) →
  `state=UNREACHABLE` + ретраи тиками; деструктивных действий по факту
  «не отвечает» нет (S7: флап ≠ смерть).
- **Сбор `usage`** — периодический (HealthIntervalSec): занятость диска из
  платформенной админ-грани/метрик → ключ `nodes/node<k>/usage` —
  квота-наблюдение `resources.disk` (enforcement-примитива нет, arch/23
  §2) и алерты панели (t34).
- Слепая проба (docker-хост молчит) — никаких действий; одно пересоздание
  за тик.

### D. AddNodeProcess

Заявка панели (`nodes/<n>/state=NOT_INITIALIZED`, `node<max+1>` — §1.1):
подъём новой ноды (по A: placement/portalloc/volume/контейнер/bucket) +
RMW `endpoints`. Включение в mesh + backfill существующих объектов —
механика ownS3 (R4 `docs/owns3-replication-requirements.md`), появляется
с t35; до неё добавление завершается подъёмом ноды и bucket. Финал —
`state=RUNNING`.

### E. RemoveNodeProcess

Маркер панели `nodes/<n>/state=TO_REMOVE`: гварды (не последняя нода
кластера; при мёртвых остальных — нет живой RUNNING-ноды кроме выводимой —
отказ фазы: вывод не рвёт последнюю точку записи пула) → исключение из
mesh (механика ownS3 — t35; до неё — нет) → остановка/снос контейнера и
volume → чистка ключей ноды и записи portalloc → RMW `endpoints`. Отказ
данных осознан: volume ноды сносится (после t35 копия — на остальных нодах
mesh; до него репликации нет и объекты выводимой ноды теряются — R1 §9);
фиксируется в рисках §9.

### F. SyncProbeProcess (контракт — t33, каркас канонизируется)

Периодический и по-командный (§1.1 `sync-probe`) зонд: PUT-пробы
межнодовых линков (connectivity) и сверка листингов bucket между нодами
(sync) — форматы и state-агрегаты arch/23 §3. Источник истины о
синхронности — собственный зонд: штатные метрики репликации лаг данных не
отражают (факт 7 `docs/s3-replication.md`); S3-операции зонда —
per-попыточный таймаут + пульс (§6).

## 6. Надёжность

- **Идемпотентность**: каждый шаг перепроверяет факт (контейнер есть?
  health отвечает? bucket существует? правило mesh стоит? `state` уже
  снят?); отказ между фазами — ретрай тиком по journal-фазе.
- **Takeover** ≤ TTL 15 с: клэймы/journal/states/portalloc — в etcd;
  смерть инстанса гасит lease — другой инстанс продолжает с записанной
  фазы; двойной контроллер невозможен (операции — только под живым
  клэймом).
- **Отказ docker-хоста**: ноды на нём `UNREACHABLE`, размещение не
  меняется (`placed_on` фиксирован — перепривязка на месте не допускается,
  arch/23 §2), новые ноды — только на резолвируемые хосты.
- **Что теряется при отключении машины X**: volume ноды — локальное
  хранение без гарантий durability, доступность её доли теряется до
  возврата машины; до mesh-репликации ownS3 (t35) гибель диска ноды —
  потеря её доли данных (реплик нет); после t35 — переживание отказа
  машины с точностью до лага доставки (RPO = лаг репликации, R2
  `docs/owns3-replication-requirements.md`), восстановление полноты —
  backfill из живых нод (R4 там же, процесс D); гарантии строятся от
  реплицированных источников (etcd + mesh), не от локальных volume.
- **Отказ etcd**: контрол-плейн заморожен; живые ноды S3 от него не
  зависят (клиенты пишут по последним `endpoints`).
- **Watchdog зависших циклов** — общий каркас `LoopWatchdog` (Shared.Core,
  [14-pgworker.md](14-pgworker.md) §6): инварианты живости/поллинга
  обязательны и для циклов S3Worker; «пульс конечного внешнего вызова»
  (S3-операции зонда — per-попыточный таймаут + пульс) — тот же канон.

## 7. Наблюдаемость

`/healthz` — честный health по канону воркеров: `etcd-reachable`,
`docker-hosts` (per-host ping), `loops-alive`, `claims`. Prometheus-
метрики — единый каркас [18-metrics.md](18-metrics.md) (`/metrics` на
порту API): циклы/клэймы/фазы процессов/операции воркера
(`s3worker_*` в Meter-именах). Телеметрия самих нод — метрики ownS3
(owns3/05 §5); scrape-механика — вне канона. Диагностика фаз —
`/s3worker/work/<C>` и `nodes/node<k>/state`.

## 8. Конфигурация (appsettings + env)

```
S3Worker:Etcd { Endpoints[] }                      # единый etcd-контур (arch/17 S1)
S3Worker:Bucket                                    # имя per-install bucket (arch/19 §5;
                                                   #   создаётся оркестратором на каждой ноде)
S3Worker:Docker { Mode: Plain|Swarm, Hosts[{Name,Endpoint}], SwarmManager,
                  PortRange{From,To}, Images{OwnS3="owns3:dev"},
                  Tls, Ssh, ContourNetwork, AdvertisedHost }
S3Worker:Loops { ScanIntervalSec=5, KeepaliveSec=5, Watchdog{...} }
S3Worker:Thresholds { NodeBootSec=100, NodeDeadSec=90, HealthIntervalSec=15 }
S3Worker:Parallelism { MaxClusters=4 }
S3Worker:Api { AdvertiseUrl, EnableSeedEndpoint=false, Tls{...} }   # mTLS-грань, §1.1
# env-секреты S3W_*: S3W_API_TLS_{CERT,KEY,CLIENT_CA}, S3W_DOCKER_TLS_{CA,CERT,KEY},
#   S3W_DOCKER_SSH_KEY[_PATH], S3W_NODE_SECRETS__<PROFILE>__* (§4)
```

Fail-fast валидации старта — по образцу pg (неполные секретные пары,
отрицательные пороги); `EnableSeedEndpoint` — при необходимости
стендового сида, default `false` (паттерн
[14-pgworker.md](14-pgworker.md) §1.1.1).

## 9. Риски

| # | Риск | Митигация |
|---|---|---|
| R1 | До mesh-репликации ownS3 (t35) реплик нет: гибель диска ноды — потеря её доли данных; N=1 — потеря всего кластера | рекомендация N≥2/3 (гео-минимум 3), алерт панели (t34); mesh — t35; риск осознан валидацией, не запретом |
| R2 | Снос кластера/вывод ноды уничтожает данные бэкапов его нод | осознанное действие оператора: панель t34 — confirm (§5 B/E); риск-строка канона |
| R3 | Собственный зонд — единственный честный источник синхронности | зонд — процесс F (t33), форматы arch/23 §3; метрики доставки ownS3 (R2 `docs/owns3-replication-requirements.md`) — наблюдаемость механики, не источник истины о данных |
| R4 | Отсутствие per-cluster сети — межнодовые линки зависят от advertised-адресов и открытых межхостовых портов | ограничение фиксируется явно (§2); раснос по площадкам — явные селекторы оператора |
| R5 | Mesh-ownS3 зависит от t35 (до неё N≥2 без синхронизации) | кластеры с N≥2 декларативно валидны, mesh отсутствует — статусы `sync` это отразят (arch/23 §5) |

## 10. Дальше

- Контракт домена — [23-s3-clusters.md](23-s3-clusters.md) (ключи, статусы,
  дискавери).
- Потребитель пула — [19-backups.md](19-backups.md) (bucket per-install,
  креды).
- Образцы воркеров — [14-pgworker.md](14-pgworker.md) (паттерны API/
  placement/watchdog), [16-kafkaworker.md](16-kafkaworker.md),
  [21-valkeyworker.md](21-valkeyworker.md).
- Принципы синхронизации —
  [17-synchronization-principles.md](17-synchronization-principles.md).
- Отложенные задачи — [roadmap/s3.md](roadmap/s3.md) (t33 зонды,
  t34 панель, t36 клиентская интеграция; mesh-ownS3 —
  [roadmap/owns3.md](roadmap/owns3.md) t35).
- Требования mesh-ownS3 —
  `docs/owns3-replication-requirements.md`; изыскания платформ
  репликации (MinIO/Garage) — справочник фактов `docs/s3-replication.md`.

---

→ Указатель — [README.md](README.md). Контракт домена —
[23-s3-clusters.md](23-s3-clusters.md).
