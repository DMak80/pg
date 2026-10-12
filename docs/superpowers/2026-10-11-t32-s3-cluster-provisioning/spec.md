# t32-s3-cluster-provisioning — спецификация

S3Worker: оркестратор кластеров S3-хранилища бэкапов (платформа ownS3) —
provisioning по декларации, надзор, изменение состава, HTTP API, поставка
и стенд/E2E-механика.

Канон домена: [`arch/23-s3-clusters.md`](../../../arch/23-s3-clusters.md)
(контракт etcd `/s3/` + `/s3worker/`) и
[`arch/24-s3worker.md`](../../../arch/24-s3worker.md) (оркестратор);
задача — пункт `t32-s3-cluster-provisioning` трека
[`arch/roadmap/s3.md`](../../../arch/roadmap/s3.md). Спека следует канону;
расхождения канона и реализации — блокирующее замечание ревью.

## 1. Контекст и цель

Пул S3-серверов хранилища бэкапов становится управляемым объектом системы:
панель заявляет кластер через HTTP API воркера, воркер поднимает ноды
ownS3 контейнерами (volume данных + контейнер на хосте из
placement-селектора), создаёт bucket per-install, пишет `endpoints` и
снимает `state` (arch/24 §1). S3Worker в кодовой базе отсутствует —
создаётся с нуля по образцу существующих воркеров
(ValkeyWorker/KafkaWorker: решение-наблюдение — те же слои Core/Etcd/
Docker/Provisioning/App, та же координация Shared.Etcd, тот же каркас
healthz/metrics/mTLS-API).

**Решения пользователя (2026-10-11, границы задачи):**

1. **Платформа кластеров — только ownS3.** Платформа `minio` снимается с
   канона arch-first правкой (см. §2): `config.platform` — единственное
   значение `"owns3"`. Вместе с MinIO упразднены его механики: bucket
   versioning, правила bucket replication fan-out с уникальными
   приоритетами и флагами `delete,delete-marker,existing-objects`,
   backfill `existing-objects` как операция оркестратора, выравнивание
   после долгих отказов (пере-добавление правил/`mc mirror`), чистка
   физических версий поверх versioning. Синхронизация данных между нодами
   — автоматическая внутренняя механика ownS3 (`t35-owns3-replication`,
   трек owns3; требования —
   [`docs/owns3-replication-requirements.md`](../../owns3-replication-requirements.md));
   до неё кластеры с N≥2 декларативно валидны без синхронизации
   (arch/23 §5). Удаления ownS3 — физические (versioning отсутствует):
   ретенция arch/19 §4 работает напрямую, чистки версий не требуется.
2. **Стендовая часть — полностью**: deploy/docker-compose.yml (два
   инстанца), интеграция в полный dev-stand (`00-up.sh`, сеть, etcd-контур,
   чек `checks/`), docker-E2E серия.
3. **`POST /api/s3/clusters/{c}/sync-probe`** появляется в t32 как
   заглушка: 202 Accepted без исполнения, ключи `connectivity`/`sync`
   не пишет; исполнение — t33 (arch/24 §1.1: «эндпоинт канонизируется
   сейчас»).

**Цель задачи**: рабочий сервис S3Worker, исполняющий декларативный
контракт arch/23/arch/24 процессы A–E (зонд F — контракт t33, в t32 —
только 202-заглушка эндпоинта), с идемпотентностью по факту, takeover и
journal-before-manipulations по arch/17, поставленный в deploy-контур и
стенд, покрытый юнит-/интеграционными/E2E-тестами по канонам проекта.

## 2. Arch-first правки (сделаны в этой ветке, источник истины)

Правки канона выполнены ДО спеки реализации (arch-first); код обязан
зеркалить обновлённый канон.

- [`arch/23-s3-clusters.md`](../../../arch/23-s3-clusters.md):
  - §2 `config`: `platform` — единственное значение `"owns3"`
    (фиксируется при создании навсегда; точка расширения контракта);
    `nodes` 1..9 (рекомендация 2–3);
  - §3 `connectivity`: обоснование PUT-проб — R3
    `docs/owns3-replication-requirements.md` (факт 8
    `docs/s3-replication.md` — справочная параллель);
  - §5: раздел «Платформа нод» (единственная — owns3; health `/healthz`;
    usage — админ-грань `GET /admin/storage`; удаления физические);
  - §6: env нод — только `OWNS3_ROOT_*` + `OWNS3_ACCESS_KEYS__<i>__*`.
- [`arch/24-s3worker.md`](../../../arch/24-s3worker.md):
  - шапка/границы: контейнер ownS3; границы — «механика mesh-репликации
    ownS3 (t35)»;
  - §1.1 валидация: `platform` = `owns3`, `nodes` 1..9 (рекомендация
    2–3);
  - §2/§8: `Images{OwnS3}`; §8 дополнен `S3Worker:Bucket` — имя
    per-install bucket (arch/19 §5), создаётся оркестратором на каждой
    ноде;
  - §4: передача в ноды — только OWNS3-env;
  - §5: P2 (env ownS3, `OWNS3_DATA_DIR=/data`, адреса mesh-соседей —
    только с t35), P3 (CreateBucket root-кредом профиля, права admin,
    owns3/05 §3), P4 (no-op до t35), D (mesh+backfill — с t35),
    E (гварды: не последняя нода; при мёртвых остальных — нет живой
    RUNNING-ноды кроме выводимой — отказ фазы);
  - §6: «что теряется при отключении машины X» — до t35 реплик нет;
  - §9 риски: перенумерованы R1–R5 (сняты MinIO-EOL и предел 4+ нод
    MinIO; R1 — отсутствие реплик до t35);
  - §10: требования mesh — owns3-replication-requirements,
    s3-replication — справочник изысканий.
- [`arch/roadmap/s3.md`](../../../arch/roadmap/s3.md): шапка трека
  (платформа ownS3), порядок «provisioning → синхронность/связность →
  панель; replication-mesh — t35», переписан пункт t32.
- [`arch/roadmap/README.md`](../../../arch/roadmap/README.md): строка
  трека s3 — «кластеры ownS3 … mesh-репликация (t35) …».

**Не трогается** (вне зоны t32): `docs/s3-replication.md` и
`docs/owns3-replication-requirements.md` (справочники фактов/требований);
`arch/adminpanel/*` и сервис `as-minio` стенда (переходный read-only-архив
бэкапов, история `docs/owns3-migration.md` — не кластерный трек);
`arch/22`/`arch/owns3/*` (домен ownS3); `arch/19` (bucket per-install уже
канонизирован).

## 3. Принципы

- **Arch-first**: источники истины — arch/23, arch/24; утверждения о
  механиках — только по ним; код зеркалит канон.
- **Паттерны arch/17**: единый etcd-контур (S1); идемпотентность каждого
  шага по факту (перепроверка «контейнер есть? health отвечает? bucket
  существует? state уже снят?»); journal-before-manipulations
  (`/s3worker/work/<C>`, S6 — фазовые записи не затирают накопленные
  треки); мутации `/s3/` — только держателем клэйма `<C>`
  (`/s3worker/claims/<C>`, S5, фиксированный порядок записей);
  деструктив — только по положительному свидетельству docker-инспекции
  (S7); takeover ≤ TTL 15 с + тик (значимое состояние — в etcd).
- **Multi-host**: docker-хостов много, отключение любого — штатный
  сценарий; локальные volumes — кэш без durability, значимое состояние —
  только etcd (AGENTS.md).
- **Poll, без watch**: тик `ScanIntervalSec=5` покрывает динамику
  (arch/23 §1).
- **Поллинг-инвариант**: одиночный внешний вызов (etcd/docker/HTTP-пробы
  нод/S3-операции) не молчит дольше окна проверки watchdog — таймаут
  вызова = половина окна; `LoopWatchdog` (Shared.Core, arch/14 §6) на
  всех циклах.
- **Секреты вне etcd**: per-install env-профили `S3W_NODE_SECRETS__*`,
  значения никогда не попадают в etcd/журнал; mTLS-only API.
- **Производство-ready** внутри контура, но система домашняя:
  enterprise-защиты не проектируются.

## 4. Компоненты (структура кода)

Новые проекты в solution-папке `/s3/` (`src/PgWorker.slnx`), по образцу
ValkeyWorker/KafkaWorker:

| Проект | Содержимое |
|---|---|
| `src/S3Worker.Core` | модель домена: `S3ClusterSnapshot` (config + nodes: placement/resources/state/placed_on/usage/endpoints), `S3NodeState`; ownS3-клиент ноды: интерфейс `IOwnS3NodeClient` + реализация (health `/healthz` — HttpClient; bucket-операции — AWSSDK.S3 по образцу `AdminPanel.Probes/S3/MinioS3Client` и `PgWorker.Backups/BackupS3`; usage `GET /admin/storage` — SigV4 GET по конвейеру подписи owns3/05 §5) |
| `src/S3Worker.Etcd` | `S3SnapshotParser`: чтение префикса `/s3/` в снапшот; unknownKeys-толерантность, битый JSON — parseError без падения тика (arch/23 §2/§9) |
| `src/S3Worker.Docker` | `ClusterDriver` над `Shared.Docker`: создание/снос контейнеров `s3w-<C>-node<k>` и volumes `s3w-<C>-node<k>-data`, инспекция (положительное свидетельство S7), порт-публикации |
| `src/S3Worker.Provisioning` | процессы A–E (§5 ниже), `PlacementResolver` (arch/23 §4), `PortAllocIndex`/`PortAllocHealer` (t90-паттерн, образцы ValkeyWorker), `SecretProfileStore` (env `S3W_NODE_SECRETS__<PROFILE>__*`), ротация секретов |
| `src/S3Worker.App` | `Program` (DI, fail-fast валидации), `Options` (arch/24 §8), Loops (`ReconcileLoop` тик 5 с, `KeepaliveLoop`), `Api/Operations` (7 эндпоинтов §6), `TlsEndpoints` (mTLS-грань), healthz, метрики `s3worker_*` |

Поставка и инфраструктура:

- `docker/S3Worker.Dockerfile` — по правилам образов воркеров: сборка
  publish на хосте, образ — runtime-слой с `COPY` publish-вывода
  (AGENTS.md: без sdk-стадии/копирования исходников).
- `deploy/docker-compose.yml` — сервисы `s3worker` + `s3worker-2`
  (x-s3worker-env/base merge-якоря по образцу pgworker/kafkaworker/
  valkeyworker): docker.sock, общий TLS-volume `pgw-api-tls` (per-install
  пакет `kfw-install-ca`, серверный серт API — env-фоллбек
  `S3W_API_TLS_*` до появления ключа `/workers/api_tls/s3writer` — его
  пишет панель t34), уникальные host-порт API и `Api:AdvertiseUrl`
  per инстанс, уникальность — случайный `InstanceId` (arch/24 §2);
  env-профили секретов из `deploy/.env`.
- `deploy/.env.example` — дополнить: `S3W_NODE_SECRETS__<PROFILE>__*`,
  `S3W_API_TLS_*`, `S3W_BUCKET`, endpoints etcd, AdvertisedHost.
- `src/tests/S3Worker.UnitTests`, `src/tests/S3Worker.IntegrationTests`
  (+ `E2e/` внутри IntegrationTests по образцу ValkeyWorker/E2eFixture).

## 5. Процессы (машины состояний, arch/24 §5)

Классификация тика: `config.state=NOT_INITIALIZED` → A;
`TO_REMOVE` → B; иначе Active-ветка: надзор C → add-node D → remove-node
E. Все операции — только под живым клэймом `<C>`; journal —
`/s3worker/work/<C>`; шаги идемпотентны (перепроверка факта).

### A. ProvisioningProcess (P0–P5)

- **P0**: claim `<C>` + journal `op=provision`; перечитка config (гонка
  `TO_REMOVE` посреди provisioning — безопасная остановка).
- **P1**: резолв placement всех нод (селектор → кандидаты-хосты →
  детерминированный выбор least-loaded по числу занятых портов/тасков,
  tie-break по имени; ноль кандидатов — transient-фейл фазы, ретрай
  тиком; journal с перечнем кандидатов) → `placed_on`; portalloc — под
  глобальным клэймом `/s3worker/locks/portalloc` (не взял — journal
  `waiting-portalloc-lock`, следующий тик); валидация профиля секретов
  (env воркера; неполный — permanent-отказ фазы с диагностикой).
- **P2**: создание нод: volume `s3w-<C>-node<k>-data` + контейнер
  `s3w-<C>-node<k>` (образ `S3Worker:Docker:Images:OwnS3`; env: креды
  профиля `OWNS3_ROOT_*`/`OWNS3_ACCESS_KEYS__<i>__*`, `OWNS3_DATA_DIR=/data`;
  лимиты cpu/mem из resources; контейнерный S3-порт 9000 → host-порт из
  portalloc; restart-политика `unless-stopped`; опция ContourNetwork —
  ноды в сети контура поставки); health-ожидание — бюджет
  `NodeBootSec` (≤100 с), transient-толерантный цикл.
- **P3**: bucket per-install (`S3Worker:Bucket`) на каждой ноде —
  идемпотентно (exists → пропуск; CreateBucket root-кредом профиля).
- **P4**: no-op до t35 (mesh-синхронизация — механика ownS3, arch/23 §5).
- **P5**: put `endpoints` (advertised host:S3-порт всех нод) + config:
  txn `compare mod_revision` → put канонического JSON без `state`;
  journal done.

### B. DeprovisioningProcess (X0–X3)

X0 claim + journal `op=deprovision` → X1 маркеры `REMOVING` всем нодам →
X2 docker: снос контейнеров и volumes на всех хостах `Hosts[]`
(404 = ок) + чистка `/s3worker/portalloc/<C>`; порядок «сначала docker,
потом etcd» → X3 etcd: `del --prefix /s3/clusters/<C>/` + del
`/s3worker/{work,portalloc,rotations}/<C>`; journal done. Снос кластера
уничтожает данные бэкапов его нод — осознанное действие оператора
(risk R2 arch/24 §9).

### C. NodeSupervisor (надзор, тик)

- health-проба каждой ноды `/healthz` по `host:port` из portalloc;
- снесённый контейнер (положительное свидетельство docker-инспекции,
  S7) → пересоздание на том же хосте (`placed_on` фиксирован; volume
  сохраняется), `state=PROVISIONING`; в `RUNNING` переводит следующий
  цикл по health; **одно пересоздание за тик**;
- недоступная нода (проба молчит дольше `NodeDeadSec`) →
  `state=UNREACHABLE` + ретраи тиками; деструктивных действий по факту
  «не отвечает» нет;
- сбор `usage` (`HealthIntervalSec`): `GET /admin/storage` ownS3 →
  `{"used_bytes","total_bytes","updated_unix"}` в
  `nodes/node<k>/usage`;
- слепая проба (docker-хост молчит) — никаких действий;
- мутация ресурсов (PUT resources, arch/24 §1.1): cpu/mem —
  rolling-пересоздание контейнера надзором (образец kafka-мутация №15);
  disk — обновление декларации (квота-наблюдение через usage, алерты —
  t34).

### D. AddNodeProcess

Заявка (`nodes/node<max+1>/state=NOT_INITIALIZED`): подъём по A
(placement/portalloc/volume/контейнер/bucket) + RMW `endpoints`; финал —
`state=RUNNING`. Mesh+backfill — с t35.

### E. RemoveNodeProcess

Маркер `state=TO_REMOVE`: гварды (не последняя нода кластера; при
мёртвых остальных — нет живой RUNNING-ноды кроме выводимой — отказ
фазы) → остановка/снос контейнера и volume → чистка ключей ноды и
записи portalloc → RMW `endpoints`. До t35 репликации нет — объекты
выводимой ноды теряются (risk R1, осознан).

### Ротация секретов

Заявка `/s3worker/rotations/<C>` (POST rotations): rolling-пересоздание
нод с новым env профиля — по одной за тик (volume не трогается), по
завершении del заявки.

### F. SyncProbeProcess — НЕ реализуется (t33)

В t32 — только 202-заглушка эндпоинта (§6); ключи
`/s3/clusters/<C>/{connectivity,sync}` не пишет никто.

## 6. HTTP API (arch/24 §1.1)

Та же mTLS-грань, что `/healthz` и `/metrics` (Kestrel, серверный серт —
env-фоллбек `S3W_API_TLS_*` до ключа `/workers/api_tls/s3worker`,
пишет панель t34; клиенты — клиентские серты per-install `kfw-install-ca`;
без валидного серта — отказ на TLS-хендшейке). Дискавери:
`/s3worker/api/<id>` ставит сам инстанс (`Api:AdvertiseUrl`,
`cert_thumbprint`).

| Метод+путь | Поведение |
|---|---|
| `POST /api/s3/clusters` | создание: клэйм-txn `version(config)==0` + пакет PUT (config + nodes × {placement, resources, state=NOT_INITIALIZED}); сбой — компенсация `del --prefix /s3/clusters/<C>/`; 201 / 409 (есть) / 400 (валидация) / 422 (secret_profile) |
| `DELETE /api/s3/clusters/{c}` | перевод `config.state=TO_REMOVE` (RMW по mod_revision); already → 204 без записи; 404 |
| `POST /api/s3/clusters/{c}/nodes` | добавление ноды `node<max+1>`: клэйм-txn + put placement/resources/state=NOT_INITIALIZED; гварды: кластер Active, nodes < 9 |
| `DELETE /api/s3/clusters/{c}/nodes/{n}` | маркер `nodes/<n>/state=TO_REMOVE` (one-way); гварды: не последняя нода; 404/409 |
| `PUT /api/s3/clusters/{c}/nodes/{n}/resources` | мутация ресурсов (cpu/mem rolling; disk — декларация) |
| `POST /api/s3/clusters/{c}/rotations` | заявка ротации `/s3worker/rotations/<C>` (клэйм-txn); повтор — идемпотентен |
| `POST /api/s3/clusters/{c}/sync-probe` | **заглушка t32**: 202 Accepted, no-op; исполнение — t33 |

Валидация (сервер — источник истины): `name` `^[a-z][a-z0-9_]{0,62}$`;
`platform` = `"owns3"`; `nodes` 1..9; `placement` — обязательный валидный
селектор каждой ноды (400); `resources` — cpu 0.01..64, mem/disk
1..65536 GiB (форматы adminpanel/02 §9.3); `secret_profile` — непустое
имя из env воркера (422 с перечнем доступных имён, без значений).

Seed-эндпоинт — `EnableSeedEndpoint=false` default (паттерн pg §1.1.1;
при необходимости стендового сида — включается конфигом).

## 7. Docker-механика (arch/24 §2)

- Транспорт — общий клиент `Shared.Docker` (plain/swarm; env
  `S3W_DOCKER_TLS_{CA,CERT,KEY}`, `S3W_DOCKER_SSH_KEY[_PATH]`,
  `S3W_DOCKER_SSH_FINGERPRINT`); конфиг `S3Worker:Docker { Mode,
  Hosts[{Name,Endpoint}], SwarmManager, PortRange{From,To},
  Images{OwnS3="owns3:dev"}, Tls, Ssh, ContourNetwork, AdvertisedHost }`.
- Placement-резолюция: селектор → предикат равенства по атрибутам хоста
  (plain: `docker_host` из Hosts[]; swarm: `swarm_node`/`swarm_labels`
  из `GET /nodes`); повторные проходы идемпотентны (тот же хост, пока
  селектор не изменён выводом ноды); анти-аффинити автоматикой не
  вводится.
- Portalloc: контейнерный 9000 → host-слоты `PortRange`; закрепление
  `/s3worker/portalloc/<C>` (переживает rebuild); довыделение — только
  держателем глобального клэйма `/s3worker/locks/portalloc`; занятость =
  docker-публикации ∪ portalloc всех кластеров.
- Advertised-адреса — от фактических публикаций (`AdvertisedHost` —
  single-host/стенд; multi-host — фактические имена хостов); значение
  обязано резолвиться клиентами (попадает в `endpoints`).
- Сеть per-cluster НЕ создаётся (arch/24 §2); `ContourNetwork` (стенд/E2E)
  — ноды в сети контура поставки, сеть воркером не создаётся/не удаляется.
- Имена docker-объектов: контейнер `s3w-<C>-node<k>`, volume
  `s3w-<C>-node<k>-data` (детерминированы); restart `unless-stopped`.

## 8. Секреты (arch/23 §6, arch/24 §4)

- Per-install env-профили:
  `S3W_NODE_SECRETS__<PROFILE>__ROOT_USER`/`__ROOT_PASSWORD`;
  `S3W_NODE_SECRETS__<PROFILE>__ACCESS_KEYS__<i>__{ACCESSKEY,SECRETKEY,POLICY}`
  (`read-write`|`read-only`). Профиль един на кластер
  (`config.secret_profile`).
- Передача — env контейнера при создании; значения никогда не попадают в
  etcd/журнал/лог.
- Отсутствие/неполнота профиля — permanent-отказ фазы P1 с диагностикой;
  API — 422 с перечнем доступных имён.
- Ротация — rolling-пересоздание (§5).

## 9. Наблюдаемость (arch/24 §7)

- `/healthz` (mTLS-грань): `etcd-reachable`, `docker-hosts` (per-host
  ping), `loops-alive`, `claims`.
- `/metrics` (тот же порт, каркас arch/18, Meter `s3worker_*`): циклы,
  клэймы, фазы процессов (journal-события через `WorkJournal.PhaseWritten`
  → `WorkerMetricsInstrumentation` — seam как у ValkeyWorker), операции
  API, счётчик unknownKeys.
- Диагностика фаз — `/s3worker/work/<C>` + `nodes/node<k>/state`;
  телеметрия нод — метрики ownS3 (scrape-механика — вне задачи).

## 10. Стенд

- **deploy**: `deploy/docker-compose.yml` — `s3worker` + `s3worker-2`
  (§4); сборка образа `s3worker:dev` — publish на хосте + compose build
  (как pgworker/valkeyworker); `deploy/.env.example` — новые S3W_-переменные.
- **dev-stand**: полный стенд (`dev-stand/adminpanel/checks/00-up.sh`)
  поднимает s3worker-сервисы в стендовой сети с единым etcd-контуром
  (`as-etcd-1/2/3`) — строка подъёма deploy-проекта дополняется
  (`... up -d --build pgworker pgworker-2 owns3 s3worker s3worker-2`),
  publish-фаза S3Worker.App с `[PHASE]`-меткой; демо-профиль секретов в
  `.env` стенда.
- **checks**: новый чек `dev-stand/adminpanel/checks/<очередной номер>-s3worker.sh`
  по образцу существующих: healthz/metrics воркера, демо-кластер через
  API (создание → RUNNING → endpoints → снос), зачистка за собой.

## 11. Тесты

### Юнит (S3Worker.UnitTests)

- `S3SnapshotParser`: канонические примеры значений arch/23 §2.1 (критерий
  приёмки парсеров), unknownKeys, битый JSON — parseError без падения,
  state-семантика «отсутствие = Active».
- Placement-резолвер: селектор → кандидаты; ноль — transient; детерминизм
  (least-loaded, tie-break); идемпотентность повторных проходов.
- Порт-аллокатор: довыделение под клэймом, занятость
  публикации ∪ portalloc, healer (E9-лестница: утеря журнала →
  реконструкция из inspect → новая аллокация).
- Гварды процессов: E (последняя нода / мёртвые остальные), ротация,
  классификатор тика (A/B/Active-ветка).
- Валидации API: name/platform/nodes/placement/resources/secret_profile
  (400/422).

### Интеграционные (S3Worker.IntegrationTests)

- API-контур (mTLS, тестовая PKI по образцу ValkeyWorker `TestPki`):
  создание/удаление/валидации/конфликт-кейсы клэйм-txn, компенсация при
  сбое пакета PUT.
- Координация: клэймы/leader/portalloc-lock против etcd-фикстуры.

### E2E (docker, IntegrationTests/E2e)

Фикстура по канонам `docs/e2e-isolation.md` + `docs/e2e-launch.md` и
эталону E2eFixture/ValkeyE2eEnvironment: guid-идентификатор во всех
именах; свой etcd (testcontainer, динамический порт); сборка Release
**на хосте** (`dotnet publish` — автосборка до старта контура, отказ
сборки — fail-fast; `PGW_TEST_E2E_NOBUILD=1` — только бисект-лазейка);
образы `s3worker:e2e-<guid>` (runtime+publish-COPY) и `owns3:dev`
(publish `OwnS3.App` на хосте → docker build); docker.sock; динамические
host-порты (`assignRandomHostPort` + `GetMappedPublicPort`); телеметрия —
docker-логи/inspect и host.log в `/tmp/pgw-e2e-artifacts-<guid>/` ДО
удаления, `[PHASE]`-метки для фаз длиннее 60 с, упавший сценарий —
`MarkFailed()` + остановленные (не удалённые) контейнеры +
`README-cleanup.txt`; `NodeBootSec`-бюджеты ≤ 100 с; полный teardown при
любом исходе + **ассерт чистоты** (не осталось контейнеров/volumes/сетей
префикса guid, ключей etcd-префикса).

Сценарии (минимум):

1. **Provisioning**: заявка через API → кластер N=2 → P0–P5:
   `config` без `state`, ноды `RUNNING`, `placed_on`, `endpoints` в etcd
   (формат arch/23 §2.1), контейнеры/volumes существуют, bucket создан
   на каждой ноде (проверка S3-клиентом), health жив.
2. **Идемпотентность**: повторные тики — нет дублирующих действий
   (порт-аллокации стабильны, состояния не мигают).
3. **AddNode**: node2 → node3 → `RUNNING`, RMW `endpoints` (новая нода
   в списке, прежние на месте).
4. **RemoveNode**: маркер → гварды → снос контейнера/volume, чистка
   portalloc/ключей, RMW `endpoints`; последняя нода — 409.
5. **Deprovision**: `TO_REMOVE` → X0–X3 → etcd-префикс кластера пуст,
   контейнеров/volumes нет (ассерт чистоты).
6. **Надзор**: `docker rm -f` контейнера ноды → пересоздание на том же
   `placed_on`, `PROVISIONING` → `RUNNING`, volume (данные) сохранён;
   `docker pause` (контейнер running, health молчит) → `UNREACHABLE`
   по `NodeDeadSec`, деструктива нет;
   `usage`-ключ обновляется.
7. **Takeover**: два инстанса воркера; kill держателя клэйма → второй
   подхватывает процесс с journal-фазы ≤ TTL 15 с + тик (доводка
   прерванного provisioning).
8. **Ротация**: заявка → rolling-пересоздание по одной ноде за тик →
   del заявки; креды в контейнере обновлены (по факту env инспекции —
   без печати значений).
9. **sync-probe**: 202, ключи `connectivity`/`sync` не появляются.
10. **API-валидации**: 400/404/409/422-кейсы (§6).

Мерж-гейт задачи (AGENTS.md): задачи, трогающие код воркеров, прогоняют
docker-E2E на свежем Release; для t32 — полный прогон E2E-серии
S3Worker (задача создаёт новую серию).

## 12. Фазы работ

1. **Arch-правки** — выполнены (§2): arch/23, arch/24, roadmap/s3.md,
   roadmap/README.md.
2. **Каркас**: проекты S3Worker.{Core,Etcd,Docker,Provisioning,App} в
   slnx; Options + fail-fast валидации старта; Program/DI; координация
   (ClaimStore/PortAllocLock/WorkJournal — Shared.Etcd, префикс
   `/s3worker`); mTLS-грань + healthz + метрики; Loops (ReconcileLoop
   тик, Keepalive, Watchdog).
3. **Чтение домена**: `S3SnapshotParser` (юниты на канонических
   примерах arch/23 §2.1).
4. **Docker-слой**: ClusterDriver, PlacementResolver, PortAllocIndex/
   Healer (+юниты/интеграция).
5. **Клиент ownS3**: health/bucket/usage (+юниты на моках, контракты
   таймаутов).
6. **Процессы**: A → C → B → D → E → ротация → resources-мутация
   (+юниты; интеграционные прогоняет реальный контур).
7. **HTTP API**: 7 эндпоинтов, валидации, компенсации (+интеграционные
   API-тесты).
8. **Поставка**: Dockerfile, deploy compose ×2, .env.example.
9. **Стенд**: 00-up.sh, dev-stand compose, чек.
10. **E2E**: фикстура + сценарии §11; мерж-гейт — свежий Release.

Порядок фаз 2–7 допускает перестановку внутри слоя при сохранении
«юниты раньше интеграции»; фазы 8–10 — после устойчивых процессов.

## 13. Ограничения и границы (что НЕ входит в t32)

- **Mesh-репликация ownS3** (доставка, backfill, конфиг per-install) —
  t35; в t32 кластеры N≥2 валидны без синхронизации, P4 — no-op.
- **Зонд синхронности/связности** (процесс F, ключи `connectivity`/
  `sync`) — t33; в t32 — 202-заглушка.
- **Панель домена S3** (глава adminpanel/02, UI, алерты) — t34.
- **Клиентская интеграция пула** (запись в любой/чтение по
  приоритетам) — t36.
- **Scrape метрик нод ownS3** — вне канона и задачи.
- **Квоты диска enforcement** — примитива нет (arch/23 §2): disk —
  декларация + наблюдение `usage`; алерты — t34.
- **MinIO** — платформа снята с канона (§2); кластеров minio не
  существует.

## 14. Критерии приёмки

1. Канон обновлён arch-first (§2) и код ему не противоречит: ключи
   `/s3/`, `/s3worker/` — форматы arch/23 §2/§8 (канонические примеры
   §2.1 — критерий приёмки парсеров); `/s3/` и `/s3worker/` пишет
   только S3Worker (панель/ сиды — через API).
2. Заявка `POST /api/s3/clusters` (platform=owns3, N 1..9, валидные
   placement/resources/secret_profile) → кластер доезжает до Active:
   `config` без `state`, ноды `RUNNING`, `endpoints` записаны,
   bucket per-install существует на каждой ноде.
3. Все операции идемпотентны: повторный тик/повторный запрос — no-op
   по факту; прерывание процесса между фазами — доводка ретраем тика
   по journal-фазе (проверено E2E-сценарием прерывания/takeover).
4. Takeover: смерть инстанса-держателя → продолжение вторым инстансом
   ≤ TTL 15 с + тик; двойной контроллер невозможен (клэймы).
5. Deprovision (`TO_REMOVE`) и RemoveNode оставляют чистые etcd и
   docker (контейнеры, volumes, portalloc) — ассерт чистоты E2E.
6. Надзор: снесённый контейнер пересоздаётся на `placed_on` с
   сохранением volume; молчащая нода → `UNREACHABLE` (деструктива нет);
   `usage` обновляется периодически.
7. Секреты: значения профилей не появляются в etcd, логах, журнале,
   метриках; ротация — rolling, заявка гасится по завершении.
8. API-валидации: name/platform/nodes/placement/resources/
   secret_profile → 400/422 по §6; sync-probe → 202 (no-op).
9. Стенд: полный подъём `00-up.sh` поднимает s3worker×2; чек
   s3worker зелёный; deploy compose соответствует правилам поставки
   (docker.sock, TLS-volume, уникальные порты/AdvertiseUrl).
10. E2E-серия §11 зелёная на свежем Release (мерж-гейт), с полной
    самоочисткой и телеметрией по e2e-launch/e2e-isolation; серии
    прогонов не оставляют осиротевших сетей/контейнеров.
11. `TreatWarningsAsErrors=true` — сборка без предупреждений;
    `BrokerBootSec`-аналоги (NodeBootSec ≤ 100 с) в фикстурах.

## 15. Риски

| # | Риск | Митигация |
|---|---|---|
| 1 | До t35 реплик нет: гибель диска ноды/вывод ноды — потеря её доли (arch/24 R1) | риск осознан каноном; рекомендация 2–3 ноды; алерты — t34; mesh — t35 |
| 2 | Снос кластера/ноды уничтожает данные (arch/24 R2) | осознанное действие оператора; confirm — панель t34; риск-строка канона |
| 3 | OwnS3-админ-грань `/admin/storage` — нефункциональная деградация usage | usage-сбор transient-толерантен (ретраи тиками), не блокирует остальные функции надзора |
| 4 | E2e-долгота (provisioning нескольких нод + ownS3 boot) | бюджеты NodeBootSec ≤ 100 с, `[PHASE]`-метки, fast-fail; по правилам — без «тихих» фаз |
| 5 | Расширение контракта ключами t33/t34 после t32 | unknownKeys-толерантность канона (arch/23 §2); форматы §3 расширяемы |

## 16. Открытые вопросы

Нет — все ключевые развилки закрыты решениями пользователя (§1).
