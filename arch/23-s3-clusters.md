# 23. S3-кластеры хранилища бэкапов: контракт etcd (контроль-плейн + дискавери) ★

Канон ключей S3-домена: контроль-плейн кластеров `/s3/` (декларирует панель
AdminPanel через HTTP API воркера — исполняет S3Worker, канон —
[24-s3worker.md](24-s3worker.md)), координация воркера `/s3worker/` и
**клиентский дискавери** (`endpoints`). Назначение домена — **пул
S3-серверов хранилища бэкапов с гео-распределением** (привязка нод к
docker/swarm-хостам) и mesh-репликацией между нодами; потребитель —
подсистема бэкапов ([19-backups.md](19-backups.md)); для клиентов пул —
обычный S3 (§7). PgWorker — клиент пула (читатель `endpoints`), участником
контракта не является.

Имена: кластер `<C>` — `^[a-z][a-z0-9_]{0,62}$` (как pg/kafka/valkey; без
дефиса); ноды `node1..nodeN` (имя генерирует панель, `node<max+1>`, ≤ 9).

## 1. Транспорт: HTTP JSON gateway `/v3/*`

Как панель и PgWorker ([adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md)
§1, [14-pgworker.md](14-pgworker.md) §3): `HttpClient` против gRPC-gateway
etcd (JSON+base64), `POST /v3/kv/range` / `/v3/kv/put` / `/v3/kv/txn` /
`/v3/lease/*`. Один общий etcd-контур со стендом. **Poll, без watch** —
тик воркера 5 с / панели 3 с покрывают динамику.

Два корневых префикса: `/s3/` (контроль-плейн кластеров) и `/s3worker/`
(координация воркера; панель читает избирательно — §8).

## 2. Ключи кластера `/s3/clusters/<C>/`

| Ключ | Формат значения | Пишет | Примечание |
|---|---|---|---|
| `config` | JSON `{"platform":"owns3","nodes":N,"secret_profile":"<имя>","created_unix":T,"state"?:"NOT_INITIALIZED"\|"TO_REMOVE"}` | панель через API воркера (создание, TO_REMOVE), воркер (снимает `state` после инициализации, актуализирует `nodes` при изменении состава (D/E), txn по `mod_revision`) | `state` — только у невыполненных заявок: отсутствие = Active (семантика pg/kafka/valkey); `platform` — единственное значение `"owns3"` (фиксируется при создании навсегда; точка расширения контракта); `nodes` — плановое число нод при создании (заявка панели), после D/E — поддерживается воркером равным фактическому составу, 1..9 (структурный предел; операционная рекомендация 2–3 — гео-минимум и бюджет R1); `secret_profile` — имя per-install env-профиля кредов нод (§6; секреты вне etcd) |
| `nodes/node<k>/placement` | JSON-селектор атрибутов хоста: `{"docker_host":"h1"}` (plain) \| `{"swarm_node":"n1"}` \| `{"swarm_labels":{"geo":"dc1",...}}` \| комбинация атрибутов | панель через API воркера | явная привязка ноды; модель «хост = набор атрибутов» (§4); привязка = предикат равенства по всем указанным атрибутам; изменение привязки живой ноды — только выводом/перевозом ноды (перепривязка на месте не допускается: данные тома привязаны к хосту) |
| `nodes/node<k>/resources` | JSON `{"cpu":"2","mem":"4Gi","disk":"200Gi"}` | панель через API воркера (мутация ресурсов) | форматы — канон pg ([adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md) §9.3; kafka — §10.3): cpu — десятичные ядра 0.01..64, mem/disk — целые GiB, в etcd `"<n>Gi"`; cpu/mem — лимиты контейнера; `disk` — целевой объём volume данных ноды (декларация: создание volume и операционная квота-наблюдение; жёсткого enforcement-примитива у docker-volume нет — занятость наблюдается `usage` и алертами панели t34) |
| `nodes/node<k>/state` | строка `NOT_INITIALIZED`\|`PROVISIONING`\|`RUNNING`\|`UNREACHABLE`\|`REMOVING`\|`TO_REMOVE` | `NOT_INITIALIZED`/`TO_REMOVE` — **только панель** (заявка / one-way маркер демонтажа ноды); остальные — воркер | жизненный цикл ноды — семантика kafka/valkey; `UNREACHABLE` ставит надзор по health-пробам (платформенные health-грани — §5) |
| `nodes/node<k>/placed_on` | строка — имя docker-хоста `"h1"` | воркер (при резолвции placement) | фактическое размещение после резолвции селектора (§4); переживает rebuild (том ноды живёт на этом хосте) |
| `nodes/node<k>/usage` | JSON `{"used_bytes":N,"total_bytes":N,"updated_unix":T}` | воркер (надзор, периодически) | занятость диска ноды из платформенной админ-грани/метрик; кормит квота-наблюдение `resources.disk` и панельные алерты (t34) |
| `endpoints` | строка `"h1:p1,h2:p2,..."` | воркер (после подъёма; RMW при изменении состава) | клиентские адреса пула (advertised host + S3-порт из `/s3worker/portalloc/<C>`) — **точка дискавери клиентов** (t36) |

Неизвестные ключи внутри `/s3/` — не ошибка: лог + счётчик `unknownKeys`
читателя (как pg/kafka/valkey; система развивается, парсеры не падают).

### 2.1. Канонические примеры значений (критерий приёмки парсеров)

`config` при создании (заявка, панель через API воркера):

```json
{"platform":"owns3","nodes":3,
 "secret_profile":"backups-main",
 "created_unix":1760156800,"state":"NOT_INITIALIZED"}
```

`config` Active-кластера (после provisioning — поле `state` снято воркером):

```json
{"platform":"owns3","nodes":3,
 "secret_profile":"backups-main",
 "created_unix":1760156800}
```

`config` с заявкой удаления: то же + `"state":"TO_REMOVE"`.

`placement` — plain: `{"docker_host":"h1"}`; swarm: `{"swarm_node":"n1"}` и
`{"swarm_labels":{"geo":"dc1"}}` (комбинация атрибутов — конъюнкция, §4).

`resources`: `{"cpu":"2","mem":"4Gi","disk":"200Gi"}`.

`state`: `"RUNNING"`; `placed_on`: `"h1"`.

`usage`:

```json
{"used_bytes":107374182400,"total_bytes":214748364800,
 "updated_unix":1760156860}
```

`endpoints`: `"h1:18001,h2:18002,h3:18003"` (advertised host + S3-порт из
`/s3worker/portalloc/<C>`).

## 3. Статусы наблюдения (связность и синхронность)

Контракт ключей фиксируется этим каноном; запись реализуется задачами трека
([roadmap/s3.md](roadmap/s3.md): зонд — t33, процесс F
[24-s3worker.md](24-s3worker.md) §5).

| Ключ | Формат | Кто пишет |
|---|---|---|
| `/s3/clusters/<C>/connectivity` | JSON `{"state":"OK"\|"DEGRADED"\|"DOWN","checked_unix":T,"links":{"node1":{"node2":"ok"\|"offline",...},...},"probe_error"?}` | зонд t33 (периодически + по команде API): связность нод между собой — PUT-пробы межнодовых линков (health-проба недоступность записи не доказывает: детект недоступности строится на PUT-таймауте и переборе — R3 `docs/owns3-replication-requirements.md`; параллель MinIO — факт 8 `docs/s3-replication.md`) |
| `/s3/clusters/<C>/sync` | JSON `{"state":"SYNCED"\|"LAGGING"\|"DIVERGED","checked_unix":T,"pairs":[{"from":"node1","to":"node2","missing":N,"extra":N,"lag_bytes"?,"lag_objects"?}...],"last_error"?}` | зонд t33: сверка содержимого bucket между нодами (list/хвосты; штатные метрики репликации лаг данных не отражают — факт 7 `docs/s3-replication.md`; админ-грань ownS3 «стартовая позиция/хвост префикса per-target» — R5 `docs/owns3-replication-requirements.md`) |

`state`-агрегаты: connectivity `DOWN` — нет ни одного живого линка записи;
`DEGRADED` — часть линков offline; sync `LAGGING` — догон известен и
мал/растёт; `DIVERGED` — устойчивое расхождение/дыра (алерты панели — t34).

Форматы толерантны к расширению полей: парсеры панели читают известные
поля, новые не ломают чтение.

Состав/здоровье нод наблюдается `nodes/*/state` + `usage` — отдельного
health-ключа нет: состояние живости несёт `state`, детальная телеметрия —
метрики [18-metrics.md](18-metrics.md).

`connectivity` OK-кластера из 3 нод (линки симметричны, проба в обе
стороны):

```json
{"state":"OK","checked_unix":1760156900,
 "links":{"node1":{"node2":"ok","node3":"ok"},
          "node2":{"node1":"ok","node3":"ok"},
          "node3":{"node1":"ok","node2":"ok"}}}
```

`sync` SYNCED (пары без расхождений):

```json
{"state":"SYNCED","checked_unix":1760156900,
 "pairs":[{"from":"node1","to":"node2","missing":0,"extra":0},
          {"from":"node1","to":"node3","missing":0,"extra":0},
          {"from":"node2","to":"node3","missing":0,"extra":0}]}
```

## 4. Модель размещения (атрибуты хостов, предикат привязки)

- **Хост — набор атрибутов**: plain-режим — атрибут `docker_host` (имя из
  таблицы `S3Worker:Docker:Hosts[]` воркера, [24-s3worker.md](24-s3worker.md)
  §2); swarm — атрибуты `swarm_node` (hostname ноды swarm) и `swarm_labels`
  (`node.labels.*` — произвольные пары ключ-значение, в т.ч. гео-метки
  площадок).
- **Привязка ноды = предикат**: `placement`-селектор — конъюнкция равенств
  по атрибутам; нода кластера обязана иметь селектор (валидация API: пустой
  или невалидный — 400).
- **Резолюция** (воркер, фаза provisioning): по селектору — список
  кандидатов-хостов. Ноль кандидатов — transient-фейл фазы (journal, ретрай
  тиком: хост может появиться в конфигурации). Несколько кандидатов —
  детерминированный выбор (least-loaded по числу занятых портов/тасков,
  tie-break по имени; journal-запись с перечнем кандидатов). Результат —
  `placed_on`; повторные проходы идемпотентны (тот же хост, пока селектор
  не изменён выводом ноды).
- **Анти-аффинити** по отношению к другим нодам того же кластера НЕ
  вводится автоматикой: раснос по площадкам — обязанность оператора
  (гео-схема — явные селекторы); канон фиксирует рекомендацию «ноды одного
  кластера — на разных хостах/площадках» и панельную индикацию совпадений
  (t34).

## 5. Платформа нод

- **owns3** — единственная платформа пула: образ `owns3:dev` (правила
  E2E-образов: сборка .NET на хосте, publish-COPY, в registry не
  кладётся); нода — ownS3 single-drive (`OWNS3_DATA_DIR=/data`); bucket
  per-install ([19-backups.md](19-backups.md) §5) на каждой ноде
  (CreateBucket admin-кредом, идемпотентно);
  mesh-синхронизация — внутренняя механика ownS3 (конфиг per-install —
  R6 `docs/owns3-replication-requirements.md`; реализация и глава
  канона `arch/owns3/07-replication.md` — t35): до неё кластеры с N≥2
  декларативно валидны, но синхронизации нет (статусы `sync` это
  отразят). Health — `/healthz`
  ([owns3/05-access-config.md](owns3/05-access-config.md) §5);
  занятость диска — админ-грань `GET /admin/storage` (там же).
  Удаления ownS3 — физические (versioning отсутствует): ретенция
  arch/19 §4 работает напрямую, чистки версий не требуется.
- **Общее**: S3-порт контейнера 9000 → host-порт из portalloc; bucket
  per-install — единое имя на всех нодах кластера (клиент с одним
  комплектом кредов работает с любой нодой); создание/поддержание bucket —
  обязанность оркестратора (идемпотентно).

## 6. Секреты (контрактная часть)

- Креды нод — per-install env-профили воркера, вне etcd и git (направление
  [14-pgworker.md](14-pgworker.md) §4 / [19-backups.md](19-backups.md) §7):
  профиль = имя + root-пара + опциональные статические access keys
  (read-write для записи бэкапов, read-only для панели — симметрия
  действующего стенда, [owns3/05-access-config.md](owns3/05-access-config.md)
  §6).
- Кластер ссылается на профиль полем `config.secret_profile`; профиль един
  на все ноды кластера (клиент пишет в любую ноду одним ключом;
  mesh-настройка — один комплект).
- Формат env — по прецеденту `OWNS3_ACCESS_KEYS__<i>__*` (owns3/05 §2);
  имена переменных профиля:
  - `S3W_NODE_SECRETS__<PROFILE>__ROOT_USER`,
    `S3W_NODE_SECRETS__<PROFILE>__ROOT_PASSWORD`;
  - `S3W_NODE_SECRETS__<PROFILE>__ACCESS_KEYS__<i>__ACCESSKEY`,
    `S3W_NODE_SECRETS__<PROFILE>__ACCESS_KEYS__<i>__SECRETKEY`,
    `S3W_NODE_SECRETS__<PROFILE>__ACCESS_KEYS__<i>__POLICY`
    (`read-write`\|`read-only`).
- В ноды передаётся env контейнера:
  `OWNS3_ROOT_USER`/`OWNS3_ROOT_PASSWORD` + `OWNS3_ACCESS_KEYS__<i>__*`.
- Отсутствие/неполнота профиля — отказ фазы provisioning с диагностикой
  (валидация API — 422 по факту env воркера).
- Ротация — заявка `/s3worker/rotations/<C>` (§8): rolling-пересоздание
  нод с новым env профиля; поддержка рестарт-семантики платформ: ownS3 —
  переконфигурация env + рестарт
  ([owns3/05-access-config.md](owns3/05-access-config.md) §2).

## 7. Клиентская грань пула (границы для t36)

Пул для клиентов — `endpoints`-набор обычных S3-серверов: запись — перебор
«любой доступный» с per-попыточным таймаутом PUT (health-проба
недоступность не доказывает — факт 8 `docs/s3-replication.md`; быстрый
отказ/успех, не молчание — R3
`docs/owns3-replication-requirements.md`); чтение/восстановление — по
приоритетам per-cluster (страховка на лаг доставки); синхронизация между
серверами — не забота клиентов. Layout объектов —
[19-backups.md](19-backups.md) §5 без изменений. Канон фиксирует границу;
реализация и правки arch/19/arch/14 — t36.

## 8. Координация воркера `/s3worker/`

Порт схемы `/kafkaworker/` ([15-kafka-clusters.md](15-kafka-clusters.md)
§4) / `/valkeyworker/` ([20-valkey-clusters.md](20-valkey-clusters.md) §3)
один в один, свой префикс:

| Ключ | Тип | Назначение |
|---|---|---|
| `/s3worker/leader` | lease TTL 15 с | лидер singleton-процессов |
| `/s3worker/claims/<C>` | lease TTL 15 с | пер-кластерный клэйм (exclusivity обработки одним инстансом) |
| `/s3worker/work/<C>` | обычный | журнал фаз `{"op","phase","updated_unix","instance","last_error"?}` — сохранение накопленных треков S6 ([17-synchronization-principles.md](17-synchronization-principles.md)) |
| `/s3worker/portalloc/<C>` | обычный | `{"node<k>":{"host":"h","s3":18001}}` — закрепление S3-портов, переживает rebuild |
| `/s3worker/locks/portalloc` | lease TTL 15 с | глобальный portalloc-клэйм (t90-паттерн: взаимоисключение секции довыделения портов; txn `version==0` + put-with-lease) |
| `/s3worker/instances/<id>` | lease TTL 15 с | живость инстансов (диагностика) |
| `/s3worker/api/<id>` | lease TTL 15 с | дискавери API воркера: `{"url","instance","since_unix","cert_thumbprint"?}` — ставит сам инстанс; читает панель |
| `/s3worker/rotations/<C>` | обычный | заявка ротации секретов профиля кластера `{"requested_unix","requested_by"}` (панель через API воркера — клэйм-txn; del воркером по завершении) |

Панель читает из `/s3worker/` только `api/` и `rotations/` (t34);
остальные ключи — координационные, вне панельного контракта.

`portalloc`:

```json
{"node1":{"host":"h1","s3":18001},
 "node2":{"host":"h2","s3":18002}}
```

## 9. Панель (проекция — t34)

Панель читает `/s3/` и `/s3worker/{api,rotations}/`, мутирует только через
API S3Worker (панель в etcd домена S3 не пишет ничего); проекция в
[adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md) (глава
домена S3 по образцу §10 kafka / §11 valkey) — задача t34.

Совместимость с механикой панели: unknownKeys-толерантность (§2), битый
JSON — parseError-запись без падения тика, `state`-семантика
«отсутствие = Active».

---

→ Указатель — [README.md](README.md). Исполнительная сторона контракта —
[24-s3worker.md](24-s3worker.md); мутации панели — через его HTTP API
(§1.1 там).
