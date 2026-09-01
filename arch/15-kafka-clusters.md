# 15. Kafka-кластеры: контракт etcd (контроль-плейн + дискавери) ★

Канон ключей Kafka-домена: контроль-плейн кластеров `/kafka/` (декларирует
панель AdminPanel — исполняет KafkaWorker, канон —
[16-kafkaworker.md](16-kafkaworker.md)), координация воркера `/kafkaworker/`
и **клиентский дискавери** (приложение читает адреса/креды/реестр топиков
исключительно отсюда — полная аналогия pg dsn/`app_password`/routing,
[11-bucket-sharding.md](11-bucket-sharding.md) §2).

Имена: кластер `<C>` — `^[a-z][a-z0-9_]{0,62}$` (как pg; без дефиса);
брокеры `broker1..brokerN` (имя генерирует панель, `broker<max+1>`, ≤ 9);
топик `<T>` — валидное Kafka-имя, не начинающееся с `__` (internal-топики
Kafka в реестр не попадают).

## 1. Транспорт: HTTP JSON gateway `/v3/*`

Как панель и PgWorker ([adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md)
§1, [14-pgworker.md](14-pgworker.md) §3): `HttpClient` против gRPC-gateway
etcd (JSON+base64), `POST /v3/kv/range` / `/v3/kv/put` / `/v3/kv/txn` /
`/v3/lease/*`. Один общий etcd-кластер со стендом pg. **Poll, без watch** —
тик воркера 5 с / панели 3 с покрывают динамику.

Два новых корневых префикса: `/kafka/` (контроль-плейн кластеров) и
`/kafkaworker/` (координация воркера; панель читает избирательно — §4).

## 2. Ключи кластера `/kafka/clusters/<C>/`

| Ключ | Формат значения | Пишет | Примечание |
|---|---|---|---|
| `config` | JSON `{"brokers":B,"replication_factor":R,"min_insync_replicas":M,"default_partitions":P,"default_retention_ms":X,"created_unix":T,"state"?:"NOT_INITIALIZED"\|"TO_REMOVE"}` | панель (создание, TO_REMOVE, конфиг-мутации), воркер (снимает `state` после инициализации, txn по `mod_revision`) | `state` — только у невыполненных заявок: отсутствие = Active (семантика pg 02 §2.1); поля 2–5 — mutable-конфиги кластера (converge воркером, без рестартов) |
| `brokers/broker<k>/state` | строка `NOT_INITIALIZED`\|`PROVISIONING`\|`RUNNING`\|`UNREACHABLE`\|`REMOVING`\|`TO_REMOVE` | `NOT_INITIALIZED` и `TO_REMOVE` — **только панель** (заявка создания/маркер демонтажа, one-way); остальные — воркер | `TO_REMOVE` — маркер демонтажа (аналог pg §9.6): до разбора виден в UI с бейджем |
| `brokers/broker<k>/resources` | JSON `{"cpu":"2","mem":"4Gi","disk":"40Gi"}` | панель | заявка ресурсов ноды (лимиты контейнера; форматы как pg §9.3) |
| `brokers/broker<k>/role` | `"controller"\|"broker"` | воркер (план provisioning) | `controller` — combined-нода (участник KRaft-кворума); `broker` — broker-only; фиксируется при создании ноды навсегда |
| `endpoints` | строка `"h1:p1,h2:p2,..."` | воркер (после подъёма; при add/remove брокера — RMW) | клиентские bootstrap-адреса (advertised host + клиентский порт из portalloc) — **точка дискавери клиентов** |
| `app_user` | `"app"` | воркер (ensure, txn put-if-absent) | per-cluster SASL-пользователь |
| `app_password` | 32 симв `[A-Za-z0-9]` | воркер (ensure + ротация) | per-cluster SASL-пароль; панель читает для проб, в UI/API не отдаёт (как dsn-пароль pg) |
| `topics/<T>` | JSON — гибрид автосинка и конфиг-заявки, см. §3 | воркер (автосинк факта), панель (только `desired`-часть, RMW) | реестр топиков для дискавери |
| `topics/<T>/desired.create` | JSON `{"partitions":P,"replication_factor":R,"configs"?:{...},"requested_unix":T,"requested_by":"u"}` | панель (клэйм-txn `version==0`), воркер — только del | заявка создания (t01); `configs` — только управляемые (`retention.ms`, `min.insync.replicas`); отсутствие = брокерные дефолты |
| `topics/<T>/desired.delete` | JSON `{"requested_unix":T,"requested_by":"u"}` | панель (клэйм-txn `version==0`), воркер — только del | заявка удаления (деструктивная; t01) |

Неизвестные ключи внутри `/kafka/` — не ошибка: лог + счётчик `unknownKeys`
в снапшоте читателя (как pg 02 §2.1; система развивается, парсеры не падают).

### 2.1. Канонические примеры значений (критерий приёмки парсеров)

`config` при создании (заявка, панель):

```json
{"brokers":3,"replication_factor":3,"min_insync_replicas":2,
 "default_partitions":12,"default_retention_ms":604800000,
 "created_unix":1756500000,"state":"NOT_INITIALIZED"}
```

`config` Active-кластера (после provisioning — поле `state` снято воркером):

```json
{"brokers":3,"replication_factor":3,"min_insync_replicas":2,
 "default_partitions":12,"default_retention_ms":604800000,
 "created_unix":1756500000}
```

`config` с заявкой удаления (панель): то же + `"state":"TO_REMOVE"`.

`endpoints`: `"host.docker.internal:16001,host.docker.internal:16002,host.docker.internal:16003"`
(advertised-хост CLIENT-listener по правилу [16](16-kafkaworker.md) §2.1;
порт — клиентский host-порт ноды из `/kafkaworker/portalloc/<C>`).

`topics/orders` с desired-заявкой (панель поставила, автосинк ещё не применил):

```json
{"partitions":12,"replication_factor":3,
 "configs":{"retention.ms":"604800000","min.insync.replicas":"2"},
 "desired":{"partitions":16,"configs":{"retention.ms":"86400000"}},
 "desired_unix":1750000000,"desired_by":"admin",
 "synced_unix":1750000100,"missing":false}
```

`topics/ghost` — топик исчез из Kafka при живой заявке (`missing:true`):

```json
{"partitions":3,"replication_factor":1,
 "configs":{"retention.ms":"604800000"},
 "desired":{"configs":{"retention.ms":"86400000"}},
 "desired_unix":1750000200,"desired_by":"admin",
 "synced_unix":1750000300,"missing":true}
```

`topics/audit/desired.create` (заявка создания с начальными конфигами, t01 §3.1):

```json
{"partitions":12,"replication_factor":3,
 "configs":{"retention.ms":"86400000","min.insync.replicas":"2"},
 "requested_unix":1750000000,"requested_by":"admin"}
```

`topics/audit/desired.create` без начальных конфигов (брокерные дефолты):

```json
{"partitions":6,"replication_factor":3,
 "requested_unix":1750000050,"requested_by":"admin"}
```

`topics/orders/desired.delete` (заявка удаления):

```json
{"requested_unix":1750000100,"requested_by":"admin"}
```

## 3. Ключ топика: автосинк + конфиг-заявка

Канонический JSON значения `topics/<T>`:

```json
{"partitions":12,"replication_factor":3,
 "configs":{"retention.ms":"604800000","min.insync.replicas":"2"},
 "desired":{"partitions":16,"configs":{"retention.ms":"86400000"}}?,
 "desired_unix":1750000000?,"desired_by":"admin"?,
 "synced_unix":1750000100,"missing":false}
```

- **Факт** (`partitions`, `replication_factor`, `configs`, `synced_unix`) —
  пишет только воркер (автосинк): `replication_factor` — read-only факт
  (мутация RF — roadmap reassignment); `configs` — фактические значения
  управляемых конфигов (строковые, как отдаёт Kafka).
- **`desired`** — конфиг-заявка панели (управляемые поля: `partitions` — только
  увеличение; `configs.retention.ms`, `configs.min.insync.replicas`; лишние
  поля — 400). Панель пишет RMW: read → set `desired`/`desired_unix`/
  `desired_by` → txn `compare mod_revision == прочитанной` → put.
  `desired` отсутствует (null) — заявки нет.
- **Протокол автосинка (воркер, тик `TopicSyncIntervalSec`)**: ListTopics+
  Describe → для каждого не-`__` топика read-modify-write ключа: обновить
  факт, сохранить `desired`; если `desired` отличается от факта по управляемым
  полям → применить к Kafka (`IncrementalAlterConfigs`, `CreatePartitions`) и
  снять `desired` (записать факт = заявке, `desired=null`) — тот же txn RMW.
  Проигрыш compare → re-read, следующий тик (панель успела переписать
  desired — применится свежий).
- **Топик исчез из Kafka**: `desired` нет → воркер удаляет ключ (реестр =
  факт); `desired` есть → ключ НЕ удаляется, `missing=true` (заявка не
  исполнима: создавать топики воркер не умеет — roadmap). Панель показывает
  «топик отсутствует, заявка не исполнена» + warning-алерт; отмена заявки —
  мутация панели (`desired=null`) → следующий автосинк удалит ключ. Топик
  появился → `missing=false`, `desired` применяется штатно.
- **Появление нового топика** (создан CLI/клиентом) → воркер кладёт ключ с
  фактом (`desired` отсутствует). Исчезновение и появление — штатные циклы
  реестра.

### 3.1. Lifecycle-заявки создания/удаления (t01)

Ключи `topics/<T>/desired.create` / `topics/<T>/desired.delete` (§2): ставит панель
клэйм-txn `version==0` (повтор при живой заявке — 409; отмена — del ключа заявки),
воркер после исполнения/чистки делает del. Обе заявки на один `<T>` запрещены
панелью; etcd-мусор — delete авторитетен, create чистится ДО исполнения delete.

Исполнение — тем же тиком TopicSync (§16 5 D), порядок decide: чистка create
(коллизия) → delete → create → факт-синк; один топик — одно lifecycle-действие
за тик:

| Ситуация | Действие воркера |
|---|---|
| `desired.delete` + топик в факте | journal → DeleteTopics → одной txn: del `topics/<T>` (факт-ключ; живой `desired` гасится с ним) + del заявки |
| `desired.delete` + топика нет | del заявки (+ del факт-ключа, если висит missing-ключ) — «исполнено внешне» |
| `desired.create` + топика нет | journal → CreateTopics(partitions, RF, configs?) → del заявки; факт-ключ кладёт следующий автосинк-тик |
| `desired.create` + топик есть | del заявки + journal-note «уже существует, параметры не применены» |
| обе живы | del `desired.create` + journal-warning; исполняется delete |
| заявка на `__`-имя | del + журнал, не исполняя |

Идемпотентность: CreateTopics → AlreadyExists = исполнено; DeleteTopics →
TopicDoesNotExist = исполнено; отказ между мутацией и del заявки — следующий тик
сходится по факту. `missing`-семантика не меняется; create на missing-топике —
«пересоздание» (панель требует отменить живой `desired` раньше; обход etcd не
ломается: после create `missing=false`, `desired` применится штатно).

## 4. Координация воркера `/kafkaworker/`

Порт схемы `/pgworker/` ([14-pgworker.md](14-pgworker.md) §3.3) один в один,
свой префикс:

| Ключ | Тип | Назначение |
|---|---|---|
| `/kafkaworker/leader` | lease TTL 15 с | лидер singleton-задач (регулярные снапшоты P12) |
| `/kafkaworker/claims/<C>` | lease TTL 15 с | пер-кластерный клэйм (exclusivity обработки одним инстансом) |
| `/kafkaworker/work/<C>` | обычный | журнал фаз `{"op","phase","updated_unix","instance","last_error"?}` |
| `/kafkaworker/portalloc/<C>` | обычный | `{"broker<k>":{"host":"h","client":16001}}` — закрепление клиентских портов (переживает rebuild) |
| `/kafkaworker/instances/<id>` | lease TTL 15 с | живость инстансов (диагностика) |
| `/kafkaworker/api/<id>` | lease TTL 15 с | **дискавери API воркера** (arch/16 §1.1): `{"url":"http://<host>:<port>","instance":"<id>","since_unix":…}` — ставит сам инстанс; ключ жив = инстанс жив и URL валиден. Читает панель; префикс `/kafka/` и этот координационный слой пишет только воркер (мутации панели — через его API) |
| `/kafkaworker/rotations/<C>` | обычный | заявка ротации app-пароля `{"requested_unix","requested_by"}` (панель, клэйм-txn; формат и протокол — pg 02 §9.8) |
| `/kafkaworker/rebalances/<C>` | обычный | заявка ребалансировки партиций `{"requested_unix","requested_by"}` (панель, клэйм-txn — протокол ротаций; del воркером по завершении или панелью — отмена) |
| `/kafkaworker/reassignments/<C>` | обычный | прогресс текущего reassignment — пишет только воркер: `{"mode":"drain"\|"balance","drain_broker"?,"partitions_total","partitions_remaining","submitted_unix","updated_unix","instance","last_error"?}`; ключ живёт только во время операции (put при старте, del по завершении — пусто = операции нет) |

Панель читает из `/kafkaworker/` только `rotations/`, `rebalances/`,
`reassignments/` (очередь ротаций и ребалансировок + прогресс reassignment
в UI); остальные ключи не читает и не пишет.

## 5. Клиентский дискавери (приложения)

Приложение читает из etcd и только из него (полная аналогия pg
dsn/app_password/routing):

1. `/kafka/clusters/<C>/endpoints` → `bootstrap.servers`;
2. `/kafka/clusters/<C>/app_user` + `app_password` → SASL/PLAIN креды
   (`security.protocol=SASL_PLAINTEXT`, `sasl.mechanisms=PLAIN`);
3. `/kafka/clusters/<C>/topics/` (префикс) → реестр топиков: имена +
   partitions + RF + конфиги (по нему выбирается топик и ожидаемая
   параллельность). Читатель реестра фильтрует leaf-ключи заявок
   `desired.{create,delete}` по числу сегментов (факт-ключи — 6 сегментов).

Библиотека дискавери (по образцу `Puzzle .../ha-db-etcd-clusters`,
watch-long-poll/poll) — roadmap (`t05-kafka-discovery-lib`, в репозиторий
Puzzle); контракт etcd выше уже содержит всё необходимое.

## 6. Обработка сбоев (толерантность читателей)

| Случай | Поведение |
|---|---|
| Битый JSON в значении ключа (`config`, `resources`, `topics/<T>`, `topics/<T>/desired.create`/`desired.delete`, заявка ротации/ребалансировки, прогресс reassignment) | ключ пропускается, в снапшот попадает parseError-запись (без исключения), warning-алерт `kafka-key-malformed`; заявка/прогресс с битым JSON для воркера — мусор: reassignment-оператор разбирается по факту Kafka, битый прогресс перезаписывается |
| Неизвестный ключ внутри `/kafka/` | лог-строка + счётчик `unknownKeys`; парсер не падает. В т.ч. неизвестный leaf под `topics/<T>/` |
| Active-кластер без `endpoints` | критический алерт `kafka-endpoints-missing` (воркер ещё не дописал / потеря ключа) |
| `config.state` — незнакомое значение | толерантно: трактуется как Active-ветка с raw-строкой state (state-значения строкой — система развивается) |
| Топик без части факт-полей | читается с null-полями (desired/missing — главные для UI) |

---

→ Указатель — [README.md](README.md). Исполнительная сторона контракта —
[16-kafkaworker.md](16-kafkaworker.md); мутации панели —
[adminpanel/02-etcd-contract.md](adminpanel/02-etcd-contract.md) (глава Kafka).
