# Спецификация: управление кластерами Kafka в админ-панели + KafkaWorker

> Dev-flow, фаза спецификации. Worktree: `feat-kafka-admin-worker`. Дата: 2026-08-30.
> Решения пользователя (зафиксированы): топики — **только просмотр + автосинк факта в etcd**
> (создание/удаление топиков — roadmap); **SASL_PLAINTEXT + per-cluster креды** (TLS — roadmap);
> дефолт кластера **3 брокера / RF=3 / min.insync.replicas=2** (диапазон 1..9); мутации —
> **полный пакет** (default-конфиги + конфиги существующих топиков + add/remove брокера).

## 1. Цель

Третий домен админ-панели (etcd, pg, **kafka**) и исполнительная сторона для него:

1. **AdminPanel** получает раздел **kafka**: создание Kafka-кластера (заявка в etcd),
   изменение параметров кластера и конфигов существующих топиков, добавление/удаление
   брокеров, просмотр состояния (брокеры/топики/группы подписчиков и их лаги), удаление
   кластера, ротация app-пароля.
2. **KafkaWorker** — новый фоновый сервис (.NET 10) по образцу PgWorker: по состоянию
   в etcd управляет жизненным циклом Kafka-кластеров через docker (plain / swarm)
   (provisioning/deprovisioning/надзор/автосинк топиков/converge конфигов).
3. **Дискавери ТОЛЬКО через etcd**: Kafka-клиент приложения получает адреса кластера
   (bootstrap), креды и реестр топиков исключительно из etcd — полной аналогией pg
   (dsn/`app_password`/routing). Реестр топиков наполняется **автосинком факта**
   из Kafka-кластера (источник истины по составу топиков — сам кластер).

Канон размещается в `arch/` (arch-first): новый контракт etcd и канон воркера —
новые файлы `arch/15-kafka-clusters.md` (контракт etcd + клиентский дискавери) и
`arch/16-kafkaworker.md` (канон воркера); контракт панели — расширение
`arch/adminpanel/02-etcd-contract.md` (раздел kafka) и `arch/adminpanel/03-panels.md`
(эндпоинты/DTO/UI). **На фазе spec — только этот план; правка `arch/` — в plan/execute.**

## 2. Принципы

1. **Декларативная заявочная модель, как у pg**: панель заявляет (state-ключи и desired-
   конфиги в etcd), KafkaWorker исполняет и пишет факт. Панель никогда не трогает
   контейнеры и Kafka напрямую (мутации кластера — только через etcd; пробы — read-only).
2. **Автосинк топиков — единственный источник реестра**: KafkaWorker периодически
   сверяет фактические топики кластера с etcd и приводит etcd к факту. Заявочная модель
   для топиков — **только их конфиги** (`desired`-часть ключа топика): retention.ms /
   min.insync.replicas / увеличение partitions. Создание/удаление топиков — roadmap.
3. **Надёжность через идемпотентность**: каждый шаг перепроверяет факт; состояние
   переживает смерть воркера (etcd + тома брокеров); координация нескольких инстансов —
   lease-клэймы в etcd (порт PgWorker: takeover ≤ TTL 15 с + тик).
4. **SASL_PLAINTEXT**: один per-cluster секрет `app_user`/`app_password` (генерирует
   воркер, txn put-if-absent; ротация — заявкой панели). Его используют воркер
   (AdminClient), панель (read-only пробы) и приложения. Контроллерный (KRaft) канал —
   PLAINTEXT внутри закрытой docker-сети (домашнее окружение, AGENTS.base: без
   enterprise-защит). ACL/authorization и TLS — roadmap.
5. **KRaft без ZooKeeper** (в Kafka 4.x ZooKeeper удалён): combined-режим
   broker+controller; кворум контроллеров (`min(3, brokers)`) фиксируется при создании;
   добавляемые позже брокеры — broker-only (кворум не меняется); контроллерные ноды
   не удаляются и не пересоздаются с потерей тома.
6. **Копирование из референсов**: каркас, etcd-клиент, docker-драйвер, координация,
   порт-аллокатор — из `src/PgWorker.*`; AdminClient-обёртки и converge-логика
   управления конфигами топиков — из `../Puzzle` `docs/01.16-kafka.md` §7
   (`IKafkaTopicAdmin`: describe→decide→act, только увеличение partitions, RF read-only).
   Дубли кода между PgWorker/KafkaWorker/AdminPanel — осознанные (унификация —
   roadmap-задача `t08-unify-adminpanel-duplicates`, туда же расширить scope).
7. **Язык и стиль**: документация/комментарии — русский; идентификаторы — английский;
   .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`, CPM
   (`src/Directory.Packages.props`; новый пакет — только `Confluent.Kafka` 2.14.2,
   версия из референса Puzzle). Тесты — AAA-комментарии.

## 3. Контракт etcd (канон — будущий `arch/15`)

Транспорт — HTTP JSON gateway `/v3/*` (как панель/PgWorker), poll без watch. Один общий
etcd кластер со стендом pg. Два новых корневых префикса: `/kafka/` (контроль-плейн
кластеров) и `/kafkaworker/` (координация воркера — панель читает избирательно).

Имя кластера `<C>`: `^[a-z][a-z0-9_]{0,62}$` (как pg; без дефиса). Брокеры:
`broker1..brokerN`, имя генерирует панель (`broker<max+1>`, ≤ 9). Топик `<T>`:
валидное Kafka-имя, не начинающееся с `__` (internal-топики Kafka в реестр
не попадают, панель их не показывает).

### 3.1. Ключи кластера `/kafka/clusters/<C>/`

| Ключ | Формат значения | Пишет | Примечание |
|---|---|---|---|
| `config` | JSON `{"brokers":B,"replication_factor":R,"min_insync_replicas":M,"default_partitions":P,"default_retention_ms":X,"created_unix":T,"state"?:"NOT_INITIALIZED"\|"TO_REMOVE"}` | панель (создание §9.1-аналог, TO_REMOVE, конфиг-мутации), воркер (снимает `state` после инициализации, txn по `mod_revision`) | `state` — только у невыполненных заявок: отсутствие = Active (семантика pg 02 §2.1); поля 2–5 — mutable-конфиги кластера (converge воркером) |
| `brokers/broker<k>/state` | строка `NOT_INITIALIZED`\|`PROVISIONING`\|`RUNNING`\|`UNREACHABLE`\|`REMOVING`\|`TO_REMOVE` | `NOT_INITIALIZED` и `TO_REMOVE` — **только панель** (заявка создания/маркер демонтажа, one-way); остальные — воркер | `TO_REMOVE` — маркер демонтажа (аналог pg §9.6): до разбора виден в UI с бейджем |
| `brokers/broker<k>/resources` | JSON `{"cpu":"2","mem":"4Gi","disk":"40Gi"}` | панель | заявка ресурсов ноды (лимиты контейнера; форматы как pg §9.3) |
| `brokers/broker<k>/role` | `"controller"`\|`"broker"` | воркер (план provisioning) | combined-нода (`controller`) — участник KRaft-кворума; `broker` — broker-only; фиксируется при создании ноды навсегда |
| `endpoints` | строка `"h1:p1,h2:p2,..."` | воркер (после подъёма; при add/remove — RMW) | клиентские bootstrap-адреса (host-порты из portalloc) — **точка дискавери клиентов** |
| `app_user` | `"app"` | воркер (ensure, txn put-if-absent) | per-cluster SASL-пользователь |
| `app_password` | 32 симв `[A-Za-z0-9]` | воркер (ensure + ротация) | per-cluster SASL-пароль; панель читает для проб, в UI/API не отдаёт (как dsn-пароль pg) |
| `topics/<T>` | JSON — гибрид автосинка и конфиг-заявки, см. §3.2 | воркер (автосинк факта), панель (только `desired`-часть, RMW) | реестр топиков для дискавери |

Неизвестные ключи внутри `/kafka/` — не ошибка: лог + счётчик `unknownKeys` (как pg 02 §2.1).

### 3.2. Ключ топика: автосинк + конфиг-заявка

Канонический JSON значения `topics/<T>`:

```json
{"partitions":12,"replication_factor":3,
 "configs":{"retention.ms":"604800000","min.insync.replicas":"2"},
 "desired":{"partitions":16,"configs":{"retention.ms":"86400000"}}?,
 "desired_unix":1750000000?,"desired_by":"admin"?,
 "synced_unix":1750000100,"missing":false}
```

- **Факт** (`partitions`, `replication_factor`, `configs`, `synced_unix`) — пишет только
  воркер (автосинк): `replication_factor` — read-only факт (мутация RF — roadmap reassignment);
  `configs` — фактические значения управляемых конфигов (строковые, как отдаёт Kafka).
- **`desired`** — конфиг-заявка панели (управляемые поля: `partitions` — только
  увеличение; `configs.retention.ms`, `configs.min.insync.replicas`; лишние поля —
  400). Панель пишет RMW: read → set `desired`/`desired_unix`/`desired_by` → txn
  `compare mod_revision == прочитанной` → put. `desired=null` (отсутствует) — заявки нет.
- **Протокол автосинка (воркер, тик `TopicSyncIntervalSec`)**: ListTopics+Describe →
  для каждого не-`__` топика read-modify-write ключа: обновить факт, сохранить
  `desired`; если `desired` отличается от факта по управляемым полям → применить к
  Kafka (`IncrementalAlterConfigs`, `CreatePartitions`) и снять `desired` (записать
  факт = заявке, `desired=null`) — тот же txn RMW. Проигрыш compare → re-read, следующий
  тик (панель успела переписать desired — применится свежий).
- **Топик исчез из Kafka**: `desired` нет → воркер удаляет ключ (реестр = факт);
  `desired` есть → ключ НЕ удаляется, `missing=true` (заявка не исполнима: создавать
  топики воркер не умеет — roadmap). Панель показывает «топик отсутствует, заявка
  не исполнена» + warning-алерт; отмена заявки — мутация панели
  (`desired=null`) → следующий автосинк удалит ключ. Топик появился → `missing=false`,
  `desired` применяется штатно.
- **Появление нового топика** (создан CLI/клиентом) → воркер кладёт ключ с фактом
  (`desired` отсутствует). Исчезновение и появление — штатные циклы реестра.

### 3.3. Координация воркера `/kafkaworker/`

Порт схемы `/pgworker/` (arch/14 §3.3) один в один, свой префикс:

| Ключ | Тип | Назначение |
|---|---|---|
| `/kafkaworker/leader` | lease TTL 15 с | лидер singleton-задач (снапшоты) |
| `/kafkaworker/claims/<C>` | lease TTL 15 с | пер-кластерный клэйм (exclusivity) |
| `/kafkaworker/work/<C>` | обычный | журнал фаз `{"op","phase","updated_unix","instance","last_error"?}` |
| `/kafkaworker/portalloc/<C>` | обычный | `{"broker<k>":{"host":"h","client":16001}}` — закрепление портов (переживает rebuild) |
| `/kafkaworker/instances/<id>` | lease TTL 15 с | живость инстансов (диагностика) |
| `/kafkaworker/rotations/<C>` | обычный | заявка ротации app-пароля `{"requested_unix","requested_by"}` (панель, клэйм-txn; формат и протокол — pg 02 §9.8) |

Панель читает из `/kafkaworker/` только `rotations/` (очередь ротаций в UI); остальные
ключи не читает и не пишет.

### 3.4. Мутации панели (протоколы — порты pg 02 §9)

Все — по действующим паттернам панели: активный endpoint из снапшота, валидация на
сервере (ProblemDetails), идемпотентность, компенсации/без-компенсации как у pg.

| Мутация | Протокол |
|---|---|
| **Создание кластера** `POST /api/kafka/clusters` | (1) клэйм-txn `version(config)==0` + put config (`state=NOT_INITIALIZED`); (2) пакет PUT: `brokers/broker<k>/state=NOT_INITIALIZED` × B + `brokers/broker<k>/resources` × B; (3) сбой → компенсация `del --prefix /kafka/clusters/<C>/` (частичный кластер безопасен, повтор — 409). Форма: name, brokers 1..9 (def 3), replicationFactor 1..9 ≤ brokers (def 3), minInSyncReplicas 1..RF (def 2), defaultPartitions 1..1000 (def 12), defaultRetentionMs 1..2147483647 (def 604800000 = 7 дней), cpu 0.01..64 / mem 1..65536 Gi / disk 1..65536 Gi (def 2/2Gi/20Gi) |
| **Удаление кластера** `DELETE /api/kafka/clusters/{c}` | PUT config RMW: `state=TO_REMOVE` с сохранением остальных полей (идемпотентно; протокол = pg §9.4). Съём контейнеров/очистка ключей — воркер |
| **Изменение default-конфигов** `PUT /api/kafka/clusters/{c}/config` | RMW- txn по `mod_revision`: обновить `replication_factor`/`min_insync_replicas`/`default_partitions`/`default_retention_ms` (те же границы); применяет воркер как dynamic broker configs (converge, без рестартов). 404/409 (не Active)/503 — как pg |
| **Добавление брокера** `POST /api/kafka/clusters/{c}/brokers` | тело: resources {cpu, mem, disk}; имя генерит сервер `broker<max+1>`; клэйм-txn `version(brokers/<b>/state)==0` + put `NOT_INITIALIZED` + put resources; сбой → компенсация точечными del (аналог pg §9.5). Поднимает воркер (broker-only, кворум не меняется) |
| **Удаление брокера** `DELETE /api/kafka/clusters/{c}/brokers/{b}` | маркер `brokers/<b>/state=TO_REMOVE` (one-way, идемпотентно; протокол = pg §9.6). Серверные пред-проверки: не `controller`; не последний брокер; по live-пробе на брокере нет партиций-реплик (иначе 409 «сначала reassignment» — roadmap). Воркер перепроверит авторитетно (guard'ы) |
| **Конфиг-заявка топика** `PUT /api/kafka/clusters/{c}/topics/{t}` | тело `{partitions?, retentionMs?, minInSyncReplicas?}` (хотя бы одно поле; partitions — только больше фактического); 404 кластер/топик (топик должен существовать в реестре и не быть missing); RMW-txn по §3.2. Без компенсации: неудавшаяся запись просто не встала — повтор идемпотентен |
| **Отмена конфиг-заявки** `DELETE /api/kafka/clusters/{c}/topics/{t}/desired` | RMW-txn: `desired=null` (убрать поля desired_*); 404 если заявки нет. Нужна для missing-топиков (после отмены автосинк удалит ключ) и для «передумали» |
| **Ротация app-пароля** `POST /api/kafka/clusters/{c}/app-password/rotate` | клэйм-txn `/kafkaworker/rotations/<C>` `version==0` + put (протокол = pg §9.8 один в один: 409 «уже запрошена», 201 `{cluster, requestedUnix, requestedBy}`); UI-модалка предупреждает о rolling-перезапуске брокеров |

### 3.5. Клиентский дискавери (приложения)

Приложение читает из etcd и только из него (полная аналогия pg dsn/app_password/routing):

1. `/kafka/clusters/<C>/endpoints` → `bootstrap.servers`;
2. `/kafka/clusters/<C>/app_user` + `app_password` → SASL/PLAIN креды
   (`security.protocol=SASL_PLAINTEXT`, `sasl.mechanisms=PLAIN`);
3. `/kafka/clusters/<C>/topics/` (префикс) → реестр топиков: имена + partitions +
   RF + конфиги (по нему выбирается топик и ожидаемая параллельность).

Библиотека дискавери (по образцу `Puzzle .../ha-db-etcd-clusters`, watch-long-poll/poll)
— roadmap (k05, в репозиторий Puzzle); контракт etcd выше уже содержит всё необходимое.

## 4. KafkaWorker (канон — будущий `arch/16`)

### 4.1. Модель размещения

- **Нода кластера = контейнер/сервис** из образа `apache/kafka:4.0.0` (пин в CPM-независимой
  настройке `Images:Node`; official-образ, KRaft-only, готов к env-конфигурации).
  Кастомный образ НЕ собирается (отличие от pgworker-node): вся конфигурация — env,
  генерирует воркер при создании контейнера (idempotent-сверка по имени).
- **Роли KRaft**: при создании B брокеров ноды `broker1..broker_m` (m = min(3,B)) —
  `PROCESS_ROLES=broker,controller` (кворум `KAFKA_CONTROLLER_QUORUM_VOTERS` фиксируется
  по этим нодам), `broker_{m+1}..B` — `broker`-only. `brokers/<k>/role` фиксирует роль.
- **Listeners**: `CONTROLLER :9093` (PLAINTEXT, внутренняя сеть), `INTERNAL :9092`
  (межброкерный, SASL_PLAINTEXT, advertised = docker-DNS имя ноды), `CLIENT :9094`
  (SASL_PLAINTEXT, опубликован на хост портом из portalloc, advertised =
  `<AdvertisedClientHost || docker-хост>:<клиентский порт>`). Сеть `kfw-net` (alias =
  имя ноды) — как `pgw-net`.
- **SASL/PLAIN JAAS**: env `KAFKA_LISTENER_NAME_{INTERNAL,CLIENT}_PLAIN_SASL_JAAS_CONFIG`
  со списком пользователей. В MVP в списке один пользователь `app`; при ротации —
  двухпользовательское окно (§4.4).
- **Служебные топики**: RF `min(3,B)`, minISR `min(2,B)` (формулы от фактического B —
  1-брокерный стенд стартует); `auto.create.topics.enable=false` (создание — явное, CLI).
- **Начальные default-конфиги** из заявки: `log.retention.ms`, `num.partitions`,
  `default.replication.factor`, `min.insync.replicas` — env брокеров при создании.
- **Volume**: `kfw-<C>-<b>-data` → `/var/lib/kafka/data`; имя детерминировано, данные
  переживают пересоздание контейнера.
- **Placement/порты**: порт PgWorker — анти-аффинити нод по docker-хостам
  (plain: `Hosts[]`, swarm: `SwarmManager`); порт-аллокатор из диапазона
  `16000–16999` (1 клиентский порт на ноду), закрепление в `/kafkaworker/portalloc/<C>`;
  лимиты контейнера из `resources` (cpu/mem; disk-заявка — инфо, квоты томов roadmap).
- Сам воркер — контейнер с `docker.sock` (или swarm-manager), volume снапшотов,
  запускается через `deploy/docker-compose.yml` (docker/KafkaWorker.Dockerfile по
  образцу PgWorker.Dockerfile; многостадийный publish → aspnet:10.0, healthz).
  **Env-секретов per-install нет** (единственный секрет — per-cluster `app_password`,
  живёт в etcd; отличие от PGW_*-набора pg).

### 4.2. Процессы (машины состояний; порт arch/14 §5)

Классификация тика: `config.state=NOT_INITIALIZED` → Provisioning; `TO_REMOVE` →
Deprovisioning; иначе Active-ветка (надзор + converge + scale + ротация + автосинк).
Все операции над кластером — только под живым клэймом `<C>`; journal-before-manipulations.

- **A. ProvisioningProcess (K0–K6)**: клэйм+journal → план (placement, порты, роли,
  ensure `app_user`/`app_password` txn put-if-absent — порт P1.5) → создать B контейнеров
  (по одному, `state=PROVISIONING`; при re-run существующие сверяются и пропускаются) →
  ждать готовности: AdminClient `DescribeCluster` отвечает, контроллер избран, число
  брокеров = B (бюджет 10 мин, транзиент-толерантно) → `state=RUNNING` у всех →
  применить dynamic broker configs из config (§4.3) → put `endpoints` →
  config: txn (compare `mod_revision`) → put канонического JSON **без** `state` →
  journal done. Гонка «панель пишет TO_REMOVE посреди provisioning» — перечитывание
  config перед фазами (порт R6): смена state безопасно прекращает процесс.
- **B. DeprovisioningProcess (X0–X3)**: клэйм+journal → удалить контейнеры/сервисы и
  volumes (`kfw-<C>-*`, включая сироты; 404 = ок) → `del --prefix /kafka/clusters/<C>/` +
  `/kafkaworker/{claims,work,portalloc}/<C>*` + `/kafkaworker/rotations/<C>` → снапшот,
  клэйм снят явно. Порядок «сначала docker, потом etcd» — как pg D-процесса.
- **C. NodeSupervisor (надзор)**: сверка декларации с фактом — снесённый контейнер
  пересоздаётся (тот же volume, env из portalloc), `PROVISIONING→RUNNING`; проба
  AdminClient: брокер молчит дольше `NodeDeadSec` (90 с) → `state=UNREACHABLE` +
  пересоздание контейнера; том утрачен и RF>1 → чистый том, брокер rejoin'ится
  репликацией (self-healing Kafka); RF=1 и том утрачен → warning-журнал (данные
  потеряны — документированное поведение). Ноды `TO_REMOVE`/`REMOVING`/`PROVISIONING`
  чужих процессов надзор не трогает (границы как у pg C).
- **D. TopicSyncProcess (автосинк + desired-converge)** — тик `TopicSyncIntervalSec`
  (15 с): протокол §3.2. Внутри — describe→decide→act с seam-интерфейсом
  `IKafkaTopicClient` (порт `IKafkaTopicAdmin` из Puzzle §7.3: fake в тестах,
  `KafkaTopicClientAdapter` — единственное место с Confluent-типами; ретраи
  Polly jitter поверх оркестрации — повтор безопасен). Уменьшение partitions —
  перманентный отказ журнала (Kafka не умеет; панель отсекает раньше).
- **E. ClusterConfigConverger** (Active-ветка, лёгкий): describe dynamic broker
  configs (по одному брокеру) vs `config.{default_*}` → при отличии
  `IncrementalAlterConfigs` на всех брокерах (идемпотентный Set; применяется без
  рестартов). `replication_factor`/`min_insync` из config — также стартовые env
  новых брокеров и свойства новых топиков по умолчанию (фактические топики не
  трогаются — только desired-заявками).
- **F. AddBrokerProcess**: `brokers/<b>/state=NOT_INITIALIZED` у Active-кластера:
  план (host/порт; `role=broker`), создать контейнер (env: `QUORUM_VOTERS` уже
  зафиксирован — нода подключается к кворуму), ждать появления в DescribeCluster,
  RMW `endpoints` (добавить адрес), `state=RUNNING`.
- **G. RemoveBrokerProcess**: маркер `TO_REMOVE`: guards (кластер Active; не
  controller; не последний; на брокере нет реплик партиций — по DescribeTopics,
  иначе journal-ожидание: после roadmap-reassignment демонтаж продолжится сам) →
  удалить контейнер+volume → del префикс `brokers/<b>/` + RMW `endpoints` (убрать
  адрес) + portalloc-фильтрация → journal done.
- **H. AppPasswordRotator (ротация, фазы A/B/C — без окна недоступности)**:
  заявка `/kafkaworker/rotations/<C>`; NEW = генерация (32 симв); **A)** пересоздать
  контейнеры брокеров по одному (rolling; ждать возврата в ISR) с JAAS из ДВУХ
  пользователей (OLD+NEW) — все клиенты работают со OLD; **B)** ОДНА txn:
  `[compare value(app_password)==OLD][put NEW; del заявки]` — клиенты перечитывают
  etcd и переподключаются с NEW; **C)** rolling-пересоздание с JAAS только NEW
  (снятие OLD-пользователя). Отказ между фазами безопасен (оба креда валидны;
  перезапуск процесса идёт с записанной фазой из journal). Окно «часть брокеров
  знает только NEW» невозможно по построению. Уведомление в UI-модалке: выполнять
  в тихое окно (rolling-рестарты).
- **Снапшоты**: порт P12 — лидер регулярно (6 ч) + «до/после» provisioning/
  deprovisioning/ротации (`/snapshots`, retention).

### 4.3. Проекты и структура

`src/` (тот же `PgWorker.slnx`, новая solution-папка `/kafka/`):

| Проект | Содержимое |
|---|---|
| `KafkaWorker.Core` | модель домена (`KafkaClusterSnapshot`, states), `Result`, DI-каркас, Retry (копии из PgWorker.Core), планирование (Placement/PortAllocator — порты), генератор env брокера (`NodeEnvBuilder`), генератор пароля |
| `KafkaWorker.Etcd` | копия клиента `EtcdGateway` + Coordination (`ClaimStore`, `WorkJournal`), парсер префикса `/kafka/`, `SnapshotJob` |
| `KafkaWorker.Docker` | копия `DockerEngine`/`PlainClusterDriver`/`SwarmClusterDriver` (переименованные под kafka-объекты) |
| `KafkaWorker.Provisioning` | процессы §4.2, `KafkaClusterClient` (AdminClient-адаптер + `IKafkaTopicClient` seam), пробы готовности |
| `KafkaWorker.App` | `Program.cs` (композиция по образцу PgWorker.App), Loops (`ReconcileLoop`/`KeepaliveLoop`/`SnapshotLoop`), health-checks, appsettings |

Тесты: `tests/KafkaWorker.UnitTests` (парсер, решения converge/TopicSync на fake,
env-билдер, порт-планирование), `tests/KafkaWorker.IntegrationTests` (Testcontainers
etcd + `apache/kafka:4.0.0`: provisioning минимального 1-брокерного кластера,
TopicSync против реального Kafka, SASL-подключение по ключам из etcd).

Конфигурация (appsettings + env-оверрайды):

```
KafkaWorker:Etcd { Endpoints[] }
KafkaWorker:Docker { Mode: Plain|Swarm, Hosts[{Name,Endpoint}], SwarmManager,
                     PortRange{From=16000,To=16999}, Images{Node="apache/kafka:4.0.0"} }
KafkaWorker:Loops { ScanIntervalSec=5, KeepaliveSec=5, ErrorDelayMs=2000,
                    TopicSyncIntervalSec=15 }
KafkaWorker:Thresholds { BrokerBootSec=600, NodeDeadSec=90 }
KafkaWorker:Parallelism { MaxClusters=4 }
KafkaWorker:Snapshots { Dir="/snapshots", RetentionFiles=10 }
KafkaWorker:AdvertisedClientHost=null   # null → адрес docker-хоста ноды (placement)
```

## 5. AdminPanel: раздел kafka

### 5.1. Снапшот и refresher

Отдельный от `EtcdSnapshot` (pg) домен, симметричная механика:

- `KafkaSnapshot` (immutable record, `AdminPanel.Core`): `BuiltAtUtc`,
  `KafkaClusterInfo[]` (config+state, brokers[{name,state,role,resources}],
  `endpoints`, topics — §3.2-модель), `KafkaProbeResult[]`, `Alerts[]`,
  `UnknownKeyCount`, ротации (`/kafkaworker/rotations/`).
- `KafkaSnapshotRefresher` (BackgroundService, тик 3 с, `AdminPanel.Etcd`): range
  `/kafka/clusters/` + `/kafkaworker/rotations/` → парсеры (толерантные, как pg §2.1)
  → `KafkaAlertEngine` → атомарная замена в `KafkaSnapshotStore`. Отказ тика —
  снапшот прежний + `reachable=false` (sticky/failover endpoint'ов общий с pg-циклом
  настройками `AdminPanel:Etcd:Endpoints`, но обход свой).
- **Live-проба** (`AdminPanel.Probes`, тик 15 с, `KafkaEnabled=true`): Confluent
  AdminClient: bootstrap = `endpoints` из etcd (через `HostMap`, как Patroni/SQL-пробы
  pg), SASL/PLAIN из `app_user`/`app_password` (панель их читает из etcd, наружу не
  отдаёт). Даёт: брокеры (id/host, контроллер, live), топики (partitions/RF/
  under-replicated/ISR-детали), consumer groups (state, members, totalLag = sum(end
  offsets − committed) по ListOffsets+DescribeGroups). Ошибка пробы не роняет etcd-часть.

### 5.2. API (`AdminPanel.Api`, модуль `/api/kafka/*`)

Инспекция: `GET /api/kafka/clusters` (сводный), `GET /api/kafka/clusters/{c}`
(детали: brokers, topics c desired/missing, groups+lags — из пробы). Мутации — §3.4.
`GET /api/alerts` объединяет алерты pg- и kafka-движков; `GET /api/overview` получает
сводку kafka (clusters count, критические). JSON/camelCase/ProblemDetails; всё под
cookie-сессию, как существующее API.

### 5.3. UI (frontend)

Навигация — раздел «Kafka»: `KafkaClustersPage` (список: имя, state-бейдж, брокеры
running/всего, топики, endpoints; кнопка «Создать кластер» → модальная форма §3.4-полей
с дефолтами 3/3/2) и `KafkaClusterDetailsPage` с вкладками:

- **Брокеры**: name, state, role (controller/broker), resources, host-адрес; колонка
  действий — «Убрать брокера» (guards: controller/последний/непустой — дизейбл с
  пояснением, серверный 409 текстом); кнопка «Добавить брокера» (форма resources);
- **Топики**: name, partitions, RF, retention, minISR, `desired`-бейдж («заявка:
  +partitions/конфиги», возраст), `missing`-подсветка; per-row «Изменить конфиги»
  (модал: partitions↑/retention/minISR) и «Отменить заявку»; подпись «состав топиков
  управляется на стороне Kafka (CLI/клиенты) — панель синхронизирует реестр из etcd»;
- **Группы** (проба): group, state, members, totalLag (сортировка по лагу); fallback
  «проба отключена/недоступна»;
- шапка: бейдж TO_REMOVE, кнопки «Изменить параметры» (default-конфиги), «Сменить
  app-пароль» (предупреждение о rolling-рестарте), «Удалить кластер» (красная, с
  подтверждением; при TO_REMOVE скрыты).

Polling/тема/401-обработка — общие компоненты layout. Формы — Mantine-модалы с
клиентской валидацией-зеркалом §3.4 и ProblemDetails-ошибками в теле (как pg-формы).

### 5.4. Алерты (`KafkaAlertEngine`, пороги `AdminPanel:KafkaAlerts`)

| kind | severity | Условие |
|---|---|---|
| `kafka-cluster-not-initialized` | info | state=NOT_INITIALIZED |
| `kafka-cluster-to-remove` | info | state=TO_REMOVE |
| `kafka-broker-not-running` | critical | Active-кластер, broker state ∉ {RUNNING}, кроме fresh-PROVISIONING |
| `kafka-endpoints-missing` | critical | Active без `endpoints` |
| `kafka-topic-missing-desired` | warning | topics: `missing=true` (заявка не исполнима — топик отсутствует) |
| `kafka-desired-stale` | warning | desired не снят дольше `StaleDesiredSec` (600) — converge буксует |
| `kafka-topic-under-replicated` | warning | проба: партиции с USR>0 дольше тика |
| `kafka-group-lag-high` | warning | проба: totalLag группы > `GroupLagMessages` (100000) |
| `kafka-rotation-pending` | info | живая заявка ротации |
| `kafka-key-malformed` | warning | ключ не разобран |

## 6. Стенд и поставка

- **dev-stand/adminpanel/** (единый стенд панели): quick-профиль расширяется kafka-сидом
  (`kafka-seed.sh`: ключи 1–2 кластеров, включая NOT_INITIALIZED/TO_REMOVE-примеры и
  topics с desired/missing); new профиль `kafka` в compose: сервис `kafkaworker`
  (build `docker/KafkaWorker.Dockerfile`, docker.sock, endpoint etcd стенда) —
  e2e полного цикла. Новые чеки: `50-kafka-api.sh` (API на сиде), `55-kafka-e2e.sh`
  (создание кластера из панели → воркер поднял → конфиг-мутация применилась →
  add/remove брокера → desired топика (созданного kafka-topics CLI) → лаги группы →
  ротация → удаление кластера; проверка дискавери: kcat/скрипт подключается только по
  ключам etcd).
- **deploy/docker-compose.yml**: сервис `kafkaworker` рядом с pgworker (тот же образ-паттерн,
  порт healthz 8081, volume `kfw-snapshots`, env `KafkaWorker__Etcd__Endpoints__0`).

## 7. Волны реализации (границы)

Каждая волна — самостоятельный срез dev-flow (spec = эта; plan разбит по волнам;
код+тесты+e2e волны мержится отдельно; порядок строгий).

- **Волна A — воркер и контракт** (бэкенд-фундамент): arch-канон (`arch/15`, `arch/16`,
  правка-указатели в `arch/README`); проекты `src/KafkaWorker.*` + тесты; процессы
  A/B/C (provisioning/deprovisioning/надзор), ClusterConfigConverger, app-секрет;
  `docker/KafkaWorker.Dockerfile` + `deploy/docker-compose.yml`; админ-клиент
  (DescribeCluster/DescribeConfigs/IncrementalAlterConfigs); e2e-граница: etcdctl-сид
  заявки → воркер поднимает 1/3-брокерный кластер → ключи факта (`endpoints`, states,
  config без state) на месте → дискавери-проверка подключением с кредами из etcd →
  TO_REMOVE очищает всё. AddBroker/RemoveBroker/TopicSync/ротация — НЕ входят.
- **Волна B — панель: кластеры**: `KafkaSnapshot`+парсеры+refresher+store; API
  (создание/удаление/конфиг-мутация/add/remove брокера/ротация — contracts §3.4);
  воркер добирает AddBrokerProcess/RemoveBrokerProcess/AppPasswordRotator; UI
  (список/детали/формы/брокеры) + кластерные алерты + базовая live-проба
  (DescribeCluster — брокеры); стенд: kafka-сид + `50-kafka-api.sh` + e2e создания/
  удаления/брокеров/ротации. Топики/группы — НЕ входят (реестр topics парсером
  читается, вкладка скрыта).
- **Волна C — топики, группы, лаги**: воркер: TopicSyncProcess (автосинк+desired,
  §3.2) + алерт-кормление фактами; панель: вкладка Топики (desired-мутации, отмена,
  missing), проба групп+лагов, вкладка Группы, оставшиеся алерты; e2e
  `55-kafka-e2e.sh` полного цикла (топик CLI → desired → converge → лаги →
  исчезновение/missing → отмена).

## 8. Ограничения, допущения, выносы в roadmap

Допущения (приняты без пользователя, обоснование — принципы §2):

- Образ `apache/kafka:4.0.0` (KRaft-only; ZooKeeper отсутствует в 4.x — выбор снят);
  pinned в конфиге, обновление — правкой версии.
- Один per-cluster SASL-кред для всех ролей (воркер/панель/приложения); ACL и
  admin/app-разделение — roadmap (вместе с TLS).
- `auto.create.topics.enable=false`; internal-топики `__*` в реестр etcd не попадают.
- Контроллерный listener PLAINTEXT (закрытая docker-сеть); интер-брокерный — SASL.
- Форма создания: brokers 1..9 def 3; RF ≤ brokers def 3; minISR ≤ RF def 2;
  partitions def 12; retention def 7 дней.
- Панель читает `app_password` (для SASL-проб), но никогда не показывает в UI/API.
- KafkaWorker не требует env-секретов per-install (единственный секрет per-cluster).

Выносы в roadmap (в plan/execute завести новый трек `arch/roadmap/kafkaworker.md`,
нумерация tNN внутри трека — прецедент `arch/adminpanel/roadmap/`; на фазе spec файлы
не трогаются):

- `t01-kafka-topic-lifecycle` — создание/удаление топиков из панели (декларации
  `topics/<T>/desired.create`-паттерн, исполняет воркер); снимает ограничение
  «desired применим только к существующим».
- `t02-kafka-reassignment` — reassignment партиций (drain брокера, ребалансировка);
  разблокирует удаление непустого брокера; требует kafka-reassign-интеграцию
  (Confluent.Kafka API нет — через AdminClient-обход или kafka-инструменты в контейнере).
- `t03-kafka-security` — TLS (SASL_SSL), ACL/authorization, разделение admin/app кредов.
- `t04-kafka-metrics` — Prometheus-метрики воркера и панели (лаги, USR, фазы).
- `t05-kafka-discovery-lib` — клиентская библиотека дискавери kafka из etcd (в Puzzle,
  по образцу ha-db: watch-long-poll/poll, кэш, событие).
- `t06-kafka-node-regen` — rolling-перегенерация существующих брокеров с новыми
  ресурсами (лимиты cpu/mem) и новыми server-props.

## 9. Критерии приёмки

1. `dotnet build src/PgWorker.slnx` — 0 warnings; `dotnet test` зелёный (unit без
   Docker, integration с Docker).
2. **Дискавери только через etcd**: свежий клиент подключается к кластеру, используя
   исключительно ключи `endpoints` + `app_user`/`app_password` (волна A); реестр
   topics в etcd читается клиентом после волны C (автосинк); в UI/логах панели
   пароль не появляется.
3. **Жизненный цикл** (волны A/B): заявка панели (или сида) → воркер поднимает
   кластер (3 брокера, RF=3, minISR=2), `endpoints` и `RUNNING`-статы в etcd, config
   без `state`; конфиг-мутация применяется без рестартов (проверка DescribeConfigs);
   add broker — кворум не меняется, новый брокер в кластере; remove broker — guards
   (controller/последний/непустой) отклоняют, пустой демонтируется с RMW `endpoints`;
   TO_REMOVE удаляет контейнеры, тома и весь префикс `/kafka/clusters/<C>/` +
   координационные ключи.
4. **Ротация** (волна B): заявка → фазы A/B/C без окна «брокер не принимает рабочий
   кред» (до завершения фазы C валидны оба пароля — переходное окно безопасно);
   после завершения фаз клиент со старым паролем отвергается, с новым (из etcd)
   работает; повторная заявка во время исполнения — 409.
5. **Топики/группы** (волна C): топик, созданный kafka-topics CLI, появляется в etcd
   ≤ 2 тиков автосинка; desired (retention/minISR/partitions↑) применяется и снимается;
   уменьшение partitions — 400; удаление топика CLI: без desired ключ исчезает, с
   desired — `missing=true` + алерт + отмена из панели убирает ключ; лаги групп видны
   во вкладке Группы при включённой пробе.
6. **Панель**: все мутации §3.4 дают корректные 400/404/409/503 (ProblemDetails),
   идемпотентны на повторе; снапшот переживает отказ etcd (stale-бейдж, прежние
   данные); неизвестные ключи не роняют парсер.
7. **Надёжность воркера**: смерть инстанса посреди процесса — takeover вторым
   инстансом ≤ TTL 15 с + тик, продолжение с journal-фазы; все операции идемпотентны
   (повтор тика после сбоя безопасен).
8. e2e чеки стенда (`50-kafka-api.sh`, `55-kafka-e2e.sh`) зелёные с чистого состояния.

## 10. Открытые вопросы

Нет — продуктовые решения зафиксированы ответами пользователя (топики: автосинк+desired;
SASL_PLAINTEXT; дефолт 3/3/2; полный пакет мутаций), остальные — допущения §8.
