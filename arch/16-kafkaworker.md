# 16. KafkaWorker: оркестратор Kafka-кластеров ★

**KafkaWorker** — фоновый сервис (.NET 10), который по состоянию в etcd
управляет жизненным циклом Kafka-кластеров через docker (plain / docker
swarm). Это исполнительная сторона декларативного контракта
[15-kafka-clusters.md](15-kafka-clusters.md): панель AdminPanel **заявляет**
кластер (`/kafka/clusters/<C>/config` c `state=NOT_INITIALIZED`) — воркер
**поднимает** KRaft-кластер, обеспечивает per-cluster SASL-креды, пишет факт
(`endpoints`, states, реестр топиков) и снимает `state`; перевод панелью в
`TO_REMOVE` — воркер демонтирует кластер полностью.

Восемь процессов:
1. **Provisioning** (A, K0–K6) — от `NOT_INITIALIZED` до рабочего кластера;
2. **Deprovisioning** (B, X0–X3) — от `TO_REMOVE` до чистого etcd и удалённых
   контейнеров/томов;
3. **NodeSupervisor** (C, надзор) — снесённый контейнер пересоздаётся, молчащий
   брокер помечается/пересоздаётся;
4. **TopicSync** (D) — автосинк реестра топиков + исполнение desired-заявок;
5. **ClusterConfigConverger** (E) — converge mutable-конфигов кластера как
   dynamic broker configs (без рестартов);
6. **AddBroker** (F) — подъём broker-only ноды в Active-кластере;
7. **RemoveBroker** (G) — демонтаж брокера по маркеру `TO_REMOVE` (с guard'ами);
8. **AppPasswordRotator** (H, фазы A/B/C) — ротация per-cluster app-пароля
   без окна недоступности.

Свойства: несколько инстансов работают одновременно (координация —
lease-клэймы в etcd, `/kafkaworker/`); смерть контролирующего инстанса не
роняет процессы — takeover ≤ TTL 15 с + тик; все операции идемпотентны;
состояние переживает смерть контроллера (etcd + тома брокеров).

Границы (что НЕ входит): создание/удаление топиков из панели, reassignment
партиций, TLS/ACL, Prometheus-метрики, клиентская библиотека дискавери,
rolling-перегенерация нод с новыми ресурсами —
[roadmap/kafkaworker.md](roadmap/kafkaworker.md).

---

## 1. Роль в системе и разделение ответственности

```
AdminPanel (UI)          KafkaWorker (исполнитель)             docker-хосты
─────────────            ──────────────────────               ────────────
создание кластера  ──►   /kafka/clusters/<C>/config.state=      контейнеры/
(заявка структуры)       NOT_INITIALIZED          ──читает──►   сервисы
удаление кластера  ──►   config.state=TO_REMOVE   ──создаёт/    apache/kafka:4.0.0
add/remove брокера ──►   brokers/<b>/state         удаёт ──►    (KRaft, SASL)
ротация пароля     ──►   /kafkaworker/rotations/<C>
конфиг-заявка      ──►   topics/<T>.desired
инспекция (read-   ◄──   endpoints, states, реестр
only, всё видит)         топиков, снятие state
```

- **Панель** — декларатор и наблюдатель: пишет только `state`-ключи заявок
  (`NOT_INITIALIZED`/`TO_REMOVE`), `resources`, `topics/<T>.desired` и заявку
  ротации; читает всё (контракт мутаций — adminpanel/02 §Kafka).
- **KafkaWorker** — исполнитель: единственный, кто создаёт/удаляет контейнеры
  брокеров, пишет `endpoints`, `brokers/<b>/{state,role}`, `app_user`/
  `app_password`, факт `topics/<T>`, снимает `state` у config, чистит
  префикс кластера при TO_REMOVE.
- **Приложение** — читает только дискавери-ключи (15 §5): `endpoints`,
  `app_user`/`app_password`, реестр `topics/`. Напрямую в docker/Kafka не
  ходит.

## 2. Модель размещения

### 2.1. Нода кластера = контейнер `apache/kafka:4.0.0`

- Образ — официальный `apache/kafka:4.0.0` (пин в настройке
  `Images:Node`), KRaft-only (ZooKeeper в 4.x удалён), полностью
  конфигурируется env. Кастомный образ НЕ собирается (отличие от
  `pgworker-node`): вся конфигурация — env, генерирует воркер при создании
  контейнера (идемпотентная сверка по имени; таблица env — §2.2).
- **Роли KRaft**: при создании B брокеров ноды `broker1..broker_m`
  (m = min(3,B)) — `PROCESS_ROLES=broker,controller` (кворум
  `KAFKA_CONTROLLER_QUORUM_VOTERS` фиксируется по этим нодам),
  `broker_{m+1}..B` — broker-only. `brokers/<k>/role` фиксирует роль навсегда;
  добавляемые позже брокеры — всегда broker-only (кворум не меняется),
  контроллерные ноды не демонтируются.
- **Listeners**: `CONTROLLER :9093` (PLAINTEXT, внутренняя docker-сеть),
  `INTERNAL :9092` (межброкерный, SASL_PLAINTEXT, advertised = docker-DNS
  имя ноды), `CLIENT :9094` (SASL_PLAINTEXT, опубликован на хост портом из
  portalloc, advertised — по правилу ниже). Сеть `kfw-net` (alias = имя
  ноды) — как `pgw-net`.
- **Advertised-правило CLIENT-listener (канон)**: advertised-хост ноды =
  `KafkaWorker:AdvertisedClientHost`, если задан; иначе — имя docker-хоста
  размещения (Name из `Hosts[]` / swarm-ноды). Требование: значение обязано
  резолвиться КЛИЕНТАМИ (оно попадает в `endpoints` → bootstrap клиентов).
  Паттерн локального docker-хоста: имя `local` в контейнере воркера
  нерезолвимо — лечится `extra_hosts: "local:host-gateway"` (прецедент
  pgworker, `deploy/docker-compose.yml`); для клиентов на хосте/в контейнерах
  стендов рекомендуем `KafkaWorker__AdvertisedClientHost=host.docker.internal`
  (клиенты-контейнеры резолвят нативно; клиенты на хосте — через `HostMap`
  панели `host.docker.internal:<port>` → `localhost:<port>`; `docker run`-
  инструменты — `--add-host host.docker.internal:host-gateway` на Linux).
- **SASL/PLAIN JAAS**: env `KAFKA_LISTENER_NAME_{INTERNAL,CLIENT}_PLAIN_
  SASL_JAAS_CONFIG` со списком пользователей. Обычно один пользователь
  `app`; при ротации — двухпользовательское окно (`user_app=<old>` +
  `user_app2=<new>`, §5 H).
- **Служебные топики**: RF `min(3,B)`, minISR `min(2,B)` (формулы от
  фактического B — 1-брокерный стенд стартует); `auto.create.topics.enable=
  false` (создание топиков — явное, CLI/клиентами).
- **Начальные default-конфиги** из заявки (`config.{default_*}`) — env
  брокеров при создании (§2.2) и одновременно converge-цель (§5 E).
- **Volume**: `kfw-<C>-<b>-data` → `/var/lib/kafka/data` (`KAFKA_LOG_DIRS`);
  имя детерминировано, данные переживают пересоздание контейнера.
- **Placement/порты**: анти-аффинити нод по docker-хостам (порт
  PlacementPlanner PgWorker); порт-аллокатор из диапазона `16000–16999`
  (**1 клиентский порт на ноду**), закрепление в
  `/kafkaworker/portalloc/<C>`; лимиты контейнера из `resources` (cpu/mem;
  disk-заявка — инфо, квоты томов — roadmap).
- Сам воркер — контейнер с `docker.sock` (или swarm-manager), volume
  снапшотов (`kfw-snapshots`), поставляется через `deploy/docker-compose.yml`
  (`docker/KafkaWorker.Dockerfile`). **Env-секретов per-install нет**
  (единственный секрет — per-cluster `app_password`, живёт в etcd;
  отличие от PGW_*-набора pg).

### 2.2. Канонический env-набор брокера (генерирует NodeEnvBuilder)

| Env | Значение |
|---|---|
| `CLUSTER_ID` | детерминированный KRaft cluster-id из имени кластера (22 симв base64url от SHA-256) — одинаков у всех нод `<C>`, переживает пересоздание контейнера |
| `KAFKA_NODE_ID` | числовой id ноды (k из `broker<k>`) |
| `KAFKA_PROCESS_ROLES` | `broker,controller` (k ≤ m) или `broker` |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | `1@host:9093,2@host:9093,…` — только controller-ноды (внутренние адреса сети `kfw-net`) |
| `KAFKA_LISTENERS` | `CONTROLLER://:9093,INTERNAL://:9092,CLIENT://:9094` |
| `KAFKA_ADVERTISED_LISTENERS` | `INTERNAL://<docker-DNS имя ноды>:9092,CLIENT://<AdvertisedClient>:<клиентский порт>` (AdvertisedClient — правило §2.1; CONTROLLER не advertised) |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` | `CONTROLLER:PLAINTEXT,INTERNAL:SASL_PLAINTEXT,CLIENT:SASL_PLAINTEXT` |
| `KAFKA_CONTROLLER_LISTENER_NAMES` | `CONTROLLER` |
| `KAFKA_INTER_BROKER_LISTENER_NAME` | `INTERNAL` |
| `KAFKA_SASL_ENABLED_MECHANISMS` | `PLAIN` |
| `KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL` | `PLAIN` (Kafka требует явный механизм при SASL на INTERNAL) |
| `KAFKA_LISTENER_NAME_INTERNAL_PLAIN_SASL_JAAS_CONFIG` | JAAS: `username="inter" password="<inter-pwd>" user_inter="<inter-pwd>" user_app=<pwd>` (при ротации + `user_app2=<new>`); inter-креды — inter-broker-клиент (§2.2) |
| `KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG` | только список пользователей (без username/password — клиенты внешние) |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | `min(3,B)` |
| `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR` | `min(3,B)` |
| `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR` | `min(2,B)` |
| `KAFKA_DEFAULT_REPLICATION_FACTOR` | R из заявки |
| `KAFKA_MIN_INSYNC_REPLICAS` | M из заявки |
| `KAFKA_NUM_PARTITIONS` | P из заявки |
| `KAFKA_LOG_RETENTION_MS` | X из заявки |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `false` |
| `KAFKA_LOG_DIRS` | `/var/lib/kafka/data` (том `kfw-<C>-<b>-data`) |

JAAS-формат (PLAIN): `org.apache.kafka.common.security.plain.PlainLoginModule required user_app="<password>";`.

**Inter-broker-креды (§2.2)**: INTERNAL требует SASL и у брокера-КЛИЕНТА —
`username`/`password` прямо в INTERNAL-JAAS (без них фолловеры не подключаются,
ISR проседает до лидера → NOT_ENOUGH_REPLICAS при minISR≥2; вскрыто 3-брокерным
e2e волны C). Пользователь `inter` с детерминированным per-cluster паролем
(`InterBrokerPassword` NodeEnvBuilder: SHA-256 имени кластера → 32 симв
[A-Za-z0-9]); НЕ ротируется (ротация app не должна ломать репликацию), в etcd
не хранится — listener доступен только внутри закрытой сети `kfw-net`.

### 2.3. Режимы Plain / Swarm

Порт режимов PgWorker (§2.2–2.3 arch/14): plain — `Hosts[{Name,Endpoint}]`,
per-host клиент Engine API, контейнеры `kfw-<C>-<b>`, restart-политика
`unless-stopped`; swarm — `SwarmManager`, нода = сервис `replicas=1`,
placement constraint, `publish mode=host`. Объекты для сверок — префикс
`kfw-<C>-`. Сеть `kfw-net` — attachable, создаётся воркером при
provisioning (аналог `pgw-net`).

## 3. Контракт etcd

Транспорт и схема ключей — [15-kafka-clusters.md](15-kafka-clusters.md).
Читаемые/пишемые воркером — таблицы ниже (зеркало 15 §2/§4).

### 3.1. Читаемые ключи

| Ключ | Зачем |
|---|---|
| `/kafka/clusters/<C>/config` | заявка (B/R/M/P/X) + `state` (NOT_INITIALIZED/TO_REMOVE/отсутствует=Active) |
| `/kafka/clusters/<C>/brokers/broker<k>/state` | состояния нод (свои же записи + заявки панели NOT_INITIALIZED/TO_REMOVE) |
| `/kafka/clusters/<C>/brokers/broker<k>/resources` | лимиты контейнера (cpu/mem; disk — инфо) |
| `/kafka/clusters/<C>/topics/<T>` | реестр топиков (факт + desired-заявки) |
| `/kafkaworker/rotations/<C>` | заявка ротации app-пароля |

### 3.2. Пишемые ключи

| Ключ | Когда | Значение |
|---|---|---|
| `/kafka/clusters/<C>/brokers/broker<k>/state` | весь жизненный цикл | PROVISIONING/RUNNING/UNREACHABLE/REMOVING |
| `/kafka/clusters/<C>/brokers/broker<k>/role` | план provisioning | `controller`\|`broker` (фиксация навсегда) |
| `/kafka/clusters/<C>/endpoints` | после подъёма; RMW при add/remove | `h1:p1,...` — advertised-хосты + клиентские порты |
| `/kafka/clusters/<C>/app_user` + `app_password` | provisioning ensure; ротация | `"app"` / 32 симв; txn put-if-absent / txn-коммит ротации |
| `/kafka/clusters/<C>/topics/<T>` | автосинк (тик D) | факт + сохранение/снятие desired (RMW- txn) |
| `/kafka/clusters/<C>/config` | txn по завершении provisioning | пере-put канонического JSON **без** `state` (compare mod_revision) |
| `/kafka/clusters/<C>/` (префикс) | TO_REMOVE, финал X2 | `del --prefix` |
| `/kafkaworker/{claims,work,portalloc}/<C>*` + `/kafkaworker/rotations/<C>` | TO_REMOVE, финал X2 | del — **очистка координации включает rotations**: остаточная заявка ротации не переживает удаление кластера (иначе вечный алерт `kafka-rotation-pending`) |

## 4. Секреты

Одна группа — **per-cluster, в etcd, генерирует воркер**: `app_user`=
`"app"`, `app_password` (32 симв `[A-Za-z0-9]`, txn put-if-absent — порт
P1.5 pg). Используют воркер (AdminClient), панель (read-only пробы) и
приложения (SASL/PLAIN). Ротация — процесс H по заявке панели.
Env-секретов per-install нет.

## 5. Процессы (машины состояний)

**Состояния ноды** (`brokers/<b>/state`): `NOT_INITIALIZED` (панель
заявила) → `PROVISIONING` (воркер создаёт контейнер) → `RUNNING` (брокер
в кластере); `UNREACHABLE` (молчит дольше NodeDeadSec);
`REMOVING` (в процессе демонтажа); `TO_REMOVE` (маркер панели, one-way).

Классификация тика: `config.state=NOT_INITIALIZED` → Provisioning (A);
`TO_REMOVE` → Deprovisioning (B); иначе Active-ветка: надзор (C) →
converger (E) → scale-проход remove (G) → add (F) → ротация (H, по одному
за тик) → TopicSync (D, тик `TopicSyncIntervalSec`). Все операции — только
под живым клэймом `<C>`; journal-before-manipulations.

### A. ProvisioningProcess (K0–K6)

```
K0 claim + journal(op=provision); снапшот P12 «до»
K1 план: placement + порт-аллокация (закрепление portalloc);
   роли: broker1..m — controller (m=min(3,B)); фиксация brokers/<k>/role;
   journal phase=planned
K2 ensure app-секрета: app_user/app_password txn put-if-absent
   (проигрыш → re-read существующих)
K3 на каждую ноду: volume + контейнер (env §2.2, лимиты resources,
   сеть kfw-net, клиентский host-порт) + state=PROVISIONING;
   существующие (re-run) — сверка и пропуск
K4 ждать готовности: DescribeCluster отвечает, контроллер избран,
   брокеров = B (бюджет BrokerBootSec=600 с, транзиент-толерантно);
   state=RUNNING у всех
K5 применить dynamic broker configs из заявки (Converger E — стартовый
   converge); put endpoints (advertised host:clientPort, запятая);
   config: txn (compare mod_revision) → put канонического JSON без state
K6 снапшот P12 «после»; journal done
```

Гонка «панель пишет TO_REMOVE посреди provisioning»: перечитывание config
перед фазами (порт R6) — смена state безопасно прекращает процесс.

### B. DeprovisioningProcess (X0–X3)

```
X0 claim + journal(op=deprovision); снапшот P12 «до»
X1 docker: удалить контейнеры/сервисы и volumes kfw-<C>-* (включая
   сироты из ListAsync; 404 = ок); порядок «сначала docker, потом etcd»
X2 etcd: del --prefix /kafka/clusters/<C>/ + del
   /kafkaworker/{claims,work,portalloc}/<C>* + del /kafkaworker/rotations/<C>
X3 снапшот P12 «после»; клэйм снят явно
```

### C. NodeSupervisor (надзор)

Сверка декларации с фактом docker + AdminClient-проба (DescribeCluster —
кто реально в кластере; кластер целиком недоступен → молчание трекается
по всем RUNNING-брокерам):

- **Снесённый контейнер** (объекта `kfw-<C>-<b>` нет) → пересоздание с тем же
  volume/env (адреса из portalloc, advertised стабилен), `state=PROVISIONING`;
  в `RUNNING` переводит следующий цикл по факту готовности.
- **Брокер молчит** дольше `NodeDeadSec` (90 с; трек first_seen — в
  `/kafkaworker/work/<C>`.unreachable, порт PgWorker) → `state=UNREACHABLE` +
  пересоздание с ЧИСТЫМ томом: RF>1 — rejoin репликацией (self-healing
  Kafka); RF=1 — journal-warning «данные потеряны» (документированное
  поведение) — warning-ы тика агрегируются в supervision-запись журнала.
- Ноды `TO_REMOVE`/`REMOVING`/`PROVISIONING` чужих процессов надзор не трогает.

### D. TopicSyncProcess (автосинк + desired-converge)

Тик `TopicSyncIntervalSec` (15 с), только под клэймом `<C>`. Протокол —
15 §3 (describe→decide→act): новый факт-топик → put ключ с фактом;
исчез без desired → del ключа; исчез с desired → put `missing:true`;
desired отличается по управляемым полям → применить
(AlterTopicConfigs — конфиги, CreatePartitions — только увеличение) и
снять desired (тот же RMW-txn по mod_revision; проигрыш → re-read,
следующий тик). Уменьшение partitions — перманентный отказ журнала
(панель отсекает раньше). `__`-топики — пропуск. Ретраи Polly jitter
поверх оркестрации (повтор безопасен).

### E. ClusterConfigConverger

Active-ветка, лёгкий: describe dynamic broker configs (по одному брокеру)
vs `config.{default_*}` → при отличии AlterBrokerConfigs на всех брокерах
(идемпотентный Set; применяется **без рестартов**). Маппинг полей заявки →
Kafka-конфиги: `default_retention_ms`→`log.retention.ms`,
`default_partitions`→`num.partitions`,
`replication_factor`→`default.replication.factor`,
`min_insync_replicas`→`min.insync.replicas`. Фактические топики не
трогаются (только desired-заявками).

### F. AddBrokerProcess

`brokers/<b>/state=NOT_INITIALIZED` у Active-кластера: план (host/порт;
`role=broker`) → контейнер (env: `QUORUM_VOTERS` уже зафиксирован — нода
подключается к кворуму) → ждать появления в DescribeCluster → RMW
`endpoints` (добавить адрес) → `state=RUNNING`. Уже RUNNING → no-op.

### G. RemoveBrokerProcess

Маркер `TO_REMOVE`: guards (кластер Active; не controller; не последний
брокер; на брокере нет реплик партиций — по DescribeTopics, иначе
journal-ожидание: после roadmap-reassignment демонтаж продолжится сам) →
`state=REMOVING` → удалить контейнер+volume → del префикс
`brokers/<b>/` + RMW `endpoints` (убрать адрес) + portalloc-фильтрация →
journal done. Идемпотентен на повторе.

### H. AppPasswordRotator (фазы A/B/C — без окна недоступности)

Заявка `/kafkaworker/rotations/<C>`; NEW = генерация (32 симв).

- **A)** rolling пересоздание контейнеров брокеров по одному (ждать
  возврата в ISR) с JAAS из ДВУХ пользователей (OLD+NEW) — все клиенты
  работают со OLD;
- **B)** ОДНА txn: `[compare value(app_password)==OLD][put NEW; del
  заявки]` — клиенты перечитывают etcd и переподключаются с NEW;
- **C)** rolling пересоздание с JAAS только NEW (снятие OLD-пользователя).

Отказ между фазами безопасен (оба креда валидны; перезапуск идёт с
записанной фазой из journal). Окно «часть брокеров знает только NEW»
невозможно по построению. Снапшоты P12 «до» (старт ротации) и «после»
(финал). Битая заявка — мусор: del с journal (панель до того получает
409 «уже запрошена»). Уведомление в UI-модалке: выполнять в тихое окно
(rolling-рестарты).

## 6. Надёжность

- **Идемпотентность**: каждый шаг перепроверяет факт (контейнер есть?
брокер в кластере? desired уже применён?); именование детерминировано
  (`kfw-<C>-<b>[-data]`, порты в portalloc).
- **Takeover**: состояние в etcd (journal, states, portalloc, endpoints);
  смерть инстанса гасит lease ≤ 15 с, следующий продолжает с journal-фазы.
  Двойной контроллер невозможен: операции — только под живым клэймом.
- **Атомарность etcd**: переходы — txn с compare (`mod_revision` config,
  `version==0` клэймы, RMW topics/endpoints).
- **Снапшоты P12**: лидер регулярно (раз в 6 ч) + **«до/после» в точках
  изменений** — provisioning (K0/K6), deprovisioning (X0/X3), ротация
  (старт/финал). Add/remove брокеров — без снапшотов (точки изменений —
  только три перечисленных). Retention `/snapshots`.
- **Ретраи**: короткие сетевые — Polly jitter; ожидание подъёма брокера —
  транзиент-толерантный цикл с бюджетом `BrokerBootSec`.
- **Отказ etcd**: контрол-плейн заморожен; живые брокеры от него не
  зависят. **Отказ docker-хоста**: размещение фиксировано (portalloc);
  нода UNREACHABLE → сценарии надзора.

## 7. Наблюдаемость

Health `/healthz`: `etcd-reachable` (все endpoints), `docker-hosts`
(per-host ping), `loops-alive` (последний тик каждого цикла), `claims`
(сколько держим), `snapshot-freshness`. Логи: claim/takeover, фазы
процессов (journal-фаза), rebuild ноды, converge-изменения.
Diag-ключи: `/kafkaworker/work/<C>`, `brokers/<b>/state`. Prometheus —
roadmap (t04).

## 8. Конфигурация (appsettings + env-оверрайды)

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

`AdvertisedClientHost=null` допустим только когда имя docker-хоста
резолвимо клиентами само по себе; для локальных стендов —
`host.docker.internal` (правило §2.1).

## 9. Риски

| # | Риск | Митигация |
|---|---|---|
| R1 | Образ `apache/kafka` сторонний (CVE/брейкинг-чейнджи 4.x) | версия пин (4.0.0, `Images:Node`); обновление — правкой настройки |
| R2 | Порт-коллизии 16000–16999 с ручными контейнерами | portalloc проверяет фактическую занятость перед созданием; коллизия → сдвиг порта |
| R3 | SASL-JAAS env регенерируется при каждом пересоздании контейнера (идемпотентность env) | env детерминирован от (заявка, portalloc, креды); окно ротации держит оба креда |
| R4 | RF=1: потеря тома = потеря данных | warning-журнал (документированное поведение); RF>1 — self-healing репликацией |

---

→ Возврат к [README.md](README.md). Контракт etcd —
[15-kafka-clusters.md](15-kafka-clusters.md); отложенные задачи —
[roadmap/kafkaworker.md](roadmap/kafkaworker.md).
