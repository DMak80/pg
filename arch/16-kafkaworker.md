# 16. KafkaWorker: оркестратор Kafka-кластеров ★

**KafkaWorker** — фоновый сервис (.NET 10), который по состоянию в etcd
управляет жизненным циклом Kafka-кластеров через docker (plain / docker
swarm). Это исполнительная сторона декларативного контракта
[15-kafka-clusters.md](15-kafka-clusters.md): панель AdminPanel **заявляет**
кластер через **HTTP API воркера** (§1.1; `state=NOT_INITIALIZED` в
`/kafka/clusters/<C>/config`) — воркер **поднимает** KRaft-кластер,
обеспечивает per-cluster SASL-креды (роли admin/app + per-cluster CA,
§2.3), пишет факт (`endpoints`, states, реестр
топиков) и снимает `state`; перевод в `TO_REMOVE` — воркер демонтирует
кластер полностью. **Ответственность изменений etcd**: префиксы
`/kafka/`, `/kafkaworker/` пишет ТОЛЬКО KafkaWorker (панель и сиды ходят
через его API, §1.1); панель etcd только читает.

Одиннадцать процессов:
1. **Provisioning** (A, K0–K6) — от `NOT_INITIALIZED` до рабочего кластера;
2. **Deprovisioning** (B, X0–X3) — от `TO_REMOVE` до чистого etcd и удалённых
   контейнеров/томов;
3. **NodeSupervisor** (C, надзор) — снесённый контейнер пересоздаётся, молчащий
   брокер помечается/пересоздаётся;
4. **TopicSync** (D) — автосинк реестра топиков + исполнение desired-заявок;
5. **ClusterConfigConverger** (E) — converge mutable-конфигов кластера как
   dynamic broker configs (без рестартов) + converge ACL роли app (§2.3);
6. **AddBroker** (F) — подъём broker-only ноды в Active-кластере;
7. **RemoveBroker** (G) — демонтаж брокера по маркеру `TO_REMOVE` (с guard'ами;
   непустой сначала опустошается reassignment-процессом I);
8. **PasswordRotator** (H, фазы A/B/C) — ротация per-cluster паролей
   (app / admin) без окна недоступности;
9. **PartitionReassigner** (I) — reassignment партиций: drain брокера с
   репликами (разблокирует G) и ребалансировка по заявке панели (через
   kafka-reassign CLI в контейнере брокера — AdminClient API reassignment
   в Confluent.Kafka нет);
10. **NodeRegenerator** (J) — rolling-перегенерация брокеров: автоконверге
    лимитов контейнера (cpu/mem) к `brokers/<b>/resources` — пересоздание
    контейнера по одному за тик (том сохраняется), env пересобирается из
    текущей декларации (новые server-props применяются тем же рестартом).
11. **SecurityMigrator** (M) — одноразовая converge-миграция премиграционных
    кластеров (SASL_PLAINTEXT, без admin/CA) к канону безопасности §2.1/§2.3:
    полный рестарт кластера разом (окно недоступности, §5 M).

Свойства: несколько инстансов работают одновременно (координация —
lease-клэймы в etcd, `/kafkaworker/`); смерть контролирующего инстанса не
роняет процессы — takeover ≤ TTL 15 с + тик; все операции идемпотентны;
состояние переживает смерть контроллера (etcd + тома брокеров).

Границы (что НЕ входит): bandwidth-throttle reassignment (лимит нагрузки —
батчами партиций), preferred leader election, TLS/ACL, ротация CA и серверных
сертификатов (t03 закладывает долгоживущие серты; смена — roadmap),
метрики самих Kafka-брокеров (JMX-exporter; лаги/USR снимает коллектор —
arch/18 §4), клиентская библиотека дискавери —
[roadmap/kafkaworker.md](roadmap/kafkaworker.md).

---

## 1. Роль в системе и разделение ответственности

```
AdminPanel (UI)          KafkaWorker (исполнитель)             docker-хосты
─────────────            ──────────────────────               ────────────
мутации (создание/ ──►   HTTP API воркера (§1.1)                контейнеры/
удаление/брокеры/        ──пишет──► /kafka/clusters/<C>/config  сервисы
ротация/топики/          .state=NOT_INITIALIZED/TO_REMOVE,     apache/kafka:4.0.0
ребалансировка;          brokers/<b>/state, topics/<T>.desired (KRaft, SASL)
сид)                     ──читает──► декларации
                         ──создаёт/удаёт──►
                         endpoints, states, реестр
                         топиков, снятие state
инспекция (read-   ◄──                                           
only, всё видит;         ключ /kafkaworker/api/<id> =
URL API — из etcd)       URL воркера (§1.1)
```

- **Панель** — декларатор и наблюдатель: **etcd только читает** (kafka-снапшот);
  все мутации kafka-домена (контракт — adminpanel/02 §Kafka) отправляет в
  HTTP API воркера (§1.1).
- **KafkaWorker** — исполнитель: единственный, кто создаёт/удаляет контейнеры
  брокеров, пишет `endpoints`, `brokers/<b>/{state,role}`, `app_user`/
  `app_password`, факт `topics/<T>`, снимает `state` у config, чистит
  префикс кластера при TO_REMOVE — и единственный, кто **записывает
  декларации/заявки** в etcd (приёмник мутаций панели и сида через свой
  API, §1.1).

### 1.1. HTTP API воркера (мутации панели, сиды)

Та же HTTP-грань, что `/healthz` (порт `:8080`, TLS — §1.1 ниже). Префикс `/api` — приёмник
ВСЕХ мутаций kafka-домена (панель в etcd не пишет ничего): 15 мутаций
контракта adminpanel/02 §10.2 — сигнатуры/валидации/протоколы записи 1:1
(меняется исполнитель: была панель, стал воркер; guard'ы читают etcd
напрямую, без панельного снапшота). Плюс стендовый сид — `POST
/api/seed/demo` (2 кластера `events`/`pending`, топики-архетипы, заявка
ротации, ребалансировка, drain-прогресс; набор — 1:1 сид-фикстуры
интеграционных тестов; флаг `KafkaWorker:Api:EnableSeedEndpoint`, default
`false`; идемпотентен: живой `/kafka/clusters/events/config` → 200
`{"seeded":false}`).

**Дискавери API**: ключ `/kafkaworker/api/<instanceId>` (lease TTL 15 с,
паттерн `instances/<id>`; arch/15 §4) — value
`{"url":"https://<host>:<port>","instance":"<id>","since_unix":…}`. Воркер
ставит ключ сам при старте; URL — из `KafkaWorker:Api:AdvertiseUrl`
(достижим панелью: compose-сеть стенда `https://kafkaworker:8080` или
`host.docker.internal:<порт>`; в deploy —
`https://host.docker.internal:8081`). Панель кеширует живые ключи в
kafka-снапшоте и зовёт любой живой (failover на следующий; все умерли —
503 + critical-алерт `worker-api-unreachable`).

**Аутентификация — mTLS (t03)**: вся HTTP-грань воркера (вкл. `/healthz`)
обслуживается только по TLS; клиенты (панель) аутентифицируются
клиентским сертификатом, подписанным per-install API-CA. Серверный
сертификат и доверие клиентским — env-секреты
`KFW_API_TLS_{CERT,KEY,CLIENT_CA}` (PEM; или пути `…_PATH` из volume) —
**единственные per-install секреты воркера** (осознанное исключение из
§4: транспортная граница API не может жить в etcd — etcd-клиент сам
ходит по HTTP). Клиент без валидного серта — 401 (TLS-хендшейк-отказ).
`X-Api-Key`/`KFW_API_KEY` удалён. Отключение TLS
(`KafkaWorker:Api:Tls:AllowInsecureHttp`, default false) — только для
in-memory WAF-тестов; в deploy/стенде всегда mTLS.
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
- **Listeners (канон безопасности t03)**: `CONTROLLER :9093` (PLAINTEXT,
  KRaft-кворум — только внутри закрытой сети `kfw-net-<C>`), `INTERNAL :9092`
  (межброкерный, **SASL_SSL**, advertised = docker-DNS имя ноды), `CLIENT
  :9094` (**SASL_SSL**, опубликован на хост портом из portalloc, advertised —
  по правилу ниже). TLS-сертификаты — per-cluster CA (§2.3); механизм
  SASL — PLAIN поверх TLS. Сеть `kfw-net-<C>` — per-cluster,
  attachable (alias = имя ноды, уникален в пределах сети своего кластера;
  t09-фикс: единая сеть с общими короткими алиасами `broker<N>` ломала
  Raft-голоса через docker-DNS round-robin — на одном docker-хосте собирался
  максимум один кластер; несколько кластеров обязаны быть изолированы сетями).
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
  SASL_JAAS_CONFIG` со списком пользователей по ролям (§2.3): `admin`
  (воркер/панель/CLI) и `app` (приложения); при ротации — двухпользовательское
  окно (`user_<name>=<old>` + `user_<name>2=<new>`, §5 H).
- **Служебные топики**: RF `min(3,B)`, minISR `min(2,B)` (формулы от
  фактического B — 1-брокерный стенд стартует); `auto.create.topics.enable=
  false` (создание — явное: панелью (lifecycle-заявки, 15 §3.1) или
  CLI/клиентами; автосоздание продюсером запрещено).
- **Начальные default-конфиги** из заявки (`config.{default_*}`) — env
  брокеров при создании (§2.2) и одновременно converge-цель (§5 E).
- **Volume**: `kfw-<C>-<b>-data` → `/var/lib/kafka/data` (`KAFKA_LOG_DIRS`);
  имя детерминировано, данные переживают пересоздание контейнера.
- **Placement/порты**: анти-аффинити нод по docker-хостам (порт
  PlacementPlanner PgWorker); порт-аллокатор из диапазона `16000–16999`
  (**1 клиентский порт на ноду**), закрепление в
  `/kafkaworker/portalloc/<C>`; лимиты контейнера из `resources` (cpu/mem;
  disk-заявка — инфо, квоты томов — roadmap). Занятость для довыделения =
  docker-публикации ∪ записи portalloc ВСЕХ чужих кластеров
  (`/kafkaworker/portalloc/*`, кроме своего — свой переиспользуется как
  закрепление; закрывает кросс-кластерную коллизию, включая окно «portalloc
  записан, контейнеры ещё не созданы»). **Глобальный portalloc-клэйм** (t91):
  довыделение новых портов (недобор нод, не переиспользование закреплений) —
  глобально взаимоисключающая секция «чтение занятости → выбор портов →
  запись», выполняется только держателем `/kafkaworker/locks/portalloc`
  (arch/15 §4; txn `version==0` + put-with-lease TTL 15 с). Не взял →
  InProgress (следующий тик ~5 с); смерть держателя гасит lease ≤ 15 с —
  takeover без оператора. Полностью закреплённый portalloc (rebuild, ранний
  выход без записи) клэйма не требует. Касается всех точек довыделения:
  provision K1, add-broker (§5 A/F).
- Сам воркер — контейнер с `docker.sock` (или swarm-manager), volume
  снапшотов (`kfw-snapshots`), volume TLS-секретов API (`kfw-api-tls`),
  поставляется через `deploy/docker-compose.yml`
  (`docker/KafkaWorker.Dockerfile`). **Env-секреты per-install — только
  TLS API** (§1.1): per-cluster-секреты (`app_password`, `admin_password`,
  `ca_key`) живут в etcd, генерирует воркер (§4).

### 2.2. Канонический env-набор брокера (генерирует NodeEnvBuilder)

| Env | Значение |
|---|---|
| `CLUSTER_ID` | детерминированный KRaft cluster-id из имени кластера (22 симв base64url от SHA-256) — одинаков у всех нод `<C>`, переживает пересоздание контейнера |
| `KAFKA_NODE_ID` | числовой id ноды (k из `broker<k>`) |
| `KAFKA_PROCESS_ROLES` | `broker,controller` (k ≤ m) или `broker` |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | `1@host:9093,2@host:9093,…` — только controller-ноды (внутренние адреса сети `kfw-net-<C>` кластера) |
| `KAFKA_LISTENERS` | `CONTROLLER://:9093,INTERNAL://:9092,CLIENT://:9094` |
| `KAFKA_ADVERTISED_LISTENERS` | `INTERNAL://<docker-DNS имя ноды>:9092,CLIENT://<AdvertisedClient>:<клиентский порт>` (AdvertisedClient — правило §2.1; CONTROLLER не advertised) |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` | `CONTROLLER:PLAINTEXT,INTERNAL:SASL_SSL,CLIENT:SASL_SSL` (§2.1, t03) |
| `KAFKA_CONTROLLER_LISTENER_NAMES` | `CONTROLLER` |
| `KAFKA_INTER_BROKER_LISTENER_NAME` | `INTERNAL` |
| `KAFKA_SASL_ENABLED_MECHANISMS` | `PLAIN` |
| `KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL` | `PLAIN` (Kafka требует явный механизм при SASL на INTERNAL) |
| `KAFKA_SSL_KEYSTORE_TYPE` | `PEM` (серт/ключ брокера — строками env, §2.3) |
| `KAFKA_SSL_KEYSTORE_CERTIFICATE_CHAIN` | PEM-сертификат ноды (CN=`broker<k>`, SAN — правило §2.3), подписан per-cluster CA |
| `KAFKA_SSL_KEYSTORE_KEY` | PEM PKCS#8 приватный ключ ноды |
| `KAFKA_SSL_TRUSTSTORE_TYPE` | `PEM` |
| `KAFKA_SSL_TRUSTSTORE_CERTIFICATES` | PEM per-cluster CA (`ca_pem`) — доверие всем слушателям SASL_SSL |
| `KAFKA_AUTHORIZER_CLASS_NAME` | `org.apache.kafka.metadata.authorizer.StandardAuthorizer` (KRaft-ACL, §2.3) |
| `KAFKA_SUPER_USERS` | `User:admin;User:inter;User:ANONYMOUS` — принципалы SASL-имён (воркер/панель/CLI и inter-broker не ограничены ACL) + `User:ANONYMOUS`: principal PLAINTEXT-CONTROLLER-кворума, без super.user контроллер-регистрация получает CLUSTER_AUTHORIZATION_FAILED |
| `KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND` | `false` (deny по умолчанию; ACL роли app — converge E, §2.3) |
| `KAFKA_LISTENER_NAME_INTERNAL_PLAIN_SASL_JAAS_CONFIG` | JAAS: `username="inter" password="<inter-pwd>" user_inter="<inter-pwd>" user_admin=<pwd>[ user_admin2=<new>] user_app=<pwd>[ user_app2=<new>]`; inter-креды — inter-broker-клиент (§2.2) |
| `KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG` | только список пользователей (без username/password — клиенты внешние): `user_admin`[+2] `user_app`[+2] |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | `min(3,B)` |
| `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR` | `min(3,B)` |
| `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR` | `min(2,B)` |
| `KAFKA_DEFAULT_REPLICATION_FACTOR` | R из заявки |
| `KAFKA_MIN_INSYNC_REPLICAS` | M из заявки |
| `KAFKA_NUM_PARTITIONS` | P из заявки |
| `KAFKA_LOG_RETENTION_MS` | X из заявки |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `false` |
| `KAFKA_LOG_DIRS` | `/var/lib/kafka/data` (том `kfw-<C>-<b>-data`) |

JAAS-формат (PLAIN): `org.apache.kafka.common.security.plain.PlainLoginModule required user_<name>="<password>";` — по одному `user_<name>` на кред, через пробел.

**Inter-broker-креды (§2.2)**: INTERNAL требует SASL и у брокера-КЛИЕНТА —
`username`/`password` прямо в INTERNAL-JAAS (без них фолловеры не подключаются,
ISR проседает до лидера → NOT_ENOUGH_REPLICAS при minISR≥2; вскрыто 3-брокерным
e2e волны C). Пользователь `inter` с детерминированным per-cluster паролем
(`InterBrokerPassword` NodeEnvBuilder: SHA-256 имени кластера → 32 симв
[A-Za-z0-9]); НЕ ротируется (ротация app/admin не должна ломать репликацию), в etcd
не хранится — listener доступен только внутри закрытой сети `kfw-net-<C>`
кластера. `inter` — super.user (§2.3): репликация/internal-топики не зависят
от ACL.

### 2.3. PKI, TLS и authorization (t03 — канон безопасности кластера)

**Per-cluster CA** (генерирует воркер, provisioning K2 / миграция M1; ensure
txn put-if-absent — единый механизм с кредами):

- `ca_key` — self-signed CA (RSA-2048, CN=`kfw-<C>-ca`, срок 10 лет),
  приватный ключ PEM PKCS#8; секрет etcd: подпись сертификатов нод
  (provisioning/add-broker/rebuild/NodeRegenerator). Панель не читает.
- `ca_pem` — публичный сертификат того же CA; точка дискавери (приложения и
  панель строят truststore из него — 15 §5). Ротация CA/сертов — roadmap
  (серты нод долгоживущие, 10 лет).

**Сертификаты нод**: генерирует воркер при сборке env (KafkaWorker.Core,
`CertificateRequest` .NET — без внешних инструментов), подпись `ca_key`;
CN=`broker<k>`, SAN: `DNS:broker<k>` (docker-DNS alias, INTERNAL advertised)
и `DNS:<AdvertisedClientHost>` либо `IP:<адрес>` (CLIENT advertised — то,
что клиенты валидируют по `endpoints`). В контейнер — PEM-строками env
(`ssl.keystore.type=PEM`, таблица §2.2); SAN-правило покрывает ОБА
SASL_SSL-listener'а одним сертификатом. Срок 10 лет, key size RSA-2048.
Клиентская аутентификация на listener'ах — `none` (принципалы из SASL/PLAIN:
`User:<sasl-username>`).

**Роли принципалов (authorization, StandardAuthorizer KRaft)**:

| Принципал | Роль | Права |
|---|---|---|
| `User:admin` (кред `admin_user`/`admin_password`, etcd, ensure + ротация H) | администратор | `super.users` — полный доступ без ACL-перечисления; воркер (AdminClient+CLI), панель (пробы), сид |
| `User:inter` | inter-broker | `super.users` — репликация и internal-топики не зависят от ACL |
| `User:app` (кред `app_user`/`app_password`, etcd) | приложения | ACL (converge E, идемпотентно DescribeAcls→diff→Create/DeleteAcls): TOPIC `*` {READ,WRITE,DESCRIBE}, GROUP `*` {READ,DESCRIBE}, TRANSACTIONAL_ID `*` {WRITE,DESCRIBE}; `__`-топики app-креду не нужны (internal-имена зарезервированы каноном 15 §1) |

`allow.everyone.if.no.acl.found=false` — доступ по умолчанию запрещён.
ACL живут в KRaft-метаданных (тома) — переживают пересоздания контейнеров.

**Клиентские подключения воркера**: AdminClient воркера — `admin` + `ca_pem`
по CLIENT endpoints из etcd (SASL_SSL, 15 §5; INTERNAL advertised — docker-DNS,
недостижим из процесса воркера вне сети `kfw-net-<C>`). reassign-CLI — `admin`
по INTERNAL (docker exec в контейнере брокера, §2.4). Пробы панели — `admin` +
`ca_pem` по CLIENT. Приложения — `app` + `ca_pem` по CLIENT (15 §5).

### 2.4. Kafka-CLI в контейнере брокера (транспорт процессов)

Образ `apache/kafka` несёт CLI-инструменты `/opt/kafka/bin/*.sh`
(kafka-topics, kafka-reassign-partitions, kafka-console-*); воркер выполняет
их **docker exec в контейнере живого брокера** (порт PgWorker `ExecNodeAsync`:
plain — running-контейнер по имени `kfw-<C>-<b>`, swarm — running-таск
сервиса). Транспорт подключения CLI — **INTERNAL-listener `broker<k>:9092`
SASL_SSL** (закрытая сеть `kfw-net-<C>` кластера; креды роли admin,
`--command-config`: `security.protocol=SASL_SSL` + `sasl.mechanism=PLAIN` +
JAAS admin + PEM-truststore `ca_pem`, записанный рядом однострочной
printf-обёрткой как файл `/tmp/kfw-ca.pem` — Java-клиент принимает
`ssl.truststore.type=PEM` + `ssl.truststore.location=<файл>`;
advertised INTERNAL = docker-DNS имена — резолвятся внутри сети; от
клиентского advertised-правила §2.1 и host-портов не зависит). Ограничение JVM
CLI: env `KAFKA_HEAP_OPTS=-Xmx256m` — не конкурировать с брокером за
memory-лимит контейнера. Операции CLI — идемпотентные submit-вызовы (повтор
безопасен).

### 2.5. Режимы Plain / Swarm

Порт режимов PgWorker (§2.2–2.3 arch/14): plain — `Hosts[{Name,Endpoint}]`,
per-host клиент Engine API, контейнеры `kfw-<C>-<b>`, restart-политика
`unless-stopped`; swarm — `SwarmManager`, нода = сервис `replicas=1`,
placement constraint, `publish mode=host`. Объекты для сверок — префикс
  `kfw-<C>-`. Сеть `kfw-net-<C>` — attachable, per-cluster, создаётся
  воркером при provisioning кластера и удаляется при полном демонтаже
  (последний брокер; пул subnet'ов docker-хоста конечен — сиротские сети
  не копим); короткие алиасы `broker<N>` уникальны
  в своей сети — несколько кластеров на одном docker-хосте изолированы
  (t09-фикс; бэквард-совместимость: существующие контейнеры в старой единой
  `kfw-net` продолжают работать — кластер самодостаточен внутри своей сети).

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
| `/kafkaworker/admin_rotations/<C>` | заявка ротации admin-пароля (H) |
| `/kafkaworker/rebalances/<C>` | заявка ребалансировки партиций (I) |

### 3.2. Пишемые ключи

| Ключ | Когда | Значение |
|---|---|---|
| `/kafka/clusters/<C>/brokers/broker<k>/state` | весь жизненный цикл | PROVISIONING/RUNNING/UNREACHABLE/REMOVING |
| `/kafka/clusters/<C>/brokers/broker<k>/role` | план provisioning | `controller`\|`broker` (фиксация навсегда) |
| `/kafka/clusters/<C>/endpoints` | после подъёма; RMW при add/remove | `h1:p1,...` — advertised-хосты + клиентские порты |
| `/kafka/clusters/<C>/app_user` + `app_password` | provisioning ensure; ротация H | `"app"` / 32 симв; txn put-if-absent / txn-коммит ротации |
| `/kafka/clusters/<C>/admin_user` + `admin_password` | provisioning ensure; ротация H; миграция M1 | `"admin"` / 32 симв; txn put-if-absent / txn-коммит ротации |
| `/kafka/clusters/<C>/ca_pem` + `ca_key` | provisioning ensure (K2); миграция M1 | PEM per-cluster CA (§2.3); txn put-if-absent |
| `/kafka/clusters/<C>/topics/<T>` | автосинк (тик D) | факт + сохранение/снятие desired (RMW-txn); + del `topics/<T>/desired.{create,delete}` после исполнения lifecycle-заявок (одной txn с del факт-ключа при delete) |
| `/kafka/clusters/<C>/config` | txn по завершении provisioning | пере-put канонического JSON **без** `state` (compare mod_revision) |
| `/kafka/clusters/<C>/` (префикс) | TO_REMOVE, финал X2 | `del --prefix` |
| `/kafkaworker/reassignments/<C>` | процесс I: put при работе, del по завершении | `{"mode","drain_broker"?,"partitions_total","partitions_remaining","submitted_unix","updated_unix","instance","last_error"?}` (15 §4) |
| `/kafkaworker/regens/<C>` | процесс J: put при старте первого пересоздания, del по сходимости | `{"brokers_total","brokers_remaining","current_broker"?,"updated_unix","instance","last_error"?}` (15 §4) — прогресс rolling-регенерации |
| `/kafkaworker/locks/portalloc` | захват секции довыделения портов (t91, §2.1) | `{"instance":"<id>","since_unix":…}` — lease TTL 15 с, txn `version==0` + put-with-lease; del + revoke lease по завершении секции (arch/15 §4) |
| `/kafkaworker/rebalances/<C>` | процесс I: del по завершении ребалансировки | заявка панели, дожившая до факта == план (порядок «сначала факт, потом del заявки» — повтор тика после сбоя del безвреден) |
| `/kafkaworker/{claims,work,portalloc}/<C>*` + `/kafkaworker/{rotations,admin_rotations,rebalances,reassignments,regens}/<C>` | TO_REMOVE, финал X2 | del — **очистка координации включает заявки и прогресс**: остаточные заявки/прогресс не переживают удаление кластера (иначе вечные алерты `kafka-rotation-pending`/`kafka-rebalance-pending`) |

## 4. Секреты

Per-cluster, в etcd, генерирует воркер (ensure txn put-if-absent; §2.3):

- **app** — `app_user`=`"app"`, `app_password` (32 симв `[A-Za-z0-9]`):
  приложения (SASL/PLAIN поверх TLS, ACL-роль app). Ротация — процесс H
  по заявке панели.
- **admin** — `admin_user`=`"admin"`, `admin_password`: воркер
  (AdminClient/CLI), панель (read-only пробы), super.user. Ротация —
  процесс H по заявке панели (`admin_rotations/`).
- **CA кластера** — `ca_key` (приватный, подпись сертов нод) + `ca_pem`
  (публичный, дискавери). Не ротируются (roadmap).
- **inter-broker** — детерминированный пароль из имени кластера (§2.2),
  вне etcd, super.user.

Env-секреты per-install — только TLS HTTP API (§1.1:
`KFW_API_TLS_{CERT,KEY,CLIENT_CA}`): транспортная граница API не может
жить в etcd — etcd-клиент сам ходит по HTTP.

## 5. Процессы (машины состояний)

**Состояния ноды** (`brokers/<b>/state`): `NOT_INITIALIZED` (панель
заявила) → `PROVISIONING` (воркер создаёт контейнер) → `RUNNING` (брокер
в кластере); `UNREACHABLE` (молчит дольше NodeDeadSec);
`REMOVING` (в процессе демонтажа); `TO_REMOVE` (маркер панели, one-way).

Классификация тика: `config.state=NOT_INITIALIZED` → Provisioning (A);
`TO_REMOVE` → Deprovisioning (B); иначе Active-ветка: **премиграционный
кластер (нет `ca_pem`/`admin_password` или брокеры без SASL_SSL-env) →
SecurityMigrator (M, до всего Active)**; надзор (C) → converger (E, вкл.
ACL) → reassignment (I, тик `ReassignIntervalSec` — drain
TO_REMOVE-брокеров с репликами и заявка ребалансировки) → scale-проход
remove (G) → add (F) → ротация (H, по одному за тик — app или admin) →
регенерация (J, одно пересоздание за тик) → TopicSync (D, тик
`TopicSyncIntervalSec`).
Reassignment стоит перед remove — к моменту G дренируемый брокер уже пуст.
Все операции — только под живым клэймом `<C>`;
journal-before-manipulations. Kafka-шаги Active (E–J, D) пропускаются на
время backoff недоступного кластера (15→60→300 с, сброс при успехе, t05) —
тик успех; docker-надзор и provisioning не гейтятся (self-healing
не зависит от kafka).

### A. ProvisioningProcess (K0–K6)

```
K0 claim + journal(op=provision); снапшот P12 «до»
K1 план: placement + порт-аллокация (закрепление portalloc) — под
   глобальным portalloc-клэймом /kafkaworker/locks/portalloc (§2.1):
   занят = docker ∪ portalloc чужих кластеров; не взял клэйм → journal
   waiting-portalloc-lock (InProgress, следующий тик);
   роли: broker1..m — controller (m=min(3,B)); фиксация brokers/<k>/role;
   journal phase=planned
K2 ensure секретов кластера: CA (ca_pem/ca_key) + креды admin + app —
   txn put-if-absent (проигрыш → re-read существующих)
K3 на каждую ноду: volume + контейнер (env §2.2, лимиты resources,
   сеть kfw-net-<C>, клиентский host-порт) + state=PROVISIONING;
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
кто реально в кластере). **Данные неприкосновенны** (spec §4.2 C): надзор
никогда не уничтожает тома из-за недоступности пробы/сети.

- **Снесённый контейнер** (объекта `kfw-<C>-<b>` нет; docker-факт, не зависит
  от пробы) → пересоздание с тем же volume/env (адреса из portalloc,
  advertised стабилен), `state=PROVISIONING`; в `RUNNING` переводит следующий
  цикл по факту готовности.
- **Брокер молчит** дольше `NodeDeadSec` (90 с; трек first_seen — в
  `/kafkaworker/work/<C>`.unreachable, порт PgWorker; счётчик стартует/держится
  только по УСПЕШНОМУ ответу пробы «в кластере нет брокера X») →
  `state=UNREACHABLE` + пересоздание КОНТЕЙНЕРА: **том сохраняется всегда**
  (брокер возвращается со своими данными; RF>1 — rejoin репликацией
  self-healing Kafka). **Чистый том — только при доказанной физической утрате
  тома** (объект volume `kfw-<C>-<b>-data` не существует в docker — терять
  нечего; «не можем проверить» = том жив): RF=1 + утрата — journal-warning
  «единственная копия данных потеряна» (документированное поведение).
  Warning-ы тика агрегируются в supervision-запись журнала.
- **Одно пересоздание по молчанию за тик**: следующий молчащий брокер — после
  возврата предыдущего в кластер/ISR (следующий тик). Никаких массовых
  пересозданий подряд.
- **Слепая проба** (DescribeCluster недоступен / кластер не поднят): бюджет
  молчания не стартует и не исполняется, прошлый трек сохраняется (чистка
  только по исчезновению из декларации); пересоздания по молчанию запрещены —
  собственная слепота воркера не повод трогать брокеров. Слепота пробы ≠
  молчание брокеров.
- **Backoff недоступного кластера** (t05): окно 15→60→300 с (сброс при
  успехе; писатели — проба надзора и коллектор метрик — первые kafka-контакты
  конвейера) гейтит kafka-пробу (окно активно → слепая проба без сети и без
  клиента; unreachable-трек заморожен — флап ≠ смерть, S7) и kafka-шаги
  Active-ветки; docker-часть надзора и provisioning не гейтятся. В etcd
  backoff ничего не пишет. **Лестница E9** (arch/17): безадресные
  Supervisable-брокеры — до любых деструктивных действий — portalloc →
  реконструкция из inspect живого контейнера (published-порт CLIENT + host,
  put-if-absent под `locks/portalloc`, проигрыш txn → re-read) → новая
  аллокация (S7-свидетельство смерти) + RMW endpoints; тупик «не закреплён
  в portalloc» устранён; клэйм занят → journal waiting-portalloc-lock
  (InProgress, следующий тик).
- Перевод `PROVISIONING`→`RUNNING` — по трём фактам: контейнер жив, зрячая
  проба видит брокера, advertised-адрес уже в `endpoints` (владелец
  процесса — add-broker F — пишет endpoints ДО RUNNING; иначе чужой процесс
  «догоняется» и адрес выпадает из bootstrap-списка). `endpoints` сходится
  к portalloc-канону тиком надзора (расхождение → RMW; закрывает недоехавший
  RMW лестницы E9).
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

Lifecycle-заявки (15 §3.1): исполнение перед факт-синком (порядок: чистка
create-коллизий → delete → create → sync), guards и идемпотентность — там же.

### E. ClusterConfigConverger

Active-ветка, лёгкий: describe dynamic broker configs (по одному брокеру)
vs `config.{default_*}` → при отличии AlterBrokerConfigs на всех брокерах
(идемпотентный Set; применяется **без рестартов**). Маппинг полей заявки →
Kafka-конфиги: `default_retention_ms`→`log.retention.ms`,
`default_partitions`→`num.partitions`,
`replication_factor`→`default.replication.factor`,
`min_insync_replicas`→`min.insync.replicas`. Фактические топики не
трогаются (только desired-заявками).

**ACL-converge (t03, тот же лёгкий шаг тика)**: DescribeAcls → diff к
каноническому ACL-плану роли app (§2.3) → CreateAcls/DeleteAcls
(идемпотентная сходимость; admin/inter — super.users, ACL для них не
нужны). Авторизатор StandardAuthorizer включён env нод (§2.2);
deny-by-default — app-клиенты получают доступ только после первого
converge (секунды после подъёма; снапшот ACL живёт в KRaft-метаданных —
повторные converge почти всегда no-op).

### F. AddBrokerProcess

`brokers/<b>/state=NOT_INITIALIZED` у Active-кластера: план (host/порт;
`role=broker`) (добор портов — под глобальным portalloc-клэймом §2.1;
не взял → journal waiting-portalloc-lock) → контейнер (env:
`QUORUM_VOTERS` уже зафиксирован — нода
подключается к кворуму) → ждать появления в DescribeCluster → RMW
`endpoints` (добавить адрес) → `state=RUNNING`. Уже RUNNING → no-op.

### G. RemoveBrokerProcess

Маркер `TO_REMOVE`: guards (кластер Active; не controller; не последний
брокер; на брокере нет реплик партиций **включая internal-топики** — по
describe-all; реплики есть → journal-ожидание: процесс I дренирует брокер,
демонтаж продолжится сам следующими тиками) → `state=REMOVING` → удалить
контейнер+volume → del префикс `brokers/<b>/` + RMW `endpoints` (убрать
адрес) + portalloc-фильтрация → journal done. Идемпотентен на повторе.
Демонтаж исполняется только по пустому брокеру без USR (партиции затронутых
топиков полностью синхронны) — удаления, роняющие доступность, исключены.

### H. PasswordRotator (фазы A/B/C — без окна недоступности; роли app|admin)

Заявка `/kafkaworker/rotations/<C>` (app) или `/kafkaworker/admin_rotations/<C>`
(admin); NEW = генерация (32 симв). Ротируются независимо (механизм один):

- **A)** rolling пересоздание контейнеров брокеров по одному (ждать
  возврата в ISR) с JAAS из ДВУХ пользователей роли (OLD+NEW: `user_app`+
  `user_app2` либо `user_admin`+`user_admin2`) — все клиенты работают
  со OLD;
- **B)** ОДНА txn: `[compare value(<роль>_password)==OLD][put NEW; del
  заявки]` — клиенты перечитывают etcd и переподключаются с NEW;
- **C)** rolling пересоздание с JAAS только NEW (снятие OLD-пользователя).

Отказ между фазами безопасен (оба креда валидны; перезапуск идёт с
записанной фазой из journal). Окно «часть брокеров знает только NEW»
невозможно по построению. Снапшоты P12 «до» (старт ротации) и «после»
(финал). Битая заявка — мусор: del с journal (панель до того получает
409 «уже запрошена»). Уведомление в UI-модалке: выполнять в тихое окно
(rolling-рестарты). Ротация admin не трогает app-кред и inter; воркер
переключается на NEW-кред клиента после B (следующий снапшот-тик).

### I. PartitionReassigner (drain + ребалансировка)

Reassignment партиций: (а) **drain** — опустошение брокеров `TO_REMOVE` с
репликами (разблокирует G); (б) **balance** — ребалансировка размещения по
заявке панели `/kafkaworker/rebalances/<C>`. AdminClient API reassignment в
Confluent.Kafka нет — исполнение через `kafka-reassign-partitions.sh`
в контейнере брокера (§2.4). Тик `ReassignIntervalSec`, только под клэймом;
describe-all (метаданные всех топиков **включая `__`**) → decide (чистые
функции плана) → act (submit батча ≤ `ReassignBatchPartitions` партиций) →
прогресс-ключ `/kafkaworker/reassignments/<C>` (15 §4).

- **Drain-план** (per партиция с репликой на drain-брокере):
  newReplicas = старые реплики без drain (порядок сохранён — лидер меняется
  только если он и есть drain) + добор наименее загруженных живых брокеров
  (greedy по счётчику плана) до `min(len(старых), число целей)` реплик.
  Инвариант плана: `newReplicas ≥ min.insync.replicas` топика — иначе
  journal-отказ с причиной (оператор снижает minISR заявкой или добавляет
  брокеров); для internal-топиков minISR владеет воркер — снижает сам до
  `min(2, B')` (формулы §2.1 от фактического числа брокеров B'). Снижение RF
  (целей меньше старого RF) — допустимое следствие демонтажа; факт в реестре
  обновит автосинк D.
- **Balance-план** (цель = converge к декларации): юзер-топики — RF
  `min(config.replication_factor, число целей)`, internal — формулы §2.1;
  первая реплика (лидер) сохраняется, добор остальных — наименее загруженные
  живые брокеры, детерминизм сортировкой (topic, partition, brokerId).
  Сходимость = факт == план по всем партициям → del заявки.
- **Цели переезда** — только `RUNNING`-брокеры (не TO_REMOVE/REMOVING/
  PROVISIONING/UNREACHABLE). Заявка balance при живых drain-кандидатах
  ждёт (journal waiting-drain): сначала демонтаж, потом баланс.
- **Завершение** (критерий по факту метаданных): drain — drain-брокер
  отсутствует в Replicas всех партиций и затронутые топики без USR
  (ISR == assignment); баланс — факт == план. Слепая проба — никаких
  действий (передержка, как надзор C): подача вслепую запрещена.
- **Идемпотентность/отказоустойчивость**: повторная подача того же
  assignment безопасна (семантика KIP-455); состояние = факт Kafka +
  прогресс-ключ (перестраиваем: потеря ключа — следующий тик пересчитает
  план от факта); смерть контроллера — takeover, in-flight reassignment
  живёт в Kafka; сбой между фактом и del заявки — повтор тика доигрывает.
  Дедуп подач: тот же батч не переподаётся чаще раза в
  `ReassignRetrySubmitSec` (защита от потерянного submit без спама CLI).
- Батчи партиций (`ReassignBatchPartitions`) — самоограничение нагрузки
  (bandwidth-throttle — roadmap); новый батч — только после завершения
  предыдущего (по критерию выше).

### J. NodeRegenerator (rolling-перегенерация брокеров)

Автоконверге лимитов контейнера к декларации `brokers/<b>/resources`
(изменение — мутация №15 панели через API, adminpanel/02 §10.2). Docker не
меняет лимиты живого контейнера — сходимость только пересозданием; триггер —
**только расхождение cpu/mem** (сверка inspect контейнера: NanoCpus/Memory
vs декларация; `resources=null` или `disk` — не сверяются: disk — инфо,
квоты томов — roadmap). Env пересобирается из текущей декларации тем же
пересозданием (NodeEnvBuilder — детерминизм R3): новые server-props
применяются попутно, но **env-дрейф триггером НЕ является** — иначе
конфиг-мутации, уже применённые converger'ом E без рестартов, требовали бы
лишних рестартов.

- **Один брокер за тик**: снять контейнер (том сохраняется всегда) →
  пересоздать (EnsureNode с лимитами из декларации) → `state=PROVISIONING`;
  в `RUNNING` доводит AddBrokerProcess (F) следующими тиками по факту
  DescribeCluster; следующий брокер регенерируется только после `RUNNING`
  предыдущего. Прецедент темпа — надзор C («одно пересоздание по молчанию
  за тик»).
- **Прогресс-ключ** `/kafkaworker/regens/<C>` (15 §4): put при старте
  первого пересоздания (`brokers_total`/`brokers_remaining`/`current_broker`),
  обновление на каждом шаге, del по сходимости (все стабильные брокеры
  `RUNNING` и лимиты == декларации). Ключ жив = операция идёт (панель
  показывает в деталях кластера).
- **Guard'ы**: только `RUNNING`-брокеры (TO_REMOVE/REMOVING/PROVISIONING/
  NOT_INITIALIZED/UNREACHABLE — чужие процессы, их владельцы доведут);
  живая заявка ротации (`rotations/<C>` или journal-фаза ротации) или живой
  reassignment (`reassignments/<C>`) — передержка (journal waiting,
  без действий: пересоздания не смешиваются с чужими rolling/переездами).
  Inspect недоступен (docker-хост молчит) — ошибка тика, никаких пересозданий
  вслепую (порт слепоты надзора C).
- **Идемпотентность/takeover**: состояние = факт docker + декларация +
  прогресс-ключ (перестраиваем от факта; потеря ключа — следующий тик
  пересчитает расхождения). PUT ресурсов посреди регенерации безопасен:
  если новый контейнер собрался по старой декларации — следующий тик увидит
  расхождение снова и перегенерирует (сходимость к последней декларации).
- Регенерация — без снапшотов P12 (etcd-декларацию воркер не меняет; как
  add/remove брокеров, §6).

### M. SecurityMigrator (премиграционные кластеры → канон t03)

Детект (Active-ветка, до всех остальных шагов): у кластера нет
`ca_pem`/`ca_key`/`admin_password` в etcd ИЛИ env живых контейнеров
брокеров без `KAFKA_SSL_TRUSTSTORE_TYPE` (docker inspect; etcd-ключи могли
уже появиться ensure'ом M1). Kafka не допускает смешанные security-протоколы
inter-broker у нод одного кластера — rolling «по одному» роняет ISR ниже
minISR на середине, поэтому миграция — **полный рестарт кластера разом**
(административная операция с объявленным окном недоступности ~1–3 мин;
прецедент — мажор-апгрейд):

```
M0 claim + journal(op=migrate-security); снапшот P12 «до»; guard'ы:
   живых ротаций/reassignment/regens нет (передержка journal-waiting)
M1 ensure: CA (ca_pem/ca_key) + admin-кред txn put-if-absent
M2 stop ВСЕХ контейнеров брокеров кластера разом → пересоздание всех
   с каноническим env §2.2/§2.3 (SASL_SSL, authorizer, JAAS admin+app;
   порты/тома/сеть/roles те же — placement и portalloc не меняются)
M3 ждать готовности (K4-паттерн: DescribeCluster с admin-кредом по
   CLIENT endpoints из etcd — SASL_SSL, бюджет BrokerBootSec) → state=RUNNING у всех
M4 ACL-converge (E) стартовый; endpoints не меняются (хосты/порты те же);
   снапшот P12 «после»; journal done
```

Идемпотентность: каждый шаг перепроверяет факт (ключи etcd есть? env
контейнеров уже SSL? брокеры в кластере?); отказ между M2-нодами → повтор
тика доводит (пересоздание оставшихся; поднятые пропускаются сверкой env).
Данные неприкосновенны (тома сохраняются всегда). Клиентский контракт
меняется одним шагом: приложения/панель, ещё не умеющие TLS, получают
отказ до обновления (breaking change дискавери 15 §5 — заявляется релизом
t03; панель обновляется тем же релизом). После M кластер неотличим от
поднятого канонически (второй детект-проход — no-op).

## 6. Надёжность

- **Идемпотентность**: каждый шаг перепроверяет факт (контейнер есть?
  брокер в кластере? desired уже применён? reassignment-батч завершён?);
  именование детерминировано (`kfw-<C>-<b>[-data]`, порты в portalloc);
  повторная подача того же reassignment-assignment безопасна (I).
- **Takeover**: состояние в etcd (journal, states, portalloc, endpoints);
  смерть инстанса гасит lease ≤ 15 с, следующий продолжает с journal-фазы.
  Двойной контроллер невозможен: операции — только под живым клэймом.
- **Атомарность etcd**: переходы — txn с compare (`mod_revision` config,
  `version==0` клэймы, RMW topics/endpoints).
- **AdminClient-кэш (t05)**: фабрика — sharable-кэш per
  (bootstrap, user, password); Create — «получить клиент ключа», DisposeAsync
  адаптера — no-op (владение у кэша); пины librdkafka
  `reconnect/retry.backoff.ms ≥ 1000` (+ rdkafka-лог на Debug); Failed
  операции → unhealthy-инвалидация → пересоздание (Dispose заменяемого —
  в фоне); неактивные ключи вытесняются; остановка — детерминированный
  Dispose (кэш = IDisposable DI-синглтон). Инцидент-класс: t11 (панель).
- **Снапшоты P12**: лидер регулярно (раз в 6 ч) + **«до/после» в точках
  изменений** — provisioning (K0/K6), deprovisioning (X0/X3), ротация
  (старт/финал). Add/remove брокеров и регенерация (J) — без снапшотов
  (точки изменений — только три перечисленных). Retention `/snapshots`.
- **Ретраи**: короткие сетевые — Polly jitter; ожидание подъёма брокера —
  транзиент-толерантный цикл с бюджетом `BrokerBootSec`.
- **Отказ etcd**: контрол-плейн заморожен; живые брокеры от него не
  зависят. **Отказ docker-хоста**: размещение фиксировано (portalloc);
  нода UNREACHABLE → сценарии надзора.

## 7. Наблюдаемость

Health `/healthz`: `etcd-reachable` (все endpoints), `docker-hosts`
(per-host ping), `loops-alive` (последний тик каждого цикла), `claims`
(сколько держим), `snapshot-freshness`. Канон честного health (t09):

- **healthz = последнее состояние цикла, а не первый сбой**: успешный тик
  гасит `StatusError` прошлого тика (живой-Ф7, порт циклов PgWorker) —
  transient-сбой (пересоздание etcd-контейнера и т.п.) ≠ вечный 503 до
  рестарта воркера;
- **чек всегда отдаёт структуру**: активные пробы (etcd/docker-hosts)
  оборачиваются catch-all — сетевое исключение пробы становится
  `Result.Failed` → `Degraded` с данными секций, а не исключением чека;
- **etcd-клиент — SocketsHttpHandler** с `PooledConnectionLifetime`
  (периодический пере-резолв DNS; лечит застарелый пул коннектов после
  пересоздания etcd-контейнера) и последовательным IPv4-first-резолвом в
  `ConnectCallback` — параллельные A/AAAA-запросы .NET против Docker
  embedded DNS (127.0.0.11) флейпят «Name or service not known».

**Единая правда для панели** (t09): панель опрашивает `/healthz` живых
инстансов по URL из `/kafkaworker/api/<id>` (порт паттерна PgWorker,
arch/adminpanel/02 §2.3.2) — docker-health и панель видят одно и то же
здоровье, degraded/unhealthy виден алертом `worker-unhealthy` ≤ 2 тиков
поллера и гаснет после восстановления.

Логи: claim/takeover, фазы процессов (journal-фаза), rebuild ноды,
converge-изменения. Diag-ключи: `/kafkaworker/work/<C>`,
`brokers/<b>/state`. Prometheus-метрики — единый каркас [18-metrics.md](18-metrics.md):
`/metrics` на том же порту `:8080` (словарь §2.2 воркер-паттерн + §2.3
коллектор лагов/USR).

## 8. Конфигурация (appsettings + env-оверрайды)

```
KafkaWorker:Etcd { Endpoints[] }
KafkaWorker:Docker { Mode: Plain|Swarm, Hosts[{Name,Endpoint}], SwarmManager,
                     PortRange{From=16000,To=16999}, Images{Node="apache/kafka:4.0.0"} }
KafkaWorker:Loops { ScanIntervalSec=5, KeepaliveSec=5, ErrorDelayMs=2000,
                    TopicSyncIntervalSec=15, ReassignIntervalSec=15,
                    ReassignBatchPartitions=10 }
KafkaWorker:Thresholds { BrokerBootSec=600, NodeDeadSec=90, ReassignExecSec=180,
                         ReassignRetrySubmitSec=120 }
KafkaWorker:Parallelism { MaxClusters=4 }
KafkaWorker:Snapshots { Dir="/snapshots", RetentionFiles=10 }
KafkaWorker:AdvertisedClientHost=null   # null → адрес docker-хоста ноды (placement)
KafkaWorker:Api { AdvertiseUrl (https://…), EnableSeedEndpoint=false,
                  Tls { ServerCertPem|ServerCertPath, ServerKeyPem|ServerKeyPath,
                        ClientCaPem|ClientCaPath, AllowInsecureHttp=false } }
                                        # §1.1: mTLS HTTP API — серверный серт +
                                        # доверие клиентским (per-install API-CA);
                                        # URL https:// в /kafkaworker/api/<id>
# env-секреты (§1.1/§4): KFW_API_TLS_CERT / KFW_API_TLS_KEY /
#   KFW_API_TLS_CLIENT_CA (PEM) или KFW_API_TLS_{CERT,KEY,CLIENT_CA}_PATH
#   (файлы volume kfw-api-tls); KFW_API_KEY удалён (t03)
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
| R5 | JVM CLI в контейнере брокера конкурирует с брокером за memory-лимит (OOM-kill) | `KAFKA_HEAP_OPTS=-Xmx256m` (§2.4); exec одноразовый, не параллелится |
| R6 | Долгий drain на больших объёмах данных (минуты–часы) | батчи партиций + прогресс-ключ (UI видит остаток); bandwidth-throttle — roadmap |
| R7 | Уменьшение mem/cpu в мутации №15 ниже рабочего набора брокера → OOM-килл / деградация (валидация §10.3 границы не отсекает) | ответственность оператора (UI-предупреждение в модалке); OOM-рестарт подхватит надзор C (том жив — данные не теряются) |
| R8 | SAN сертификата ноды не покрывает фактический advertised-хост (смена `AdvertisedClientHost` после подъёма) → TLS-отказ клиентов на CLIENT | SAN строится по тому же advertised-правилу §2.1 при каждой пересборке env; смена host-настройки требует пересоздания брокеров (NodeRegenerator-событие или пере-ensure: снапшот-детект env vs декларация — M-механика) |
| R9 | Окно миграции M: клиенты без TLS-поддержки получают отказ после M2 (breaking change дискавери) | заявляется релизом (панель обновляется тем же релизом; приложения — библиотекой дискавери); окно ~1–3 мин, тома живы, повтор тика доводит |
| R10 | Приватный ключ CA (`ca_key`) в etcd — компрометация etcd = подделка сертов кластера | etcd — уже хранилище per-cluster-секретов (паролей); зона доверия контроль-плейна; ротация CA — roadmap; доступ к etcd только из закрытой сети установки |

---

→ Возврат к [README.md](README.md). Контракт etcd —
[15-kafka-clusters.md](15-kafka-clusters.md); отложенные задачи —
[roadmap/kafkaworker.md](roadmap/kafkaworker.md).
