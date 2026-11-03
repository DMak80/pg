# KafkaWorker: модель размещения и узлы кластера

Уровень 3. Как устроены брокеры кластера: контейнер, KRaft-роли, listeners,
сеть, адресация, канонический env и режимы Plain/Swarm. Процессы,
использующие модель, — [жизненный цикл](02-cluster-lifecycle.md),
[масштабирование](03-broker-scaling.md); PKI и SASL — [безопасность](04-security.md).

## 1. Нода кластера = контейнер `apache/kafka:4.0.0`

- Образ — официальный `apache/kafka:4.0.0` (пин в настройке `Images:Node`),
  KRaft-only (ZooKeeper в 4.x удалён), полностью конфигурируется env.
  Кастомный образ НЕ собирается (отличие от `pgworker-node`): вся
  конфигурация — env, генерирует воркер при создании контейнера
  (идемпотентная сверка по имени; таблица env — §2).
- **Роли KRaft**: при создании B брокеров ноды `broker1..broker_m`
  (m = min(3,B)) — `PROCESS_ROLES=broker,controller` (кворум
  `KAFKA_CONTROLLER_QUORUM_VOTERS` фиксируется по этим нодам),
  `broker_{m+1}..B` — broker-only. `brokers/<k>/role` фиксирует роль навсегда;
  добавляемые позже брокеры — всегда broker-only (кворум не меняется),
  контроллерные ноды не демонтируются.
- **Listeners (канон безопасности)**: `CONTROLLER :9093` (PLAINTEXT,
  KRaft-кворум — только внутри закрытой сети `kfw-net-<C>`), `INTERNAL :9092`
  (межброкерный, **SASL_SSL**, advertised = docker-DNS имя ноды), `CLIENT
  :9094` (**SASL_SSL**, опубликован на хост портом из portalloc, advertised —
  по правилу ниже). TLS-сертификаты — per-cluster CA
  ([04-security.md](04-security.md) §1); механизм SASL — PLAIN поверх TLS.
  Сеть `kfw-net-<C>` — per-cluster, attachable (alias = имя ноды, уникален в
  пределах сети своего кластера; единая сеть с общими короткими алиасами
  `broker<N>` ломала Raft-голоса через docker-DNS round-robin — на одном
  docker-хосте собирался максимум один кластер; несколько кластеров обязаны
  быть изолированы сетями).
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
  SASL_JAAS_CONFIG` со списком пользователей по ролям
  ([04-security.md](04-security.md) §2): `admin` (воркер/панель/CLI) и `app`
  (приложения); при ротации — двухпользовательское окно
  (`user_<name>=<old>` + `user_<name>2=<new>`,
  [H. PasswordRotator](processes/h-password-rotator.md)).
- **Служебные топики**: RF `min(3,B)`, minISR `min(2,B)` (формулы от
  фактического B — 1-брокерный стенд стартует); `auto.create.topics.enable=
  false` (создание — явное: панелью (lifecycle-заявки,
  [kafka-clusters/03-topics.md](../kafka-clusters/03-topics.md).1) или CLI/клиентами;
  автосоздание продюсером запрещено).
- **Начальные default-конфиги** из заявки (`config.{default_*}`) — env
  брокеров при создании (§2) и одновременно converge-цель
  ([E. ClusterConfigConverger](processes/e-config-converger.md)).
- **Volume**: `kfw-<C>-<b>-data` → `/var/lib/kafka/data` (`KAFKA_LOG_DIRS`);
  имя детерминировано, данные переживают пересоздание контейнера.
- **Placement/порты**: анти-аффинити нод по docker-хостам (общий
  `Shared.Core/Planning`); порт-аллокатор из диапазона `16000–16999`
  (**1 клиентский порт на ноду**), закрепление в
  `/kafkaworker/portalloc/<C>`; лимиты контейнера из `resources` (cpu/mem;
  disk-заявка — инфо, квоты томов — roadmap). Занятость для довыделения =
  docker-публикации ∪ записи portalloc ВСЕХ чужих кластеров
  (`/kafkaworker/portalloc/*`, кроме своего — свой переиспользуется как
  закрепление; закрывает кросс-кластерную коллизию, включая окно «portalloc
  записан, контейнеры ещё не созданы»). **Глобальный portalloc-клэйм**:
  довыделение новых портов (недобор нод, не переиспользование закреплений) —
  глобально взаимоисключающая секция «чтение занятости → выбор портов →
  запись», выполняется только держателем `/kafkaworker/locks/portalloc`
  ([kafka-clusters/04-coordination.md](../kafka-clusters/04-coordination.md); txn `version==0` +
  put-with-lease TTL 15 с). Не взял → InProgress (следующий тик ~5 с); смерть
  держателя гасит lease ≤ 15 с — takeover без оператора. Полностью
  закреплённый portalloc (rebuild, ранний выход без записи) клэйма не
  требует. Касается всех точек довыделения: provision K1, add-broker
  ([A](processes/a-provisioning.md), [F](processes/f-add-broker.md)).
- Сам воркер — контейнер с `docker.sock` (или swarm-manager), volume
  снапшотов (`kfw-snapshots`), volume TLS-секретов API (`kfw-api-tls`),
  поставляется через `deploy/docker-compose.yml`
  (`docker/KafkaWorker.Dockerfile`). **Env-секреты per-install — только
  TLS API** ([07-api.md](07-api.md) §2): per-cluster-секреты
  (`app_password`, `admin_password`, `ca_key`) живут в etcd, генерирует
  воркер ([04-security.md](04-security.md) §3).
- **Два инстанса по умолчанию**: деплой-рецепт поднимает второй сервис
  `kafkaworker-2` того же образа — уникальный хост-порт публикации
  (`KFW_API_HOST_PORT2`, ряд 8081/8084) и уникальный `Api:AdvertiseUrl`
  (lease-ключ `/kafkaworker/api/<InstanceId>`), общие на оба инстанса тома
  `kfw-snapshots` и `kfw-api-tls:ro` (лидерство снапшотов мигрирует между
  инстансами), уникальность — случайный `InstanceId` без env-идентификации.
  Dev-стенд — симметрично: `as-kafkaworker-2` (advertise по compose-DNS
  `https://kafkaworker-2:8080`, без хост-публикации порта).

## 2. Канонический env-набор брокера (генерирует NodeEnvBuilder)

| Env | Значение |
|---|---|
| `CLUSTER_ID` | детерминированный KRaft cluster-id из имени кластера (22 симв base64url от SHA-256) — одинаков у всех нод `<C>`, переживает пересоздание контейнера |
| `KAFKA_NODE_ID` | числовой id ноды (k из `broker<k>`) |
| `KAFKA_PROCESS_ROLES` | `broker,controller` (k ≤ m) или `broker` |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | `1@host:9093,2@host:9093,…` — только controller-ноды (внутренние адреса сети `kfw-net-<C>` кластера) |
| `KAFKA_LISTENERS` | `CONTROLLER://:9093,INTERNAL://:9092,CLIENT://:9094` |
| `KAFKA_ADVERTISED_LISTENERS` | `INTERNAL://<docker-DNS имя ноды>:9092,CLIENT://<AdvertisedClient>:<клиентский порт>` (AdvertisedClient — правило §1; CONTROLLER не advertised) |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` | `CONTROLLER:PLAINTEXT,INTERNAL:SASL_SSL,CLIENT:SASL_SSL` (§1) |
| `KAFKA_CONTROLLER_LISTENER_NAMES` | `CONTROLLER` |
| `KAFKA_INTER_BROKER_LISTENER_NAME` | `INTERNAL` |
| `KAFKA_SASL_ENABLED_MECHANISMS` | `PLAIN` |
| `KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL` | `PLAIN` (Kafka требует явный механизм при SASL на INTERNAL) |
| `KAFKA_SSL_KEYSTORE_TYPE` | `PEM` (серт/ключ брокера — строками env, [04-security.md](04-security.md) §1) |
| `KAFKA_SSL_KEYSTORE_CERTIFICATE_CHAIN` | PEM-сертификат ноды (CN=`broker<k>`, SAN — правило [04-security.md](04-security.md) §1), подписан per-cluster CA |
| `KAFKA_SSL_KEYSTORE_KEY` | PEM PKCS#8 приватный ключ ноды |
| `KAFKA_SSL_TRUSTSTORE_TYPE` | `PEM` |
| `KAFKA_SSL_TRUSTSTORE_CERTIFICATES` | PEM per-cluster CA (`ca_pem`) — доверие всем слушателям SASL_SSL |
| `KAFKA_AUTHORIZER_CLASS_NAME` | `org.apache.kafka.metadata.authorizer.StandardAuthorizer` (KRaft-ACL, [04-security.md](04-security.md) §2) |
| `KAFKA_SUPER_USERS` | `User:admin;User:inter;User:ANONYMOUS` — принципалы SASL-имён (воркер/панель/CLI и inter-broker не ограничены ACL) + `User:ANONYMOUS`: principal PLAINTEXT-CONTROLLER-кворума, без super.user контроллер-регистрация получает CLUSTER_AUTHORIZATION_FAILED |
| `KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND` | `false` (deny по умолчанию; ACL роли app — converge [E](processes/e-config-converger.md), [04-security.md](04-security.md) §2) |
| `KAFKA_LISTENER_NAME_INTERNAL_PLAIN_SASL_JAAS_CONFIG` | JAAS: `username="inter" password="<inter-pwd>" user_inter="<inter-pwd>" user_admin=<pwd>[ user_admin2=<new>] user_app=<pwd>[ user_app2=<new>]`; inter-креды — inter-broker-клиент (§3) |
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

JAAS-формат (PLAIN): `org.apache.kafka.common.security.plain.PlainLoginModule
required user_<name>="<password>";` — по одному `user_<name>` на кред, через
пробел.

## 3. Inter-broker-креды

INTERNAL требует SASL и у брокера-КЛИЕНТА — `username`/`password` прямо в
INTERNAL-JAAS (без них фолловеры не подключаются, ISR проседает до лидера →
NOT_ENOUGH_REPLICAS при minISR≥2; вскрыто 3-брокерным e2e волны C).
Пользователь `inter` с детерминированным per-cluster паролем
(`InterBrokerPassword` NodeEnvBuilder: SHA-256 имени кластера → 32 симв
[A-Za-z0-9]); НЕ ротируется (ротация app/admin не должна ломать репликацию),
в etcd не хранится — listener доступен только внутри закрытой сети
`kfw-net-<C>` кластера. `inter` — super.user ([04-security.md](04-security.md) §2):
репликация/internal-топики не зависят от ACL.

## 4. Режимы Plain / Swarm

Порт режимов PgWorker ([14-pgworker.md](../14-pgworker.md) §2.2–2.3): plain —
`Hosts[{Name,Endpoint}]`, per-host клиент Engine API, контейнеры
`kfw-<C>-<b>`, restart-политика `unless-stopped`; swarm — `SwarmManager`,
нода = сервис `replicas=1`, placement constraint, `publish mode=host`.
Объекты для сверок — префикс `kfw-<C>-`. Сеть `kfw-net-<C>` — attachable,
per-cluster, создаётся воркером при provisioning кластера и удаляется при
полном демонтаже (последний брокер; пул subnet'ов docker-хоста конечен —
сиротские сети не копим); короткие алиасы `broker<N>` уникальны в своей
сети — несколько кластеров на одном docker-хосте изолированы
(бэквард-совместимость: существующие контейнеры в старой единой `kfw-net`
продолжают работать — кластер самодостаточен внутри своей сети).

Клиент Engine API — общий `Shared.Docker` (канон-суперсет семантики: start
304-идемпотентен, create при отсутствии образа — pull+retry, exec-ошибка
включает stdout, label контейнеров/сервисов — `kafkaworker`).

## 5. Где это используется

- Создание кластера — [A. ProvisioningProcess](processes/a-provisioning.md)
  (роли, env, порты, сеть, тома).
- Добавление брокера — [F. AddBrokerProcess](processes/f-add-broker.md).
- Rolling-перегенерация ресурсов — [J. NodeRegenerator](processes/j-node-regenerator.md).
- Пересоздание при молчании — [C. NodeSupervisor](processes/c-node-supervisor.md).
