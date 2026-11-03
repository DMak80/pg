# Kafka-кластеры: ключи кластера `/kafka/clusters/<C>/`

Уровень 3. Полный контракт ключей контроль-плейна домена, кто и когда
пишет, канонические примеры значений. Семантика топиков —
[03-topics.md](03-topics.md); координация воркера —
[04-coordination.md](04-coordination.md).

## 1. Ключи

| Ключ | Формат значения | Пишет | Примечание |
|---|---|---|---|
| `config` | JSON `{"brokers":B,"replication_factor":R,"min_insync_replicas":M,"default_partitions":P,"default_retention_ms":X,"created_unix":T,"state"?:"NOT_INITIALIZED"\|"TO_REMOVE"}` | панель (создание, TO_REMOVE, конфиг-мутации), воркер (снимает `state` после инициализации, txn по `mod_revision`) | `state` — только у невыполненных заявок: отсутствие = Active (семантика pg 02 §2.1); поля 2–5 — mutable-конфиги кластера (converge воркером, без рестартов) |
| `brokers/broker<k>/state` | строка `NOT_INITIALIZED`\|`PROVISIONING`\|`RUNNING`\|`UNREACHABLE`\|`REMOVING`\|`TO_REMOVE` | `NOT_INITIALIZED` и `TO_REMOVE` — **только панель** (заявка создания/маркер демонтажа, one-way); остальные — воркер | `TO_REMOVE` — маркер демонтажа (аналог pg §9.6): до разбора виден в UI с бейджем |
| `brokers/broker<k>/resources` | JSON `{"cpu":"2","mem":"4Gi","disk":"40Gi"}` | панель | заявка ресурсов ноды (лимиты контейнера; форматы как pg §9.3); изменение существующих — мутация №15 ([adminpanel/02-etcd-contract.md](../adminpanel/02-etcd-contract.md) §10.2, через API воркера): лимиты cpu/mem сводит к декларации NodeRegenerator ([kafkaworker/03-broker-scaling.md](../kafkaworker/03-broker-scaling.md) §3) — rolling-пересоздание контейнера; `disk` — инфо-поле, действий не вызывает |
| `brokers/broker<k>/role` | `"controller"\|"broker"` | воркер (план provisioning) | `controller` — combined-нода (участник KRaft-кворума); `broker` — broker-only; фиксируется при создании ноды навсегда |
| `endpoints` | строка `"h1:p1,h2:p2,..."` | воркер (после подъёма; при add/remove брокера — RMW) | клиентские bootstrap-адреса (advertised host + клиентский порт из portalloc) — **точка дискавери клиентов** |
| `app_user` | `"app"` | воркер (ensure, txn put-if-absent) | per-cluster SASL-пользователь **приложений** (ACL-роль app, [kafkaworker/04-security.md](../kafkaworker/04-security.md) §2: READ/WRITE/DESCRIBE топиков + группы) |
| `app_password` | 32 симв `[A-Za-z0-9]` | воркер (ensure + ротация) | per-cluster SASL-пароль приложений; в UI/API панели не отдаётся (как dsn-пароль pg) |
| `admin_user` | `"admin"` | воркер (ensure, txn put-if-absent) | per-cluster SASL-пользователь **администратора** (воркер AdminClient/CLI, пробы панели; super.user, [kafkaworker/04-security.md](../kafkaworker/04-security.md) §2) |
| `admin_password` | 32 симв `[A-Za-z0-9]` | воркер (ensure + ротация) | per-cluster SASL-пароль администратора; панель читает для проб, в UI/API не отдаёт |
| `ca_pem` | PEM-сертификат self-signed per-cluster CA; **в окне ротации — bundle: конкатенация PEM OLD+NEW** (фаза D CaRotator, [kafkaworker/04-security.md](../kafkaworker/04-security.md) §4), после коммита — только NEW | воркер (provisioning, txn put-if-absent; ротация — CaRotator) | публичный CA-серт кластера (CN=`kfw-<C>-ca-<отпечаток>` — subject уникален на генерацию, 10 лет; [kafkaworker/04-security.md](../kafkaworker/04-security.md) §1): доверие клиентов TLS (SASL_SSL INTERNAL/CLIENT, [kafkaworker/01-placement.md](../kafkaworker/01-placement.md) §1) — **точка дискавери приложений и панели** ([05-client-discovery.md](05-client-discovery.md) §1); PEM-конкатенация стандартна для truststore (`ssl.ca.pem`/`ssl.ca.location`) — читатели используют значение как есть |
| `ca_key` | PEM PKCS#8 приватный ключ CA; при ротации перезаписывается ключом NEW CA (фаза C, [kafkaworker/04-security.md](../kafkaworker/04-security.md) §4) — OLD-ключ уничтожается перезаписью | воркер (provisioning, txn put-if-absent; ротация — CaRotator) | секрет кластера: подпись брокерных сертификатов (provisioning/add-broker/rebuild/ротация); панель НЕ читает, в UI/API не отдаётся |
| `ca_next_key` | PEM PKCS#8 приватный ключ НОВОЙ CA; staging — живут только в окне ротации (фазы P→C, [kafkaworker/04-security.md](../kafkaworker/04-security.md) §4) | воркер (CaRotator, фаза P, txn put-if-absent; фаза C — del) | подпись брокерных сертов в rolling-фазе R до коммита; отсутствует вне ротации — ensure не создаёт |
| `ca_next_pem` | PEM-сертификат НОВОЙ CA; staging, аналогично `ca_next_key` | воркер (CaRotator) | источник bundle для `ca_pem` (фаза D) и подписи в фазе R; в UI/API панели не отдаётся |
| `topics/<T>` | JSON — гибрид автосинка и конфиг-заявки, [03-topics.md](03-topics.md) §1 | воркер (автосинк факта), панель (только `desired`-часть, RMW) | реестр топиков для дискавери |
| `topics/<T>/desired.create` | JSON `{"partitions":P,"replication_factor":R,"configs"?:{...},"requested_unix":T,"requested_by":"u"}` | панель (клэйм-txn `version==0`), воркер — только del | заявка создания; `configs` — только управляемые (`retention.ms`, `min.insync.replicas`); отсутствие = брокерные дефолты |
| `topics/<T>/desired.delete` | JSON `{"requested_unix":T,"requested_by":"u"}` | панель (клэйм-txn `version==0`), воркер — только del | заявка удаления (деструктивная) |

Неизвестные ключи внутри `/kafka/` — не ошибка: лог + счётчик `unknownKeys`
в снапшоте читателя (как pg 02 §2.1; система развивается, парсеры не падают,
[06-fault-tolerance.md](06-fault-tolerance.md)).

## 2. Канонические примеры значений (критерий приёмки парсеров)

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
(advertised-хост CLIENT-listener по правилу
[kafkaworker/01-placement.md](../kafkaworker/01-placement.md) §1;
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

`topics/audit/desired.create` (заявка создания с начальными конфигами,
[03-topics.md](03-topics.md) §2):

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

`admin_user`: `"admin"`; `admin_password`: 32 симв `[A-Za-z0-9]` (генератор
как app_password). `ca_pem`/`ca_key` — PEM одной строкой с `\n`-переносами:
`-----BEGIN CERTIFICATE-----\n…\n-----END CERTIFICATE-----` (ca_key —
`BEGIN PRIVATE KEY`, PKCS#8).
