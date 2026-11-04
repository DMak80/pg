# 00. Сквозные технологии и общие технические правила

Уровень 1. Технологии и правила, действующие во всех компонентах и не
принадлежащие одному из них. Полное описание — по ссылкам.

## 1. Общие технические правила

### 1.1. Принципы синхронизации распределённого состояния

Единый контур истины и конвергенция динамического DCS, инварианты внешних
систем, ключи без вырожденных значений, клэймы и фиксированный порядок
мутаций, журналы фаз, «транспортный флап ≠ смерть», свежесть инструментов,
порядок расследования. Каждый принцип описан как правило → почему критично →
кейс → чем проверяется; реестр ошибок использования пополняется.

→ [17-synchronization-principles.md](17-synchronization-principles.md)

### 1.2. Межкомпонентные коммуникации и границы оркестрации

Контейнеры общаются только стандартными сетевыми протоколами (HTTP(S), SQL и
репликация, Kafka, RESP, S3, etcd); stdout/stderr контейнера — не канал связи.
Управление контейнерами — только запуск/остановка/удаление и проверка факта
(list/inspect); чтение логов, exec, stats/events — вне продукта.

→ [25-component-communications.md](25-component-communications.md)

## 2. Технологии

### 2.1. etcd — единое хранилище состояния и координации

Один кластер на установку; доступ по HTTP JSON gateway `/v3/*`; TTL/lease,
клэймы и txn. В нём — декларации доменов, координация воркеров, scopes
Patroni и дискавери API. Адрес задаётся явно при старте; «вторых» etcd нет.

→ [04-deploy-etcd.md](04-deploy-etcd.md) — контур и развёртывание,
[17-synchronization-principles.md](17-synchronization-principles.md) — правила
работы с общим состоянием

### 2.2. Docker Engine API и контейнерная оркестрация

Plain docker и docker swarm; транспорт к Engine API — `tcp://` + mTLS (канон),
`ssh://`-туннель, `unix://` только для стенда. Воркеры — несколько инстансов с
lease-координацией; операции идемпотентны.

→ [25-component-communications.md](25-component-communications.md) §2,
[pgworker/01-placement.md](pgworker/01-placement.md) §3

### 2.3. TLS, mTLS и PKI

Внутренние каналы — mTLS: API воркеров (per-install API-CA), канал
PgWorker ↔ PgNode (per-install REST-CA), per-cluster CA у Kafka; у клиентской
грани PG — TLS (`sslmode=require`). Сетевая модель описывает зоны,
firewall-матрицу и аутентификацию.

→ [13-network-security.md](13-network-security.md)

### 2.4. Prometheus-метрики и наблюдаемость

`System.Diagnostics.Metrics` + OpenTelemetry Prometheus exporter; `/metrics` на
Kestrel-порту рядом с `/healthz`; единый словарь имён, типов и лейблов; стек
Prometheus + Grafana + Alertmanager.

→ [18-metrics.md](18-metrics.md)

### 2.5. Объектное хранилище S3

S3-совместимое объектное хранилище: домен S3-кластеров — пул ownS3-инстансов
с mesh-репликацией; бэкапы шардов кладутся в bucket пула (layout
per-cluster/per-shard, один bucket на установку).

→ [s3-clusters/README.md](s3-clusters/README.md),
[19-backups.md](19-backups.md) §5

### 2.6. PostgreSQL: физическая и логическая репликация

Streaming replication (WAL) — основа HA и приёмника WAL; логическая репликация
со слотами — основа онлайн-переезда бакетов (failover slots, синхронизация
sequence).

→ [11-bucket-sharding.md](11-bucket-sharding.md) §4/§7/§10,
[19-backups.md](19-backups.md) §3
