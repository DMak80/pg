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

→ [synchronization/README.md](synchronization/README.md)

### 1.2. Межкомпонентные коммуникации и границы оркестрации

Контейнеры общаются только стандартными сетевыми протоколами (HTTP(S), SQL и
репликация, Kafka, RESP, S3, etcd); stdout/stderr контейнера — не канал связи.
Управление контейнерами — только запуск/остановка/удаление и проверка факта
(list/inspect); чтение логов, exec, stats/events — вне продукта.

→ [communications/01-rules.md](communications/01-rules.md)

## 2. Технологии

### 2.1. etcd — единое хранилище состояния и координации

Один кластер на установку; доступ по HTTP JSON gateway `/v3/*`; TTL/lease,
клэймы и txn. В нём — декларации доменов, координация воркеров, scopes
Patroni и дискавери API. Адрес задаётся явно при старте; «вторых» etcd нет.

→ [pg-clusters/04-etcd-dcs.md](pg-clusters/04-etcd-dcs.md) — контур и развёртывание,
[synchronization/README.md](synchronization/README.md) — правила
работы с общим состоянием

### 2.2. Docker Engine API и контейнерная оркестрация

Plain docker и docker swarm; транспорт к Engine API — `tcp://` + mTLS (канон),
`ssh://`-туннель, `unix://` только для стенда. Воркеры — несколько инстансов с
lease-координацией; операции идемпотентны.

→ [communications/01-rules.md](communications/01-rules.md) §2,
[pgworker/01-placement.md](pgworker/01-placement.md) §3

### 2.3. TLS, mTLS и PKI

Внутренние каналы — mTLS: API воркеров (per-install API-CA), канал
PgWorker ↔ PgNode (per-install REST-CA), per-cluster CA у Kafka; у клиентской
грани PG — TLS (`sslmode=require`). Сетевая модель описывает зоны,
firewall-матрицу и аутентификацию.

→ [pg-clusters/13-network-security.md](pg-clusters/13-network-security.md)

### 2.4. Prometheus-метрики и наблюдаемость

`System.Diagnostics.Metrics` + OpenTelemetry Prometheus exporter; `/metrics` на
Kestrel-порту рядом с `/healthz`; единый словарь имён, типов и лейблов; стек
Prometheus + Grafana + Alertmanager.

→ [metrics/README.md](metrics/README.md)

### 2.5. Объектное хранилище S3

S3-совместимое объектное хранилище: домен S3-кластеров — пул ownS3-инстансов
с mesh-репликацией; бэкапы шардов кладутся в bucket пула (layout
per-cluster/per-shard, один bucket на установку).

→ [s3-clusters/README.md](s3-clusters/README.md),
[backups/06-s3-layout.md](backups/06-s3-layout.md)

### 2.6. PostgreSQL: физическая и логическая репликация

Streaming replication (WAL) — основа HA и приёмника WAL; логическая репликация
со слотами — основа онлайн-переезда бакетов (failover slots, синхронизация
sequence).

→ [pg-clusters/05-postgres-patroni.md](pg-clusters/05-postgres-patroni.md),
[pg-clusters/11-buckets.md](pg-clusters/11-buckets.md) §4/§7,
[backups/02-wal-archive.md](backups/02-wal-archive.md)

### 2.7. E2E-тестирование

Сквозные правила docker-E2E для всех E2E-проектов монорепо: единица
окружения — тестовый класс с полностью своим контуром (сеть, etcd, S3,
SUT-воркер), динамические хост-порты, чистка строго по своим объектам,
обязательный teardown и ассерт чистоты; телеметрия прогона снимается до
удаления, падение разбирается по логам без перезапуска, статика собирается
отдельной фазой.

→ [testing/README.md](testing/README.md) — канон;
[testing/01-e2e-launch.md](testing/01-e2e-launch.md) — запуск и телеметрия;
[testing/02-e2e-isolation.md](testing/02-e2e-isolation.md) — изоляция и
чек-лист; команды — [operations/04-e2e-runs.md](operations/04-e2e-runs.md)
