# Graph Report - arch  (2026-08-19)

## Corpus Check
- Corpus is ~23,032 words - fits in a single context window. You may not need a graph.

## Summary
- 136 nodes · 243 edges · 16 communities (7 shown, 9 thin omitted)
- Extraction: 93% EXTRACTED · 7% INFERRED · 0% AMBIGUOUS · INFERRED: 18 edges (avg confidence: 0.84)
- Token cost: 669,624 input · 0 output

## Community Hubs (Navigation)
- Бакетные скрипты: общая библиотека
- Кластер HA: Patroni и etcd
- HAProxy, WAL и репликация
- Каркас документации
- Определение мастера и деплой
- Runbook переезда и каталог
- Скрипт health.sh
- Скрипт find-leader.sh
- Скрипт patronictl.sh
- Скрипт rebuild-node.sh
- HAProxy HA: keepalived VIP
- Скрипт cluster-state.sh
- Скрипт get-role.sh
- Скрипт switchover.sh
- Шаблон topology.env
- Бэкап pg_basebackup

## God Nodes (most connected - your core abstractions)
1. `cmd_move()` - 13 edges
2. `err()` - 12 edges
3. `create-bucket.sh script` - 12 edges
4. `README: отказоустойчивый PostgreSQL-кластер на 3 ноды (Docker)` - 12 edges
5. `cmd_finalize()` - 11 edges
6. `move-bucket.sh script` - 10 edges
7. `etcd: распределённое KV-хранилище (DCS)` - 10 edges
8. `08. Операционка: switchover, бэкапы, добавление ноды` - 9 edges
9. `cutover_flip()` - 8 edges
10. `cmd_rollback()` - 8 edges

## Surprising Connections (you probably didn't know these)
- `SPILO_CONFIGURATION: Patroni YAML внутри env` --semantically_similar_to--> `configs/patroni/patroni.yml: альтернативная конфигурация Patroni (без Spilo)`  [INFERRED] [semantically similar]
  05-deploy-postgres.md → configs/patroni/patroni.yml
- `Switchover: плановая смена лидера (patronictl switchover)` --semantically_similar_to--> `Switchover: плановая смена мастера без отказа`  [INFERRED] [semantically similar]
  08-operations.md → 01-architecture.md
- `P4: слот держит WAL — переполнение диска при initial copy` --semantically_similar_to--> `Проблема: диск переполнен WAL на ноде`  [INFERRED] [semantically similar]
  12-bucket-pitfalls.md → 09-troubleshooting.md
- `configs/postgres/docker-compose.yml: сервис postgres (Spilo, hostname=pg1)` --conceptually_related_to--> `Spilo: Docker-образ PostgreSQL + Patroni (Zalando)`  [INFERRED]
  configs/postgres/docker-compose.yml → 01-architecture.md
- `etcd-контрол-плейн: ключи /buckets/*, /shards/*` --conceptually_related_to--> `etcd: распределённое KV-хранилище (DCS)`  [INFERRED]
  12-bucket-pitfalls.md → 01-architecture.md

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **HA-стек кластера: PostgreSQL + Patroni + etcd + HAProxy (в образе Spilo)** — arch_01_architecture_postgresql, arch_01_architecture_patroni, arch_01_architecture_etcd, arch_01_architecture_haproxy, arch_01_architecture_spilo [EXTRACTED 1.00]
- **Способы определения текущего лидера кластера** — arch_07_identify_master_etcd_leader_key, arch_07_identify_master_patroni_cluster_endpoint, arch_07_identify_master_primary_endpoint, arch_07_identify_master_patronictl_list, arch_07_identify_master_pg_is_in_recovery, arch_07_identify_master_haproxy_stats, arch_07_identify_master_findleader [EXTRACTED 1.00]
- **Runbook онлайн-переезда бакета между шардами** — arch_11_bucket_sharding_bucket_catalog, arch_11_bucket_sharding_logical_replication, arch_11_bucket_sharding_cutover, arch_11_bucket_sharding_sequences_sync, arch_11_bucket_sharding_move_rollback, arch_11_bucket_sharding_createbucket, arch_11_bucket_sharding_movebucket [EXTRACTED 1.00]

## Communities (16 total, 9 thin omitted)

### Community 0 - "Бакетные скрипты: общая библиотека"
Cohesion: 0.17
Nodes (31): catalog_check_table(), catalog_row(), catalog_sql(), err(), info(), mover_conninfo(), pub_exists(), require_bins() (+23 more)

### Community 1 - "Кластер HA: Patroni и etcd"
Cohesion: 0.12
Nodes (22): etcd: распределённое KV-хранилище (DCS), Failover: автоматическое переключение при отказе лидера, Patroni: менеджер репликации и failover, Spilo: Docker-образ PostgreSQL + Patroni (Zalando), Switchover: плановая смена мастера без отказа, synchronous_mode: синхронная репликация Patroni, 3-нодовая топология: pg1/pg2/pg3 + pg-lb, chrony/NTP: синхронизация времени (+14 more)

### Community 2 - "HAProxy, WAL и репликация"
Cohesion: 0.12
Nodes (20): HAProxy: единая точка входа, PostgreSQL: 3 экземпляра, 1 primary + 2 replicas, Защита от split-brain через кворум DCS, Streaming replication (WAL over TCP), WAL (Write-Ahead Log), configs/haproxy/haproxy.cfg, pg_is_in_recovery(): роль ноды в текущем соединении, Проблема: split-brain (два лидера) (+12 more)

### Community 3 - "Каркас документации"
Cohesion: 0.41
Nodes (14): 01. Архитектура кластера, 02. Топология стенда, 03. Подготовка хостов, 04. Деплой кластера etcd, 05. Деплой PostgreSQL + Patroni (Spilo), 06. Деплой HAProxy (точка входа), 07. Как узнать, кто сейчас мастер, 08. Операционка: switchover, бэкапы, добавление ноды (+6 more)

### Community 4 - "Определение мастера и деплой"
Cohesion: 0.15
Nodes (14): Patroni REST API (:8008): /, /primary, /replica, /read-only, /read-write, Docker Engine + Compose plugin v2, INITIAL_CLUSTER_STATE: new → existing после первого старта, Static bootstrap etcd (--initial-cluster), configs/postgres/pg.env: переменные окружения Spilo, Порт 5433 → read-only (лидер + реплики), Порт 5432 → мастер (httpchk GET /primary), Ключ /service/<scope>/leader в etcd (источник правды) (+6 more)

### Community 5 - "Runbook переезда и каталог"
Cohesion: 0.31
Nodes (9): Каталог бакетов: таблица buckets (bucket_id, shard_id, state), Роутер бакетов (в приложении): bucket_id → DSN шарда, scripts/create-bucket.sh: создать бакет на шарде, Cutover: FROZEN → лаг 0 → flip владельца в каталоге, scripts/move-bucket.sh: move/status/rollback/finalize, Синхронизация sequences на cutover (setval), P10: расходимость кэшей роутеров (корневая причина P1), P1: соединения-«призраки» после flip (потеря записи) (+1 more)

### Community 6 - "Скрипт health.sh"
Cohesion: 0.83
Nodes (3): ok(), health.sh script, warn()

## Knowledge Gaps
- **25 isolated node(s):** `buckets-common.sh script`, `cluster-state.sh script`, `find-leader.sh script`, `get-role.sh script`, `switchover.sh script` (+20 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **9 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `HAProxy: единая точка входа` connect `HAProxy, WAL и репликация` to `Кластер HA: Patroni и etcd`, `Определение мастера и деплой`, `Runbook переезда и каталог`?**
  _High betweenness centrality (0.119) - this node is a cross-community bridge._
- **Why does `etcd: распределённое KV-хранилище (DCS)` connect `Кластер HA: Patroni и etcd` to `Определение мастера и деплой`?**
  _High betweenness centrality (0.083) - this node is a cross-community bridge._
- **Why does `3-нодовая топология: pg1/pg2/pg3 + pg-lb` connect `Кластер HA: Patroni и etcd` to `HAProxy, WAL и репликация`?**
  _High betweenness centrality (0.068) - this node is a cross-community bridge._
- **What connects `buckets-common.sh script`, `cluster-state.sh script`, `find-leader.sh script` to the rest of the system?**
  _25 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Кластер HA: Patroni и etcd` be split into smaller, more focused modules?**
  _Cohesion score 0.12121212121212122 - nodes in this community are weakly interconnected._
- **Should `HAProxy, WAL и репликация` be split into smaller, more focused modules?**
  _Cohesion score 0.11578947368421053 - nodes in this community are weakly interconnected._