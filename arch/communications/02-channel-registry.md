# Коммуникации: реестр каналов

Уровень 3. Реестр каналов компонентов: способ связи и соответствие правилам
[01-rules.md](01-rules.md) §§1–2. Родитель — [README.md](README.md); соседние
документы — [01-rules.md](01-rules.md), [03-deviations.md](03-deviations.md).

Способ указан как в коде; «канон» — соответствует §§1–2, «отклонение» —
исторический механизм, переход по roadmap ([03-deviations.md](03-deviations.md)).

| # | Компонент 1 → Компонент 2 | Способ | Статус |
|---|---|---|---|
| 1 | AdminPanel → PgWorker | HTTP API воркера `:8080` (`/api/…`), дискавери — etcd-ключ `/pgworker/api/<id>` | канон |
| 2 | AdminPanel → KafkaWorker/S3Worker/ValkeyWorker | HTTP API воркера | канон |
| 3 | AdminPanel, все воркеры, Patroni-ноды, агенты PgNode (внутри PG-нод) → etcd | etcd-протокол (единый кластер, [synchronization/01-truth-and-convergence.md](../synchronization/01-truth-and-convergence.md) §S1) | канон |
| 4 | AdminPanel → Patroni | HTTP REST `:8008` (`GET /cluster`, live-пробы) | канон |
| 5 | AdminPanel → PostgreSQL | SQL (Npgsql) | канон |
| 6 | PgWorker → PostgreSQL-ноды | SQL (Npgsql) | канон |
| 7 | PgWorker → Patroni | HTTP REST `:8008` (PATCH sync-mode, `GET /cluster`) | канон |
| 8 | Patroni → etcd | etcd DCS | канон |
| 9 | Patroni → PostgreSQL | streaming-репликация | канон |
| 11 | PgNode → PostgreSQL | протокол репликации (`START_REPLICATION PHYSICAL`, слоты; источник — `127.0.0.1:5432` своей ноды, агент живёт в её контейнере, [backups/02-wal-archive.md](../backups/02-wal-archive.md)) | канон |
| 12 | PgNode → S3-пул | S3 API | канон |
| 14 | KafkaWorker → Kafka | Kafka-протокол | канон |
| 15 | KafkaWorker-джобы → KafkaWorker | etcd-журнал (`/kafkaworker/work/<C>`, WorkJournal) | канон |
| 16 | S3Worker → ownS3-инстансы | S3 API (mesh, бакеты, deny-list) | канон |
| 17 | ValkeyWorker → Valkey-ноды | RESP | канон |
| 18 | Prometheus → панель, воркеры, шлюзы, агенты PgNode (внутри PG-нод) | HTTP scrape `/metrics` (воркеры — Kestrel `:8080`; PgNode — `:8080` внутри контейнера ноды, [metrics/02-dictionary.md](../metrics/02-dictionary.md) §8) | канон |
| 19 | PgWorker → docker (ноды, джобы) | create/start/stop/kill/remove, сети, тома; list/inspect (состояние, exit-код, порты, env/cmd, лейблы, StartedAt) | канон (§2) |
| 20 | S3Worker → docker/swarm (ownS3) | сервисы/placement, тома; list/inspect | канон (§2) |
| 21 | PgNode → PgWorker | etcd-ключ агента `/pgworker/backups/<C>/<X>/agent/<N>` (lease-живость, фазы, `tli_jump`, `pool_exhausted` — [backups/02-wal-archive.md](../backups/02-wal-archive.md), [backups/05-etcd-contract.md](../backups/05-etcd-contract.md)); воркер читает, агент пишет только свой ключ | канон |
| 22 | PgWorker → PgNode (операции ноды) | HTTP API агента `alias:8080` по имени ноды в общей сети воркера и нод, без публикации на хост (docker и swarm). ОДИН процесс агента PgNode исполняет ВСЕ операции ноды: полный (`/api/backup/full*`) и verify/restore/drill (`/api/ops/*`) — [backups/README.md](../backups/README.md); обеспечение pg_hba (`/api/node/hba-ensure`; каналы операции — [backups/08-secrets.md](../backups/08-secrets.md)), pg_dump схемы (`/api/node/pgdump`, DDL-переезд [pgworker/06-bucket-moves.md](../pgworker/06-bucket-moves.md)), проверка данных ноды (`/api/node/pgdata`, [pgworker/08-operability.md](../pgworker/08-operability.md)) — `/api/node/*`; статусы — GET того же API | канон |
| 23 | PgWorker ↔ PgNode (хендшейк старта и канал доставки секретов) | статусы живут в etcd: агент пишет `/pgworker/nodes/<C>/<X>/<N>/status` (lease; машина `not_initialized → initializing → ready \| start_failed`, `applied_revision`), воркер читает-мониторит (poll-тик) — [pgworker/04-security.md](../pgworker/04-security.md) §4. Команды — mTLS `:8081` внутри docker-сети кластера, на хост не публикуется ([pg-clusters/13-network-security.md](../pg-clusters/13-network-security.md) §2): серверный серт ноды per-install REST-CA (SAN), клиентский серт воркера из того же CA; ГРАНИЦА ПОРТА: маршруты `/api/bootstrap*` обслуживаются ТОЛЬКО листенером `:8081` (единый роутер Kestrel не является границей — обращения по plaintext `:8080` → `404`); клиентская верификация агентом — цепочка к CA + EKU ClientAuth + CN=pgworker (серверные серты нод того же CA клиентом канала НЕ являются, [pg-clusters/13-network-security.md](../pg-clusters/13-network-security.md) §2); инициатор — всегда воркер: по `not_initialized` — команда доставки (тело: per-install пароли PG + `is_tool_start`: false — агент стартует компоненты с надзором старта ≤3 попыток/компонент; true — агент только готовит параметры запуска (готовность ЭТОГО пути = параметры: `ready` по факту рендера, select 1 не выполняется), стартуют сами tool-команды; атомарный txn-переход в `initializing`, повтор при ином состоянии — `ok` без действий), по расхождению `applied_revision` на `ready` — команда применения (ротации, без тела). HTTP-коды канала — ЗАКРЫТЫЙ набор: доставка `200` (принято/`ok`-пинок) \| `400` (аргументы неполны/невалидны — статус не меняется) \| `503` (etcd недоступен агенту); применение `200` (принято, применение/публикация статуса — фон) \| `409` (нода не `ready`) \| `503`. Реакция на `start_failed` — пересоздание ноды с новым диском ([pgworker/processes/c-node-supervisor.md](../pgworker/processes/c-node-supervisor.md)) | канон |
| 24 | KafkaWorker → Kafka (reassignment партиций) | Kafka-протокол: Describe — AdminClient, подача — AlterReassignments/KIP-455 (миниклиент, [kafkaworker/processes/i-partition-reassigner.md](../kafkaworker/processes/i-partition-reassigner.md)) | канон |
