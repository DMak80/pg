# PgNode: HTTP API ноды

Уровень 3. Приём команд PgWorker и публикация статусов операций. Контракт
операций домена — [backups/README.md](../backups/README.md); процессы —
[processes/](processes/); доставка секретов —
[pgworker/04-security.md](../pgworker/04-security.md) §3–§4.

## 1. Каналы

- **`:8080`** — `/metrics` (Prometheus) + HTTP API операций
  (`/api/backup/full*`, `/api/ops/*`, `/api/node/*`). Канал
  `alias:8080` по имени ноды в общей сети воркера и нод; наружу хоста
  не публикуется. Plain HTTP внутри сети.
- **`:8081`** — mTLS-сервер загрузочного агента: команды
  `/api/bootstrap*` (доставка per-install паролей, применение материала).
  Верификация — [01-placement.md](01-placement.md) §2; по plaintext `:8080`
  эти маршруты — `404`.

## 2. API операций

| Метод+путь | Назначение | Статус/коды |
|---|---|---|
| `POST /api/backup/full` | запуск полного, тело `{"id"}` (id детерминирован воркером) | 202; 400 — id вне allowlist; 409 — операция уже идёт; 503 — стоп-гейт (`STOPPED`, `/pgworker/backups-disabled`, `Enabled=false`) |
| `GET /api/backup/full` | снимок полного: фазы `basebackup|uploading`, `wal_start_segment`, `size_bytes`, прогресс (`bytes_done`/`parts_done`), `error` | 200; 404 — операции нет/vanished |
| `POST /api/backup/full/cancel` | отмена полного (гасит BASE_BACKUP-соединение и multipart-сессию) | 202/200 (best-effort) |
| `POST /api/ops/verify` | запуск verify полного (S3-контекст read-списка + `sha256`? ожидаемый в теле) | 202; 400 — id вне allowlist; 409 — чужой ops-операции; 503 — гейт |
| `POST /api/ops/restore` | запуск restore (тело: `backup_id`, `target_time`?, `recovery_timeout_sec`, `chain_life`?, `sha256`?, S3-контекст) ИЛИ drill (поле `id`) | 202; 400 — id вне allowlist; 409 — чужой ops-операции; 503 — гейт |
| `GET /api/ops/restore` | снимок restore/drill: `id` операции реестра, `kind`, фазы `downloading|recovering`, `restored_to_lsn`?, `system_id`?, `started_unix`?, `error`? | 200; 404 — операции нет |
| `GET /api/ops/verify` | снимок verify: `id` операции реестра, `kind`, `downloading|verifying`, `started_unix` | 200; 404 — операции нет |
| `POST /api/ops/cancel` | отмена идущей ops-операции (внешний канал мимо воркера) | операция `state=cancelled` (НЕ failed) |
| `POST /api/node/pgdump` | `pg_dump --schema-only` схемы (DDL-перенос переезда) | снимок операции/результат |
| `POST /api/node/pgdata` | проба данных ноды: `PG_VERSION` файловой системой контейнера | Present/Absent/Unknown |
| `POST /api/node/hba-ensure` | обеспечение строки pg_hba для `backup_exec` | применение/ожидание |

**Граница операций — allowlist id.** Все POST операций принимают
идентификатор (`id` полного, `backup_id`, drill-`id`) только по allowlist
`^[A-Za-z0-9._-]{1,64}$`; иное — 400 до любой операции (id попадает в
SQL-команду `BASE_BACKUP (LABEL …)`, S3-ключи и логи — произвольный текст
там недопустим). Символ `'` в id исключён allowlist'ом; LABEL
экранируется независимо (защита от изменения соглашения рендера).

## 3. Общий ops-реестр

Ops-операции (verify/restore/drill) делят ОДИН реестр — одна ops-операция
на процесс; полная операция (full) в реестр не входит и ops-операцию не
блокирует. Снимок операции IMMUTABLE в памяти: `id`, `kind`, фаза,
`started_unix`, результат/`error`; отмена — `state=cancelled` (не failed). `POST` при
занятом реестре — 409; снимок 409 НЕ различает свою/чужую операцию —
воркер откладывает запуск (transient) до освобождения реестра. Принять 409
за «свою живую» нельзя: verify затирает снимки общего реестра, и воркер
ушёл бы в REJOINING от чужого succeeded без `system_id`.

**GET-снимок несёт `id` и `kind` операции реестра.** Реестр один на
процесс — снимок в нём может оказаться чужим (прошлая/параллельная
операция); GET-ответ обязан позволять сверку «своя/чужая»: супервизоры
принимают исход ТОЛЬКО по снимку своей операции — с собственным `id` И
собственным `kind`. Снимок с чужим `id`/`kind`, а также снимок БЕЗ `kind`
(контракт старого приёмника) — не доказательство (вердиктов по нему не
пишут, свои перезапуски продолжают): воркер и приёмник поставляются одним
контуром и обновляются одновременно, поэтому снимок без вида операции
трактуется как чужой — ложный вердикт невозможен, в худшем случае запуск
откладывается до совпадения версий.

Таблица значений `kind` (заполняет приёмник при `TryBegin` и каждом
обновлении снимка):

| `kind` | Операция | Источник `id` снимка |
|---|---|---|
| `verify` | потоковая проверка полного (`POST /api/ops/verify`) | `backup_id` команды |
| `restore` | восстановление в PGDATA ноды (`POST /api/ops/restore` без `id`) | `backup_id` команды |
| `drill` | drill-восстановление в scratch (`POST /api/ops/restore` с `id`) | `id` команды |

Полный бэкап своего реестра вида операции не имеет. Отмена (cancel API)
применяется к running-операции; остановка приёмника снимок не
перезаписывает ([06-restore-drill.md](06-restore-drill.md) §2 п.5.4).
Терминальный `succeeded` restore/drill-операции несёт `restored_to_lsn`
обязательно (успех доказан выходом из recovery): `succeeded` без LSN —
нарушение контракта операции, супервизор дрилла трактует его vanished-семантикой
([../backups/processes/g-drill.md](../backups/processes/g-drill.md) §1).

Повторные команды идемпотентны по смыслу: `POST /api/ops/restore` при уже
живой операции — 409; повторный пин применения материала — no-op по
материальному гейту ([processes/f-node-agent.md](processes/f-node-agent.md)).
Резюмов операций нет: vanished (рестарт процесса) → GET 404 → воркер
решает по своим бюджетам (переснятие/повтор/FAILED).

## 4. Доставка секретов и применение

`:8081` (mTLS): команда доставки при `not_initialized` (тело:
`{su_password, standby_password, is_tool_start?}`), команда применения
материала на `ready` (без тела). Агент проверяет аргументы, атомарно
переводит `not_initialized → initializing` txn-сравнением; на `ready`
перечитывает bootstrap-ключ и применяет точечно по материалу. Полный
автомат — [processes/f-node-agent.md](processes/f-node-agent.md); семантика
применения — [pgworker/04-security.md](../pgworker/04-security.md) §3.

## 5. Статусы для воркера

Помимо GET-снимков операций, агент публикует в etcd:
`/pgworker/backups/<C>/<X>/agent/<N>` (lease TTL 15 с; факт сессии
приёмника) и `/pgworker/nodes/<C>/<X>/<N>/status` (lease TTL 15 с;
статусная машина агента). Воркер читает их тиками; панель — через снапшот
([backups/05-etcd-contract.md](../backups/05-etcd-contract.md)).
