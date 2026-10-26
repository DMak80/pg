# t26-backup-jobs-in-receiver — перенос backup-джобов внутрь WalReceiver

Пункт роадмапа: `t26-backup-jobs-in-receiver` (`arch/roadmap/backup.md`, ← t25 —
слита). Каноны: `arch/19-backups.md`, `arch/25-component-communications.md`,
`arch/14-pgworker.md`, `arch/18-metrics.md`. Dev-flow: Фаза 1 (spec) — этот
документ; arch-first — правки канонов идут ДО кода (Фаза 2/plan, Фаза 3/код).

## 1. Цель

Backup-операции (полный бэкап, verify, drill, restore) исполняет **WalReceiver**
— .NET-процесс внутри контейнера PG-ноды, уже стримящий WAL в S3. PgWorker
перестаёт запускать ephemeral docker-джобы бэкапов и читать docker-логи:
управление — командами ВНЕШНЕГО HTTP API приёмника (у каждой операции своя
команда со своими параметрами), наблюдение — опросом статуса по тому же API.

Архитектурное разделение ролей (канон после t26):

- **WalReceiver работает с данными**: pg_basebackup, скачивание/загрузка S3,
  локальный recovery postgres, pg_verifybackup — всё локально в своём
  контейнере (.NET-механика, child-процессы PG-утилит через `Process.Start`).
- **PgWorker управляет**: выбирает источник/ноду, создаёт tool-контейнеры,
  подаёт команды API, держит бюджеты и etcd-статусы, супервизит, чистит.

Упраздняется:

- ephemeral-контейнеры `pgw-backup-full-*` и образ `pgworker-backup`
  (`docker/PgWorker.Backup.Dockerfile`, `docker/backup/entrypoint.sh`);
- inline-bash restore/drill/verify-джобы (`RestoreJobCommand`,
  `DrillJobCommand`, `VerifyJobCommand`) и stdout-протокол маркеров
  (`BackupJobLog`, `RestoreJobLog`, `VerifyLog`, `DrillJobLog`);
- чтение docker-логов (`GetContainerLogsAsync`) из продуктовых путей —
  закрывается отклонение arch/25 №22 ЦЕЛИКОМ; пункты роадмапа `t23` и `t24`
  удаляются мерж-коммитом t26 (см. §6 Р6/Р7).

Verify/drill/restore исполняются в **tool-контейнере**: контейнер образа
`pgworker-node` в tool-режиме (env отключает Patroni/supervisord-программы;
WalReceiver — инструментальный режим без identity: не стримит, etcd-статусов
агента не пишет, ждёт команду API). При старте tool-контейнера — никаких
команд, только старт контейнера; после старта API — единственный канал команд
и статуса (arch/25 §1/§2).

Инварианты, НЕ меняющиеся: etcd-контракт `/pgworker/backups/*` (пишет только
воркер + статус-ключи приёмников стрима), планировщик полных (инвариант
одного активного, бэкофф, WAL_CHAIN_ERROR/gap-правила), контроль цепочки, ретенция,
супервизор/сироты, RPO-инвариант подтверждения-по-доставке, стоп-семантика,
DR-механика restore (source_pool, демонтаж/REJOINING/initialize-щит).

## 2. Принципы

1. **Arch-first**: правки `arch/19` (§2, §3.5, §3.6, §5, §6, §7, §8, §9, §10),
   `arch/25` (реестр каналов), `arch/14` (§2.1/§2.4 — API приёмника
   `alias:8080` в общей сети воркера и нод, без публикации на хост),
   `arch/18` (§2.8 — API на том же :8080) — первой фазой, до кода.
2. **Только стандартные протоколы** (arch/25 §1): PgWorker ↔ WalReceiver —
   HTTP; WalReceiver → S3 — S3 API (.NET-клиент); WalReceiver → PostgreSQL —
   протокол PG + локальные child-процессы PG-утилит (внутри своего
   контейнера — не межконтейнерное взаимодействие). Docker от воркера —
   только §2 (create/start/.../list/inspect); docker-логи/exec в продуктовых
   путях бэкапов — НЕТ (exec-гварды pg_hba — существующее отклонение №23,
   задача t03, НЕ трогается).
3. **Полностью .NET**: никаких bash-скриптов и stdout-маркеров; фазы/итоги
   операций — в памяти приёмника, отдаются JSON по API; вся инцидентная
   механика bash-джоб (chown 101:101, standby.signal, auto.conf-правки,
   регистр сегментов WAL, канонический набор каталогов PGDATA) переносится
   в .NET 1:1.
4. **Надёжность (максимально надёжное решение)**: идемпотентность каждого
   шага; резюмов нет (pg_basebackup/recovery не резюмсятся — FAILED →
   переснятие новым id, как сегодня); transient/permanent-семантика
   сохраняется; бюджеты таймаутов — воркерские факты etcd (started_unix +
   часы воркера), отмена — cancel-команда API best-effort; takeover — всё
   состояние в etcd, имена контейнеров детерминированы.
5. **Multi-host**: гарантии — только от реплицированных источников (etcd,
   S3); память приёмника и staging — кэш без durability (рестарт операции =
   vanished → FAILED → переснятие; staging чистится/перезаполняется).
6. **Секреты**: не в etcd-статусах, не в GET-статусах API, не в логах.
   Full backup — приёмник резолвит сам из etcd (как для стрима). Tool-операции
   — секреты только в теле POST-команды запуска (аналог сегодняшнего env
   джобов; уровень доверия тот же — закрытый контур установки).
7. **API без аутентификации**: домашняя установка, published-порт, симметрия
   `/metrics` и Patroni REST (№4 — basic-auth там про REST Patroni; для
   приёмника аутентификация не вводится — enterprise-защиты не нужны).

## 3. Структура/компоненты

### 3.1. Канал PgWorker → WalReceiver: alias:8080 в общей сети, БЕЗ публикации на хост

- **Порт `:8080` приёмника наружу хоста docker-ноды НЕ публикуется** —
  в обоих режимах развёртывания (docker и swarm). `NodePorts`/portalloc НЕ
  меняются вовсе (никакого четвёртого «Receiver»-порта, EnsureNode-публикации
  и миграции portalloc-записей нет); канон arch/14 §2.1 «наружу хоста порт
  НЕ публикуется» сохраняется.
- **Адресация API — `alias:8080` по имени ноды в общей сети**. Инвариант
  поставки (решение пользователя): контейнеры воркера и нод ВСЕГДА находятся
  в одной сети — контурный режим (стенд/E2E): сеть контура поставки;
  прод-docker и swarm: сеть развёртывания, где ноды несут alias (per-cluster
  `pgw-net-<C>`/overlay — воркер в ней по построению поставки). Alias = имя
  ноды (docker-имя), симметрия `ResolveNodeRef`/контур-адресации источников
  — распространяется на ОБА режима.
- **Tool-контейнеры** (verify/drill/restore) — замечание их не касается:
  динамическая host-публикация API-порта сохраняется (воркер при create
  публикует свободный динамический порт — зонд свободных портов, правило
  AGENTS.md) → адрес `host:<динамический порт>`; в контуре — alias сети
  контура `:8080` без публикации. Сеть для S3: прод — default bridge +
  `extra_hosts` (`host.docker.internal:host-gateway`, `local:host-gateway`
  — как у сегодняшних джобов); контур — сеть контура.
- Скрейп Prometheus (`Docker:ScrapeNetwork`, arch/18 §2.8) не меняется.
- **Диагностика недостижимости**: alias не резолвится / соединение
  отвергнуто / 5xx / таймаут — transient для процессов воркера (ретраи
  тиком, journal-факт с адресом), НЕ краш тика; стойкая недостижимость —
  обычные бюджеты (FAILED по возрасту). Стоп-семантика подсистемы не
  меняется.

### 3.2. API WalReceiver (тот же Kestrel `:8080`)

JSON-контракт; маршруты минимального API (Minimal APIs). Все ответы —
короткие; фазы/ошибки — однострочные (JSON-безопасность, как сегодня в
result-JSON). Состояния операций: `running | succeeded | failed`; у полного
дополнительно `uploading` (фаза фиксации `wal_start_segment`).

Нодовый режим (полный бэкап; приёмник с identity):

| Метод/путь | Семантика |
|---|---|
| `POST /api/backup/full` | тело `{"id":"<YYYYMMDDHHMMSSZ>"}`; 202 — принята; 409 — уже есть активная (гварды приёмника); 503 — стоп-гейты (wal-ключ STOPPED, `/pgworker/backups-disabled`, `Backups:Enabled=false`-семантика) — операция не начинается; **WAL_CHAIN_ERROR — НЕ стоп-гейт полного**: 202, операция начинается (WAL_CHAIN_ERROR — нарушение цепочки WAL в архиве S3, НЕ состояние кластера; его единственное лечение — пересъём полного, гейтом он блокироваться не может — решение пользователя, §6 Р17) |
| `GET /api/backup/full` | 200 `{"state","phase":"basebackup\|uploading","wal_start_segment"?,"size_bytes"?,"error"?}`; 404 — активной нет (vanished-семантика для воркера) |
| `POST /api/backup/full/cancel` | 202 — best-effort: kill child `pg_basebackup`, отмена upload, чистка staging |

Tool-режим (verify/drill/restore; одна активная операция на контейнер):

| Метод/путь | Семантика |
|---|---|
| `POST /api/ops/verify` | тело `{"backup_id","s3":{"endpoints":[…],"access_key","secret_key","bucket","prefix":"<C>/<X>","ca_pem"?}}`; 202/409 |
| `GET /api/ops/verify` | `{"state","phase":"downloading\|verifying","error"?}`; 404 |
| `POST /api/ops/restore` | тело `{"backup_id","target_time"?:PG-совместимый (yyyy-MM-dd HH:mm:sszzz; воркер разбирает RFC3339 заявки и нормализует — разбор на воркере),"recovery_timeout_sec","s3":{…}}`; 202/409; `target_time` отсутствует = `latest` |
| `GET /api/ops/restore` | `{"state","phase":"downloading\|recovering","restored_to_lsn"?,"system_id"?,"error"?}`; 404 |
| `POST /api/ops/cancel` | 202 — best-effort (kill postgres/pg_verifybackup/скачивание, чистка) |

Секреты — ТОЛЬКО в POST-телах; GET-статусы и логи их не содержат.
`wal_start_segment` появляется с фазы uploading (из `backup_label`), как
сегодня. `system_id` — из `pg_controldata` (как сегодня в result-JSON).
Коды: 5xx — транспортная/внутренняя ошибка (воркер трактует transient).

### 3.3. Режимы WalReceiver и образ

Оба режима — один бинарь `walreceiver`, один образ `pgworker-node`
(правило «publish на хосте, в контейнер — артефакты» не меняется; сборка
образа — `docker/node/Dockerfile`, WalReceiver publish тот же):

- **Нодовый** (сегодняшний): env `PGW_BACKUP_CLUSTER/SHARD/NODE` + `PGW_ETCD`
  → supervisord-программа `walreceiver`, стрим WAL + НОВОЕ: API полного
  бэкапа на том же :8080. Операция полного — фоновая Task в процессе,
  параллельная стриму; статус — в памяти (IMMUTABLE-снимок, публикация —
  атомарная замена ссылки, паттерн `LiveFacts`).
- **Два независимых гейта в приёмнике** (не смешивать): гейт СТРИМА —
  wal-ключ STOPPED/WAL_CHAIN_ERROR, `/pgworker/backups-disabled` ⇒ приёмник
  `paused`, стрим замёрз (БЕЗ ИЗМЕНЕНИЙ, arch/19 §3); гейт ОПЕРАЦИИ
  полного — STOPPED/`backups-disabled`/`Enabled=false` ⇒ 503 на POST,
  WAL_CHAIN_ERROR — НЕ гейт (202, операция начинается; заживление
  WAL_CHAIN_ERROR — сам
  пересъём, §6 Р17). Замирание стрима при WAL_CHAIN_ERROR не мешает параллельному
  полному: стрим стоит (backpressure, слот держит WAL), полный пишет в S3.
- **Tool-режим** контейнера: env `PGW_NODE_TOOL=1` — (а) entrypoint образа
  НЕ запускает spilo-программу supervisord (Patroni/haproxy/doorman не
  стартуют); WalReceiver — главный процесс контейнера (PID-1 через
  entrypoint-ветку); (б) WalReceiver работает без identity (не требует
  `PGW_BACKUP_*`/`PGW_ETCD`, не пишет agent-ключи, не стримит) — ждёт
  команду API. `PGW_BACKUP_S3_BUCKET`/CA tool-режиму не нужны — весь
  S3-контекст приходит командой.
- Hosted-сервис стрима в tool-режиме не стартует (нет identity — нет цикла);
  Kestrel поднимается всегда (метрики + API).

Supervisord-конфиг/entrypoint: tool-режим отключает `[program:spilo]`
(стартует только supervisord-минимум + walreceiver, либо walreceiver
напрямую entrypoint'ом — выбор за планом, канон: «при старте tool-контейнера
никаких PG-команд, только старт контейнера»).

### 3.4. Механика операций (.NET, преемственность bash-механики)

Все child-процессы — `Process.Start` с теми же флагами, что сегодня в bash;
все инцидентные нюансы переносятся 1:1 (см. `RestoreJobCommand`/entrypoint):
chown `101:101` + chmod 700 PGDATA ПОСЛЕ модификаций, `standby.signal`
удаляется + `recovery.signal`, pristine `auto.conf` приберегается
(`cp` → `.orig`) и возвращается после promote с вычисткой
`archive_mode/archive_command`, `data_directory/hba_file/ident_file`
-переопределения, `wal_level=replica`, `summarize_wal=off`,
`shared_preload_libraries=''`, `ssl=off`, `logging_collector=off`,
trust-строка `pg_hba` на время поллинга, `HOME=/tmp`, setpriv-эквивалент
(запуск pg_ctl под uid/gid 101 — в .NET: `ProcessStartInfo` с
`UserName`/группой или обёртка `setpriv`, выбор за планом), канонический
`mkdir -p` набор каталогов PGDATA, регистр сегментов (`%f` ВЕРХНИЙ →
S3-ключи строчные — нормализация в fetch-режиме), `pg_is_in_recovery()`
поллинг с бюджетом (`recovery_timeout_sec` из команды), promote-окно
рестарта postmaster ретраится до бюджета.

- **Full backup** (нодовый режим): staging `/home/postgres/pgdata/backup-staging/
  <id>/` (внутри data-volume ноды; чистится после исхода и при старте
  новой операции) → `pg_basebackup -d "<DSN 127.0.0.1:5432 backup_exec>"
  -D <staging>/full -X stream --checkpoint=fast --manifest-checksums=SHA256`
  (пароль — через env `PGPASSWORD`, НЕ в argv; DSN-параметры — как у
  стримного подключения приёмника) → `wal_start_segment` из `backup_label`
  (парсер `BackupLabel` уже есть) → upload: рекурсивный обход staging,
  файлы 1:1 в `full/<id>/` (вкл. `backup_manifest`, `pg_wal/`) + дублирование
  закрытых сегментов набора в общий префикс `wal/` — .NET `IBackupS3`/
  `PoolBackupS3` (write-список по СВОЕЙ фактической роли REST-пробой —
  симметрия стриму; перебор нод, circuit-breaker 45 с, `s3-pool-exhausted`);
  `size_bytes` — сумма файлов. Смена роли ноды посреди полного НЕ рвёт
  операцию (write-список зафиксирован на старте).
- **Verify** (tool): list+скачивание `full/<id>/` .NET-клиентом в staging
  (том tool-контейнера) → `pg_verifybackup -D <staging>` → итог; сбой
  скачивания — transient (воркер держит PENDING, ретраи тиками),
  `pg_verifybackup` ≠ 0 — permanent (данные плохие) — семантика §5 arch/19
  сохраняется.
- **Restore/drill** (tool): механика одна (как сегодня один bash-скрипт);
  цель — data-volume первой ноды шарда (restore) / свой ephemeral volume
  (drill); PGDATA = `<volume>/pgroot/data` (Spilo-layout, env-контракт
  точек монтирования сохраняется). Скачивание полного → `restore_command`
  для postgres: sub-режим того же бинаря `walreceiver fetch-wal <seg>
  <dest>` — S3-контекст из env процесса (WalReceiver выставил при старте
  postgres — env наследуется до restore_command; секреты не в argv),
  перебор нод ReadAll, нормализация регистра, объекта нет → ненулевой exit
  (конец WAL). Локальный postgres: `pg_ctl start` (unix-socket,
  `listen_addresses=''`, без сети), поллинг `pg_is_in_recovery()`, снятие
  `pg_current_wal_lsn()`, `pg_ctl stop -m fast`, чистка recovery-остатков,
  `system_id` из `pg_controldata`. Drill = restore latest (без target) —
  инцидентные правки достают обоим (как сегодня).

### 3.5. Процессы воркера (перестройка на API)

Общий клиент: `WalReceiverApiClient` (HttpClient, per-вызов ~10 с; 5xx/таймаут
= transient). Все процессы сохраняют тиковую модель, клэйм-гварды,
journal-before-manipulations, transient/permanent-семантику, takeover.

- **BackupProcess** (§2 arch/19): G0–G3 без изменений (вкл. G1 ensure пароля,
  G2 роль + pg_hba-гвард, резолв пула, выбор источника `ResolveBackupSourceAsync`
  — sync-standby fallback мастер — ВЫБОР НОДЫ ОСТАЁТСЯ ЗА ВОРКЕРОМ; node/role
  в etcd-статусе — резолв воркера). Вместо create джоб-контейнера: PLANNED
  (etcd) → `POST /api/backup/full` приёмнику ВЫБРАННОЙ ноды → 202 ⇒ RUNNING;
  409 при PLANNED ⇒ операция уже жива (пере-старт прошлого тика) ⇒ RUNNING;
  503/5xx ⇒ PLANNED остаётся, следующий тик повторит (WAL_CHAIN_ERROR
  планировщик §2 требует переснять — API приёмника при WAL_CHAIN_ERROR
  отвечает 202 и пересъём
  не блокирует, §6 Р17). Супервизия: `GET` →
  `uploading` (фиксация `WalStartSegment`, как сегодня из маркеров) →
  `succeeded` (COMPLETED + `size_bytes`, `verify` PENDING при on_create) /
  `failed` (FAILED + `error`); 404 при RUNNING-статусе etcd ⇒ FAILED
  `operation-vanished` (рестарт приёмника/ноды; переснятие новым id —
  резюмов нет). Бюджет `Job:FullTimeoutSec` — ПЕРВЫЙ гвард (как сегодня):
  FAILED по возрасту + `POST cancel` best-effort; staging чистит приёмник.
  Контейнера полного больше нет — cleanup только cancel.
- **BackupVerifyProcess**: цепочечная проверка list-ами + LSN-разбор
  `.history` — БЕЗ изменений (воркер, S3). Checksums-фаза: tool-контейнер
  (create+start, published API-порт, ephemeral staging-volume
  `pgw-backup-verify-<C>-<X>-<id>` — имена/префиксы D1 сохраняются) →
  `POST /api/ops/verify` (S3-контекст read-списка) → поллинг `GET`:
  `succeeded` ⇒ OK + `checked_unix`; сбой скачивания (транзиент статуса
  `failed` с маркером загрузки — план фиксирует маркер) ⇒ PENDING ретраи
  тиками; `failed` ⇒ verify FAILED + `error`. Бюджет `Job:VerifyTimeoutSec`:
  kill+rm tool-контейнера, кандидат остаётся PENDING, попытка зачтена
  (вердикт FAILED по таймауту НЕ ставится — как сегодня). Итог ⇒ снос
  контейнера+volume.
- **RestoreProcess**: PLANNED-валидация, демонтаж, REJOINING (EnsureNode,
  initialize-щит system_id, SQL-гейт мастера) — БЕЗ изменений. Фаза RUNNING:
  tool-контейнер tool-режимом, монтирующий data-volume первой ноды (имя
  `pgw-backup-restore-<C>-<X>-<id>` — детерминизм/D1 живы), create+start →
  `POST /api/ops/restore` (backup_id / target_time / recovery_timeout_sec /
  S3-контекст source_pool DR-заявки ?? собственного пула) → поллинг `GET`:
  `phase` → статус etcd (панель видит downloading/recovering); `succeeded` ⇒
  REJOINING (+`restored_to_lsn`, `system_id`); `failed` ⇒ permanent-FAILED
  (чистка volume первой ноды — как сегодня). Бюджеты `Job:RestoreTimeoutSec`
  (статусный) и `recovery_timeout_sec` (внутри операции) — сохраняются;
  исчерпание ⇒ FAILED + снос контейнера. Демонтаж — ТОЛЬКО до первого старта
  tool-контейнера (409 volume-in-use прецедент жив).
- **RestoreDrillProcess**: отбор/валидация/cleaning-фаза — БЕЗ изменений;
  запуск — tool-контейнер (свой volume `pgw-backup-drill-<C>-<X>-<id>`,
  контейнер+volume одно имя — D1 жив) + `POST /api/ops/restore` (latest,
  без target) + поллинг; `Drill:TimeoutSec` / `drill-vanished` — как сегодня.

### 3.6. Конфигурация (§9 arch/19)

- `PgWorker:Backups:Job { Image }` — УПРАЗДНЯЕТСЯ (tool-контейнеры — образ
  нод `NodeImage`); `FullTimeoutSec/VerifyTimeoutSec/RestoreTimeoutSec` —
  ОСТАЮТСЯ (бюджеты воркера).
- `Staging { Dir }` — новый смысл: путь staging полного в data-volume ноды
  (дефолт `/home/postgres/pgdata/backup-staging`); `Staging.QuotaBytes` —
  УПРАЗДНЯЕТСЯ (tmpfs-квот джоб нет; ENOSPC → FAILED реактивно).
- `Agent { Cpu, Mem }` — лимиты tool-контейнеров (было: всех джобов) —
  сохраняются; приёмник в нодах — по-прежнему без отдельных лимитов
  (бюджет полного — общий контейнер ноды).
- `Restore { RecoveryTimeoutSec }` — сохраняется (в команде restore/drill).
- Остальное (Policy/Retry/Wal/Retention/Quota/Drill/Supervisor/S3.Bucket/
  CaBundlePath) — без изменений.

### 3.7. Образы и чистки

- `pgworker-backup` упраздняется: `docker/PgWorker.Backup.Dockerfile`,
  `docker/backup/entrypoint.sh`, сборка в `E2ePrepare`, упоминания в
  images-инфраструктуре/ранбуке — убрать (локальный образ, в registry
  не значился).
- Deprovisioning D1: префиксы `pgw-backup-verify-<C>-`,
  `pgw-backup-restore-<C>-`, `pgw-backup-drill-<C>-` — живы (tool-контейнеры);
  `pgw-backup-full-<C>-` умирает вместе с контейнерами. D2 — без изменений.
- Стоп-семантика/deprovisioning S3-объекты не трогают — без изменений.
- Метрики: НОВЫХ серий приёмника не вводится (осознанно: наблюдаемость
  операций — etcd-статусы воркера + журнал; `/metrics` остаётся как есть).

### 3.8. Тесты

- Юниты: API-контракт приёмника (маршруты/гварды/статусы — фейки операций);
  механика операций (child-процессы через абстракцию, фейки; инцидентные
  правки — табличные тесты auto.conf/signal/каталогов/регистра); процессы
  воркера на фейке `WalReceiverApiClient` (202/409/503/404, бюджеты,
  vanished, cancel); адресация alias:8080 (резолв адреса приёмника ноды
  из topology/portalloc-фактов + режим сети).
- Интеграция/E2E: сценарии бэкапов адаптируются (assertions на etcd-статусы
  не меняются); изоляция/teardown/телеметрия — по AGENTS.md и
  docs/e2e-isolation.md; динамические порты, зачистка серий.
  Гейт: свежий Release, сборка и тесты ОТДЕЛЬНЫМИ командами; `E2ePrepare`
  (уже без pgworker-backup); минимум `Scale_AddEmptyShard` + ПОЛНЫЙ прогон
  E2eFixture (меняются образ ноды/supervisord, адресация API и все
  бэкап-пути; tool-контейнеры с динамическими портами).

## 4. Фазы (для plan)

1. **Arch-first** (ДО кода): правки канонов — см. §5 «Отражение в arch»
   (arch/19, arch/25, arch/14, arch/18). Ревью arch-правок — по текстам
   канонов, без кода.
2. **WalReceiver**: API-хост (маршруты/гварды/статусы), tool-режим
   (Program/identity/стрим-гейт), операции .NET (full → verify → restore/
   drill: скачивание, upload, child-процессы, recovery-механика, fetch-wal
   sub-режим), staging-чистки; supervisord/entrypoint tool-ветка; образ
   pgworker-node (тот же publish).
3. **Воркер**: `WalReceiverApiClient` + адресация alias:8080 (режимы сети,
   диагностика недостижимости — transient); перестройка BackupProcess/
   BackupVerifyProcess/
   RestoreProcess/RestoreDrillProcess на API; конфиг (§3.6); удаление
   джоб-спек/логов (`BackupJobSpec`, `RestoreJobSpec`, `VerifyJobSpec`,
   `DrillJobSpec`, `*JobLog`, `*JobCommand`, `GetContainerLogsAsync`-пути);
   упразднение `pgworker-backup` (Dockerfile/entrypoint/E2ePrepare).
4. **Тесты**: юниты → интеграция → docker-E2E (§3.8); мерж-гейт — полный
   E2eFixture на свежем Release.
5. **Мерж-коммит**: удалить из `arch/roadmap/backup.md` пункты `t26`, `t24`,
   `t23` (правило роадмапа «слито → тег удаляется тем же коммитом»; t23/t24
   поглощены — решения пользователя, §6 Р6/Р7), вычистить `←`-ссылки.

## 5. Отражение в arch (состав правок канонов, arch-first)

- **arch/19 §2** (механика полных): исполнитель — приёмник WalReceiver ноды
  по API (команда `POST /api/backup/full {id}`, статус GET, cancel);
  стоп-гейты POST полного — STOPPED/`backups-disabled`/`Enabled=false`;
  **WAL_CHAIN_ERROR — НЕ стоп-гейт полного** (202; WAL_CHAIN_ERROR — разрыв
  цепочки в АРХИВЕ
  S3, не состояние кластера; пересъём — единственное лечение и гейтом не
  блокируется; выбор источника при WAL_CHAIN_ERROR — штатный: sync-standby,
  fallback мастер; стрим-гейт приёмника при WAL_CHAIN_ERROR — без изменений,
  §3 arch/19); состояние wal-ключа `BROKEN` переименовано в
  `WAL_CHAIN_ERROR` (перечень `ACTIVE|DEGRADED|STOPPED|WAL_CHAIN_ERROR`;
  имена с GAP запрещены — коллизия с полем `gap` TLI-зазора 58P01); pg_basebackup
  локально `127.0.0.1:5432`, upload .NET S3-клиентом по write-списку своей
  роли; staging — каталог data-volume ноды (реактивный ENOSPC); выбор
  ноды-источника и node/role-факты — воркер; бюджеты — воркер + cancel
  best-effort; vanished ⇒ FAILED; stdout-протокол джоба упразднён.
- **arch/19 §3.5/§3.6** (restore/drill): tool-контейнер образа pgworker-node
  (tool-режим: без Patroni, WalReceiver — главный процесс, без identity),
  старт без команд; операции — команды/статус API; механика восстановления —
  WalReceiver (.NET, child-процессы, restore_command = `walreceiver
  fetch-wal`); REJOINING/демонтаж/щит/бюджеты — как было.
- **arch/19 §5** (verify): checksums-фаза — tool-контейнер + API (механика
  pg_verifybackup та же); цепочечная проверка — без изменений.
- **arch/19 §6** (лимиты): Agent {Cpu,Mem} — tool-контейнеры; staging-тома
  джоб упразднены (staging полного — data-volume ноды); бюджеты зависших —
  воркерские, отмена — cancel API.
- **arch/19 §7** (секреты): env-джобов больше нет; tool-операции получают
  S3-контекст командой API (секреты только в POST, не в статусах/логах);
  полный в ноде — резолв приёмником из etcd (как стрим); CA https — PEM в
  команде (tool) / резолв приёмником (нода, уже есть).
- **arch/19 §8/§9/§10**: карта разделов, конфиг (§3.6 выше), риски
  (stdout-протокол — удаляется; сетевые риски tool-контейнеров/S3 — по
  extra_hosts/контуру, как у джобов сегодня).
- **arch/25**: №22 — ЗАКРЫТ (docker-логи из продуктовых путей уходят;
  запись канона «PgWorker → WalReceiver: HTTP API операций бэкапов
  (`alias:8080` в общей сети воркера и нод, без публикации на хост — docker
  и swarm)»); №13 (Backup-джобы → S3) — упраздняется
  (S3-доступ у приёмника — уже №12); §5-переходы №22 убрать.
- **arch/14 §2.1/§2.4**: инвариант «контейнеры воркера и нод всегда в одной
  сети» — адресация API приёмника `alias:8080` (docker и swarm), порт
  :8080 наружу хоста НЕ публикуется (строка таблицы портов walreceiver:
  «/metrics + API операций»); portalloc/NodePorts без изменений;
  недостижимость — transient с journal-диагностикой.
- **arch/18 §2.8**: :8080 приёмника несёт и API операций (помимо /metrics);
  новых серий нет.

## 6. Принятые решения (закрытые развилки)

Решения пользователя (AskUserQuestion, 2026-10-25):

- **Р1. Канал API** — `alias:8080` по имени ноды в общей сети воркера и
  нод; порт `:8080` наружу хоста НЕ публикуется ни в docker, ни в swarm
  (правка по замечанию пользователя, 2026-10-25, каскадный сброс Фазы 3:
  «для Receiver не нужно создавать host:<receiver-port из portalloc> —
  они не должны публиковаться на хост; порт публикуется только alias:8080
  и в докере, и в swarm»). `NodePorts`/portalloc НЕ меняются. Инвариант
  поставки (дословный ответ пользователя): «их контейнеры всегда находятся
  в одной сети». Tool-контейнеры — динамическая host-публикация API-порта
  сохраняется (в замечание не входят).
- **Р2. Образ tool-контейнера** — тот же `pgworker-node` с tool-режимом
  (env отключает Patroni-программу; WalReceiver — главный процесс,
  инструментальный режим без identity). Отдельный лёгкий образ НЕ вводится.
- **Р3. Механика операций** — полностью .NET (Process.Start PG-утилит,
  .NET S3-клиент; bash-скрипты и stdout-маркеры умирают вместе с
  pgworker-backup).
- **Р4. Параметры операций** — смешанная модель: full в нодах — приёмник
  резолвирует сам из etcd (команда несёт только id; 503 — стоп-гейты
  операции, см. Р17);
  tool-операции — полный контекст (S3 endpoints/креды/bucket/префикс/
  backup_id/цель) в команде запуска от воркера; секреты только в POST.
- **Р5. Staging полного** — каталог в существующем data-volume ноды
  (`/home/postgres/pgdata/backup-staging/<id>`), чистка приёмником, ENOSPC →
  FAILED реактивно; отдельных томов/tmpfs НЕ вводится.
- **Р6. t24-job-status-channel** — удаляется из роадмапа ЦЕЛИКОМ мерж-коммитом
  t26 (docker-логи уходят полностью).
- **Р7. t23-backup-job-endpoint** — удаляется тоже (предмет задачи — mc-джобы —
  упраздняется; S3-доступ .NET-клиентом по спискам пула).

Решения, однозначно вытекающие из задачи/кода (зафиксированы здесь):

- **Р8. Бюджеты** — воркерские (started_unix + часы воркера, факты etcd);
  отмена — `cancel`-команда API best-effort (нода — чужой контейнер, воркер
  процесс убить не может; для tool-контейнеров — kill+rm контейнера, как
  сегодня).
- **Р9. Рестарт приёмника посреди операции** — операция исчезает (404) ⇒
  воркер пишет FAILED `operation-vanished`, переснятие новым id (резюмов
  нет — pg_basebackup/recovery не резюмятся; миграция RUNNING-ключей старого
  формата при обновлении — тот же путь).
- **Р10. Выбор ноды-источника полного** — за воркером (как сегодня:
  sync-standby fallback мастер); write-список S3 — приёмник берёт по СВОЕЙ
  фактической роли (REST-проба, симметрия стриму), фиксируется на старте
  операции.
- **Р11. Гварды** — приёмник: 409 на вторую активную операцию; воркер:
  инвариант одного активного полного на шард (как сегодня); 503 — стоп-гейты
  операции полного (STOPPED/`backups-disabled`/`Enabled=false`, Р17);
  гейт СТРИМА приёмника (STOPPED/WAL_CHAIN_ERROR/disabled ⇒ `paused`) — без
  изменений, к операции полного не применяется (кроме 503-гейтов выше).
- **Р12. Аутентификация API** — не вводится (домашняя установка;
  enterprise-доработки — только через roadmap).
- **Р13. Пароль backup_exec** — в argv child-процессов НЕ передаётся
  (env `PGPASSWORD`; сегодня bash раскрывает DSN в argv — фиксируем
  улучшение без изменения механики).
- **Р14. G2 pg_hba-гвард** (docker exec) — остаётся у воркера без изменений
  (отклонение №23 → задача t03, вне t26).
- **Р15. Инцидентная механика restore** (auto.conf/standby.signal/chown 101/
  регистр сегментов/каталоги initdb/promote-окно) — переносится в .NET 1:1,
  включая возврат pristine auto.conf и вычистку archive_mode.
- **Р16. Метрики** — новых серий приёмника не вводится (осознанно;
  наблюдаемость — через etcd-статусы воркера и журнал).
- **Р17. WAL_CHAIN_ERROR — не стоп-гейт полного** (решение пользователя,
  каскадный сброс из Фазы 6 E2E `Backup_ChainBroken_SelfHeals`, дословно:
  «в арх правку при BROKEN приоритет источник — sync-standby, fallback на
  мастер, 503 НЕ ДОЛЖЕН распространяться на полный бэкап при BROKEN. BROKEN
  это нарушение цепочки wal в архиве, это НЕ ЗНАЧИТ что сам кластер
  BROKEN»). Переименование (выбор пользователя): состояние wal-ключа
  `BROKEN` переименовано в **`WAL_CHAIN_ERROR`** — перечень состояний
  `ACTIVE|DEGRADED|STOPPED|WAL_CHAIN_ERROR`; имена с GAP запрещены
  (коллизия с полем `gap` — TLI-зазор 58P01, отдельная сущность).
  Семантика: WAL_CHAIN_ERROR — нарушение цепочки WAL в АРХИВЕ (S3), НЕ
  состояние кластера и НЕ свидетельство о данных нод. `POST
  /api/backup/full` при WAL_CHAIN_ERROR отвечает 202 — операция начинается;
  503-гейты операции — только STOPPED, `/pgworker/backups-disabled`,
  `Backups:Enabled=false`. Выбор ноды-источника при WAL_CHAIN_ERROR —
  штатный, без изменений (sync-standby по Patroni `GET /cluster`, fallback
  на мастер). Стоп-семантика СТРИМА приёмника при WAL_CHAIN_ERROR (paused,
  замирание) — без изменений; заживление — пересъёмом полного, который
  гейтом больше не блокируется (прежнее противоречие «503 при
  WAL_CHAIN_ERROR» vs «пересъём — единственное лечение» — устранено).

## 7. Ограничения

- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`,
  `Directory.Packages.props`; идентификаторы — английские, комментарии/
  документация — русские (AGENTS.base.md §8).
- Никаких новых отклонений arch/25 §1/§2 (docker-логи/exec/копирование
  файлов НЕ появляются; list/inspect для tool-контейнеров — разрешённая
  проверка факта запуска).
- Тестовые порты — динамические; `BrokerBootSec` ≤ 100 с; изоляция и
  teardown E2E по канонам; телеметрия E2E (`[PHASE]`, артефакты, MarkFailed).
- E2E-образы: publish на хосте, в контейнер — только артефакты (запрет
  `COPY src/`).
- Свежесть бинаря в гейте: отдельная сборка → `--no-build` тесты.

## 8. Критерии приёмки

1. `grep GetContainerLogsAsync src/` — вызовов из продуктовых путей НЕТ
   (остаётся только API движка, пользователей вне тестов телеметрии — нет);
   образ `pgworker-backup` и его сборка отсутствуют в репо/E2ePrepare.
2. Канал API: `NodePorts`/portalloc-контракт и записи НЕ изменились; у нод
   кластера нет published-порта приёмника (`docker inspect` нод — публикаций
   :8080 нет); воркер адресует операции `alias:8080` (контур и общая сеть
   поставки); tool-контейнеры — динамический host-порт API (проверка
   инспектом tool-контейнера); скрейп Prometheus работает как раньше.
3. Полный бэкап: планировщик создаёт PLANNED → API-запуск → RUNNING →
   UPLOADING (wal_start_segment) → COMPLETED; файлы в S3 1:1 (вкл.
   `backup_manifest`, `pg_wal/`) + дублирование сегментов в `wal/`;
   etcd-статусы формата §4 arch/19 без изменений (парсеры панели живы);
   staging после исхода чист.
4. FAILED-пути: age-бюджет ⇒ FAILED + cancel; рестарт приёмника ⇒
   operation-vanished ⇒ переснятие с бэкоффом; 503 стоп-гейтов
   (STOPPED/`backups-disabled`/`Enabled=false`) ⇒ операция не стартует,
   статус не портится; **WAL_CHAIN_ERROR ⇒ POST полного 202, операция
   стартует** (источник — штатный sync-standby fallback мастер; стрим
   приёмника замёрз по гейту стрима; E2E `Backup_ChainBroken_SelfHeals` —
   заживление пересъёмом сходится, лив-лока 503 нет; wal-ключ в состоянии
   `WAL_CHAIN_ERROR`, не `BROKEN`).
5. Verify: on_create + периодическая перепроверка; OK/FAILED + причины;
   таймаут ⇒ PENDING (вердикт НЕ ставится), ретраи.
6. Restore: заявка через API воркера (contract §3.5 arch/19 не меняется) —
   демонтаж → tool-контейнер → восстановление PGDATA в volume первой ноды →
   REJOINING → COMPLETED (wal-ключ удалён, переснятие планировщиком);
   target_time/latest/DR-source_pool работают; drill: SUCCEEDED/FAILED +
   restored_to_lsn, cleaning-доводка, D1/D2-чистки живы.
7. Tool-контейнер: старт без команд; все операции — только через API;
   Patroni в нём не стартует; после итога — контейнер/volume снесены.
8. Docker-E2E на свежем Release: полный E2eFixture зелёный (вкл.
   TLI-сценарии/бэкап-серии), минимум `Scale_AddEmptyShard`; юниты/
   интеграция зелёные; зачистка серий по канонам.
9. Arch-документы обновлены ДО кода (§5); расхождений проекта с arch/ нет;
   мерж-коммит удаляет t26/t24/t23 из роадмапа.
