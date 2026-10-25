# t25: WalReceiver внутри PG-контейнера ноды — spec

## 1. Цель

Приёмник WAL бэкапов исполняется процессом ВНУТРИ контейнера PG-ноды,
работает с etcd напрямую и несёт собственные метрики. Упраздняются:
отдельный long-running контейнер `pgw-backup-wal-<C>-<X>-<N>`, образы
`pgworker-wal` (dev/e2e), конфиг `Wal:AgentImage`, инвариант «нода и её
wal-агенты в одной сети», чтение docker-логов wal-агентов воркером
(отклонение arch/25 №21 закрывается: фазы — etcd, факты — метрики).
Arch-канон уже обновлён (первая фаза задачи, см. §8) — код зеркалит его.

Канон после правок (источник истины):
- arch/19 §3 «Агент» — приёмник в контейнере ноды, идентичность из env,
  роль источника REST-пробой, цели/креды — резолв из etcd, permanent —
  состояние, стоп-семантика через etcd;
- arch/19 §4 — статус-ключ `/pgworker/backups/<C>/<X>/agent/<N>` (lease),
  глобальный `/pgworker/backups-disabled`, agents-факты wal-ключа из
  статус-ключей приёмников;
- arch/14 §2.1 — состав образа ноды (+`walreceiver`), env
  `PGW_BACKUP_*`, приёмник не образует сетевой сущности;
- arch/18 §2.8/§5.2/§5.4 — словарь метрик приёмника, джоба
  `walreceiver-nodes`, второй file_sd;
- arch/25 — №21 теперь канон (WalReceiver → PgWorker через etcd), №3/№18
  расширены.

## 2. Принципы

- Arch-first: контракт уже в `arch/`; каждая правка кода зеркалит его
  пункт в пункт. Расхождение кода с arch/19 §3/§4 — блокирующее.
- Инвариант RPO не меняется: подтверждение слота ТОЛЬКО после успешного
  put (arch/19 §3); стрим от хвоста S3; staging-тома нет.
- Двойная архивация (мастер + sync-standby) сохраняется; состав
  стримящих приёмников определяют сами ноды (REST-проба роли).
- «Истина прогресса — S3»: контроль цепочки/BROKEN/lag остаётся за
  воркером (WalStreamProcess), статусные ключи приёмников — факт
  живости/роли/маркеров, не источник прогресса.
- Стандартные каналы (arch/25 §1): приёмник ↔ воркер — только etcd;
  воркер не читает логи нод и не делает exec для подсистемы WAL
  (history-доклад — приёмником из ФС своей ноды).
- Надёжность: приёмник — демон без permanent-выходов (перезапуск
  permanent-причину не лечит, воркер процесс в ноде не перезапускает);
  supervisord ноды перезапускает только падения процесса.
- Идемпотентность каждого шага, transient vs permanent (arch/17).

## 3. Целевая структура и компоненты

### 3.1. Приёмник `PgWorker.WalReceiver` (программа `walreceiver`)

Точка входа — `WebApplication` (ASP.NET, база образа уже требует
aspnet-рантайм): Kestrel слушает `0.0.0.0:8080` с единственным
эндпоинтом `/metrics` (`Shared.Metrics.AddAppMetrics/MapAppMetrics`,
Meter `PgWorker.WalReceiver`, словарь arch/18 §2.8) + фоновый
hosted-сервис стрима. Ядро `WalReceiverCore` (репликация, сборка
сегментов, tli-процедура, backpressure) переиспользуется; обвязка
переписывается.

Главный цикл приёмника (каждая итерация ~5 c при ожидании; в стриме —
пока поток жив):

1. **Идентичность/env**: `PGW_BACKUP_CLUSTER/SHARD/NODE` (env контейнера
   ноды, ставит `EnsureNode`/`SpiloEnvBuilder`), etcd — `PGW_ETCD`
   (URLs через запятую), bucket `PGW_BACKUP_S3_BUCKET`, опц.
   `PGW_BACKUP_S3_CA_PEM`. Невалидный/отсутствующий env идентичности —
   старт-отказ: лог в stdout контейнера (диагностика) и exit —
   supervisord не перезапускает (`autorestart=unexpected`,
   `exitcodes=0` у обёртки).
2. **Стоп-гейты (etcd)**: глобальный ключ `/pgworker/backups-disabled`
   есть → state `waiting_disabled`, пауза; wal-ключ
   `/pgworker/backups/<C>/<X>/wal` со state `STOPPED`/`BROKEN` →
   state `paused`, пауза. Живость etcd недоступна — продолжаем работу
   по последним известным гейтам (стрим не рвём из-за etcd-моргания:
   слот держит, RPO не страдает; гейты перепроверяются при следующем
   успешном чтении).
3. **Роль источника**: `https://127.0.0.1:8008/primary` → 200 ⇒
   master; `/sync` → 200 ⇒ sync (любой не-200/ошибка ⇒ role=none,
   state `waiting_role`). CA — REST-CA ноды (`SSL_RESTAPI_CA` env,
   приёмник материализует PEM во временный файл; прецедент —
   master-lease.py; 127.0.0.1 в SAN серта, arch/14 §2.4 п.6).
   Опрос роли: на каждом цикле подключения + периодически в стриме
   (5 c) — смена роли без обрыва стрима (sync → async) завершает
   стрим-сессию чисто и возвращает `waiting_role`.
4. **Креды/цели (резолв из etcd, живой)**: пароль
   `/clusters/<C>/backup_password` (нет → `waiting_password`); policy
   `/pgworker/backups/<C>/policy` → секция `s3` (нет →
   `waiting_policy`); пул — `BackupPoolResolver` + `S3PoolStore`
   (PgWorker.Backups, тот же резолв, что у воркера) → write-список по
   роли (`write_master`/`write_replica`), read-список, креды
   (Failed-резолв → `waiting_policy` с ошибкой в `error` — transient,
   ретрай). CA https — `PGW_BACKUP_S3_CA_PEM` → `CaBundlePath`
   (фабрика HttpClient, как сегодня).
5. **Слот**: имя `BackupNames.Slot(<C>,<X>)`; слота нет →
   `waiting_slot` (ensure делает воркер, arch/19 §3; lost-слот — тоже
   `waiting_slot`: воркер-контроль лечит recreate).
6. **Стрим**: существующая механика `WalReceiverCore` — подключение к
   `127.0.0.1:5432` (user `backup_exec`, пароль из etcd, dbname
   `postgres`), сверка `wal_segment_size` (mismatch →
   `failed_permanent`, НЕ exit), резолв хвоста S3 read-списком
   (пустой префикс — от `restart_lsn` слота), `START_REPLICATION`,
   сборка сегментов → put с перебором write-списка → подтверждение
   слота после успешного put. Исчерпание write-списка: флаг
   `pool_exhausted=true` в статусе (снимается первым успешным put),
   ретрай-цикл, стрим стоит. TLI-переход: `TIMELINE_HISTORY`, при
   недоступности — чтение `pg_wal/<TLI>.history` ФС ноды
   (`/home/postgres/pgdata/pgroot/data/pg_wal/`; нет и файла — факт
   `history_missing` в статусе). Ре-доклад инвентаря history: при
   (ре)старте стрима и далее раз за цикл подключения приёмник
   перечисляет `.history`-файлы `pg_wal` своей ноды и кладёт в S3
   `wal/` отсутствующие (list read-списком, put идемпотентен, вне
   hot-path, отказ put — diagnostic-log) — страховка переходов вне
   окна стрима, когда приёмник не наблюдает промоут (arch/19 §3).
   tli-процедура 58P01 — без изменений, факт `tli_jump` пишется в
   статус. Разрыв LSN протокола → `failed_permanent` (не exit).
7. **Статус-ключ** `/pgworker/backups/<C>/<X>/agent/<N>`: lease TTL
   15 c, keepalive 5 c; значение
   `{"node","role","state","last_delivered_segment"?,"last_delivered_unix"?,"pool_exhausted","tli_jump"?,"error"?,"updated_unix"}`
   (формат — arch/19 §4; `state` ∈ `starting|waiting_role|
   waiting_password|waiting_policy|waiting_slot|waiting_disabled|
   streaming|paused|failed_permanent`). Обновляется при смене
   состояния/фактов и не реже раза в 5 c (вместе с keepalive).
   etcd-put статуса не должен рвать стрим: put идёт вне hot-path
   (после закрытия сегмента / по таймеру), отказ put — diagnostic-log,
   keepalive продолжает.
8. **Метрики** (arch/18 §2.8): `walreceiver_role`,
   `walreceiver_streaming`, `walreceiver_segments_delivered_total`,
   `walreceiver_bytes_delivered_total`, `walreceiver_backlog_bytes`
   (serverWalEnd потока − подтверждённый хвост),
   `walreceiver_put_retries_total`, `walreceiver_put_errors_total`,
   `walreceiver_reconnects_total`,
   `walreceiver_last_success_timestamp_seconds`; лейблы
   cluster/shard/node (+role у role-серии).
9. **Отмена**: SIGTERM (supervisord stop / docker stop ноды) —
   graceful: стрим-сессия закрывается, lease отпускается, exit 0.

Старый env-контракт `WalReceiverEnv` (PG_HOST/S3_ENDPOINTS/…, exit-коды
3/4/5/6) упраздняется вместе с контейнерным образом: программа больше
не запускается воркером как джоб-контейнер.

### 3.2. Образ ноды `pgworker-node` (docker/node)

- `Dockerfile`: `ARG WAL_PUBLISH_DIR` (default `artifacts/node-wal`) —
  COPY self-contained publish-вывода приёмника в `/opt/pgw-wal/`;
  сборка образа ноды теперь ДВУХШАГОВАЯ: (1) на хосте
  `dotnet publish src/PgWorker.WalReceiver -c Release -r <rid>
  --self-contained -o artifacts/node-wal` (RID по хосту сборки:
  linux-arm64 / linux-x64; без trimming — рефлексия Npgsql/AWSSDK/OTel
  не выдерживает), (2) `docker build -f docker/node/Dockerfile
  -t pgworker-node:dev .` (контекст — корень репо, как сегодня).
  Канон «publish на хосте, в контейнер — только артефакты» сохраняется.
- `supervisord.conf`: программа `walreceiver` —
  `command=/opt/pgw-wal/walreceiver` (обёртка не нужна: бинарь
  самодостаточен; условность запуска обеспечивает сам бинарь —
  отсутствие `PGW_BACKUP_CLUSTER` → старт-отказ exit, см. §3.1 п.1),
  `user=postgres` (доступ к `pg_wal` PGDATA), `autorestart=unexpected`,
  `exitcodes=0` (restarts только на падения), `startsecs=5`,
  stdout/stderr → `/dev/stdout` (локальная диагностика).
- `docker-entrypoint.sh`: без изменений (env приёмника приходит из env
  контейнера, файлы материализует сам приёмник).

### 3.3. Воркер: EnsureNode и драйвер

- `SpiloEnvBuilder.Build` + `ClusterDriver.EnsureNode`: в env ноды
  добавляются `PGW_BACKUP_CLUSTER/SHARD/NODE` (из topology/nodeName —
  всегда, независимо от `Backups:Enabled`: приёмник сам гейтится
  etcd-стоп-ключами) и `PGW_BACKUP_S3_BUCKET` /
  `PGW_BACKUP_S3_CA_PEM` (из `BackupsOptions.S3.Bucket`/
  `CaBundlePath` — воркер читает файл CA и передаёт PEM значением;
  bucket пуст ⇒ env не ставится, приёмник стоит в `waiting_policy`
  с диагностикой «env PGW_BACKUP_S3_BUCKET is not set» — старт-отказа
  живого процесса нет: валидация старта воркера
  `Enabled=true → Bucket непустой` — fail-fast инсталляции, приёмник
  в контейнере ноды — отдельный процесс, восстанавливается сам при
  появлении env).
- `ClusterDriver`: УДАЛЯЮТСЯ `EnsureBackupAgentAsync`,
  `ListBackupAgentsAsync`, `BackupAgentLogsAsync`, тип
  `BackupAgentNames`. `RemoveBackupAgentsAsync` ОСТАЁТСЯ как
  миграционно-санитарный снос контейнеров старого формата
  `pgw-backup-wal-<C>-*` (вызов — в начале `WalStreamProcess.TickAsync`
  под клэймом: один list на тик кластера; найдены — снос, пусто —
  no-op). Это закрывает конфликт «старый агент держит слот, новый
  приёмник не стартует» при развёртывании t25 без оператора.
- Deprovisioning D1/`RemoveClusterAsync`: чистка префикса
  `pgw-backup-*` остаётся (джобы + возможные legacy-остатки).

### 3.4. Воркер: WalStreamProcess

Остаётся (без изменений семантики): гвард restore-заявки; ensure
`backup_password`; резолв пула (для контроля); резолв мастера +
sync-standby; слоты (ensure/зонды/lost-recreate); контроль цепочки по
расписанию `Wal:VerifyIntervalSec` (union-листинг, chain_start/ratchet,
BROKEN/гэпы, lag-зонд); планировщик полных; метрики §2.7-наблюдатели.

Меняется:

1. **Шаг (4–5) ensure-агентов УДАЛЯЕТСЯ** (контейнеров больше нет).
   Вместо него — чтение статус-ключей приёмников:
   `Range /pgworker/backups/<C>/<X>/agent/` → факты для поля `agents`
   wal-ключа: ключ жив (lease) → `running`; ключа нет → `absent`
   (`exited` новыми записями не пишется — legacy-значение парсера).
   Нестандартный случай «источник без живого приёмника» (мастер или
   sync без running-ключа при не-BROKEN wal-ключе) → `DEGRADED` c
   `error="wal receiver not running on <N>"` (transient-семантика;
   алерт панельный по DEGRADED остаётся общим).
2. **Gap (tli_jump)**: источник факта — статус-ключ мастер-приёмника
   (`tli_jump`-факт, живёт до следующего прыжка), не хвост docker-лога.
   Гейты установки/снятия прежние (покрытие COMPLETED-полным /
   исчезновение профиля из листинга). `ParseTliJumpMarker` (парсер
   логов) удаляется.
3. **Pool-exhaustion**: `IsPoolExhaustionActive` (позиционный детектор
   лога) удаляется; маркер `s3-pool-exhausted` в `error` wal-ключа
   ставится по флагу `pool_exhausted` статус-ключа мастер-приёмника.
4. **History-доклад `DeliverMissingHistoriesAsync` (docker-exec)
   удаляется** — history докладывает приёмник в обоих окнах: при
   наблюдаемом переходе (§3.1 п.6) и ре-докладом инвентаря при
   (ре)старте/цикле стрима — переходы вне окна стрима закрываются от
   любой ноды, физически содержащей файл (arch/19 §3). Контроль
   воркера изменений НЕ получает: новых docker-операций и etcd-ключей
   нет, `history_missing` остаётся фактом статус-ключа.
5. **Стоп-семантика**: `StopAllAsync` (`Enabled=false`) — агенты не
   сносятся; воркер: держатель глобального лидера `/pgworker/leader`
   при каждом тике лидера поддерживает plain-ключ
   `/pgworker/backups-disabled` (put `{"updated_unix"}`); каждый
   инстанс при старте с `Enabled=true` делает `del` ключа.
   `StopShardAsync` (QUARANTINED) — wal-ключ `STOPPED` (приёмники
   замирают сами), docker-вызовов нет. `BreakAsync` — wal-ключ
   `BROKEN` без сноса агентов (приёмники замирают по гейту; слот
   пересоздаётся как сегодня).
6. **Резолв источников** остаётся нужным для слотов/лаг-зонда/агентских
   ожиданий (мастер+sync по Patroni/portalloc — как сегодня).

### 3.5. Конфигурация PgWorker

- `BackupsOptions.Wal.AgentImage` УДАЛЯЕТСЯ (вместе с валидацией
  старта и env `PgWorker__Backups__Wal__AgentImage` в E2E-фикстурах).
- Валидация старта: `Backups:Enabled=true` → непустой
  `S3:Bucket` (как сегодня); новых обязательных ключей нет.
- Секция `Wal { VerifyIntervalSec, LagMaxSegments, StaleSec }` — без
  изменений (контроль воркера).

### 3.6. Образы, стенд, E2E-инфраструктура

- УДАЛЯЮТСЯ: `docker/PgWorker.Wal.Dockerfile`,
  `docker/PgWorker.Wal.E2E.Dockerfile`, сервис `pgworker-wal`
  (build-профиль) в `deploy/docker-compose.yml`, сборка
  `pgworker-wal:dev` в `dev-stand/adminpanel/checks/00-up.sh` (вместо
  неё — publish приёмника в `artifacts/node-wal` + пересборка
  `pgworker-node:dev` тем же скриптом, `[PHASE]`-метки), `WalImage` и
  `EnsureWalImageAsync` в `E2eEnvironment`.
- `E2eEnvironment.PrepareStaticAssetsAsync`: добавляется publish
  WalReceiver self-contained (RID хоста) → `artifacts/e2e/node-wal` +
  сборка `pgworker-node:e2e-<wt>` с `--build-arg WAL_PUBLISH_DIR=…`
  (контекст корень, как сегодня; лог — `[PHASE]` + тайминг).
- `Metrics.SdGenerator`: тем же проходом пишет
  `sd/walreceiver-nodes.json` (те же записи per-node, порт `:8080`,
  http) — конфиг `SdGenerator:WalOutputPath` (arch/18 §5.4/§8);
  `prometheus.yml` стенда — джоба `walreceiver-nodes` (file_sd, без
  TLS-конфига).
- Дашборд `dashboards/backups.json` — панель per-node приёмников
  (role/streaming/segments/backlog/put-ошибки).

### 3.7. Миграция живых контуров (развёртывание t25)

1. Снос legacy wal-контейнеров — автоматически (§3.3, санитарный вызов
   в тике WalStreamProcess; идемпотентен).
2. Ноды старого образа (без приёмника): надзор НЕ пересоздаёт живые
   ноды — приёмники отсутствуют, контроль видит «источник без живого
   приёмника» → DEGRADED с именем ноды (диагностика оператору).
   Обновление — штатным механизмом пересоздания нод (recreate-API /
   `docker rm -f pgw-…` → надзор поднимет с новым образом; данные в
   volume сохраняются). Runbook-заметка в `docs/runbook.md` (образ
   ноды: двухшаговая сборка + обновление живых кластеров).

## 4. Фазы работ (порядок реализации)

1. **Приёмник**: новый Program (WebApplication + hosted-цикл §3.1),
   статус-ключ/lease, стоп-гейты, роль REST, резолв etcd
   (BackupPoolResolver), self-contained publish. Юниты: контракт
   статуса, гейты, резолв, машина состояний (fake etcd/REST/S3 —
   существующие фейковые транспорты WalReceiver-тестов).
2. **Образ ноды**: Dockerfile/supervisord; сборка; sanity-прогон контейнера
   (приёмник стартует, /metrics отвечает, без env — старт-отказ).
3. **Воркер**: env ноды (SpiloEnvBuilder/EnsureNode), санитарный снос
   legacy, WalStreamProcess по §3.4 (агенты из etcd, gap/exhaustion из
   статуса, удаление docker-логов/exec-доклада), удаление
   AgentImage/BackupAgentNames/BackupAgentLogsAsync. Юниты: agents-факты
   из ключей, DEGRADED «не жив», gap из tli_jump-факта, стоп-семантика
   (backups-disabled put/del), «docker-вызовы агентов отсутствуют»
   (fake-драйвер: ноль вызовов логов/агент-ensure).
4. **Метрики/стенд**: словарь §2.8 в приёмнике, sd-generator + второй
   file_sd, prometheus.yml, дашборд.
5. **Инфраструктура**: deploy-compose/00-up.sh/E2eEnvironment
   (§3.6), runbook-заметка.
6. **E2E**: адаптация сценариев (§6) + прогон мерж-гейта.

## 5. Ограничения

- Не трогаются: планировщик полных, ретенция, verify/drill/restore,
  панельный контракт wal-ключа (значения `agents[].state` —
  running/absent; `exited` остаётся легаси-значением парсера),
  инвариант подтверждения-по-доставке, tli-процедура, слотная
  механика воркера, sticky-правила portalloc/сетей нод.
- Воркер не получает новых docker-операций сверх §2 arch/25 (create/
  start/stop/kill/remove/сети/тома + list/inspect); санитарный снос —
  stop/remove.
- Никаких новых etcd-ключей кроме двух канонизированных
  (`agent/<N>`, `backups-disabled`); префикс панели `/pgworker/backups/`
  читается ею же толерантно.
- Приёмник не публикует портов наружу хоста и не слушает ничего,
  кроме `/metrics` на `:8080` внутри контейнера.
- Multi-host: потеря docker-хоста = потеря ноды и её приёмника вместе
  (одна сущность) — худший случай не хуже текущего (агент на том же
  хосте); гарантии по-прежнему от etcd/S3.

## 6. Критерии приёмки

1. **Юниты**: приёмник — машина состояний §3.1 (гейты disabled/
   STOPPED/BROKEN, роль, waiting_*, failed_permanent без exit,
   pool_exhausted set/reset, tli_jump-факт, lease-статус пишется);
   воркер — agents-факты running/absent из Range по ключам, DEGRADED
   «источник без приёмника», gap по tli_jump-факту, exhaustion-маркер
   по флагу, стоп-семантика (put/del `backups-disabled`), отсутствие
   вызовов `BackupAgentLogsAsync`/agent-ensure в тике (fake-драйвер).
2. **Интеграция (docker, динамические порты)**: контейнер
   `pgworker-node:dev` с env идентичности поднимается: приёмник жив,
   `/metrics` внутри контейнера отдаёт словарь §2.8; без env —
   старт-отказ и supervisord не лупит; lease-ключ появляется/гаснет
   (TTL) со смертью процесса.
3. **docker-E2E** (мерж-гейт, свежий Release, две отдельные команды:
   build → `--no-build`): сценарии
   `E2eBackupPoolScenarios`/`E2eBackupJobsScenarios`/
   `E2eBackupVerifyScenarios`/`E2eBackupFailsOnBadS3Scenarios`/
   `E2eDrillScenarios` адаптированы (ассерты wal-потока — по
   агент-ключам etcd и объектам S3, не по `docker ps pgw-backup-wal-*`)
   и зелёные; минимум `Scale_AddEmptyShard` на свежем Release; полный
   контур `E2eFixture` — при изменениях provisioning. Специфика t25:
   - обе ноды-источника (мастер+sync) стримят (агент-ключи обеих,
     сегменты в S3); async-реплика — `waiting_role`, не стримит;
   - исчерпание write-списка → флаг в агент-ключе → DEGRADED
     wal-ключа с `s3-pool-exhausted`; подъём ноды пула — self-heal;
   - разворот кластера на старых wal-контейнерах (или их имитация)
     → санитарный снос первым тиком, слот свободен, новый приёмник
     стримит;
   - `Enabled=false` → `backups-disabled` жив → приёмники
     `waiting_disabled`; enable → ключ снят → стрим возобновляется;
   - tli-сценарий (промоут) — `tli_jump`-факт в агент-ключе, gap в
     wal-ключе, восстановление покрытием; `.history` — приёмником при
     ЛЮБОМ окне перехода: наблюдаемом стримом или вне окна стрима
     (ре-доклад инвентаря §3.1 п.6 при следующем (ре)старте/цикле
     подключения ноды, физически содержащей файл) — PITR target_time
     через TLI-переход достижим.
4. **Стенд**: полный подъём `00-up.sh` собирает pgworker-node:dev
   (publish приёмника + build, `[PHASE]`-логи), сервис `pgworker-wal`
   отсутствует; джоба `walreceiver-nodes` в Prometheus up (при
   ScrapeNetwork), серии приёмника в TSDB.
5. **Контроль чистоты контура** — по канонам e2e-isolation/e2e-launch
   (teardown, ассерт чистоты, логи в артефакты).
6. **Мерж-гейт roadmap**: пункт `t25-wal-receiver-in-pgnode` удаляется
   из `arch/roadmap/backup.md` мерж-коммитом; пункт `t24-job-status-channel`
   корректируется тем же коммитом — из него уходит агентная часть
   (остаётся только джобовая, №22); зависимость `t26 ← t25`
   сохраняется.

## 7. Принятые решения (закрытые развилки)

1. **Механика запуска/надзора внутри ноды — supervisord** (уже канон
   образа ноды, arch/14 §2.1): программа `walreceiver`,
   `autorestart=unexpected`/`exitcodes=0` — перезапуск только падений;
   permanent-семантика — состояние `failed_permanent` в etcd, НЕ exit
   (перезапуск не лечит, воркер процесс чужого контейнера не
   перезапускает — arch/25 §2). Альтернативы (systemd/s6/собственный
   супервизор-процесс, docker-restart-политика) отвергнуты: вторая
   сущность надзора в образе не нужна.
2. **Упаковка .NET в Spilo-образ — self-contained publish без
   trimming** (RID хоста сборки, single-dir): нулевые зависимости от
   базового образа, отсутствие чужого рантайма в Spilo; trimming
   рискован рефлексией стека. Альтернатива (apt-установка
   aspnet-рантайма в Spilo-базу) отвергнута: утяжеляет базовый образ и
   вводит внешнюю зависимость сборки.
3. **Идентичность приёмника — явные env `PGW_BACKUP_*` от EnsureNode**
   (не вывод из PATRONI_SCOPE/мастер-ключа): scope `<C>-<X>`
   неразложим однозначно (дефисы в именах); единая точка генерации env
   ноды — SpiloEnvBuilder. `PGW_ETCD` переиспользуется.
4. **Роль источника — REST-пробы Patroni loopback** (`/primary`,
   `/sync`; официальный API Patroni 4.x; CA ноды, 127.0.0.1 в SAN,
   живой прецедент master-lease.py). Альтернативы: SQL-зонд
   (`pg_is_in_recovery` не различает sync/async на самой реплике),
   чтение DCS `/service/<scope>/sync` (хрупкий формат Patroni-ключа),
   назначение роли воркером (возвращает env-дрейф и пересоздания —
   против цели задачи). Смена ролей подхватывается приёмником без
   участия воркера.
5. **Стримят только мастер и sync-standby** (как сегодня): приёмник
   живёт во ВСЕХ нодах, `waiting_role` в остальных — дёшев (REST-проба
   + etcd-полл ~1/5 c), инвариант двойной архивации сохранён.
6. **Статус приёмника — lease-ключ TTL 15 c** (keepalive 5 c) по
   образцу `instances/<id>`/`api/<id>`: «ключ жив = приёмник жив» без
   доверия часам нод. `exited` из новых записей воркера исчезает:
   живой процесс с ошибкой — `running` + `error` в wal-ключе; мёртвый
   процесс/нода — ключа нет = `absent` (действия оператора/воркера
   одинаковы, различие не нагрузочное). Панельный парсер толерантен
   (канон arch/19 §4).
7. **Глобальный стоп-ключ `/pgworker/backups-disabled` — plain, вне
   per-cluster префиксов**: per-cluster STOPPED-ключи на пустых
   префиксах ломали бы панельное правило «пустой префикс = подсистема
   не включена» (ложный BackupFullMissing); lease отвергнут: смерть
   всех инстансов воркера при `Enabled=false` непреднамеренно
   разрешала бы стрим — plain-ключ сохраняет соответствие конфигу;
   `del` при старте с `Enabled=true` гарантирует снятие.
8. **`pool_exhausted`/`tli_jump` — факты в статус-ключе приёмника**
   (не stdout-маркеры, не воркерские детекторы лога): закрывает
   отклонение №21; позиционный детектор хвоста лога и окно вымывания
   200 строк уходят; факты персистентны до события-снятия.
9. **`.history`-fallback — чтение `pg_wal` из ФС ноды приёмником**:
   приёмник живёт в контейнере с PGDATA (user=postgres); воркеровский
   docker-exec-доклад (часть отклонения №23) удаляется. Достройка —
   ре-доклад инвентаря history: при (ре)старте стрима и далее раз за
   цикл подключения приёмник перечисляет `.history`-файлы `pg_wal`
   своей ноды и кладёт в S3 `wal/` отсутствующие (list read-списком,
   put идемпотентен, вне hot-path, отказ put — diagnostic-log) — та же
   ФС ноды и тот же канал, что и стрим-доклад; новых etcd-контрактов
   и docker-операций НЕ появляется. Переходы вне окна стрима (промоут,
   пока нода не стримила) закрываются от любой ноды, физически
   содержащей файл: после промоута `000000NN.history` лежит в `pg_wal`
   нового мастера, после restore — в pgdata восстановленной ноды.
   Ноды без файла — факт `history_missing` (без изменений), контроль
   воркера не меняется (arch/19 §3).
10. **Слот/креды/policy — резолв приёмником из etcd на каждом цикле**
    (переиспользуются BackupPoolResolver/S3PoolStore/BackupPolicyReader):
    протухание env и пересоздания по кредам исчезают по построению;
    ensure/зонды/lost-лечение слотов остаются за воркером (канон §3).
    Bucket/CA — единственное из «куда бэкапиться», что остаётся env:
    per-install константы (не секреты, не дрейфуют).
11. **Санитарный снос legacy `pgw-backup-wal-*` — в тике
    WalStreamProcess** (list + stop/remove, идемпотентно): без него
    старый агент держал бы физический слот (один активный consumer) и
    блокировал приёмник; снос без оператора — конвергентно.
12. **Метрики приёмника — собственный Kestrel `:8080` внутри контейнера
    ноды, http, порт наружу не публикуется; скрейп — по
    ScrapeNetwork `alias:8080` вторым file_sd**. Альтернативы
    (трансляция серий воркером через etcd — потеря пер-нодой точности
    и лишний контракт; публикация четвёртого host-порта ноды — расход
    portalloc) отвергнуты. Без ScrapeNetwork теле­метрия приёмника
    недоступна снаружи — состояние доставки наблюдаемо сериями §2.7 и
    etcd (асимметрия приемлема, как у patroni-скрейпа).
13. **Новые Prometheus-алерты на серии приёмника не заводятся**:
    здоровье доставки уже закрывают BackupWalLagHigh/BackupWalStale
    (истина по объектам S3); серии приёмника — дашборд/разбор. Отдельный
    коллектор не вводится (серии экспонирует сам приёмник).
14. **Усыновлённые (`object`) ноды — вне подсистемы**: приёмник есть
    только в образе `pgworker-node`; усыновлённый внешний контейнер
    приёмника не несёт, WAL-архивация с него невозможна (зафиксировано
    в arch/19 §3 «Подключение»). Расширение (внедрение приёмника в
    чужой контейнер) не заводится — против R9 (чужой контейнер движок
    не трогает).
15. **arch/02-topology.md не тронут**: документ описывает метал-стенд
    (таблица «что крутится на хостах»: etcd + Spilo + HAProxy) и
    wal-агентов/PgWorker-оркестрацию не упоминает — править нечего;
    упоминание arch/02 в пункте роадмапа снимается мерж-коммитом
    вместе с самим пунктом.
16. **Приёмник стартует во всех нодах всегда** (env идентичности
    ставится независимо от `Backups:Enabled`): гейты — etcd
    (backups-disabled/policy/wal-ключ); «выключенный» приёмник стоит
    копеечно (полл), а включение подсистемы не требует пересоздания
    нод. Bucket-пустой кейс закрыт валидацией старта воркера.
17. **`WalReceiverCore` переиспользуется**, обвязка (Program/статусы/
    гейты/резолв) пишется заново под контракт §3.1; старый
    env-контракт и exit-коды 3/4/5/6 упраздняются (контейнерного
    супервизора с пересозданием больше нет — семантика permanent
    переехала в состояние).
18. **Панель не меняется**: контракт wal-ключа (agents/gap/error)
    семантически сохранён; панельный парсер уже толерантен. UI-правки
    не входят в задачу.
19. **`docs/runbook.md`** — правка минимальная: сборка образа ноды
    (двухшаговая), обновление живых нод (recreate), снятие сервиса
    `pgworker-wal`.

## 8. Arch-правки (выполнены до кода, в этом worktree)

- `arch/19-backups.md` — §1 (схема/оркестратор), §2 (образ), §3
  («Агент» переписан: идентичность/роль/резолв/надзор/стоп-семантика;
  инвариант, двойная архивация, `.history` (наблюдаемый переход +
  ре-доклад инвентаря при (ре)старте/цикле стрима), «Подключение»,
  «Истина прогресса», непрерывность/pool_exhausted, стоп-семантика),
  §3.5 (демонтаж restore/COMPLETED), §4 (шапка писателей, строка wal
  `agents`, НОВЫЕ ключи `agent/<N>` и `backups-disabled`), §6
  (лимиты), §7 (передача секретов/CA), §8 (карта), §9 (конфиг без
  AgentImage), §10 (риски).
- `arch/14-pgworker.md` — §2.1 (состав образа + таблица ролей с
  `walreceiver :8080`; per-cluster сеть без wal-агентов; ScrapeNetwork
  несёт и скрейп приёмника), §2.4-хвост и §5-инвариант поллинга — без
  wal-агентов.
- `arch/18-metrics.md` — §2.8 (словарь приёмника), §5.2 (джоба
  `walreceiver-nodes`), §5.4 (второй file_sd), §8
  (`SdGenerator:WalOutputPath`).
- `arch/25-component-communications.md` — №3/№11/№18 расширены, №21
  переписан в канон (WalReceiver → PgWorker через etcd), §5 —
  отклонения №22–24 (№21 закрыто настоящей задачей).
- `arch/adminpanel/04-local-stand.md` — стендовая сеть без wal-агентов.
- `arch/README.md` — строка оглавления 19.
- `arch/02-topology.md` — без правок (решение №15).
