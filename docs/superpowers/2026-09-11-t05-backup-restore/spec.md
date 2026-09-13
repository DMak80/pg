# Spec: t05-backup-restore — восстановление шарда из бэкапа (полный + WAL, PITR)

Дата: 2026-09-11
Канон: [arch/19-backups.md](../../../arch/19-backups.md) §3.5 (восстановление — раздел добавлен этой задачей), §2 (контракт образа/планировщик), §4 (etcd-контракт), §5 (layout S3), §7 (секреты), §9 (конфиг), §10 (риски); [arch/14-pgworker.md](../../../arch/14-pgworker.md) §1.1 (эндпоинт API), §5 A/C (паттерны EnsureNode/P2.2/Д3). Roadmap: `arch/roadmap/backup.md` (t05). Предыдущие задачи подсистемы в main: t02 (полные бэкапы), t03 (WAL-стрим).

## 1. Цель

Восстановление шарда из бэкапов по команде оператора через PgWorker:
полный бэкап из S3 + накат WAL-цепочки до цели (PITR) — `latest` (конец
цепочки) или `target_time` (RFC3339, откат к моменту до порчи). Кластер
(шард) уничтожается и поднимается из бэкапов с проверкой данных —
закрывается автоматическим docker-E2E и ручным прогоном dev-станда по
runbook-сценариям.

Решения фазы brainstorming (зафиксированы с пользователем):

1. **Канал команды** — HTTP API воркера `POST /api/clusters/{c}/shards/{x}/restore`
   (все мутации — через API, воркер единственный писатель etcd; UI — t08
   без изменения контракта).
2. **Гранулярность** — шард: первая нода восстанавливается из бэкапа+PITR,
   реплики догоняются Patroni (`pg_basebackup` с восстановленного лидера).
   Параметр `source` (S3-префикс источника, default — свой `<C>/<X>`)
   закрывает DR-сценарий «восстановление в новый кластер»: оператор
   пересоздаёт кластер декларацией и зовёт restore с source погибшего.
   Потеря одной ноды живого шарда — НЕ restore (Patroni rebuild быстрее;
   runbook отсылает к `POST /api/ha/{scope}/nodes/{node}/recreate`).
3. **PITR-таргеты** — `latest` + `target_time` (RFC3339). LSN/named restore
   point — вне скоупа (YAGNI до запроса).
4. **Подтверждение** — тело запроса обязано нести `confirm` = имя шарда
   (точное имя перезатираемого); мисматч → 400.
5. **Приёмка** — автоматический E2E-гейт (свежий Release) + ручной прогон
   runbook-сценариев на dev-станде (`dev-stand/adminpanel/checks/00-up.sh`)
   с фиксацией результата в задаче.

Границы t05:

- ВХОДИТ: restore-джоб (inline-команда контейнера), `RestoreProcess`
  (машина тика: валидация → демонтаж → джоб → rejoin → пост-обработка),
  API-эндпоинт, etcd-контракт `restore/<id>` + модель/парсер, гварды
  исключения шарда из контуров воркера на время restore, расширение
  планировщика t02 (срочный полный при сброшенной цепочке), панельный парсер
  + алерт `restore-failed`, E2E-маркеры (latest/target_time/DR), runbook
  `docs/backup-restore.md`.
- НЕ входит: verify полных (t04 — статусы verify читаются, restore по
  непроверенному бэкапу допустим: валидация — существование+цепочка),
  ретенция/чистка S3 (t06), супервизор-reconcile (t07), UI бэкапов (t08),
  отмена бегущего restore (FAILED → новая заявка; YAGNI), инкрементальные
  резюмы наката (plain-механика канона).

## 2. Принципы

1. **arch-first**: правки канона (arch/19 §2/§3.5/§4/§8/§9/§10, arch/14
   §1.1) внесены ДО кода в этой же задаче; расхождения вскрываются — канон
   правится тем же dev-flow.
2. **PgWorker — единственный писатель** `/pgworker/backups/*` (клэйм `<C>`);
   API принимает команду и сам пишет заявку-статус; джоб контракта etcd не
   знает (stdout-маркеры + exit-код — протокол t02).
3. **Restore — операция владельца шарда**: пока активен restore-ключ шарда,
   контуры воркера (надзор/эвакуация/планировщик полных/усыновление) шард не
   трогают — иначе пустой Patroni поверх восстановления и ложная эвакуация.
4. **Паттерны arch/17/14**: идемпотентность каждого шага (перепроверка
   факта), journal-before-manipulations (PLANNED до сноса), transient vs
   permanent, takeover по статусу+детерминированным именам.
5. **Механика — базовые утилиты PG**: PITR средствами сервера
   (`restore_command` + `recovery_target_*` + `pg_ctl`), без WAL-G/pgBackRest;
   джоб слушает только unix-socket (`listen_addresses=''`) — сетевых кредов
   PG не нужно, S3-креды — env контейнера (§7 канона).
6. **Данные дороже всего**: S3-объекты restore только читает; перезатирается
   только целевой шард (подтверждённый `confirm`); RPO = точка бэкапа/WAL-цели.

## 3. Структура/компоненты

### 3.1. etcd-контракт (arch/19 §4)

Ключ `/pgworker/backups/<C>/<X>/restore/<id>` (`id = YYYYMMDDHHMMSSZ` UTC
принятия заявки, коллизия — суффикс `-2/-3` как у полных):

```json
{
  "state": "PLANNED|RUNNING|REJOINING|COMPLETED|FAILED",
  "backup_id": "20260911120000Z",
  "source": "<srcC>/<srcX>",            // default = собственный <C>/<X>
  "target": "latest" | "time:<RFC3339>",
  "node": "s1a",                        // первая нода декларации (в неё restore)
  "requested_unix": 1760000000, "requested_by": "operator",
  "started_unix": null, "finished_unix": null,
  "phase": null,                        // RUNNING: downloading|recovering (из логов джоба)
  "restored_to_lsn": null,              // из result-JSON джоба
  "error": null
}
```

- Максимум один активный (не COMPLETED/FAILED) restore на шард — гвард API
  (409) и гвард процесса.
- По COMPLETED ключ `wal` шарда УДАЛЯЕТСЯ (сброс цепочки → планировщик t02
  немедленно переснимает полный — инвариант «поднятый шард имеет валидную
  цепочку или активный полный»; PITR-назад несшиваем со старой TLI-цепочкой).
- Deprovisioning D2 чистит весь префикс; remove-shard помечает активный
  restore `FAILED/error=cancelled-by-remove`.

Модель — `PgWorker.Etcd/Parsing/BackupsModel.cs`: enum `RestoreStatus
{Planned,Running,Rejoining,Completed,Failed}`, record `RestoreOperationState`;
`BackupsParser` разбирает `restore/<id>` в `ShardBackups` (толерантно,
битые → parseErrors — паттерн существующего парсера).

### 3.2. API-эндпоинт (arch/14 §1.1)

`POST /api/clusters/{c}/shards/{x}/restore` (mTLS-грань воркера, по образцу
`RotateClusterSecretsHandler`/`RecreateNodeHandler`):

- Тело: `{"backup_id"?: string, "target_time"?: string(RFC3339),
  "source_cluster"?: string, "source_shard"?: string, "confirm": string,
  "requested_by"?: string}`. `target_time` задан → цель time, иначе latest.
  `source_cluster`/`source_shard` заданы → source-override (DR).
- Валидации (400/404/409): имена канонические; кластер существует и Active
  (404/409); шард заявлен в декларации (404); `confirm == x` (400, сообщение
  с ожидаемым именем); активный restore на шарде уже есть (409 — идемпотентность
  повтора как у ротации); `target_time` парсится как RFC3339 (400).
  Существование бэкапа/цепочки — НЕ API-валидация (дело процесса: при DR
  etcd-статусов нет) → процесс FAILED с причиной.
- Успех — 202 + DTO `{"cluster","shard","restore_id","state":"PLANNED",...}`;
  воркер пишет PLANNED-ключ сам (клэйм не нужен: ключ заявки — journal-запись,
  выполняет держатель следующим тиком).

### 3.3. Restore-джоб (механика, arch/19 §3.5)

Ephemeral контейнер `pgw-backup-restore-<C>-<X>-<id>`, образ
`Backups:JobImage` (pgworker-backup — база `postgres:18` уже несёт серверные
бинари того же мажора PG, что Spilo-18; Dockerfile НЕ меняется, контракт
образа дополнен каноном §2). Inline-команда от воркера
(`Restore/RestoreJobCommand.cs` — билдер bash-скрипта, единственное место
restore-механики; `Restore/RestoreJobSpec.cs` — ContainerSpec: env, монтирование
data-volume первой ноды `pgw-<C>-<X>-<n>-data` в точку `/restore`, лимиты
`Agent { Cpu, Mem }`, restart-политика `no`, портов нет, extra_hosts — как у
джоба t02).

Скрипт джоба (фазы — stdout-маркеры по протоколу t02):

1. `{"phase":"downloading"}` — `mc cp --recursive` из
   `s3://<bucket>/<srcC>/<srcX>/full/<backup_id>/` в PGDATA-каталог Spilo
   layout (`<точка-monтирования-volume>/pgroot/data`; volume-корень узла /home/postgres/pgdata — arch/14 §2.1), `chown -R
   101:101` (uid:gid postgres в Spilo; численно — в джобе postgres:18 uid
   иной, численное chown обязательно).
2. Кладёт `/restore-wal.sh` (restore_command: `mc cp` сегмента/`.history` из
   `wal/`-префикса прямо в запрошенный `%p`; объекта нет → exit 1 — конец
   WAL) и дополняет `postgresql.auto.conf`: `restore_command`,
   `recovery_target_action=promote`, при цели time — `recovery_target_time`
   (цель latest — БЕЗ recovery_target: конец WAL = выход из recovery).
3. `{"phase":"recovering"}` — `pg_ctl -w start` с
   `-o "-c listen_addresses=''"` (unix-socket only, сети нет, кредов нет) →
   поллинг `pg_is_in_recovery()` до выхода из recovery (бюджет
   `Restore:RecoveryTimeoutSec`, дефолт 1800) → `pg_ctl stop -m fast` →
   убрать recovery-строки из `auto.conf`.
4. Result-JSON: `{"ok":true,"restored_to_lsn":"0/..."}` или
   `{"ok":false,"error":"..."}` + exit-код. PG-диагностики (target not
   reached / конец WAL раньше цели / дыра) — в error одной строкой
   (JSON-безопасность — по образцу entrypoint t02).

Env джоба: S3-комплект (MC_HOST-alias одной строкой — паттерн
`WalAgentCommand.McHost`), bucket, source-префикс, backup_id, цель
(target_time или пусто), PGDATA-путь, бюджет recovery. Никаких паролей PG.

### 3.4. RestoreProcess (машина тика под клэймом `<C>`)

`src/PgWorker.Backups/Process/RestoreProcess.cs`; вызов — из
`ClusterProcesses`/`ReconcileLoop` Active-ветки после `WalStreamAsync` (одна
активная заявка кластера за тик — обрабатываем старейшую). Guard'ы: клэйм
наш; `Backups:Enabled=true` (restore — часть подсистемы: S3-комплект обязана
иметь); кластер Active; шард заявлен. Ошибка шарда не роняет тик.

Фазы (статус → действия; все шаги идемпотентны, transient → ретрай тиком):

1. **PLANNED → валидация**: шард без усыновлённых (`object`) нод — чужой
   PGDATA-контур → **permanent FAILED** «restore усыновлённых шардов не
   поддерживается» (ручной путь — runbook); источник бэкапа — `backup_id`
   из заявки или
   новейший: свой шард → max COMPLETED из etcd-статусов; source-override
   (или своих нет) → `BackupS3.ListFullsAsync` (новый метод: list префикса
   `<srcC>/<srcX>/full/`) — max id с `backup_manifest`. Читаем
   `wal_start_segment`: свой — из etcd-статуса полного; чужой/отсутствует —
   `BackupS3.DownloadTextAsync` объекта `full/<id>/backup_label` (новый
   метод, разбор START WAL LOCATION — прецедент entrypoint t02). Цепочка:
   `BackupS3.ListWalAsync(srcC, srcX)` → `WalChain.CheckChain` от
   wal_start — дыра → **permanent FAILED** (error с границами дыры).
   Валидация проходит → фиксируем `backup_id` в статусе, `started_unix`.
2. **RUNNING (демонтаж)**: WAL-агент шарда стоп (`RemoveBackupAgentsAsync`);
   контейнеры+data-volumes ВСЕХ нод шарда снесены (stop+rm, volume rm;
   404=ок — идемпотентно); HA-scope `/service/<C>-<X>/` вычищен (точечные
   initialize/leader/sync + префиксы optime//members/ — образец Д3-чистки
   ProvisioningProcess; `request_*` НЕ трогаем); ноды
   `state=REBUILDING` (панель отображает строкой — контракта не меняет);
   portalloc/dsn НЕ трогаем (закрепления живы, порты те же). journal
   `/pgworker/work/<C>` op=`backup-restore`.
3. **RUNNING (restore-джоб)**: volume первой ноды создан (docker volume
   create; первая нода = min по имени из декларации шарда) → джоб-контейнер
   по `RestoreJobSpec` → супервиз по образцу `SuperviseActiveAsync` t02:
   поллинг логов (`phase` → статус), exit 0 + `ok:true` → фаза 4; иначе
   **FAILED** (error из result-JSON/exit-кода), контейнер+volume-остатки
   чистятся; running сверх `RecoveryTimeoutSec` + грейс → докилл → FAILED
   «recovery-бюджет исчерпан». RUNNING без контейнера → перезапуск
   идемпотентен (PLANNED-паттерн t02).
4. **REJOINING**: `EnsureNode` первой ноды на восстановленном volume
   (существующая механика провижининга: Spilo-конфиг тот же) → ожидание
   Patroni-пробы, идентифицирующей нашу ноду как ЛИДЕРА (P2.2-образец:
   `role=leader/primary` + `state=running/streaming`; бюджет
   `PatroniBootSec`) — Patroni поднимает существующие данные (bootstrap с
   непустым PGDATA при чистом DCS) → остальные ноды: чистые `EnsureNode`,
   реплики догоняются `pg_basebackup` с лидера → **контракт rejoin (решение
   2026-09-12): COMPLETED ждёт лидера running/streaming + реплики,
   успешно СТАРТОВАВШИЕ синхронизацию** (`state=creating replica` —
   basebackup пошёл — или уже `running/streaming`); полное окончание
   basebackup ожидает Patroni (свой retry), не заявка — иначе rejoin
   висит на копировании данных (замер E2E: 55–70 с на ~100 МБ) и флейкует
   на любом разумном бюджете.
5. **COMPLETED**: пост-обработка — del ключа `wal` шарда (сброс цепочки);
   `finished_unix`, `restored_to_lsn`; журнал `phase=done`. Новый полный
   снимет планировщик (§3.5-расширение), агента поднимет WalStreamProcess.

**Гварды исключения** (реакция на активный restore-ключ шарда — чтение из
снапшота префикса `/pgworker/backups/`, который цикл уже парсит):
`NodeSupervisor` скипает шард (не пересоздаёт снесённые ноды, не rebuild'ит,
не считает ShardDeadSec); `BucketEvacuator` не берёт шард в кандидаты;
`BackupProcess` скипает шард (полные не снимаются); `AdoptionProcess`/
`MoveRepairProcess` шард не касаются; `RemoveShardProcess` — явная ветка:
активный restore → `FAILED(cancelled-by-remove)` и демонтаж продолжается;
`DeprovisioningProcess` D1 расширяет префикс чистки джоб-контейнеров до
`pgw-backup-` (полные + restore одним префиксом — обобщение
`JobContainerPrefix`).

### 3.5. Расширение планировщика t02 (инвариант цепочки)

`BackupPlanner.IsDue` дополняется: полный due, если возраст последнего
COMPLETED > `full_max_age_sec` **ИЛИ ключ `wal` шарда отсутствует** (цепочка
не заведена — свежий шард или сброшена restore'ом). Бэкофф/один-активный не
меняются. Совместимость: шард до первого запуска агента уже due (нет
COMPLETED) — поведение прежнее; после restore — немедленный новый полный
(точка входа новой цепочки). Юнит-покрытие: due при отсутствии wal-ключа,
not-due при живом wal-ключе и свежем полном, due по возрасту при живом
wal-ключе.

### 3.6. BackupS3 (два новых метода)

`ListFullsAsync(cluster, shard)` — list-v2 с пагинацией по префиксу
`full/` (id бэкапов для DR-поиска); `DownloadTextAsync(cluster, shard,
objectKey)` — маленький текстовый объект (`backup_label`). Остальное —
переиспользование `ListWalAsync`/кредов.

### 3.7. Панель AdminPanel (минимальная грань)

- `AdminPanel.Etcd` `BackupsParser` — разбор `restore/<id>` (модель
  «только поля статусов», битые → parseErrors толерантно).
- Правило алертов `restore-failed` (critical, `IAlertRule`-паттерн): у шарда
  живого Active-кластера restore в FAILED — текст = `error` статуса, remedy —
  «разбор по docs/backup-restore.md, повтор заявки». Активный restore
  (PLANNED/RUNNING/REJOINING) — без алерта (журнал/фазы видны в статусе).
- UI-грань бэкапов — t08; алерт виден в общем списке.

### 3.8. Конфигурация

`BackupsRuntimeOptions` дополняется `RestoreRecoveryTimeoutSec = 1800`
(appsettings: `PgWorker:Backups:Restore { RecoveryTimeoutSec=1800 }`,
env `PGW_BACKUP_RESTORE_RECOVERY_TIMEOUT_SEC` не нужен — не секрет;
deploy/docker-compose.yml — без изменений, образ тот же). Прочие бюджеты
(Patroni-подъём) — существующий `PatroniBootSec`.

### 3.9. Runbook `docs/backup-restore.md`

Разделы (русский, команды curl 1:1 API, ожидаемые статусы etcd, диагностика
FAILED):

1. **Полная потеря ноды**: живой шард → `POST /api/ha/{scope}/nodes/{node}/recreate`
   (Patroni rebuild, restore не нужен); весь шард мёртв (алерты
   provision/shard-no-leader, контейнеры/диски погибли) →
   `POST .../restore` latest; проверка данных контрольными SELECT.
2. **Порча данных** (DROP/ошибочный UPDATE): остановить запись приложением →
   определить время порчи (журналы приложения/Patroni) → restore с
   `target_time` за минуту до порчи → проверка → новый полный снимется сам.
3. **Восстановление в новый кластер (DR)**: etcd-контур и ноды погибли, S3
   жив → пересоздать кластер той же декларацией (`POST /api/clusters`) →
   дождаться Active (пустые шарды) → на каждый шард `POST .../restore` c
   `source_cluster`/`source_shard` погибшего → проверка данных.
   Плюс: сводка механики (что делает воркер по фазам), необратимость
   (RPO = точка бэкапа/цели), чтение статусов etcdctl'ом, таблица ошибок
   FAILED (дыра цепочки / target не достигнут / бюджет наката).

## 4. Фазы

- **Ф0. Канон (выполнен в этой задаче до кода)**: arch/19 — §1 (границы/
  схема), §2 (контракт образа + планировщик-инвариант), §3.5 (новый раздел
  restore), §4 (ключ restore), §8 (карта), §9 (конфиг Restore), §10 (риски);
  arch/14 — §1.1 (эндпоинт). Spec-гейт: ревью правок канона.
- **Ф1. Ядро без docker/S3**: `RestoreJobCommand` (билдер скрипта: layout
  Spilo, chown 101, auto.conf/recovery-параметры по цели, restore-wal.sh),
  `RestoreJobSpec`, модель `RestoreOperationState` + JSON-сериализация
  статуса, `BackupNames` (RestoreKey/ContainerName/префиксы). Юниты:
  скрипт (цели latest/time, escape, cleanup auto.conf), сериализация,
  разбор backup_label, ListFulls-парсинг id.
- **Ф2. S3 + процесс**: `BackupS3.ListFullsAsync/DownloadTextAsync`
  (интеграция против testcontainers-MinIO, динамический порт);
  `RestoreProcess` (валидация, демонтаж, джоб+супервиз, rejoin,
  пост-обработка), DI + `ClusterProcesses`/`ReconcileLoop`-врезка,
  гварды исключения (NodeSupervisor/BucketEvacuator/BackupProcess/
  RemoveShard/Deprovisioning D1), `BackupPlanner.IsDue`-расширение.
  Интеграционные тесты (fake-docker/S3 — существующие паттерны FakeBackupDeps):
  happy-path фаз, идемпотентность повторных тиков, permanent-отказ по дыре,
  transient-ретрай, takeover (status+имена), cancelled-by-remove,
  D1-чистка restore-джобов, IsDue-кейсы.
- **Ф3. API**: `RestoreShardHandler` + роут/DTO/коды (202/400/409/404) в
  mTLS-грани; интеграционные WAF-тесты (существующие паттерны Api-тестов):
  гварды confirm/активной заявки/RFC3339, клэйм-независимая запись PLANNED.
- **Ф4. Панель**: парсер restore-ключей + правило `restore-failed`; юниты
  правил (снапшоты с FAILED/активным restore, кластер жив/удалён).
- **Ф5. E2E** (`E2eRestoreScenarios`, Release, `PGW_TEST_DOCKER=1`,
  окружение `E2eEnvironment` с `withMinio:true`, per-method изоляция —
  паттерн E2eBackupScenarios):
  - `Restore_Latest_RebuildsDestroyedShard`: кластер → INSERT контрольных
    строк → полный COMPLETED + wal-сегменты (INSERT-нагрузка/pg_switch_wal)
    → снос контейнеров+volume нод шарда (симуляция гибели) → POST restore
    latest → COMPLETED → SELECT-сверка строк; ключ wal удалён, новый полный
    появляется (инвариант цепочки), агент поднят (ACTIVE).
  - `Restore_TargetTime_PitrRollback`: данные T0 → бэкап → данные T1 →
    фиксируется `T_cut` → порча (DROP TABLE после T_cut) → restore
    target_time=T_cut → таблицы/строки на T_cut живы, порчи нет.
  - `Restore_NewCluster_FromSourcePrefix` (DR): кластер A с бэкапами →
    deprovision (etcd чист, S3 жив) → recreate кластера A (та же декларация)
    → Active → restore source = прежний префикс (source_cluster=A) →
    данные совпадают с моментом бэкапа.
  - Бюджеты короткие (≤300 c на фазу), порты только динамические, полная
    зачистка контура в teardown (контейнеры/сети/тома/etcd-префикс).
- **Ф6. Runbook + ручной стенд**: `docs/backup-restore.md`; ручной прогон
  трёх сценариев на dev-стенде (00-up.sh + deploy/) с фиксацией в задаче
  (команды/результаты SELECT-сверок).
- **Ф7. Мерж-гейт**: полный прогон юниты+интеграция+E2E на свежем Release
  (маркеры Restore_* зелёные), зачистка контейнеров/сетей после серий
  (`docker rm -f` + `network prune`), roadmap-гейт (снятие тега
  t05-backup-restore из arch/roadmap/backup.md тем же коммитом мержа).

## 5. Ограничения

- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`, LangVersion
  latest; пакеты — только `Directory.Packages.props` (новых пакетов нет).
- Порты docker в тестах — только динамические; никаких литералов-портов.
  Таймауты фикстур ≤ 100 с (BrokerBootSec-аналог), E2E-бюджеты фаз ≤ 300 с.
- Зачистка контейнеров и сетей между сериями обязательна (incl.
  `docker network prune -f`); каждый E2E/интеграционный тест полностью
  чистит за собой (teardown при любом исходе — AGENTS.md).
- Язык: документация/комментарии — русский; идентификаторы — английский.
  Тесты — AAA-комментариями.
- Не трогать: HA-контур живых (не restore) шардов, конфиги Patroni нод,
  префиксы чужих писателей; S3-объекты restore не удаляет никогда.
- Каноническая модель t01 (`BackupsParser`) — обратная совместимость:
  существующие ключи не меняются; restore-ключи добавляются толерантно.
- Локально собираемые образы в registry 192.168.0.1:5000 не класть; образ
  `pgworker-backup` не меняется (контракт дополнен документационно).

## 6. Критерии приёмки

1. **AC1 (restore latest, E2E)**: уничтоженный шард (контейнеры+volume)
   восстанавливается POST restore latest → статус COMPLETED → контрольные
   данные читаются SELECT'ом; ноды RUNNING; dsn/portalloc не изменились.
2. **AC2 (PITR, E2E)**: restore с `target_time` до порчи → данные на
   момент цели (порчи нет, данные до цели живы); `restored_to_lsn` в статусе.
3. **AC3 (DR, E2E)**: кластер deprovision'ed (etcd чист) → пересоздан →
   restore с source-override → данные восстановлены из бэкапов погибшего
   префикса.
4. **AC4 (пост-инвариант)**: после COMPLETED — ключ `wal` удалён, новый
   полный PLANNED/RUNNING появляется без участия оператора, агент
   возвращается в ACTIVE (E2E-проверка в AC1-сценарии).
5. **AC5 (API)**: 202 + PLANNED-ключ; 400 confirm-мисматч/RFC3339; 409 при
   активной заявке; 404 кластер/шард (WAF-интеграция).
6. **AC6 (гварды исключения)**: интеграционно — во время активного restore
   надзор не пересоздаёт ноды шарда, эвакуатор не забирает шард, планировщик
   полных скипает шард; remove-shard → FAILED cancelled-by-remove; D1/D2
   чистят restore-джобы и ключи.
7. **AC7 (permanent/transient)**: дыра WAL-цепочки → FAILED с границами;
   transient (docker/S3-отказ) → статус не меняется, ретраи тиками;
   рестарт воркера посреди restore → takeover продолжает с фазы.
8. **AC8 (панель)**: парсер читает restore-ключи без parseErrors; алерт
   `restore-failed` зажигается по FAILED живого кластера (юниты правил).
9. **AC9 (runbook + стенд)**: `docs/backup-restore.md` покрывает три
   сценария; ручной прогон на dev-стенде зафиксирован в задаче (критерий
   задачи «кластер уничтожается и поднимается из бэкапов с проверкой
   данных»).
10. **AC10 (мерж-гейт)**: полный прогон зелёный на свежем Release (маркеры
    Restore_*); зачистка после серий; roadmap-гейт снятия тега t05; правки
    канона Ф0 — в ветке, канон и код не расходятся.

## 7. Риски

| Риск | Митигация |
|---|---|
| Надзор/эвакуация вмешиваются в шард посреди restore (пустой Patroni поверх восстановления, ложная эвакуация) | гварды исключения по активному restore-ключу (§3.4) — снапшот префикса уже парсится циклом; интеграционные тесты AC6 |
| Restore-джоб на postgres:18 несовместим с данными Spilo (uid, layout, мажор) | layout/chown зафиксированы скриптом (§3.3); мажор PG совпадает (18); E2E — единственный честный прогон полного контура |
| Patroni не поднимает восстановленный PGDATA (ожидает initdb/чужой initialize) | HA-scope вычищен ДО rejoin (Д3-образец); идентифицирующая проба P2.2; scope чист — Patroni бутстрапится с существующих данных; E2E AC1 закрывает |
| Долгий накат (большая база/длинная цепочка) — оператор слеп | маркеры фаз в статусе (`phase`), бюджет `RecoveryTimeoutSec`, FAILED с причиной; резюмов нет — осознанно (канон §3.5) |
| `target_time` недостижим (раньше старта полного/позже конца WAL) | PG завершает recovery ошибкой / джоб ловит по бюджету → FAILED с причиной; непрерывность цепочки проверяется до демонтажа (PLANNED-валидация) |
| PITR-назад ломает сшиваемость WAL-цепочки (новая TLI с меньшими номерами) | сброс ключа `wal` + немедленный новый полный (инвариант планировщика §3.5); старые S3-объекты не трогаются |
| Ошибка оператора: restore не в тот шард | `confirm` = имя шарда (400); runbook проговаривает необратимость; максимум один активный restore на шард |
| Гонка «restore-заявка ставится, а клэйм у другого инстанса» | API пишет только PLANNED-ключ (без мутаций docker/кластера); исполняет держатель клэйма — конфликтной записи нет |
| Сеть: restore-джобу нужен S3 c docker-хоста ноды | джоб на docker-хосте целевой ноды (как t02), extra_hosts host-gateway; S3-endpoint per-install обязан быть достижим отовсюду (канон §10) |
| Восстановление на усыновлённом (object) шард — PGDATA-путь/volume чужие | guard: шард с object-нодами → permanent FAILED «restore усыновлённых шардов не поддерживается» (runbook: ручной путь); в E2E не покрывается |
