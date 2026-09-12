# Spec: t04-backup-verify — проверки полных бэкапов (pg_verifybackup + WAL-цепочка)

Дата: 2026-09-11
Канон: [arch/19-backups.md](../../../arch/19-backups.md) §2 (планировщик — валидность), §3 (LSN-разбор history при verify), §4 (etcd-контракт: `verify.interval_sec`, `verify.error`, алерт `backup-verify-failed`), §5 (layout S3 + механика verify), §8–§10; панельный контракт — [arch/adminpanel/02-etcd-contract.md](../../../arch/adminpanel/02-etcd-contract.md) §2.3.1. Все правки канона этой задачей внесены в Ф0 (см. §4). Roadmap: [`t04-backup-verify`](../../../arch/roadmap/backup.md); контекст — t02 (полные бэкапы), t03 (WAL-стрим) — в main.

## 1. Цель

Верификация полных бэкапов каждого шарда: целостность файлов
(`pg_verifybackup` по `backup_manifest`, SHA256) и полнота WAL-цепочки
общего префикса `wal/` от `wal_start_segment` бэкапа до точки бэкапа —
вердикт в etcd (`verify.state` OK/FAILED + время и причина), по политике —
периодическая перепроверка оставшихся бэкапов, критичный панельный алерт
при невалидном. Проваленный verify перестаёт считаться «свежестью» в
планировщике t02 — воркер сам переснимает полный.

Границы t04:

- ВХОДИТ: процесс воркера `BackupVerifyProcess` (выбор кандидатов,
  цепочечная проверка list-ами, ephemeral verify-джоб `pg_verifybackup`,
  супервиз, transient/permanent-семантика, периодика по политике),
  расширение `WalChain` (диапазон до точки бэкапа + строгий LSN-разбор
  `.history`), расширение S3-обёртки (list произвольного префикса, GET
  history-объекта), расширение модели/JSON/парсеров (`verify.error`,
  `verify.interval_sec`), валидность в планировщике t02 (`IsDue`/
  `BackoffPassed`), панельные парсер+правило `backup-verify-failed`
  (critical) и `backup-full-stale` на валидных, E2E-маркер.
- НЕ входит: восстановление/PITR (t05), ретенция и удаление бэкапов/WAL
  (t06), reconcile S3↔etcd и сироты (t07), UI-грань бэкапов (t08),
  API приёма policy (t06; policy-ключ — ручная запись etcdctl), проверка
  самих WAL-объектов по checksum (WAL вне manifest не имеет контрольных
  сумм — полнота цепочки по именам/LSN, целостность сегмента гарантирует
  только restore t05).

### Решения пользователя (зафиксированы вопросами)

| Вопрос | Решение |
|---|---|
| Полнота WAL-цепочки — до какой точки | До конца бэкапного набора: непрерывность `wal/` от `wal_start_segment` до последнего сегмента набора `-X stream` этого бэкапа (точка бэкапа). Дыры ПОЗЖЕ бэкапа — домен wal-статуса t03 (DEGRADED), verify конкретного полного не портят |
| Периодическая перепроверка | Да: дефолт 7 дней (604800 c); `verify.interval_sec` в policy-ключе кластера перекрывает конфиг-дефолт `PgWorker:Backups:Policy:VerifyIntervalSec` (паттерн `full_max_age_sec`) |
| Полный LSN-разбор `.history` | Да, в verify: воркер скачивает history-объекты и строго валидирует TLI-переходы по точке переключения (`switchWALLSN`); runtime-контроль t03 остаётся на эвристике номеров |
| Панельные алерты | Одно правило `backup-verify-failed` (critical) на невалидный полный (текст — `verify.error`); без правила «нет валидного за окно» (YAGNI до t06) |
| Влияет ли verify на «свежесть» | Да: планировщик t02 и панельный `backup-full-stale` считают «последний валидный» = COMPLETED с `verify ≠ FAILED` (OK, PENDING «идёт», без verify); проваленный полный свежестью не считается → переснятие по бэкоффу |

## 2. Принципы

1. **arch-first**: канон arch/19 — источник правды; правки (§2/§3/§4/§5/
   §8/§9/§10 + adminpanel/02 §2.3.1) внесены в этой задаче ДО кода;
   расхождения вскрываются — канон правится тем же dev-flow.
2. **PgWorker — единственный писатель** `/pgworker/backups/*` (клэйм
   `<C>`); verify-джоб etcd не касается (stdout result-JSON + exit-код —
   протокол t02); панель читает и вычисляет алерты.
3. **Истина — факты**: verify-цепочка строится list-ом S3 и содержимым
   `.history` (без скачивания сегментов); «идёт проверка» выводится из
   наличия контейнера по детерминированному имени (takeover-совместимо,
   без новых полей контракта).
4. **Transient ≠ permanent**: сбой скачивания (S3/сеть/staging ENOSPC) —
   transient (статус остаётся PENDING, ретраи тиками); ненулевой
   `pg_verifybackup` и дыра цепочки — permanent FAILED (данные плохие).
   Образец — семантика джоба t02 (exit-код — истина итога).
5. **Verify идемпотентен и безопасен**: перезапуск джоба ничего не меняет
   в S3 (только чтение), vanished-джоб перезапускается (PENDING) —
   переснятие полного не требуется (в отличие от самого бэкапа).
6. **Паттерны arch/17/14**: идемпотентность каждого шага,
   journal-before-manipulations (этапы verify — в журнал `/pgworker/work/
   <C>`, op=`backup-verify`), ошибка шарда не роняет остальные, максимум
   один verify-джоб на шард одновременно.

## 3. Структура/компоненты

### 3.1. Механика verify (две проверки — один вердикт)

Кандидат на проверку — полный со статусом `COMPLETED` (ключ
`/pgworker/backups/<C>/<X>/full/<id>`):

- **due-on-create**: `verify=PENDING` (проставляет t02 при COMPLETED,
  если `verify.on_create=true` — уже в коде);
- **due-periodic**: `verify.state=OK` и `now − checked_unix >
  verify.interval_sec`, либо `verify` отсутствует вовсе (никогда не
  проверялся; `on_create=false` не отключает периодику);
- `verify.state=FAILED` — терминален: не перепроверяется (алерт горит,
  лечение — переснятие; runbook-разбор — t05, удаление — t06).

Порядок проверок одного кандидата (тиковая машина §3.3):

1. **Цепочка (воркер, без контейнера)**: list `wal/` + list
   `full/<id>/pg_wal/` → `WalChain.CheckRange(wal_start_segment,
   последний сегмент набора, объекты wal/, содержимое history из
   диапазона)` (§3.4). Дыра → `verify=FAILED` + error с границами —
   джоб checksums НЕ запускаем (вердикт уже определён, экономим
   скачивание). Transient list/GET → шард-skip тиком (статус не трогаем).
2. **Checksums (verify-джоб)**: цепочка цела → ephemeral контейнер
   `pgw-backup-verify-<C>-<X>-<id>`: `mc cp` `full/<id>/` 1:1 в staging →
   `pg_verifybackup` (manifest SHA256, вкл. `pg_wal/` набора) →
   result-JSON в stdout + exit-код. exit 0 → `verify=OK` + checked_unix;
   exit ≠ 0 c `phase=verify` → permanent `FAILED` + error; `phase=
   download` (mc/staging) → transient: контейнер снести, остаёмся в
   PENDING, ретрай следующим тиком.

Итог пишется в существующее поле статуса полного
(`verify{state,checked_unix,error}` — расширение контракта Ф0);
`checked_unix` пишется и при OK, и при FAILED.

### 3.2. Verify-джоб (инлайн-команда, контракт образа §2)

`VerifyJobCommand` (паттерн `WalAgentCommand` t03 — inline bash, поведение
версионируется кодом воркера, образ не пересобирается):

- env (ПРЕФИКС тот же `PGW_BK_*`, что у джоба t02): `PGW_BK_S3_ENDPOINT/
  REGION/BUCKET/ACCESS_KEY/SECRET_KEY`, `PGW_BK_PREFIX=<C>/<X>`,
  `PGW_BK_ID`, `PGW_BK_STAGING_DIR`; креды — только env (не argv/ps);
  `MC_HOST_pgw` собирёт воркер (образец entrypoint t02).
- скрипт: `mc cp --recursive pgw/$BUCKET/$PREFIX/full/$ID/ $STAGING/full/`
  (fail → `{"ok":false,"phase":"download","error":...}`, exit 1) →
  `pg_verifybackup "$STAGING/full"` (fail → `{"ok":false,"phase":"verify",
  "error":...}`, exit 1; вывод утилиты — stderr, result-JSON — stdout,
  однострочный error без кавычек — образец FAIL() entrypoint t02) →
  успех: `{"ok":true}`.
- Контейнер: образ `Backups:Job.Image` (тот же `pgworker-backup` —
  `pg_verifybackup` входит в postgres:18); `Cmd` — полная замена
  ENTRYPOINT (контракт ContainerSpec); портов нет; staging-volume
  `pgw-backup-verify-<C>-<X>-<id>` (квота `Staging:QuotaBytes` → tmpfs,
  ENOSPC → download-phase transient); лимиты `Agent { Cpu, Mem }`;
  `restart-policy no`; extra_hosts — как у джоба t02 (S3 per-install
  достижим отовсюду, канон §10).
- Docker-хост — хост ноды-источника полного (`node`-факт статуса,
  portalloc; staging-квоты рядом с данным); узел исчез из portalloc →
  первый engine таблицы `Docker:Hosts` (journal-факт выбора).
- Супервиз (воркер, по детерминированному имени — takeover-совместимо):
  running → ждать следующих тиков; exited → итог по exit-коду + логу
  (парс `BackupJobLog`-образцом); нет контейнера при PENDING → запуск
  заново со шага цепочки (идемпотентно). После итога — контейнер и volume
  сносятся (как CleanupJob t02).

### 3.3. Процесс `BackupVerifyProcess` (новый, `src/PgWorker.Backups`)

Тик под клэймом `<C>` (вход — снапшот кластера + `ClusterBackups`, как
`BackupProcess`). Guard'ы: клэйм наш; `Backups:Enabled=true` (иначе
no-op — джобы не стартуют, идущие не убиваются: ephemeral, умрут сами);
кластер Active. Для каждого шарда снапшота (dsn не нужен — verify чисто
по S3; шард без ключей полных пропускается) — независимо:

1. Живой verify-контейнер шарда (по префиксу имени
   `pgw-backup-verify-<C>-<X>-`) → супервиз §3.2, новых не стартуем
   (инвариант: максимум один verify-джоб на шард).
2. Кандидат due (§3.1) — младший по due (PENDING-очередь раньше
   периодики; несколько due — по одному за тик на шард).
3. Цепочечная проверка → джоб (§3.1). Этапы — журнал
   (`started/verified-ok/verify-failed/...`, op=`backup-verify`).
4. Статус — put в ключ полного (полная перезапись JSON статуса:
   `BackupStatusJson.Serialize` с расширенным verify).

Wiring: `IClusterProcesses.VerifyBackupsAsync(...)` после
`WalStreamAsync`, до `repair` (не блокирует тик: запуск — create+start,
поллинг — следующим тиком); префикс `/pgworker/backups/` уже читается
циклом (t02/t03). Deprovisioning D1 (чистка джоб-контейнеров кластера)
дополняется префиксом `pgw-backup-verify-<C>-*` (+volume-префикс) —
образец существующих `JobContainerPrefix`/`JobVolumePrefix`.

### 3.4. Ядро цепочки (расширение `Model/`)

- **`WalChain.CheckRange(start, end, objects, historyContents)`** —
  непрерывность `[start..end]`: существующий `Check` идёт от start по
  Next(); расширение — стоп на `end` (сегмент `end` обязан присутствовать
  — он из листинга `full/<id>/pg_wal/`; отсутствие объекта `wal/<end>`
  при наличии в наборе = дыра дублирования t02). TLI-переходы в диапазоне
  — строго по LSN (см. ниже), при отсутствии содержимого history —
  фолбэк на эвристику t03 (совместимость сигнатур: старый `Check` —
  частный случай).
- **`WalHistory` (парсер `.history`)**: файл = строки
  `<parentTLI> <switchWALLSN> [<reason>]`; LSN `<hex>/<hex>` → позиция
  сегмента. Валидация перехода `old→new`: history-файл `new` существует,
  его parent == old, `switchWALLSN` лежит в границах сегмента первого
  сегмента нового TLI (или предыдущего — точка внутри сегмента);
  расхождение → дыра с границами.
- **`BackupS3`**: `ListAsync(cluster, shard, prefix)` — обобщение
  `ListWalAsync` (пагинация list-v2, тот же паттерн; `wal/` и
  `full/<id>/pg_wal/`); `GetObjectAsync(cluster, shard, key)` —
  скачивание history-файлов (крошечные) для строгого разбора.

### 3.5. Планировщик t02 — валидность (правка `BackupPlanner`)

- `IsDue`: «последний валидный» = max finished_unix среди COMPLETED с
  `verify == null | PENDING | OK`; валидных нет или возраст > окна → due.
- `BackoffPassed`: n = попытки после последнего валидного = FAILED-джобы
  + COMPLETED с `verify=FAILED` (бэкофф растёт и от провалов verify —
  переснятие не молотит).
- Панельный аналог — §3.6; `SuperviseActiveAsync` t02 не меняется
  (PENDING уже проставляется при `VerifyOnCreate`).

### 3.6. Панель AdminPanel

- **Парсер** (`AdminPanel.Etcd/Parsing/BackupsParser`): из full-ключей
  читать `verify{state,checked_unix?,error?}`; `ShardLastCompletedUnix`
  становится «последний валидный» (COMPLETED с `verify ≠ FAILED`);
  добавляется `ShardVerifyFailures: shard → {id, error, checked_unix}`
  (последний FAILED по checked_unix; для текста алерта). Битые значения —
  parseErrors + пропуск (паттерн панели).
- **Правило `BackupVerifyFailedRule`** (`backup-verify-failed`,
  critical, per-shard): в статусе любого COMPLETED-полного шарда
  `verify.state=FAILED` → алерт с текстом `verify.error` (провал
  checksums / дыра цепочки с границами); remedy — «воркер переснимает
  автоматически; для разбора — runbook t05»; паттерн
  `BackupFullStaleRule`.
- **`BackupFullStaleRule`**: код не меняется (вход уже «валидные»);
  обновляются юниты (FAILED-verify свежий полный больше не гасит алерт).
- Модель `ClusterBackupsInfo` расширяется (`ShardVerifyFailures`),
  `BackupsInfo`/UI — без изменений (UI — t08).

### 3.7. Конфигурация и модель

- `BackupsRuntimeOptions` + `VerifyIntervalSec=604800`; options-маппер
  App (`PgWorker:Backups:Policy:VerifyIntervalSec`); per-cluster
  policy-ключ: `verify.interval_sec` перекрывает (резолв в процессе:
  `policy.Verify.IntervalSec ?? options.VerifyIntervalSec`; `interval_sec
  <= 0` — периодика выключена, on_create работает).
- Модель воркера (`PgWorker.Etcd/Parsing/BackupsModel`): `BackupVerify` +
  `Error`; `BackupPolicy` + `VerifyIntervalSec` (парсер: отсутствие
  `interval_sec` в policy-ключе → дефолт конфига, не null-политика).
  `BackupStatusJson`: писать `verify.error` (опционально, как остальные
  nullable-поля).
- Требование `wal_start_segment` у кандидата: заполняется с UPLOADING
  (t02); COMPLETED без него (аномалия) → permanent FAILED `verify:
  "нет wal_start_segment"` без джоба.

### 3.8. Наблюдаемость

Логи воркера: старт/итог verify-джоба, дыры цепочки с границами,
transient-ретраи. Всё значимое — в verify-статусе ключа (панель/оператор)
+ журнал op=`backup-verify`. Метрика по образцу t03
(`verifyObserver`): counter `pgworker_backup_verify_total{result=ok|
failed|transient}` — минимально, без gauge-зоопарка.

## 4. Фазы

- **Ф0. Канон (выполнено в этой spec-фазе)**: arch/19 §2 (валидность в
  планировщике/бэкоффе), §3 (LSN-разбор history при verify; runtime —
  эвристика), §4 (policy `verify.interval_sec`, verify-поле `error`,
  суточный алерт на валидных, алерт `backup-verify-failed`), §5 (механика
  verify: джоб + цепочка + transient/permanent), §8 (карта), §9
  (`Policy.VerifyIntervalSec`), §10 (риски); adminpanel/02 §2.3.1.
- **Ф1. Модель/ядро**: `BackupVerify.Error` + `VerifyIntervalSec`
  (модель, JSON-сериализация, парсеры воркера), `WalHistory` (LSN-парсер),
  `WalChain.CheckRange`, `VerifyJobCommand`; юниты (разбор history
  валидный/невалидный switchLSN, диапазон: цель/дыра до end/отсутствие
  end-объекта в wal/, fallback-эвристика; команда джоба — env-контракт).
- **Ф2. S3**: `ListAsync(prefix)`, `GetObjectAsync`; интеграция против
  MinIO-testcontainer (динамический порт; list пагинация/GET history).
- **Ф3. Процесс**: `BackupVerifyProcess` (машина тика: due-резолв,
  цепочка, джоб, супервиз, transient/permanent, периодика), wiring
  `ClusterProcesses`/`ReconcileLoop`/DI, D1-префиксы verify-контейнеров;
  интеграция с fake-docker/fake-S3 (паттерны `FakeBackupDeps`): on_create
  happy-path, порча download-phase → transient PENDING, порча
  verify-phase → FAILED, дыра цепочки → FAILED без джоба, периодика по
  interval, vanished-джоб → перезапуск, инвариант одного джоба на шард.
- **Ф4. Планировщик t02**: `IsDue`/`BackoffPassed` на валидных; юниты
  (`BackupPlannerTests`): FAILED-verify свежий полный → due; бэкофф
  считает verify-фейлы.
- **Ф5. Панель**: парсер verify-полей + валидная свежесть;
  `BackupVerifyFailedRule`; юниты (парсер, правило: FAILED → critical с
  error-текстом; stale на валидных).
- **Ф6. E2E** (Release, `PGW_TEST_DOCKER=1`, per-Fact окружение
  `E2eEnvironment` с MinIO + образ `pgworker-backup:e2e`): маркер
  `Backup_Verify_Ok_OnCreate` — кластер → COMPLETED → verify PENDING →
  джоб → `verify.state=OK` (+`checked_unix`) в etcd; сценарий порчи —
  удалить объект из `full/<id>/` в MinIO → policy `interval_sec` мал →
  перепроверка → `verify.state=FAILED` + `error` (без переснятия в окне
  теста). Мерж-гейт: маркер зелёный на свежем Release (AGENTS.md).
- **Ф7. Мерж-гейт**: полный прогон юниты→интеграция→E2E; зачистка
  контейнеров/сетей после КАЖДОЙ серии (`docker rm -f` +
  `network prune -f`); roadmap-гейт — снятие тега `t04-backup-verify` из
  `arch/roadmap/backup.md` тем же мерж-коммитом.

## 5. Ограничения

- .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`; версии
  пакетов — `Directory.Packages.props` (новых пакетов не планируется:
  AWSSDK.S3 уже подключён).
- Порты docker в тестах — только динамические (`assignRandomHostPort:
  true`/`GetMappedPublicPort`); никаких литералов-портов. Таймауты
  фикстур ≤ 100 с; E2E-бюджеты короткие (форсирование политикой
  `interval_sec`, не ожиданием).
- Зачистка тестовых контейнеров/сетей между сериями обязательна
  (`docker rm -f` + `docker network prune -f`); каждый E2E/интеграционный
  тест полностью чистит за собой (teardown при любом исходе; проверка
  чистоты — ассерт).
- Язык: документация/комментарии — русский; идентификаторы — английский;
  тесты — AAA-комментарии.
- Не трогать: HA-контур нод, конфиги Patroni, префиксы чужих писателей;
  S3-объекты verify только читает (никаких удалений/перезаписей — R4);
  контракт полных/WAL-ключей — только расширение `verify.error`/
  `interval_sec` (обратная совместимость: старые записи без полей
  парсятся).
- Verify-джоб не публикует портов; S3-креды — только env (не argv, не в
  статусы/логи); в `registry 192.168.0.1:5000` локальные образы не
  класть.
- FAILED-verify терминален (не перепроверяется автоматически) — осознанное
  решение: повторная проверка — переснятие/ручной runbook; пересмотр —
  t06/t07.

## 6. Критерии приёмки

1. **AC1 (on_create)**: E2E (Release): COMPLETED полный при
   `verify.on_create=true` → `verify.state=PENDING` → verify-джоб
   `pgw-backup-verify-*` → `verify.state=OK` + `checked_unix`; воркерный
   и панельный парсеры читают без parseErrors.
2. **AC2 (checksums-порча)**: интеграция/E2E: удалённый/битый объект в
   `full/<id>/` (MinIO) → перепроверка по `interval_sec` →
   `verify.state=FAILED`, `error` содержит причину `pg_verifybackup`;
   download-сбой (недоступный S3) → статус остаётся PENDING (transient),
   ретраи.
3. **AC3 (цепочка)**: юниты `CheckRange`: непрерывность `[start..end]`,
   дыра внутри → FAILED c границами, отсутствие `wal/<end>` → FAILED;
   интеграция: дыра в `wal/` → FAILED без запуска джоба (контейнер не
   создаётся).
4. **AC4 (TLI строго)**: юниты `WalHistory`: валидный переход
   (switchWALLN в границах), невалидная точка переключения → FAILED;
   verify использует содержимое history (GET из S3).
5. **AC5 (периодика)**: интеграция: policy `verify.interval_sec` мал →
   OK-полный перепроверяется (`checked_unix` растёт); FAILED не
   перепроверяется; `interval_sec<=0` — только on_create.
6. **AC6 (планировщик)**: юниты `BackupPlanner`: COMPLETED+verify FAILED
   не даёт свежести → `IsDue=true`; бэкофф n растёт от verify-фейлов;
   панельный `backup-full-stale` (юниты) горит при свежем-но-битом
   последнем полном.
7. **AC7 (алерт)**: юниты правила: `verify.state=FAILED` →
   `backup-verify-failed` critical с текстом `verify.error`; без FAILED —
   молчит; пустой префикс — молчит.
8. **AC8 (takeover/супервиз)**: интеграция: vanished verify-джоб →
   повторный запуск (PENDING), итог корректен; два due-кандидата —
   проверяются по одному за тик; deprovisioning чистит
   verify-контейнеры/volumes.
9. **AC9 (канон/мерж-гейт)**: полный прогон зелёный на свежем Release с
   маркером `Backup_Verify_Ok_OnCreate`; зачистка серий; канон-правки Ф0
   в ветке; тег t04 снят из roadmap мерж-коммитом.

## 7. Риски

| Риск | Митигация |
|---|---|
| Стоимость verify: скачивание полного из S3 (трафик + staging размером бэкапа) | `interval_sec` 7 дней по умолчанию; on_create — один раз на бэкап; ephemeral джоб (ресурс занят на время); цепочечная часть — list-ами без скачивания |
| Staging-квота verify = размер бэкапа (ENOSPC на больших базах) | ENOSPC → download-phase transient (ретраи, не FAILED); оператору — `Staging:QuotaBytes`/лимиты §6 канона; поведение задокументировано |
| Дыра цепочки «после бэкапа» не видна verify (проверяем до точки бэкапа) | Осознанная семантика (решение пользователя): целостность дальше — wal-статус t03 (DEGRADED) и периодика новых полных |
| Список `wal/` растёт с длиной цепочки (тысячами объектов) | list-v2 пагинация (уже t03); диапазон `[start..end]` позволяет игнорировать объекты ниже start (сортировка имён); t06 сдвинет старт ретенцией |
| Гонка: агент t03 грузит сегменты, verify листит «дыру» сегмента в полёте | Проверяемый диапазон закрыт набором бэкапа (сегменты которого уже в `wal/` — дублирование t02 до COMPLETED); сегменты выше точки бэкапа в CheckRange не входят |
| FAILED-терминальность прячет «самоизлечившийся» бэкап | Осознанно (решение пользователя): алерт + runbook; переснятие дешевле ручной реанимации объекта; пересмотр в t06/t07 |
| Ложный FAILED при аномалии статуса (нет `wal_start_segment`) | Явный permanent-вердикт «нет wal_start_segment» без джоба; t02 заполняет поле с UPLOADING — случай экзотический (ранний упавший UPLOADING не COMPLETED) |
| Длинные имена `pgw-backup-verify-<C>-<X>-<id>` | Тот же класс имён, что pgw-backup-full t02 (живёт); ограничения имён — канон §10 arch/19 |
| Расхождение spec↔arch при будущих правках | arch-правки всегда первой фазой; ревью plan↔spec по чек-листам dev-flow |
