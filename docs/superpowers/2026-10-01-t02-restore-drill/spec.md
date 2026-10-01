# t02-restore-drill — плановое тестовое восстановление по расписанию

- **Дата**: 2026-10-01
- **Roadmap**: [`arch/roadmap/reliability.md`](../../../arch/roadmap/reliability.md), тег `t02-restore-drill` (P1, влияние D — отсутствие потери данных; мерж-гейт трека: тег снимается из roadmap И строка в [`reliability-report.md`](../../../arch/roadmap/reliability-report.md) обновляется тем же мерж-коммитом). Не путать с `t02-backup-full-daily` из карты arch/19 §8 — это задача ДРУГОЙ серии (сквозная надёжность).
- **Worktree**: `/Users/demakaev/ZCodeProject/worktrees/t02-restore-drill` (ветка `t02-restore-drill`); правки — только здесь.
- **Тип**: доработка подсистемы бэкапов — новый фоновый процесс воркера + ephemeral drill-джоб + ключ etcd + панельные алерты/грань + runbook + E2E.
- **Канон (обновляется этой задачей, arch-first, до кода)**: [`arch/19-backups.md`](../../../arch/19-backups.md) — новый §3.6 «Дрилл восстановимости», строка ключа `drill` и поле `drill.interval_days` policy в §4, секция конфигурации в §9, риски в §10; панельная проекция — [`arch/adminpanel/02-etcd-contract.md`](../../../arch/adminpanel/02-etcd-contract.md) §2.3.1 (строка ключа `drill`), §9.x (мутация policy-поля из панели); тело policy API — [arch/14-pgworker.md](../../../arch/14-pgworker.md) §1.1 (BackupsPolicyHandler расширяется). Правки arch/ выполняются на Фазе 6 dev-flow по плану (в spec-фазе — только проект, §3.8).
- **Образцы**: `BackupVerifyProcess` (`src/PgWorker.Backups/Process/BackupVerifyProcess.cs`, t04 — тиковая машина проверки + ephemeral джоб + супервиз по имени); `RestoreJobCommand`/`RestoreJobSpec`/`RestoreProcess` (`src/PgWorker.Backups/Restore/`, t05 — механика разворачивания full+WAL в изолированном контейнере); расписание — `RetentionProcess` (`_lastPassUnix` + IntervalSec); панельные правила — `BackupVerifyFailedRule`/`RestoreFailedRule`; приём policy — `BackupsPolicyHandler` (PUT, замещение целиком).
- **Решения пользователя (гейты уточнений, зафиксированы)**: (1) исполнитель — **процесс воркера + ephemeral drill-джоб** (новый `RestoreDrillProcess` по образцу существующих тиковых машин + контейнер `pgw-backup-drill-*`); (2) выбор шарда — **один шард за проход, наименее свежий по дриллу** (покрытие всех шардов по кругу без списков конфигурации); (3) расписание — **дефолт 1 сутки, период редактируется из панели числом в сутках** (per-cluster policy-поле `drill.interval_days`, 0 — выключено); (4) состав контрольных данных — **«старт + выход из recovery»** (2026-10-01, гейт ревью spec): checking-фазы и SQL-пробы НЕ входят — дрилл доказывает разворачиваемость (postgres стартовал на восстановленном PGDATA и вышел из recovery), целостность файлов остаётся за verify t04; сверка счётчиков строк с продом не имеет смысла — прод уходит вперёд точки бэкапа; (5) изоляция — **изолированный дизайн остаётся** (2026-10-01, гейт ревью): дрилл не создаёт нод в кластере, HA-scope и synchronous-режим мастера не затрагивает; все состояния дрилла — **в etcd** («что произошло / что происходит / куда двигаться»): снос тестового контура — видимая доводимая фаза `cleaning`, переживает рестарт воркера.

## 1. Цель

Восстановимость бэкапов доказывается регулярно и автоматически, а не только
E2E-тестами dev-контура. Сегодня: verify t04 проверяет целостность файлов
(манифест SHA256 + непрерывность WAL-цепочки), но НЕ разворачивание; реальный
restore — разовая деструктивная заявка оператора (arch/19 §3.5: демонтаж
шарда). Прод-бэкап фактически не разворачивался никогда: связка
«скачать полный → накатить WAL → выйти из recovery» проверяется только тестами.

Дрилл — периодический автоматический прогон НЕдеструктивного
восстановления: берётся бэкап реального шарда (тот же кандидат, что выбрал бы
DR-restore), разворачивается во временный изолированный контур (ephemeral
контейнер + volume, без сети), контролируется исход разворачивания — postgres
стартовал и вышел из recovery (решение пользователя п.4), контур сносится.
Провал дрилла или его молчаливая пропажа — алерт панели: восстановимость не
доказана — прямая характеристика D.

Функциональные результаты:

1. **RestoreDrillProcess** — тиковая машина воркера под клэймом `<C>`:
   по расписанию выбирает ОДИН шард кластера (наименее свежий по последнему
   дриллу), запускает drill-джоб, супервизит, фиксирует исход в etcd, сносит
   контур при любом исходе.
2. **Drill-джоб** — ephemeral контейнер `pgw-backup-drill-<C>-<X>-<id>` (образ
   `pgworker-backup`, контракт arch/19 §2): скачать `full/<id>/` + накат
   WAL-цепочки до latest + локальный postgres на unix-socket + поллинг выхода
   из recovery (механика restore-джоба t05) — в СВОЙ ephemeral volume,
   прод-шард не трогается вовсе.
3. **Контракт etcd**: ключ `/pgworker/backups/<C>/<X>/drill` — состояние
   последнего/текущего дрилла шарда, включая доводимую фазу сноса контура
   (`cleaning`); пишет ТОЛЬКО воркер под клэймом `<C>`; per-cluster policy
   расширяется полем `drill.interval_days`.
4. **Панель**: алерты `backup-drill-failed` (critical — последний дрилл
   провалился) и `backup-drill-stale` (warning — успешного дрилла нет/он
   просрочен при включённой подсистеме); статус последнего дрилла per-shard и
   поле «интервал, суток» (мутация через панельную команду-прокси в
   policy-API воркера).
5. **Runbook**: раздел «дрилл» в `docs/backup-restore.md` — как читать
   статус, что делать при провале, как отключить/перенастроить.
6. **Тесты**: юниты чистых функций, интеграции (реальный etcd + FakeS3 +
   FakeBackupEngine), панельные юниты, docker-E2E кейсы «дрилл успешен +
   контур чист» и «дрилл провален → алерт».

## 2. Принципы

1. **Arch-first**: контракт (`arch/19` §3.6/§4/§9/§10, `adminpanel/02` §2.3.1,
   `arch/14` §1.1) обновляется ДО кода (Фаза 6 по плану); код зеркалит канон.
2. **Дрилл = НЕдеструктивный прогон DR-пути**: кандидат бэкапа и валидация —
   те же, что у restore-заявки t05 (новейший COMPLETED с манифестом +
   непрерывность WalChain от `wal_start_segment`): дрилл доказывает ровно
   тот путь, который сработает при реальном DR, не более удобный суррогат.
3. **Контроль = «старт + выход из recovery»** (решение пользователя п.4):
   дрилл проверяет разворачиваемость и только; целостность файлов — verify
   t04, логика данных — оператор. Механика проверки — сам факт: локальный
   postgres стартовал на восстановленном PGDATA и достиг конца WAL
   (`pg_is_in_recovery() = false`).
4. **Изоляция временному контуру** (решение пользователя п.5): drill-джоб
   пишет ТОЛЬКО в свой ephemeral volume `pgw-backup-drill-<C>-<X>-<id>`,
   поднимает postgres БЕЗ сети (`listen_addresses=''`, unix-socket —
   механика t05), не публикует портов, не подключается к per-cluster сетям.
   Дрилл НЕ создаёт и НЕ добавляет нод в кластер, не входит в per-cluster
   сеть и HA-scope: тестовой ноды в кластере нет ПО ПОСТРОЕНИЮ —
   `synchronous_mode`/`synchronous_mode_strict` мастера (Patroni) не
   затрагивается, контуру нечего «отключать на мастере». Прод-ноды,
   HA-scope, WAL-агент, слоты — не трогаются. S3 — только чтение (list +
   скачивание).
5. **Снос при любом исходе — доводимая фаза в etcd** (решение пользователя
   п.5): терминальный исход (успех/провал/таймаут) ⇒ снос тестового контура
   как журналируемая доводимая фаза — journal `drill-cleanup/<X>/<id>` ДО
   rm, в ключе `phase:"cleaning"` («что происходит»), после
   подтверждённого удаления контейнера И volume — `phase` снимается: ключ =
   чистый терминальный итог («что произошло»), journal done. Снос переживает
   рестарт воркера: тик находит терминальный ключ с `phase:"cleaning"` ⇒
   идемпотентно доводит по детерминированным именам. «Куда двигаться дальше»
   — `error` ключа + Hint/Remedy панельных алертов; новых полей статуса без
   нужды не заводится. Осиротевших контейнеров/сетей дрилл не оставляет:
   сетей не создаёт вовсе; transient docker на сносе — статус уже
   терминальный, следующий тик повторяет rm.
6. **Паттерны arch/17**: идемпотентность каждого шага (повторный тик по
   RUNNING-ключу = супервиз существующего джоба; доводка cleaning — та же
   идемпотентность), journal-before-manipulations (RUNNING-статус до create
   контейнера; `drill-cleanup` в journal ДО rm контейнера/volume),
   transient vs permanent (S3/docker-транспорт — transient, статус не
   меняем; провал recovery/бюджет — permanent-FAILED).
7. **Single-writer**: префикс `/pgworker/backups/*` пишет только держатель
   клэйма `<C>`; панель читает снапшот, алерты — вычисления панели (воркер
   алертов не пишет — канон §4). Мутация интервала — через policy-API
   воркера (панель — прокси, в etcd не пишет).
8. **Takeover**: всё состояние дрилла — в etcd-ключе + детерминированные
   имена контейнера/volume — новый инстанс продолжает супервиз и доводку
   сноса; статус RUNNING без контейнера (возраст > бюджета) → FAILED,
   доводка сноса best-effort.
9. **Ресурсная дисциплина**: один drill-джоб на шард (один ключ = одна
   активность), один запуск за проход кластера; лимиты контейнера — те же
   `Agent { Cpu, Mem }`, что у остальных джобов бэкапов.

## 3. Структура и компоненты

### 3.1. Контракт etcd (канон arch/19 §4; отражение — код)

| Ключ | Значение | Писатель |
|---|---|---|
| `/pgworker/backups/<C>/<X>/drill` | состояние последнего/текущего дрилла шарда: `{"state":"RUNNING\|SUCCEEDED\|FAILED","id","backup_id","started_unix","finished_unix"?,"phase"?,"restored_to_lsn"?,"error"?}`; `id` — как у полных (`YYYYMMDDHHMMSSZ`, BackupPlanner.NextId); `phase` — `downloading\|recovering` (фазы джоба) \| `cleaning` (идёт снос тестового контура после терминального исхода); снятый `phase` у терминального ключа = контейнер и volume подтверждённо удалены, чистый итог; по ключу видно «что происходит / что произошло» — снос контура не «забывается»; ключ ПЕРЕЗАПИСЫВАЕТСЯ каждым новым дриллом (история — фазы журнала воркера `backup-drill/<X>/<id>`); ключ один на шард — «максимум один активный дрилл» следует из формата | воркер (держатель клэйма `<C>`) |
| `/pgworker/backups/<C>/policy` (расширение) | новое поле `{"drill":{"interval_days":N}}`: период дрилов шардов кластера в сутках; `N=0` — дрилл кластера выключен; отсутствует → дефолт конфига `Drill:IntervalDays` (§3.2). Остальные поля policy неизменны | policy-API воркера (BackupsPolicyHandler, приём от панели/оператора) |

Гигиена ключа: отдельной чистки нет — ключ перезаписывается на месте,
история не копится в etcd. Deprovisioning D2 (`del --prefix
/pgworker/backups/<C>/`) сносит ключ автоматически (per-cluster префикс);
D1 добавляет префикс контейнеров `pgw-backup-drill-<C>-` в чистку джобов
бэкапов (рядом с `pgw-backup-full-`/`verify`/`restore`).

Backwards-compat: панельный парсер толерантен к отсутствию drill-ключей
(подсистема не включена / дриллов не было) — правила дрилла молчат; JSON
без новых полей (`drill` в policy) парсится как «дефолт интервала».

### 3.2. Конфигурация (канон arch/19 §9)

Новая секция опций `BackupsRuntimeOptions` (склейка `PgWorker:Backups` в
`Program.cs`):

- `DrillIntervalDays = 1` — дефолт периода дрилов (суток) для кластеров без
  policy-поля; `0` — глобальное выключение дрилов (новых запусков нет,
  активный доводится до терминального исхода и снос доводится);
- `DrillTimeoutSec = 21600` — бюджет активного дрилла (от `started_unix`):
  исчерпан ⇒ FAILED `drill-timeout` + kill+rm контейнера/volume через
  cleaning-фазу сноса (§3.3 п.2; по образцу JobRestoreTimeoutSec;
  REJOINING-фаз у дрилла нет — после джоба терминальный исход сразу);
- бюджет наката WAL внутри джоба — переиспользуется
  `RestoreRecoveryTimeoutSec` (1800; drill-джоб получает его тем же env);
- лимиты контейнера — существующие `Agent { Cpu, Mem }` (как restore-джоб).

Валидация старта: отрицательные `DrillIntervalDays`/`DrillTimeoutSec` —
fail-fast (как остальные таймауты §9).

### 3.3. RestoreDrillProcess (`src/PgWorker.Backups/Process/RestoreDrillProcess.cs`)

Тиковая машина под клэймом `<C>` (Op = `backup-drill`), вызов — из
ReconcileLoop (§3.5). Шаги тика:

1. **Гварды**: клэйм наш; `Enabled=false` → только доводка активного дрилла
   и сноса (§3.5, стоп-семантика) без новых запусков; кластер Active.
2. **Супервиз активного и доводка сноса** (по одному за тик, старейший
   `started_unix`): ключ `drill` в состоянии RUNNING ⇒ супервиз джоба;
   терминальный ключ (SUCCEEDED/FAILED) с `phase:"cleaning"` ⇒ доводка
   сноса (краш-рекавери, последний подпункт):
   - возраст RUNNING-ключа `started_unix` > `DrillTimeoutSec` ⇒ исход
     FAILED `drill-timeout: <age> с > DrillTimeoutSec` (kill+rm — в сносе
     ниже);
   - контейнер running ⇒ фаза из логов (`downloading|recovering` —
     протокол t02) в статус при изменении; ждём;
   - контейнер exited ⇒ exit-код 0 + result-JSON `{"ok":true,…}` ⇒ исход
     SUCCEEDED (+`finished_unix`, `restored_to_lsn`); иначе FAILED (+`error`
     из result-JSON или `exit <N>`);
   - контейнера нет у RUNNING-ключа (list пуст, transient-отказ list
     исключён) ⇒ takeover-аномалия: возраст < бюджета — transient-ожидание
     (джоб мог быть создан тиком, чей create подтверждён, а start/list
     моргнул), иначе исход FAILED `drill-vanished` (образец t02 «RUNNING без
     контейнера → FAILED»);
   - **снос после терминального исхода** (все пути выше сходятся сюда):
     journal `drill-cleanup/<X>/<id>` ДО манипуляций ⇒ в ключе терминальный
     `state` + `finished_unix` + `phase:"cleaning"` ⇒ kill+rm контейнера
     `pgw-backup-drill-<C>-<X>-<id>` и rm volume (своим детерминированным
     именем, у vanished-пути контейнера уже нет — чистится volume) ⇒
     подтверждение (повторный list/inspect по обоим именам пуст) ⇒ `phase`
     снимается — чистый терминальный итог, journal-фаза `drill-done/<X>/<id>`
     / `drill-failed/<X>/<id>`; transient docker rm ⇒ ключ остаётся с
     `phase:"cleaning"`, следующий тик повторяет;
   - **краш-рекавери**: тик находит терминальный ключ с `phase:"cleaning"`
     (или незакрытый journal `drill-cleanup` — тот же сигнал) ⇒ идемпотентная
     доводка rm по детерминированным именам до подтверждения — снос
     переживает рестарт воркера.
3. **Отбор кандидата** (новых запусков нет, пока у кластера есть
   незавершённый дрилл — RUNNING либо терминальный с `phase:"cleaning"`):
   интервал кластера = `policy.drill.interval_days` ?? `DrillIntervalDays`;
   `0` → нет запусков. Кандидаты — шарды декларации (не `ToRemove`) c
   COMPLETED-полными и БЕЗ активной restore-заявки (гвард «restore владеет
   жизненным циклом шарда», arch/19 §3.5 — список активных restore берётся
   из backups-модели тика). Готовность: `now − (finished_unix последнего
   терминального дрилла шарда)` ≥ `interval_days×86400`; ключа дрилла нет
   вообще ⇒ готов немедленно (первый дрилл). Выбор — МИНИМУМ `finished_unix`
   среди готовых (наименее свежий; «никогда» = 0 — обслуживается первым).
4. **Валидация кандидата** (та же, что RestoreProcess.ValidateAsync для
   own-source, чистые функции переиспользуются): новейший COMPLETED-полный
   (etcd; равный выбор планировщика свежести §2 — verify.state ≠ FAILED
   не требуется: restore-заявка в DR берёт новейший COMPLETED без оглядки
   на verify, дрилл доказывает тот же путь) ⇒ манифест
   `full/<id>/backup_manifest` существует (S3) ⇒ `wal_start_segment`
   (etcd-статус либо backup_label из S3) ⇒ WalChain непрерывна (list
   `wal/`). S3-отказ ⇒ transient (тикт повторит). Отказ валидации
   (манифеста нет/цепочка дырявая) ⇒ FAILED-дрилл БЕЗ запуска джоба:
   `started_unix` ставится, `error` — причина («полный без манифеста» /
   границы дыры от WalChain); контейнера/volume не было — снос не нужен,
   ключ сразу чистый терминальный итог; это честный исход «восстановимость
   не доказана» — оператор видит ровно то, что увидел бы DR.
5. **Запуск**: journal-before-manipulations — put ключа
   `{"state":"RUNNING","id","backup_id","started_unix":now}` ⇒ create+start
   контейнера `pgw-backup-drill-<C>-<X>-<id>` (§3.4; docker-хост первой
   ноды шарда из portalloc — как restore-джоб; engine null ⇒ transient,
   ключ остаётся RUNNING — следующий тик создаст). Отказ create/start ⇒
   transient (джоб пересоздаётся по имени идемпотентно).

### 3.4. Drill-джоб: `DrillJobCommand` + `DrillJobSpec` (`src/PgWorker.Backups/Drill/`)

- **`DrillJobCommand`** — inline bash (паттерн RestoreJobCommand; единственное
  место механики — воркер версионирует кодом, arch/19 §2): скачивание
  `full/<id>/` в PGDATA (env-контракт RestoreJobCommand) ⇒ восстановление
  канонических пустых каталогов initdb ⇒ `restore_command` через mc ⇒
  recovery-настройки auto.conf (wal_level=replica, summarize_wal=off,
  ssl/logging_off, пути джоба — все инцидентные правки t05 переносятся) ⇒
  `recovery_target_action=promote` БЕЗ target (latest) ⇒ локальный старт
  postgres (unix-socket `/tmp`, trust hba, `listen_addresses=''`) ⇒
  поллинг выхода из recovery (`pg_is_in_recovery() = false`) с бюджетом
  `PGW_RECOVERY_TIMEOUT_SEC` ⇒ снятие `pg_current_wal_lsn()` ⇒ stop
  postgres ⇒ cleanup recovery-остатков ⇒ result-JSON
  `{"ok":true,"restored_to_lsn":"…"}` / `{"ok":false,"error":…}` (протокол
  t02: stdout-маркеры фаз `{"phase":"downloading"}` /
  `{"phase":"recovering"}` + result; воркер толерантен к чужим строкам).
  SQL-проверок данных НЕТ (решение пользователя п.4): единственные
  SQL-обращения — поллинг recovery-флага и снятие LSN, обе — механика t05.
  Сверка счётчиков строк с продом не производится и не имеет смысла —
  данные прода уходят вперёд от точки бэкапа, счётчики восстановленной
  копии к моменту дрилла уже не соответствуют проду; дрилл доказывает
  разворачиваемость (старт + выход из recovery), целостность файлов —
  verify t04.
- **`DrillJobSpec`** — ContainerSpec по образцу RestoreJobSpec: образ
  `JobImage`, `VolumeName: pgw-backup-drill-<C>-<X>-<id>` (СВОЙ ephemeral
  volume — точка монтирования `/drill`, PGDATA `/drill/pgroot/data`),
  `Ports: []`, `Network: null`, `RestartPolicy: "no"`, лимиты
  `Agent { Cpu, Mem }`, `ExtraHosts` для advertised-S3, env: S3-комплект
  (MC_HOST), `SRC_PREFIX=<C>/<X>`, `BACKUP_ID`, `TARGET_TIME=""`,
  `PGW_RECOVERY_TIMEOUT_SEC` (env-контракт RestoreJobCommand без
  изменений).
- Имя контейнера: `pgw-backup-drill-<C>-<X>-<id>` (`BackupNames.
  DrillContainerName/DrillVolumeName`; префикс `pgw-backup-` — семейство
  джобов бэкапов для D1).

### 3.5. Врезка, стоп-семантика, deprovisioning

- **ReconcileLoop** (`ClusterProcesses.DrillAsync` + врезка): после
  `backup-restore` (гвард restore-владения шарда уже отработал в отборе),
  до `repair`. Вызов ВСЕГДА (как backup-wal): при `Backups:Enabled=false`
  процесс доводит активный дрилл до терминального исхода И доводит снос
  контура (`cleaning` до подтверждённого удаления — иначе выключение
  оставляло бы осиротевший exited-контейнер до включения), новых не
  стартует; гвард `DrillIntervalDays=0` — то же.
- **remove-shard / демонтаж шарда**: дрилл не зависит от нод (S3+docker-хост
  + свой volume) — снос шарда не ломает идущий дрилл; ключ `drill` живёт в
  per-cluster префиксе и уходит с D2. Активный restore на шарде ⇒ дрилл
  шарда не стартует (отбор п.3); обратного гварда не нужно (дрилл безвреден
  для restore: своего volume, портов и сетей нет).
- **DR-защита t04-orphan-dr-hold / verify / ретенция**: дрилл S3 только
  читает — сироты, hold/заявки, ретенционные удаления не затрагиваются;
  verify идёт параллельно (свой префикс имён `pgw-backup-verify-*`).
- **Deprovisioning**: D1 убивает контейнер дрилла (новый префикс в списке
  чистки джобов бэкапов), volume `pgw-backup-drill-<C>-…` сносится тем же
  проходом (префикс); D2 чистит ключ.

### 3.6. Панель

- **Парсер** (`BackupsParser`): чтение `<X>/drill`-ключей → `DrillInfo`
  (state/id/backup_id/started/finished/phase/restored_to_lsn/error) в
  `ClusterBackupsInfo`; битый JSON — KeyParseError + пропуск (паттерн
  парсера). Policy-парсер: `drill.interval_days` (null → нет поля).
- **Алерт `backup-drill-failed`** (`BackupDrillFailedRule`, critical — по
  образцу backup-verify-failed): последний дрилл шарда живого Active-кластера
  FAILED — текст `error`, labels drillId/backupId; Hint «дрилл восстановления
  провалился — восстановимость из бэкапа не доказана», Remedy OperatorRunbook
  («разбор по docs/backup-restore.md §дрилл»).
- **Алерт `backup-drill-stale`** (`BackupDrillStaleRule`, warning): у шарда
  Active-кластера есть COMPLETED-полные, интервал дриллов включён
  (policy/дефолт ≠ 0), а успешного дрилла нет вовсе ЛИБО он старше `2×interval` —
  «восстановимость не доказывается» (дриллы молча не исполняются: вечный
  transient, гонки выключения). Пустой префикс бэкапов кластера — правило
  молчит (подсистема не включена — образец backup-full-stale).
- **DTO и UI**: грань «Хранилище бэкапов»/детали кластера — per-shard
  статус последнего дрилла (SUCCEEDED/FAILED/RUNNING + фаза
  `downloading|recovering|cleaning` + возраст, restored_to_lsn, error);
  поле «Дрилл каждые N суток» (число; 0 = выкл) — форма читает текущую
  policy (ретенционные поля + verify + drill) и PUTит полное тело через
  новую панельную команду-прокси `UpdateBackupsPolicyCommand` (образец
  OrphansCommands) в `POST /api/clusters/{c}/backups/policy`; панель в etcd
  не пишет.
- **API воркера** (`BackupsPolicyHandler` расширение): тело принимает
  `"drill":{"interval_days":int}` — валидация диапазона [0..3650], мисматч
  → 400 перечнем (существующий формат ошибок). Замещение целиком
  сохраняется: поле `drill` в теле отсутствует → в записываемую policy
  секция `drill` НЕ кладётся (кластер живёт на глобальном дефолте
  конфига); панель в своей форме всегда шлёт полный набор полей, включая
  текущее значение drill (прочитанное из policy/дефолта).

### 3.7. Runbook (`docs/backup-restore.md`)

Новый раздел «Дрилл восстановимости»: что такое ключ drill/его фазы
(`downloading|recovering|cleaning`; снятая фаза = контур подтверждённо
снесён); чтение статуса (etcdctl-формат + панель); действия при FAILED
(разбор причины: битый полный/WAL-дыра — см. разделы verify/restore;
восстановимость доказывается повторным дриллом после лечения); настройка
интервала (policy-API curl + панель; 0 — выключение); дрилл не влияет на
Patroni-контур и synchronous-режим мастера: нод в кластере не создаёт,
HA-scope не трогает; взаимосвязь с verify (verify — целостность файлов,
дрилл — разворачиваемость: старт postgres и выход из recovery; зелёный
verify при красном дрилле = проблема механики разворачивания — разбор по
`error` статуса дрилла).

### 3.8. Изменения в arch/ (проектируются здесь; правки — Фаза 6 dev-flow по плану)

1. `arch/19-backups.md`: §3.6 «Дрилл восстановимости (reliability t02)» —
   роль/механика/фазы/бюджеты/изоляция/чистка (текст §3.3–3.5 этого spec в
   канонической лексике; контроль — «старт + выход из recovery»; фазы
   джоба `downloading|recovering`; снос контура — доводимая фаза
   `cleaning` с journal `drill-cleanup` и переживанием рестарта воркера;
   изоляция — без нод в кластере/HA-scope, synchronous-режим мастера не
   затрагивается); §4 — строка ключа `drill` + поле `drill.interval_days`
   policy; §9 — секция `Drill { IntervalDays=1, TimeoutSec=21600 }`; §10 —
   риски (нагрузка дрилла/большие базы, параллельные дриллы кластеров);
   §8 карта задач НЕ трогается (задача трека reliability, не серии
   t02–t07).
2. `arch/adminpanel/02-etcd-contract.md`: §2.3.1 — строка ключа `drill`
   (панель читает); §9 — мутация policy из панели (поле drill, полный PUT).
3. `arch/14-pgworker.md` §1.1: тело policy-API расширяется `drill.
   interval_days` (одна строка к описанию BackupsPolicy-эндпоинта).

## 4. Фазы

1. **Чистые функции + контракт** (§3.1–3.3): `DrillStatusJson`
   (сериализация/парсинг статуса, вкл. `cleaning`-фазу и её снятие), отбор
   кандидата `DrillPlanner.SelectCandidate(shards, fulls, drills, restores,
   intervalDays, now)` (чистая функция: готовность по finished_unix,
   least-recently-drilled, гварды restore/ToRemove, блокировка при
   незакрытом cleaning кластера), `DrillJobLog.Parse` (маркеры фаз +
   result-JSON); юниты (TDD: отбор при отсутствии ключей/просрочке/нулевом
   интервале/активном restore/незакрытом cleaning; парсинг result).
2. **Drill-джоб + процесс** (§3.4, п.2–5 §3.3): `DrillJobCommand`/
   `DrillJobSpec` (механика RestoreJobCommand до выхода из recovery,
   без SQL-проверок данных), `RestoreDrillProcess` (супервиз/таймаут/
   снос-cleaning/валидация/запуск); интеграции `RestoreDrillProcessTests`
   (OwnEtcd + FakeS3 + FakeBackupEngine по образцу BackupVerifyProcessTests):
   запуск по расписанию, takeover (RUNNING + живой контейнер),
   таймаут-бюджет, exit-код ⇒ SUCCEEDED/FAILED ⇒ cleaning ⇒ контейнер и
   volume удалены, phase снят; рестарт посреди сноса (терминальный ключ с
   `phase:"cleaning"` ⇒ следующий тик дочищает контейнер и volume);
   валидационный FAILED без джоба (чистый итог без cleaning); гварды
   (Enabled=false — доводка без новых; интервал 0; активный restore).
3. **Врезка + опции + policy-API** (§3.2, §3.5, §3.6 API): `BackupsRuntimeOptions`
   + валидация + Program.cs-склейка; `ClusterProcesses.DrillAsync` +
   ReconcileLoop-врезка; D1-префикс чистки; `BackupsPolicyHandler` — поле
   `drill.interval_days`; API-интеграции (валидация 400, замещение целиком,
   дефолт при отсутствии).
4. **Панель** (§3.6): BackupsParser (drill-ключи + policy-поле) ⇒ DTO ⇒
   правила `backup-drill-failed`/`backup-drill-stale` ⇒ команда-прокси
   policy ⇒ UI (статус per-shard + поле интервала); панельные юниты
   (BackupsParserTests, BackupDrillFailedRuleTests, BackupDrillStaleRuleTests,
   мапперы) + интеграции (грань бэкапов, мутация policy).
5. **Runbook + E2E + гейт** (§3.7): runbook-раздел; docker-E2E кейс в
   `E2eBackupScenarios` (реальный MinIO + drill-джоб образа pgworker-backup:
   кластер → дождаться COMPLETED-полного → дрилл стартует сам (первый —
   сразу) → SUCCEEDED с `restored_to_lsn` → контейнер/volume отсутствуют
   (ассерт чистоты), ключ SUCCEEDED без phase; негативная ветка: порча
   WAL-цепочки → FAILED + панельный алерт); мерж-гейт — кейс-маркер
   `Scale_AddEmptyShard` на свежем Release (AGENTS.md), снятие тега из
   reliability.md + строка в reliability-report.md тем же мерж-коммитом.

## 5. Ограничения

- Механика restore-заявки оператора (t05) НЕ меняется: гварды, фазы,
  ключи restore — как были; дрилл не пишет в `restore/*`-ключи и не
  монтирует volume нод.
- Сироты/DR-hold (t04), ретенция (t06), супервизор (t07), verify (t04) —
  не затрагиваются (дрилл — читатель S3).
- Контроль дрилла — только «старт + выход из recovery» (решение
  пользователя п.4): SQL-проверки данных (таблицы/строки/канарейки),
  сверка с эталоном прода — НЕ делаются; целостность файлов — verify t04.
- Дрилл не создаёт нод в кластере и не трогает HA-scope/synchronous-режим
  мастера (решение пользователя п.5 — изолированный дизайн).
- Инкрементальные бэкапы/PITR-цели дрилла: дрилл всегда latest (цель DR);
  target_time-дрилл — вне скоупа.
- Глобальная одновременность: параллельные дриллы разных кластеров
  допустимы (по одному на клэйм; домашняя установка — 1–2 кластера);
  глобальный лидер-гвард НЕ вводится — фиксируется риском в arch/19 §10.
- E2E/интеграции — каноны AGENTS.base §12/AGENTS.md: своё окружение,
  динамические порты, полный teardown, ассерт чистоты, BrokerBootSec ≤ 100 с,
  зачистка контейнеров/сетей после серий.
- Код — .NET 10, `Nullable=enable`, `TreatWarningsAsErrors=true`.

## 6. Критерии приёмки

- **AC1 (автозапуск по расписанию)**: Active-кластер с COMPLETED-полным:
  первый дрилл шарда стартует немедленно, повторный — не раньше
  `interval_days` (интеграция, сжатое время); за проход — один шард
  (наименее свежий); `interval_days=0`/`DrillIntervalDays=0` — запусков
  нет.
- **AC2 (изоляция)**: за время дрилла прод-ноды шарда живы (state RUNNING,
  Patroni-scope не тронут, synchronous-режим мастера без изменений),
  drill-контейнер без сети/портов, пишет только в
  `pgw-backup-drill-<C>-<X>-<id>`; нод в кластере дрилл не создаёт и в
  HA-scope не входит (E2E + интеграция).
- **AC3 (контроль: старт + выход из recovery)**: drill-джоб докатывает
  WAL-цепочку и выводит postgres из recovery (`pg_is_in_recovery()=false`),
  `restored_to_lsn` — в ключе и result-JSON; recovery-цель недостижима
  (конец WAL/дыра) или бюджет `RestoreRecoveryTimeoutSec` исчерпан ⇒
  FAILED с причиной из result/лога (интеграция + E2E).
- **AC4 (чистота при любом исходе, включая рестарт посреди сноса)**:
  успех, провал, таймаут, валидационный отказ — контейнер и volume
  удалены, `phase` снят, ключ = чистый терминальный итог (E2E/интеграции
  ассертят отсутствие по имени); ВКЛЮЧАЯ рестарт воркера посреди сноса —
  терминальный ключ с `phase:"cleaning"` ⇒ следующий тик идемпотентно
  дочищает контейнер и volume (интеграционный кейс); D1-deprovisioning
  сносит их же.
- **AC5 (алерт провала)**: последний дрилл FAILED ⇒ панельный алерт
  `backup-drill-failed` (critical, labels drillId/backupId, error в тексте);
  SUCCEEDED/фазы дрилла — без алерта.
- **AC6 (алерт молчания)**: полные есть, интервал включён, успешного дрилла
  нет/старее 2×интервала ⇒ `backup-drill-stale` (warning); выключенный
  интервал/пустой префикс — правило молчит.
- **AC7 (интервал из панели)**: поле «суток» в грани бэкапов кластера
  PUT-ится полной policy через панельную команду-прокси в API воркера
  (валидация 0..3650, мисматч → 400); новый период применяется следующим
  проходом без рестарта воркера.
- **AC8 (не мешает соседям)**: активная restore-заявка ⇒ дрилл шарда не
  стартует; идущий дрилл не мешает restore/verify/ретенции (разные
  имена/volumes/ключи; интеграция гварда).
- **AC9 (takeover/бюджеты)**: рестарт инстанса воркера при живом
  drill-джобе — новый инстанс супервизит по ключу+имени до исхода;
  RUNNING без контейнера старше `DrillTimeoutSec` ⇒ FAILED
  `drill-timeout`/`drill-vanished` (+ доводка сноса volume).
- **AC10 (стоп-семантика)**: `Backups:Enabled=false` — новых запусков нет,
  активный дрилл доведён до терминального исхода, снос контура доведён
  (`cleaning` закрыт) — ключ/контейнер не остаются RUNNING/cleaning
  навсегда.
- **AC11 (runbook + arch)**: раздел дрилов в docs/backup-restore.md (вкл.
  строку о невлиянии на Patroni-контур/synchronous-режим мастера); arch/
  правки (§3.8) смержены ДО кода (arch-first, ревью plan↔spec).
- **AC12 (мерж-гейт)**: юниты/интеграции/E2E зелёные на свежем Release
  (кейс-маркер `Scale_AddEmptyShard` + drill-кейс); тег `t02-restore-drill`
  снят из `arch/roadmap/reliability.md`, строка в `reliability-report.md`
  перенесена в «Сделано» тем же мерж-коммитом.
