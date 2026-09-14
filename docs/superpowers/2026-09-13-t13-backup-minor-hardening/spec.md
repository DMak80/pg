# t13-backup-minor-hardening — два advisory из code-review t07 (spec)

Дата: 2026-09-13. Канон — [`arch/19-backups.md`](../../../arch/19-backups.md)
(уточнения §3/§6 — фаза 0 этого плана, см. §4). Roadmap-текст задачи —
`arch/roadmap/backup.md`, запись `t13-backup-minor-hardening` (снимается из
roadmap мерж-гейтом этого же коммита).

Решения пользователя (2026-09-13, зафиксированы):

1. **Advisory 1 (ветка «слот исчез» в `WalStreamProcess`)** — лечение
   «фолбэк + журнал»: `baseUnix: wal.LastUploadedUnix ?? clock-сейчас` +
   journal-заметка (и лог) о битом ключе при `last_uploaded_unix == null`.
   Комбинирует оба варианта roadmap: продвижение есть (BROKEN ставится, слот
   пересоздаётся, агент останавливается), факт битого ключа виден оператору.
2. **Advisory 2 (`BackupProcess.SuperviseActiveAsync`)** — лечение «перенос
   гварда выше»: возрастной бюджет проверяется первым в цикле активного, ДО
   резолва source/engine/list; `FAILED job-timeout` пишется по факту etcd
   (`started_unix` + now — docker-доступ не нужен); cleanup контейнера/volume —
   best-effort (при доступном источнике — как раньше; при недоступном —
   journal-пометка о пропущенном cleanup). Счётчик отсутствия источника —
   отклонён: при живом `started_unix` возраст уже несёт нужный факт, новое
   поле контракта/состояние не требуется.

---

## 1. Цель

Устранить две точки хрупкости подсистемы бэкапов, замеченные code-review t07
(2026-09-13): сегодняшнее поведение корректно, но каждый из дефектов при
нарушении сопутствующего инварианта превращается в бесконечную петлю без
продвижения:

- **A1 — форс-мьют в ветке «слот исчез»** (`WalStreamProcess.TickShardAsync`,
  шаг (3)): `baseUnix: wal.LastUploadedUnix!.Value`. Сегодня мьют недостижим
  только потому, что оба читателя wal-ключа (снапшотный `BackupsParser.
  TryParseWal` и `WalStatusWriter.Parse`) отбрасывают ключ без
  `last_uploaded_unix` (→ `Wal = null` → `chainKnown = false` → ветка не
  заходит). Связь «гвард парсера спасает мьют потребителя» нигде не
  зафиксирована и уже однажды рвалась (ed1561b: снапшотный парсер не знал
  `state=BROKEN`, который знал `WalStatusWriter`). При нарушении инварианта
  писателя (ручная правка ключа, будущий писатель без `last_uploaded_unix`,
  ослабление гварда парсера) каждый тик: `InvalidOperationException` →
  пер-шардовый catch → journal `shard-error` → следующий тик то же — вечно,
  без BROKEN, без пересъёма, с нечитаемым сообщением об ошибке.
- **A2 — возрастной бюджет после transient-гвардов**
  (`BackupProcess.SuperviseActiveAsync`): таймаут-гвард
  `SupervisionTimeouts.IsTimedOut` стоит после трёх transient-пропусков
  (`source is null` → `engine is null` → `list`-отказ → `continue`). При
  источнике, исчезнувшем из portalloc навсегда (replace ноды / рассинхрон
  portalloc при живом шарде), вечный RUNNING никогда не получает FAILED по
  таймауту и вечно держит инвариант «один активный полный на шард»
  (`BackupPlanner.HasActive` → новые полные не планируются — шард навсегда
  без бэкапов). Pre-existing t02, замечено ревью t07.

Оба фикса — точечные, поведение при неповреждённых инвариантах не меняется
(существующие тесты остаются зелёными без правок ожиданий, кроме порядка
вычисления `nowUnix`).

## 2. Принципы

- **Arch-first**: уточнения поведения — сначала в `arch/19-backups.md`
  (фаза 0), затем код. Контракт etcd НЕ меняется: новые поля/форматы не
  вводятся, машины состояний те же.
- **Возраст — самостоятельный факт etcd** (решение 2): вердикт
  `FAILED job-timeout` выводится из `started_unix` статуса и часов воркера и
  не требует docker-доступа; transient-пропуск источника не откладывает
  бюджет (джобу с возрастом > 6 ч всё равно нечем оправдаться).
- **Факт над записью, но без краха тика** (решение 1): при битом
  `last_uploaded_unix` ветка «слот исчез» использует фолбэк `clock-сейчас`
  только как defensive-значение для параметра `baseUnix` (в `BreakAsync` при
  `wal != null` — нашем случае, `chainKnown` — параметр не попадает в
  BROKEN-запись: запись строится из живого `wal`, чей `last_uploaded_unix`
  и так отсутствовал). Принцип ревью Ф4-2 №2 («никогда не now()») не
  нарушается: now не подменяет наблюдаемый факт в etcd-ключе; битый ключ
  дополнительно маркируется journal-заметкой.
- **Идемпотентность и transient/permanent-паттерны arch/17**: тик,
  завершившийся на любом шаге, повторяется следующим тиком без порчи
  состояния; cleanup — идемпотентный best-effort (404/vanished = успех).
- **Ошибка шарда не роняет остальные** (арх-паттерн WalStreamProcess):
  journal-заметка о битом ключе — не исключение, а факт тика.

## 3. Структура/компоненты (что меняем)

### 3.1. `WalStreamProcess` — ветка «слот исчез» (A1)

Файл `src/PgWorker.Backups/WalStreamProcess.cs`, шаг (3)
`TickShardAsync`, вызов `BreakAsync` при
`chainKnown && wal.State is Active or Degraded`:

- `baseUnix: wal.LastUploadedUnix!.Value` →
  `baseUnix: wal.LastUploadedUnix ?? clock.GetUtcNow().ToUnixTimeSeconds()`
  (now вычисляется рядом, локально);
- при `wal.LastUploadedUnix is null` — до вызова `BreakAsync`:
  `journal.WritePhaseAsync(cluster, Op, $"wal-key-invalid/{shard.Name}",
  claims.InstanceId, "last_uploaded_unix отсутствует — битый ключ
  /pgworker/backups/<C>/<X>/wal (ручная правка/иной писатель); ветка
  «слот исчез» идёт с фолбэком времени", ct)` +
  `logger?.LogWarning(...)` — диагноз виден оператору, тик продолжает
  продвижение (BROKEN + recreateSlot + стоп агента как сегодня).

Инвариант для будущих правок (зафиксировать комментарием в коде): параметр
`baseUnix` в `BreakAsync` потребляется только при `wal == null` (создание
записи с нуля); в ветке «слот исчез» `wal != null` всегда (`chainKnown`), т.е.
фолбэк не влияет на содержимое BROKEN-записи. Если будущая правка начнёт
использовать `baseUnix` в записи при живом `wal` — это место пересмотреть
(подмена факта now()-временем запрещена принципом §2).

### 3.2. `BackupProcess.SuperviseActiveAsync` — порядок гвардов (A2)

Файл `src/PgWorker.Backups/Process/BackupProcess.cs`, метод
`SuperviseActiveAsync`. Новый порядок тела цикла по активным
(PLANNED/RUNNING/UPLOADING):

1. **Возрастной бюджет — ПЕРВЫЙ** (t13): `IsTimedOut(active.StartedUnix,
   nowUnix, options.JobFullTimeoutSec)` → `put` FAILED
   `error="job-timeout: <age> с > FullTimeoutSec"` (journal-before-
   manipulations, как сегодня) → cleanup best-effort (п. 2 ниже) → journal
   `job-timeout/<shard>/<id>` → `continue`. `nowUnix` вычисляется в начале
   итерации (сейчас — после list).
2. **Cleanup best-effort внутри таймаут-ветки**: резолв `source` из
   portalloc по `active.Node` и `engine = EngineFor(source.Host)` только для
   cleanup; при `source is null || engine is null || list`-отказе — cleanup
   пропускается, journal-факт `job-timeout/...` получает пометку
   «cleanup пропущен: источник недоступен» (в details той же journal-записи,
   отдельных phase не заводим). При доступном источнике — как сегодня:
   `found is not null` → `CleanupJobAsync` (kill+rm контейнера и volume;
   PLANNED без контейнера — FAILED без kill, комментарий сохраняется).
   Пропущенный cleanup не блокирует ничего: имя контейнера детерминированное,
   id уникальный, FAILED уже в etcd — осиротевший контейнер на живом хосте
   не держит инвариант «один активный».
3. Далее — без изменений: резолв `source`/`engine`/`list` → `found` →
   PLANNED-запуск / created-довыгон / vanished / running / exited (те же
   transient-гварды для НЕ-истёкших активных).

Следствия переноса (осознанные, зафиксировать в тестах):

- transient-отказ `list` (или etcd-глитч portalloc) на ДЕЙСТВИТЕЛЬНО
  истёкшем джобе теперь всё равно ставит FAILED — вердикт правомерен:
  возраст самодостаточен (решение 2). Сегодня такой джоб застревал до
  восстановления источника, при навсегда исчезнувшем — вечно.
- PLANNED с возрастом > бюджета при недоступном источнике → FAILED
  job-timeout без kill — расширение уже существующего поведения («PLANNED
  без контейнера — FAILED без kill») на случай недоступного источника.

### 3.3. Тесты

- **Юнит** `src/tests/PgWorker.UnitTests/Backups/BackupProcessTests.cs`
  (паттерн `Rig`/`FakeBackupEngine`):
  - новый: RUNNING `started = now−7h`, portalloc жив, но БЕЗ ноды
    `active.Node` (источник исчез навсегда) → тик ставит FAILED
    `job-timeout`, journal `job-timeout/<shard>/<id>`, cleanup не зовётся
    (`Engine.Removed` пуст);
  - новый (опционально, при поддержке фейка): `list`-отказ при возрасте >
    бюджета → FAILED, cleanup пропущен;
  - регресс без правок: `Супервиз_полный_старше_бюджета_FAILED_jobtimeout_
    и_переснятие` (живой источник) и `TransportError_KeepsStatus` (возраст <
    бюджета) остаются зелёными.
- **Интеграция** `src/tests/PgWorker.IntegrationTests/Backups/
  WalStreamProcessTests.cs` (паттерн own-etcd + `FakeWalSqlExecutor` со
  пустыми `Slots`): новый кейс — в `backups` передаётся `WalStreamState`
  ACTIVE с `LastUploadedUnix = null` (моделирует нарушение инварианта
  писателя — снапшот-объект в память, парсеры не задействованы) → тик
  успешен (нет `shard-error`), wal-ключ → BROKEN c error про слот, слот
  пересоздан, агент остановлен, journal содержит `wal-key-invalid/<shard>`.

### 3.4. Что НЕ меняем (границы)

- Контракт etcd (§4 канона): форматы, поля, состояния — без изменений;
  AdminPanel не затронута.
- `RestoreProcess`/`BackupVerifyProcess` (там свои порядки гвардов) —
  вне скоупа: roadmap-запись говорит только про `BackupProcess.
  SuperviseActiveAsync`; у restore «вечный» активный завершается заявкой
  оператора, у verify — квотой попыток (checked_unix). Аналогичный перенос
  туда — отдельной задачей, если ревью укажет.
- Счётчики отсутствия источника, новые поля статуса, память воркера — не
  заводим (отклонённый вариант решения 2).
- Гварды парсеров wal-ключа (`BackupsParser.TryParseWal`,
  `WalStatusWriter.Parse`) — не ослабляем и не усиливаем: они остаются
  первой линией (битый ключ → `Wal=null` → путь «первый старт»), лечение A1 —
  вторая линия на случай их будущего рассинхрона (прецедент ed1561b).

## 4. Фазы

- **Фаза 0 — arch/19 (первая правка, arch-first)**: два точечных уточнения
  канона без изменения контракта:
  - §6 «Бюджеты зависших джобов»: после строки про полный — дополнение, что
    вердикт FAILED по возрасту не требует docker-доступа (факт etcd
    `started_unix` + часы воркера): transient-пропуск источника
    (portalloc/engine/list) не откладывает бюджет; kill+rm — best-effort
    при доступном хосте источника, при недоступном — journal-пометка
    (контейнер-сирота с детерминированным именем ничего не блокирует).
  - §3 «Правила непрерывности» (абзац про «слот исчез»): дополнение, что при
    нарушении инварианта записи (`last_uploaded_unix` отсутствует — ручная
    правка/будущий писатель при ослабленном гварде парсера) ветка не
    крашится тиком: defensive-фолбэк времени + journal-заметка о битом
    ключе; BROKEN/пересоздание слота работают как обычно.
- **Фаза 1 — A1 (WalStreamProcess)**: правка §3.1 + интеграционный тест
  §3.3 (TDD: тест сначала — красный на `.Value`-мьюте, затем правка).
- **Фаза 2 — A2 (BackupProcess.SuperviseActiveAsync)**: правка §3.2 +
  юнит-тесты §3.3 (TDD: тест «источник исчез + возраст» сначала — красный
  на текущем порядке гвардов, затем перенос).
- **Фаза 3 — прогоны и зачистка**: `dotnet build` (TreatWarningsAsErrors) →
  юниты Backups → интеграция Backups (own-etcd/own-minio, динамические
  порты, teardown-чистка) → E2E-маркер мерж-гейта на свежем Release:
  `DOTNET_CLI_UI_LANGUAGE=en PGW_TEST_DOCKER=1 dotnet test
  src/PgWorker.slnx -c Release --filter
  FullyQualifiedName~Scale_AddEmptyShard` (задача трогает код воркеров;
  provisioning/portalloc/moves не меняются — полный E2eFixture не
  требуется). После КАЖДОЙ серии — зачистка:
  `docker rm -f $(docker ps -aq)` (кроме as-*/adminpanel стенда, если
  поднят) + `docker network prune -f`; сети per-cluster движком не
  подбираются ryuk'ом.

## 5. Ограничения

- Никаких изменений контракта etcd и AdminPanel.
- Никаких новых конфиг-опций (бюджеты `Job:FullTimeoutSec` и паттерны
  journal — существующие).
- Поведение при целых инвариантах — идентично сегодняшнему (кроме
  недостижимого ранее FAILED для мёртвого источника — это и есть лечение).
- Правки только в worktree `feat-t13-backup-minor-hardening`; главный
  репозиторий (с незакоммиченной roadmap-записью) не трогаем.

## 6. Критерии приёмки

- **AC1 (A1)**: тик WalStreamProcess при wal-ключе ACTIVE/DEGRADED с
  `LastUploadedUnix = null` (инвариант писателя нарушен) и исчезнувшем
  слоте: тик завершается без `shard-error`; wal-ключ → BROKEN (error про
  слот, граница разрыва = `last_uploaded_segment` на момент обнаружения);
  агент остановлен; слот пересоздан immediate+reserved; journal содержит
  заметку о битом ключе (`wal-key-invalid/<shard>`). В BROKEN-запись
  now()-фолбэк не попадает (запись строится из живого `wal`).
- **AC2 (A2)**: активный полный (PLANNED/RUNNING/UPLOADING) с возрастом
  `now − started_unix > Job:FullTimeoutSec` получает FAILED
  `job-timeout` тиком супервизии независимо от резолва source/engine/list:
  юнит-тест с portalloc без ноды-источника джоба зелёный; «вечный» RUNNING
  при навсегда исчезнувшем источнике больше не держит инвариант «один
  активный на шард» (следующий тик планирует переснятие по общему бэкоффу).
- **AC3 (cleanup best-effort)**: при недоступном источнике cleanup
  пропускается без падения тика, journal-факт `job-timeout/...` несёт
  пометку; при доступном — kill+rm контейнера и staging-volume как до t13.
- **AC4 (регресс)**: существующие тесты WalStreamProcessTests /
  BackupProcessTests (вкл. `Супервиз_полный_старше_бюджета_...` с живым
  источником и `TransportError_KeepsStatus` с неистёкшим бюджетом) —
  зелёные без правки ожиданий.
- **AC5 (гейты)**: build (Release, warnings-as-errors) зелёный; юниты и
  интеграция Backups зелёные; E2E-маркер `Scale_AddEmptyShard` на свежем
  Release зелёный; после серий — зачистка docker-контейнеров и сетей
  (гейт AGENTS.md); roadmap-запись `t13-backup-minor-hardening` снята тем
  же мерж-коммитом.
