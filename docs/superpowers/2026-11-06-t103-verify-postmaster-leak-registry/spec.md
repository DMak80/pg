# t103: verify/restore-check PgNode — жизненный цикл postmaster и сверка id итога операции

Spec задачи `t103-verify-postmaster-leak-registry` (вводная — пункт в
`arch/roadmap/bugs.md`, логи прогона — `arch/roadmap/bugs/t103-verify-postmaster-leak-registry/`
в главном репо, вне git). Все диагнозы ниже подтверждены по логам прогона
(лог ноды `container-pgw-bksprd9eeeea8-shard1-shard1a.log`, postmaster-лог
`job-pgw-bksprd9eeeea8-shard1-shard1a-restore-pg.log`, воркер-лог
`container-pgw-ew-bksprd9eeeea8-…-20261106-060441.log`) и по коду worktree.

## 1. Цель

Устранить два дефекта, воспроизведённых упавшим E2E
`E2eSupervisorSelfHealScenarios.Backup_ChainBroken_SelfHeals`
(полный прогон 2026-11-06):

- **Д1 — утечка postmaster restore/drill-операции.** Postmaster, запущенный
  restore/drill-операцией приёмника, не гарантированно остановлен при любом
  исходе операции: переживает рестарт процесса приёмника (сирота в каталоге
  операции), а scratch-каталог дрилла чистится ДО/БЕЗ его остановки — каталог
  удаляется из-под живого процесса. Повторный запуск той же операции упирается
  в живого сироту (`FATAL: lock file "postmaster.pid" already exists`).
- **Д2 — итог проверки полного из чужой операции.** BackupVerifyProcess
  записывает verify-вердикт в etcd-запись полного по снимку ОБЩЕГО ops-реестра
  приёмника БЕЗ сверки id снимка с id проверяемого полного: терминальный
  failed-снимок чужой drill-операции (с её текстом и путём её каталога) попал
  в запись нового полного как его «проверка FAILED», хотя его собственная
  проверка не запускалась. Та же дыра в RestoreProcess: чужой succeeded-снимок
  может перевести заявку restore в REJOINING без system_id (запирает bootstrap
  ноды — сценарий, прямо описанный в `arch/backups/processes/f-restore.md` §2).

Порядок работ: ПЕРВОЙ правится `arch/` (правила), затем код зеркалит канон.

## 2. Диагноз (подтверждённый разбор по уликам)

### 2.1 Хронология прогона (нода shard1a, все времена UTC)

| Время | Событие | Источник |
|---|---|---|
| 06:02:26 | Полный №1 `20261106060207Z` COMPLETED | etcd-дамп |
| 06:02:28.43 | `POST /api/ops/verify` → 202: потоковая verify №1 заняла общий ops-реестр приёмника | лог ноды |
| 06:02:28.48 | `POST /api/ops/restore` (drill id `20261106060228Z`) → **409** — реестр занят verify; drill-запуск отложен | лог ноды |
| 06:02:31.61 | verify №1 — `succeeded` (потоковая сверка 512 МБ за ~3 с) | лог ноды |
| 06:02:31.63 | Воркер пишет `verify=OK` в полный №1 — корректный итог СВОЕЙ проверки | воркер-лог |
| 06:02:31.67 | Drill-процесс (сверка id видит чужой succeeded) перезапускает drill → 202: инкарнация A | лог ноды |
| 06:02:31.72–34.41 | A: стрим + распаковка 512 МБ в `backup-drill/20261106060228Z/pgdata` | `[restore]`-строки |
| 06:02:34.57 | A: `pg_ctl start -w` → postmaster **PID 331**, recovery тянет WAL | postmaster-лог |
| 06:02:49.05 | **Тест штатно даёт `supervisorctl stop pgnode`** (сценарий останавливает приёмники нод перед порчей WAL) → graceful-выход pgnode. Дочерние процессы A (pg_ctl-ожидание, postmaster 331) **НЕ гашены — сироты**. Реестр операций умирает вместе с процессом | лог ноды (`stopped: pgnode (exit status 0)`) |
| 06:02:58.55 | `supervisorctl start pgnode` (шаг сценария) — новый процесс, реестр пуст | лог ноды (`spawned: pgnode`) |
| 06:03:01.15 | Drill-процесс: `GET /api/ops/restore` → **404** (рестарт приёмника) → vanished-семантика: повторный POST → 202: инкарнация B ТОЙ ЖЕ операции (тот же id/каталог) | лог ноды |
| 06:03:03.66 | B: распаковка в тот же каталог (A ещё жива в нём) | `[restore]`-строки |
| 06:03:03.74 | B: `pg_ctl start` → postgres **PID 933** `FATAL: lock file "postmaster.pid" already exists … HINT: Is another postmaster (PID 331) running` | postmaster-лог |
| ~06:03:03.8 | B: failed-снимок `pg_ctl start failed: another server might be running…` + хвост лога; **finally чистит scratch `Directory.Delete` ПРИ ЖИВОМ постгрессе 331** (каталог удалён из-под процесса) | код `RestoreOperation.cs` |
| 06:03:04.71 | Постгресс 331: `restore_command` теряет cwd (`sh: getcwd() failed`) → `FATAL: WAL ends before consistent recovery point` → 06:03:04.74 postmaster 331 умирает | postmaster-лог |
| 06:03:06–09 | Полный №2 `20261106060306Z` COMPLETED (воркер переснимает после WAL_CHAIN_ERROR — штатно) | etcd-дамп |
| 06:03:10.72 | BackupVerifyProcess (кандидат — полный №2, PENDING on_create): `GET /api/ops/verify` → OpsStatus отдал Current общего реестра — failed-снимок **B** (id `20261106060228Z`, phase recovering, error «pg_ctl start failed…» с путём каталога A/B) → воркер **без сверки id** пишет в ПОЛНЫЙ №2 `verify=FAILED` текстом чужой drill-операции | воркер-лог, etcd-дамп |

### 2.2 Д1 — корневые механики (код `src/PgWorker.PgNode/Operations/RestoreOperation.cs`, `Program.cs`)

1. **Рестарт приёмника не гасит операцию.** Операции запускаются
   `Task.Run(() => operation.RunAsync(CancellationToken.None))` (Program.cs,
   MapOpsRoutes); при остановке хоста pgnode никто не отменяет задачу и не
   убивает дочернее дерево (`ProcessRunner` убивает дерево только по
   cancellation token). Грейсфул-выход оставляет живыми pg_ctl-ожидание и
   postmaster операции. Канон уже предусматривает рестарт приёмника
   («GET 404 vanished» в c-verify §3 / g-drill §1), но не описывает гашение
   процессов операции при остановке.
2. **Чистка scratch без остановки постгресса.** В `finally` drill-ветка
   делает `Directory.Delete(pgdata, recursive)` сразу; остановка постгресса
   (`StopBestEffortAsync`) есть только в ЧАСТИ fail-путей (отмена,
   recovery-budget, пустой LSN) и отсутствует в ветке «pg_ctl start failed»,
   ветке «pg_ctl stop failed» и — главное — в самом finally перед удалением.
   Для restore-ветки return Patroni (`supervisorctl start spilo`) в finally
   тоже происходит без гарантии остановки постгресса операции.
3. **Повторный запуск не защищён от сироты в каталоге.** Vanished-перезапуск
   (штатная семантика) стартует pg_ctl в каталог, который может удерживать
   живой постгресс прошлой инкарнации, — без предварительной проверки
   `postmaster.pid`/`pg_ctl status`.
4. `pg_ctl start -w` при исчерпании старт-ожидания возвращает fail, НЕ убивая
   запущенный постгресс — «жертва попытки» остаётся жить (дополняет 2–3).

Примечание-уточнение к формулировке роадмапа: «OK-пометка при живом
процессе» — не дефект записи: OK поставлен потоковой verify №1, которая
реально завершилась (постгресс ей не нужен); живой постгресс принадлежал
drill-операции. ВременнАя близость — следствие гонки verify × drill за общий
ops-реестр (штатный 409-гвард). Дефект Д1 — именно жизненный цикл постгресса
restore/drill-операции (пункты 1–4 выше).

### 2.3 Д2 — корневая механика (код `src/PgWorker.Backups/Process/BackupVerifyProcess.cs`, `RestoreProcess.cs`)

- API приёмника отдаёт снимок ОБЩЕГО ops-реестра через оба маршрута
  (`GET /api/ops/verify` и `GET /api/ops/restore` — один `OpsStatus`),
  снимок содержит `id` (`OperationSnapshot.Id`; DTO `ReceiverStatus.Id`
  на воркере уже парсится).
- `RestoreDrillProcess` сверку делает (строка ~358: «foreign snapshot →
  relaunching via the vanished path» — прецедент t98, канон g-drill §1).
- `BackupVerifyProcess.TickShardAsync` сверки НЕ делает: любой терминальный
  снимок считается итогом проверки кандидата → чужой failed записывается в
  полный (наблюдённое), чужой succeeded может дать ложный OK.
- `RestoreProcess.SuperviseRestoreOpAsync` сверки НЕ делает: чужой
  succeeded (без system_id) переводит заявку в REJOINING с `SystemId=null`
  (запирает bootstrap); чужой failed/cancelled роняет/сносит заявку.

## 3. Принципы

- Источник истины — `arch/`: правила жизненного цикла постгресса и сверки id
  фиксируются в каноне ПЕРВОЙ правкой, код зеркалит.
- Максимальная надёжность без P/Invoke: управление постгрессом — только
  штатными утилитами (`pg_ctl status/stop/start`), файловые проверки —
  штатный .NET API.
- Общий ops-реестр остаётся одним на процесс приёмника (канон не меняется);
  все различия «своя/чужая» операция — на стороне супервизоров воркера по
  `id` снимка.
- Схема именования drill-каталога не меняется: `backup-drill/<id>` (id
  уникален на операцию; повторное использование — штатный vanished-перезапуск
  той же операции). Защиту обеспечивает остановка сироты, а не переименование.
- Терминальный исход операции не блокируется чисткой: неудалённый остаток
  (нестопаемый постгресс) подбирается следующим запуском через защиту
  каталога; терминальные семантики (failed/cancelled/succeeded) неизменны.
- Никаких изменений бюджетов (таймаутов) системы без отдельного согласования.

## 4. Arch-first: правки канона (первая фаза работ)

### 4.1 `arch/pgnode/06-restore-drill.md` — механика (главная правка)

Дополнить §2 (механика restore; drill по §3 наследует) пунктом «жизненный
цикл postmaster» после описания подготовки и перед `pg_ctl start`:

1. **Защита целевого каталога перед стартом.** Перед `pg_ctl start`
   (и restore в PGDATA ноды, и drill в scratch) приёмник проверяет каталог:
   `pg_ctl status -D <pgdata>` — postmaster работает (прошлая инкарнация
   операции, пережившая рестарт приёмника) → `pg_ctl stop -m fast -w` с
   коротким бюджетом, при неудаче — `pg_ctl stop -m immediate -w`; не
   остановился — операция failed с честной причиной (не бороться с
   нестопаемым). Postmaster не работает → остаточный `postmaster.pid`
   (если файл остался) удаляется.
2. **Лог операции.** `/tmp/restore-pg.log` очищается перед `pg_ctl start`
   операции: диагностика провала старта (хвост лога в `last_error`) содержит
   строки только своей инкарнации.
3. **Остановка при любом исходе.** При завершении операции в ЛЮБОМ исходе
   (успех, провал любой фазы после старта постгресса, отмена, провал старта)
   постгресс операции остановлен (`pg_ctl stop -m fast -w`, при неудаче
   `-m immediate -w`, короткие бюджеты) ДО чистки scratch (drill) и ДО
   возврата Patroni (restore). Неудалённый остаток каталога (нестопаемый
   постгресс) НЕ блокирует терминальный исход — каталог дочистится следующим
   запуском через защиту каталога (п. 1).
4. **Рестарт приёмника.** Остановка процесса приёмника (supervisord) гасит
   активную ops-операцию: отмена (kill дерева child-процессов runner'ом) +
   остановка постгресса операции с ожиданием — сирот приёмник не оставляет.
   Жёсткий SIGKILL вне контроля — остаток закрывает защита каталога (п. 1)
   при следующем запуске.

### 4.2 `arch/pgnode/processes/d-restore.md` и `e-drill.md` — процессы

- `d-restore.md` §1 (фазы), шаг recovering: одной строкой — старт в
  подготовленный каталог (защита от постгресса прошлой инкарнации), при
  любом исходе постгресс остановлен до чистки/возврата Patroni; ссылка на
  06-restore-drill §2 (механика). Аналогичная строка в `e-drill.md` §2
  (чистка scratch — ПОСЛЕ остановки постгресса).
- Без дублирования деталей: механика живёт в 06 (уровень 3), процессы
  (уровень 4) держат инвариант и ссылку.

### 4.3 `arch/backups/01-full-backup.md` §7 + `arch/pgnode/processes/c-verify.md` — сверка id при verify

Симметрично g-drill §1 дополнить: ops-реестр приёмника один на процесс —
GET `/api/ops/verify` может вернуть чужой/прошлый снимок (напр., терминал
restore/drill-операции с текстом pg_ctl). Снимок с чужим `id` —
доказательства итога проверки НЕТ (ни OK, ни FAILED): vanished-семантика
(повторный POST идемпотентно поверх терминального снимка; чужой running —
ожидание освобождения реестра). В c-verify.md — строка в «Исходы».

### 4.4 `arch/backups/processes/f-restore.md` §2 — сверка id при restore

В автомате, где описан 409-гвард запуска («снимок 409 не различает
свою/чужую»), дополнить: супервиз операции сверяет `id` снимка с backup_id
выбранного полного заявки; чужой id в любом состоянии — чужой итог/фаза:
вердиктов и переходов фаз по нему нет (REJOINING от чужого succeeded без
system_id запрещён), запуск откладывается до освобождения реестра
(transient, семантика 409).

## 5. Структура/компоненты (кодовые правки, зеркалят канон)

### 5.1 `src/PgWorker.PgNode/Operations/RestoreOperation.cs`

- Новый приватный асинхронный метод (имя ориентировочное —
  `EnsurePostmasterDownAsync(string pgdata, CancellationToken token)`):
  `pg_ctl status -D` → работает: `stop -m fast -w -t 10` → повторный status →
  работает: `stop -m immediate -w -t 10` → повторный status → работает:
  вернуть отказ; не работает: удалить `postmaster.pid`, если файл существует.
  Все вызовы через `_runner` (шов тестов), бюджеты внутри 30 с.
- Вызов защиты ПЕРЕД циклом `pg_ctl start` (обе операции: restore и drill)
  и очистка `/tmp/restore-pg.log` там же (до первой попытки старта — хвост
  всех попыток инкарнации сохраняется для диагностики).
- `finally`: для drill — ПЕРЕД `Directory.Delete` вызвать
  `EnsurePostmasterDownAsync(CancellationToken.None)` (best-effort, ошибки
  остановки не маскируют терминальный исход); для restore — ПЕРЕД
  `supervisorctl start spilo`.
- Ветка финального провала старта («pg_ctl start failed … tail»): перед
  `Fail` — best-effort остановка жертвы попытки (закрывается общим
  finally-стопом; отдельный вызов не обязателен, но допустим для симметрии
  с ретрай-веткой).

### 5.2 `src/PgWorker.PgNode/Program.cs` (MapOpsRoutes)

- Подписка на `app.Lifetime.ApplicationStopping`: при остановке хоста —
  `active?.CancelAsync()` с синхронным ожиданием завершения (общий бюджет
  ожидания ≤ 15 с; `RestoreOperation.CancelAsync` уже гасит CTS → kill дерева
  child-процессов + `pg_ctl stop -m fast` — постгресс операции остановлен).
  Verify-операция дочерних процессов не имеет — CancelAsync безвреден.
- Локальную переменную `active` не реорганизовывать в новый DI-сервис (минимальная
  правка), если реализация не потребует иного.

### 5.3 `src/PgWorker.Backups/Process/BackupVerifyProcess.cs`

- В `TickShardAsync` после получения `snapshot` и ДО switch по состоянию —
  сверка: `snapshot.Id != full.Id` → чужой снимок: warning в host-лог +
  journal-фаза `foreign-snapshot/<shard>/<id>` (по образцу
  RestoreDrillProcess) → реланч-семантика vanished: повторный
  `StartVerifyAsync` (приёмник примет поверх терминального; чужой running
  даст 409 — принято, тик повторит); verify-статус кандидата не трогаем.
- Существующие ветки switch не меняются (для СВОЕГО id поведение прежнее).

### 5.4 `src/PgWorker.Backups/Process/RestoreProcess.cs`

- В `SuperviseRestoreOpAsync` сразу после получения `status` — сверка
  `status.Id` с backup_id выбранного полного заявки (поле заявки, по которому
  запускалась операция; точное имя поля — по коду `RestoreOperationState`):
  чужой id → transient-ожидание (статус/фаза заявки не трогаются, никаких
  переходов REJOINING/FAILED; journal-фаза `foreign-snapshot/<shard>/<id>`;
  запуск повторится штатной 409/vanished-механикой фазы demolished).
- Чужой `running` — тоже ожидание (реестр занят чужой операцией).

### 5.5 Тесты

Юниты (`src/tests/PgWorker.UnitTests/PgNode/RestoreOperationTests.cs`,
мок `IProcessRunner`; комментарии по AAA):

- orphan-running: `pg_ctl status` → работает; остановлен fast; старт
  продолжился; каталог пережил чистку корректно;
- stale-pid: status → не работает; `postmaster.pid` удалён;
- unstoppable-orphan: status → работает; fast и immediate не помогли →
  failed-снимок с причиной остановки, старт не выполнялся;
- drill-finally-stop: провал операции после старта постгресса → перед
  удалением scratch выполняется stop (порядок вызовов мока).

Интеграционные (`src/tests/PgWorker.IntegrationTests/Backups/`):

- `BackupVerifyProcessTests.cs`: чужой failed-снимок (id ≠ кандидата) →
  статус кандидата не изменён (PENDING), выполнен повторный POST; чужой
  succeeded → не OK; свой снимок — прежнее поведение (регресс-гвард);
- `RestoreProcessTests.cs`: чужой succeeded (id ≠ backup_id заявки, без
  system_id) → заявка НЕ в REJOINING, статус не тронут; чужой failed →
  заявка не FAILED.

E2E (`src/tests/PgWorker.E2eTests/E2e/E2eDrillScenarios.cs`, новый сценарий,
полная изоляция по канонам `arch/testing/02-e2e-isolation.md`):

- `Drill_ReceiverRestart_PostmasterNotLeaked`: дождаться активного дрилла
  (фаза recovering), `supervisorctl stop pgnode` → `supervisorctl start
  pgnode` первой ноды, дождаться терминального итога дрилла. Ассерты:
  (а) итог дрилла достигнут и его причина — СВОЯ (не «another server might
  be running» / «lock file postmaster.pid»);
  (б) в логах контура нет FATAL «another postmaster»;
  (в) на ноде нет постгресса-сироты в drill-каталоге (проверка факта, не
  таймингов — напр. `pg_ctl status` каталога / отсутствие процесса);
  (г) scratch-каталог дрилла отсутствует после терминала;
  (д) verify-статусы полных кластера не содержат чужих FAILED (нет ошибки
  операции дрилла в записях полных).
- Существующие `Drill_Succeeds_CleansUp` (чистая ветка) и
  `Backup_ChainBroken_SelfHeals` (сценарий-свидетель) — прогон без правок.

### 5.6 Роадмап

Мерж-гейт задачи (правило `arch/roadmap/README.md` и AGENTS.md): тег
`t103-verify-postmaster-leak-registry` удаляется из
`arch/roadmap/bugs.md` ТЕМ ЖЕ мерж-коммитом (папка логов
`bugs/t103-…` вне git — остаётся на месте).

## 6. Фазы работ

1. **Канон**: правки `arch/` по §4 (06-restore-drill, d-restore, e-drill,
   01-full-backup §7, c-verify, f-restore §2). Коммит в feature-ветке.
2. **Код приёмника**: RestoreOperation (защита каталога, очистка лога,
   finally-стоп) + Program.cs (ApplicationStopping → CancelAsync).
3. **Код воркера**: сверка id в BackupVerifyProcess и RestoreProcess.
4. **Тесты**: юниты RestoreOperation, интеграционные BackupVerifyProcess /
   RestoreProcess, новый E2E-сценарий.
5. **Верификация** (мерж-гейт; сборка и тесты — двумя ОТДЕЛЬНЫМИ командами):
   1. `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`
      (статическая подготовка E2E одним процессом ДО тестов);
   2. `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`;
   3. `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter "FullyQualifiedName~Drill_|FullyQualifiedName~Backup_ChainBroken_SelfHeals"`.
   Плюс юниты/интеграционные серии затронутых проектов (`PgWorker.UnitTests`,
   `PgWorker.IntegrationTests`) c зачисткой контейнеров между сериями.
6. **Роадмап-гейт**: удаление тега t103 из bugs.md мерж-коммитом.

## 7. Ограничения

- P/Invoke запрещён; только штатные утилиты PG и управляемый .NET API.
- Бюджеты (таймауты) существующие не меняются; новые бюджеты остановки
  короткие (stop ≤ 10 с на попытку, суммарно ≤ 30 с; shutdown-ожидание
  ≤ 15 с) — согласованы этим spec.
- Полный бэкап (`FullBackupOperation`, свой реестр) и его дети при
  остановке приёмника — ВНЕ области задачи; при необходимости — отдельный
  пункт роадмапа.
- Поведение общего ops-реестра (одна ops-операция на процесс, 409-гвард,
  TryBegin поверх терминального) не меняется.
- Схема именования drill-каталога (`backup-drill/<id>`) и путь
  `/tmp/restore-pg.log` (телеметрия E2E снимает этот файл) не меняются —
  лог только очищается перед стартом операции.
- Рестарта упавших прогонов «для выяснения» не выполнять; разбор — по
  собранным логам.

## 8. Принятые решения (закрытые развилки; все подтверждены пользователем)

1. **Объём**: чинить ОБА дефекта (Д1 + Д2) в одном флоу — они сцеплены
   (Д2-сценарий порождён постгрессом-сиротой Д1).
2. **Каталог drill**: имя НЕ менять — каталог уже уникален на операцию
   (id дрилла); повторное использование имени — штатный vanished-перезапуск
   той же операции. Закрывается защитой каталога (остановка сироты + снятие
   остаточного pid), а не переименованием.
3. **Сверка id**: в ОБЕИХ процессах — BackupVerifyProcess (буква задачи) и
   RestoreProcess (найденная при разборе дыра той же природы — REJOINING от
   чужого succeeded).
4. **E2E-гейт**: точечные существующие кейсы (`Drill_Succeeds_CleansUp`,
   `Backup_ChainBroken_SelfHeals`) на свежем Release + НОВЫЙ
   регресс-сценарий `Drill_ReceiverRestart_PostmasterNotLeaked`.
5. **Роадмап-пункт**: тег t103 удаляется из `arch/roadmap/bugs.md` тем же
   мерж-коммитом (правило AGENTS.md/README роадмапа — однозначно).
6. **«OK при живом процессе»** из формулировки роадмапа — закрыт как
   НЕ-дефект записи (уточнение диагностики, §2.2 примечание): OK поставлен
   закончившейся потоковой verify; постгресс принадлежал drill-операции.
7. **Чужой running-снимок** (verify/restore-супервиз): ожидание освобождения
   реестра, POST не спамить лишний раз кроме реланч-пути по образцу drill
   (409 дёшев; единообразие с RestoreDrillProcess важнее микросэкономии).
8. **Нестопаемый постгресс**: честный failed операции с причиной; НЕ
   блокировать терминальный исход чисткой; остаток каталога подбирается
   следующим запуском (защита каталога). Выбрано как максимально надёжное
   без зависаний.
9. **Shutdown приёмника**: гасится активная ops-операция (CancelAsync +
   ожидание ≤ 15 с); full-операция — вне области (см. §7).

## 9. Критерии приёмки

1. Канон `arch/` обновлён (§4) и закоммичен ДО кодовых правок; код
   зеркалит канон (ревью сверяет по пунктам §4 ↔ §5).
2. Юнит-тесты RestoreOperation закрывают все новые ветки (orphan/stale-pid/
   unstoppable/finally-порядок); интеграционные — сверку id обоих процессов
   (чужой failed/succeeded не меняет статус; свой — прежнее поведение).
3. Новый E2E `Drill_ReceiverRestart_PostmasterNotLeaked` зелёный; в его
   прогоне отсутствуют FATAL «another postmaster» и чужие verify-FAILED в
   записях полных; scratch дрилла отсутствует после терминала.
4. Гейт-прогон §6.5 зелёный (E2ePrepare → build → точечные E2E на свежем
   Release, двумя отдельными командами, `--no-build`; юниты+интеграция
   затронутых проектов; зачистка контейнеров/сетей между сериями).
5. Тег `t103-verify-postmaster-leak-registry` удалён из
   `arch/roadmap/bugs.md` тем же мерж-коммитом; исторических упоминаний
   задачи в `AGENTS.md`/`arch/**`/`docs/**` (кроме `docs/superpowers/`)
   не осталось.
6. Бюджеты системы не изменены; P/Invoke отсутствует; языковые конвенции
   (документация — русский, идентификаторы — английский) соблюдены.
