# t103: verify/restore-check PgNode — жизненный цикл postmaster и сверка id итога операции

Spec задачи `t103-verify-postmaster-leak-registry` (вводная — пункт в
`arch/roadmap/bugs.md`, логи прогона — `arch/roadmap/bugs/t103-verify-postmaster-leak-registry/`
в главном репо, вне git; пункт роадмапа удалён этой веткой). Настоящая
редакция — ДОРАБОТКА по итогам независимого MR-ревью
(`.my/review_fix-t103-verify-postmaster-leak-registry.md`, находки #1–#4):
первая реализация (коммиты `746e74d1..c3548814`) прошла прогоны, но ревью
нашло blocker и 3 major. Объём доработки — ТОЛЬКО #1–#4 (blocker + major);
minors/nits (#5–#36) — ВНЕ объёма, не трогаются. Исходные диагнозы
подтверждены по логам прогона (лог ноды
`container-pgw-bksprd9eeeea8-shard1-shard1a.log`, postmaster-лог
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

Д1/Д2 уже закрыты первой реализацией ветки; MR-ревью вскрыло в ней регресс и
три пробела — они и есть цель доработки (все — по каноничной семантике
`arch/`, код обязан зеркалить):

- **Р1 [#1, blocker] — останов приёмника публикует операторскую отмену.**
  Shutdown-хук `ApplicationStopping` зовёт публичный `CancelAsync()` — задача
  операции публикует `state=cancelled, error="cancelled by operator"`, а
  Kestrel ещё отдаёт этот снимок. Воркер исполняет разрушительную
  операторскую ветку: restore — снос ноды + удаление заявки; drill — удаление
  ключа; verify — PENDING с отсрочкой `verify.interval_sec`. Штатный
  `supervisorctl stop|restart pgnode` (деплой, E2E) посреди операции
  срабатывает как несуществовавшая операторская отмена.
- **Р2 [#2, major] — `active` не обнуляется после завершения операции.**
  Хук/cancel-маршрут зовут `CancelAsync` завершённой restore-операции —
  безусловный `pg_ctl stop -m fast` по ПРОД-PGDATA ноды (восстановленный
  Patroni-постгресс): любой последующий stop/restart pgnode на ноде, где
  когда-либо завершался restore, гасит прод-постгресс.
- **Р3 [#3, major] — чужой снимок в RestoreProcess «ждёт» без выхода.**
  Ветка чужого id возвращает `TransientAsync` БЕЗ сброса `Phase`: для фаз
  `launching/downloading/recovering` диспетчер — только GET-поллинг, повторной
  отправки POST нет; персистентный чужой снимок → ожидание до
  `JobRestoreTimeoutSec = 86400` → FAILED + снос ноды. Spec §5.4 первой
  редакции уже обещал «запуск повторится штатной 409/vanished-механикой фазы
  demolished» — код не делает.
- **Р4 [#4, major] — сверка «только по id» не различает вид операции.**
  id verify-снимка = `backup_id` кандидата, id restore-снимка = `id ??
  backup_id`: для ОДНОГО полного оба снимка имеют ОДИН id, а оба GET-маршрута
  отдают один `registry.Current`. Вид операции в контракте нет: verify
  принимает restore-снимок как свой (ложный OK/FAILED), restore принимает
  verify-снимок (running→фаза verifying; succeeded→REJOINING `SystemId=null`;
  failed→FAILED; cancelled→удаление заявки).

Порядок работ: ПЕРВОЙ правится `arch/` (уточнение канона), затем код
зеркалит.

## 2. Диагноз (подтверждённый разбор по уликам)

### 2.1 Хронология исходного прогона (нода shard1a, все времена UTC)

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
| ~06:03:03.8 | B: failed-снимок `pg_ctl start failed: another server might be running…` + хвост лога; finally чистит scratch `Directory.Delete` ПРИ ЖИВОМ постгрессе 331 | код `RestoreOperation.cs` |
| 06:03:04.71 | Постгресс 331: `restore_command` теряет cwd (`sh: getcwd() failed`) → `FATAL: WAL ends before consistent recovery point` → 06:03:04.74 postmaster 331 умирает | postmaster-лог |
| 06:03:06–09 | Полный №2 `20261106060306Z` COMPLETED (воркер переснимает после WAL_CHAIN_ERROR — штатно) | etcd-дамп |
| 06:03:10.72 | BackupVerifyProcess (кандидат — полный №2, PENDING on_create): `GET /api/ops/verify` → OpsStatus отдал Current общего реестра — failed-снимок **B** (id `20261106060228Z`, phase recovering, error «pg_ctl start failed…» с путём каталога A/B) → воркер **без сверки id** пишет в ПОЛНЫЙ №2 `verify=FAILED` текстом чужой drill-операции | воркер-лог, etcd-дамп |

### 2.2 Д1 — корневые механики (код `src/PgWorker.PgNode/Operations/RestoreOperation.cs`, `Program.cs`)

1. **Рестарт приёмника не гасит операцию.** Операции запускаются
   `Task.Run(() => operation.RunAsync(CancellationToken.None))` (Program.cs,
   MapOpsRoutes); при остановке хоста pgnode никто не отменяет задачу и не
   убивает дочернее дерево (`ProcessRunner` убивает дерево только по
   cancellation token). Канон предусматривает рестарт приёмника
   («GET 404 vanished» в c-verify §3 / g-drill §1), но не описывал гашение
   процессов операции при остановке.
2. **Чистка scratch без остановки постгресса.** В `finally` drill-ветка
   делала `Directory.Delete(pgdata, recursive)` сразу; остановка постгресса
   была только в ЧАСТИ fail-путей и отсутствовала в самом finally перед
   удалением; для restore-ветки возврат Patroni в finally происходил без
   гарантии остановки постгресса операции.
3. **Повторный запуск не защищён от сироты в каталоге.** Vanished-перезапуск
   (штатная семантика) стартует pg_ctl в каталог, который может удерживать
   живой постгресс прошлой инкарнации, — без предварительной проверки
   `postmaster.pid`/`pg_ctl status`.
4. `pg_ctl start -w` при исчерании старт-ожидания возвращает fail, НЕ убивая
   запущенный постгресс — «жертва попытки» остаётся жить.

Примечание-уточнение к формулировке роадмапа: «OK-пометка при живом
процессе» — не дефект записи: OK поставлен потоковой verify №1, которая
реально завершилась; живой постгресс принадлежал drill-операции. Дефект Д1 —
именно жизненный цикл постгресса restore/drill-операции (пункты 1–4).

### 2.3 Д2 — корневая механика (код `src/PgWorker.Backups/Process/BackupVerifyProcess.cs`, `RestoreProcess.cs`)

- API приёмника отдаёт снимок ОБЩЕГО ops-реестра через оба маршрута
  (`GET /api/ops/verify` и `GET /api/ops/restore` — один `OpsStatus`),
  снимок содержит `id` (`OperationSnapshot.Id`; DTO `ReceiverStatus.Id`
  на воркере парсится).
- `RestoreDrillProcess` сверку делает (строка ~358: «foreign snapshot →
  relaunching via the vanished path» — прецедент t98, канон g-drill §1).
- `BackupVerifyProcess` и `RestoreProcess` сверки НЕ делали: чужой failed
  записывался в полный, чужой succeeded переводил заявку в REJOINING с
  `SystemId=null`.

### 2.4 Регрессии и пробелы первой реализации (MR-ревью #1–#4; подтверждено по коду ветки c3548814)

1. **Р1 — shutdown публикует `cancelled`.** Механика:
   `Program.cs` → `ApplicationStopping.Register` → `active?.CancelAsync()` →
   `RestoreOperation._cts.Cancel()` → catch OCE →
   `Update(Cancelled, phase, error: "cancelled by operator")`
   (`RestoreOperation.cs:411-419`; у VerifyOperation — симметричная ветка
   `:212-216`). `StopApplication` выполняет колбэки `ApplicationStopping`
   синхронно ДО остановки Kestrel: пока хук блокирован (стоп постгресса до
   10 с), GET отдаёт воркеру снимок `cancelled` → воркер исполняет ветки
   «оператор отменил» (`RestoreProcess.cs:835-867` — RemoveNodeAsync + снятие
   щита + удаление заявки; `RestoreDrillProcess.cs:437-456` — удаление ключа
   дрилла; `BackupVerifyProcess.cs:286-308` — PENDING с отсрочкой). Канон
   же требует: остановка приёмника → процесс умер → GET 404 → vanished →
   повтор POST (c-verify §3, 06-restore-drill §2). Метки «shutdown vs
   оператор» в контракте нет.
2. **Р2 — `active` не обнуляется.** `active` присваивается только в
   POST-хендлерах (`Program.cs:346,375`); реестр после завершения хранит
   терминальный снимок. `CancelAsync` не различает running/терминал:
   `_cts.Cancel()` безвреден, но `StopBestEffortAsync(_pgdata)` для restore —
   `pg_ctl stop -m fast` по прод-PGDATA (`RestoreOperation` конструируется с
   `pgdata = NodePaths.Pgdata`). Тот же staleness — в cancel-маршруте
   (`Program.cs:384-391`, pre-existing); shutdown-хук делает его
   автоматическим на КАЖДЫЙ штатный останов.
3. **Р3 — чужой снимок без выхода.** `RestoreProcess.SuperviseRestoreOpAsync`
   (`:747-757`) при чужом id → `TransientAsync` без изменения `Phase`.
   Инвариант диспетчера (`:451-457`): в фазах
   `launching/downloading/recovering` команду НЕ повторяют (только
   GET-поллинг) — повтор только ветками vanished (404) и failed+downloading,
   обе сбрасывают `Phase="demolished"` → `LaunchStepAsync` повторяет POST.
   Чужой снимок персистентен (терминал чужой операции остаётся в реестре) →
   заявка висит до `JobRestoreTimeoutSec = 86400` → `restore-job-timeout` +
   снос ноды. Новый тест `RestoreProcessTests` кодифицировал ошибочное
   ожидание (`Phase == "recovering"`, `StartCalls == 0`).
4. **Р4 — id без вида операции.** `Program.cs:339` кладёт в TryBegin verify
   id `body.BackupId`; `:369` — restore id `body.Id ?? body.BackupId`;
   `VerifyOperation.Update` несёт `_request.BackupId`. Для restore без
   drill-id снимки обеих операций ОДНОГО полного совпадают по id, а
   `/api/ops/verify` и `/api/ops/restore` отдают один `registry.Current` —
   сверка `id ==` пропускает снимок чужого вида (ложные вердикты/переходы,
   см. §1 Р4). Уровень-2 канон (`arch/pgnode/02-api.md` §3: «исход ТОЛЬКО по
   снимку с собственным id») неполон: равенство id не доказывает «своя
   операция».

## 3. Принципы

- Источник истины — `arch/`: правила жизненного цикла постгресса, сверки
  снимка и разделения каналов отмены фиксируются в каноне ПЕРВОЙ правкой,
  код зеркалит.
- Максимальная надёжность без P/Invoke: управление постгрессом — только
  штатными утилитами (`pg_ctl status/stop/start`), файловые проверки —
  штатный .NET API.
- Общий ops-реестр остаётся одним на процесс приёмника (канон не меняется);
  различия «своя/чужая» операция — на стороне супервизоров воркера по
  `id` И виду операции (`kind`) снимка.
- **Остановка приёмника ≠ операторская отмена.** Гашение операции при
  остановке хоста — внутренний канал: снимок НЕ перезаписывается
  (`cancelled` публикует ТОЛЬКО внешний операторский канал cancel API);
  после выхода процесса реестра нет — воркер получает транспортный отказ
  (transient), затем GET 404 → vanished-семантика → повтор POST. Воркер
  никогда не исполняет операторскую ветку по факту остановки приёмника.
- **Отмена/гашение применяется только к ЖИВОЙ операции.** Завершённая
  операция (терминальный снимок) повторного CancelAsync/стопа не получает —
  ссылка на активную операцию живёт ровно до завершения её задачи.
- Схема именования drill-каталога не меняется: `backup-drill/<id>`;
  защиту обеспечивает остановка сироты, а не переименование.
- Терминальный исход операции не блокируется чисткой: неудалённый остаток
  подбирается следующим запуском через защиту каталога; терминальные
  семантики (failed/cancelled/succeeded) неизменны.
- Никаких изменений бюджетов (таймаутов) системы без отдельного
  согласования.

## 4. Arch-first: правки канона (первая фаза работ)

Канон Д1/Д2 уже обновлён первой реализацией (коммиты `cb6f51c7`,
`87795d4b`: 06-restore-drill §2 п.5 «жизненный цикл postmaster», d-restore
§1, e-drill §2, 01-full-backup §7, c-verify §2, f-restore §2 — сверка id).
Доработка УТОЧНЯЕТ эти же файлы под Р1–Р4.

### 4.1 `arch/pgnode/06-restore-drill.md` §2 п.5.4 — остановка приёмника ≠ отмена (Р1, Р2)

Переформулировать п.5.4 («Остановка приёмника»):

- Остановка процесса приёмника гасит активную ops-операцию: отмена задачи
  (runner убивает дерево child-процессов) + остановка postmaster операции с
  ожиданием (≤ 15 с) — сирот приёмник не оставляет. Гашение — ВНУТРЕННИЙ
  канал, отдельный от операторской отмены: снимок операции НЕ перезаписывается
  (никакого `cancelled`), реестр умирает вместе с процессом. Операторская
  отмена (`POST /api/ops/cancel`, внешний канал) — прежняя семантика
  `state=cancelled` («операции не было»: воркер удаляет заявку/ключ дрилла,
  verify — PENDING с отсрочкой). Жёсткий SIGKILL вне контроля — остаток
  закрывает защита каталога (п. 5.1) при следующем запуске.
- Отмена/гашение применяется ТОЛЬКО к живой (running) операции; завершённая
  операция (терминальный снимок) не отменяется и не останавливается
  повторно — ссылка на активную операцию прекращает действовать по
  завершении задачи операции.

### 4.2 `arch/pgnode/02-api.md` §3 — вид операции в контракте снимка (Р4; таблица значений)

В абзац «GET-снимок несёт `id` операции реестра» внести:

- Снимок несёт `id` И `kind` — вид операции реестра. Таблица значений
  `kind` (заполняет приёмник при TryBegin/Update):

  | kind | Операция | Источник id снимка |
  |---|---|---|
  | `verify` | потоковая проверка полного (`POST /api/ops/verify`) | `backup_id` команды |
  | `restore` | восстановление в PGDATA ноды (`POST /api/ops/restore` без `id`) | `backup_id` команды |
  | `drill` | drill-восстановление в scratch (`POST /api/ops/restore` с `id`) | `id` команды |

- Супервизоры принимают исход ТОЛЬКО по снимку своей операции: собственный
  `id` И собственный `kind` (правила супервизов — 01-full-backup §7,
  f-restore §2, g-drill §1). Снимок с чужим `id`/`kind`, а также снимок БЕЗ
  `kind` (контракт старого приёмника) — доказательства итога НЕТ: воркер и
  приёмник поставляются одним контуром и обновляются одновременно, снимок
  без вида операции трактуется как чужой (строгая трактовка: ложный вердикт
  невозможен, в худшем случае запуск откладывается до совпадения версий).
- Отмена (cancel API) применяется к running-операции; остановка приёмника
  снимок не перезаписывает (см. 06-restore-drill §2 п.5.4).

### 4.3 `arch/backups/01-full-backup.md` §7 + `arch/pgnode/processes/c-verify.md` §2 — сверка verify (Р4)

- 01-full-backup §7, правило сверки снимка: чужим является снимок с чужим
  `id` ИЛИ чужим `kind` (не `verify`) ИЛИ без `kind` — доказательства итога
  проверки НЕТ (ни OK, ни FAILED): vanished-семантика (повторный POST
  идемпотентно поверх терминального; чужой running — 409, ожидание
  освобождения реестра).
- c-verify §2, «Исходы»: строку про чужой `id` дополнить — чужой `kind`
  (restore/drill того же полного) и отсутствие `kind` — тоже «не итог
  проверки», супервиз перезапускает операцию vanished-семантикой (ссылка на
  01-full-backup §7 сохраняется).

### 4.4 `arch/backups/processes/f-restore.md` §2 — сверка и demolished-повтор restore (Р3, Р4)

В абзац сверки id (добавленный первой реализацией) внести два уточнения:

- Чужой снимок = чужой `id` ИЛИ чужой `kind` (не `restore`) ИЛИ отсутствие
  `kind`: вердиктов и переходов по нему нет (REJOINING от чужого succeeded
  без system_id запрещён).
- Чужой снимок в ЛЮБОМ состоянии (в т.ч. чужой running) — сброс фазы заявки
  в `demolished` + transient: команда повторяется следующим тиком
  (симметрично ветке vanished 404; повторный POST при живой чужой операции
  получает 409 — семантика освобождения реестра; по терминалу чужой — 202,
  своя операция стартует заново, резюмов нет — идемпотентно). Ожидание
  чужого снимка БЕЗ сброса фазы запрещено: диспетчер в фазах
  launching/downloading/recovering команду не повторяет — заявка висела бы
  до статусного бюджета.

### 4.5 Согласование без правок

- `g-drill.md` §1: сверка дрилла остаётся по `id` (id дрилла уникален самой
  операции, не совпадает с id полного) — правка не требуется.
- `d-restore.md`/`e-drill.md`: инварианты первой реализации остаются;
  shutting-down-семантика — уровень механики 06, процессы не дублируют.

## 5. Структура/компоненты (кодовые правки, зеркалят канон)

Уже в ветке (первая реализация, НЕ переWorkывается): защита каталога
pre-start, очистка `/tmp/restore-pg.log`, finally-стоп, сверка id в обоих
процессах. Доработка — точечные правки поверх.

### 5.1 `src/PgWorker.PgNode/Operations/` — контракт вида операции и канал гашения

- `IReceiverOperation`: новый метод `Task ShutdownAsync()` — внутренний канал
  гашения при остановке хоста (рядом с операторским `Task CancelAsync()`).
- `OperationSnapshot`: новый опциональный параметр `string? Kind = null`
  (последним — существующие вызовы не ломаются); full-операция kind не
  заполняет (null), ops-операции — всегда.
- `RestoreOperation`:
  - приватное поле `_shutdown` (volatile-запись), выставляется в
    `ShutdownAsync` ДО `_cts.Cancel()`;
  - `ShutdownAsync`: `_shutdown = true; _cts.Cancel(); await
    StopBestEffortAsync(_pgdata);` (тело гашения то же, что CancelAsync,
    различие — флаг и семантика публикации; общий приватный core-метод
    допустим);
  - catch OCE: публиковать `Update(Cancelled, …, "cancelled by operator")`
    ТОЛЬКО при `!_shutdown`; при shutdown — без Update (finally выполняет
    стоп постгресса и возврат Patroni как при любом исходе);
  - каждый `Update`/`Fail` несёт `Kind: _request.Id is null ? "restore" :
    "drill"` (kind определяется самой операцией, не состоянием реестра).
- `VerifyOperation`: симметрично — `_shutdown`, `ShutdownAsync`
  (`_shutdown = true; _cts.Cancel(); return Task.CompletedTask;`), catch OCE
  публикует `cancelled` только при `!_shutdown`; `Update`/`Fail` несут
  `Kind: "verify"`.
- `ApiContracts.OpsStatusResponse`: новое поле
  `[JsonPropertyName("kind")] string? Kind = null`; `OpsStatus` передаёт
  `snapshot.Kind`.

### 5.2 `src/PgWorker.PgNode/Program.cs` (MapOpsRoutes) — активная операция и хук

- Вместо одиночной `active` — пара `IReceiverOperation? active = null;
string? activeId = null;` (activeId — id TryBegin-снимка запущенной
операции: `body.BackupId` для verify, `body.Id ?? body.BackupId` для
restore/drill).
- TryBegin-снимки получают kind: verify → `"verify"`; restore → `body.Id
  is null ? "restore" : "drill"` (канон 02-api §3, таблица §4.2).
- Обнуление по завершении: задача запускается с continuation —
  `Task.Run(() => operation.RunAsync(CancellationToken.None)).ContinueWith(_ =>
  { if (ReferenceEquals(active, operation)) { active = null; activeId = null; } })`
  (обнуление только СВОЕЙ ссылки — не затирает более новую операцию).
- Гвард живой операции (общий для хука и cancel-маршрута): операция
  отменяема ТОЛЬКО если `active is not null && registry.Current is {
  State: OperationStates.Running } cur && cur.Id == activeId` — running-снимок
  именно этой операции (канон 06 §2 п.5.4, §4.1).
- Shutdown-хук `ApplicationStopping`: при выполненном гварде —
  `active.ShutdownAsync()` с ожиданием ≤ 15 с (прежний бюджет; без гварда —
  ничего не делает: завершённая операция не трогается, снимок не
  перезаписывается — воркер после смерти процесса идёт vanished-путём
  404 → повтор POST).
- Cancel-маршрут `/api/ops/cancel`: тот же гвард; не выполнен → 404 «no
  active operation» (в т.ч. после терминала операции — active обнулён);
  выполнен → прежний `await active.CancelAsync()` → 202 «cancelling».

### 5.3 `src/PgWorker.Backups/WalReceiverApiClient.cs` + `BackupVerifyProcess.cs`

- `ReceiverStatus`: новое поле `[JsonPropertyName("kind")] string? Kind =
  null` (парсинг GET/409-тел приёмника).
- Сверка в `TickShardAsync` (расширение существующей): чужой снимок =
  `!string.Equals(snapshot.Id, full.Id, …) || snapshot.Kind != "verify"`
  (в т.ч. `Kind is null`); поведение чужой ветки прежнее — warning,
  journal-фаза `foreign-snapshot/<shard>/<id>`, реланч POST
  vanished-семантикой (409 принят), verify-статус кандидата не трогаем.

### 5.4 `src/PgWorker.Backups/Process/RestoreProcess.cs`

- Сверка в `SuperviseRestoreOpAsync`: чужой снимок =
  `!string.Equals(status.Id, op.BackupId, …) || status.Kind != "restore"`
  (в т.ч. `Kind is null`).
- Чужая ветка — сброс фазы вместо ожидания (канон f-restore §2, §4.4):
  `PutStatusAsync(cluster, shard.Name, op with { Phase = "demolished" }, ct)`
  (при неудаче put — `Result.Failed`), затем `TransientAsync(cluster,
  $"foreign-snapshot/{shard}/{op.Id}", …)` — следующий тик из demolished
  повторяет POST (LaunchStepAsync: 409 → transient-повтор, 202 → операция
  стартует заново). Переходов заявки (REJOINING/FAILED/удаление) по чужому
  снимку нет — как и прежде.

### 5.5 Тесты

Юниты (`src/tests/PgWorker.UnitTests/PgNode/`, мок `IProcessRunner`;
AAA-комментарии):

- `RestoreOperationTests`:
  - `ShutdownWhileRunning_DoesNotPublishCancelled`: операция в фазе
    recovering с живым «постгрессом» (фейк `PgCtlStatusExit = 0`), вызвать
    `ShutdownAsync`, дождаться задачи — снимок НЕ `cancelled` (остался
    running своей фазы), postmaster остановлен (stop-вызовы в журнале
    фейка);
  - существующий контракт-тест операторского cancel → `cancelled`
    («cancelled by operator») остаётся без правок (регресс-гвард канала).
- `VerifyOperationTests` (при наличии шва): shutdown не публикует
  `cancelled`; операторский cancel — публикует.

Интеграционные:

- `src/tests/PgWorker.IntegrationTests/Backups/BackupApiOperationTests.cs`
  (шов `WebApplicationFactory<Program>` + `FakeOpsFactory`; фейк-операции
  получают `ShutdownAsync`):
  - `HostShutdown_ActiveOp_NotCancelled`: POST /api/ops/verify → операция
    running → `factory.Server.Host.StopAsync()` (триггерит
    ApplicationStopping) → `OpsRegistry.Current` НЕ `cancelled`;
    `FakeOps` фиксирует вызов `ShutdownAsync`;
  - `CancelAfterTerminal_Returns404_NoCancelCall`: операция завершена
    (фейк публикует терминал, `active` обнулён continuation'ом) → POST
    /api/ops/cancel → 404; `FakeOps.CancelCalls == 0` (обнуление active).
- `BackupVerifyProcessTests.cs`: хелперы `ApiOk`/`ApiDownloadTransient`/
  `ApiVerifyFailed` и снимки тестов получают `Kind: "verify"` (дефолт
  параметра); vf1/vf2 сохраняют ассерты; НОВЫЙ
  `ForeignKindSameId_NotMarked`: снимок `succeeded` с id кандидата и
  `kind: "restore"` → кандидат не изменён (не OK), выполнен повторный POST
  (снимок чужого вида — чужой итог).
- `RestoreProcessTests.cs`: снимки тестов — `Kind: "restore"`; fr1/fr2
  ПЕРЕПИСАТЬ под Р3: после первого тика `Phase == "demolished"`, статус
  заявки Running (не REJOINING/FAILED); после второго тика —
  `_receiver.StartCalls == 1` (повторный POST из demolished); НОВЫЙ
  `ForeignKindSameBackupId_NotRejoining`: снимок `succeeded` с
  backup_id заявки и `kind: "verify"` → demolished-повтор, заявка НЕ в
  REJOINING, `SystemId` — null.

E2E (`src/tests/PgWorker.E2eTests/E2e/E2eDrillScenarios.cs`):
`Drill_ReceiverRestart_PostmasterNotLeaked` — без изменений (ассерты (а)–(д)
покрывают и Р1: если бы stop pgnode публиковал `cancelled`, воркер удалил бы
ключ дрилла и терминал своей семантики не наступил — ассерт (а) краснеет).
Существующие `Drill_Succeeds_CleansUp`, `Backup_ChainBroken_SelfHeals` —
прогон без правок.

### 5.6 Роадмап

Пункт `t103-verify-postmaster-leak-registry` уже удалён из
`arch/roadmap/bugs.md` этой веткой (первый мерж-гейт-коммит); повторного
действия нет. Исторических упоминаний t103 вне `docs/superpowers/` в
`AGENTS.md`/`arch/**`/`docs/**` не остаётся (проверка — критерий 6).

## 6. Фазы работ

1. **Канон**: уточнения `arch/` по §4 (06-restore-drill §2 п.5.4,
   02-api §3 с таблицей kind, 01-full-backup §7, c-verify §2, f-restore
   §2). Коммит в feature-ветке ДО кодовых правок.
2. **Контракт и приёмник**: OperationSnapshot/OpsStatusResponse/ReceiverStatus
   — kind; IReceiverOperation.ShutdownAsync; RestoreOperation/
   VerifyOperation — `_shutdown` + kind в Update/Fail; Program.cs —
   active/activeId, continuation-обнуление, гварды, kind в TryBegin, хук →
   ShutdownAsync.
3. **Воркер**: BackupVerifyProcess — сверка id+kind; RestoreProcess — сверка
   id+kind + demolished-сброс чужой ветки.
4. **Тесты**: юниты shutdown; интеграционные BackupApiOperationTests
   (shutdown/cancel-after-terminal); обновление vf/fr + новые kind-кейсы.
5. **Верификация** (мерж-гейт; сборка и тесты — двумя ОТДЕЛЬНЫМИ командами):
   1. `DOTNET_CLI_UI_LANGUAGE=en dotnet run --project src/tests/PgWorker.E2ePrepare -c Release`
      (статическая подготовка E2E одним процессом ДО тестов);
   2. `DOTNET_CLI_UI_LANGUAGE=en dotnet build src/PgWorker.slnx -c Release --nologo`;
   3. `DOTNET_CLI_UI_LANGUAGE=en dotnet test src/tests/PgWorker.E2eTests -c Release --no-build --filter "FullyQualifiedName~Drill_|FullyQualifiedName~Backup_ChainBroken_SelfHeals"`.
   Плюс юниты/интеграционные серии затронутых проектов
   (`PgWorker.UnitTests`, `PgWorker.IntegrationTests`) с зачисткой
   контейнеров между сериями.

## 7. Ограничения

- Объём доработки — ТОЛЬКО #1–#4 (blocker + major MR-ревью); minors/nits
  (#5–#36, в т.ч. stopwaitsecs vs 15 c, ожидание задачи операции в хуке,
  guard-after-tar) — ВНЕ объёма, не трогаются.
- Бюджеты не меняются: shutdown-ожидание остаётся ≤ 15 c; stop-бюджеты
  первой реализации — без изменений; `stopwaitsecs` supervisord — НЕ
  трогать (minor #5 — за владельцем); E2E-бюджеты сценария — без изменений.
- P/Invoke запрещён; только штатные утилиты PG и управляемый .NET API.
- Поведение общего ops-реестра (одна ops-операция на процесс, 409-гвард,
  TryBegin поверх терминального) не меняется; операторский cancel API —
  контракт прежний (`cancelled` = «операции не было»).
- Обратная совместимость контракта снимка НЕ поддерживается: снимок без
  `kind` (старый приёмник) трактуется как чужой — смешанных версий
  воркер/приёмник в поставке нет (контур обновляется целиком).
- Полный бэкап (`FullBackupOperation`, свой реестр) — вне области; схема
  имён `backup-drill/<id>` и путь `/tmp/restore-pg.log` не меняются.
- Рестарта упавших прогонов «для выяснения» не выполнять; разбор — по
  собранным логам.

## 8. Принятые решения (закрытые развилки; 1–9 — первой редакции, подтверждены пользователем; 10–15 — доработки по MR-ревью)

1. **Объём**: чинить ОБА дефекта (Д1 + Д2) в одном флоу — они сцеплены.
2. **Каталог drill**: имя НЕ менять; закрывается защитой каталога, а не
   переименованием.
3. **Сверка id**: в ОБЕИХ процессах — BackupVerifyProcess и RestoreProcess
   (добавлено доработкой: сверка дополняется видом операции, решение 13).
4. **E2E-гейт**: точечные существующие кейсы + регресс-сценарий
   `Drill_ReceiverRestart_PostmasterNotLeaked` (уже в ветке; в доработке —
   без изменений).
5. **Роадмап-пункт**: удалён мерж-гейтом ветки (исполнено).
6. **«OK при живом процессе»** — закрыт как НЕ-дефект записи (§2.2
   примечание).
7. **Чужой running-снимок verify-супервиза**: реланч-POST по образцу drill
   (409 дёшев); для restore-супервиза — сброс demolished (решение 12),
   единообразия ожидания БЕЗ повторного запуска больше нет.
8. **Нестопаемый постгресс**: честный failed операции; терминальный исход
   чисткой не блокируется.
9. **Shutdown приёмника**: гасится активная ops-операция с ожиданием ≤ 15 с;
   full-операция — вне области.
10. **Р1 — разделение каналов отмены**: каналы разведены МЕТОДОМ интерфейса
    `ShutdownAsync()` (не флагом-параметром CancelAsync и не скрытым
    состоянием): хук физически не может позвать операторскую ветку.
    Shutdown-гашение НЕ публикует `cancelled` и вообще не перезаписывает
    снимок — снимок остаётся running своей фазы на время остановки; после
    смерти процесса воркер видит транспортный отказ (transient) → GET 404
    (реестр пуст) → vanished-повтор POST. Пересечение каналов (операторская
    отмена во время остановки): первым гасит тот, кто первым взял отмену;
    дважды снимок не перезаписывается.
11. **Р2 — время жизни active**: обнуление в continuation задачи операции
    (ReferenceEquals-гвард от затирания более новой операции) + гвард
    «running-снимок именно этой операции» (сверка `registry.Current.Id` с
    запомненным `activeId`) в хуке И cancel-маршруте. DI-реорганизация
    реестра ссылок не вводится (минимальная правка MapOpsRoutes).
12. **Р3 — чужой снимок restore**: сброс `Phase="demolished"` + transient
    (повтор POST следующим тиком) — симметрично 404-ветке; БЕЗусловен для
    любого чужого состояния, включая чужой running (409-гвард держит).
    Ожидание без сброса фазы удалено.
13. **Р4 — вид операции в контракте**: поле `kind` (JSON-имя `kind`;
    значения `verify|restore|drill`; константы — по образцу
    `OperationStates`). Заполняет ПРИЁМНИК: Program.cs при TryBegin
    (verify → verify; restore → `Id is null ? restore : drill`) и операции
    в каждом Update/Fail (kind определяется из собственного запроса
    операции, не из Current). На воркере парсится `ReceiverStatus.Kind`.
    Сверка: verify-супервиз — `id == full.Id && kind == verify`;
    restore-супервиз — `id == op.BackupId && kind == restore`; drill —
    прежняя сверка id (id дрилла уникален операции; #26/nit вне объёма).
    **null/отсутствующий kind = чужой снимок** (строгая трактовка): ложный
    вердикт исключён всегда; поставка воркера и приёмника единая
    (deploy/E2E пересобирают контур целиком), окна смешанных версий нет;
    фиксируется каноном 02-api §3.
14. **Тестовый долг Р3**: кодифицирующий ошибочное ожидание тест
    (`Phase=="recovering"`, `StartCalls==0`) ПЕРЕПИСАН на сброс
    фазы/повторный запуск (двухтиковый ассерт), не удалён.
15. **FakeOps/FakeRunner** интеграционных швов расширяются `ShutdownAsync`/
    kind — тестовые двойники зеркалят контракт, отдельные фейковые механики
    не вводятся.

## 9. Критерии приёмки

1. Канон `arch/` обновлён по §4 и закоммичен ДО кодовых правок; код
   зеркалит канон (ревью сверяет §4 ↔ §5: 06 §2 п.5.4 ↔ 5.1/5.2; 02-api §3
   ↔ 5.1/5.3/5.4; f-restore §2 ↔ 5.4).
2. **Р1 закрыт**: shutdown хоста при живой операции не публикует `cancelled`
   (юнит RestoreOperation + интеграционный HostShutdown_ActiveOp_
   NotCancelled зелёные); операторский cancel — прежний контракт
   (`cancelled/"cancelled by operator"`).
3. **Р2 закрыт**: после терминала операции cancel-маршрут отвечает 404 без
   вызова CancelAsync (CancelAfterTerminal_Returns404_NoCancelCall); хук
   обнуляет active — повторный stop/restart pgnode после завершённого
   restore не посылает стоп в PGDATA.
4. **Р3 закрыт**: чужой снимок (любое состояние) сбрасывает Phase заявки в
   `demolished`; второй тик повторяет POST (fr1/fr2 переписаны, зелёные);
   ожидание без выхода отсутствует в коде.
5. **Р4 закрыт**: `kind` присутствует в OperationSnapshot/OpsStatusResponse/
   ReceiverStatus; обе сверки (id+kind) отклоняют снимок чужого вида и без
   kind (vf/fr kind-кейсы зелёные); снимок старого приёмника трактуется
   как чужой.
6. Гейт-прогон §6.5 зелёный (E2ePrepare → build → точечные E2E на свежем
   Release, двумя отдельными командами, `--no-build`; юниты+интеграция
   затронутых проектов; зачистка контейнеров/сетей между сериями).
7. Бюджеты системы не изменены (15 c shutdown-ожидания — прежний;
   stopwaitsecs/E2E-бюджеты не тронуты); P/Invoke отсутствует; minors/nits
   ревью не «закрывались» попутно; языковые конвенции (документация —
   русский, идентификаторы — английский) соблюдены.
8. Исторических упоминаний t103 вне `docs/superpowers/` нет
   (`grep -rn "t103"` по `AGENTS.md`/`arch/`/`docs/` — пусто).
